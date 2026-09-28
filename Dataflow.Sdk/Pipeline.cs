using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using global::HuginnLabs.Proto;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;

namespace Dev.HuginnLabs.Dataflow;

/// <summary>
/// Delivery path from Span.End to the ingestion API: events land in a
/// bounded replay buffer and a background sender streams them over the
/// bidirectional StreamEvents RPC, trimming the buffer as the server
/// acknowledges durability (ack watermark). Failed batches are retried.
/// </summary>
internal static class Pipeline
{
    private static readonly List<TraceEvent> Buffer = new();
    private static readonly object BufLock = new();
    private static long _base = 1; // seq of the oldest buffered event
    private static long _seq;
    private static GrpcChannel? _channel;

    internal static long NextSeq() => Interlocked.Increment(ref _seq);

    internal static void Enqueue(TraceEvent e)
    {
        lock (BufLock)
        {
            Buffer.Add(e);
            while (Buffer.Count > Dataflow.Cfg.BufferSize)
            {
                Buffer.RemoveAt(0);
                Interlocked.Increment(ref _base);
            }
        }
    }

    internal static void Start()
    {
        // Plaintext http:// endpoints need the h2c switch; production
        // endpoints use https and ignore this.
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        _channel = GrpcChannel.ForAddress(
            Dataflow.Cfg.Endpoint.Contains("://")
                ? Dataflow.Cfg.Endpoint
                : "http://" + Dataflow.Cfg.Endpoint,
            new GrpcChannelOptions { MaxReceiveMessageSize = 16 * 1024 * 1024 });
        _ = Task.Run(SenderLoop);
    }

    private static async Task SenderLoop()
    {
        var backoff = TimeSpan.FromMilliseconds(500);
        while (true)
        {
            try
            {
                await Task.Delay(300);
                lock (BufLock) { if (Buffer.Count == 0) continue; }
                await FlushAsync();
                backoff = TimeSpan.FromMilliseconds(500);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"dataflow: send failed, retrying: {e.Message}");
                await Task.Delay(backoff);
                if (backoff < TimeSpan.FromSeconds(10)) backoff *= 2;
            }
        }
    }

    private static async Task FlushAsync()
    {
        // Snapshot up to 500 events WITHOUT removing them: the ack watermark
        // decides what leaves the replay buffer. Network I/O outside the lock.
        List<TraceEvent> batch;
        lock (BufLock)
        {
            batch = Buffer.Take(500).ToList();
        }
        if (batch.Count == 0) return;

        long acked = 0;
        var options = new CallOptions(
            headers: new Metadata { { "x-api-key", Dataflow.Cfg.ApiKey } },
            deadline: DateTime.UtcNow.AddSeconds(30));
        var call = Client().StreamEvents(options);

        foreach (var ev in batch)
        {
            await call.RequestStream.WriteAsync(ev);
        }
        await call.RequestStream.CompleteAsync();

        await foreach (var ack in call.ResponseStream.ReadAllAsync())
        {
            if (ack.LastSeq > acked) acked = ack.LastSeq;
        }
        if (acked > 0) Trim(acked);
    }

    private static void Trim(long acked)
    {
        lock (BufLock)
        {
            var drop = Math.Min(acked + 1 - Interlocked.Read(ref _base), Buffer.Count);
            if (drop <= 0) return;
            Buffer.RemoveRange(0, (int)drop);
            _base += drop;
        }
    }

    private static DataflowService.DataflowServiceClient? _client;

    private static DataflowService.DataflowServiceClient Client() =>
        _client ??= new DataflowService.DataflowServiceClient(_channel!);
}

internal static class Crypto
{
    private const int KeyLen = 32;
    private const int IvLen = 12;
    private const int SaltLen = 16;
    private const int Iterations = 10_000;

    private static readonly object InitLock = new();
    private static byte[]? _key;
    private static string _saltHex = "";

    internal static bool Init(string secret)
    {
        if (_key is not null) return true; // one key per process
        if (string.IsNullOrEmpty(secret)) return false;
        lock (InitLock)
        {
            if (_key is not null) return true;
            var salt = new byte[SaltLen];
            System.Security.Cryptography.RandomNumberGenerator.Fill(salt);
            using var deriver = new System.Security.Cryptography.Rfc2898DeriveBytes(
                secret, salt, Iterations, System.Security.Cryptography.HashAlgorithmName.SHA256);
            _key = deriver.GetBytes(KeyLen);
            _saltHex = Convert.ToHexString(salt).ToLowerInvariant();
            return true;
        }
    }

    internal static string SaltHex() => _saltHex;

    internal static global::HuginnLabs.Proto.PayloadData Seal(Dictionary<string, object?> payload)
    {
        var json = JsonWriter.Write(payload);
        if (_key is null)
        {
            return new global::HuginnLabs.Proto.PayloadData { Encrypted = false, Data = Google.Protobuf.ByteString.CopyFromUtf8(json) };
        }
        var iv = new byte[IvLen];
        System.Security.Cryptography.RandomNumberGenerator.Fill(iv);
        using var gcm = new System.Security.Cryptography.AesGcm(_key, 16);
        var plain = Encoding.UTF8.GetBytes(json);
        var ct = new byte[plain.Length];
        var tag = new byte[16];
        gcm.Encrypt(iv, plain, ct, tag);
        var data = new byte[ct.Length + tag.Length];
        ct.CopyTo(data, 0);
        tag.CopyTo(data, ct.Length);
        return new global::HuginnLabs.Proto.PayloadData
        {
            Encrypted = true,
            Data = Google.Protobuf.ByteString.CopyFrom(data),
            Iv = Google.Protobuf.ByteString.CopyFrom(iv),
            KeySalt = _saltHex,
        };
    }
}

internal static class JsonWriter
{
    internal static string Write(Dictionary<string, object?> payload)
    {
        var sb = new StringBuilder(256);
        sb.Append('{');
        var first = true;
        foreach (var (k, v) in payload)
        {
            if (!first) sb.Append(',');
            first = false;
            WriteString(sb, k);
            sb.Append(':');
            switch (v)
            {
                case null: sb.Append("null"); break;
                case Span.RawJson raw: sb.Append(raw.Json); break;
                case string s: WriteString(sb, s); break;
                case bool b: sb.Append(b ? "true" : "false"); break;
                case IFormattable n: sb.Append(n.ToString(null, System.Globalization.CultureInfo.InvariantCulture)); break;
                default: WriteString(sb, v.ToString() ?? ""); break;
            }
        }
        sb.Append('}');
        return sb.ToString();
    }

    private static void WriteString(StringBuilder sb, string s)
    {
        sb.Append('"');
        foreach (var c in s)
        {
            switch (c)
            {
                case '"': sb.Append("\\\""); break;
                case '\\': sb.Append("\\\\"); break;
                case '\n': sb.Append("\\n"); break;
                case '\r': sb.Append("\\r"); break;
                case '\t': sb.Append("\\t"); break;
                default:
                    if (c < ' ') sb.Append("\\u").Append(((int)c).ToString("x4"));
                    else sb.Append(c);
                    break;
            }
        }
        sb.Append('"');
    }
}

internal static class Agent
{
    private static readonly long StartedMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    private static Dictionary<string, string>? _cached;

    internal static IReadOnlyDictionary<string, string> Attrs()
    {
        if (_cached is not null) return _cached;
        var os = System.Runtime.InteropServices.RuntimeInformation.OSDescription;
        os = os.ToLowerInvariant() switch
        {
            var s when s.Contains("windows") => "windows",
            var s when s.Contains("darwin") => "darwin",
            var s when s.Contains("linux") => "linux",
            _ => os,
        };
        var attrs = new Dictionary<string, string>
        {
            ["agent.os"] = $"{os}/{System.Runtime.InteropServices.RuntimeInformation.OSArchitecture}",
            ["agent.runtime"] = $".NET {Environment.Version}",
            ["agent.sdk"] = $"dotnet-sdk/{Dataflow.SdkVersion}",
            ["agent.cpu"] = Environment.ProcessorCount.ToString(),
            ["agent.pid"] = Environment.ProcessId.ToString(),
            ["agent.started"] = StartedMs.ToString(),
        };
        var env = Environment.GetEnvironmentVariable("DATAFLOW_ENV");
        if (!string.IsNullOrEmpty(env)) attrs["agent.env"] = env;
        var ver = Environment.GetEnvironmentVariable("DATAFLOW_APP_VERSION");
        if (!string.IsNullOrEmpty(ver)) attrs["agent.app_version"] = ver;
        return _cached = attrs;
    }
}

internal static class Pii
{
    private static readonly (string Category, string[] Keywords)[] Categories =
    {
        ("password", new[] { "password", "passwd", "pwd" }),
        ("secret", new[] { "token", "secret", "apikey", "api_key", "credential", "session", "jwt", "auth" }),
        ("payment", new[] { "card", "pan", "cvv", "cvc", "iban", "expiry" }),
        ("email", new[] { "email", "e_mail", "mail" }),
        ("phone", new[] { "phone", "mobile", "tel", "msisdn" }),
        ("government_id", new[] { "ssn", "passport", "tax_id", "national_id" }),
        ("birth", new[] { "birth", "dob", "age" }),
        ("name", new[] { "first_name", "last_name", "full_name", "surname", "customer_name", "display_name" }),
        ("address", new[] { "street", "zip", "postal", "street_address", "postal_address", "home_address", "billing_address", "shipping_address", "mailing_address" }),
        ("geo", new[] { "city", "country", "region", "location", "lat", "lon", "lng" }),
        ("ip", new[] { "ip", "ip_address", "client_ip", "remote_addr" }),
        ("device", new[] { "device", "user_agent", "imei", "fingerprint" }),
    };

    internal static string Classify(List<string> fields)
    {
        var seen = new SortedSet<string>();
        foreach (var field in fields)
        {
            var norm = new string(field.ToLowerInvariant()
                .Select(c => char.IsAsciiLetterOrDigit(c) ? c : '_').ToArray());
            var tokens = new HashSet<string>(norm.Split('_'));
            foreach (var (category, keywords) in Categories)
            {
                if (seen.Contains(category)) continue;
                foreach (var kw in keywords)
                {
                    var hit = kw.Contains('_') ? norm.Contains(kw) : tokens.Contains(kw);
                    if (hit)
                    {
                        seen.Add(category);
                        break;
                    }
                }
            }
        }
        return string.Join(",", seen);
    }
}
