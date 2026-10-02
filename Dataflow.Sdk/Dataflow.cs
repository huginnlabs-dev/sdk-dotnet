using System;
using System.Data.Common;
using System.Threading;

namespace Dev.HuginnLabs.Dataflow;

/// <summary>
/// HuginnLabs Dataflow SDK for .NET — entry point. Spans stream over gRPC
/// (<c>StreamEvents</c>) with an ack-watermark replay buffer; payload
/// values are AES-256-GCM encrypted with a PBKDF2-derived key that never
/// leaves the process.
/// </summary>
public static partial class Dataflow
{
    /// <summary>SDK version stamped into agent metadata and the manifest.</summary>
    public const string SdkVersion = "0.7.0";

    /// <summary>Immutable SDK configuration.</summary>
    public sealed record Settings
    {
        public string Endpoint { get; init; } = "";
        public string ApiKey { get; init; } = "";
        public string ServiceName { get; init; } = "";
        public string EncryptionKey { get; init; } = "";
        public double SampleRatio { get; init; } = 1.0;
        public int BufferSize { get; init; } = 10_000;
        public bool Disabled { get; init; }
    }

    private static Settings _settings = new Settings { Disabled = true };
    private static int _started;

    private static string Env(string key, string fallback = "") =>
        Environment.GetEnvironmentVariable(key) is { Length: > 0 } v ? v : fallback;

    /// <summary>
    /// Configures the SDK from <c>DATAFLOW_*</c> environment variables and
    /// starts the background sender. Idempotent: repeated calls update the
    /// settings but never spawn a second sender.
    /// </summary>
    public static void Configure() => Configure(new Settings
    {
        Endpoint = Env("DATAFLOW_ENDPOINT"),
        ApiKey = Env("DATAFLOW_API_KEY"),
        ServiceName = Env("DATAFLOW_SERVICE_NAME"),
        EncryptionKey = Env("DATAFLOW_ENCRYPTION_KEY"),
        SampleRatio = double.TryParse(Env("DATAFLOW_SAMPLE_RATIO"), out var r) ? r : 1.0,
        BufferSize = int.TryParse(Env("DATAFLOW_BUFFER_SIZE"), out var b) ? b : 10_000,
        Disabled = Env("DATAFLOW_DISABLED", "false") == "true",
    });

    public static void Configure(Settings settings)
    {
        _settings = settings;
        if (Enabled() && Interlocked.Exchange(ref _started, 1) == 0)
        {
            if (string.IsNullOrEmpty(settings.EncryptionKey))
            {
                Console.Error.WriteLine("dataflow: warning: no encryption key set; captured payloads are sent as plaintext");
            }
            Pipeline.Start();
        }
    }

    /// <summary>True when spans are collected and shipped.</summary>
    public static bool Enabled() =>
        !_settings.Disabled && _settings.Endpoint.Length > 0 && _settings.ApiKey.Length > 0;

    internal static Settings Cfg => _settings;

    /// <summary>The configured service name (defaults to the host name).</summary>
    public static string ServiceName()
    {
        var n = _settings.ServiceName;
        if (n.Length > 0) return n;
        return Environment.MachineName is { Length: > 0 } m ? m : "unknown";
    }

    internal static void Enqueue(global::HuginnLabs.Proto.TraceEvent e) => Pipeline.Enqueue(e);

    /// <summary>Opens a child span of the ambient span (or a new trace).</summary>
    public static Span StartSpan(string name, string type = "FUNCTION_CALL")
    {
        var parent = Span.CurrentSpan();
        return new Span(name, type, parent, incomingTraceId: null);
    }

    /// <summary>Opens an entry-point span, adopting the incoming trace id.</summary>
    public static Span StartServerSpan(string route, string? incomingTraceId = null)
    {
        var span = new Span(route, "HTTP_SERVER", parent: null, incomingTraceId);
        foreach (var (k, v) in Agent.Attrs()) span.Attr(k, v);
        return span;
    }

    /// <summary>Runs <paramref name="body"/> inside a named span scope.</summary>
    public static TraceScope Trace(string name, string type = "FUNCTION_CALL") => new(name, type);

    /// <summary>
    /// Wraps an ADO.NET connection so every command execution emits a
    /// DB_QUERY span (verb + table as the name, the dialect in
    /// <c>db.system</c>, the statement text in <c>db.statement</c> —
    /// parameter values are never captured). <paramref name="system"/> is
    /// the database system or driver name ("npgsql", "mysql", "sqlite",
    /// "sqlserver"); known driver names normalize to the system
    /// ("npgsql" → "postgres").
    /// <code>await using var conn = Dataflow.Wrap(new NpgsqlConnection(cs), "npgsql");</code>
    /// </summary>
    public static DbConnection Wrap(DbConnection connection, string system) =>
        new DataflowConnection(connection, system);
}
