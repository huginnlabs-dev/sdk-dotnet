using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Dev.HuginnLabs.Dataflow;
using HuginnLabs.Proto;
using Microsoft.Extensions.Logging;
using Xunit;

namespace DataflowSdk.Tests;

// Log shipping end to end: record shape via the buffered-queue seams, wire
// shape against a real HTTP stub on the IPv6 loopback. One test class so the
// enable/disable flips stay sequential — it shares the "Dataflow Globals"
// collection with the other suites mutating the global settings/buffer.
// The constructor resets every global (env var, trace listener, settings,
// queue) so tests are order-independent.
[Collection("Dataflow Globals")]
public class LogTests
{
    public LogTests()
    {
        Environment.SetEnvironmentVariable("DATAFLOW_HTTP_URL", null);
        Dataflow.UninstallLogHandler();
        Dataflow.Configure(new Dataflow.Settings { Disabled = true });
        Logs.LoopPaused = true;
        Logs.Clear();
        lock (Pipeline.BufLock) Pipeline.Buffer.Clear();
    }

    // Logs ship over a URL-form DATAFLOW_ENDPOINT (the manifest's direct
    // base resolution) — never the DATAFLOW_HTTP_URL env var, which is
    // process-global and leaks into the parallel scan-tool tests. The same
    // URL feeds the gRPC sender too; its h2c prefaces (and the startup
    // manifest POST) arrive at the stub and are filtered by path, while the
    // gRPC retries harmlessly in the background.
    private static LogStub EnableLogging()
    {
        var stub = new LogStub();
        Dataflow.Configure(new Dataflow.Settings
        {
            Endpoint = stub.BaseURL,
            ApiKey = "test-key",
            ServiceName = "sdk-tests",
            Disabled = false,
        });
        Logs.LoopPaused = true;
        Logs.Clear();
        return stub;
    }

    private static void EnableTracingOnly(string endpoint)
    {
        Environment.SetEnvironmentVariable("DATAFLOW_HTTP_URL", null);
        Dataflow.Configure(new Dataflow.Settings
        {
            Endpoint = endpoint,
            ApiKey = "test-key",
            ServiceName = "sdk-tests",
            Disabled = false,
        });
        Logs.LoopPaused = true;
        Logs.Clear();
    }

    private static List<LogLine> Pending() => new(Logs.PendingSnapshot());

    private static List<TraceEvent> Spans()
    {
        lock (Pipeline.BufLock) return new List<TraceEvent>(Pipeline.Buffer);
    }

    private static async Task<List<LogStub.Request>> WaitForRequests(
        LogStub stub, int count, int timeoutMs = 5000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            var snap = stub.Snapshot();
            if (snap.Count >= count) return snap;
            await Task.Delay(25, TestContext.Current.CancellationToken);
        }
        var final = stub.Snapshot();
        Assert.True(final.Count >= count, $"expected {count} requests, saw {final.Count}");
        return final;
    }

    // ------------------------------------------------------------------
    // Ambient trace correlation

    [Fact]
    public void Helpers_AttachAmbientSpanIds()
    {
        using var _ = EnableLogging();
        string traceId, spanId, innerSpanId;
        using (var scope = Dataflow.Trace("orders.Handler"))
        {
            traceId = scope.Span.TraceIdValue();
            spanId = scope.Span.SpanId;
            Dataflow.Info("outer line");

            using (var inner = Dataflow.Trace("orders.Inner"))
            {
                Dataflow.Warn("inner line");
                innerSpanId = inner.Span.SpanId;
            }
        }

        // Outside any span the ids stay empty.
        Dataflow.Error("bare line");

        var recs = Pending();
        Assert.Equal(3, recs.Count);
        Assert.Equal("outer line", recs[0].Message);
        Assert.Equal(traceId, recs[0].TraceId);
        Assert.Equal(spanId, recs[0].SpanId);

        // The child joins the same trace with its own span id.
        Assert.Equal(traceId, recs[1].TraceId);
        Assert.Equal(innerSpanId, recs[1].SpanId);

        Assert.Equal("", recs[2].TraceId);
        Assert.Equal("", recs[2].SpanId);
    }

    [Fact]
    public void Helpers_CarryTimestampServiceNameAndFields()
    {
        using var _ = EnableLogging();
        var before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        Dataflow.Info("order placed", new Dictionary<string, object>
        {
            ["user"] = "ann",
            ["count"] = 42,
            ["ratio"] = 0.5,
        });

        var rec = Assert.Single(Pending());
        Assert.InRange(rec.Timestamp, before - 1000, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1000);
        Assert.Equal("sdk-tests", rec.ServiceName);
        Assert.Equal("ann", rec.Fields["user"]);
        Assert.Equal("42", rec.Fields["count"]);
        Assert.Equal("0.5", rec.Fields["ratio"]);
    }

    // ------------------------------------------------------------------
    // Level mapping

    [Fact]
    public void Helpers_MapLevels()
    {
        using var _ = EnableLogging();
        Dataflow.Debug("d");
        Dataflow.Info("i");
        Dataflow.Warn("w");
        Dataflow.Error("e");

        // Log() normalizes: case-insensitive, "warning" folds onto warn,
        // unknown levels degrade to info.
        Dataflow.Log("DEBUG", "l1");
        Dataflow.Log("  warn  ", "l2");
        Dataflow.Log("warning", "l3");
        Dataflow.Log("verbose", "l4");
        Dataflow.Log("", "l5");

        var recs = Pending();
        Assert.Equal(9, recs.Count);
        Assert.Equal("debug", recs[0].Level);
        Assert.Equal("info", recs[1].Level);
        Assert.Equal("warn", recs[2].Level);
        Assert.Equal("error", recs[3].Level);
        Assert.Equal("debug", recs[4].Level);
        Assert.Equal("warn", recs[5].Level);
        Assert.Equal("warn", recs[6].Level);
        Assert.Equal("info", recs[7].Level);
        Assert.Equal("info", recs[8].Level);
    }

    // ------------------------------------------------------------------
    // Clamps (client-side mirrors of the server limits)

    [Fact]
    public void Records_ClampMessageAndFields()
    {
        using var _ = EnableLogging();
        var fields = new Dictionary<string, object>();
        for (var i = 0; i < 60; i++) fields[$"f{i}"] = new string('v', 600);
        fields[""] = "no key";
        fields["nullable"] = null!;

        Dataflow.Error(new string('m', 10_000), fields);

        var rec = Assert.Single(Pending());
        Assert.Equal("error", rec.Level);
        Assert.Equal(8192, rec.Message.Length);
        // 50-field cap keeps the first 50 inserts; empty keys are skipped
        // and the overflow entries never make it in.
        Assert.Equal(50, rec.Fields.Count);
        Assert.DoesNotContain("nullable", rec.Fields.Keys);
        Assert.Equal(512, rec.Fields["f0"].Length);
    }

    // ------------------------------------------------------------------
    // Wire contract: POST {base}/api/v1/logs, X-Api-Key, JSON body

    [Fact]
    public void Flush_PostsWireShapeApiKeyAndTraceIds()
    {
        using var stub = EnableLogging();
        using var scope = Dataflow.Trace("orders.Handler");
        Dataflow.Info("hello", new Dictionary<string, object> { ["k"] = "v" });

        Dataflow.FlushLogs(); // synchronous: when it returns the POST is done

        var req = Assert.Single(stub.Snapshot());
        Assert.Equal("POST", req.Method);
        Assert.Equal("/api/v1/logs", req.Path);
        Assert.Equal("test-key", req.ApiKey);

        using var doc = JsonDocument.Parse(req.Body);
        var logs = doc.RootElement.GetProperty("logs");
        Assert.Equal(1, logs.GetArrayLength());
        var line = logs[0];
        Assert.Equal("hello", line.GetProperty("message").GetString());
        Assert.Equal("info", line.GetProperty("level").GetString());
        Assert.Equal("sdk-tests", line.GetProperty("service_name").GetString());
        Assert.Equal(scope.Span.TraceIdValue(), line.GetProperty("trace_id").GetString());
        Assert.Equal(scope.Span.SpanId, line.GetProperty("span_id").GetString());
        var ts = line.GetProperty("timestamp").GetInt64();
        Assert.True(Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - ts) < 60_000);
        Assert.Equal("v", line.GetProperty("fields").GetProperty("k").GetString());
    }

    [Fact]
    public void Flush_BatchesAtMost1000PerPost()
    {
        using var stub = EnableLogging();
        // 1050 lines overflow the 1024 queue (26 oldest dropped), and the
        // 1024 survivors leave in two POSTs: 1000 + 24.
        for (var i = 0; i < 1050; i++) Dataflow.Info($"m{i}");

        Dataflow.FlushLogs();

        var reqs = stub.Snapshot();
        Assert.True(reqs.Count == 2, $"expected 2 POSTs (1000 + 24), saw {reqs.Count}, dropped={Logs.Dropped}");
        var batches = reqs.Select(r =>
            JsonDocument.Parse(r.Body).RootElement.GetProperty("logs").GetArrayLength()).ToList();
        Assert.Equal(1000, batches[0]);
        Assert.Equal(24, batches[1]);
    }

    // ------------------------------------------------------------------
    // Overflow: drop-oldest with a counter

    [Fact]
    public void Queue_OverflowDropsOldestAndCounts()
    {
        using var _ = EnableLogging();
        for (var i = 0; i < 1100; i++) Dataflow.Info($"m{i}");

        Assert.Equal(1024, Logs.PendingSnapshot().Count);
        Assert.Equal(76, Logs.Dropped);
        // The oldest survivor is line 76.
        Assert.Equal("m76", Logs.PendingSnapshot()[0].Message);
        Assert.Equal("m1099", Logs.PendingSnapshot()[1023].Message);
    }

    // ------------------------------------------------------------------
    // Delivery failures: one retry, then drop

    [Fact]
    public void Flush_RetriesOnceThenDrops()
    {
        var stub = EnableLogging();
        stub.FailFirst = 2; // the first attempt and its retry both 500

        Dataflow.Info("lost-1");
        Dataflow.Info("lost-2");
        Dataflow.FlushLogs();

        Assert.Equal(2, stub.Snapshot().Count); // exactly one retry happened
        Assert.Equal(2, Logs.Dropped);

        // The pipeline recovers: the next batch ships.
        stub.FailFirst = 0;
        Dataflow.Info("kept");
        Dataflow.FlushLogs();

        var reqs = stub.Snapshot();
        Assert.Equal(3, reqs.Count);
        Assert.Contains("kept", reqs[2].Body);
        Assert.Equal(2, Logs.Dropped); // only the first batch was counted
    }

    // ------------------------------------------------------------------
    // Background flusher: 500ms ticker / 50-line wake

    [Fact]
    public async Task Flusher_LoopShipsOnItsOwn()
    {
        using var stub = EnableLogging();
        Logs.LoopPaused = false; // let the daemon run

        Dataflow.Info("tick line");
        var reqs = await WaitForRequests(stub, 1);

        Assert.Contains("tick line", reqs[0].Body);
    }

    // ------------------------------------------------------------------
    // Microsoft.Extensions.Logging bridge

    [Fact]
    public void LoggerProvider_ForwardsLevelsStateAndTraceIds()
    {
        using var _ = EnableLogging();
        using var factory = LoggerFactory.Create(b =>
        {
            b.AddProvider(new DataflowLoggerProvider());
            b.SetMinimumLevel(LogLevel.Trace);
        });
        var logger = factory.CreateLogger("orders");
        Assert.True(logger.IsEnabled(LogLevel.Information));

        using var scope = Dataflow.Trace("orders.Handler");
        logger.LogTrace("tiny");
        logger.LogDebug("dbg");
        logger.LogInformation("placed {OrderId} at {Place}", 7, "ber");
        logger.LogWarning("hmm");
        logger.LogError("bad");
        logger.LogCritical("worst");

        var recs = Pending();
        Assert.Equal(6, recs.Count);
        Assert.Equal("debug", recs[0].Level);
        Assert.Equal("debug", recs[1].Level);
        Assert.Equal("info", recs[2].Level);
        Assert.Equal("warn", recs[3].Level);
        Assert.Equal("error", recs[4].Level);
        Assert.Equal("error", recs[5].Level);

        // Structured state entries become stringified fields; scopes are
        // skipped in v1 and the message template is not a field.
        Assert.Equal("placed 7 at ber", recs[2].Message);
        Assert.Equal("7", recs[2].Fields["OrderId"]);
        Assert.Equal("ber", recs[2].Fields["Place"]);
        Assert.False(recs[2].Fields.ContainsKey("{OriginalFormat}"));
        Assert.Equal(scope.Span.TraceIdValue(), recs[2].TraceId);
    }

    // ------------------------------------------------------------------
    // System.Diagnostics.Trace bridge (InstallLogHandler)

    [Fact]
    public void InstallLogHandler_BridgesTraceSeveritiesIdempotently()
    {
        using var stub = EnableLogging();

        Dataflow.InstallLogHandler();
        Dataflow.InstallLogHandler(); // second install is a no-op
        Assert.Equal(1, Dataflow.LogHandlerCount);
        Assert.Single(Trace.Listeners.OfType<DataflowTraceListener>());

        try
        {
            Trace.TraceError("boom {0}", 7);
            Trace.TraceWarning("careful");
            Trace.TraceInformation("fyi");
            Trace.Write("plain");

            Dataflow.FlushLogs();

            var req = Assert.Single(stub.Snapshot());
            using var doc = JsonDocument.Parse(req.Body);
            var logs = doc.RootElement.GetProperty("logs");
            Assert.Equal(4, logs.GetArrayLength());
            Assert.Equal("error", logs[0].GetProperty("level").GetString());
            Assert.Equal("boom 7", logs[0].GetProperty("message").GetString());
            Assert.Equal("warn", logs[1].GetProperty("level").GetString());
            Assert.Equal("info", logs[2].GetProperty("level").GetString());
            Assert.Equal("info", logs[3].GetProperty("level").GetString());
        }
        finally
        {
            Dataflow.UninstallLogHandler();
        }

        Assert.Equal(0, Dataflow.LogHandlerCount);
        Assert.DoesNotContain(Trace.Listeners.OfType<DataflowTraceListener>().ToList(), _ => true);
    }

    // ------------------------------------------------------------------
    // Disabled / logging-off

    [Fact]
    public void Disabled_EverythingIsANoOp()
    {
        Dataflow.Configure(new Dataflow.Settings { Disabled = true });

        Dataflow.Debug("x");
        Dataflow.Info("x");
        Dataflow.Warn("x");
        Dataflow.Error("x");
        Dataflow.Log("error", "x");
        Dataflow.FlushLogs(); // must not throw

        Assert.Empty(Pending());

        using var factory = LoggerFactory.Create(b => b.AddProvider(new DataflowLoggerProvider()));
        var logger = factory.CreateLogger("t");
        Assert.False(logger.IsEnabled(LogLevel.Information));
        logger.LogInformation("suppressed");

        Dataflow.InstallLogHandler();
        try
        {
            Trace.TraceInformation("quiet");
            Dataflow.FlushLogs();
            Assert.Empty(Pending());
        }
        finally
        {
            Dataflow.UninstallLogHandler();
        }
    }

    [Fact]
    public void BareHostPortEndpoint_LoggingStaysOffWhileTracingWorks()
    {
        // A bare gRPC endpoint with no DATAFLOW_HTTP_URL override resolves
        // no HTTP base: records are not even buffered, spans still flow.
        EnableTracingOnly("10.1.2.3:4317");

        using (Dataflow.Trace("orders.Handler"))
        {
            Dataflow.Info("hidden");
        }

        Assert.Empty(Pending());
        var span = Assert.Single(Spans());
        Assert.Equal("orders.Handler", span.Name);
    }
}

/// <summary>
/// Minimal HTTP stub on a random [::1] port: a raw TcpListener speaking just
/// enough HTTP/1.1 to capture POST /api/v1/logs requests (no HttpListener,
/// no URL ACLs). Every request is answered Connection: close, so one
/// connection == one request. FailFirst answers that many requests 500
/// before turning healthy (the retry-then-drop seam).
/// </summary>
internal sealed class LogStub : IDisposable
{
    internal sealed record Request(string Method, string Path, string ApiKey, string Body);

    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _cts = new();
    private readonly List<Request> _requests = new();
    private readonly object _lock = new();

    internal volatile int FailFirst;

    public LogStub()
    {
        _listener = new TcpListener(IPAddress.IPv6Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _ = Task.Run(AcceptLoop);
    }

    public int Port { get; }
    public string BaseURL => $"http://[::1]:{Port}";

    internal List<Request> Snapshot()
    {
        lock (_lock) return new List<Request>(_requests);
    }

    private async Task AcceptLoop()
    {
        while (!_cts.IsCancellationRequested)
        {
            TcpClient client;
            try { client = await _listener.AcceptTcpClientAsync(_cts.Token); }
            catch { return; }
            using (client)
            {
                try { Handle(client); }
                catch { /* malformed or timed-out request: drop it */ }
            }
        }
    }

    private void Handle(TcpClient client)
    {
        var stream = client.GetStream();
        stream.ReadTimeout = 3000;
        var data = new MemoryStream();
        var buffer = new byte[16_384];

        var headerEnd = -1;
        while ((headerEnd = FindHeaderEnd(data)) < 0)
        {
            var n = stream.Read(buffer, 0, buffer.Length);
            if (n <= 0) return;
            data.Write(buffer, 0, n);
        }

        var raw = data.GetBuffer();
        var head = Encoding.ASCII.GetString(raw, 0, headerEnd);
        var lines = head.Split("\r\n");
        var parts = lines[0].Split(' ');
        if (parts.Length < 2) return;

        string? apiKey = null;
        var contentLength = 0;
        for (var i = 1; i < lines.Length; i++)
        {
            var colon = lines[i].IndexOf(':');
            if (colon <= 0) continue;
            var name = lines[i][..colon].Trim();
            var value = lines[i][(colon + 1)..].Trim();
            if (name.Equals("x-api-key", StringComparison.OrdinalIgnoreCase)) apiKey = value;
            else if (name.Equals("content-length", StringComparison.OrdinalIgnoreCase))
                contentLength = int.Parse(value);
        }

        var bodyStart = headerEnd + 4;
        while (data.Length - bodyStart < contentLength)
        {
            var n = stream.Read(buffer, 0, buffer.Length);
            if (n <= 0) break;
            data.Write(buffer, 0, n);
        }
        // ToArray() re-snapshots: GetBuffer()'s array goes stale when the
        // stream reallocates while a large body is still being read.
        var snap = data.ToArray();
        var bodyCount = (int)Math.Clamp(contentLength, 0, snap.Length - bodyStart);
        var body = Encoding.UTF8.GetString(snap, bodyStart, bodyCount);

        // The same endpoint serves the gRPC sender's h2c prefaces and the
        // startup manifest POST — they get a canned 200 and are not logged.
        bool isLogPost = parts[0] == "POST" && parts[1] == "/api/v1/logs";
        int status;
        lock (_lock)
        {
            if (isLogPost)
            {
                _requests.Add(new Request(parts[0], parts[1], apiKey ?? "", body));
                status = _requests.Count <= FailFirst ? 500 : 200;
            }
            else
            {
                status = 200;
            }
        }

        var reason = status == 200 ? "OK" : "Internal Server Error";
        var response = Encoding.ASCII.GetBytes(
            $"HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        stream.Write(response, 0, response.Length);
    }

    private static int FindHeaderEnd(MemoryStream data)
    {
        var buf = data.GetBuffer();
        var len = (int)data.Length;
        for (var i = 0; i + 3 < len; i++)
        {
            if (buf[i] == 13 && buf[i + 1] == 10 && buf[i + 2] == 13 && buf[i + 3] == 10) return i;
        }
        return -1;
    }

    public void Dispose()
    {
        _cts.Cancel();
        _listener.Stop();
        _cts.Dispose();
    }
}
