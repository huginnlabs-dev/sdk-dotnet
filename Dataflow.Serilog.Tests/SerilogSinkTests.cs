using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Dev.HuginnLabs.Dataflow;
using Serilog;
using Serilog.Events;
using Xunit;

namespace DataflowSdkSerilog.Tests;

// Serilog sink end to end: level/message/field mapping via the buffered-queue
// seams, wire shape against a real HTTP stub on the IPv6 loopback. One test
// class so the enable/disable flips stay sequential; the constructor resets
// every global (env var, settings, queue) so tests are order-independent.
public class SerilogSinkTests
{
    public SerilogSinkTests()
    {
        Environment.SetEnvironmentVariable("DATAFLOW_HTTP_URL", null);
        Dataflow.Configure(new Dataflow.Settings { Disabled = true });
        Logs.LoopPaused = true;
        Logs.Clear();
    }

    // Logging ships over a URL-form DATAFLOW_ENDPOINT (the manifest's direct
    // base resolution). The same URL feeds the gRPC sender; its h2c prefaces
    // (and the startup manifest POST) arrive at the stub and are filtered by
    // path, while the gRPC retries harmlessly in the background.
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

    private static ILogger SinkLogger() => new LoggerConfiguration()
        .MinimumLevel.Verbose()
        .WriteTo.Sink(DataflowSerilog.SerilogSink()) // the factory convenience
        .CreateLogger();

    private static ILogger ExtensionLogger() => new LoggerConfiguration()
        .MinimumLevel.Verbose()
        .WriteTo.Dataflow() // the idiomatic registration
        .CreateLogger();

    private static List<LogLine> Pending() => new(Logs.PendingSnapshot());

    // ------------------------------------------------------------------
    // Level + message mapping

    [Fact]
    public void WireLevel_FoldsAllSixSerilogLevels()
    {
        Assert.Equal("debug", DataflowSink.WireLevel(LogEventLevel.Verbose));
        Assert.Equal("debug", DataflowSink.WireLevel(LogEventLevel.Debug));
        Assert.Equal("info", DataflowSink.WireLevel(LogEventLevel.Information));
        Assert.Equal("warn", DataflowSink.WireLevel(LogEventLevel.Warning));
        Assert.Equal("error", DataflowSink.WireLevel(LogEventLevel.Error));
        Assert.Equal("error", DataflowSink.WireLevel(LogEventLevel.Fatal));
    }

    [Fact]
    public void Sink_MapsLevelsOntoWireVocabulary()
    {
        using var _ = EnableLogging();
        var logger = SinkLogger();

        logger.Verbose("v");
        logger.Debug("d");
        logger.Information("i");
        logger.Warning("w");
        logger.Error("e");
        logger.Fatal("f");

        var recs = Pending();
        Assert.Equal(6, recs.Count);
        Assert.Equal(new[] { "debug", "debug", "info", "warn", "error", "error" },
            recs.Select(r => r.Level).ToArray());
        Assert.Equal(new[] { "v", "d", "i", "w", "e", "f" },
            recs.Select(r => r.Message).ToArray());
    }

    [Fact]
    public void Sink_RendersMessageTemplateAndCarriesProperties()
    {
        using var _ = EnableLogging();
        ExtensionLogger().Information("order {OrderId} placed at {Place}", 7, "ber");

        var rec = Assert.Single(Pending());
        // Serilog renders string scalars quoted in the rendered message.
        Assert.Equal("order 7 placed at \"ber\"", rec.Message);
        Assert.Equal("7", rec.Fields["OrderId"]);
        Assert.Equal("ber", rec.Fields["Place"]);
        // The template itself is not a field.
        Assert.False(rec.Fields.ContainsKey("{OriginalFormat}"));
    }

    [Fact]
    public void Sink_StringifiesScalarPropertyTypes()
    {
        using var _ = EnableLogging();
        SinkLogger().Information("mixed {S} {B} {D} {N} {Nul}", "ann", true, 0.5, 42L, (object?)null);

        var rec = Assert.Single(Pending());
        Assert.Equal("ann", rec.Fields["S"]);
        Assert.Equal("True", rec.Fields["B"]);
        Assert.Equal("0.5", rec.Fields["D"]); // invariant culture
        Assert.Equal("42", rec.Fields["N"]);
        Assert.Equal("", rec.Fields["Nul"]); // null scalars stringify empty
    }

    [Fact]
    public void Sink_AttachedExceptionRidesOnTheMessage()
    {
        using var _ = EnableLogging();
        SinkLogger().Error(new InvalidOperationException("boom"), "request failed");

        var rec = Assert.Single(Pending());
        Assert.Equal("error", rec.Level);
        Assert.StartsWith("request failed ", rec.Message);
        Assert.Contains("boom", rec.Message);
    }

    // ------------------------------------------------------------------
    // Clamps (client-side mirrors of the server limits)

    [Fact]
    public void Sink_ClampsPropertiesToServerLimits()
    {
        using var _ = EnableLogging();
        var template = string.Join(" ", Enumerable.Range(0, 60).Select(i => $"{{f{i}}}"));
        var args = Enumerable.Range(0, 60).Select(_ => (object?)new string('v', 600)).ToArray();
        SinkLogger().Information(template, args);

        var rec = Assert.Single(Pending());
        // 50-field cap keeps the first 50; values clip at 512 chars.
        Assert.Equal(50, rec.Fields.Count);
        Assert.Equal(512, rec.Fields["f0"].Length);
        Assert.False(rec.Fields.ContainsKey("f59"));
    }

    [Fact]
    public void Sink_MessageClipsAtPipelineLimit()
    {
        using var _ = EnableLogging();
        SinkLogger().Information(new string('m', 10_000));

        var rec = Assert.Single(Pending());
        Assert.Equal(8192, rec.Message.Length); // Logs.MaxMessage, applied by Ship
    }

    // ------------------------------------------------------------------
    // Wire contract: POST {base}/api/v1/logs, X-Api-Key, JSON body

    [Fact]
    public void Flush_PostsWireShapeApiKeyAndTraceIds()
    {
        using var stub = EnableLogging();
        using var scope = Dataflow.Trace("orders.Handler");
        ExtensionLogger().Information("hello {k}", "v");

        Dataflow.FlushLogs(); // synchronous: when it returns the POST is done

        var req = Assert.Single(stub.Snapshot());
        Assert.Equal("POST", req.Method);
        Assert.Equal("/api/v1/logs", req.Path);
        Assert.Equal("test-key", req.ApiKey);

        using var doc = JsonDocument.Parse(req.Body);
        var logs = doc.RootElement.GetProperty("logs");
        Assert.Equal(1, logs.GetArrayLength());
        var line = logs[0];
        Assert.Equal("hello \"v\"", line.GetProperty("message").GetString());
        Assert.Equal("info", line.GetProperty("level").GetString());
        Assert.Equal("sdk-tests", line.GetProperty("service_name").GetString());
        // Ambient span correlation works through the sink too.
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
        var logger = SinkLogger();
        // 1050 lines overflow the 1024 queue (26 oldest dropped), and the
        // 1024 survivors leave in two POSTs: 1000 + 24.
        for (var i = 0; i < 1050; i++) logger.Information("m{i}", i);

        Dataflow.FlushLogs();

        var reqs = stub.Snapshot();
        Assert.True(reqs.Count == 2, $"expected 2 POSTs (1000 + 24), saw {reqs.Count}, dropped={Logs.Dropped}");
        var batches = reqs.Select(r =>
            JsonDocument.Parse(r.Body).RootElement.GetProperty("logs").GetArrayLength()).ToList();
        Assert.Equal(1000, batches[0]);
        Assert.Equal(24, batches[1]);
    }

    // ------------------------------------------------------------------
    // Disabled / logging-off

    [Fact]
    public void Disabled_SinkIsANoOp()
    {
        Dataflow.Configure(new Dataflow.Settings { Disabled = true });

        var logger = SinkLogger();
        logger.Verbose("x");
        logger.Information("x");
        logger.Error(new Exception("x"), "x");
        DataflowSerilog.SerilogSink().Emit(new LogEvent( // direct Emit, no logger
            DateTimeOffset.UtcNow, LogEventLevel.Information, null,
            new Serilog.Parsing.MessageTemplateParser().Parse("direct"),
            new List<LogEventProperty>()));
        Dataflow.FlushLogs(); // must not throw

        Assert.Empty(Pending());
    }

    [Fact]
    public void BareHostPortEndpoint_SinkShipsNothingWhileTracingWorks()
    {
        // A bare gRPC endpoint with no DATAFLOW_HTTP_URL override resolves
        // no HTTP base: the sink buffers nothing, spans still flow.
        Dataflow.Configure(new Dataflow.Settings
        {
            Endpoint = "10.1.2.3:4317",
            ApiKey = "test-key",
            ServiceName = "sdk-tests",
            Disabled = false,
        });
        Logs.LoopPaused = true;
        Logs.Clear();

        using (Dataflow.Trace("orders.Handler"))
        {
            SinkLogger().Information("hidden");
        }

        Assert.Empty(Pending());
        lock (Pipeline.BufLock)
            Assert.Single(Pipeline.Buffer);
    }
}

/// <summary>
/// Minimal HTTP stub on a random [::1] port: a raw TcpListener speaking just
/// enough HTTP/1.1 to capture POST /api/v1/logs requests (no HttpListener,
/// no URL ACLs). Every request is answered Connection: close, so one
/// connection == one request.
/// </summary>
internal sealed class LogStub : IDisposable
{
    internal sealed record Request(string Method, string Path, string ApiKey, string Body);

    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _cts = new();
    private readonly List<Request> _requests = new();
    private readonly object _lock = new();

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
        if (isLogPost)
        {
            lock (_lock) _requests.Add(new Request(parts[0], parts[1], apiKey ?? "", body));
        }

        var response = Encoding.ASCII.GetBytes("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
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
