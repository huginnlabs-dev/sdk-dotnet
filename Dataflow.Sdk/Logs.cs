using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Dev.HuginnLabs.Dataflow;

// Application log shipping with trace correlation. The Log/Info/Warn/Error/
// Debug helpers, the MEL bridge (DataflowLoggerProvider) and the
// System.Diagnostics.Trace bridge (DataflowTraceListener) buffer log lines
// in-process and POST them to the server's REST log endpoint in batches;
// every line is stamped with the ambient span's trace/span ids (when one is
// open on the calling async flow) so logs line up with traces in the
// dashboard's Logs tab.
//
// Logging is strictly best-effort: it never blocks, never throws, and drops
// the oldest lines on overflow. It uses the manifest's HTTP base resolution
// — a bare host:port DATAFLOW_ENDPOINT with no DATAFLOW_HTTP_URL override
// has no derivable HTTP base and logging stays silently off.

public static partial class Dataflow
{
    /// <summary>
    /// Ships an application log line at debug level. <paramref name="fields"/>
    /// values are stringified; the record is stamped with the ambient span's
    /// trace/span ids. A no-op when logging is disabled. Never blocks.
    /// </summary>
    public static void Debug(string message, Dictionary<string, object>? fields = null) =>
        Logs.Ship(Logs.NormalizeLevel("debug"), message, Logs.StringifyFields(fields));

    /// <summary>Info-level flavour of <see cref="Debug"/>.</summary>
    public static void Info(string message, Dictionary<string, object>? fields = null) =>
        Logs.Ship(Logs.NormalizeLevel("info"), message, Logs.StringifyFields(fields));

    /// <summary>Warn-level flavour of <see cref="Debug"/>.</summary>
    public static void Warn(string message, Dictionary<string, object>? fields = null) =>
        Logs.Ship(Logs.NormalizeLevel("warn"), message, Logs.StringifyFields(fields));

    /// <summary>Error-level flavour of <see cref="Debug"/>.</summary>
    public static void Error(string message, Dictionary<string, object>? fields = null) =>
        Logs.Ship(Logs.NormalizeLevel("error"), message, Logs.StringifyFields(fields));

    /// <summary>
    /// Ships an application log line at the given level ("debug", "info",
    /// "warn", "error" — case-insensitive; unknown levels log as info).
    /// </summary>
    public static void Log(string level, string message, Dictionary<string, object>? fields = null) =>
        Logs.Ship(Logs.NormalizeLevel(level), message, Logs.StringifyFields(fields));

    /// <summary>
    /// Synchronously ships any buffered log lines (one HTTP round trip per
    /// ≤1000-line batch, a single retry each). A no-op when logging is
    /// disabled; explicit shutdown paths can call it before exit.
    /// </summary>
    public static void FlushLogs() => Logs.Flush();

    /// <summary>
    /// Bridges <c>System.Diagnostics.Trace</c> into the log pipeline for apps
    /// that do not route through Microsoft.Extensions.Logging: after this
    /// call, <c>Trace.TraceError</c> / <c>TraceWarning</c> /
    /// <c>TraceInformation</c> / <c>Write</c> / <c>WriteLine</c> become
    /// shipped log lines (error / warn / info). Idempotent, like
    /// <see cref="CaptureUncaught"/>; <see cref="UninstallLogHandler"/>
    /// removes it.
    ///
    /// For Microsoft.Extensions.Logging apps prefer the provider instead:
    /// <code>builder.Logging.AddProvider(new DataflowLoggerProvider());</code>
    /// </summary>
    public static void InstallLogHandler()
    {
        if (Interlocked.Exchange(ref _logHandlerInstalled, 1) == 1) return;
        var listener = _traceListener ??= new DataflowTraceListener();
        System.Diagnostics.Trace.Listeners.Add(listener);
    }

    /// <summary>Unsubscribes the listener installed by <see cref="InstallLogHandler"/>. Idempotent.</summary>
    public static void UninstallLogHandler()
    {
        if (Interlocked.Exchange(ref _logHandlerInstalled, 0) == 0) return;
        var listener = _traceListener;
        if (listener is not null) System.Diagnostics.Trace.Listeners.Remove(listener);
    }

    // 0 = no listener installed, 1 = exactly one. Doubles as the
    // idempotency latch and the test seam for install/uninstall counts.
    private static int _logHandlerInstalled;
    private static DataflowTraceListener? _traceListener;

    /// <summary>Test seam: how many trace listeners are installed (0 or 1).</summary>
    internal static int LogHandlerCount => Volatile.Read(ref _logHandlerInstalled);
}

/// <summary>One shipped application log line (wire shape of POST /api/v1/logs entries).</summary>
internal sealed record LogLine(
    [property: JsonPropertyName("timestamp")] long Timestamp,
    [property: JsonPropertyName("level")] string Level,
    [property: JsonPropertyName("message")] string Message,
    [property: JsonPropertyName("trace_id")] string TraceId,
    [property: JsonPropertyName("span_id")] string SpanId,
    [property: JsonPropertyName("service_name")] string ServiceName,
    [property: JsonPropertyName("fields")] IReadOnlyDictionary<string, string> Fields);

internal sealed record LogBatch([property: JsonPropertyName("logs")] IReadOnlyList<LogLine> Logs);

/// <summary>
/// Buffers log lines and flushes them to the REST endpoint from a daemon
/// task, independently of the gRPC trace stream. The queue is bounded:
/// on overflow the oldest line is dropped and counted (same policy as the
/// event replay buffer). A failed batch is retried once, then dropped.
/// </summary>
internal static class Logs
{
    // Wire caps and flush thresholds — mirrors the Go SDK.
    internal const int BufferSize = 1024;
    internal const int FlushLines = 50;
    internal const int FlushIntervalMs = 500;
    internal const int MaxBatch = 1000;
    internal const int MaxMessage = 8192;
    internal const int MaxFields = 50;
    internal const int MaxFieldValue = 512;

    private static readonly object Lock = new();
    private static readonly Queue<LogLine> PendingQ = new();
    private static readonly SemaphoreSlim Wake = new(0, 1);
    // One client per process: 5s cap, same as the manifest report.
    private static readonly HttpClient Http = new() { Timeout = TimeSpan.FromSeconds(5) };
    private static long _dropped;
    private static int _loopStarted;

    // Test seam: when true the daemon loop pauses; tests trigger flushes
    // explicitly via Dataflow.FlushLogs so batching is deterministic.
    internal static volatile bool LoopPaused;

    /// <summary>Starts the flusher daemon once per process (from Pipeline.Start).</summary>
    internal static void EnsureStarted()
    {
        if (Interlocked.Exchange(ref _loopStarted, 1) == 1) return;
        _ = Task.Run(Loop);
    }

    /// <summary>
    /// The HTTP API base for log shipping: only when tracing is enabled and
    /// the manifest's resolution yields one. A bare host:port gRPC endpoint
    /// with no DATAFLOW_HTTP_URL override resolves nothing — logging stays
    /// silently off, like the manifest report.
    /// </summary>
    internal static string? Base() =>
        Dataflow.Enabled() ? Manifest.HttpBaseURL(Dataflow.Cfg.Endpoint) : null;

    private static async Task Loop()
    {
        while (true)
        {
            try
            {
                // The 50-line threshold wakes the loop early; otherwise it
                // flushes whatever is buffered every 500ms.
                await Wake.WaitAsync(TimeSpan.FromMilliseconds(FlushIntervalMs));
                if (LoopPaused) continue;
                Flush();
            }
            catch
            {
                // the flusher must outlive any transient failure
            }
        }
    }

    /// <summary>Buffers one record; a no-op when logging is off. Never blocks, never throws.</summary>
    internal static void Ship(string level, string? message, IReadOnlyDictionary<string, string>? fields)
    {
        if (Base() is null) return;
        try
        {
            var ambient = Span.CurrentSpan();
            var rec = new LogLine(
                Timestamp: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Level: level,
                Message: Clip(message ?? "", MaxMessage),
                TraceId: ambient?.TraceId ?? "",
                SpanId: ambient?.SpanId ?? "",
                ServiceName: Dataflow.ServiceName(),
                Fields: fields is { Count: > 0 } ? new Dictionary<string, string>(fields) : new Dictionary<string, string>());
            Enqueue(rec);
        }
        catch
        {
            // logging must never take the app down
        }
    }

    private static void Enqueue(LogLine rec)
    {
        var full = false;
        lock (Lock)
        {
            if (PendingQ.Count >= BufferSize)
            {
                PendingQ.Dequeue();
                Interlocked.Increment(ref _dropped);
            }
            PendingQ.Enqueue(rec);
            full = PendingQ.Count >= FlushLines;
        }
        if (full)
        {
            try { Wake.Release(); }
            catch (SemaphoreFullException) { } // a wake is already pending
        }
    }

    /// <summary>Drains the buffer and POSTs it in batches of ≤ <see cref="MaxBatch"/>.</summary>
    internal static void Flush()
    {
        try
        {
            while (true)
            {
                LogLine[] batch;
                lock (Lock)
                {
                    if (PendingQ.Count == 0) return;
                    var n = Math.Min(PendingQ.Count, MaxBatch);
                    batch = new LogLine[n];
                    for (var i = 0; i < n; i++) batch[i] = PendingQ.Dequeue();
                }
                var baseUrl = Base();
                if (baseUrl is null || !Post(baseUrl, batch))
                {
                    Interlocked.Add(ref _dropped, batch.Length);
                }
            }
        }
        catch
        {
            // best-effort: flush failures never surface
        }
    }

    /// <summary>Sends one batch; one retry on failure, then the batch is dropped.</summary>
    private static bool Post(string baseUrl, LogLine[] batch)
    {
        var body = JsonSerializer.Serialize(new LogBatch(batch));
        for (var attempt = 0; attempt < 2; attempt++)
        {
            try
            {
                using var request = new HttpRequestMessage(
                    HttpMethod.Post, baseUrl + "/api/v1/logs")
                {
                    Content = new StringContent(body, Encoding.UTF8, "application/json"),
                };
                request.Headers.Add("X-Api-Key", Dataflow.Cfg.ApiKey);
                using var response = Http.Send(request);
                if (response.IsSuccessStatusCode) return true;
            }
            catch
            {
                // fall through to the retry, then drop
            }
        }
        return false;
    }

    /// <summary>Maps input to the wire vocabulary debug|info|warn|error. Unknown levels degrade to info.</summary>
    internal static string NormalizeLevel(string? level) => level?.Trim().ToLowerInvariant() switch
    {
        "debug" => "debug",
        "warn" or "warning" => "warn",
        "error" => "error",
        _ => "info",
    };

    /// <summary>Folds a Microsoft.Extensions.Logging level onto the four wire levels.</summary>
    internal static string WireLevel(LogLevel level) => level switch
    {
        LogLevel.Trace or LogLevel.Debug => "debug",
        LogLevel.Information => "info",
        LogLevel.Warning => "warn",
        _ => "error",
    };

    /// <summary>Stringifies caller field values, clipping to the server's 50×512 limits.</summary>
    internal static Dictionary<string, string> StringifyFields(Dictionary<string, object>? fields)
    {
        var outp = new Dictionary<string, string>();
        if (fields is null) return outp;
        foreach (var (key, value) in fields)
        {
            if (string.IsNullOrEmpty(key) || outp.Count >= MaxFields) continue;
            outp[key] = Clip(Convert.ToString(value, CultureInfo.InvariantCulture) ?? "", MaxFieldValue);
        }
        return outp;
    }

    /// <summary>Extracts structured-log state entries as fields (scopes are skipped in v1).</summary>
    internal static Dictionary<string, string> StateFields<TState>(TState state)
    {
        var outp = new Dictionary<string, string>();
        if (state is IEnumerable<KeyValuePair<string, object?>> entries)
        {
            foreach (var entry in entries)
            {
                // "{OriginalFormat}" is the message template, not a field.
                if (string.IsNullOrEmpty(entry.Key) || entry.Key == "{OriginalFormat}") continue;
                if (outp.Count >= MaxFields) break;
                outp[entry.Key] = Clip(Convert.ToString(entry.Value, CultureInfo.InvariantCulture) ?? "", MaxFieldValue);
            }
        }
        return outp;
    }

    internal static string Clip(string s, int max) => s.Length <= max ? s : s[..max];

    // ------------------------------------------------------------------
    // Test seams (mirroring Pipeline.Buffer): assert on buffered records
    // directly; the paused loop never interferes.

    internal static IReadOnlyList<LogLine> PendingSnapshot()
    {
        lock (Lock) return new List<LogLine>(PendingQ);
    }

    internal static void Clear()
    {
        lock (Lock) PendingQ.Clear();
        Interlocked.Exchange(ref _dropped, 0);
    }

    internal static long Dropped => Interlocked.Read(ref _dropped);
}

/// <summary>
/// ILoggerProvider for Microsoft.Extensions.Logging apps — zero new package
/// references (the abstractions ship in the ASP.NET Core shared framework):
/// <code>builder.Logging.AddProvider(new DataflowLoggerProvider());</code>
/// </summary>
public sealed class DataflowLoggerProvider : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName) => new DataflowLogger();

    public void Dispose() { } // nothing to release
}

/// <summary>
/// Forwards MEL records into the log pipeline with trace correlation.
/// Levels map onto the wire vocabulary (Trace/Debug→debug, Information→info,
/// Warning→warn, Error/Critical→error; None is never forwarded) and
/// structured-log state entries become stringified fields; scopes are
/// skipped in v1. When the SDK is disabled the logger reports disabled and
/// forwards nothing.
/// </summary>
public sealed class DataflowLogger : ILogger
{
    // Scopes are skipped in v1; a shared no-op scope keeps the signature
    // nullability-agnostic across abstraction versions.
    public IDisposable BeginScope<TState>(TState state) where TState : notnull => SharedScope.Instance;

    public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None && Dataflow.Enabled();

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel)) return;
        var message = formatter?.Invoke(state, exception) ?? state?.ToString() ?? "";
        Logs.Ship(Logs.WireLevel(logLevel), message, Logs.StateFields(state));
    }

    private sealed class SharedScope : IDisposable
    {
        internal static readonly SharedScope Instance = new();
        public void Dispose() { }
    }
}

/// <summary>
/// TraceListener bridge installed by <see cref="Dataflow.InstallLogHandler"/>:
/// Trace severities map onto the wire vocabulary (Error/Critical→error,
/// Warning→warn, everything else→info) and plain Write/WriteLine ships as
/// info. Best-effort by construction — listener failures are swallowed.
/// </summary>
public sealed class DataflowTraceListener : TraceListener
{
    public override void Write(string? message) => Ship("info", message);
    public override void WriteLine(string? message) => Ship("info", message);

    public override void Fail(string? message) => Ship("error", message);

    public override void Fail(string? message, string? detailMessage) =>
        Ship("error", detailMessage is { Length: > 0 } ? $"{message} {detailMessage}" : message);

    // Trace.TraceError/Warn/Info route through TraceSource into these
    // cache-carrying virtuals (the only public TraceEvent overloads on
    // .NET Core). Severities map onto the wire vocabulary.
    public override void TraceEvent(TraceEventCache? eventCache, string? source,
        TraceEventType eventType, int id, string? message) =>
        Ship(WireLevel(eventType), message);

    public override void TraceEvent(TraceEventCache? eventCache, string? source,
        TraceEventType eventType, int id, string? format, params object?[]? args) =>
        Ship(WireLevel(eventType), FormatSafely(format, args));

    private static string WireLevel(TraceEventType eventType) => eventType switch
    {
        TraceEventType.Critical or TraceEventType.Error => "error",
        TraceEventType.Warning => "warn",
        _ => "info",
    };

    private static string? FormatSafely(string? format, object?[]? args)
    {
        if (args is not { Length: > 0 }) return format;
        try { return string.Format(format, args); }
        catch (FormatException) { return format; }
    }

    private static void Ship(string level, string? message)
    {
        if (string.IsNullOrEmpty(message)) return;
        try
        {
            Logs.Ship(level, message, null);
        }
        catch
        {
            // a logging bridge must never break the app's own output
        }
    }
}
