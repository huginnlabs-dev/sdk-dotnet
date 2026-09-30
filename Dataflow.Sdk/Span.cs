using System;
using System.Collections.Generic;
using System.Threading;

namespace Dev.HuginnLabs.Dataflow;

/// <summary>
/// One measured unit of work. The ambient span flows through
/// <c>AsyncLocal</c>, so child spans join the trace across awaits without
/// explicit plumbing.
/// </summary>
public sealed class Span
{
    private static readonly AsyncLocal<Span?> Ambient = new();

    /// <summary>The span active on the current async flow, or null.</summary>
    public static Span? CurrentSpan() => Ambient.Value;

    internal static void SetAmbient(Span? span) => Ambient.Value = span;

    private readonly object _lock = new();
    private readonly Dictionary<string, string> _attrs = new();
    private readonly Dictionary<string, object?> _payload = new();

    internal string TraceId { get; private set; }
    internal string SpanId { get; } = NewId();
    internal string ParentSpanId { get; }
    private readonly string _name;
    private readonly string _type;
    private readonly long _startMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    private readonly long _startTicks = Environment.TickCount64;
    private string _callee = "";
    private string _caller = "";
    private string _error = "";
    private int _status;
    private bool _ended;
    private readonly bool _sampled;

    internal Span(string name, string type, Span? parent, string? incomingTraceId)
    {
        _name = name;
        _type = type;
        TraceId = incomingTraceId is { Length: > 0 } tid
            ? tid
            : parent?.TraceId ?? NewId();
        ParentSpanId = parent?.SpanId ?? "";
        // Caller attribution: the enclosing span's package; roots stay
        // empty so the flow graph never grows self-edges.
        _caller = parent?.SnapshotCallee() ?? "";
        if (type == "FUNCTION_CALL")
        {
            var dot = name.IndexOf('.');
            if (dot > 0) _callee = name[..dot];
        }
        var ratio = Dataflow.Cfg.SampleRatio;
        _sampled = ratio >= 1.0 || Random.Shared.NextDouble() < ratio;
    }

    /// <summary>Plaintext attribute (metadata entry).</summary>
    public Span Attr(string key, string value)
    {
        lock (_lock) { _attrs[key] = value; }
        return this;
    }

    /// <summary>Payload field; encrypted at End when a key is configured.</summary>
    public Span Data(string key, object? value)
    {
        lock (_lock) { _payload[key] = value; }
        return this;
    }

    /// <summary>Payload field with a pre-serialized JSON document.</summary>
    public Span DataJson(string key, string json) => Data(key, new RawJson(json));

    /// <summary>Marks this span's package (or host) for the data-flow graph.</summary>
    public Span Callee(string pkg)
    {
        lock (_lock) { _callee = pkg; }
        return this;
    }

    public Span RecordError(string message)
    {
        lock (_lock)
        {
            _error = _error.Length == 0 ? message : $"{_error}; {message}";
            if (_status == 0) _status = 500;
        }
        return this;
    }

    public Span RecordError(Exception e) =>
        RecordError($"{e.GetType().Name}: {e.Message}");

    /// <summary>HTTP status or gRPC code.</summary>
    public Span Status(int code)
    {
        lock (_lock) { _status = code; }
        return this;
    }

    public string TraceIdValue() => TraceId;

    private string SnapshotCallee() { lock (_lock) { return _callee; } }

    /// <summary>Ends the span and queues it for delivery. Idempotent.</summary>
    public void End()
    {
        if (_sampled == false) return;
        Dictionary<string, string> attrs;
        Dictionary<string, object?> payload;
        string error, callee, caller;
        int status;
        lock (_lock)
        {
            if (_ended) return;
            _ended = true;
            attrs = new(_attrs);
            payload = new(_payload);
            error = _error;
            callee = _callee;
            caller = _caller;
            status = _status;
        }

        var durationMs = Environment.TickCount64 - _startTicks;

        var meta = new Dictionary<string, string>(attrs);
        if (payload.Count > 0)
        {
            var keys = new List<string>(payload.Keys);
            keys.Sort();
            meta["data.fields"] = string.Join(",", keys);
            var pii = Pii.Classify(keys);
            if (pii.Length > 0) meta["data.pii"] = pii;
        }

        Dataflow.Enqueue(new global::HuginnLabs.Proto.TraceEvent
        {
            EventId = NewId(),
            Seq = Pipeline.NextSeq(),
            TraceId = TraceId,
            SpanId = SpanId,
            ParentSpanId = ParentSpanId,
            Type = TypeOf(_type),
            ServiceName = Dataflow.ServiceName(),
            Name = _name,
            CallerPackage = caller,
            CalleePackage = callee,
            FunctionName = _name,
            Timestamp = _startMs,
            DurationMs = durationMs,
            StatusCode = status,
            ErrorMessage = error,
            Payload = payload.Count > 0 ? Crypto.Seal(payload) : null,
            Metadata = { meta },
        });
    }

    internal void JoinTrace(string traceId)
    {
        if (!string.IsNullOrEmpty(traceId)) TraceId = traceId;
    }

    private static global::HuginnLabs.Proto.EventType TypeOf(string t) => t switch
    {
        "HTTP_SERVER" => global::HuginnLabs.Proto.EventType.HttpServer,
        "HTTP_CLIENT" => global::HuginnLabs.Proto.EventType.HttpClient,
        "GRPC" => global::HuginnLabs.Proto.EventType.Grpc,
        "DB_QUERY" => global::HuginnLabs.Proto.EventType.DbQuery,
        _ => global::HuginnLabs.Proto.EventType.FunctionCall,
    };

    internal static string NewId() => Guid.NewGuid().ToString("N");

    /// <summary>Wrapper for pre-serialized JSON payload values.</summary>
    public sealed record RawJson(string Json);
}

/// <summary>
/// AutoDispose scope making a span ambient for the calling flow — the .NET
/// analogue of the Rust/C++ RAII trace:
/// <c>using var t = Dataflow.Trace("booking.Create");</c>
/// </summary>
public sealed class TraceScope : IDisposable
{
    private readonly Span _previous;
    private bool _disposed;

    public Span Span { get; }

    internal TraceScope(string name, string type)
    {
        _previous = Span.CurrentSpan();
        Span = Dataflow.StartSpan(name, type);
        Span.SetAmbient(Span);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        Span.End();
        Span.SetAmbient(_previous);
    }
}
