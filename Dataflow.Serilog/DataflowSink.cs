using System.Collections.Generic;
using System.Globalization;
using Serilog.Core;
using Serilog.Events;

namespace Dev.HuginnLabs.Dataflow;

/// <summary>
/// Serilog sink that forwards log events into the Dataflow log pipeline.
/// Levels map onto the wire vocabulary (Verbose/Debug→debug, Information→info,
/// Warning→warn, Error/Fatal→error), <c>RenderMessage()</c> is the message and
/// event properties become stringified fields — first 50 keys, values clipped
/// at 512 chars, mirroring the server's limits. Events enter the pipeline
/// through <c>Logs.Ship</c> directly (no level re-normalization), so they ride
/// the same batched shipping as every other bridge — 1024-line drop-oldest
/// buffer, ≤1000-line POST batches, 5s timeout with a single retry — and are
/// stamped with the ambient span's trace/span ids like any other shipped line.
///
/// An exception on the event is carried on the message (the wire record has no
/// exception field); <c>Logs.Ship</c> clips the result at 8192 chars.
///
/// Strictly best-effort, like the pipeline itself: when the SDK is disabled
/// (or no HTTP base resolves) <see cref="Emit"/> is a no-op, and it never
/// throws into the logging pipeline.
///
/// The namespace is deliberately flat: nesting this package under
/// <c>...Dataflow.Serilog</c> would shadow the Serilog root namespace inside
/// its own source files.
/// </summary>
public sealed class DataflowSink : ILogEventSink
{
    /// <summary>Emit one log event into the Dataflow pipeline.</summary>
    public void Emit(LogEvent logEvent)
    {
        try
        {
            // Logs.Ship re-derives the base anyway; checking up front keeps
            // a disabled SDK free of message-render and field-stringify work.
            if (Logs.Base() is null) return;
            Logs.Ship(WireLevel(logEvent.Level), Message(logEvent), Fields(logEvent.Properties));
        }
        catch
        {
            // a logging sink must never break the app's own output
        }
    }

    /// <summary>Folds a Serilog level onto the four wire levels.</summary>
    internal static string WireLevel(LogEventLevel level) => level switch
    {
        LogEventLevel.Verbose or LogEventLevel.Debug => "debug",
        LogEventLevel.Information => "info",
        LogEventLevel.Warning => "warn",
        _ => "error", // Error, Fatal
    };

    /// <summary>The rendered message; an attached exception rides along.</summary>
    private static string Message(LogEvent logEvent)
    {
        var message = logEvent.RenderMessage();
        return logEvent.Exception is not { } ex ? message
            : message.Length > 0 ? $"{message} {ex}" : ex.ToString();
    }

    /// <summary>
    /// Stringifies event properties into wire fields: first 50 keys, values
    /// clipped at 512 chars (the pipeline's <c>Logs</c> caps). Null keys are
    /// skipped like everywhere else in the pipeline.
    /// </summary>
    private static IReadOnlyDictionary<string, string>? Fields(
        IReadOnlyDictionary<string, LogEventPropertyValue> properties)
    {
        if (properties.Count == 0) return null;
        var fields = new Dictionary<string, string>(properties.Count);
        foreach (var (name, value) in properties)
        {
            if (string.IsNullOrEmpty(name) || fields.Count >= Logs.MaxFields) break;
            fields[name] = Logs.Clip(Stringify(value), Logs.MaxFieldValue);
        }
        return fields;
    }

    /// <summary>Scalars render bare (invariant culture); structures render JSON-ish.</summary>
    private static string Stringify(LogEventPropertyValue value) => value switch
    {
        ScalarValue { Value: null } => "",
        ScalarValue { Value: string s } => s,
        ScalarValue { Value: IFormattable f } => f.ToString(null, CultureInfo.InvariantCulture),
        ScalarValue { Value: { } v } => v.ToString() ?? "",
        _ => value.ToString() ?? "",
    };
}
