using System.Collections.Generic;
using System.Globalization;
using NLog;
using NLog.Targets;

namespace Dev.HuginnLabs.Dataflow;

/// <summary>
/// NLog target that forwards log events into the Dataflow log pipeline.
/// Levels map onto the wire vocabulary (Trace/Debug→debug, Info→info,
/// Warn→warn, Error/Fatal→error), the target's layout — defaulted to
/// <c>${message}</c> here, since the wire record carries its own timestamp
/// and level — is the message, and message-template properties become
/// stringified fields — first 50 keys, values clipped at 512 chars,
/// mirroring the server's limits. Events enter the pipeline through
/// <c>Logs.Ship</c> directly (no level re-normalization), so they ride the
/// same batched shipping as every other bridge — 1024-line drop-oldest
/// buffer, ≤1000-line POST batches, 5s timeout with a single retry — and
/// are stamped with the ambient span's trace/span ids like any other
/// shipped line.
///
/// An exception on the event is carried on the message (the wire record has
/// no exception field); <c>Logs.Ship</c> clips the result at 8192 chars.
///
/// Strictly best-effort, like the pipeline itself: when the SDK is disabled
/// (or no HTTP base resolves) <see cref="Write"/> is a no-op, and it never
/// throws into the NLog pipeline.
///
/// The namespace is deliberately flat: nesting this package under
/// <c>...Dataflow.NLog</c> would put a <c>Dataflow</c> namespace fragment
/// between <c>Dev.HuginnLabs</c> and the SDK's own types.
/// </summary>
[Target("Dataflow")]
public sealed class DataflowTarget : TargetWithLayout
{
    public DataflowTarget()
    {
        Name = "Dataflow";
        // TargetWithLayout's longdate|level|logger|message default renders a
        // full line; the wire record already has timestamp and level.
        Layout = "${message}";
    }

    /// <summary>Forwards one log event into the Dataflow pipeline.</summary>
    protected override void Write(LogEventInfo logEvent)
    {
        try
        {
            // Logs.Ship re-derives the base anyway; checking up front keeps
            // a disabled SDK free of layout-render and field-stringify work.
            if (Logs.Base() is null) return;
            Logs.Ship(WireLevel(logEvent.Level), Message(logEvent), Fields(logEvent));
        }
        catch
        {
            // a logging target must never break the app's own output
        }
    }

    /// <summary>Folds an NLog level onto the four wire levels.</summary>
    // NLog's LogLevel is a class of static instances, not an enum — a plain
    // equality chain, not a switch of patterns. Off never reaches Write.
    internal static string WireLevel(LogLevel level)
    {
        if (level == LogLevel.Trace || level == LogLevel.Debug) return "debug";
        if (level == LogLevel.Info) return "info";
        if (level == LogLevel.Warn) return "warn";
        return "error"; // Error, Fatal
    }

    /// <summary>The rendered layout; an attached exception rides along.</summary>
    private string Message(LogEventInfo logEvent)
    {
        var message = RenderLogEvent(Layout, logEvent);
        return logEvent.Exception is not { } ex ? message
            : message.Length > 0 ? $"{message} {ex}" : ex.ToString();
    }

    /// <summary>
    /// Stringifies message-template properties into wire fields: first 50
    /// keys, values clipped at 512 chars (the pipeline's <c>Logs</c> caps).
    /// Non-string and empty keys are skipped, like everywhere else in the
    /// pipeline; "{OriginalFormat}" is the message template, not a field.
    /// </summary>
    private static IReadOnlyDictionary<string, string>? Fields(LogEventInfo logEvent)
    {
        var properties = logEvent.Properties;
        if (properties.Count == 0) return null;
        var fields = new Dictionary<string, string>(properties.Count);
        foreach (var (key, value) in properties)
        {
            if (key is not string name || string.IsNullOrEmpty(name) || name == "{OriginalFormat}") continue;
            if (fields.Count >= Logs.MaxFields) break;
            fields[name] = Logs.Clip(Stringify(value), Logs.MaxFieldValue);
        }
        return fields;
    }

    /// <summary>Scalars render bare (invariant culture).</summary>
    private static string Stringify(object? value) => value switch
    {
        null => "",
        string s => s,
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };
}
