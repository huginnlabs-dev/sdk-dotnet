using System;
using System.Threading;
using System.Threading.Tasks;

namespace Dev.HuginnLabs.Dataflow;

public static partial class Dataflow
{
    // Wire caps: error_message carries the exception summary (first line of
    // ToString — type + message), error.stack the raw stack trace clipped
    // from the top (the throwing frames come first).
    private const int MaxErrorMessage = 500;
    private const int MaxStackChars = 8192;

    /// <summary>
    /// Runs <paramref name="action"/> and records an uncaught exception on
    /// the ambient span — or a synthetic "exception" span when none is
    /// open — before rethrowing with the original stack preserved: status
    /// 500, the exception summary as <c>error_message</c> and the raw stack
    /// trace in the <c>error.stack</c> attribute.
    ///
    /// <code>Dataflow.Capture(() => Charge(order));</code>
    ///
    /// Best-effort by construction: when tracing is disabled this is a
    /// plain call, and recording failures never mask the original
    /// exception.
    /// </summary>
    public static void Capture(Action action)
    {
        if (!Enabled()) { action(); return; }
        try
        {
            action();
        }
        catch (Exception e)
        {
            RecordCrash(e, "exception");
            throw; // bare rethrow: the caller still sees the original stack
        }
    }

    /// <summary>Value-returning variant of <see cref="Capture(Action)"/>.</summary>
    public static T Capture<T>(Func<T> func)
    {
        if (!Enabled()) return func();
        try
        {
            return func();
        }
        catch (Exception e)
        {
            RecordCrash(e, "exception");
            throw;
        }
    }

    /// <summary>
    /// Records unhandled exceptions (<c>AppDomain.UnhandledException</c>)
    /// and unobserved task exceptions (<c>TaskScheduler.UnobservedTaskException</c>)
    /// as synthetic "uncaught exception" spans, parented to the ambient
    /// trace when one is open. Idempotent: repeated calls never stack a
    /// second pair of handlers. The unhandled handler runs synchronously
    /// during process death — recording stays fast, and there is nothing
    /// to rethrow into.
    /// </summary>
    public static void CaptureUncaught()
    {
        if (Interlocked.Exchange(ref _uncaughtInstalled, 1) == 1) return;
        AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
        TaskScheduler.UnobservedTaskException += OnUnobservedTaskException;
    }

    /// <summary>Unsubscribes the handlers installed by <see cref="CaptureUncaught"/>. Idempotent.</summary>
    public static void IgnoreUncaught()
    {
        if (Interlocked.Exchange(ref _uncaughtInstalled, 0) == 0) return;
        AppDomain.CurrentDomain.UnhandledException -= OnUnhandledException;
        TaskScheduler.UnobservedTaskException -= OnUnobservedTaskException;
    }

    // 0 = no handlers installed, 1 = exactly one pair. Doubles as the
    // idempotency latch and the test seam for subscribe/unsubscribe counts.
    private static int _uncaughtInstalled;

    /// <summary>Test seam: how many uncaught-handler pairs are installed (0 or 1).</summary>
    internal static int UncaughtHandlerCount => Volatile.Read(ref _uncaughtInstalled);

    // Internal so the tests can drive the recording path directly — there
    // is no safe way to fire a real UnhandledException in a test host.
    internal static void OnUnhandledException(object? sender, UnhandledExceptionEventArgs args)
    {
        if (args.ExceptionObject is Exception e) RecordCrash(e, "uncaught exception");
        // The process is about to die: recording cannot rethrow.
    }

    internal static void OnUnobservedTaskException(object? sender, UnobservedTaskExceptionEventArgs args) =>
        RecordCrash(args.Exception, "uncaught exception");
    // Never SetObserved(): recording must not change the app's fate.

    /// <summary>
    /// The single recording path for every crash flavor. Writes onto the
    /// ambient span when one is open (its owner ends it later); otherwise
    /// creates and immediately ends a synthetic span. Guarded so a
    /// bookkeeping failure can never mask the original exception.
    /// </summary>
    private static void RecordCrash(Exception e, string syntheticName)
    {
        if (!Enabled()) return;
        try
        {
            var ambient = Span.CurrentSpan();
            var span = ambient ?? StartSpan(syntheticName);
            span.RecordError(FormatExceptionMessage(e));
            span.Status(500);
            var stack = FormatStack(e.StackTrace);
            if (stack.Length > 0) span.Attr("error.stack", stack);
            if (ambient is null) span.End(); // synthetic spans end on the spot
        }
        catch
        {
            // best-effort: never mask the original exception
        }
    }

    /// <summary>First line of <c>Exception.ToString()</c>, capped — the wire <c>error_message</c>.</summary>
    internal static string FormatExceptionMessage(Exception e)
    {
        var line = e.ToString();
        var nl = line.IndexOfAny(LineBreaks);
        if (nl >= 0) line = line[..nl];
        return line.Length <= MaxErrorMessage ? line : line[..MaxErrorMessage];
    }

    /// <summary>Raw stack trace capped at 8192 characters from the top (test seam).</summary>
    internal static string FormatStack(string? stack)
    {
        if (string.IsNullOrEmpty(stack)) return "";
        return stack.Length <= MaxStackChars ? stack : stack[..MaxStackChars];
    }

    private static readonly char[] LineBreaks = { '\r', '\n' };
}
