using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Dev.HuginnLabs.Dataflow;
using HuginnLabs.Proto;
using Xunit;

namespace DataflowSdk.Tests;

// Both classes flip the global enable/disable state and assert on the
// shared replay buffer — xunit must never run them in parallel.
// SqlText/ScanTool tests are stateless w.r.t. the SDK and stay outside.
[CollectionDefinition("Dataflow Globals")]
public sealed class DataflowGlobalsCollection { }

// Crash capture end to end over the replay buffer. One test class so the
// enable/disable flips stay sequential.
[Collection("Dataflow Globals")]
public class CrashTests
{
    // Nothing listens on port 9; the sender retries harmlessly in the
    // background while the tests assert on the replay buffer.
    private static void EnableTracing()
    {
        Dataflow.Configure(new Dataflow.Settings
        {
            Endpoint = "http://127.0.0.1:9",
            ApiKey = "test-key",
            ServiceName = "sdk-tests",
            BufferSize = 10_000,
            Disabled = false,
        });
        lock (Pipeline.BufLock) Pipeline.Buffer.Clear();
    }

    private static void DisableTracing()
    {
        Dataflow.Configure(new Dataflow.Settings { Disabled = true });
        lock (Pipeline.BufLock) Pipeline.Buffer.Clear();
    }

    private static void ClearSpans() { lock (Pipeline.BufLock) Pipeline.Buffer.Clear(); }

    private static List<TraceEvent> Spans()
    {
        lock (Pipeline.BufLock) return new List<TraceEvent>(Pipeline.Buffer);
    }

    private static TraceEvent SingleSpan() => Assert.Single(Spans());

    // ------------------------------------------------------------------
    // Dataflow.Capture

    [Fact]
    public void Capture_Disabled_IsPurePassThrough()
    {
        DisableTracing();

        Assert.Equal(42, Dataflow.Capture(() => 42));
        var ran = false;
        Dataflow.Capture(() => ran = true);
        Assert.True(ran);

        // Exceptions still propagate — with no span bookkeeping at all.
        var boom = new InvalidOperationException("boom");
        Assert.Same(boom, Assert.Throws<InvalidOperationException>(() => Dataflow.Capture(() => throw boom)));
        Assert.Empty(Spans());
    }

    [Fact]
    public void Capture_RecordsOnAmbientSpanAndRethrowsOriginal()
    {
        EnableTracing();
        var boom = new InvalidOperationException("boom");
        string traceId;
        using (var scope = Dataflow.Trace("orders.Handler"))
        {
            traceId = scope.Span.TraceIdValue();
            var caught = Assert.Throws<InvalidOperationException>(
                () => Dataflow.Capture(() => throw boom));
            Assert.Same(boom, caught); // exact instance, original stack preserved
        } // scope ends here, queuing the span

        var span = SingleSpan();
        Assert.Equal(traceId, span.TraceId);
        Assert.Equal(500, span.StatusCode);
        Assert.Equal("System.InvalidOperationException: boom", span.ErrorMessage);
        // error.stack carries the throw-site frames. The exact string is
        // the FormatStack seam's business (asserted below): an exception's
        // StackTrace string keeps growing as frames unwind, so only stable
        // content is asserted here.
        var stack = span.Metadata["error.stack"];
        Assert.False(string.IsNullOrEmpty(stack));
        Assert.Contains("Crash.cs", stack); // the Capture() throw frame
        Assert.Contains("CrashTests", stack);
        Assert.True(stack.Length <= 8192);
    }

    [Fact]
    public void Capture_Func_ReturnsValueAndRethrowsOriginalInstance()
    {
        EnableTracing();
        Assert.Equal("ok", Dataflow.Capture(() => "ok"));

        var boom = new DivideByZeroException("div");
        var caught = Assert.Throws<DivideByZeroException>(() => Dataflow.Capture<object?>(
            () => throw boom));
        Assert.Same(boom, caught);

        var span = SingleSpan();
        Assert.Equal(500, span.StatusCode);
        Assert.StartsWith("System.DivideByZeroException: div", span.ErrorMessage);
    }

    [Fact]
    public void Capture_NoAmbientSpan_EmitsSyntheticSpan()
    {
        EnableTracing();
        Assert.Null(Span.CurrentSpan());

        var boom = new InvalidOperationException("no span here");
        Assert.Same(boom, Assert.Throws<InvalidOperationException>(() => Dataflow.Capture(() => throw boom)));

        var span = SingleSpan();
        Assert.Equal("exception", span.Name);
        Assert.Equal("", span.ParentSpanId); // synthetic spans are roots
        Assert.Equal(500, span.StatusCode);
        Assert.Equal("System.InvalidOperationException: no span here", span.ErrorMessage);
        Assert.Contains("Crash.cs", span.Metadata["error.stack"]);
    }

    // ------------------------------------------------------------------
    // Formatting seams (the stack cap is exercised here, not by growing
    // real stacks)

    [Fact]
    public void FormatExceptionMessage_TakesFirstLineCappedAt500()
    {
        var multi = new InvalidOperationException("line1\nline2\r\nline3");
        Assert.Equal("System.InvalidOperationException: line1",
            Dataflow.FormatExceptionMessage(multi));

        var huge = new InvalidOperationException(new string('x', 1000));
        var clipped = Dataflow.FormatExceptionMessage(huge);
        Assert.Equal(500, clipped.Length);
        Assert.StartsWith("System.InvalidOperationException: xxx", clipped);
    }

    [Fact]
    public void FormatStack_CapsAt8192CharsFromTheTop()
    {
        Assert.Equal(new string('s', 8192), Dataflow.FormatStack(new string('s', 20_000)));
        Assert.Equal("   at Frames", Dataflow.FormatStack("   at Frames"));
        Assert.Equal("", Dataflow.FormatStack(null));
        Assert.Equal("", Dataflow.FormatStack(""));
    }

    // ------------------------------------------------------------------
    // Dataflow.CaptureUncaught / IgnoreUncaught

    [Fact]
    public void CaptureUncaught_IsIdempotent()
    {
        Dataflow.IgnoreUncaught(); // normalize whatever a previous test left
        Assert.Equal(0, Dataflow.UncaughtHandlerCount);

        Dataflow.CaptureUncaught();
        Assert.Equal(1, Dataflow.UncaughtHandlerCount);
        Dataflow.CaptureUncaught(); // second install is a no-op, never a second pair
        Assert.Equal(1, Dataflow.UncaughtHandlerCount);

        Dataflow.IgnoreUncaught();
        Assert.Equal(0, Dataflow.UncaughtHandlerCount);
        Dataflow.IgnoreUncaught(); // unsubscribing again stays a no-op
        Assert.Equal(0, Dataflow.UncaughtHandlerCount);
    }

    [Fact]
    public void UncaughtHandlers_RecordSyntheticSpans()
    {
        EnableTracing();
        Dataflow.CaptureUncaught();
        try
        {
            // A real unhandled exception always carries a stack; a plain
            // `new` one has none, so throw-and-catch to give it one.
            var fatal = new InvalidOperationException("fatal");
            try { throw fatal; } catch { /* stack captured */ }

            Dataflow.OnUnhandledException(this,
                new UnhandledExceptionEventArgs(fatal, isTerminating: true));

            var crash = SingleSpan();
            Assert.Equal("uncaught exception", crash.Name);
            Assert.Equal(500, crash.StatusCode);
            Assert.Equal("System.InvalidOperationException: fatal", crash.ErrorMessage);
            Assert.Contains("CrashTests", crash.Metadata["error.stack"]);

            ClearSpans();
            Dataflow.OnUnobservedTaskException(this, new UnobservedTaskExceptionEventArgs(
                new AggregateException("task died", new InvalidOperationException("inner"))));

            var unobserved = SingleSpan();
            Assert.Equal("uncaught exception", unobserved.Name);
            Assert.Equal(500, unobserved.StatusCode);
            // First line of ToString: type + message (the inner dump that
            // follows never leaks into error_message).
            Assert.Equal("System.AggregateException: task died (inner)", unobserved.ErrorMessage);
        }
        finally
        {
            Dataflow.IgnoreUncaught();
        }
    }

    [Fact]
    public void UncaughtHandlers_Disabled_RecordNothing()
    {
        DisableTracing();
        Dataflow.CaptureUncaught();
        try
        {
            Dataflow.OnUnhandledException(this,
                new UnhandledExceptionEventArgs(new InvalidOperationException("fatal"), isTerminating: true));
            Dataflow.OnUnobservedTaskException(this, new UnobservedTaskExceptionEventArgs(
                new AggregateException("task died")));
            Assert.Empty(Spans());
        }
        finally
        {
            Dataflow.IgnoreUncaught();
        }
    }
}
