using System;
using System.Globalization;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Dev.HuginnLabs.Dataflow;

/// <summary>
/// Outgoing HTTP calls recorded as HTTP_CLIENT spans carrying the
/// X-Dataflow-Trace-Id header, so a downstream instrumented service joins
/// the same trace.
///
/// <code>var api = new HttpClient(new DataflowHttpHandler());</code>
///
/// Best-effort by construction: when tracing is disabled the handler is a
/// pure pass-through, and span bookkeeping failures never escape
/// <c>SendAsync</c>.
/// </summary>
public sealed class DataflowHttpHandler : DelegatingHandler
{
    internal const string TraceHeader = "X-Dataflow-Trace-Id";

    public DataflowHttpHandler() : this(new HttpClientHandler()) { }

    public DataflowHttpHandler(HttpMessageHandler innerHandler) : base(innerHandler) { }

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var span = StartSpan(request);
        try
        {
            var response = await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
            Finish(span, (int)response.StatusCode, error: null);
            return response;
        }
        catch (Exception e)
        {
            Finish(span, null, e);
            throw;
        }
    }

    private static Span? StartSpan(HttpRequestMessage request)
    {
        if (!Dataflow.Enabled()) return null;
        try
        {
            var uri = request.RequestUri;
            var span = Dataflow.StartSpan(
                uri is null
                    ? request.Method.Method
                    : $"{request.Method.Method} {uri.Authority}{uri.AbsolutePath}",
                "HTTP_CLIENT");
            if (uri is not null) span.Callee(uri.Authority);
            span.Attr("http.method", request.Method.Method);
            if (uri is not null) span.Attr("http.url", uri.ToString());
            // Adopting downstream services read this; never clobber an
            // explicit trace id the caller already attached.
            if (!request.Headers.Contains(TraceHeader))
                request.Headers.TryAddWithoutValidation(TraceHeader, span.TraceIdValue());
            return span;
        }
        catch
        {
            return null; // best-effort: tracing must never break the call
        }
    }

    private static void Finish(Span? span, int? statusCode, Exception? error)
    {
        if (span is null) return;
        try
        {
            if (error is not null)
            {
                span.RecordError(error);
                span.Status(503);
            }
            else
            {
                span.Status(statusCode!.Value);
                span.Attr("http.status_code",
                    statusCode!.Value.ToString(CultureInfo.InvariantCulture));
                if (statusCode >= 500) span.RecordError($"http {statusCode}");
            }
            span.End();
        }
        catch
        {
            // best-effort
        }
    }
}
