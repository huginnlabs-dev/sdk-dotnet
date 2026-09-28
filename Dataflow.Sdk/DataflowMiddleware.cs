using System.Text;
using Microsoft.AspNetCore.Http;

namespace Dev.HuginnLabs.Dataflow;

/// <summary>
/// ASP.NET Core middleware: wraps every request into an HTTP_SERVER span
/// with agent metadata, redacted request headers and the response status.
///
/// <code>app.UseMiddleware&lt;DataflowMiddleware&gt;();</code>
///
/// Handlers join the trace via <c>using var t = Dataflow.Trace("...")</c> —
/// the ambient span flows through AsyncLocal across awaits.
/// </summary>
public sealed class DataflowMiddleware
{
    private static readonly string[] Redacted =
    {
        "Authorization", "Proxy-Authorization", "Cookie", "Set-Cookie", "X-Api-Key",
    };

    private readonly RequestDelegate _next;

    public DataflowMiddleware(RequestDelegate next) => _next = next;

    public async Task InvokeAsync(HttpContext context)
    {
        Dataflow.Configure();
        if (!Dataflow.Enabled())
        {
            await _next(context);
            return;
        }

        var route = context.Request.Path.Value ?? "/";
        var span = Dataflow.StartServerSpan(
            $"{context.Request.Method} {route}",
            context.Request.Headers["X-Dataflow-Trace-Id"].FirstOrDefault());
        span.Attr("http.method", context.Request.Method);
        span.Attr("http.path", route);
        if (context.Request.QueryString.HasValue)
            span.Attr("http.query", context.Request.QueryString.Value!);
        span.Attr("http.remote_addr", context.Connection.RemoteIpAddress?.ToString() ?? "");
        foreach (var header in context.Request.Headers)
        {
            var lower = header.Key.ToLowerInvariant();
            var value = Redacted.Contains(header.Key, StringComparer.OrdinalIgnoreCase)
                ? "[REDACTED]"
                : string.Join(", ", header.Value.ToArray());
            span.Attr($"http.header.{lower}", value);
        }

        Span.SetAmbient(span);
        try
        {
            await _next(context);
            var status = context.Response.StatusCode;
            span.Status(status);
            span.Attr("http.status_code", status.ToString());
            if (status >= 500) span.RecordError($"http {status}");
        }
        catch (Exception e)
        {
            span.RecordError(e);
            span.Status(500);
            throw;
        }
        finally
        {
            span.End();
            Span.SetAmbient(null);
        }
    }
}

/// <summary>
/// Outgoing HTTP calls recorded as HTTP_CLIENT spans carrying the
/// X-Dataflow-Trace-Id header, so a downstream instrumented service joins
/// the same trace.
/// </summary>
public static class TracedHttp
{
    private static readonly HttpClient Client = new() { Timeout = TimeSpan.FromSeconds(30) };

    public static async Task<HttpResponseMessage> GetAsync(string url) =>
        await SendAsync(HttpMethod.Get, url, content: null);

    public static async Task<HttpResponseMessage> PostJsonAsync(string url, string json) =>
        await SendAsync(HttpMethod.Post, url, new StringContent(json, Encoding.UTF8, "application/json"));

    public static async Task<HttpResponseMessage> SendAsync(HttpMethod method, string url, HttpContent? content)
    {
        using var scope = Dataflow.Trace($"{method.Method} {new Uri(url).Authority}{new Uri(url).AbsolutePath}", "HTTP_CLIENT");
        var span = scope.Span;
        var uri = new Uri(url);
        span.Callee(uri.Authority);
        span.Attr("http.method", method.Method);
        span.Attr("http.url", url);

        using var request = new HttpRequestMessage(method, url);
        request.Headers.Add("X-Dataflow-Trace-Id", span.TraceIdValue());
        if (content is not null) request.Content = content;
        try
        {
            var response = await Client.SendAsync(request);
            span.Status((int)response.StatusCode);
            span.Attr("http.status_code", ((int)response.StatusCode).ToString());
            return response;
        }
        catch (Exception e)
        {
            span.RecordError(e);
            span.Status(503);
            throw;
        }
    }
}
