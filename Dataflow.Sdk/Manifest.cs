using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace Dev.HuginnLabs.Dataflow;

/// <summary>
/// Service manifest: one best-effort HTTP POST at startup describing this
/// service (framework, runtime, dependency inventory from the loaded
/// assemblies). The server turns it into the project's service catalog.
/// Failures are silent — tracing never depends on the manifest reaching
/// the server.
/// </summary>
internal sealed record ManifestDependency(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("version")] string Version);

internal sealed record ManifestBody(
    [property: JsonPropertyName("service_name")] string ServiceName,
    [property: JsonPropertyName("language")] string Language,
    [property: JsonPropertyName("sdk_version")] string SdkVersion,
    [property: JsonPropertyName("runtime_version")] string RuntimeVersion,
    [property: JsonPropertyName("framework")] string Framework,
    [property: JsonPropertyName("os_arch")] string OsArch,
    [property: JsonPropertyName("app_version")] string AppVersion,
    [property: JsonPropertyName("dependencies")] IReadOnlyList<ManifestDependency> Dependencies);

internal static class Manifest
{
    // Server caps the reported dependency list at 500; stay under it.
    private const int MaxDeps = 500;

    // Well-known web frameworks detected by assembly name prefix; first
    // match wins, everything else reports an empty framework.
    private static readonly (string Prefix, string Name)[] KnownFrameworks =
    {
        ("Microsoft.AspNetCore", "aspnetcore"),
        ("Nancy", "nancy"),
        ("ServiceStack", "servicestack"),
    };

    // One client per process: 5s cap, same as the Go SDK.
    private static readonly HttpClient Http = new() { Timeout = TimeSpan.FromSeconds(5) };

    /// <summary>Derives the startup manifest from the loaded assemblies.</summary>
    internal static ManifestBody BuildManifest(string serviceName, string sdkVersion)
    {
        List<(string Name, string Version)> assemblies;
        try
        {
            assemblies = AppDomain.CurrentDomain.GetAssemblies()
                .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
                .Select(a => a.GetName())
                .Select(n => (Name: n.Name ?? "", Version: n.Version?.ToString() ?? ""))
                .ToList();
        }
        catch
        {
            assemblies = new List<(string Name, string Version)>();
        }

        var framework = "";
        foreach (var (prefix, name) in KnownFrameworks)
        {
            if (assemblies.Any(a => a.Name.StartsWith(prefix, StringComparison.Ordinal)))
            {
                framework = name;
                break;
            }
        }

        var dependencies = assemblies
            .OrderBy(a => a.Name, StringComparer.Ordinal)
            .Take(MaxDeps)
            .Select(a => new ManifestDependency(a.Name, a.Version))
            .ToList();

        return new ManifestBody(
            ServiceName: serviceName,
            Language: "csharp",
            SdkVersion: sdkVersion,
            RuntimeVersion: Environment.Version.ToString(),
            Framework: framework,
            OsArch: OsArch(),
            AppVersion: Environment.GetEnvironmentVariable("DATAFLOW_APP_VERSION") ?? "",
            Dependencies: dependencies);
    }

    /// <summary>
    /// Resolves the HTTP API base for manifest reporting: an explicit
    /// DATAFLOW_HTTP_URL wins (needed when the gRPC endpoint is a bare
    /// host:port); URL-form endpoints map directly; a bare gRPC endpoint
    /// with no override has no derivable HTTP base and reporting is
    /// skipped.
    /// </summary>
    internal static string? HttpBaseURL(string endpoint)
    {
        var env = Environment.GetEnvironmentVariable("DATAFLOW_HTTP_URL");
        if (!string.IsNullOrWhiteSpace(env)) return env.Trim().TrimEnd('/');
        if (endpoint.StartsWith("https://", StringComparison.Ordinal) ||
            endpoint.StartsWith("http://", StringComparison.Ordinal))
        {
            return endpoint.TrimEnd('/');
        }
        return null;
    }

    /// <summary>
    /// Reports the manifest once per process. Best-effort: short timeout,
    /// silent failures, runs on its own task so startup is never delayed.
    /// </summary>
    internal static void Send(string endpoint, string apiKey, string serviceName)
    {
        var baseUrl = HttpBaseURL(endpoint);
        if (baseUrl is null || string.IsNullOrEmpty(apiKey)) return;
        _ = Task.Run(async () =>
        {
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                var body = JsonSerializer.Serialize(
                    BuildManifest(serviceName, Dataflow.SdkVersion));
                using var request = new HttpRequestMessage(
                    HttpMethod.Post, baseUrl + "/api/v1/manifest")
                {
                    Content = new StringContent(body, Encoding.UTF8, "application/json"),
                };
                request.Headers.Add("X-Api-Key", apiKey);
                using var _ = await Http.SendAsync(request, cts.Token);
                // Any error response is silently ignored.
            }
            catch
            {
                // best-effort: manifest failures never surface
            }
        });
    }

    private static string OsArch()
    {
        var os = RuntimeInformation.OSDescription.ToLowerInvariant() switch
        {
            var s when s.Contains("linux") => "linux",
            var s when s.Contains("windows") => "windows",
            var s when s.Contains("darwin") => "mac",
            _ => RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ? "linux"
                : RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "windows"
                : RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? "mac"
                : "",
        };
        return $"{os}/{RuntimeInformation.OSArchitecture.ToString().ToLowerInvariant()}";
    }
}
