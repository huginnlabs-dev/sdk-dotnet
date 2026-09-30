using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Dev.HuginnLabs.Dataflow.Scan;
using Xunit;

namespace DataflowSdk.Tests;

// Route scanner end to end over fixtures written to temp directories:
// attribute routing (class [Route] prefix + action [Http*] verbs), minimal
// APIs, skip rules, JSON shape, and the --print CLI path. One test class
// keeps the Console/Environment mutations sequential.
public class ScanToolTests : IDisposable
{
    private readonly string _root = Path.Combine(
        Path.GetTempPath(), "dataflow-scan-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
            // temp cleanup is best-effort
        }
    }

    private void Write(params (string RelPath, string Content)[] files)
    {
        foreach (var (rel, content) in files)
        {
            var full = Path.Combine(_root, rel);
            Directory.CreateDirectory(Path.GetDirectoryName(full)!);
            File.WriteAllText(full, content);
        }
    }

    private static (int Code, string StdOut, string StdErr) Capture(Func<int> action)
    {
        var originalOut = Console.Out;
        var originalErr = Console.Error;
        var stdout = new StringWriter();
        var stderr = new StringWriter();
        Console.SetOut(stdout);
        Console.SetError(stderr);
        try
        {
            return (action(), stdout.ToString(), stderr.ToString());
        }
        finally
        {
            Console.SetOut(originalOut);
            Console.SetError(originalErr);
        }
    }

    private static string? ClearHttpUrlEnv()
    {
        var previous = Environment.GetEnvironmentVariable("DATAFLOW_HTTP_URL");
        Environment.SetEnvironmentVariable("DATAFLOW_HTTP_URL", null);
        return previous;
    }

    // ------------------------------------------------------------------
    // Attribute routing

    [Fact]
    public void ControllerRoutes_PrefixPlusActionVerbs()
    {
        Write(("Controllers/OrdersController.cs", """
            using Microsoft.AspNetCore.Mvc;

            [Route("api/[controller]")]
            public class OrdersController : ControllerBase
            {
                [HttpGet("{id}")]
                public Order Get(int id) => new();

                [HttpPost]
                public IActionResult Create([FromBody] Order order) => Ok();

                [HttpDelete("id/{id:int}")]
                public IActionResult Remove(int id) => NoContent();
            }
            """));

        var result = ScanTool.ScanDirectory(_root);

        Assert.Equal(3, result.Routes.Count);
        Assert.Equal(new ScannedRoute("GET", "/api/orders/{id}", "OrdersController.Get", "Controllers/OrdersController.cs"),
            result.Routes[0]);
        // [controller] resolves to the lowercased class name without the
        // Controller suffix, exactly like ASP.NET template replacement.
        Assert.Equal(new ScannedRoute("POST", "/api/orders", "OrdersController.Create", "Controllers/OrdersController.cs"),
            result.Routes[1]);
        Assert.Equal(new ScannedRoute("DELETE", "/api/orders/id/{id:int}", "OrdersController.Remove", "Controllers/OrdersController.cs"),
            result.Routes[2]);
    }

    [Fact]
    public void ControllerToken_WithoutSuffixClass_LowercasesFullName()
    {
        Write(("Inventory.cs", """
            using Microsoft.AspNetCore.Mvc;

            [Route("api/[Controller]")]
            public class Inventory : Controller
            {
                [HttpGet("items")]
                public IActionResult Items() => Ok();
            }
            """));

        var routes = ScanTool.ScanDirectory(_root).Routes;

        var route = Assert.Single(routes);
        Assert.Equal("GET", route.Method);
        Assert.Equal("/api/inventory/items", route.Path);
        Assert.Equal("Inventory.Items", route.Handler);
    }

    [Fact]
    public void BareVerbWithoutPrefix_KeepsEmptyPath()
    {
        Write(("HealthController.cs", """
            using Microsoft.AspNetCore.Mvc;

            public class HealthController
            {
                [HttpGet]
                public IActionResult Ping() => Ok();
            }
            """));

        var routes = ScanTool.ScanDirectory(_root).Routes;

        var route = Assert.Single(routes);
        Assert.Equal(new ScannedRoute("GET", "", "HealthController.Ping", "HealthController.cs"), route);
    }

    [Fact]
    public void AbsoluteActionTemplate_OverridesClassPrefix()
    {
        Write(("OpsController.cs", """
            using Microsoft.AspNetCore.Mvc;

            [Route("internal/v1")]
            public class OpsController : ControllerBase
            {
                [HttpGet("/healthz")]
                public IActionResult Health() => Ok();
            }
            """));

        var routes = ScanTool.ScanDirectory(_root).Routes;

        var route = Assert.Single(routes);
        Assert.Equal("/healthz", route.Path);
    }

    [Fact]
    public void AcceptVerbs_ExpandToOneRoutePerVerb()
    {
        Write(("PaymentsController.cs", """
            using Microsoft.AspNetCore.Mvc;

            public class PaymentsController
            {
                [AcceptVerbs("get", "POST")]
                [Route("pay/{id}")]
                public IActionResult Pay(int id) => Ok();
            }
            """));

        var routes = ScanTool.ScanDirectory(_root).Routes;

        Assert.Equal(2, routes.Count);
        Assert.Equal("GET", routes[0].Method);
        Assert.Equal("POST", routes[1].Method);
        Assert.All(routes, r =>
        {
            Assert.Equal("/pay/{id}", r.Path);
            Assert.Equal("PaymentsController.Pay", r.Handler);
        });
    }

    [Fact]
    public void ActionToken_InRouteTemplate_ResolvesToActionName()
    {
        Write(("TagsController.cs", """
            using Microsoft.AspNetCore.Mvc;

            [Route("api/[controller]/[action]")]
            public class TagsController
            {
                [HttpGet]
                public IActionResult All() => Ok();
            }
            """));

        var routes = ScanTool.ScanDirectory(_root).Routes;

        var route = Assert.Single(routes);
        Assert.Equal("/api/tags/all", route.Path);
    }

    [Fact]
    public void RouteOnlyAction_WithoutVerb_IsSkipped()
    {
        Write(("ThingsController.cs", """
            using Microsoft.AspNetCore.Mvc;

            public class ThingsController
            {
                [Route("things")]
                public IActionResult Everything() => Ok();
            }
            """));

        Assert.Empty(ScanTool.ScanDirectory(_root).Routes);
    }

    [Fact]
    public void RazorPageModel_HandlersAreNotRoutes()
    {
        Write(("Pages/Index.cshtml.cs", """
            using Microsoft.AspNetCore.Mvc.RazorPages;

            public class IndexModel : PageModel
            {
                public void OnGet() { }

                public IActionResult OnPost() => RedirectToPage();
            }
            """));

        Assert.Empty(ScanTool.ScanDirectory(_root).Routes);
    }

    // ------------------------------------------------------------------
    // Minimal APIs

    [Fact]
    public void MinimalApis_MapVerbsWithEmptyHandler()
    {
        Write(("Program.cs", """
            var builder = WebApplication.CreateBuilder(args);
            var app = builder.Build();

            app.MapGet("/orders", () => new[] { 1, 2 });
            app.MapPost("/orders", () => Results.Ok());
            app.MapPut("/orders/{id}", (int id) => Results.Ok());
            app.MapDelete("/orders/{id}", (int id) => Results.NoContent());
            app.MapPatch("/orders/{id}", () => Results.Ok());

            app.Run();
            """));

        var routes = ScanTool.ScanDirectory(_root).Routes;

        Assert.Equal(5, routes.Count);
        Assert.Collection(routes,
            r => Assert.Equal(new ScannedRoute("GET", "/orders", "", "Program.cs"), r),
            r => Assert.Equal(new ScannedRoute("POST", "/orders", "", "Program.cs"), r),
            r => Assert.Equal(new ScannedRoute("PUT", "/orders/{id}", "", "Program.cs"), r),
            r => Assert.Equal(new ScannedRoute("DELETE", "/orders/{id}", "", "Program.cs"), r),
            r => Assert.Equal(new ScannedRoute("PATCH", "/orders/{id}", "", "Program.cs"), r));
    }

    // ------------------------------------------------------------------
    // Skip rules, comments and caps

    [Fact]
    public void Scan_SkipsBinObjGitAndTestsFiles()
    {
        Write(
            ("bin/Generated.cs", "app.MapGet(\"/bin-route\", () => 1);\n"),
            ("obj/Generated.cs", "app.MapGet(\"/obj-route\", () => 1);\n"),
            (".git/hooks/Hook.cs", "app.MapGet(\"/git-route\", () => 1);\n"),
            ("OrdersTests.cs", "app.MapGet(\"/tests-route\", () => 1);\n"),
            ("Real/OrdersService.cs", "app.MapGet(\"/real-route\", () => 1);\n"));

        var result = ScanTool.ScanDirectory(_root);

        Assert.Equal(1, result.FilesScanned);
        var route = Assert.Single(result.Routes);
        Assert.Equal("/real-route", route.Path);
    }

    [Fact]
    public void Scan_CapsRoutesAtThousand()
    {
        var source = new StringBuilder("var app = App.Run();\n");
        for (var i = 0; i < 1100; i++)
        {
            source.Append($"app.MapGet(\"/p{i}\", () => 1);\n");
        }
        Write(("Many.cs", source.ToString()));

        var result = ScanTool.ScanDirectory(_root);

        Assert.Equal(1000, result.Routes.Count);
        Assert.Equal("/p999", result.Routes[999].Path);
    }

    [Fact]
    public void Scan_CommentedOutAttributes_AreIgnored()
    {
        Write(("ThingsController.cs", """
            using Microsoft.AspNetCore.Mvc;

            public class ThingsController
            {
                // [HttpGet("/ghost")]
                // public IActionResult Ghost() => Ok();

                [HttpGet("/real")]
                public IActionResult Real() => Ok();
            }
            """));

        var routes = ScanTool.ScanDirectory(_root).Routes;

        var route = Assert.Single(routes);
        Assert.Equal("/real", route.Path);
    }

    // ------------------------------------------------------------------
    // JSON contract and CLI

    [Fact]
    public void CatalogBody_SerializesServerContract()
    {
        var body = new CatalogBody("orders-api", new[]
        {
            new ScannedRoute("GET", "/api/orders/{id}", "OrdersController.Get", "Controllers/OrdersController.cs"),
        });

        var json = JsonSerializer.Serialize(body);

        Assert.Equal(
            """{"service_name":"orders-api","routes":[{"method":"GET","path":"/api/orders/{id}","handler":"OrdersController.Get","source_file":"Controllers/OrdersController.cs"}]}""",
            json);
    }

    [Fact]
    public void PrintFlag_EmitsJsonToStdout_WithoutPosting()
    {
        Write(("OrdersController.cs", """
            using Microsoft.AspNetCore.Mvc;

            [Route("api/[controller]")]
            public class OrdersController : ControllerBase
            {
                [HttpGet("{id}")]
                public Order Get(int id) => new();
            }
            """));

        var (code, stdout, stderr) = Capture(() => ScanTool.Run(
            new[] { "--dir", _root, "--service", "orders-api", "--print" }));

        Assert.Equal(0, code);
        Assert.Equal("", stderr);
        Assert.Contains("found 1 route(s)", stdout);

        var json = stdout.Substring(stdout.IndexOf('{'));
        using var doc = JsonDocument.Parse(json);
        Assert.Equal("orders-api", doc.RootElement.GetProperty("service_name").GetString());
        var routes = doc.RootElement.GetProperty("routes");
        Assert.Equal(1, routes.GetArrayLength());
        Assert.Equal("GET", routes[0].GetProperty("method").GetString());
        Assert.Equal("/api/orders/{id}", routes[0].GetProperty("path").GetString());
        Assert.Equal("OrdersController.Get", routes[0].GetProperty("handler").GetString());
        Assert.Equal("OrdersController.cs", routes[0].GetProperty("source_file").GetString());
    }

    [Fact]
    public void Run_DefaultsServiceNameToDirectoryName()
    {
        Write(("Ping.cs", "app.MapGet(\"/ping\", () => 1);\n"));

        var (code, stdout, _) = Capture(() => ScanTool.Run(new[] { "--dir", _root, "--print" }));

        Assert.Equal(0, code);
        using var doc = JsonDocument.Parse(stdout.Substring(stdout.IndexOf('{')));
        Assert.Equal(new DirectoryInfo(_root).Name, doc.RootElement.GetProperty("service_name").GetString());
    }

    [Fact]
    public void Run_BadUsage_ReturnsExitCode2()
    {
        var previous = ClearHttpUrlEnv();
        try
        {
            Assert.Equal(2, Capture(() => ScanTool.Run(new[] { "--nope" })).Code);
            Assert.Equal(2, Capture(() => ScanTool.Run(new[] { "--dir" })).Code);
            Assert.Equal(2, Capture(() => ScanTool.Run(new[] { "--dir", _root, "--service", "s", "--url", "host:9999" })).Code);
            Assert.Equal(2, Capture(() => ScanTool.Run(new[] { "--dir", Path.Combine(_root, "missing") })).Code);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DATAFLOW_HTTP_URL", previous);
        }
    }

    [Fact]
    public void Run_BareEndpointWithHttpUrlEnv_Posts()
    {
        Write(("Ping.cs", "app.MapGet(\"/ping\", () => 1);\n"));
        using var stub = new HttpStub();
        var previous = Environment.GetEnvironmentVariable("DATAFLOW_HTTP_URL");
        Environment.SetEnvironmentVariable("DATAFLOW_HTTP_URL", stub.BaseUrl);
        try
        {
            var (code, _, stderr) = Capture(() => ScanTool.Run(
                new[] { "--dir", _root, "--service", "orders-api", "--api-key", "k" }));

            Assert.Equal(0, code);
            Assert.Equal("", stderr);
            Assert.Equal("POST /api/v1/catalog HTTP/1.1", stub.RequestLine);
            Assert.Equal("k", stub.Headers["x-api-key"]);
            Assert.Contains("\"service_name\":\"orders-api\"", stub.Body);
            Assert.Contains("\"path\":\"/ping\"", stub.Body);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DATAFLOW_HTTP_URL", previous);
        }
    }

    [Fact]
    public void Run_ServerError_ReturnsExitCode3()
    {
        Write(("Ping.cs", "app.MapGet(\"/ping\", () => 1);\n"));
        using var stub = new HttpStub(statusCode: 500);

        var (code, _, stderr) = Capture(() => ScanTool.Run(
            new[] { "--dir", _root, "--service", "orders-api", "--url", stub.BaseUrl, "--api-key", "k" }));

        Assert.Equal(3, code);
        Assert.Contains("HTTP 500", stderr);
    }

    // ------------------------------------------------------------------
    // Base-URL resolution (same precedence as the startup manifest)

    [Fact]
    public void HttpBaseURL_EnvWins_ThenUrlForm_BareIsSkipped()
    {
        var previous = ClearHttpUrlEnv();
        try
        {
            Environment.SetEnvironmentVariable("DATAFLOW_HTTP_URL", "http://env.example:8080/");
            Assert.Equal("http://env.example:8080",
                global::Dev.HuginnLabs.Dataflow.Manifest.HttpBaseURL("https://endpoint.example"));

            Environment.SetEnvironmentVariable("DATAFLOW_HTTP_URL", null);
            Assert.Equal("https://endpoint.example",
                global::Dev.HuginnLabs.Dataflow.Manifest.HttpBaseURL("https://endpoint.example"));
            Assert.Equal("http://endpoint.example",
                global::Dev.HuginnLabs.Dataflow.Manifest.HttpBaseURL("http://endpoint.example/"));
            Assert.Null(global::Dev.HuginnLabs.Dataflow.Manifest.HttpBaseURL("ingest.example:4317"));
            Assert.Null(global::Dev.HuginnLabs.Dataflow.Manifest.HttpBaseURL(""));
        }
        finally
        {
            Environment.SetEnvironmentVariable("DATAFLOW_HTTP_URL", previous);
        }
    }

    // ------------------------------------------------------------------
    // Minimal in-process HTTP stub for the post path (raw TCP — no
    // HttpListener URLACL requirements on Windows). Bound to the IPv6
    // loopback: on some Windows setups HttpClient's connect path for the
    // "127.0.0.1" literal resolves through a broken v6-mapped address
    // (WSAEADDRNOTAVAIL), while pure [::1] connects are reliable.

    private sealed class HttpStub : IDisposable
    {
        private readonly TcpListener _listener;

        public HttpStub(int statusCode = 200)
        {
            _listener = new TcpListener(System.Net.IPAddress.IPv6Loopback, 0);
            _listener.Start();
            StatusCode = statusCode;
            System.Threading.Tasks.Task.Run(() =>
            {
                try
                {
                    Serve();
                }
                catch (Exception)
                {
                    // listener torn down mid-test
                }
            });
        }

        public string BaseUrl => $"http://[::1]:{((System.Net.IPEndPoint)_listener.LocalEndpoint).Port}";

        private int StatusCode { get; }

        public string RequestLine { get; private set; } = "";

        public Dictionary<string, string> Headers { get; } = new(StringComparer.OrdinalIgnoreCase);

        public string Body { get; private set; } = "";

        private void Serve()
        {
            using var client = _listener.AcceptTcpClient();
            using var stream = client.GetStream();
            var received = new MemoryStream();
            var buffer = new byte[8192];
            var headerEnd = -1;
            while (headerEnd < 0)
            {
                var read = stream.Read(buffer, 0, buffer.Length);
                if (read <= 0) return;
                received.Write(buffer, 0, read);
                var bytes = received.ToArray();
                headerEnd = IndexOfHeaderEnd(bytes);
            }
            var all = received.ToArray();
            var head = Encoding.ASCII.GetString(all, 0, headerEnd);
            var lines = head.Split("\r\n");
            RequestLine = lines[0];
            foreach (var line in lines.Skip(1))
            {
                var sep = line.IndexOf(':');
                if (sep > 0) Headers[line[..sep].Trim()] = line[(sep + 1)..].Trim();
            }

            var bodyStart = headerEnd + 4;
            Headers.TryGetValue("Content-Length", out var lengthText);
            var contentLength = int.TryParse(lengthText, out var parsed) ? parsed : 0;
            while (received.Length < bodyStart + contentLength)
            {
                var read = stream.Read(buffer, 0, buffer.Length);
                if (read <= 0) break;
                received.Write(buffer, 0, read);
            }
            all = received.ToArray();
            Body = Encoding.UTF8.GetString(all, bodyStart, Math.Min(contentLength, all.Length - bodyStart));

            var reason = StatusCode == 200 ? "OK" : "Server Error";
            var response = Encoding.ASCII.GetBytes(
                $"HTTP/1.1 {StatusCode} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            stream.Write(response, 0, response.Length);
        }

        private static int IndexOfHeaderEnd(byte[] bytes)
        {
            for (var i = 0; i + 3 < bytes.Length; i++)
            {
                if (bytes[i] == (byte)'\r' && bytes[i + 1] == (byte)'\n' &&
                    bytes[i + 2] == (byte)'\r' && bytes[i + 3] == (byte)'\n')
                {
                    return i;
                }
            }
            return -1;
        }

        public void Dispose() => _listener.Stop();
    }
}
