using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

// NOTE: deliberately NOT "Dataflow.Sdk.Scan" — a global Dataflow.* namespace
// shadows the static Dataflow class for every consumer doing
// `using Dev.HuginnLabs.Dataflow;` + `Dataflow.Configure()` (the test
// project's csproj documents the same trap for generated sources).
namespace Dev.HuginnLabs.Dataflow.Scan;

/// <summary>One declared HTTP endpoint as posted to the server catalog.</summary>
public sealed record ScannedRoute(
    [property: JsonPropertyName("method")] string Method,
    [property: JsonPropertyName("path")] string Path,
    [property: JsonPropertyName("handler")] string Handler,
    [property: JsonPropertyName("source_file")] string SourceFile);

/// <summary>The POST /api/v1/catalog request body.</summary>
public sealed record CatalogBody(
    [property: JsonPropertyName("service_name")] string ServiceName,
    [property: JsonPropertyName("routes")] IReadOnlyList<ScannedRoute> Routes);

/// <summary>Scan outcome: the extracted routes plus how many files were read.</summary>
public sealed record ScanResult(IReadOnlyList<ScannedRoute> Routes, int FilesScanned);

/// <summary>
/// Static route scanner: walks a source tree, extracts declared HTTP
/// endpoints from C# files with line-oriented regexes (no Roslyn) and
/// posts them to the Dataflow server catalog.
///
/// Coverage: ASP.NET Core attribute routing — a <c>[Route]</c> on the
/// controller class is captured as the prefix and combined with
/// <c>[HttpGet]</c> / <c>[HttpPost]</c> / <c>[HttpPut]</c> / <c>[HttpDelete]</c> /
/// <c>[HttpPatch]</c> / <c>[AcceptVerbs]</c> on actions (the enclosing class is
/// tracked with a brace-depth scan); Razor Pages (PageModel + OnGet/OnPost)
/// match nothing by design; minimal APIs (<c>app.MapGet("/path", ...)</c> and
/// siblings) are captured with an empty handler. Actions with a bare
/// <c>[Route]</c> but no HTTP-method attribute are skipped — the method verb
/// is unknowable.
///
/// Tokens are resolved the way ASP.NET does when the class is known:
/// <c>[controller]</c> in a route template becomes the controller name
/// lowercased, without the "Controller" suffix (OrdersController →
/// <c>orders</c>); <c>[action]</c> becomes the action name lowercased.
/// Unknown tokens are left as literals. Route parameters keep the
/// <c>{id}</c> (and <c>{id:int}</c>) syntax.
/// </summary>
public static class ScanTool
{
    // The server caps the catalog at 1000 routes; report the first 1000.
    private const int MaxRoutes = 1000;

    private static readonly string[] SkippedDirs = { "bin", "obj", ".git" };

    /// <summary>
    /// CLI entry point. Flags: <c>--dir</c> (source root, default "."),
    /// <c>--service</c> (default: directory name), <c>--url</c> (API base;
    /// default: DATAFLOW_HTTP_URL, then URL-form DATAFLOW_ENDPOINT — a bare
    /// host:port endpoint cannot be derived and is reported),
    /// <c>--api-key</c> (default: DATAFLOW_API_KEY), <c>--print</c> (print
    /// the catalog JSON to stdout instead of posting).
    /// Exit codes: 0 ok, 2 usage/config error, 3 post failure.
    /// </summary>
    public static int Run(string[] args)
    {
        string? dir = null, service = null, url = null, apiKey = null;
        var print = false;

        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--dir":
                    if ((dir = Next(args, ref i, "--dir")) is null) return 2;
                    break;
                case "--service":
                    if ((service = Next(args, ref i, "--service")) is null) return 2;
                    break;
                case "--url":
                    if ((url = Next(args, ref i, "--url")) is null) return 2;
                    break;
                case "--api-key":
                    if ((apiKey = Next(args, ref i, "--api-key")) is null) return 2;
                    break;
                case "--print":
                    print = true;
                    break;
                case "--help":
                case "-h":
                    Usage();
                    return 0;
                default:
                    Console.Error.WriteLine($"dataflow-scan: unknown argument '{args[i]}' (try --help)");
                    return 2;
            }
        }

        var root = Path.GetFullPath(dir ?? ".");
        if (!Directory.Exists(root))
        {
            Console.Error.WriteLine($"dataflow-scan: directory not found: {root}");
            return 2;
        }
        service ??= SafeDirName(root);

        ScanResult result;
        try
        {
            result = ScanDirectory(root);
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"dataflow-scan: scan failed: {e.Message}");
            return 2;
        }

        Console.Out.WriteLine(
            $"dataflow-scan: scanned {result.FilesScanned} C# file(s), found {result.Routes.Count} route(s) under {root}");
        var body = new CatalogBody(service, result.Routes);

        if (print)
        {
            Console.Out.WriteLine(JsonSerializer.Serialize(body));
            return 0;
        }

        // Same base-URL precedence as the startup manifest: an explicit
        // --url wins, then DATAFLOW_HTTP_URL, then a URL-form
        // DATAFLOW_ENDPOINT; a bare host:port has no derivable HTTP base.
        var endpoint = url ?? Environment.GetEnvironmentVariable("DATAFLOW_ENDPOINT") ?? "";
        var baseUrl = Manifest.HttpBaseURL(endpoint);
        if (baseUrl is null)
        {
            Console.Error.WriteLine(
                "dataflow-scan: cannot derive an HTTP base URL from the endpoint " +
                $"'{endpoint}' (bare host:port). Pass --url or set DATAFLOW_HTTP_URL.");
            return 2;
        }
        apiKey ??= Environment.GetEnvironmentVariable("DATAFLOW_API_KEY");
        if (string.IsNullOrWhiteSpace(apiKey))
        {
            Console.Error.WriteLine("dataflow-scan: no API key. Pass --api-key or set DATAFLOW_API_KEY.");
            return 2;
        }

        return Post(baseUrl, apiKey, body);
    }

    /// <summary>Walks the tree (skipping bin/, obj/, .git/, *Tests.cs) and scans every remaining .cs file.</summary>
    internal static ScanResult ScanDirectory(string dir)
    {
        var routes = new List<ScannedRoute>();
        var files = 0;
        foreach (var path in EnumerateSources(dir))
        {
            files++;
            var relative = Path.GetRelativePath(dir, path).Replace('\\', '/');
            routes.AddRange(ScanFile(path, relative));
        }
        return new ScanResult(routes.Take(MaxRoutes).ToList(), files);
    }

    /// <summary>Extracts the routes declared in a single C# source file.</summary>
    internal static IReadOnlyList<ScannedRoute> ScanFile(string path, string relativePath)
    {
        string[] lines;
        try
        {
            lines = File.ReadAllLines(path);
        }
        catch (Exception)
        {
            // An unreadable file never aborts the scan.
            return Array.Empty<ScannedRoute>();
        }

        var scanner = new FileScanner(relativePath);
        foreach (var line in lines) scanner.Line(line);
        return scanner.Routes;
    }

    private static string? Next(string[] args, ref int i, string flag)
    {
        if (i + 1 >= args.Length)
        {
            Console.Error.WriteLine($"dataflow-scan: {flag} needs a value");
            return null;
        }
        return args[++i];
    }

    private static string SafeDirName(string root)
    {
        try
        {
            return new DirectoryInfo(root).Name is { Length: > 0 } n ? n : "unknown";
        }
        catch (Exception)
        {
            return "unknown";
        }
    }

    private static void Usage()
    {
        Console.Out.WriteLine(
            """
            dataflow scan — extract declared HTTP endpoints from C# sources and post them to the catalog

            usage: dotnet run --project Dataflow.Scan -- [options]

              --dir <path>      source root to scan (default: current directory)
              --service <name>  service name (default: the directory name)
              --url <base>      API base URL (default: DATAFLOW_HTTP_URL, then URL-form DATAFLOW_ENDPOINT)
              --api-key <key>   project API key (default: DATAFLOW_API_KEY)
              --print           print the catalog JSON instead of posting
            """);
    }

    private static IEnumerable<string> EnumerateSources(string dir)
    {
        var pending = new Stack<string>();
        pending.Push(dir);
        while (pending.Count > 0)
        {
            var current = pending.Pop();
            foreach (var sub in Directory.EnumerateDirectories(current))
            {
                var name = Path.GetFileName(sub);
                if (Array.IndexOf(SkippedDirs, name) >= 0) continue;
                pending.Push(sub);
            }
            foreach (var file in Directory.EnumerateFiles(current, "*.cs"))
            {
                if (file.EndsWith("Tests.cs", StringComparison.OrdinalIgnoreCase)) continue;
                yield return file;
            }
        }
    }

    private static int Post(string baseUrl, string apiKey, CatalogBody body)
    {
        try
        {
            // One client per run with a comfortable cap, mirroring the
            // manifest's approach (System.Text.Json + X-Api-Key header).
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var json = JsonSerializer.Serialize(body);
            using var request = new HttpRequestMessage(HttpMethod.Post, baseUrl + "/api/v1/catalog")
            {
                Content = new StringContent(json, Encoding.UTF8, "application/json"),
            };
            request.Headers.Add("X-Api-Key", apiKey);
            using var response = http.SendAsync(request, cts.Token).GetAwaiter().GetResult();
            if (response.IsSuccessStatusCode)
            {
                Console.Out.WriteLine(
                    $"dataflow-scan: posted {body.Routes.Count} route(s) for '{body.ServiceName}' to {baseUrl}/api/v1/catalog");
                return 0;
            }
            Console.Error.WriteLine(
                $"dataflow-scan: catalog post failed: HTTP {(int)response.StatusCode} {response.ReasonPhrase}");
            return 3;
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"dataflow-scan: catalog post failed: {e}");
            return 3;
        }
    }

    /// <summary>Line-oriented extractor for one file: regexes plus a brace-depth class stack.</summary>
    private sealed class FileScanner
    {
        private static readonly Regex ClassRegex =
            new(@"\bclass\s+(\w+)", RegexOptions.Compiled);

        private static readonly Regex MethodRegex =
            new(@"^\s*(?:\[[^\]]*\]\s*)*(?:public|internal|protected|private)\b[^=;(]*?(\w+)\s*(?:<[^=;(]*>)?\s*\(",
                RegexOptions.Compiled);

        private static readonly Regex HttpVerbRegex =
            new(@"\[Http(Get|Post|Put|Delete|Patch)\s*(?:\(\s*""([^""]*)""[^)]*\))?\s*\]",
                RegexOptions.Compiled);

        private static readonly Regex AcceptVerbsRegex =
            new(@"\[AcceptVerbs\s*\(([^)]*)\)", RegexOptions.Compiled);

        private static readonly Regex QuotedTokenRegex = new(@"""([^""]*)""", RegexOptions.Compiled);

        private static readonly Regex RouteRegex =
            new(@"\[Route\s*\(\s*""([^""]*)""", RegexOptions.Compiled);

        private static readonly Regex MapEndpointRegex =
            new(@"\bMap(Get|Post|Put|Delete|Patch)\s*\(\s*@?""([^""]*)""", RegexOptions.Compiled);

        private readonly string _relativePath;
        private readonly List<ScannedRoute> _routes = new();
        private readonly HashSet<string> _seen = new(StringComparer.Ordinal);
        private readonly List<(string Name, string Prefix, int BodyDepth)> _classes = new();

        private string? _classRoute;     // [Route] seen above the class line
        private string? _actionRoute;    // [Route] seen inside the class
        private string? _httpPath;       // path argument of the pending [Http*]
        private readonly List<string> _httpVerbs = new();
        private int _depth;
        private int _pushedThisLine;

        public FileScanner(string relativePath) => _relativePath = relativePath;

        public IReadOnlyList<ScannedRoute> Routes => _routes;

        public void Line(string raw)
        {
            // Comments are stripped (strings preserved) so commented-out
            // attributes never match; the result feeds regex + brace counting.
            var line = StripLineComment(raw);

            // Attributes first so `[HttpGet("{id}")] public X Get(...)` on a
            // single line emits instead of losing the pending state.
            CollectHttpVerbs(line);
            CollectAcceptVerbs(line);
            CollectRoutes(line);
            CollectMapEndpoints(line);

            var delta = BraceDelta(line);

            var classMatch = ClassRegex.Match(line);
            if (classMatch.Success) PushClass(classMatch.Groups[1].Value, delta);

            var methodMatch = MethodRegex.Match(line);
            if (methodMatch.Success) HandleMethodDecl(methodMatch.Groups[1].Value);

            _depth += delta;
            // Classes pushed on this line are exempt from this line's pop
            // check: in Allman style their opening brace is on the next
            // line, so the depth is still below the expected body depth.
            while (_classes.Count > _pushedThisLine && _classes[^1].BodyDepth > _depth)
            {
                _classes.RemoveAt(_classes.Count - 1);
            }
            _pushedThisLine = 0;
        }

        private void PushClass(string name, int delta)
        {
            // Attributes before the class line belong to the class; any
            // leftover action-level pending state is stale now.
            var prefix = _classRoute is null ? "" : ResolveTokens(_classRoute, name, null);
            _classRoute = null;
            _httpVerbs.Clear();
            _actionRoute = null;
            _httpPath = null;
            // Body depth assumes the opening brace comes on this line or the
            // next (delta 0 = Allman style); the pop rule works for both.
            _classes.Add((name, prefix, _depth + Math.Max(delta, 1)));
            _pushedThisLine++;
        }

        private void HandleMethodDecl(string name)
        {
            if (_classes.Count > 0)
            {
                var (className, prefix, _) = _classes[^1];
                // A constructor shares the class name and carries no route.
                if (name != className && _httpVerbs.Count > 0)
                {
                    // [action] in the class prefix resolves per action
                    // ([controller] was already resolved at class push).
                    var actionPrefix = ResolveTokens(prefix, className, name);
                    var path = CombinePaths(actionPrefix, ResolveTokens(_actionRoute, className, name), _httpPath);
                    foreach (var verb in _httpVerbs) Emit(verb, path, $"{className}.{name}");
                }
            }
            // Pending state never outlives a method declaration.
            _httpVerbs.Clear();
            _actionRoute = null;
            _httpPath = null;
        }

        private void CollectHttpVerbs(string line)
        {
            foreach (Match match in HttpVerbRegex.Matches(line))
            {
                _httpVerbs.Add(match.Groups[1].Value.ToUpperInvariant());
                if (match.Groups[2].Success) _httpPath = match.Groups[2].Value;
            }
        }

        private void CollectAcceptVerbs(string line)
        {
            foreach (Match match in AcceptVerbsRegex.Matches(line))
            {
                foreach (Match token in QuotedTokenRegex.Matches(match.Groups[1].Value))
                {
                    if (token.Groups[1].Value.Length > 0) _httpVerbs.Add(token.Groups[1].Value.ToUpperInvariant());
                }
            }
        }

        private void CollectRoutes(string line)
        {
            foreach (Match match in RouteRegex.Matches(line))
            {
                var template = match.Groups[1].Value;
                // [Route] above a class declaration is the prefix; inside the
                // class it decorates the next action.
                if (_classes.Count > 0) _actionRoute = template;
                else _classRoute = template;
            }
        }

        private void CollectMapEndpoints(string line)
        {
            foreach (Match match in MapEndpointRegex.Matches(line))
            {
                Emit(
                    match.Groups[1].Value.ToUpperInvariant(),
                    NormalizePath(match.Groups[2].Value),
                    // Minimal-API lambdas have no name; the file reference is the handler.
                    "");
            }
        }

        private void Emit(string method, string path, string handler)
        {
            var key = $"{method} {path} {handler}";
            if (!_seen.Add(key)) return;
            _routes.Add(new ScannedRoute(method, path, handler, _relativePath));
        }

        /// <summary>
        /// Resolves ASP.NET tokens the way the framework does when the class
        /// is known: [controller] → controller name lowercased without the
        /// "Controller" suffix, [action] → action name lowercased. Unknown
        /// tokens stay literal.
        /// </summary>
        private static string ResolveTokens(string? template, string className, string? actionName)
        {
            if (string.IsNullOrEmpty(template)) return "";
            var controller = className.EndsWith("Controller", StringComparison.Ordinal)
                ? className[..^"Controller".Length]
                : className;
            var result = Regex.Replace(
                template, @"\[controller\]", controller.ToLowerInvariant(), RegexOptions.IgnoreCase);
            if (actionName is not null)
            {
                result = Regex.Replace(result, @"\[action\]", actionName.ToLowerInvariant(), RegexOptions.IgnoreCase);
            }
            return result;
        }

        /// <summary>
        /// Class prefix + action [Route] + [Http*] argument, ASP.NET-style:
        /// an absolute action template (leading "/") overrides the prefix.
        /// </summary>
        private static string CombinePaths(string prefix, string? actionRoute, string? httpPath)
        {
            actionRoute ??= "";
            httpPath ??= "";
            if (httpPath.StartsWith('/')) return NormalizePath(httpPath);
            if (actionRoute.StartsWith('/')) return NormalizePath(actionRoute);
            var joined = string.Join("/", new[] { prefix, actionRoute, httpPath }
                .Where(p => !string.IsNullOrEmpty(p)));
            return NormalizePath(joined);
        }

        /// <summary>Collapses slashes and guarantees a single leading "/" (empty stays empty).</summary>
        private static string NormalizePath(string raw)
        {
            var sb = new StringBuilder(raw.Length + 1);
            foreach (var c in raw)
            {
                if (c == '/' && (sb.Length == 0 || sb[^1] == '/')) continue;
                sb.Append(c);
            }
            if (sb.Length == 0) return "";
            if (sb[0] != '/') sb.Insert(0, '/');
            while (sb.Length > 1 && sb[^1] == '/') sb.Length--;
            return sb.ToString();
        }

        /// <summary>Removes // comments while respecting string/char literals (strings kept intact).</summary>
        private static string StripLineComment(string line)
        {
            var sb = new StringBuilder(line.Length);
            var inString = false;
            var inChar = false;
            var escaped = false;
            for (var i = 0; i < line.Length; i++)
            {
                var c = line[i];
                if (escaped)
                {
                    escaped = false;
                    sb.Append(c);
                    continue;
                }
                if ((inString || inChar) && c == '\\')
                {
                    escaped = true;
                    sb.Append(c);
                    continue;
                }
                if (inString)
                {
                    if (c == '"') inString = false;
                    sb.Append(c);
                    continue;
                }
                if (inChar)
                {
                    if (c == '\'') inChar = false;
                    sb.Append(c);
                    continue;
                }
                if (c == '"')
                {
                    inString = true;
                    sb.Append(c);
                    continue;
                }
                if (c == '\'')
                {
                    inChar = true;
                    sb.Append(c);
                    continue;
                }
                if (c == '/' && i + 1 < line.Length && line[i + 1] == '/') break;
                sb.Append(c);
            }
            return sb.ToString();
        }

        /// <summary>Net brace delta of a line, ignoring braces inside string/char literals.</summary>
        private static int BraceDelta(string line)
        {
            var delta = 0;
            var inString = false;
            var inChar = false;
            var escaped = false;
            for (var i = 0; i < line.Length; i++)
            {
                var c = line[i];
                if (escaped)
                {
                    escaped = false;
                    continue;
                }
                if ((inString || inChar) && c == '\\')
                {
                    escaped = true;
                    continue;
                }
                if (inString)
                {
                    if (c == '"') inString = false;
                    continue;
                }
                if (inChar)
                {
                    if (c == '\'') inChar = false;
                    continue;
                }
                if (c == '"')
                {
                    inString = true;
                    continue;
                }
                if (c == '\'')
                {
                    inChar = true;
                    continue;
                }
                if (c == '{') delta++;
                else if (c == '}') delta--;
            }
            return delta;
        }
    }
}
