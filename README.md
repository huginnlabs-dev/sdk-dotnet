# HuginnLabs Dataflow SDK for .NET

Runtime tracing for .NET services: spans stream to the Dataflow ingestion
API over gRPC (`StreamEvents`) with an ack-watermark replay buffer, and
payload values are AES-256-GCM encrypted with a PBKDF2-derived key that
never leaves the process.

```csharp
Dataflow.Configure(); // reads DATAFLOW_* environment variables
```

| Variable | Purpose |
|---|---|
| `DATAFLOW_ENDPOINT` | gRPC endpoint of the ingestion API (`host:port` or URL) |
| `DATAFLOW_API_KEY` | Project API key |
| `DATAFLOW_SERVICE_NAME` | Service name (defaults to the host name) |
| `DATAFLOW_ENCRYPTION_KEY` | Payload encryption secret (plaintext warning if unset) |
| `DATAFLOW_SAMPLE_RATIO` | Span sampling ratio, `1.0` by default |
| `DATAFLOW_BUFFER_SIZE` | Replay buffer capacity (default `10000`) |
| `DATAFLOW_DISABLED` | `true` turns every tracer into a pass-through |

## Spans

```csharp
using var t = Dataflow.Trace("booking.Create");        // ambient child span
var span = Dataflow.StartSpan("queue.Publish");        // manual span
span.Attr("k", "v").Data("field", 42).Callee("queue"); // attrs / payload
span.End();
```

The ambient span flows through `AsyncLocal`, so children join the trace
across awaits without explicit plumbing. ASP.NET Core entry points:

```csharp
app.UseMiddleware<DataflowMiddleware>(); // HTTP_SERVER spans + agent metadata
```

## HTTP client tracing

Any `HttpClient` built on `DataflowHttpHandler` emits one `HTTP_CLIENT`
span per call — `METHOD host/path` as the name, the host as
`callee_package`, the response status as `status_code` and `http.method` /
`http.url` / `http.status_code` metadata — and stamps the outgoing request
with `X-Dataflow-Trace-Id: <trace_id>` so a downstream instrumented
service joins the same trace (the middleware adopts it on the receiving
side).

```csharp
var api = new HttpClient(new DataflowHttpHandler());          // default inner handler
var api = new HttpClient(new DataflowHttpHandler(inner));     // or wrap your own
```

The handler is best-effort: when tracing is disabled it is a pure
pass-through, existing trace ids are never clobbered, and span
bookkeeping failures never break the call.

## Database tracing

`Dataflow.Wrap` turns any ADO.NET connection into a traced one: every
command execution (reader, non-query, scalar — sync and async) emits one
`DB_QUERY` span. The name is `VERB table` derived from the command text
(`SELECT orders`, `INSERT users`, `UPDATE items` — schema-qualified names
report the bare table), `callee_package` is the database system, and the
metadata carry `db.system` plus the statement text (`db.statement`,
single-spaced, capped at 200 characters). Parameter values are never
captured.

```csharp
await using var conn = Dataflow.Wrap(new NpgsqlConnection(cs), "npgsql");
```

Driver/dialect names normalize to systems: `npgsql` → `postgres`;
`mysql`, `sqlite` and `sqlserver` (and their provider-package aliases)
pass through. All other connection and command members pass through
unchanged; when tracing is disabled the wrapper adds nothing.

## Crash capture

`Dataflow.Capture` brackets a block of code and records uncaught
exceptions before rethrowing with the original stack preserved: the span
gets status 500, the exception summary (first line of `ToString()` — type
plus message, capped at 500 characters) as `error_message`, and the raw
stack trace clipped at 8192 characters from the top in the `error.stack`
attribute. Recording lands on the ambient span when one is open, or a
synthetic `exception` span when none is.

```csharp
Dataflow.Capture(() => Charge(order));            // void
var total = Dataflow.Capture(() => ComputeTotal()); // value-returning
```

`Dataflow.CaptureUncaught()` also watches what escapes your code:
`AppDomain.UnhandledException` and `TaskScheduler.UnobservedTaskException`
are recorded as synthetic `uncaught exception` spans (parented to the
ambient trace when one is open). The subscription is idempotent and
`Dataflow.IgnoreUncaught()` removes it; unobserved exceptions are never
marked observed, so recording never changes the app's fate. The unhandled
handler runs synchronously during process death — recording stays fast
and cannot rethrow.

```csharp
Dataflow.CaptureUncaught(); // install once at startup
```

Everything here is best-effort: with the SDK disabled it is a pure
pass-through, and recording failures are swallowed so they never mask the
original exception.

## Log capture

Application log shipping with trace correlation: `Dataflow.Info` /
`Warn` / `Error` / `Debug` (and `Dataflow.Log(level, ...)` for dynamic
levels — case-insensitive `debug` / `info` / `warn` / `error`, unknown
levels degrade to info) buffer log lines in-process and POST them in
batches to `POST /api/v1/logs`. Every line is stamped with the ambient
span's trace/span ids (empty when no span is open on the calling flow),
so logs line up with traces in the dashboard's Logs tab.

```csharp
Dataflow.Info("order placed", new Dictionary<string, object>
{
    ["order_id"] = orderId,   // values are stringified
    ["channel"] = "web",
});
```

Delivery is batched and best-effort: a daemon flusher POSTs every 500ms
(or as soon as 50 lines are buffered), at most 1000 lines per request,
one retry per batch and then it is dropped and counted. The in-memory
queue holds 1024 lines — on overflow the oldest line is dropped and
counted, and logging never blocks or throws. Fields are stringified and
clipped to the server's limits (50 fields × 512 chars; messages to 8192
chars). `Dataflow.FlushLogs()` ships whatever is buffered synchronously —
call it on shutdown paths.

The base URL resolves exactly like the startup manifest: a URL-form
`DATAFLOW_ENDPOINT` is used directly, a bare `host:port` gRPC endpoint
with no `DATAFLOW_HTTP_URL` override has no derivable HTTP base and
logging stays silently off (tracing is unaffected).

### Logging bridges

`DataflowLoggerProvider` forwards Microsoft.Extensions.Logging records
into the pipeline (levels map Trace/Debug→debug, Information→info,
Warning→warn, Error/Critical→error; structured-log state entries become
stringified fields; scopes are skipped in v1). It needs no new package —
the logging abstractions ship in the ASP.NET Core shared framework:

```csharp
builder.Logging.AddProvider(new DataflowLoggerProvider());
```

For apps that do not route through `Microsoft.Extensions.Logging`,
`Dataflow.InstallLogHandler()` bridges `System.Diagnostics.Trace` instead:
`Trace.TraceError` / `TraceWarning` / `TraceInformation` and plain
`Trace.Write` ship as error / warn / info / info lines. The install is
idempotent; `Dataflow.UninstallLogHandler()` removes it.

```csharp
Dataflow.InstallLogHandler(); // once at startup, before any Trace.* calls
```

### Serilog sink

Serilog gets its own package, `Dataflow.Serilog` (the core SDK carries no
Serilog dependency): `DataflowSink` is a `Serilog.Core.ILogEventSink` that
forwards events straight into the pipeline — Verbose/Debug→debug,
Information→info, Warning→warn, Error/Fatal→error, `RenderMessage()` as
the message (string scalars render quoted, per Serilog), event properties
as stringified fields, an attached exception carried on the message. It
batches through the SDK's pipeline, so no separate batching sink package
is involved, and events keep their ambient trace/span correlation:

```csharp
Log.Logger = new LoggerConfiguration().WriteTo.Dataflow().CreateLogger();
// or: new LoggerConfiguration().WriteTo.Sink(DataflowSerilog.SerilogSink())
```

With the SDK disabled every entry point is a no-op: nothing is buffered,
nothing ships.

### NLog target

NLog gets its own package too, `Dataflow.NLog` (the core SDK carries no
NLog dependency): `DataflowTarget` is an `NLog.Targets.TargetWithLayout`
that forwards events straight into the pipeline — Trace/Debug→debug,
Info→info, Warn→warn, Error/Fatal→error, the target's layout (defaulted
to `${message}`, since the wire record carries its own timestamp and
level) as the message, message-template properties as stringified
fields, an attached exception carried on the message. It batches through
the SDK's pipeline, and events keep their ambient trace/span correlation:

```csharp
DataflowNLog.Setup(); // registers the "Dataflow" target, all levels
// or, for an explicit LogFactory:
DataflowNLog.Setup(myLogFactory);
```

Both calls create the target configuration if the factory has none
loaded, register the target under the name `Dataflow` with an
all-levels rule, and return it — call once at startup, before any
logging happens. With the SDK disabled (or a bare `host:port` endpoint
with no `DATAFLOW_HTTP_URL` override) the target is a no-op: nothing is
buffered, nothing ships.

## Route scanning

`Dataflow.Scan` is a static route scanner: it walks a source tree,
extracts the HTTP endpoints your code declares (line-oriented regexes —
no Roslyn, no build), and posts them to the Dataflow server catalog
(`POST /api/v1/catalog`, `X-Api-Key` header, capped at 1000 routes).

```sh
dotnet run --project Dataflow.Scan -- --dir ./src --service orders-api --url https://dataflow.example --api-key $DATAFLOW_API_KEY
```

Extraction coverage:

- ASP.NET Core attribute routing — a `[Route("api/[controller]")]` on the
  controller class becomes the prefix, combined with `[HttpGet]` /
  `[HttpPost]` / `[HttpPut]` / `[HttpDelete]` / `[HttpPatch]` /
  `[AcceptVerbs]` on actions. The enclosing class is tracked with a
  brace-depth scan; the handler is `ControllerClass.Action`.
- Tokens resolve the way ASP.NET does: `[controller]` → controller name
  lowercased without the `Controller` suffix (`OrdersController` →
  `orders`), `[action]` → action name lowercased. Bare `[HttpGet]` with no
  path contributes only the prefix (empty when there is none); an absolute
  action template (`[HttpGet("/healthz")]`) overrides the prefix; route
  parameters keep the `{id}` / `{id:int}` syntax. Actions with only
  `[Route]` and no method verb are skipped (the verb is unknowable).
- Minimal APIs — `app.MapGet("/path", ...)` and the `MapPost` / `MapPut` /
  `MapDelete` / `MapPatch` siblings; the handler field is empty (lambdas
  have no name, `source_file` carries the reference).
- Razor Pages (`PageModel` + `OnGet`/`OnPost`) match nothing by design.

Skipped: `bin/`, `obj/`, `.git/`, `*Tests.cs`. Routes deduplicate per file
and post as `{"service_name":"...","routes":[{"method":"GET","path":"/api/orders/{id}","handler":"OrdersController.Get","source_file":"Controllers/OrdersController.cs"}]}`.

Flags: `--dir` (default `.`), `--service` (default: the directory name),
`--url`, `--api-key`, `--print` (print the catalog JSON to stdout instead
of posting). The base URL resolves exactly like the startup manifest:
`--url` → `DATAFLOW_HTTP_URL` → URL-form `DATAFLOW_ENDPOINT` (a bare
`host:port` gRPC endpoint has no derivable HTTP base and is reported).
The API key comes from `--api-key` or `DATAFLOW_API_KEY`. Exit codes:
`0` ok, `2` usage/config error, `3` post failure.

```sh
dotnet run --project Dataflow.Scan -- --dir ./src --print   # inspect, don't post
```

## Build

```sh
dotnet build Dataflow.Sdk
dotnet test --project Dataflow.Sdk.Tests
dotnet test --project Dataflow.Serilog.Tests
dotnet test --project Dataflow.NLog.Tests
```

`protos/dataflow.proto` is the wire contract, copied next to the project
at build time (Dockerfile) and not committed; `dotnet test` uses
Microsoft.Testing.Platform (see `global.json`).

## License

See the repository root.

## Performance

The runtime overhead of every Dataflow SDK is measured with a uniform
benchmark: the same ~1 ms CPU-bound HTTP endpoint in three configs (no
instrumentation / Dataflow SDK / OpenTelemetry), one shared load driver,
spans exported live. Methodology, current numbers and reproduction steps:
The full harness is in the Dataflow monorepo `bench/`.

Measured for this SDK (ASP.NET Core + DataflowMiddleware, one child span
per request, gRPC export live): **≈ 0.5% throughput cost** on a ~14 ms
endpoint — the per-request bookkeeping disappears into the framework's
noise floor, percentiles unchanged.
