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
```

`protos/dataflow.proto` is the wire contract, copied next to the project
at build time (Dockerfile) and not committed; `dotnet test` uses
Microsoft.Testing.Platform (see `global.json`).

## License

See the repository root.
