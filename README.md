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
