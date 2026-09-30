# Changelog

## 0.5.0 — 2026-09-30

### Added

- Crash capture: `Dataflow.Capture(action)` and `Dataflow.Capture<T>(func)`
  bracket a block of code and, on an uncaught exception, record on the
  ambient span (a synthetic `exception` span when none is open) before
  rethrowing with the original stack preserved — status 500, the exception
  summary (first line of `ToString()`, capped at 500 chars) as
  `error_message`, and the raw stack trace clipped at 8192 characters from
  the top in the `error.stack` attribute.
- `Dataflow.CaptureUncaught()` / `Dataflow.IgnoreUncaught()`: subscribe /
  unsubscribe `AppDomain.UnhandledException` and
  `TaskScheduler.UnobservedTaskException` (idempotent; never
  `SetObserved()`). Both record synthetic `uncaught exception` spans
  synchronously — the unhandled handler runs during process death and
  cannot rethrow.
- Best-effort by construction: when the SDK is disabled every entry point
  is a pure pass-through, and recording failures are swallowed so they
  never mask the original exception.

## 0.4.0 — 2026-09-30

### Added

- Static route scanner (`Dataflow.Sdk.Scan.ScanTool` + the `Dataflow.Scan`
  console project): extracts declared HTTP endpoints from C# sources and
  posts them to the server catalog via best-effort `POST /api/v1/catalog`.
  Regex-based (no Roslyn): ASP.NET Core attribute routing (class
  `[Route]` prefix combined with `[HttpGet]`/`[HttpPost]`/`[HttpPut]`/
  `[HttpDelete]`/`[HttpPatch]`/`[AcceptVerbs]` actions, enclosing class
  tracked by brace depth, `[controller]`/`[action]` tokens resolved like
  ASP.NET) and minimal APIs (`app.MapGet(...)` and siblings, empty
  handler); Razor Pages match nothing by design.
- Scan CLI flags: `--dir`, `--service` (defaults to the directory name),
  `--url`, `--api-key`, `--print` (print the catalog JSON instead of
  posting). Base URL precedence matches the startup manifest: `--url` →
  `DATAFLOW_HTTP_URL` → URL-form `DATAFLOW_ENDPOINT` (a bare host:port is
  reported and skipped).

## 0.3.0 — 2026-09-30

### Added

- HTTP client tracing: `DataflowHttpHandler` (`DelegatingHandler`) emits
  one `HTTP_CLIENT` span per outgoing call (`METHOD host/path`, host as
  `callee_package`, response `status_code`, `http.method` / `http.url`
  metadata) and stamps requests with the `X-Dataflow-Trace-Id` header so
  downstream services join the trace. Pure pass-through when disabled.
- Database tracing: `Dataflow.Wrap(DbConnection, system)` returns a
  delegating `DbConnection` whose commands emit one `DB_QUERY` span per
  execution — `VERB table` from the command text, driver name normalized
  to the database system (`npgsql` → `postgres`), `db.system` +
  single-spaced 200-char-capped `db.statement` metadata; parameter values
  are never captured.
- Test suite (`Dataflow.Sdk.Tests`, xunit.v3 on Microsoft.Testing
  Platform): statement summary/clip cases, handler tracing over a fake
  `HttpMessageHandler`, and connection/command tracing over fake ADO.NET
  classes.

### Changed

- `protos/dataflow.proto` vendored copy now carries the canonical
  `EVENT_TYPE_DB_QUERY` (and `EVENT_TYPE_LLM_CALL`) enum values.

## 0.2.0 — 2026-09-30

### Added

- Service manifest reported once at startup (framework, runtime and
  dependency inventory) via best-effort `POST /api/v1/manifest`.

## 0.1.0 — 2026-09-28

### Added

- Initial SDK: gRPC span streaming with ack-watermark replay buffer,
  AES-256-GCM payload encryption (PBKDF2 key derivation), ambient-span
  tracing scopes, ASP.NET Core middleware (`HTTP_SERVER`) and the
  `TracedHttp` convenience client.
