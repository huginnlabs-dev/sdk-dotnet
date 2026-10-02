# Changelog

## 0.7.0 — 2026-10-01

### Added

- Serilog sink, isolated in a new `Dataflow.Serilog` project so the core
  SDK carries no Serilog dependency: `DataflowSink` implements
  `Serilog.Core.ILogEventSink` and enqueues straight into the log pipeline
  (`Logs.Ship`, no level re-normalization) — Verbose/Debug→debug,
  Information→info, Warning→warn, Error/Fatal→error; `RenderMessage()` as
  the message (string scalars render quoted, per Serilog); event
  properties as stringified fields (first 50 keys, values clipped at 512
  chars, the server's limits); an attached exception carried on the
  message and clipped at 8192 chars with it. Events ride the standard
  batched shipping (1024-line drop-oldest buffer, ≤1000-line POSTs, one
  retry) with ambient trace/span correlation, and `Emit` is a no-op that
  never throws while the SDK is disabled or no HTTP base resolves.
- Registration conveniences: `DataflowSerilog.SerilogSink()` for
  `WriteTo.Sink(...)` (the SDK's static `Dataflow` class cannot receive
  extension members from another assembly) and the idiomatic
  `WriteTo.Dataflow()` on `LoggerSinkConfiguration`.
- Test suite (`Dataflow.Serilog.Tests`, xunit.v3 on Microsoft.Testing
  Platform): per-level mapping, message-template rendering, scalar
  stringification (string/bool/double/long/null), 50×512 property clamps,
  8192 message clip, exception-on-message, wire shape + `X-Api-Key` +
  trace ids against a raw IPv6-loopback `TcpListener` HTTP stub,
  1000-line batch split, and disabled / bare-host:port no-ops.

## 0.6.0 — 2026-09-30

### Added

- Application log shipping with trace correlation: `Dataflow.Info` /
  `Warn` / `Error` / `Debug` and `Dataflow.Log(level, ...)` buffer lines
  in a bounded queue (1024, drop-oldest with a dropped counter) and a
  daemon flusher POSTs them to `POST /api/v1/logs` in batches of ≤1000
  (500ms ticker / 50-line wake, 5s timeout, one retry per batch then
  drop). Every line carries a unix-ms timestamp, the wire level
  (`debug|info|warn|error`, unknown levels degrade to info), the
  stringified fields (clamped to the server's 50×512; messages to 8192
  chars), the service name and the ambient span's trace/span ids — empty
  when no span is open on the calling flow.
- `Dataflow.FlushLogs()` synchronously ships buffered lines for shutdown
  paths.
- `DataflowLoggerProvider` / `DataflowLogger`: a Microsoft.Extensions
  Logging `ILoggerProvider` that forwards records into the pipeline with
  level mapping and structured-log state as fields (scopes skipped in
  v1) — zero new package references, the abstractions ship in the
  ASP.NET Core shared framework. Register via
  `logging.AddProvider(new DataflowLoggerProvider())`.
- `Dataflow.InstallLogHandler()` / `Dataflow.UninstallLogHandler()`: an
  idempotent `System.Diagnostics.Trace` listener bridge (Trace severities
  map onto the wire vocabulary) for apps that do not route through
  Microsoft.Extensions.Logging.
- Logging is best-effort and strictly off when there is no HTTP base: a
  bare `host:port` `DATAFLOW_ENDPOINT` with no `DATAFLOW_HTTP_URL`
  override ships nothing (records are not even buffered) while tracing
  keeps working. With the SDK disabled every entry point is a no-op.
- Test suite: record/wire shape, batching, overflow, retry-then-drop,
  level mapping, clamps, both bridges and disabled no-ops against a raw
  `TcpListener` HTTP stub on the IPv6 loopback.

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
