# Changelog

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
