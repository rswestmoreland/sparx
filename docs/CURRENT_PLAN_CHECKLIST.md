# Current Plan and Release Checklist

## Current status

- Core ingest, tokenization, sparse row construction, baselines, alert creation,
  alert indexing, operator workflows, recovery observability, hard-silence
  `V_DROP`, sharp-drop `V_DROP`, and source-stream `V_DROP` are implemented in
  the current checkpoint.
- Source-stream `V_DROP` is behind a default-off source-stream gate.
- Parser-class subjects, vendor-event-family subjects, heartbeat checks,
  maintenance calendars, cross-tenant outage correlation, source-stream-specific
  threshold knobs, and AlertV1 schema changes remain deferred.

## Completed capability areas

- repository scaffold and module layout
- config loading, validation, and CLI parsing
- Fjall-backed global and tenant DB adapters
- deterministic key builders and value encodings
- directory discovery, cursors, and plain/gzip readers
- syslog, key/value, JSON, CSV, CEF, and plaintext tokenization
- feature dictionaries, feature emission, and entity sketches
- open-window checkpointing and finalization
- DF-ring, centroid, and fixed-layout stats baselines
- scoring and AlertV1 creation
- JSONL/stdout output sink behavior and replay spool helpers
- fixture validation and end-to-end smoke coverage
- tenant purge, migrate, policy show/check, alert query/export, drill/extract
- runtime `status`, `status --json`, `oneshot`, and `run`
- recovery backlog, age, trend, replay-rate, and long-window analytics
- active `alert_idx_*` persistence and structured alert filtering
- `V_DROP` hard-silence detection for device and tenant aggregate subjects
- `V_DROP` sharp-drop detection for device and tenant aggregate subjects
- source-stream identity, catalog, stats, expected-source state, provenance,
  AlertV1 construction, open-state/dedup helpers, policy gate, runtime
  integration, and bounded diagnostics

## Remaining pre-release work

1. External Rust validation
   - run formatting, build, test, clippy, and release build checks
   - provide logs for any failures
   - fix reported failures before adding features

2. Final documentation reconciliation
   - verify active docs and contracts match implementation behavior
   - keep historical checkpoint notes archived under `docs/roadmap/`
   - ensure operator docs are sufficient for install, configure, run, validate,
     and troubleshoot workflows

3. Release packaging
   - provide example configuration
   - provide tenant-policy examples
   - provide migration and purge examples
   - provide known limitations
   - create final v1 checkpoint or release artifact

## v1 completion definition

sparx v1 is complete when the current source-stream scope is either validated or
explicitly deferred, all active docs/contracts are reconciled, external Rust
validation logs are green, and release packaging is complete.

## Pre-validation hardening addendum

- [x] Security/performance/test coverage hardening review completed.
- [x] Alert drill/extract provenance resolution hardened against traversal and unsafe paths.
- [x] Spool path construction validates filesystem components and spool inventory skips symlinks.
- [x] Plain-text runtime reading uses configured chunks and line buffering is capped.
- [x] Ingest resource cap validation added for configured CPU/memory controls.
- [ ] User-run Rust toolchain validation remains required before release packaging.

- malformed but readable log records do not crash runtime processing
- source modules have concise headers and comments at non-obvious safety boundaries
- data-facing runtime helpers return explicit errors instead of panic-prone assumptions


## Open-source metadata

- [x] Root MIT license file present.
- [x] Cargo package metadata declares MIT license and author contact.
- [x] Rust source and test files include SPDX MIT headers.
- [x] README, docs, contracts, and fixtures include author/contact/license notes.


## Benchmarking

- [x] Tenant/device EPS benchmark target added.
- [ ] User-run benchmark output remains required before publishing performance claims.
