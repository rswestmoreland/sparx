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

## Remaining work order

1. Finish external Rust validation and EPS benchmark cleanup
   - run formatting, build, test, clippy, and benchmark checks under Rust 1.90 or newer
   - fix reported test and clippy failures before adding signal-processing code
   - record default and 100000-event ingest/detection EPS results

2. Review active docs, contracts, tests, and supporting files
   - confirm active docs match current behavior
   - keep historical checkpoint notes archived under `docs/roadmap/`
   - keep public README focused on current behavior and concise design explanations

3. Begin lean signal-processing MVP
   - add EWMA volume state primitives
   - add hour-of-week periodic volume baseline primitives
   - integrate mature periodic expected volume conservatively into existing spike/drop evaluation
   - keep sparse rows, AlertV1, DeviceStatsV1, and SourceStreamStatsV1 unchanged

4. Explore ingest and detection performance tuning
   - identify parser, sparse-row, durable-write, and detection hot spots using benchmark evidence
   - prefer small measured changes before parallel or async pipeline work
   - preserve deterministic behavior and fail-closed path handling

5. Release packaging
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
- [ ] Fresh external Rust 1.90 validation remains required after the current cleanup checkpoint.

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
- [x] EPS benchmark default workload adjusted for dense high-EPS logging.
- [ ] User-run benchmark output remains required before publishing performance claims.


## Benchmark follow-up

- Tenant/device EPS benchmark reports separate ingestion and detection metrics.
- Default workload target: 10000 events.
- Larger validation workload target: 100000 events.
- Optional durable oneshot timing remains available through `SPARX_BENCH_DURABLE_ONESHOT=1`.
- Saved benchmark reports in `validation_results/` are historical diagnostics until Codex produces a fresh green validation run from the current checkpoint.

## Validation cleanup addendum

- [x] Alert query fallback path tightened for incomplete or unreadable secondary indexes.
- [x] Task/review documents moved under `docs/roadmap/` with phase-oriented filenames.
- [x] Active docs index reconciled so `/docs` contains public-facing guides only.
- [ ] Codex must rerun formatting, check, test, clippy, default EPS, and 100000-event EPS validation on this checkpoint.
- [ ] Treat saved benchmark logs as historical diagnostics until fresh green validation is available.

## Signal-processing MVP addendum

- [x] Signal-processing MVP design contract added.
- [x] Sparse matrix plus signal-processing guide added.
- [x] Autocorrelation-lite recorded as deferred scope.
- [ ] Finish current Rust validation and EPS benchmark cleanup before implementation.
- [ ] Add EWMA volume state primitives.
- [ ] Add hour-of-week periodic volume baseline primitives.
- [ ] Integrate mature periodic expected volume into existing spike/drop evaluation.
- [ ] Measure benchmark impact before publishing performance claims.

## Ingest performance addendum

- [x] Ingest performance tuning plan added.
- [ ] Use benchmark output to identify hot spots before changing runtime architecture.
- [ ] Review parser allocation patterns, sparse-row update costs, durable write batching, and detection evaluation flow.
- [ ] Evaluate parallel or async pipeline strategies only after the single-threaded hot path is measured and stable.
