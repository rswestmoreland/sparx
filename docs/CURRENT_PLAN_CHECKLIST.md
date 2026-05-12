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

1. Review active docs, contracts, tests, and supporting files
   - confirm active docs match current behavior after the Rust 1.90 validation
     and performance checkpoint
   - keep historical checkpoint notes archived under `docs/roadmap/`
   - keep public README focused on current behavior, concise design
     explanations, and conservative performance estimates

2. Begin lean signal-processing MVP
   - add EWMA volume state primitives
   - add hour-of-week periodic volume baseline primitives
   - integrate mature periodic expected volume conservatively into existing spike/drop evaluation
   - keep sparse rows, AlertV1, DeviceStatsV1, and SourceStreamStatsV1 unchanged

3. Continue ingest and detection performance tuning only when benchmark evidence supports it
   - treat phase33f hot-path results as the current performance baseline
   - prefer small measured changes before parallel or async pipeline work
   - preserve deterministic behavior and fail-closed path handling

4. Release packaging
   - provide example configuration
   - provide tenant-policy examples
   - provide migration and purge examples
   - provide known limitations
   - maintain public HOWTO instructions for build, layout, run, alerts, replay, and benchmarks
   - create final v1 checkpoint or release artifact

## v1 completion definition

sparx v1 is complete when the current source-stream scope is either validated or
explicitly deferred, all active docs/contracts are reconciled, Rust validation
logs remain green for the release candidate, and release packaging is complete.

## Pre-validation hardening addendum

- [x] Security/performance/test coverage hardening review completed.
- [x] Alert drill/extract provenance resolution hardened against traversal and unsafe paths.
- [x] Spool path construction validates filesystem components and spool inventory skips symlinks.
- [x] Plain-text runtime reading uses configured chunks and line buffering is capped.
- [x] Ingest resource cap validation added for configured CPU/memory controls.
- [x] Rust 1.90 formatting, check, test, and clippy validation was reported
  green for the phase33f checkpoint.
- [ ] Repeat Rust validation for any later release-candidate checkpoint that
  changes source, tests, benches, docs, or contracts.
- [x] Malformed but readable log records do not crash runtime processing.
- [x] Source modules have concise headers and comments at non-obvious safety boundaries.
- [x] Data-facing runtime helpers return explicit errors instead of panic-prone assumptions.


## Open-source metadata

- [x] Root MIT license file present.
- [x] Cargo package metadata declares MIT license and author contact.
- [x] Rust source and test files include SPDX MIT headers.
- [x] README, docs, contracts, and fixtures include author/contact/license notes.


## Benchmarking

- [x] Tenant/device EPS benchmark target added.
- [x] EPS benchmark default workload adjusted for dense high-EPS logging.
- [x] Benchmark output recorded for default, 100000-event, durable oneshot,
  source-stream, and source-stream durable oneshot runs.


## Benchmark follow-up

- Tenant/device EPS benchmark reports separate ingestion and detection metrics.
- Default workload target: 10000 events.
- Larger validation workload target: 100000 events.
- Optional durable oneshot timing remains available through `SPARX_BENCH_DURABLE_ONESHOT=1`.
- Current planning estimates from the phase33f report are roughly 58000 to
  70000 split ingestion EPS, 740000 to 1390000 detection event EPS, and about
  3100 durable oneshot total EPS on the documented validation workloads.

## Validation cleanup addendum

- [x] Alert query fallback path tightened for incomplete or unreadable secondary indexes.
- [x] Task/review documents moved under `docs/roadmap/` with phase-oriented filenames.
- [x] Active docs index reconciled so `/docs` contains public-facing guides only.
- [x] Formatting, check, test, clippy, default EPS, 100000-event EPS, durable
  oneshot, source-stream, and source-stream durable oneshot validation were
  reported green for phase33f.
- [ ] Repeat the validation flow after this documentation reconciliation
  checkpoint if required by release process.

## Signal-processing MVP addendum

- [x] Signal-processing MVP design contract added.
- [x] Sparse matrix plus signal-processing guide added.
- [x] Autocorrelation-lite recorded as deferred scope.
- [x] Rust validation and EPS benchmark cleanup completed for the phase33f checkpoint.
- [ ] Add EWMA volume state primitives.
- [ ] Add hour-of-week periodic volume baseline primitives.
- [ ] Integrate mature periodic expected volume into existing spike/drop evaluation.
- [ ] Measure benchmark impact before publishing performance claims.

## Ingest performance addendum

- [x] Ingest performance tuning plan added.
- [x] Review parser allocation patterns, sparse-row update costs, durable write batching, and detection evaluation flow.
- [x] Add benchmark fields for dictionary size, sparse row width, row density, and byte throughput.
- [x] Apply narrow ingest hot-path optimizations for cloning, allocation, and default durable write overhead.
- [x] Rust 1.90 validation and benchmarks were reported green after ingest
  hot-path changes.
- [x] New benchmark output compared against the Phase 33d/33e baseline.
- [ ] Evaluate parallel or async pipeline strategies only after the single-threaded hot path is measured and stable.

## Public usage documentation addendum

- [x] README includes illustrative alert CLI and AlertV1 diagram assets.
- [x] Public HOWTO added for build, tenant/device log layout, configuration, oneshot/run, status, alert inspection, replay, and benchmark usage.
- [ ] Repeat validation flow if release process requires a fresh check after documentation asset changes.
