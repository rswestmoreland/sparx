# Project History

sparx has progressed from a scaffolded prototype into a near-v1 checkpoint with
runtime ingestion, sparse row construction, baselines, alerting, operator
workflows, recovery observability, and volume-loss detection.

Current major completed areas:

- configuration and CLI plumbing
- Fjall-backed embedded storage adapters
- tenant and device discovery
- cursor tracking and plain/gzip reading
- heterogeneous tokenization
- sparse feature emission and entity sketches
- window checkpointing and finalization
- baseline updates and alert scoring
- AlertV1 persistence, indexing, query, export, drill, and extract
- output sinks and replay-spool behavior
- status, JSON status, metrics, health, oneshot, and run workflows
- recovery backlog and replay-rate observability
- hard-silence, sharp-drop, and source-stream `V_DROP` behavior

Historical checkpoint notes are archived under `docs/roadmap/`.

## Security/performance/test coverage hardening checkpoint

- Hardened alert drill/extract path resolution against traversal and unsafe provenance data.
- Hardened JSONL/spool path construction and skipped symlinked spool inventory entries.
- Added ingest resource cap validation and chunked plain-text runtime reading.
- Added targeted tests for unsafe path handling, source-stream drill resolution, spool symlinks, and config bounds.
- No Rust toolchain validation is claimed for this checkpoint.

## Codebase consistency and bad-data hardening checkpoint

- Added missing source module headers and focused runtime comments for bounded line handling.
- Replaced selected data-facing runtime invariants with explicit errors instead of panic-prone assumptions.
- Added malformed-readable-log coverage for invalid UTF-8, embedded NUL bytes, bad timestamps, malformed structured input, and overlong lines.
- Confirmed active docs and contracts no longer use historical sequencing language outside `docs/roadmap/`.
- No Rust toolchain validation is claimed for this checkpoint.


## Open-source license and author metadata checkpoint

- Added the root MIT `LICENSE` file, `NOTICE.md`, and `docs/OPEN_SOURCE_RELEASE_METADATA.md`.
- Updated Cargo package metadata to use the MIT license and author contact.
- Added consistent SPDX and copyright headers to Rust source and test files.
- Added license and author notes to README, docs, contracts, and fixture documentation.
- No Rust toolchain validation is claimed for this checkpoint.


## Tenant/device EPS benchmark checkpoint

- Added a dependency-free custom Cargo bench target for end-to-end tenant/device throughput.
- The benchmark generates deterministic multi-tenant log input and reports total EPS.
- Added documentation for workload scaling controls and release-performance interpretation.
- No Rust toolchain validation is claimed for this checkpoint.


## EPS benchmark validation fix checkpoint

- Adjusted the tenant/device EPS benchmark default workload to model dense high-EPS logging.
- Added `SPARX_BENCH_EVENTS_PER_TIMESTAMP` so sparse event-time stress runs can still be measured explicitly.
- Hardened alert query fallback behavior for incomplete or unreadable secondary indexes.
- Removed clippy-reported needless returns from the oneshot processing path.
- No Rust toolchain validation is claimed for this checkpoint; external revalidation remains required.

## Rust 1.90 validation fix checkpoint

- Addressed externally reported run-mode test compile issues.
- Applied bounded clippy cleanup and targeted allowances for stable runtime helper signatures.
- Fixed the tenant/device EPS benchmark timestamp generator so multi-file device workloads stay monotonic.
- No Rust toolchain validation is claimed for this checkpoint; external revalidation remains required.


## Benchmark metric split

- Reduced the default tenant/device EPS benchmark workload to 10000 events.
- Updated the documented larger validation workload to 100000 events.
- Split benchmark output into ingestion EPS and detection EPS metrics.
- Kept optional durable oneshot timing available through `SPARX_BENCH_DURABLE_ONESHOT=1`.

## README and validation cleanup checkpoint

- Updated the public README alerting scope with clearer rarity, drift, spike, and extreme-volume descriptions.
- Reworded README current status to describe current capabilities only.
- Removed release-validation caveat text from the public README hardening section.
- Added the missing runtime drop in the remaining alert query fallback test.
- Addressed externally reported clippy diagnostics in the end-to-end smoke test helpers.
- No Rust toolchain validation is claimed for this checkpoint; external revalidation remains required.



## Signal-processing MVP documentation checkpoint

- Added a sparse matrix plus signal-processing guide.
- Added the lean signal-processing MVP plan for EWMA volume smoothing and hour-of-week periodic volume baselines.
- Added a performance tuning plan focused on parser, sparse-row, durable-write, and detection hot paths.
- Added contracts for signal-processing baselines and deferred signal-processing candidates.
- Recorded autocorrelation-lite and frequency-domain analysis as deferred candidates.
- No runtime behavior changes are claimed for this checkpoint.

## Validation and EPS cleanup checkpoint

- Tightened alert query loading so complete secondary indexes are used directly and incomplete or unreadable indexed paths fall back to primary alert scanning.
- Moved task, validation, benchmark, and handoff notes from the public docs root into phase-oriented files under `docs/roadmap/`.
- Updated documentation indexes and the current checklist to keep `/docs` focused on public-facing guides.
- No Rust toolchain validation is claimed for this checkpoint; external Codex revalidation remains required before signal-processing implementation.

## Ingest path review and benchmark field checkpoint

- Reviewed the oneshot ingestion path for bottleneck candidates before signal-processing implementation.
- Added benchmark-only output fields for dictionary size, sparse row width, row density, and byte throughput.
- Updated benchmarking documentation and roadmap notes for the new fields.
- No product runtime behavior, storage layout, alert schema, or signal-processing behavior is changed by this checkpoint.
- No Rust toolchain validation is claimed for this checkpoint; external Codex revalidation remains required because the benchmark target changed.

## Ingest hot-path optimization checkpoint

- Added atomic batch dictionary resolution and removed full per-line accumulator and dictionary cloning from window apply.
- Added an events-only tokenizer path for runtime ingest and benchmarks.
- Reduced tokenizer residual allocations, feature emission accumulator overhead, valid UTF-8 line allocation, and gzip scratch allocation.
- Combined default source-stream-disabled line checkpoint and cursor writes into one tenant DB closure.
- Preserved storage layouts, alert schemas, provenance semantics, sparse row semantics, and signal-processing scope.
- No Rust toolchain validation is claimed for this checkpoint; external Codex revalidation and benchmarks are required.

## Performance documentation reconciliation checkpoint

- Updated README performance wording with conservative ingestion, detection, and
  durable oneshot EPS planning estimates from the retained Rust 1.90 phase33f
  benchmark report.
- Reconciled active docs and contracts so they no longer describe phase33f
  validation and benchmark work as pending.
- Added a roadmap checkpoint note for the performance documentation reconciliation.
- Updated the signal-processing MVP kickoff prompt to start from the validated performance checkpoint.
- No runtime source, test, fixture, storage layout, alert schema, or benchmark
  target changes are made by this checkpoint.
- No new Rust toolchain validation is claimed for this documentation-only
  checkpoint; the retained phase33f report remains the current validation
  baseline.

## Alert visualization and HOWTO documentation checkpoint

- Added a realistic illustrative CLI-style alert screenshot asset and linked it from the README.
- Added the annotated AlertV1 diagram asset to the repository and linked it from the README.
- Added a public `docs/HOWTO.md` covering build, log directory layout, configuration, oneshot/run usage, alert inspection, replay, and benchmarks.
- Updated documentation indexes and the current checklist to reflect the new public usage guide.
- No runtime source, tests, fixtures, storage layout, alert schema, or benchmark target changes are made by this checkpoint.
- No new Rust toolchain validation is claimed for this documentation-only checkpoint; the retained phase33f report remains the current validation baseline.

## Sparse matrix visual README checkpoint

- Added the log-ingestion-to-sparse-matrix visual to the public README near the top of the project overview.
- Updated the inspiration sentence to use "a friend's enthusiasm" wording.
- No runtime source, tests, fixtures, storage layout, alert schema, or benchmark target changes are made by this checkpoint.
- No new Rust toolchain validation is claimed for this documentation/image-only checkpoint; the retained phase33f report remains the current validation baseline.
