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

