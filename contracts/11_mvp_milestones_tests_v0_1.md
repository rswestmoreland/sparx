# MVP Milestones and Tests Contract v0.1

This contract defines the v1 readiness milestones and the test/validation areas
that must remain covered before release.

## Capability milestones

1. Repository, configuration, and CLI foundation
   - deterministic config loading and validation
   - config-free command behavior where applicable
   - fail-closed DB-backed command behavior

2. Storage and runtime foundation
   - Fjall-backed global and tenant DB adapters behind `src/db/`
   - deterministic key builders and binary encodings
   - tenant DB handle cache and lifecycle controls
   - migration and purge workflows

3. Ingest and tokenization
   - tenant/device discovery
   - cursor tracking
   - plain-text and gzip readers
   - syslog, key/value, JSON, CSV, CEF, and plaintext fallback tokenization

4. Sparse row and baseline pipeline
   - feature dictionaries
   - canonical feature emission
   - entity sketches
   - open-window checkpoints
   - finalized sparse rows
   - DF-ring, centroid, and fixed-layout stats baselines

5. Alerting and operator workflows
   - scoring and `AlertV1` construction
   - secondary `alert_idx_*` persistence
   - alert query/search/show/export
   - alert drill/extract using `AlertV1.provenance`
   - structured category/entity filters with backward-safe primary-scan fallback

6. Output and recovery
   - JSONL and stdout sink behavior
   - durable spool handling for replay-compatible sinks
   - bounded deterministic replay
   - recovery backlog, age, trend, replay-rate, and long-window analytics

7. Runtime operation
   - `status`
   - `status --json`
   - `oneshot`
   - `run`
   - `/metrics`
   - `/healthz`
   - tenant policy show/check

8. Volume-loss detection
   - expected-source state
   - hard-silence `V_DROP` for device and tenant aggregate subjects
   - sharp-drop `V_DROP` for device and tenant aggregate subjects
   - source-stream `V_DROP` behind the default-off source-stream gate
   - bounded low-cardinality diagnostics

## Definition of done

- alerts can be emitted within the configured latency target
- restarts preserve offsets, open windows, and baselines
- tenant purge deletes tenant-owned DB, alert, and spool directories
- alert objects include stable scores, reasons, top features where applicable,
  entity sketches where applicable, and provenance spans
- status and enabled endpoints expose the implemented runtime, process, schema,
  run-cycle, recovery, and volume-loss diagnostic surface
- `run` and `oneshot` with `output.sink=jsonl` spool live write failures and
  attempt bounded deterministic replay passes without hiding unrecoverable
  delivery errors
- active diagnostics remain bounded and low-cardinality
- source-stream behavior remains disabled by default unless explicitly enabled
- malformed but readable log data remains bounded and does not crash runtime processing
- data-facing runtime invariants use explicit error paths where practical
- parser-class and vendor-event-family volume-loss subjects remain deferred

## Required test groups

- CLI parsing and dispatch
- config validation and precedence
- tenant policy rendering, validation, and inherited defaults
- global and tenant DB key formats
- value encoding roundtrips
- Fjall adapter and layout behavior
- tenant cache lifecycle behavior
- ingest discovery and cursor behavior
- plain-text and gzip reader behavior
- tokenizer behavior for supported formats
- feature dictionary, feature emission, and sketch behavior
- window aggregation and checkpoint behavior
- baseline update and scoring behavior
- alert object, secondary index, query, export, drill, and extract behavior
- output sink and replay-spool behavior
- runtime `status`, `oneshot`, and `run` behavior
- hard-silence, sharp-drop, and source-stream `V_DROP` behavior
- status, JSON status, Prometheus metrics, and health output
- fixture validation and end-to-end smoke behavior
- malformed-readable-log stability behavior
- source comment and maintainability consistency checks
- tenant/device EPS benchmark for end-to-end throughput tracking

## External release validation

The chat sandbox does not run the Rust toolchain. Release validation requires
user-provided logs for:

- `cargo fmt --check`
- `cargo check`
- `cargo test`
- `cargo clippy -- -D warnings`
- release build for the target environment
- tenant/device EPS benchmark output for release-performance comparison

Any reported failures must be fixed before new features are added.

## Open-source release metadata

Release validation must confirm:

- the root `LICENSE` file contains the MIT License text
- `Cargo.toml` declares `license = "MIT"` and the author contact
- Rust source and test files carry SPDX MIT headers
- README, docs, contracts, and fixtures include license and author references
