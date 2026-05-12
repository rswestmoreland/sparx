# Phase 33f - Ingest Hot Path Optimization Checkpoint

## Scope

This checkpoint applies narrow, contract-preserving ingestion hot-path changes
before the next Codex validation and benchmark run.

The goal is to reduce avoidable per-line cloning, allocations, and DB-open/write
amplification without changing product design, storage layouts, AlertV1,
DeviceStatsV1, SourceStreamStatsV1, provenance semantics, sparse row semantics,
or signal-processing behavior.

## Changes

### Window accumulator and dictionary resolution

- Added an atomic batch dictionary resolver for a line's emitted features.
- Removed the full `WindowAccumulatorV1` clone from `apply_line_v1`.
- Removed the full `FeatureDictionaryV1` clone from `apply_line_v1`.
- Kept dictionary insert behavior deterministic and input-order stable.
- Added unit coverage for batch insertion order and all-or-nothing capacity
  failure behavior.

### Durable write path

- For the default source-stream-disabled path, combined feature dictionary writes,
  open-window checkpoint writes, and cursor writes into one tenant DB closure.
- Preserved the existing source-stream-enabled ordering because source-stream
  active-observation updates can fail and should not be reordered in this pass.

### Line buffering and UTF-8 handling

- Pre-sized the active line buffer with a bounded capacity.
- Added a borrowed UTF-8 fast path for valid log lines.
- Kept lossy UTF-8 fallback for malformed log lines so malformed input remains
  non-panicking.

### Tokenizer allocation reduction

- Added an events-only tokenization entry point for ingestion and benchmarks so
  the hot path no longer allocates a copy of the message just to discard it.
- Pre-sized tokenizer event vectors.
- Changed generic key/value residual handling from owned residual strings to
  borrowed source ranges.
- Avoided one cloned key/value value string on the non-quoted common path.

### Feature emission allocation reduction

- Replaced the per-line feature accumulator `BTreeMap` with a vector-sort-coalesce
  path.
- Preserved deterministic family and feature ordering in emitted features.
- Preserved feature count coalescing behavior.

### Gzip reader allocation reduction

- Reused a gzip decoder scratch buffer inside `GzipFileReaderV1` instead of
  allocating scratch space for each chunk read.
- Did not change compressed-offset semantics or the one-byte compressed read
  guard used by the current gzip cursor model.

### Key normalization allocation reduction

- Reworked feature key normalization to avoid building an intermediate character
  vector and boundary string.
- Removed an unnecessary leading-underscore cleanup loop from tokenizer key
  normalization because the normalizer already avoids leading underscores.

## Deferred from this optimization pass

The following require separate contracts or measured follow-up because they could
change operational semantics or build characteristics:

- changing gzip cursor semantics to allow larger compressed read-ahead
- replacing or reconfiguring the gzip backend crate
- batching open-window checkpoints across multiple lines
- changing cursor persistence frequency
- adding parallel or async ingest stages
- replacing deterministic dictionary storage with a hash-first persistent model

## Files changed

- `src/features/dict.rs`
- `src/window/mod.rs`
- `src/cli/route.rs`
- `src/tokenize/generic.rs`
- `src/tokenize/mod.rs`
- `src/features/emit.rs`
- `src/ingest/reader.rs`
- `benches/tenant_device_eps.rs`
- `docs/roadmap/PHASE33F_INGEST_HOT_PATH_OPTIMIZATION_CHECKPOINT.md`
- `docs/roadmap/README.md`
- `docs/CURRENT_PLAN_CHECKLIST.md`
- `HISTORY.md`

## Required Codex validation

Run the full Rust 1.90 validation and benchmark flow after this checkpoint:

```bash
cargo fmt --check
cargo check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo bench --bench tenant_device_eps
SPARX_BENCH_TENANTS=2 SPARX_BENCH_DEVICES_PER_TENANT=10 SPARX_BENCH_FILES_PER_DEVICE=5 SPARX_BENCH_EVENTS_PER_FILE=1000 cargo bench --bench tenant_device_eps
SPARX_BENCH_DURABLE_ONESHOT=1 cargo bench --bench tenant_device_eps
SPARX_BENCH_SOURCE_STREAM=1 cargo bench --bench tenant_device_eps
SPARX_BENCH_SOURCE_STREAM=1 SPARX_BENCH_DURABLE_ONESHOT=1 cargo bench --bench tenant_device_eps
```

No Rust build, test, clippy, or benchmark result is claimed by this checkpoint.
