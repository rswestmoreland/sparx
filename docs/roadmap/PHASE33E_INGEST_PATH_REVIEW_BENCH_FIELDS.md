# Phase 33E - Ingest Path Review and Benchmark Field Additions

## Purpose

Review the ingestion hot path after the Rust 1.90 validation cleanup and add
non-invasive benchmark fields that make future performance analysis easier.

## Scope

- Review ingestion, parsing, tokenization, feature emission, dictionary
  resolution, sparse row accumulation, and durable oneshot costs.
- Add benchmark-only output fields for corpus shape, sparse row width, feature
  dictionary growth, and byte throughput.
- Do not change product runtime behavior, storage layouts, alert schemas,
  provenance semantics, or signal-processing behavior.

## Ingest path reviewed

The current oneshot ingestion path is:

1. Build deterministic tenant/device/file inventory.
2. Open each file through the plain-text or gzip reader abstraction.
3. Reconcile and persist cursor state.
4. Read configured chunks and assemble bounded physical lines.
5. Convert the buffered line to lossy UTF-8 text for safety.
6. Parse the syslog envelope.
7. Tokenize the message as CEF, JSON, CSV, generic key/value, or words.
8. Emit canonical feature strings and entity metadata.
9. Resolve feature strings through the per-tenant dictionary.
10. Apply counts to the active sparse window row.
11. Persist dictionary writes, active-window checkpoints, and cursor progress.
12. Finalize windows, build alerts, update baselines, persist AlertV1 objects,
    and emit sink output.

## Main bottleneck candidates

- Per-line active-window and dictionary cloning in `WindowAccumulatorV1::apply_line_v1`.
  This preserves atomic semantics but can become expensive as the active sparse
  row and dictionary grow.
- Per-line durable writes in the oneshot path. Every applied line can persist
  dictionary updates, active-window checkpoints, and cursor progress.
- Line buffering plus `String::from_utf8_lossy(...).into_owned()` for every
  buffered line, even when input is valid UTF-8.
- Tokenization allocation, especially residual `Vec<String>`, quoted-value
  copies, JSON flattening pairs, and per-token strings.
- Feature emission allocation through repeated `format!` calls and a per-line
  `BTreeMap<(u8, String), u32>` accumulator.
- Feature dictionary lookups use `BTreeMap<String, FeatureId>`, which preserves
  deterministic ordering but has `O(log n)` lookup cost and allocates on insert.
- Gzip reader offset tracking forces compressed-source byte accounting and uses
  a one-byte limited counting reader, which is safe for restart semantics but is
  likely slow for gzip-heavy workloads.

## Benchmark fields added

The tenant/device EPS benchmark now prints:

- `ingest_bytes_per_second`
- `dictionary_features`
- `total_sparse_features`
- `avg_sparse_features_per_row`
- `max_sparse_features_per_row`
- `avg_events_per_sparse_row`
- `max_events_per_sparse_row`

Existing fields retained:

- `ingest_sparse_rows`
- `durable_oneshot_elapsed_s`
- `durable_oneshot_total_eps`

## Recommended next measurements

After Codex validates this checkpoint, run:

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

The source-stream runs are optional but useful before the signal-processing MVP
because source-stream state is a default-off feature gate with separate runtime
cost.

## Optimization order recommendation

1. Measure row width and dictionary growth with the new benchmark fields.
2. Add internal counters or microbenchmarks for tokenizer allocation and feature
   emission only if row width does not explain the 100000-event slowdown.
3. Prototype an atomic in-place window apply plan that avoids cloning the full
   active window and dictionary on every line, while preserving rollback
   semantics on dictionary errors.
4. Measure batched active-window/cursor persistence for oneshot durable mode.
5. Review tokenizer allocation reductions and fast paths for common key/value
   logs.
6. Review gzip-heavy workloads separately before changing the gzip reader or
   adding an alternate decoder crate.

## Notes

No local Rust build, test, clippy, or benchmark result is claimed for this
checkpoint. Codex must revalidate because the benchmark target changed.
