# Rust 1.90 Validation Fix Review

This note records the bounded fixes made after external Rust 1.90 validation
reported build/test/clippy and EPS benchmark failures.

## Reported failures

External validation under Rust 1.90 confirmed that formatting and `cargo check`
passed, but the following items still failed:

- `cargo test` failed to compile `tests/run_mode.rs` because a runtime handle
  used for tenant DB access was not mutable in two tests, while two other tests
  had unnecessary mutable bindings.
- `cargo clippy --all-targets --all-features -- -D warnings` reported style and
  maintainability warnings promoted to errors.
- `cargo bench --bench tenant_device_eps` failed before producing EPS because
  generated benchmark files restarted timestamps per file, causing runtime window
  finalization to reject older next-window timestamps.

## Fixes applied

The fixes are intentionally narrow:

- Corrected mutable runtime bindings in run-mode tests.
- Made the EPS benchmark generate monotonically increasing timestamps across
  files for each device.
- Boxed large enum variants for oneshot sinks and gzip readers to reduce enum
  size.
- Added a raw key/value pair type alias for DB scan return values.
- Replaced simple clippy-reported patterns with clearer forms where behavior is
  unchanged.
- Added targeted clippy allowances only for stable functions whose argument lists
  reflect existing runtime contracts.
- Preserved runtime, replay, AlertV1, DeviceStatsV1, SourceStreamStatsV1,
  storage, and source-stream semantics.

## Required revalidation

This checkpoint still requires external Rust toolchain validation. Run:

```bash
cargo fmt --check
cargo check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo bench --bench tenant_device_eps
SPARX_BENCH_TENANTS=4 SPARX_BENCH_DEVICES_PER_TENANT=16 SPARX_BENCH_FILES_PER_DEVICE=2 SPARX_BENCH_EVENTS_PER_FILE=5000 cargo bench --bench tenant_device_eps
```

Save the results under `validation_results/` and do not commit build artifacts,
temporary benchmark roots, or generated tenant/log data.
