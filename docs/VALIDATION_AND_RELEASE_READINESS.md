# Validation and Release Readiness

Release readiness depends on retained Rust validation logs for the release
candidate. The phase33f checkpoint has a retained Rust 1.90 validation report
showing green formatting, check, test, clippy, and benchmark runs for the
performance baseline. Source changes after that baseline, including zlg input
support, require fresh validation before release claims.

Rust version requirement: **Rust 1.90 or newer** (repo-pinned via `rust-toolchain.toml`).

## Required validation

For each release-candidate checkpoint, run and retain logs for:

- `cargo fmt --check`
- `cargo check`
- `cargo test`
- `cargo clippy --all-targets --all-features -- -D warnings`
- release build as appropriate for the target environment
- `cargo bench --bench tenant_device_eps` for end-to-end EPS measurement

## Scenario validation

Representative validation should include:

- syslog, key/value, JSON, CSV, CEF, and plaintext fixtures
- plain-text, gzip, and zlg input paths
- restart recovery and cursor advancement
- open-window checkpoint restore/finalize behavior
- baseline update and scoring behavior
- hard-silence, sharp-drop, and source-stream `V_DROP` behavior
- alert query/export/drill/extract workflows
- status, JSON status, metrics, and health output
- malformed-readable-log runtime stability verification
- source comment and maintainability consistency review
- replay-spool success and fail-closed behavior
- tenant/device EPS benchmark output retained for release-performance comparison
- performance estimates updated only from green validation and benchmark runs

## Release readiness gates

v1 is ready only when:

- source-stream scope is completed and externally validated
- docs and contracts are reconciled
- no known contract drift remains
- DB-backed flows fail closed
- diagnostics remain bounded and low-cardinality
- release packaging and operator guides are complete
- Rust validation logs are green or all reported failures are fixed

## Security and resource-use hardening checks

Before release packaging, review these gates in addition to the Rust toolchain checks:

- drill and extract reject absolute or traversal provenance paths
- source-stream drill resolution uses `device_key` and stays under the tenant root
- JSONL and spool path helpers reject unsafe filesystem components
- spool replay inventory skips symlinked files and directories
- ingest read chunk and line/token caps are validator-enforced
- plain-text file processing uses bounded chunk and line-buffer behavior

See `roadmap/PHASE32C_SECURITY_PERFORMANCE_HARDENING_REVIEW.md` for the current hardening notes.


## Open-source release metadata

Before release, confirm that:

- `LICENSE` contains the MIT License text.
- `Cargo.toml` declares `license = "MIT"` and the author contact.
- Rust source and test files include SPDX MIT headers.
- README, documentation, and contracts identify the MIT license and author contact.


## EPS benchmark expectations

The tenant/device EPS benchmark should report separate `ingest_eps` and `detection_event_eps` values. The default validation workload is 10000 events. The larger validation workload is 100000 events:

```bash
SPARX_BENCH_TENANTS=2 SPARX_BENCH_DEVICES_PER_TENANT=10 SPARX_BENCH_FILES_PER_DEVICE=5 SPARX_BENCH_EVENTS_PER_FILE=1000 cargo bench --bench tenant_device_eps
```

Optional durable oneshot timing can be enabled with `SPARX_BENCH_DURABLE_ONESHOT=1`, but that storage-inclusive timing is not the primary split-path ingestion or detection EPS metric. The current checkpoint uses the phase33f report as the planning baseline: about 58000 to 70000 split-path ingestion EPS, 740000 to 1390000 detection event EPS, and about 3100 durable oneshot total EPS on the documented workloads.
