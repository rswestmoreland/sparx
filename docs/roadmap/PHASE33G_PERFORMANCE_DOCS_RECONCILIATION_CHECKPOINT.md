# Phase 33g - Performance Documentation Reconciliation Checkpoint

## Scope

This checkpoint reconciles public documentation, contracts, and roadmap status
after the phase33f Rust 1.90 validation and benchmark pass was reported green.
It does not change runtime code, storage layouts, alert schemas, tests, fixtures,
benchmarks, or signal-processing behavior.

## Validation status carried forward

The retained phase33f validation report records these passing commands:

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

Because this checkpoint changes documentation only, no new Rust validation is
claimed here. A later release-candidate checkpoint should repeat validation if
source, tests, benches, docs, or contracts change again.

## Performance estimates documented

The README and benchmarking guide now include conservative planning estimates
from the retained phase33f report:

- about 58000 to 70000 split-path ingestion events per second
- about 740000 to 1390000 detection events per second over finalized sparse rows
- about 3100 storage-inclusive durable oneshot events per second on the default workload

These values are documented as planning estimates from one modest single-node
Linux validation environment, not guaranteed throughput.

## Drift addressed

- The current plan no longer says Rust 1.90 validation and benchmarks are still
  pending for the current checkpoint.
- The validation/readiness guide now points at retained green validation logs
  while still requiring repeat validation for future release candidates.
- The ingest performance tuning plan now uses phase33f as the current baseline
  instead of describing validation as a prerequisite.
- The contracts index and README now distinguish current retained validation
  from future release-candidate validation requirements.
- The signal-processing kickoff prompt now starts from the validated performance
  checkpoint instead of stale failing-validation language.

## Files changed

- `README.md`
- `HISTORY.md`
- `docs/BENCHMARKING.md`
- `docs/CURRENT_PLAN_CHECKLIST.md`
- `docs/INGEST_PERFORMANCE_TUNING_PLAN.md`
- `docs/VALIDATION_AND_RELEASE_READINESS.md`
- `docs/roadmap/PHASE33G_PERFORMANCE_DOCS_RECONCILIATION_CHECKPOINT.md`
- `docs/roadmap/PHASE34_SIGNAL_PROCESSING_MVP_KICKOFF_PROMPT.md`
- `docs/roadmap/README.md`
- `contracts/INDEX.md`
- `contracts/README.md`

## Next recommended step

Begin the lean signal-processing MVP with a review-first pass that locks exact
EWMA and hour-of-week state shape, key families, maturity thresholds, and
fallback behavior before coding.
