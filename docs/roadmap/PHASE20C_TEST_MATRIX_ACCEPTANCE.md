# Phase 20c Release-Readiness Test Matrix and Acceptance Checklist

Phase 20c defines the external validation matrix for the current v0.1 implementation.
It is documentation-only and does not change runtime behavior, tests, persisted keys,
metrics, health output, replay behavior, recovery behavior, or scoring logic.

## Scope

This checklist is intended to be run by the user or an external validation environment
that can execute the Rust toolchain. The ChatGPT sandbox does not run the build or test
commands for this project.

Phase 20c covers:

- release-readiness validation commands
- functional test matrix by subsystem
- acceptance gates for the current implementation
- known deferred items that are not release blockers unless they are described as active
  behavior elsewhere

Phase 20c does not activate:

- new scoring behavior
- `V_DROP` / sudden loss-of-log detection
- new persisted DB keys
- new metrics or health fields
- new replay ordering or delivery semantics
- rolling or multi-anchor replay-rate history

## External validation command set

Run these from the repository root in a normal developer environment.

Required gates:

```text
cargo fmt --check
cargo test
```

Recommended stricter gate when the toolchain component is available:

```text
cargo clippy --all-targets --all-features -- -D warnings
```

Useful focused reruns after a failure:

```text
cargo test --test status_check
cargo test --test run_mode
cargo test --test oneshot_mode
cargo test --test alerts_query
cargo test --test alert_scoring
cargo test --test replay_spool
```

A release candidate is not accepted until the required gates pass in the external
environment. If the stricter lint gate is not available, record that explicitly in the
handoff notes instead of implying it passed.

## Functional test matrix

The current test suite is organized as subsystem-focused integration tests. The matrix
below records the intended release-readiness coverage and the test files that should be
used as the first evidence set.

### CLI, config, policy, and status

Coverage intent:

- CLI parsing and dispatch stay deterministic
- config validation remains fail-closed for invalid active settings
- status text and JSON output expose the active runtime and observability surface
- tenant policy commands preserve lifecycle and validation rules

Primary tests:

- `tests/cli_parse.rs`
- `tests/cli_dispatch.rs`
- `tests/config_validate.rs`
- `tests/status_check.rs`
- `tests/runtime_context.rs`
- `tests/tenant_policy.rs`

### DB layout, key encoding, and migrations

Coverage intent:

- tenant/global DB key prefixes stay stable
- tenant simple-value encodings remain deterministic
- open-window and baseline sketch encodings remain compatible
- migrations and purge workflows do not violate tenant isolation

Primary tests:

- `tests/db_keys.rs`
- `tests/db_layout.rs`
- `tests/db_global.rs`
- `tests/db_tenant.rs`
- `tests/db_tenant_cache.rs`
- `tests/db_tenant_values.rs`
- `tests/db_open_window.rs`
- `tests/db_baseline_sketch.rs`
- `tests/migrate.rs`
- `tests/tenant_purge.rs`

### Ingest, readers, tokenization, and fixtures

Coverage intent:

- directory discovery and cursors preserve deterministic file progress
- plain/gzip reader behavior remains compatible with configured inputs
- syslog envelope parsing, generic tokenizer paths, and CEF reverse parsing remain stable
- fixture validation catches format drift before it reaches scoring

Primary tests:

- `tests/ingest_discovery.rs`
- `tests/ingest_cursor.rs`
- `tests/ingest_reader.rs`
- `tests/tokenize_syslog.rs`
- `tests/tokenize_generic.rs`
- `tests/tokenize_cef.rs`
- `tests/fixture_validate.rs`

### Feature emission, baselines, windows, and scoring

Coverage intent:

- feature dictionaries and emitted features remain tenant-scoped and deterministic
- entity sketches remain bounded and stable
- window aggregation/finalization preserves sparse row semantics
- DF-ring, centroid, fixed-layout stats, and scoring behavior remain aligned
- `V_SPIKE` / `V_EXTREME` remain active volume reasons
- `V_DROP` remains planned future work and must not be asserted as active behavior

Primary tests:

- `tests/features_dict.rs`
- `tests/features_emit.rs`
- `tests/features_sketch.rs`
- `tests/window_aggregator.rs`
- `tests/baseline_df_ring.rs`
- `tests/baseline_centroid_stats.rs`
- `tests/alert_scoring.rs`

### Alert object, query, export, drill, and extract

Coverage intent:

- `AlertV1` remains the authoritative alert object
- `AlertV1.provenance: Vec<FileSpanV1>` remains the authoritative drilldown field model
- secondary alert indexes accelerate query/list/export only when coverage is complete
- mixed-history and older tenants fall back to primary scans correctly
- drill/extract uses retained provenance and does not reintroduce `source_files`

Primary tests:

- `tests/alerts_query.rs`
- `tests/alert_drill_extract.rs`

### Output, recovery, replay, run, and oneshot

Coverage intent:

- jsonl sink behavior remains deterministic
- failed jsonl writes spool safely and fail closed when required
- manual `replay-spool` remains filesystem/config based and does not open Fjall
- automated bounded replay remains deterministic
- global and per-tenant recovery observability stays consistent across status, metrics,
  and health views
- replay-rate short-window and long-window analytics remain analytic-only

Primary tests:

- `tests/sink_output.rs`
- `tests/replay_spool.rs`
- `tests/run_mode.rs`
- `tests/oneshot_mode.rs`
- `tests/status_check.rs`

### End-to-end smoke

Coverage intent:

- the integrated ingest -> tokenize -> feature -> baseline/scoring -> alert path still
  produces deterministic results for the scoped fixture flow

Primary tests:

- `tests/e2e_smoke.rs`

## Manual operator acceptance checks

These checks are not substitutes for automated tests. They are release-readiness smoke
checks for the operator-facing surface.

Required operator checks before tagging a release candidate:

1. `sparx --help` or the project-equivalent help path shows the expected command surface.
2. `sparx check` rejects invalid active config and accepts a minimal valid config.
3. `sparx status --json` emits valid JSON and includes the active recovery observability
   fields.
4. `sparx replay-spool` fails closed for unsupported stdout replay sinks.
5. alert query/export/drill/extract flows preserve deterministic ordering and provenance.
6. `/healthz` and `/metrics`, when enabled, expose the same active recovery signal family
   represented by status.

## Release acceptance gates

A Phase 20 release candidate is acceptable only when all required gates below are true:

- external `cargo fmt --check` passes
- external `cargo test` passes
- clippy result is either passing or explicitly recorded as not run due to missing local
  component/tooling
- no active contract claims behavior that is absent from source/tests
- no source/test/doc path exceeds the project path-length guardrail
- README, docs, contracts, and phase history agree on the active phase and next phase
- Contract 06 remains a continuity-only filename while its content identifies the active
  Fjall-backed embedded DB topology
- Contract 30 uses the active `metrics/v1/counter/<name>` and `metrics/v1/gauge/<name>`
  key forms for global metrics persistence
- derived replay-rate values are documented as output values, not independent persisted
  DB keys
- `AlertV1.provenance` remains authoritative for drill/extract workflows
- `V_DROP` is documented as planned future silence detection and is not described as active
  current scoring behavior
- no new runtime behavior is introduced during Phase 20 without a separate scope lock

## Known deferred items that are not Phase 20 release blockers

These remain intentionally deferred after Phase 20d:

- `V_DROP` / sudden loss-of-log scoring implementation
- richer multi-anchor or rolling replay-rate history
- live multi-process DB administration
- optional archive storage beyond the scoped current runtime
- broader deployment packaging beyond the current Enterprise Linux-oriented project shape
- new alert categories or health-alert objects beyond the active `AlertV1` scoring path

## Phase 20d closeout result

Phase 20d closed the release-readiness audit by reconciling this checklist against the
final Phase 20 docs/contracts state and packaging the release-readiness checkpoint.
Phase 20d remained documentation/checkpoint focused and did not change runtime source,
tests, persisted keys, metrics, health output, replay behavior, recovery behavior, or
scoring logic.
