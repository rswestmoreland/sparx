# Next Chat Kickoff Prompt: Signal Processing MVP and Performance Review

Use this prompt to start the next sparx chat session.

```text
You are continuing work on sparx, the Sparse Matrix Log Analyzer.

Project overview

sparx is a Rust tool for Enterprise Linux that polls per-device log directories
under each tenant, tokenizes heterogeneous log formats, builds sparse
per-window feature rows, maintains per-tenant behavioral baselines, and emits
explainable alerts for analyst and customer review.

The project centers on sparse matrix log analysis:

- row: tenant/device/window slice
- column: canonical FeatureId
- value: observed count for that feature in the window

sparx does not materialize dense zero-filled rows. Omitted features are treated
as zero without being stored.

Signal-processing direction

sparx also treats finalized sparse rows as sampled signal frames. Each finalized
window is a discrete time step, each FeatureId column can be viewed as a signal
over time, and volume counts form tenant, device, and source-stream signals.

The lean signal-processing MVP is:

1. EWMA volume smoothing
2. Hour-of-week periodic volume baselines
3. Conservative mature-slot integration into existing spike/drop evaluation

Autocorrelation-lite and DFT/FFT-style analysis are deferred and must not be
implemented without a separate contract.

Current checkpoint

Start from the latest uploaded checkpoint from the previous chat. It should
include the signal-processing documentation and contracts:

- docs/SPARSE_MATRIX_AND_SIGNAL_PROCESSING.md
- docs/SIGNAL_PROCESSING_MVP_PLAN.md
- docs/INGEST_PERFORMANCE_TUNING_PLAN.md
- contracts/40_signal_processing_baselines_v0_1.md
- contracts/41_deferred_signal_processing_candidates_v0_1.md

Current validation and performance baseline

The current checkpoint carries forward a retained Rust 1.90 validation report
showing green formatting, check, test, clippy, and benchmark runs for the
phase33f performance baseline. Later README/docs-only changes did not affect
runtime validation. The zlg input support checkpoint changes source, tests,
Cargo metadata, docs, and contracts, so fresh Rust validation is required before
treating it as the new release-candidate runtime baseline.

When validation is re-run, report only results that come from actual tool
output.

Required first task

Start with a full review of the codebase, contracts, docs, tests, fixtures,
benchmarks, and validation reports. Identify any drift, stale wording,
consistency issues, validation gaps, and benchmark-result gaps. Do not modify
files until the review and plan are provided.

Review these areas especially:

- README.md
- HISTORY.md
- docs/README.md
- docs/CURRENT_PLAN_CHECKLIST.md
- docs/BENCHMARKING.md
- docs/SPARSE_MATRIX_MODEL.md
- docs/SPARSE_MATRIX_AND_SIGNAL_PROCESSING.md
- docs/SIGNAL_PROCESSING_MVP_PLAN.md
- docs/INGEST_PERFORMANCE_TUNING_PLAN.md
- docs/DEFERRED_SCOPE.md
- contracts/21_scoring_math_thresholding_v0_1.md
- contracts/33_contract_consistency_checklist_v0_1.md
- contracts/40_signal_processing_baselines_v0_1.md
- contracts/41_deferred_signal_processing_candidates_v0_1.md
- benches/tenant_device_eps.rs
- validation_results/ if present
- tests/alerts_query.rs
- tests/e2e_smoke.rs
- tests/run_mode.rs
- tests/status_check.rs

Guardrails

- ASCII-only code/comments/tests/docs.
- No cargo/build/test/rustfmt/clippy claims unless the user provides logs.
- Do not use ripgrep.
- Do not make broad refactors.
- Fix any newly reported build/test/clippy failures before adding features.
- Do not change product design without approval.
- Do not change locked storage layouts or alert schemas.
- Do not change AlertV1.provenance semantics.
- Do not reintroduce source_files.
- Keep Fjall behind src/db/.
- Preserve the single-owner embedded DB model.
- Preserve fail-closed behavior for unsafe paths and invalid storage state.
- Preserve deterministic behavior for ordering, IDs, tie-breaks, scans, output,
  replay, and alerts.
- Do not add high-cardinality metric labels.
- Do not add device-label, source-path, source-stream-id, parser-class,
  vendor-family, per-subject, or suppression-reason Prometheus labels.
- Do not change DeviceStatsV1 or SourceStreamStatsV1 layouts.
- Do not add dense per-feature periodic state.
- Do not implement autocorrelation-lite or DFT/FFT-style analysis.
- Preserve MIT license and author metadata.

Locked behavior

- Stable hash is BLAKE3 first 16 digest bytes in lowercase hex.
- AlertV1.provenance is the authoritative drilldown model.
- Device hard-silence V_DROP is active.
- Tenant aggregate hard-silence V_DROP is active.
- Device sharp-drop V_DROP is active.
- Tenant aggregate sharp-drop V_DROP is active.
- Source-stream V_DROP is active only behind the default-off source-stream gate.
- Hard silence takes priority over sharp-drop.
- observed_expected_ratio = observed_lines / expected_lines.
- drop_ratio = 1.0 - observed_expected_ratio.
- silence_open/* is dedicated to hard-silence state.
- drop_open/* is dedicated to sharp-drop state.
- replay-spool is filesystem/config based and must not open Fjall.
- replay-spool is valid only for replay-compatible file sinks; stdout must fail
  closed.

Updated plan/checklist

1. Review and reconcile current checkpoint
   - review codebase, docs, contracts, tests, fixtures, benchmarks, and saved
     validation reports
   - identify drift or stale wording
   - confirm that the retained validation report is still the latest benchmark
     baseline
   - provide plan before editing

2. Lock EWMA and periodic baseline contracts before coding
   - lock exact state shape, key families, config defaults, maturity thresholds,
     and fallback behavior
   - update contracts/docs first if new implementation details are decided

3. Begin EWMA baseline work after contract lock
   - lock exact state shape and keys before coding
   - add state primitives and encoding tests
   - do not change detection behavior in the primitives step

4. Add periodic volume baseline primitives
   - lock hour-of-week slot calculation
   - add compact fixed-layout stats state
   - add key builders and tenant DB helpers
   - add tests for maturity and fallback

5. Integrate periodic expected volume conservatively
   - use mature periodic slot only when available
   - fall back to current general baseline when immature
   - target spike, extreme volume, sharp-drop, and relevant hard-silence
     expected-volume checks
   - preserve AlertV1 schema and ratio semantics

6. Continue ingestion and detection performance review only after MVP primitives
   - use benchmark evidence before optimizing
   - review parser allocations, sparse-row map updates, durable writes, and
     detection evaluation flow
   - evaluate parallel or async pipeline strategies only after the
     single-threaded hot path is measured
   - preserve deterministic ordering and fail-closed behavior

Expected response after first review

Provide:

- concise review summary
- current status confirmation
- whether any validation blockers remain
- drift or stale wording found
- whether the benchmark results are usable
- proposed immediate implementation plan
- proposed signal-processing MVP work sequence
- exact files likely to change
- risks and open questions

Do not modify files until the review and plan are approved.
```
