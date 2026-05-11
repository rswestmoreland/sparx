# Phase 31f - Source-stream diagnostics, validation, and closeout

Status: complete as a diagnostics, validation, and closeout checkpoint.

## Scope

Phase 31f closes the Phase 31 source-stream V_DROP sequence by adding bounded
operator diagnostics for the source-stream subject family that became active behind the
default-off gate in Phase 31e.

This phase keeps the existing runtime behavior and only surfaces aggregate and
per-tenant source-stream diagnostic counts. It does not add per-source, per-path,
parser-class, vendor-family, or device-label metrics.

## Implementation summary

Phase 31f adds source-stream V_DROP diagnostics to the existing status, JSON status,
Prometheus metrics, and health output paths:

- source-stream gate state
- source-stream subjects tracked
- source-stream open hard-silence subjects
- source-stream open sharp-drop subjects
- source-stream evaluated subjects total
- source-stream candidates total
- source-stream suppressed candidates total
- source-stream alerts emitted total
- source-stream last evaluation timestamp

The aggregate values are surfaced globally. Per-tenant values are bounded to the existing
`tenant_id` dimension only. The implementation does not include source path,
source_stream_id, parser-class, vendor-family, device, file, or suppression-reason labels.

Phase 31f also fixes a small Phase 31e typo in optional metric formatting inside
`src/observability.rs` before extending the diagnostics surface.

## Files changed

Source and tests:

- `src/observability.rs`
- `src/cli/route.rs`
- `tests/status_check.rs`
- `tests/oneshot_mode.rs`
- `tests/run_mode.rs`

Documentation and contracts:

- `README.md`
- `PHASE_HISTORY.txt`
- `docs/README.md`
- `docs/CURRENT_PLAN_CHECKLIST.md`
- `docs/PHASE31E_SOURCE_STREAM_RUNTIME_INTEGRATION.md`
- `docs/PHASE31F_SOURCE_STREAM_DIAGNOSTICS_VALIDATION_CLOSEOUT.md`
- `contracts/10_metrics_health_v0_1.md`
- `contracts/11_mvp_milestones_tests_v0_1.md`
- `contracts/33_contract_consistency_checklist_v0_1.md`
- `contracts/34_health_silence_detection_v0_1.md`
- `contracts/36_vdrop_policy_diagnostics_scope_v0_1.md`
- `contracts/38_vdrop_richer_subject_scope_v0_1.md`
- `contracts/39_source_stream_vdrop_implementation_plan_v0_1.md`
- `contracts/README.md`
- `contracts/INDEX.md`

## Preserved boundaries

Phase 31f does not change:

- source-stream policy/config schema
- tenant-policy schema
- AlertV1 schema
- AlertV1 provenance semantics
- DeviceStatsV1 layout
- SourceStreamStatsV1 layout
- replay behavior
- recovery behavior
- alert query/export/drill/extract behavior
- hashed-fallback FeatureId behavior
- parser-class or vendor-event-family behavior

## Validation notes

The checkpoint includes deterministic tests that cover:

- empty status source-stream diagnostic defaults
- JSON status source-stream aggregate and tenant diagnostics
- oneshot source-stream hard-silence status diagnostics after enabled runtime evaluation
- metrics output carrying bounded source-stream diagnostic names
- health output carrying bounded source-stream diagnostic fields

No local cargo/build/test/rustfmt/clippy run was performed in this sandbox. Rust validation
must be performed by the user in the normal local environment.

## Closeout status

Phase 31 is now complete through:

- Phase 31a source-stream identity/catalog/stats-state primitives
- Phase 31b source-stream evaluator primitives
- Phase 31c source-stream AlertV1 construction and open-state/dedup primitives
- Phase 31d source-stream policy/config gating, default disabled
- Phase 31e source-stream runtime integration behind the default-off gate
- Phase 31f source-stream diagnostics, validation, and closeout

Recommended next step: begin a final hardening and release-readiness review phase before
calling sparx v1 complete.
