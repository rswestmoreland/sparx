# Phase 28 Post-Sharp-Drop Review and Next-Scope Plan

Status: complete as a documentation-only review and planning checkpoint.

Phase 28 follows the Phase 27d sharp-drop diagnostics, validation, and closeout
checkpoint. It does not add runtime behavior, tests, persisted keys, config fields,
metrics, replay behavior, recovery behavior, or alert schema changes.

No local cargo/build/test/rustfmt run was performed in this sandbox.

## Review basis

The review used the Phase 27d checkpoint tree at a source-map and documentation level:

- root README and phase history
- docs and current plan/checklist
- contracts, especially the V_DROP, scoring, health/silence, diagnostics, and sharp-drop contracts
- source modules under `src/`, with focus on runtime routing, alert construction, DB keys/state,
  V_DROP state, metrics/observability, config, and tenant policy
- tests under `tests/`, with focus on V_DROP, status, run, oneshot, alert scoring, and DB state tests

## Current active behavior after Phase 27d

The following behavior is active and should be treated as current truth:

- Fjall remains the active embedded DB backend behind `src/db/` adapter boundaries.
- Primary `AlertV1` storage remains authoritative for alert show/export/drill workflows.
- Secondary `alert_idx_*` persistence and query acceleration remain active current behavior.
- `AlertV1.provenance: Vec<FileSpanV1>` remains the only authoritative drilldown field model.
- Hard-silence `V_DROP` is active for device and tenant aggregate subjects.
- Sharp-drop `V_DROP` is active for device and tenant aggregate subjects.
- Sharp-drop alerts use `V_DROP` with deterministic `drop_kind=sharp_drop` reason detail.
- Hard silence remains authoritative and suppresses or supersedes matching sharp-drop intervals.
- `silence_open/*` remains dedicated to hard-silence dedup state.
- `drop_open/*` remains dedicated to sharp-drop dedup state.
- V_DROP policy controls remain active through global config and tenant-policy overrides.
- V_DROP diagnostics remain bounded and low-cardinality.
- Open-silence and open-drop diagnostic counts are surfaced through status, JSON status,
  Prometheus metrics, and health output.

## Drift and stale wording reconciliation

Phase 28 found no deliberate runtime drift to correct.

The main issue was historical wording left in older phase sections that still described
sharp-drop as deferred. Those statements were correct at the time of the historical phase,
but they are no longer correct when read as a current-status summary. Phase 28 reconciles
that wording by adding later-completed notes instead of rewriting historical phase intent.

Current status after reconciliation:

- sharp-drop runtime detection is active through Phase 27d
- richer subject scopes remain deferred
- external heartbeat checks remain deferred
- maintenance-window calendars remain deferred
- suppression-reason cardinality metrics remain deferred
- AlertV1 schema changes remain deferred
- replay and recovery behavior changes remain deferred

## Remaining deferred scope

The following items are intentionally still out of scope after Phase 28:

1. Richer V_DROP subject scopes
   - per-file or source-path silence/drop detection
   - parser-class silence/drop detection
   - vendor-event-family silence/drop detection

2. External and calendar-aware health models
   - external heartbeat or reachability checks
   - planned maintenance-window calendars
   - cross-tenant outage correlation

3. Policy and diagnostic expansions
   - sharp-drop-specific config fields
   - sharp-drop-specific tenant-policy fields
   - suppression-reason label cardinality metrics
   - per-subject Prometheus series
   - device-label Prometheus metrics

4. Alert and storage expansions
   - AlertV1 schema changes
   - historical alert rewrites
   - DeviceStatsV1 layout changes
   - replay behavior changes
   - recovery behavior changes
   - tenant aggregate sharp-drop provenance sampling beyond the current empty-provenance rule

## Recommended next roadmap

The next major work should remain scope-first. Do not add runtime code for richer subject
health until the subject, state, cardinality, false-positive controls, tests, and operator
surfaces are locked.

Recommended next phase:

- Phase 29: richer V_DROP subject-scope planning and contract lock

Recommended Phase 29 sequence:

- 29a Review and validation feedback gate
  - accept any user-provided local cargo/fmt/test results first
  - fix reported build/test failures before new scope work
  - otherwise proceed as documentation-only planning

- 29b Subject-scope decision matrix
  - compare source-path, parser-class, and vendor-event-family subject models
  - evaluate cardinality, rotation behavior, expected-volume quality, provenance quality,
    and operator usefulness
  - recommend the first richer subject only after this matrix is complete

- 29c Selected subject semantic contract
  - define hard-silence and sharp-drop semantics for the selected subject type
  - define expected-vs-observed source of truth
  - define maturity gates and false-positive controls

- 29d State and dedup model
  - define tenant DB key prefixes and value encodings only if required
  - avoid changing existing `silence_open/*`, `drop_open/*`, or DeviceStatsV1 encodings unless
    explicitly approved

- 29e Alert/explanation and diagnostics plan
  - preserve AlertV1 schema and provenance authority
  - keep diagnostics bounded and low-cardinality
  - prohibit device-label, source-path-label, and per-subject Prometheus fanout

- 29f Closeout and implementation handoff
  - update docs/contracts/checklist/history
  - package checkpoint
  - recommend an implementation phase only after the contract is approved

## Phase 28 completion criteria

Phase 28 is complete when:

- the current plan/checklist records Phase 28 as a documentation-only review checkpoint
- README and docs README describe Phase 27d as active current behavior
- stale current-status wording around sharp-drop deferral is reconciled
- contracts record Phase 28 as a review and roadmap checkpoint without changing active contracts
- no source files or tests are changed
- a checkpoint zip is produced
