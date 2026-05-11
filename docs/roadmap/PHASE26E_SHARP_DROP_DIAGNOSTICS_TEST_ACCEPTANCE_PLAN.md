# Phase 26e Sharp-Drop Diagnostics, Tests, and Acceptance Plan

Status: complete for planning. Documentation and contracts only.

Phase 26e locks the diagnostics, test, fixture, and acceptance plan for a later
sharp-drop implementation phase. Runtime sharp-drop detection remains inactive.

## 1. Purpose

Sharp-drop detection is a future reduced-but-nonzero `V_DROP` path. Earlier Phase 26
subphases locked the semantic model, future state model, and AlertV1 explanation shape.
This phase defines how a future implementation must prove correctness without adding
runtime behavior during Phase 26.

## 2. Non-change statement

Phase 26e does not change:

- runtime source code
- test source code
- stats/v1 encodings
- AlertV1 schema
- replay ordering or replay semantics
- recovery behavior
- active hard-silence V_DROP behavior
- active V_DROP policy behavior
- active V_DROP diagnostics behavior

## 3. Diagnostics boundary

Sharp-drop diagnostics are optional for the first runtime implementation, but any added
surface must remain bounded and low-cardinality.

Allowed aggregate diagnostic concepts:

- evaluated sharp-drop subjects
- sharp-drop candidates
- sharp-drop suppressions
- sharp-drop alerts emitted
- open sharp-drop intervals
- closed sharp-drop intervals by recovery
- closed sharp-drop intervals by hard-silence supersession
- last sharp-drop evaluation timestamp

Prometheus and status constraints:

- no device-label metrics
- no tenant/device/source-path label fanout
- no suppression-reason label cardinality
- no per-file, parser-class, or vendor-family diagnostic dimensions
- status JSON may expose bounded aggregate counts only
- `/metrics` may expose bounded aggregate counters/gauges only
- `/healthz` may include only summarized sharp-drop health when implemented

Recommended first implementation approach:

- add diagnostics after evaluator, state, alert, and runtime behavior are stable
- reuse the existing V_DROP diagnostics family shape where practical
- keep device and tenant aggregate counts as aggregate totals, not label values
- avoid emitting one metric series per subject

## 4. Deterministic evaluator fixture plan

A future implementation must add deterministic fixtures or tests for the evaluator before
or during runtime integration.

Required evaluator cases:

1. device candidate emission when mature expected lines are high and observed lines are
   reduced but nonzero
2. tenant aggregate candidate emission from summed mature per-device bucket baselines
3. zero observed activity is routed to hard silence rather than sharp drop
4. missing current observed window is routed to hard silence rather than sharp drop
5. immature expected-source state suppresses sharp drop
6. insufficient DeviceStatsV1 line_count sample count suppresses sharp drop
7. expected-line mean below the configured floor suppresses sharp drop
8. absolute line-drop below the configured floor suppresses sharp drop
9. observed/expected ratio above the configured maximum suppresses sharp drop
10. drop_ratio below the configured minimum suppresses sharp drop
11. variance gate suppresses candidates when the drop is not statistically meaningful
12. bucket-local quiet periods do not compare against busy bucket baselines
13. invalid or non-finite computed values fail closed or suppress alert construction
14. current hard-silence open state suppresses sharp-drop emission

Recommended numeric fixture shape:

- expected_lines = `100.0`
- observed_lines = `20`
- observed_expected_ratio = `0.200000`
- drop_ratio = `0.800000`
- max observed/expected ratio = `0.25`
- min drop ratio = `0.75`

The exact implementation may use integer counters and finite floating-point computations,
but test expectations must use deterministic rounding and formatting rules from the
AlertV1 explanation contract.

## 5. State and dedup test plan

A future implementation of `drop_open/*` must include tests for:

- key builder determinism for `drop_open/v1/device/<device_key>`
- key builder determinism for `drop_open/v1/tenant`
- OpenDropStateV1 encode/decode round trip
- malformed OpenDropStateV1 payload rejection
- duplicate suppression while an equivalent open drop interval exists
- no suppression after a closed-by-recovery state
- no suppression after a closed-by-hard-silence-supersession state
- recovery closure when observed activity rises above the sharp-drop threshold
- hard-silence supersession clears the open flag and sets the superseded flag
- device and tenant aggregate state do not automatically suppress each other

The implementation must not store expected/observed ratios in OpenDropStateV1. Those
values must be recomputed from finalized windows and baseline stats.

## 6. Alert construction test plan

A future implementation must include deterministic AlertV1 tests for:

- reason code `V_DROP`
- first detail `drop_kind=sharp_drop`
- ordered reason details
- fixed six-decimal formatting for ratios and thresholds
- `score_volume = clamp01(drop_ratio)`
- `score_total = score_volume`
- `score_rarity = 0.0`
- `score_drift = 0.0`
- `top_features = []`
- device current-row provenance preservation when available
- empty device provenance only when row provenance is unavailable
- empty tenant aggregate provenance for the first implementation
- sharp-drop alert id includes `sharp_drop`
- hard-silence and sharp-drop alert ids cannot collide for the same subject/window
- no `source_files` field or alternate provenance model is reintroduced

## 7. Runtime integration test plan

A future runtime integration phase must include tests for:

- `oneshot` device sharp-drop emission
- `oneshot` tenant aggregate sharp-drop emission
- `run` device sharp-drop emission
- `run` tenant aggregate sharp-drop emission
- resolved global policy disable suppresses sharp drop
- resolved tenant-policy disable suppresses sharp drop
- threshold overrides alter candidate decisions deterministically
- hard silence still takes priority in `oneshot`
- hard silence still takes priority in `run`
- open drop duplicate suppression survives tenant DB reopen where applicable
- recovery closure happens after later finalized observations
- hard-silence supersession closes matching open drop state

Runtime tests must preserve deterministic tenant, device, file, window, alert, and detail
ordering.

## 8. Diagnostics test plan

If sharp-drop diagnostics are added in the implementation phase, tests must cover:

- aggregate evaluated-subject counts
- aggregate candidate counts
- aggregate suppression counts without unbounded suppression-reason labels
- aggregate emitted-alert counts
- aggregate open-drop interval gauges
- aggregate recovery-closure counts
- aggregate hard-silence-supersession counts
- last evaluation timestamp surfacing
- status text output
- status JSON output
- `/metrics` output when enabled
- `/healthz` output when enabled

Tests must prove that no device-label metrics or per-subject Prometheus series are added.

## 9. Acceptance gates for implementation phase

A future sharp-drop implementation is not accepted until all applicable gates pass:

- contracts and docs updated for the implemented behavior
- deterministic evaluator tests added
- deterministic state/codec tests added if `drop_open/*` is implemented
- deterministic AlertV1 construction tests added
- deterministic runtime integration tests added
- diagnostics tests added if diagnostics are implemented
- hard-silence behavior remains unchanged except for documented supersession of open
  sharp-drop state
- stats/v1 DeviceStatsV1 remains the locked 68-byte layout
- AlertV1 schema remains unchanged
- AlertV1.provenance remains authoritative
- no `source_files` field is reintroduced
- replay semantics remain unchanged
- recovery behavior remains unchanged
- no device-label metrics are added
- no unbounded label cardinality is added
- all external build/test validation is performed outside this ChatGPT sandbox and results
  are reported before claiming success

## 10. Recommended Phase 27 split

Phase 26e keeps the likely Phase 27 implementation split as provisional:

- Phase 27a: evaluator primitives and deterministic evaluator tests
- Phase 27b: OpenDropStateV1, `drop_open/*`, AlertV1 construction, and dedup tests
- Phase 27c: runtime integration for run and oneshot with policy interaction tests
- Phase 27d: diagnostics, validation hardening, docs/contracts, and closeout

Do not lock Phase 27 until Phase 26f closeout confirms all Phase 26 contracts are
consistent.
