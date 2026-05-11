# Phase 23c - V_DROP Candidate Evaluator

Phase 23c adds the first deterministic missing-window evaluator for future `V_DROP`
activation. It uses the expected-source state written by Phase 23b and decides whether a
subject is a candidate for a future sudden loss-of-log alert.

This phase does not emit alerts. It only creates a pure in-memory candidate/suppression
result that later phases can feed into `AlertV1` construction and open-silence dedup
state.

## Implemented scope

Phase 23c adds:

- `VDropEvaluationConfigV1`
- `VDropCandidateV1`
- `VDropSuppressionReasonV1`
- `VDropEvaluationV1`
- `evaluate_vdrop_candidate_v1(...)`

The evaluator accepts:

- tenant id
- subject key
- optional expected-source state
- optional open-silence state
- evaluation config

The evaluator returns either:

- a deterministic candidate with reason-detail fields, or
- a deterministic suppression reason

## Candidate rule

A hard-silence candidate is produced only when all of the following are true:

- expected-source state exists
- subject kind is device or tenant
- window size is nonzero
- last-seen window bounds are valid
- bucket is in the active 0..47 baseline bucket range
- mature window count is not greater than observed window count
- mature window count meets the configured maturity floor
- no matching open-silence state supplied to the evaluator is already open
- evaluation timestamp is later than the last seen window end timestamp
- missed windows meet the configured minimum missed-window threshold
- expected line volume meets the configured minimum expected-line floor

The evaluator is fail-closed. Missing state, invalid state, immature state, low expected
activity, timestamp inversions, counter inversions, invalid buckets, invalid window sizes,
and existing open-silence state all suppress candidate creation.

## Deterministic derived fields

The evaluator derives:

- `window_start_ts_i64` from `last_seen_window_end_ts_i64`
- `expected_windows_missed_u64` by flooring elapsed seconds over the subject window size
- `window_end_ts_i64` from the last-seen end plus the full missed-window interval
- `expected_lines_u64` from last observed lines multiplied by missed windows
- `observed_lines_u64` as zero for hard silence
- `drop_ratio_f32` as `1.0` for hard silence

The Phase 23c evaluator intentionally uses the compact expected-source state available in
Phase 23b. It does not introduce a larger per-bucket activity model. A future phase may
replace or augment this expected-lines estimate after the core hard-silence path is green.

## Reason details

Candidate reason details are emitted in deterministic contract order:

1. `subject_kind`
2. `tenant_id`
3. `device_key` for device subjects only
4. `window_start_ts`
5. `window_end_ts`
6. `last_seen_ts`
7. `expected_windows_missed`
8. `expected_lines`
9. `observed_lines`
10. `drop_ratio`
11. `bucket`

These are candidate details only. A later phase must map them into a `ReasonV1` with code
`V_DROP` during `AlertV1` construction.

## Tests

Phase 23c adds focused tests for:

- device hard-silence candidate creation
- tenant aggregate hard-silence candidate creation
- deterministic reason-detail ordering
- immature subject suppression
- not-silent suppression
- insufficient missed-window suppression
- missing expected-source state suppression
- invalid bucket suppression
- counter-inversion suppression
- matching open-silence suppression
- low expected-activity suppression

## Explicit non-goals

Phase 23c does not add:

- `AlertV1` construction for `V_DROP`
- persisted `V_DROP` alerts
- writes to `silence_open/*`
- runtime scanning for missing windows
- metrics or health fields
- sharp-drop detection
- maintenance-window suppression
- recovery or replay behavior changes

## Next phase

Phase 23d later added deterministic `V_DROP` `AlertV1` construction and open-silence
dedup state helpers. Phase 23e later activated first runtime hard-silence V_DROP integration and operator surfacing. Phase 23f later closed the first hard-silence path with validation hardening. Phase 25a later added the V_DROP config and tenant-policy surfaces. Phase 25b through Phase 25d later completed policy resolution, diagnostics, and diagnostics validation. Phase 26a now scopes future sharp-drop detection planning.


Phase 24 later locked V_DROP policy controls and diagnostics scope as planning-only work.
