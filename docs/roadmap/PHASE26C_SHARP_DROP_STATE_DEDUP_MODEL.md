# Phase 26c - Sharp-Drop State and Dedup Model Decision

Status: planning and contract-scoping only. No runtime code is active for sharp-drop
detection at this checkpoint.

## 1. Purpose

Phase 26c locks the future state and dedup model for sharp-drop detection before any
runtime implementation. Phase 26b locked the reduced-but-nonzero detection semantics.
Phase 26c answers how an emitted sharp-drop alert should be remembered so sparx does not
emit duplicate alerts for the same ongoing reduced-volume interval.

## 2. Active and inactive status

Still active before sharp-drop implementation:

- hard-silence V_DROP detection for device and tenant aggregate subjects
- `silence_open/*` hard-silence duplicate suppression and closure
- expected-source state updates from finalized windows
- bounded hard-silence V_DROP diagnostics

Still inactive after Phase 26c:

- sharp-drop evaluator
- `drop_open/*` runtime writes
- OpenDropStateV1 codec and DB helpers
- sharp-drop AlertV1 construction
- sharp-drop runtime integration
- sharp-drop diagnostics

## 3. Key family decision

Sharp-drop dedup must use a separate tenant-scoped key family:

- `drop_open/v1/device/<device_key>`
- `drop_open/v1/tenant`

These keys are reserved for sharp-drop V_DROP intervals only. They must not be used for
hard-silence intervals.

Rationale:

- hard silence and sharp drop have different closure and supersession rules
- reusing `silence_open/*` would blur the meaning of an open interval
- separate keys allow future migrations and diagnostics without changing active
  hard-silence behavior
- the active `silence_open/*` behavior remains stable for Phase 23e-25d hard silence

## 4. Value shape decision

Future implementation should introduce a separate semantic type named `OpenDropStateV1`.
It may reuse codec helpers from `OpenSilenceStateV1`, but it must be a separate public
state type and must be stored only under `drop_open/*` keys.

Recommended minimal value shape:

- fixed header length: 30 bytes before the variable alert id bytes
- `schema_version_u16` -> u16 LE, value 1
- `subject_kind_u8` -> u8
- `state_flags_u8` -> u8
- `drop_start_ts_i64` -> i64 LE
- `last_alert_window_start_ts_i64` -> i64 LE
- `last_alert_window_end_ts_i64` -> i64 LE
- `last_alert_id_len_u16` -> u16 LE
- `last_alert_id_bytes` -> ASCII lowercase hex bytes of declared length

Recommended flags:

- bit 0: open sharp-drop interval exists
- bit 1: interval closed by recovery
- bit 2: interval closed because hard silence superseded sharp drop

The minimal shape is intentional. The state is for dedup and interval closure, not for
persisting a full explanation snapshot. Alert explanation details remain in AlertV1.

Do not store floating-point expected/observed ratios in OpenDropStateV1. Future evaluation
can recompute expected and observed values from current finalized windows and baseline
stats. This avoids stale state, binary floating-point compatibility concerns, and a larger
state encoding.

## 5. Subject identity

Device sharp-drop dedup identity:

- tenant DB
- subject kind `device`
- `device_key`
- `drop_kind=sharp_drop`, implicit in the `drop_open/*` key family

Tenant aggregate sharp-drop dedup identity:

- tenant DB
- subject kind `tenant`
- key `drop_open/v1/tenant`
- `drop_kind=sharp_drop`, implicit in the `drop_open/*` key family

There is no per-bucket open-state key in the first implementation. A mature subject with
an ongoing sharp-drop interval should not emit a new alert for every bucket while the
reduced-volume condition remains open. The state records the first open timestamp and the
last alert window for operator context.

## 6. Duplicate suppression rule

A future sharp-drop candidate must be suppressed when a matching `drop_open/*` record
exists and its open flag is set.

Suppression match:

- same tenant DB
- same subject kind
- same device key for device subjects
- tenant aggregate key for tenant subjects
- open flag set

The suppression check must be deterministic and must happen after hard-silence priority is
applied.

## 7. Closure by recovery

A matching open sharp-drop interval should close when later observed activity recovers.

Recommended recovery rule for first implementation:

- current observed window exists
- observed lines are nonzero
- expected lines are mature and above the expected-line floor
- `observed_expected_ratio > max_observed_expected_ratio`
- no hard-silence candidate is active for the same subject/window

A later phase may add a distinct recovery ratio with hysteresis, but Phase 26c does not
require it. If hysteresis is added later, it should be explicit and tested to avoid flapping.

Closure action:

- clear the open bit
- set the closed-by-recovery bit
- preserve `drop_start_ts_i64`
- update `last_alert_window_start_ts_i64` and `last_alert_window_end_ts_i64` only if the
  implementation defines those fields as last-evaluated context; otherwise leave the last
  emitted-alert window unchanged
- keep `last_alert_id` as the emitted alert that opened the interval

Implementation may choose to delete closed `drop_open/*` records after closure if the
migration/retention contract explicitly approves deletion. Until then, the safer contract
is to mark closed rather than silently remove state.

## 8. Supersession by hard silence

Hard silence has priority over sharp drop.

Rules:

- if a hard-silence candidate exists for the same subject/window, do not emit a sharp-drop
  alert
- if a hard-silence interval is open for the same subject, suppress sharp-drop evaluation
- if sharp drop is open and hard silence is emitted later, mark the open sharp-drop state
  closed by hard-silence supersession
- hard-silence `silence_open/*` state must not be closed merely because sharp-drop
  conditions recover

This keeps the full-loss case authoritative and prevents an operator from seeing a sharp
reduction alert when the source has actually gone silent.

## 9. Re-opening after closure

A closed sharp-drop state may be reopened only by a later candidate after recovery or hard
silence supersession has already closed the prior interval.

Re-opening rules:

- replace the closed state with a new open OpenDropStateV1 value
- set `drop_start_ts_i64` to the new candidate window start
- set last-alert window fields from the new emitted alert
- set `last_alert_id` to the new alert id
- clear closed flags and set the open flag

A closed record must not suppress future candidates.

## 10. Evaluation ordering

Future runtime evaluation should use this ordering for each subject:

1. Resolve V_DROP policy and subject enablement.
2. Read expected-source maturity state.
3. Evaluate hard-silence eligibility.
4. If hard silence emits or remains open, close/supersede matching `drop_open/*` state and
   skip sharp drop.
5. If hard silence does not apply, evaluate sharp-drop semantic gates.
6. Check matching `drop_open/*` open state for duplicate suppression.
7. Emit sharp-drop AlertV1 only when semantic gates pass and no open duplicate exists.
8. Write or update `drop_open/*` after a sharp-drop alert is emitted.
9. Close `drop_open/*` by recovery when later observations recover.

This ordering ensures hard silence remains authoritative and sharp drop is never evaluated
as a substitute for missing or zero-volume data.

## 11. Tenant aggregate behavior

Tenant aggregate sharp-drop state should use one open record per tenant DB:

- key: `drop_open/v1/tenant`
- subject kind: tenant

The tenant aggregate open interval should close when aggregate observed volume recovers
above the semantic threshold using the same mature-device set rules used for candidate
evaluation.

A tenant aggregate sharp-drop interval must not automatically suppress device sharp-drop
alerts, and device sharp-drop intervals must not automatically suppress tenant aggregate
sharp-drop alerts. These scopes explain different operator stories:

- device: one expected source is degraded
- tenant aggregate: broader tenant-wide volume is degraded

Hard-silence priority still applies separately to each subject scope.

## 12. Diagnostics planning

Phase 26c does not activate new diagnostics. If Phase 27 adds diagnostics, they must remain
bounded and low-cardinality.

Allowed aggregate concepts:

- open sharp-drop intervals
- sharp-drop duplicate suppressions
- sharp-drop recovery closures
- sharp-drop hard-silence supersessions

Do not add device-label metrics. Do not add unbounded suppression-reason label values.

## 13. Required implementation tests

A later implementation phase must add deterministic tests for:

- OpenDropStateV1 encode/decode roundtrip
- invalid schema version rejection
- invalid subject kind rejection
- invalid alert id bytes rejection
- device drop_open key builder determinism
- tenant drop_open key builder determinism
- duplicate suppression for open device state
- duplicate suppression for open tenant aggregate state
- closed state does not suppress a later candidate
- recovery closure clears the open flag and sets the recovery flag
- hard silence supersession clears the open flag and sets the superseded flag
- hard silence open state suppresses sharp-drop emission
- tenant aggregate state does not suppress device state
- device state does not suppress tenant aggregate state

## 14. Phase 26c outcome

Phase 26c locks the future sharp-drop dedup model, but does not implement it. The next
recommended subphase is Phase 26d, focused on the future AlertV1 explanation contract for
sharp-drop alerts.
