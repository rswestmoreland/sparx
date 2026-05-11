# Phase 22 - Expected-Source State and V_DROP Implementation Planning

Phase 22 turns the Phase 21 health/silence scope lock into a concrete implementation
plan. It is still planning/contract work only. It does not activate `V_DROP`, add runtime
source code, add tests, persist new keys, emit new metrics, or change scoring behavior.

## Planning boundary

Phase 22 locks the first implementation target for `V_DROP` without changing the current
runtime:

- first active subject: tenant/device expected activity
- first aggregate subject: tenant-level aggregate expected activity
- first alert output path: existing `AlertV1`
- first persistence shape: tenant-scoped expected-source state and dedup state
- first implementation style: deterministic missing-window scan after window finalization
- first false-positive posture: conservative, fail-closed, and baseline-gated

Deferred beyond the first implementation:

- per-file/source-path silence
- parser-class silence
- vendor-event-family silence
- external heartbeat or host reachability checks
- maintenance-window calendars
- cross-tenant outage correlation
- new alert object type separate from `AlertV1`

## Why expected-source state is required

The current scorer emits alerts from finalized sparse rows. That works for high-volume,
rarity, and drift findings because a row exists. `V_DROP` is different: if a source stops
logging completely, there may be no row to score. The implementation therefore needs a
small durable expectation model that records which sources are mature enough to be
expected and when they were last seen.

The expected-source model should answer these questions deterministically:

- has this tenant/device produced enough history to be considered expected?
- when was the last finalized window observed for this subject?
- what bucket and cadence should be expected now?
- has enough time passed to consider the subject silent?
- has a matching silence alert already been emitted for this open silence interval?

## Planned tenant DB state

The first implementation should keep state in the tenant DB because `V_DROP` is tenant
scoped and must follow tenant lifecycle behavior.

Planned keys were locked in `contracts/35_expected_source_state_vdrop_plan_v0_1.md`.
Phase 23a added the core key helpers and codecs. Phase 23b later activated writes for
`silence_subject/*` expected-source state while keeping `silence_open/*` and `silence_diag/*`
planned only.

Planned state groups:

1. Device expected-source state
   - one record per expected tenant/device subject
   - tracks observed windows, last-seen window, last observed volume, and maturity

2. Tenant aggregate expected-source state
   - one record per tenant aggregate subject
   - tracks observed aggregate windows, active expected device count, and last aggregate
     activity

3. Open silence/dedup state
   - records the currently open or most recently emitted `V_DROP` interval for a subject
   - prevents repeated equivalent alerts for the same silence condition

4. Optional diagnostic counters
   - planned only
   - useful for later metrics, but not required to activate the first scoring behavior

## Planned detection flow

The first implementation should use a two-step flow.

### Step 1: update expected-source state from observed finalized windows

When a device window finalizes and has sufficient activity:

1. update the device expected-source state
2. increment observed-window counters
3. update last-seen window start/end timestamps
4. update last observed lines/bytes
5. update bucket-specific maturity metadata if implemented in the same phase
6. update tenant aggregate state for the same window bucket

This step must not emit `V_DROP`; it only updates state from data that exists.

### Step 2: scan mature expected subjects for missing windows

After the current finalization pass, scan expected-source state for mature subjects whose
last-seen timestamp is too old for the active window cadence.

A device-level `V_DROP` candidate exists when all of the following are true:

- the subject is mature
- tenant policy/runtime state does not suppress it
- the expected baseline activity floor is met
- current time/window position is beyond the grace period
- missed expected windows are at or above the threshold
- no equivalent open silence alert already exists

A tenant-aggregate `V_DROP` candidate exists when the tenant-level aggregate view shows
broad expected-device silence rather than only one isolated device.

## Alert construction plan

Future `V_DROP` alerts should use `AlertV1` with deterministic absence-of-data values:

- `device_key`: the affected device key for device silence; reserved tenant aggregate
  subject key for aggregate silence
- `device_path`: affected device path when available; tenant aggregate marker for tenant
  aggregate silence
- `lines = 0`
- `bytes = 0`
- `top_features = []`
- `provenance = []`
- `reasons` includes `V_DROP`
- `summary_analyst` and `summary_customer` explain that expected telemetry stopped
- `score_volume` should represent drop severity when implemented
- `score_rarity` and `score_drift` should remain 0.0 unless a later contract defines
  synthetic sparse features for absence-of-data alerts

Recommended `V_DROP` reason details are defined in Contract 34. Phase 22 additionally
requires implementation tests to assert deterministic ordering of those details.

## Alert id and dedup plan

A `V_DROP` alert id should be stable for the same tenant, subject, silence interval, and
reason signature.

Recommended signature components:

- `tenant_id`
- subject kind: `device` or `tenant`
- subject key
- silence window start timestamp
- silence window end timestamp
- reason code `V_DROP`
- expected windows missed

The dedup state should suppress repeated equivalent alerts while the same silence
interval remains open. Once the subject is observed again, the open silence state should
be marked closed or replaced by the next independent interval.

## Conservative default policy

The first implementation should prefer missing a weak signal over generating noisy
health alerts.

Recommended first implementation defaults:

- minimum history: derive from the existing scoring cold-start window calculation where
  possible
- minimum expected activity: reuse the existing scoring `min_lines_per_window` floor
  where possible
- missed-window threshold: at least 3 expected windows
- grace period: at least one additional window beyond the threshold
- label: `info` for early implementation unless the contract later locks high-maturity
  promotion to `outlier`
- confidence: low or medium unless maturity and missed-window severity are high

These defaults are planning targets. The implementation phase must either use them or
explicitly update the contract before coding.

## Implementation subphase recommendation

Phase 22 does not write runtime code. Recommended next implementation sequence:

### Phase 23a - Expected-source state structs and encodings

- add state structs and tenant DB key helpers
- add encode/decode tests
- add deterministic key tests
- do not emit alerts yet

### Phase 23b - State update from finalized windows

- update device and tenant aggregate expected-source state when windows finalize
- test maturity/cold-start behavior
- test disabled/terminating tenant suppression at state-use boundaries
- do not emit alerts yet

### Phase 23c - V_DROP candidate evaluator

- evaluate missing-window and sharp-drop candidates from state
- return candidate previews only
- test insufficient history, quiet bucket floor, timestamp inversion, and threshold gates

### Phase 23d - AlertV1 construction and dedup state

- build deterministic `AlertV1` objects with empty provenance
- persist primary alert and existing secondary indexes if active for other alerts
- persist/open dedup state
- test duplicate suppression and reopen-after-recovery behavior

### Phase 23e - Runtime integration and operator surfacing

- wire into run/oneshot finalization path
- add focused integration tests
- document active behavior only after tests pass

## Acceptance gates for V_DROP activation

Phase 23e satisfies the first hard-silence activation path for device and tenant aggregate
subjects. The active boundary is intentionally narrower than the full future model.

Satisfied for Phase 23e:

- planned state keys and value encodings are implemented and tested
- cold-start and low-activity suppression exist in the evaluator
- hard silence emits a deterministic `AlertV1`
- sharp-drop behavior is explicitly deferred
- duplicate suppression is tested for the active runtime path
- alert search can return `V_DROP` alerts through the normal alert path
- docs/contracts are updated from planned to active for the Phase 23e scope

Still deferred after Phase 23e:

- sharp-drop detection
- richer subject scopes
- diagnostics beyond the bounded Phase 25c hard-silence surface
- public policy knobs (implemented through Phase 25b)
- broader operator smoke and external validation hardening

## Phase 22 closeout

Phase 22 locked the implementation plan and the planned state/key contract. It kept the
runtime unchanged. Phase 23a later implemented the state structs, key helpers,
encoders/decoders, and focused codec/key tests. Phase 23b later activated finalized-window
updates for expected-source subject state. Phase 23c later added the pure candidate
evaluator, Phase 23d added deterministic alert construction and open-silence state,
Phase 23e activated the first hard-silence runtime path, and Phase 23f closed the first
hard-silence path with validation hardening. Phase 25a later added the V_DROP config and tenant-policy surfaces. Phase 25b through Phase 25d later completed policy resolution, diagnostics, and diagnostics validation. Phase 26a now scopes future sharp-drop detection planning.


Phase 24 later locked V_DROP policy controls and diagnostics scope as planning-only work.
