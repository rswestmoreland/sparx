# Phase 27c - Sharp-drop runtime integration

Status: implementation checkpoint.

Phase 27c activates sharp-drop runtime evaluation by wiring the Phase 27a and
Phase 27b primitives into the existing V_DROP runtime flow. The integration is
intentionally narrow: it uses the already-resolved V_DROP policy switches, the
existing DeviceStatsV1 bucket baselines, and the existing AlertV1 schema.

## Scope

Implemented in Phase 27c:

- collect finalized-window observations during oneshot and run processing
- derive sharp-drop expected volume from the pre-update DeviceStatsV1 bucket
  baseline for each finalized device window
- evaluate device sharp-drop candidates after hard-silence V_DROP evaluation
- evaluate tenant aggregate sharp-drop candidates by summing mature per-device
  expected-volume summaries for the same finalized window
- persist `drop_open/v1/device/<device_key>` for emitted device sharp-drop alerts
- persist `drop_open/v1/tenant` for emitted tenant aggregate sharp-drop alerts
- suppress duplicate sharp-drop alerts while a matching open-drop state remains
  open
- close open-drop state by recovery when a later evaluated window no longer
  satisfies the sharp-drop thresholds
- close open-drop state by hard-silence supersession when hard silence is
  emitted for the same subject
- emit sharp-drop AlertV1 objects through the existing sink path
- keep sharp-drop under V_DROP with first reason detail `drop_kind=sharp_drop`
- add a oneshot integration test that seeds mature stats baselines and verifies
  a device sharp-drop alert plus open-drop state persistence

Not implemented in Phase 27c:

- new config schema fields
- new tenant-policy fields
- new AlertV1 fields
- new stats encodings
- new replay semantics
- new recovery semantics
- per-device Prometheus labels
- per-subject metric series
- per-file/source-path, parser-class, vendor-family, heartbeat, or maintenance
  window drop detection

## Policy behavior

Sharp-drop runtime integration is controlled by the existing resolved V_DROP
policy:

- `enabled` gates all V_DROP processing
- `device_enabled` gates device sharp-drop evaluation
- `tenant_enabled` gates tenant aggregate sharp-drop evaluation
- `min_mature_windows` is reused as the device baseline maturity floor
- `min_expected_lines` is reused as the expected-line and absolute-drop floor

No new policy fields were added in Phase 27c. The first runtime integration uses
the locked Phase 26 defaults:

- max observed/expected ratio: `0.25`
- min drop ratio: `0.75`
- variance gate: `3.0` standard deviations when meaningful
- tenant aggregate mature-device floor: `2`

## Runtime ordering

Runtime V_DROP processing now uses this deterministic order:

1. hard-silence device evaluation
2. hard-silence tenant aggregate evaluation
3. sharp-drop device evaluation for finalized windows that have a pre-update
   stats baseline
4. sharp-drop tenant aggregate evaluation for finalized windows with enough
   mature device baselines

Hard silence remains authoritative. If hard silence emits for a subject, a
matching open sharp-drop state is closed by hard-silence supersession and the
current sharp-drop candidate for that subject is skipped.

## Expected-volume model

Device sharp-drop expected volume uses the pre-update stats row for the same
`device_key` and bucket:

- line mean is the primary expected-line signal
- byte mean is carried into the explanation and alert payload
- line standard deviation is used by the variance gate when nonzero

Tenant aggregate expected volume is the deterministic sum of mature device
expected-volume summaries for the same finalized window. A tenant aggregate
candidate is not evaluated unless at least two mature device baselines
contribute.

## Dedup and closure behavior

Phase 27c persists open-drop state using the Phase 27b key families:

- `drop_open/v1/device/<device_key>`
- `drop_open/v1/tenant`

A matching open-drop state suppresses duplicate sharp-drop alerts for the same
subject kind. A later evaluated window closes open-drop state by recovery when
that window is no longer a sharp drop because the absolute drop is below the
floor, the observed/expected ratio is above the threshold, or the drop ratio is
below the threshold.

Hard silence may supersede an open sharp-drop state for the same subject and
close it with the hard-silence-superseded flag.

## Diagnostics behavior

Phase 27c does not add new metric names or labels. Sharp-drop evaluations,
candidates, suppressed candidates, and emitted alerts contribute to the existing
aggregate V_DROP diagnostics when a mature expected-volume baseline exists and a
sharp-drop evaluation is actually performed.

The following guardrails remain locked:

- no device-label Prometheus metrics
- no per-subject metric series
- no tenant/device/source-path label fanout
- no suppression-reason label cardinality

## Tests added

Phase 27c adds a oneshot integration test that:

1. creates a tenant/device log stream
2. runs oneshot once to create tenant DB state and cursors
3. seeds mature DeviceStatsV1 baselines for all buckets
4. appends one low-volume future observation
5. runs oneshot again
6. verifies exactly one sharp-drop V_DROP alert
7. verifies device `drop_open/*` state is open
8. verifies tenant `drop_open/*` state is absent because only one mature device
   baseline exists

## Preserved behavior

Phase 27c preserves:

- DeviceStatsV1 68-byte layout
- AlertV1 schema
- AlertV1.provenance authority
- hard-silence V_DROP behavior and priority
- silence_open/* behavior
- replay-spool behavior
- recovery behavior
- existing config and tenant-policy schemas

No local cargo/build/test run was performed in this sandbox.
