# Phase 26b - Sharp-Drop Semantic Contract

Status: planning and contract-scoping only. No runtime code is active for sharp-drop
detection at this checkpoint.

## 1. Purpose

Phase 26b locks the semantic rules for future sharp-drop detection before any runtime
implementation. Sharp drop remains a future `V_DROP` subcase for reduced-but-nonzero log
activity.

Hard silence and sharp drop are distinct:

- hard silence: expected activity falls to zero, or no current observed window exists for
  the subject interval
- sharp drop: a current observed window exists and has nonzero activity, but the volume is
  far below the mature bucket-specific expected baseline

## 2. Reason family and naming

Sharp-drop findings should remain in the `V_DROP` loss-of-log family.

Locked naming:

- reason code: `V_DROP`
- detail: `drop_kind=sharp_drop`

The implementation should avoid a new top-level reason code unless a later schema or UX
review proves that `V_DROP` cannot express the operator meaning.

## 3. Ratio terminology

Phase 26b standardizes ratio terms to avoid ambiguity.

Definitions:

- `expected_lines`: expected line volume for the subject/window from mature bucket stats
- `observed_lines`: observed line volume for the current subject/window
- `observed_expected_ratio = observed_lines / expected_lines`
- `drop_ratio = 1.0 - observed_expected_ratio`

For a sharp drop, `observed_lines > 0`, `expected_lines > 0`, and `0.0 < drop_ratio < 1.0`.
For hard silence, observed activity is zero or missing, so the full-drop severity remains
`drop_ratio = 1.0` in the current V_DROP scoring model.

This means `drop_ratio` is a severity value. Larger is more severe. This preserves the
existing AlertV1 scoring convention where `score_volume = clamp01(drop_ratio)` for V_DROP
findings.

## 4. Subject scope

First implementation scope remains:

1. Device sharp drop
   - subject kind: `device`
   - subject key: `tenant_id` plus `device_key`
   - expected volume: mature `stats/v1/<device_key>/<bucket>` line-count baseline
   - observed volume: current finalized row line count for the same device/window

2. Tenant aggregate sharp drop
   - subject kind: `tenant`
   - subject key: `tenant_id`
   - expected volume: sum of mature per-device bucket baselines for tracked devices
   - observed volume: sum of current finalized rows for the contributing devices in the
     evaluation window

Deferred subject scopes remain out of Phase 26b:

- per-file/source-path sharp drop
- parser-class sharp drop
- vendor-event-family sharp drop
- external heartbeat checks
- maintenance-window calendars
- cross-tenant outage correlation

## 5. Expected-volume source

Sharp-drop detection should use existing sparse-matrix baseline stats first.

Device expected lines:

- read `stats/v1/<device_key>/<bucket>`
- use `DeviceStatsV1.line_count.mean` as `expected_lines`
- use `DeviceStatsV1.line_count.n` as the baseline sample count
- use `DeviceStatsV1.line_count.m2` only to derive stddev for the optional variance gate
- keep the locked 68-byte `DeviceStatsV1` layout unchanged

Device expected bytes:

- read `DeviceStatsV1.byte_count.mean` for explanation and possible later secondary gate
- do not require byte-count gating in the first implementation unless Phase 27 explicitly
  adds it

Tenant aggregate expected lines:

- iterate tracked device expected-source subjects for the tenant in deterministic key order
- include a device only when both expected-source maturity and DeviceStatsV1 maturity pass
- sum included device `line_count.mean` values
- count included devices as `mature_devices`
- suppress tenant aggregate sharp drop if `mature_devices` is below the tenant aggregate
  floor

This keeps expected-volume modeling inside the existing sparse-row and baseline system and
avoids a new tenant-wide stats encoding.

## 6. Required gates

A future sharp-drop candidate may be emitted only when every gate below passes.

Common gates:

- resolved V_DROP policy is enabled
- resolved subject kind is enabled
- subject is active, not disabled, not terminating, and not terminated
- expected-source state exists and is mature
- current evaluation window is valid and aligned
- bucket is valid in the current 0..47 bucket model
- relevant DeviceStatsV1 line-count baseline exists
- line-count baseline sample count is mature
- expected line mean is at or above the expected-line floor
- observed lines are greater than zero
- observed lines are lower than expected lines
- `observed_expected_ratio <= max_observed_expected_ratio`
- `drop_ratio >= min_drop_ratio`
- `expected_lines - observed_lines >= min_absolute_drop_lines`
- variance gate passes when enabled and statistically meaningful
- no matching hard-silence interval is open for the same subject
- no equivalent sharp-drop interval is already open

Zero observed lines must be handled by hard silence, not sharp drop.

## 7. Planning defaults for first implementation

These are semantic defaults for Phase 27 planning. They are not active runtime config in
Phase 26b.

Recommended defaults:

- baseline sample floor: inherit the resolved V_DROP mature-window floor
- expected-line floor: inherit the resolved V_DROP minimum expected-line floor
- minimum observed lines: `1`
- maximum observed/expected ratio: `0.25`
- minimum drop ratio: `0.75`
- minimum absolute drop lines: inherit the resolved V_DROP minimum expected-line floor;
  if that floor is configured as `0`, the absolute drop floor is disabled
- variance gate: require a drop of at least `3.0` line-count standard deviations when
  stddev is available and positive
- tenant aggregate mature-device floor: `2`
- byte-count gate: explanation-only in the first implementation

The ratio gate and absolute line floor are both required so small naturally quiet windows
do not alert only because of a large percentage change.

## 8. Variance gate

When line-count variance is meaningful:

- `variance = m2 / (n - 1)` for `n >= 2`
- `stddev = sqrt(variance)`
- `z_drop = (expected_lines - observed_lines) / stddev`
- pass when `z_drop >= min_stddev_drop`

Recommended `min_stddev_drop` is `3.0`.

If `n < 2`, `m2` is invalid, or `stddev` is zero or near zero, the variance gate must not
create a divide-by-zero path. In that case the implementation should rely on maturity,
ratio, and absolute-drop gates.

## 9. Naturally quiet period controls

Sharp drop must compare a row only against the same bucket family used by current baseline
stats. It must not compare quiet overnight or weekend windows against busy weekday hours.

Required controls:

- bucket-local baseline only
- mature expected-source state
- mature DeviceStatsV1 line-count baseline
- expected-line floor
- absolute line-drop floor
- hard-silence priority

## 10. Interaction with hard silence

Hard silence has priority over sharp drop.

Rules:

- missing current window: evaluate hard silence, not sharp drop
- zero observed lines: evaluate hard silence, not sharp drop
- open hard-silence interval for the same subject: suppress sharp drop
- hard-silence candidate and sharp-drop candidate for the same subject/window: emit hard
  silence only
- hard silence may supersede an open sharp-drop interval in the future state model

## 11. Interaction with V_SPIKE and V_EXTREME

`V_SPIKE` and `V_EXTREME` remain high-volume reasons for unusually large rows. Sharp drop
is the low-volume counterpart for existing rows.

Rules:

- sharp drop must not be inferred from high-volume scoring
- high-volume reasons must not be suppressed by sharp-drop planning
- if corrupted data or misconfiguration appears to satisfy both high-volume and sharp-drop
  conditions for the same row, suppress sharp drop or fail closed

## 12. Alert explanation semantics

Future sharp-drop alerts should use the existing AlertV1 schema.

Required detail keys in deterministic order:

1. `drop_kind`
2. `subject_kind`
3. `tenant_id`
4. `device_key` when subject kind is device
5. `window_start_ts`
6. `window_end_ts`
7. `expected_lines`
8. `observed_lines`
9. `observed_expected_ratio`
10. `drop_ratio`
11. `baseline_n`
12. `baseline_mean_lines`
13. `baseline_stddev_lines`
14. `bucket`
15. `mature_devices` for tenant aggregate alerts
16. `observed_devices` for tenant aggregate alerts

Recommended scoring:

- `score_volume = clamp01(drop_ratio)`
- `score_total = score_volume`
- `score_rarity = 0.0`
- `score_drift = 0.0`
- first implementation label should remain `info`

Provenance:

- device sharp-drop alerts may include current finalized-row provenance
- tenant aggregate sharp-drop alerts may have empty provenance unless a deterministic
  capped aggregate-provenance rule is approved
- `AlertV1.provenance` remains authoritative
- `source_files` must not be reintroduced

## 13. Policy and config scope

Phase 26b does not activate new config fields.

Future implementation may reuse existing V_DROP controls:

- `vdrop_enabled`
- `vdrop_device_enabled`
- `vdrop_tenant_enabled`
- `vdrop_min_mature_windows`
- `vdrop_min_expected_lines`

Future implementation may add sharp-drop-specific controls only after explicit approval:

- sharp-drop enabled flag
- maximum observed/expected ratio
- minimum drop ratio
- minimum absolute drop lines
- minimum baseline sample count
- minimum stddev drop
- tenant aggregate mature-device floor

## 14. Tests required before activation

Phase 27 implementation must include deterministic coverage for:

- device sharp-drop candidate from mature stats and nonzero current row
- tenant aggregate sharp-drop candidate from summed mature device stats
- suppression when observed lines are zero
- suppression when no current finalized row exists
- suppression when expected-source state is immature
- suppression when DeviceStatsV1 line-count n is immature
- suppression when expected line mean is below the floor
- suppression when absolute line drop is below the floor
- suppression when observed/expected ratio is above the maximum
- suppression when variance gate fails
- suppression when hard-silence state is open
- duplicate suppression through future `drop_open/*`
- deterministic detail ordering
- no AlertV1 schema changes
- no stats/v1 encoding changes
- no recovery or replay behavior changes
- no device-label metrics

## 15. Phase 26b outcome

Phase 26b locks sharp-drop semantics but does not implement them. The next recommended
subphase is Phase 26c, focused on the state and dedup model for future `drop_open/*`.
