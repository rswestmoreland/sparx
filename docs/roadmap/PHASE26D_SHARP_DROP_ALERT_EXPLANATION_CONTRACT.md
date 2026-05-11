# Phase 26d - Sharp-Drop AlertV1 Explanation Contract

Status: planning and contract-scoping only. No runtime code is active for sharp-drop
detection at this checkpoint.

## 1. Purpose

Phase 26d locks how future sharp-drop detections should be represented as AlertV1
objects. Phase 26b locked the reduced-but-nonzero detection semantics. Phase 26c locked
the future drop_open state and duplicate-suppression model. Phase 26d locks the alert
schema usage, reason detail ordering, scoring fields, provenance behavior, and deterministic
operator summaries before implementation.

## 2. Active and inactive status

Still active before sharp-drop implementation:

- hard-silence V_DROP detection for device and tenant aggregate subjects
- hard-silence AlertV1 construction
- hard-silence silence_open duplicate suppression and closure
- expected-source state updates from finalized windows
- bounded hard-silence V_DROP diagnostics

Still inactive after Phase 26d:

- sharp-drop evaluator
- drop_open runtime writes
- OpenDropStateV1 codec and DB helpers
- sharp-drop AlertV1 construction
- sharp-drop runtime integration
- sharp-drop diagnostics

## 3. Schema decision

Future sharp-drop alerts must use the existing AlertV1 schema. Phase 26d does not add,
remove, rename, or reinterpret top-level AlertV1 fields.

Required top-level behavior for the first implementation:

- `schema_version = 1`
- `label = info`
- `confidence = medium`
- `cold_start = false`
- `score_volume = clamp01(drop_ratio)`
- `score_total = score_volume`
- `score_rarity = 0.0`
- `score_drift = 0.0`
- `baseline_n_bucket = None` unless a later implementation explicitly reuses this field
- `baseline_centroid_norm = None`
- `top_features = []` for the first implementation
- `reasons` contains one leading V_DROP reason for the sharp-drop condition

The empty `top_features` rule is intentional for the first implementation. Sharp drop is
a volume-loss explanation, not a feature-contribution explanation. A later implementation
may add current-row feature context only with a separate approved contract update.

## 4. Subject mapping

Device sharp-drop alert mapping:

- `tenant_id` is the active tenant id
- `device_key` is the degraded device key
- `device_path` is the device path or stable device display value used by the current row
- `window_start_ts`, `window_end_ts`, `window_size_s`, and `bucket` identify the observed
  current window
- `lines` is `observed_lines`
- `bytes` is observed current-window bytes when available, otherwise `0`
- dropped counters should come from the current row when available, otherwise `0`

Tenant aggregate sharp-drop alert mapping:

- `tenant_id` is the active tenant id
- `device_key = __tenant__`
- `device_path = tenant:<tenant_id>`
- `window_start_ts`, `window_end_ts`, `window_size_s`, and `bucket` identify the aggregate
  evaluated window
- `lines` is aggregate observed lines
- `bytes` is aggregate observed bytes when available, otherwise `0`
- dropped counters should be aggregate values only if the implementation already has a
  deterministic aggregate source; otherwise use `0`

## 5. Reason code and message

Sharp-drop alerts must use:

- `reasons[0].code = V_DROP`
- `reasons[0].details[0] = (drop_kind, sharp_drop)`

Recommended reason message for the first implementation:

- `log volume dropped sharply but did not stop for this subject`

The message is operator-facing UX text and may evolve. The reason code and detail keys are
contractual.

## 6. Deterministic reason details

Sharp-drop reason details must be emitted in deterministic vector order. Do not build the
detail list by iterating over an unordered map.

Core details, in required order:

1. `drop_kind = sharp_drop`
2. `subject_kind = device|tenant`
3. `tenant_id = <tenant_id>`
4. `device_key = <device_key>` only for device subjects
5. `window_start_ts = <unix seconds>`
6. `window_end_ts = <unix seconds>`
7. `bucket = <0..47>`
8. `expected_lines = <decimal integer or fixed decimal>`
9. `observed_lines = <decimal integer>`
10. `observed_expected_ratio = <fixed decimal>`
11. `drop_ratio = <fixed decimal>`
12. `baseline_n = <decimal integer>`
13. `baseline_mean_lines = <fixed decimal>`
14. `baseline_stddev_lines = <fixed decimal or none>`
15. `z_drop = <fixed decimal or none>`
16. `max_observed_expected_ratio = <fixed decimal>`
17. `min_drop_ratio = <fixed decimal>`
18. `min_absolute_drop_lines = <decimal integer or fixed decimal>`

Tenant aggregate details, appended in required order for tenant aggregate alerts:

19. `mature_devices = <decimal integer>`
20. `observed_devices = <decimal integer>`

Byte explanation details, appended after tenant aggregate details when available:

21. `expected_bytes = <decimal integer or fixed decimal>`
22. `observed_bytes = <decimal integer>`
23. `baseline_mean_bytes = <fixed decimal>`

Byte details are explanation-only for the first implementation. Their presence must be
controlled by deterministic data availability, not by display preferences.

## 7. Numeric formatting

Reason detail values are strings. Future implementation should use stable ASCII formatting:

- integer counts: base-10 decimal with no separators
- ratios and scores: finite fixed decimal, recommended six fractional digits
- unavailable variance values: `none`
- NaN and infinity are invalid and must fail closed or suppress the candidate before alert
  construction

`drop_ratio` remains severity:

- `observed_expected_ratio = observed_lines / expected_lines`
- `drop_ratio = 1.0 - observed_expected_ratio`

## 8. Alert id and signature planning

Future sharp-drop alert IDs must be deterministic and must not collide with hard-silence
V_DROP IDs for the same subject/window.

Recommended sharp-drop alert id input tuple:

- tenant id
- subject kind
- subject key
- window start
- window end
- reason code `V_DROP`
- detail value `sharp_drop`

The signature should include the reason code and the ordered reason details. Floating-point
threshold and ratio strings in the signature must use the same deterministic formatting as
reason details.

## 9. Provenance behavior

`AlertV1.provenance` remains the only authoritative drilldown field model. Do not
reintroduce `source_files`.

Device sharp-drop provenance:

- use current finalized-row provenance when it is available
- preserve the existing FileSpanV1 cap and deterministic ordering rules
- empty provenance is acceptable only when row provenance is unavailable
- drill/extract must continue to fail closed with a clear explanation when provenance is
  empty or unavailable

Tenant aggregate sharp-drop provenance:

- use empty provenance for the first implementation
- do not aggregate arbitrary per-device spans without a separate deterministic capped
  aggregate-provenance rule

Rationale: tenant aggregate sharp drop explains a broad volume decrease across multiple
subjects. A small arbitrary span sample could mislead operators unless the sample rule is
separately locked and tested.

## 10. Summary text

Future sharp-drop summaries must be deterministic and must not require LLM generation.

Recommended analyst summary shape:

- device: `V_DROP sharp-drop score <score>. Observed <observed_lines> of <expected_lines>
  expected lines for device <device_key> in bucket <bucket>.`
- tenant aggregate: `V_DROP sharp-drop score <score>. Observed <observed_lines> of
  <expected_lines> expected tenant aggregate lines across <observed_devices> observed
  devices in bucket <bucket>.`

Recommended customer summary shape:

- device: `Log volume for this device dropped sharply compared with its normal pattern,
  but logs are still arriving.`
- tenant aggregate: `Overall tenant log volume dropped sharply compared with the normal
  pattern, but some logs are still arriving.`

Exact wording may be refined during implementation, but summaries must remain deterministic
and derived from alert fields.

## 11. Hard-silence interaction

Hard silence remains authoritative:

- if observed lines are zero, the case belongs to hard silence, not sharp drop
- if no current observed window exists, the case belongs to hard silence, not sharp drop
- if hard-silence state is open for the same subject, suppress sharp-drop emission
- if hard silence is emitted after a sharp-drop interval opens, future implementation should
  mark matching drop_open state closed by hard-silence supersession

A sharp-drop AlertV1 must not close or mutate hard-silence silence_open state.

## 12. Required implementation tests

A later implementation phase must add deterministic tests for:

- sharp-drop AlertV1 top-level fields for device alerts
- sharp-drop AlertV1 top-level fields for tenant aggregate alerts
- reason detail key ordering for device alerts
- reason detail key ordering for tenant aggregate alerts
- fixed numeric formatting for ratios and thresholds
- `drop_ratio` score mapping to `score_volume` and `score_total`
- hard-silence and sharp-drop alert id non-collision
- signature determinism from ordered reason details
- device current-row provenance preservation when available
- empty device provenance behavior when row provenance is unavailable
- empty tenant aggregate provenance for the first implementation
- no `source_files` reintroduction
- deterministic analyst and customer summaries
- no AlertV1 schema changes

## 13. Non-goals

Phase 26d does not add:

- runtime sharp-drop evaluation
- drop_open storage writes
- new AlertV1 fields
- new stats fields or encodings
- per-file, parser-class, or vendor-family subject models
- tenant aggregate provenance sampling
- replay or recovery behavior changes
- new diagnostics or metrics
