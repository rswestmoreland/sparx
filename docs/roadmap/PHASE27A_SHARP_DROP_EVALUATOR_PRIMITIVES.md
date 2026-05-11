# Phase 27a Sharp-Drop Evaluator Primitives

Phase 27a is the first implementation subphase for sharp-drop detection. It adds
storage-agnostic evaluator primitives and deterministic unit tests only. It does not add
runtime integration, persisted `drop_open/*` state, AlertV1 construction, diagnostics,
config fields, tenant-policy fields, replay behavior, or recovery behavior.

## 1. Status

Implemented in this phase:

- expected-volume summary primitives derived from existing DeviceStatsV1 bucket stats
- tenant aggregate expected-volume summation over mature device summaries
- current-window sharp-drop evaluator input type
- sharp-drop evaluator config type
- sharp-drop candidate type
- sharp-drop suppression reasons
- deterministic evaluator tests in `tests/db_silence.rs`

Still inactive after this phase:

- runtime sharp-drop alert emission
- `drop_open/*` persistence
- OpenDropStateV1 encoding and tenant DB helpers
- sharp-drop AlertV1 construction
- run and oneshot integration
- sharp-drop diagnostics and status/metrics/health surfacing

## 2. Source scope

Phase 27a changes are intentionally narrow:

- `src/db/silence.rs`
  - adds sharp-drop expected-volume, current-window, config, candidate, evaluation, and
    suppression primitives
  - adds helper conversion from DeviceStatsV1 line/byte Welford stats
  - adds helper summation for tenant aggregate expected volume
  - preserves existing hard-silence V_DROP evaluator behavior
- `src/db/mod.rs`
  - re-exports the new sharp-drop primitive types
- `tests/db_silence.rs`
  - adds deterministic evaluator tests for device and tenant aggregate cases

No other runtime source modules are changed for behavior in Phase 27a.

## 3. Expected-volume primitive

Device sharp-drop expected volume is derived from existing DeviceStatsV1 values:

- `line_count.mean` -> expected lines
- `line_count.n` -> maturity count
- `line_count.m2` -> sample standard deviation when meaningful
- `byte_count.mean` -> expected bytes for explanation only

The locked 68-byte DeviceStatsV1 encoding is unchanged.

Tenant aggregate expected volume is represented by summing mature per-device expected
volume summaries:

- expected lines are summed
- expected bytes are summed
- maturity count is the number of included mature device summaries
- line standard deviation is combined by summing variance terms and taking the square root

The primitive does not decide which devices are mature. Runtime integration must filter
mature device baselines before calling the summation helper.

## 4. Evaluator semantics

The evaluator emits a candidate only when all required gates pass:

- subject kind is valid
- window bounds are valid
- bucket is valid
- evaluator config is valid
- expected volume is finite and positive
- maturity count meets the configured floor
- expected lines meet the configured expected-line floor
- observed lines are nonzero
- absolute line drop meets the configured floor
- observed/expected ratio is at or below the configured maximum
- drop ratio is at or above the configured minimum
- variance gate passes when standard deviation is meaningful

Zero observed lines are suppressed with hard-silence priority. Hard silence remains the
full-drop case and is handled by the existing V_DROP hard-silence path.

## 5. Ratio definitions

The evaluator uses the Phase 26b definitions:

- `observed_expected_ratio = observed_lines / expected_lines`
- `drop_ratio = 1.0 - observed_expected_ratio`
- `absolute_drop_lines = expected_lines - observed_lines`
- `z_drop = absolute_drop_lines / line_stddev` when line standard deviation is meaningful

The candidate stores finite ratio values and deterministic string details for later alert
construction.

## 6. Reason detail order

The evaluator candidate produces detail keys in the same core order required for future
AlertV1 construction:

1. `drop_kind`
2. `subject_kind`
3. `tenant_id`
4. `device_key` when subject_kind is device
5. `window_start_ts`
6. `window_end_ts`
7. `bucket`
8. `expected_lines`
9. `observed_lines`
10. `observed_expected_ratio`
11. `drop_ratio`
12. `baseline_n`
13. `baseline_mean_lines`
14. `baseline_stddev_lines`
15. `z_drop`
16. `max_observed_expected_ratio`
17. `min_drop_ratio`
18. `min_absolute_drop_lines`

Additional explanation-only details may follow those core keys:

- `expected_bytes`
- `observed_bytes`
- `absolute_drop_lines`

All floating-point detail values use six fractional digits when available. The
`z_drop` detail uses `none` when line standard deviation is not meaningful.

## 7. Tests added

Phase 27a adds tests for:

- expected-volume derivation from DeviceStatsV1 line and byte stats
- device sharp-drop candidate emission for reduced nonzero activity
- zero observed lines suppressing sharp drop because hard silence has priority
- immature and low-expected baselines suppressing sharp drop
- small absolute-drop suppression
- non-severe observed/expected ratio suppression
- variance-gate suppression
- invalid expected-volume suppression
- tenant aggregate expected-volume summation and candidate emission

These tests were added to the source tree, but no local cargo/build/test run was performed
in this environment.

## 8. Acceptance notes

Phase 27a preserves:

- DeviceStatsV1 68-byte layout
- AlertV1 schema
- AlertV1 provenance authority
- hard-silence V_DROP runtime behavior
- `silence_open/*` behavior
- replay semantics
- recovery behavior
- bounded metrics policy

Phase 27b should add OpenDropStateV1, `drop_open/*` helpers, sharp-drop AlertV1
construction, and deterministic tests. Runtime integration should remain deferred until
Phase 27c.
