# Phase 26 - Sharp-Drop Detection Scope Lock and Planning

Status: planning and contract-scoping only. No runtime code is active for sharp-drop
detection at this checkpoint.

## 1. Review and reconciliation outcome

Phase 26a reviewed the Phase 25d checkpoint and reconciled the next sharp-drop scope
against the active hard-silence V_DROP implementation.

Confirmed active behavior:

- hard-silence V_DROP detection is active for device subjects and tenant aggregate subjects
- hard-silence V_DROP uses expected-source state from finalized windows
- hard-silence V_DROP emits existing AlertV1 objects through the normal alert path
- silence_open/* state suppresses duplicate hard-silence alerts while an interval remains open
- later observations close matching hard-silence open state
- global config and tenant-policy controls are active for the hard-silence path
- bounded V_DROP diagnostics are active through status, status --json, /metrics, and /healthz
- sharp-drop detection remains inactive

Reconciled drift:

- alert_idx_* persistence is accepted as current truth because the Phase 25d source,
  tests, README, contracts, and history all show Phase 15b-15d activated secondary alert
  indexes with safe fallback to primary alert scans. Future guardrails should treat
  alert_idx_time, alert_idx_cat, and alert_idx_ent as active persisted secondary indexes.
- stale wording in Contract 25 was corrected so silence_open/* is no longer described as
  unused after Phase 23e.
- Phase 22/23 wording was clarified so sharp-drop remained deferred after hard-silence
  candidate evaluation.
- preexisting non-ASCII characters in three contracts were replaced with ASCII forms.

## 2. Sharp-drop definition

Sharp-drop detection is a V_DROP subcase for reduced-but-nonzero log volume.

Hard silence detects expected activity falling to zero because no current window was
observed for a mature subject. Sharp drop detects a mature subject whose current window
still exists, but whose observed line/byte activity is far below the bucket-specific
expected baseline.

Sharp-drop must not be implemented as a high-volume anomaly and must not replace
V_SPIKE or V_EXTREME. It is the low-volume counterpart to existing volume scoring and
uses expected-vs-observed volume evidence.

## 3. First implementation scope

First sharp-drop implementation scope should cover both active hard-silence subjects:

1. device subject sharp drop
   - subject kind: device
   - key: tenant_id plus device_key
   - evidence: current finalized sparse row for that device/window plus existing
     device stats for the same bucket

2. tenant aggregate sharp drop
   - subject kind: tenant
   - key: tenant_id
   - evidence: aggregate observed activity across finalized rows in the evaluation pass
     compared to the sum of mature per-device bucket baselines for tracked expected
     device subjects

Deferred subject scopes remain unchanged:

- per-file/source-path sharp drop
- parser-class sharp drop
- vendor-event-family sharp drop
- external heartbeat or reachability checks
- maintenance-window calendars
- cross-tenant outage correlation

## 4. Reason code and details decision

Sharp-drop alerts should use the existing reason code `V_DROP` with an explicit detail:

- `drop_kind=sharp_drop`

Rationale:

- keeps hard silence and sharp drop in the same loss-of-log family
- avoids adding a new top-level reason code before the AlertV1 schema needs it
- lets operator UX distinguish hard silence from reduced-but-nonzero activity through
  deterministic reason details

Hard-silence alerts should continue to use:

- `drop_kind=hard_silence` when the implementation is next touched, or omit the field
  for backward compatibility until that change is deliberately made

Phase 27 should avoid changing already-emitted historical alert objects.

## 5. Expected volume model

Sharp-drop should use existing sparse-matrix baseline state first, without adding a new
large state family.

Device expected volume:

- use `stats/v1/<device_key>/<bucket>` DeviceStatsV1
- compare the current finalized row's `lines` and optionally `bytes` against the bucket
  Welford line_count and byte_count means
- require Welford maturity before evaluating
- keep `stats/v1/<device_key>/<bucket>` fixed at the locked 68-byte layout

Tenant aggregate expected volume:

- list tracked device expected-source subjects for the tenant
- for each mature tracked device, read its current-bucket DeviceStatsV1
- sum mature per-device expected line means to derive tenant expected lines
- sum current observed finalized-window lines across devices in the evaluation pass to
  derive tenant observed lines
- suppress tenant aggregate sharp drop if too few mature device baselines contribute

This keeps the design inside the existing sparse row and baseline model. It avoids a new
tenant-aggregate stats layout unless Phase 27 proves the per-device sum path is too costly
or insufficient.

## 6. Maturity and false-positive controls

A sharp-drop candidate must be suppressed unless all of these are true:

- V_DROP is globally enabled and the subject kind is enabled by global config plus tenant
  policy resolution
- the subject is active and not disabled, terminating, or terminated
- the subject has expected-source maturity at least equal to the configured mature-window
  floor
- the relevant DeviceStatsV1 line_count Welford state has at least the configured
  baseline sample floor
- expected line mean is at or above a configured expected-line floor
- observed lines are nonzero; zero activity belongs to hard-silence handling
- observed lines are below a configured maximum observed/expected ratio
- absolute expected-minus-observed lines exceed a configured minimum absolute drop floor
- when variance is available, observed lines are below the expected mean by a configured
  standard-deviation floor
- no matching hard-silence open interval exists
- no equivalent sharp-drop interval is already open
- timestamps, counters, and bucket values are valid

Bucket-specific baselines are mandatory so naturally quiet weekday/weekend/hour periods
are not compared against busy periods.

## 7. Interaction with V_SPIKE and V_EXTREME

V_SPIKE and V_EXTREME remain high-volume scoring reasons for rows that exist. Sharp drop
is a low-volume V_DROP path for rows that exist but are much smaller than expected.

Implementation should evaluate sharp drop after a row is finalized and after the normal
sparse-row scoring inputs are available. If a row somehow qualifies for both high-volume
and sharp-drop reasons due to configuration or data corruption, the implementation must
fail closed or suppress sharp-drop for that row.

Hard silence takes precedence over sharp drop. If no current window exists, the system
must use hard-silence logic rather than synthesizing a sharp-drop row.

## 8. Dedup and state decision

Sharp drop should not reuse silence_open/*.

Recommended new tenant-scoped open state family for Phase 27:

- `drop_open/v1/device/<device_key>`
- `drop_open/v1/tenant`

Rationale:

- hard-silence intervals and reduced-volume intervals have different closure rules
- keeping separate state prevents a low-volume interval from hiding a later hard-silence
  interval
- the two state families can share struct shape if practical, but their key families
  should remain distinct

Interaction rules:

- an open hard-silence interval suppresses sharp-drop emission for the same subject
- hard silence supersedes sharp drop; if hard silence opens for a subject with an open
  sharp-drop interval, Phase 27 should close or mark the sharp-drop interval as superseded
- a later healthy observation closes matching open sharp-drop state
- a continuing reduced-volume observation is deduped by matching drop_open/* state

## 9. AlertV1 explanation contract

Sharp-drop alerts should use the existing AlertV1 schema.

Recommended fields:

- `label = info` for the first conservative implementation
- `confidence = medium` only when maturity, expected volume, and drop severity are strong;
  otherwise low or suppressed
- `score_volume = clamp01(drop_ratio)`
- `score_total = score_volume` unless Phase 27 deliberately composes more context
- `score_rarity = 0.0`
- `score_drift = 0.0`
- `reasons[0].code = V_DROP`
- `reasons[0].details` include deterministic expected-vs-observed fields

Recommended detail order:

1. `drop_kind`
2. `subject_kind`
3. `tenant_id`
4. `device_key` when applicable
5. `window_start_ts`
6. `window_end_ts`
7. `expected_lines`
8. `observed_lines`
9. `drop_ratio`
10. `baseline_n`
11. `baseline_mean_lines`
12. `baseline_stddev_lines`
13. `bucket`
14. `mature_devices` for tenant aggregate when applicable
15. `observed_devices` for tenant aggregate when applicable

Provenance:

- device sharp-drop alerts may reuse the finalized row provenance because logs are still
  arriving
- tenant aggregate sharp-drop alerts may use empty provenance unless Phase 27 implements
  a deterministic capped aggregate-provenance selection rule
- hard-silence empty-provenance behavior remains unchanged
- AlertV1.provenance remains the only authoritative drilldown model

## 10. Diagnostics and metrics decision

Sharp-drop diagnostics should be optional in the first implementation subphase and bounded
if added.

Allowed diagnostic concepts:

- evaluated sharp-drop subjects
- sharp-drop candidates
- sharp-drop suppressions
- emitted sharp-drop alerts
- open sharp-drop intervals
- last sharp-drop evaluation timestamp

Metric labels must remain low-cardinality. Do not add device labels. Suppression-reason
cardinality metrics remain deferred unless a later phase locks a bounded allowlist.

## 11. Deterministic tests and fixtures

Phase 27 should add tests before or with implementation for:

- device sharp-drop candidate from a mature bucket baseline and nonzero current row
- tenant aggregate sharp-drop candidate from summed mature device baselines
- suppression for zero observed lines so hard silence owns that case
- suppression for immature expected-source state
- suppression for insufficient DeviceStatsV1 Welford n
- suppression for low expected-line baseline
- suppression for naturally quiet bucket
- suppression for already-open hard-silence state
- duplicate suppression for drop_open/*
- closure of drop_open/* after healthy observation
- hard-silence superseding an open sharp-drop interval
- deterministic reason-detail ordering
- no new alert schema fields
- no device-label Prometheus metrics
- no replay/recovery behavior changes

## 12. Recommended implementation split

Phase 27 implementation should follow this approved Phase 26 scope. Phase 27a later started with evaluator primitives only and kept runtime sharp-drop alert emission inactive.

Recommended implementation sequence:

- Phase 27a: sharp-drop evaluator primitives over current row plus DeviceStatsV1
- Phase 27b: drop_open/* state helpers and AlertV1 construction
- Phase 27c: runtime integration for device and tenant aggregate subjects
- Phase 27d: diagnostics, tests, docs, and closeout

## 13. Phase 26 acceptance gates

Phase 26 is complete when:

- sharp-drop remains explicitly inactive
- Contract 37 records the scope and non-goals
- README, docs, contract index, scoring contract, alert schema contract, health/silence
  contract, expected-source contract, and policy/diagnostics contract reference the new
  planning boundary
- alert_idx_* persistence drift is reconciled as active current behavior
- no runtime source files are changed
- no build/test success is claimed without external logs

## 14. Phase 26b semantic contract outcome

Phase 26b adds `docs/PHASE26B_SHARP_DROP_SEMANTIC_CONTRACT.md` and tightens Contract 37.
It remains documentation-only.

Locked semantics:

- sharp drop remains inactive until a later implementation phase
- sharp drop remains under `V_DROP` with `drop_kind=sharp_drop`
- `observed_expected_ratio = observed_lines / expected_lines`
- `drop_ratio = 1.0 - observed_expected_ratio`
- `drop_ratio` is severity, so larger values are more severe
- hard silence remains the full-drop case with `drop_ratio = 1.0`
- sharp drop requires nonzero observed lines and mature bucket-local expected line volume
- first implementation should use DeviceStatsV1 line_count Welford mean as the primary
  expected-lines source
- DeviceStatsV1 byte_count may be included in explanation but is not required as a first
  implementation gate
- tenant aggregate expected volume should sum mature per-device line_count baselines and
  should require at least two mature contributing devices by default
- recommended planning defaults are max observed/expected ratio `0.25`, min drop ratio
  `0.75`, and variance gate `3.0` stddevs when stddev is meaningful

Next recommended subphase: Phase 26c, state and dedup model decision for future
`drop_open/*`.
