# Phase 26f Sharp-Drop Planning Closeout

Phase 26f closes the Phase 26 sharp-drop planning sequence. This is a documentation-only
checkpoint. It does not implement sharp-drop runtime behavior, `drop_open/*` storage,
OpenDropStateV1 codecs, new metrics, new tests, or AlertV1 schema changes.

## 1. Scope closed by Phase 26

Phase 26 is now complete as a planning and contract-scoping phase:

- Phase 26a reviewed the Phase 25d checkpoint, reconciled drift, accepted active
  alert_idx_* persistence as current truth, and created the sharp-drop planning boundary.
- Phase 26b locked the semantic contract for reduced-but-nonzero V_DROP sharp-drop
  detection.
- Phase 26c locked the future `drop_open/*` state and dedup model.
- Phase 26d locked the future AlertV1 explanation contract.
- Phase 26e locked diagnostics, tests, and acceptance gates for a future implementation.
- Phase 26f closes the checkpoint and prepares the next implementation phase.

## 2. Current active behavior after closeout

The active runtime behavior is unchanged from the Phase 25d hard-silence path:

- hard-silence V_DROP detection is active for device subjects
- hard-silence V_DROP detection is active for tenant aggregate subjects
- expected-source state is updated from finalized windows
- open-silence dedup state suppresses duplicate hard-silence alerts
- later observations close matching open-silence state
- V_DROP policy controls are active through global config and tenant-policy overrides
- bounded V_DROP diagnostics are surfaced through status, status JSON, metrics, and health

Sharp-drop detection remains inactive.

## 3. Phase 26 locked decisions

Sharp-drop planning decisions now carried forward:

- sharp-drop remains under reason code `V_DROP`
- the first reason detail must be `drop_kind=sharp_drop`
- first implementation scope covers both device and tenant aggregate subjects
- hard silence takes priority over sharp drop
- zero observed lines belongs to hard silence, not sharp drop
- expected volume should use existing `stats/v1/<device_key>/<bucket>` DeviceStatsV1
  line_count bucket baselines as the primary signal
- DeviceStatsV1 remains the locked 68-byte layout
- byte_count baseline values are explanation-only for the first implementation unless a
  later approved scope changes that
- tenant aggregate expected volume should initially sum mature per-device bucket baselines
- `observed_expected_ratio = observed_lines / expected_lines`
- `drop_ratio = 1.0 - observed_expected_ratio`
- `drop_ratio` is the severity value for future sharp-drop scoring
- recommended planning defaults are max observed/expected ratio `0.25`, minimum drop ratio
  `0.75`, variance gate `3.0` standard deviations when meaningful, and tenant aggregate
  mature-device floor `2`
- future sharp-drop dedup should use separate `drop_open/*` keys, not `silence_open/*`
- future implementation should introduce a separate semantic type named OpenDropStateV1
- future OpenDropStateV1 should be a minimal dedup/closure state and should not store
  floating-point ratio values
- future sharp-drop alerts must reuse existing AlertV1 without schema changes
- future sharp-drop alert ids must include `sharp_drop` in their deterministic input tuple
- device sharp-drop alerts should include current finalized-row provenance when available
- tenant aggregate sharp-drop alerts should use empty provenance for the first
  implementation
- future diagnostics, if added, must be bounded, aggregate, and low-cardinality
- device-label Prometheus metrics, per-subject metric series, and suppression-reason label
  cardinality remain prohibited

## 4. Deferred behavior after Phase 26

The following remain deferred and unimplemented:

- sharp-drop evaluator logic
- `drop_open/*` key builders and storage helpers
- OpenDropStateV1 encoding and decoding
- sharp-drop AlertV1 construction
- sharp-drop runtime integration in run and oneshot
- sharp-drop diagnostics
- per-file/source-path silence detection
- parser-class silence detection
- vendor-event-family silence detection
- external heartbeat checks
- maintenance-window calendars
- replay or recovery behavior changes
- AlertV1 schema changes

## 5. Phase 27 readiness

Phase 27 may begin implementation only after this Phase 26 scope is approved. The
recommended split is:

- Phase 27a: sharp-drop evaluator primitives and deterministic evaluator tests
- Phase 27b: OpenDropStateV1, `drop_open/*`, AlertV1 construction, and dedup tests
- Phase 27c: runtime integration for run and oneshot with policy interaction tests
- Phase 27d: diagnostics if approved, validation hardening, docs/contracts, and closeout

Phase 27 must preserve:

- hard-silence V_DROP behavior and priority
- DeviceStatsV1 68-byte layout
- existing AlertV1 schema
- AlertV1.provenance as the only authoritative drilldown field model
- replay semantics
- recovery behavior
- bounded low-cardinality metrics
- deterministic ordering, IDs, tie-breaks, and emitted output

## 6. Validation status

No local cargo/build/test run was performed for Phase 26f. This checkpoint changed docs,
contracts, and phase-history materials only. Runtime source files and tests were not
intentionally changed.
