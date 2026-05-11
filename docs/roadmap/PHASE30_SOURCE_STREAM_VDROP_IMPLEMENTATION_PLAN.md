# Phase 30 Source-Stream V_DROP Implementation Plan

Status: complete as a documentation-only implementation-planning checkpoint.

Phase 30 follows the Phase 29 richer V_DROP subject-scope plan. It does not add runtime
behavior, tests, persisted keys, config fields, tenant-policy fields, metrics, replay
behavior, recovery behavior, or alert schema changes.

No local cargo/build/test/rustfmt run was performed in this sandbox.

## Purpose

Phase 30 turns the Phase 29 source-stream subject decision into a concrete implementation
plan. It locks the future identity, catalog, stats/state, evaluator, alert, dedup,
runtime, diagnostics, and acceptance boundaries for source-stream `V_DROP` work.

A source stream is a tenant/device-specific log source, normally one canonical relative
file path under a device directory. Source-stream `V_DROP` is intended to catch cases such
as one important log file going quiet or sharply dropping while the device still emits
other logs.

## Current active baseline

The following behavior is current truth and remains unchanged by Phase 30:

- hard-silence `V_DROP` is active for device and tenant aggregate subjects
- sharp-drop `V_DROP` is active for device and tenant aggregate subjects
- `silence_open/*` is hard-silence state
- `drop_open/*` is sharp-drop state
- `AlertV1.provenance: Vec<FileSpanV1>` is authoritative
- `DeviceStatsV1` remains the locked 68-byte device/bucket stats layout
- V_DROP policy controls remain active through the existing global config and tenant-policy
  override fields
- V_DROP diagnostics remain bounded and low-cardinality
- active `alert_idx_*` persistence remains current truth
- no source-stream V_DROP runtime behavior is active yet

## Phase 30 locked planning decisions

Phase 30 locks these decisions for the next implementation sequence:

- source-stream V_DROP is the next planned richer subject family
- source-stream identity is a subject id, not a FeatureId
- source-stream identity must not revive hashed-fallback FeatureId behavior
- source-stream identity must be derived from tenant id, device key, and canonical relative
  source path
- raw source paths must not be used in Prometheus labels
- source-stream expected volume must use separate stats/state and must not change
  `DeviceStatsV1`
- source-stream hard silence and sharp drop should both remain under reason `V_DROP`
- source-stream sharp drop keeps detail `drop_kind=sharp_drop`
- source-stream hard silence should use detail `drop_kind=hard_silence` for new
  source-stream hard-silence alerts to avoid ambiguity
- source-stream runtime activation should require an explicit source-stream policy gate
- the first public source-stream policy gate should default to disabled until local
  validation confirms alert volume and storage cost
- source-stream diagnostics must be aggregate/per-tenant only
- source-stream implementation must preserve replay and recovery behavior

## Source-stream identity

Future source-stream code should introduce a dedicated semantic subject kind, likely:

```text
SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1 = 3
```

This value is only a planned implementation direction in Phase 30.

The stable source-stream id is:

```text
source_stream_id = stable_hash_hex128_v1(source_stream_contract_input)
```

The canonical contract input string should be:

```text
source_stream/v1\ntenant_id=<tenant_id>\ndevice_key=<device_key>\nsource_path=<canonical_relative_source_path>\n
```

Hashing uses the existing stable hash rule: BLAKE3 first 16 digest bytes encoded as
lowercase hex.

This is not a FeatureId. It is a subject identifier for source-stream state and alert
explanation.

## Canonical relative source path

The source path component must be a canonical relative path under the device directory.
The future implementation should derive it from the same relative path already used in
`DiscoveredFileV1.file_rel` and `FileSpanV1.file_rel`.

Canonicalization rules:

- path separators are normalized to `/`
- path is relative, never absolute
- path is non-empty
- path has no empty, `.`, or `..` components
- path has no ASCII control characters, tab, newline, or NUL
- path preserves case because Enterprise Linux filesystems are case-sensitive
- path does not include tenant root, absolute host path, or database path
- path does not resolve symlinks beyond the existing ingest `follow_symlinks` behavior

If a source path cannot be canonicalized safely, source-stream V_DROP for that path must
fail closed by suppressing source-stream state updates and source-stream candidates for
that path.

## Source-stream catalog

A future implementation should add a compact source-stream catalog record so alerts can
explain the subject without storing raw paths in metric labels.

Planned key:

```text
source_stream/v1/<device_key>/<source_stream_id>/catalog
```

Planned value:

```text
SourceStreamCatalogV1
```

Planned fields:

- schema version
- source_stream_id
- device_key
- canonical_relative_source_path
- first_seen_ts
- last_seen_ts
- state flags
- reserved fields for future migration

Catalog rules:

- catalog writes are tenant-scoped
- catalog ordering and listing are deterministic by device key, source-stream id, then path
- catalog records are not FeatureId records
- catalog records must not be used to resolve sparse feature ids
- catalog records should update `last_seen_ts` when the source stream is observed
- retirement/rotation status should be a state flag, not a key deletion side effect

## Source-stream stats and expected-source state

Source-stream expected volume must not use or mutate `DeviceStatsV1`.

A future implementation should introduce a separate source-stream stats record:

```text
source_stats/v1/<device_key>/<source_stream_id>/<bucket>
```

Planned value:

```text
SourceStreamStatsV1
```

Recommended first value shape:

- fixed-size encoding using the same Welford-family shape as `DeviceStatsV1`
- line-count Welford state as the primary expected-volume signal
- byte-count Welford state as explanation-only for the first implementation
- score-total Welford state reserved or zeroed unless a later contract approves its use

The future implementation may also reuse the existing expected-source state shape for
source-stream last-seen and maturity tracking by extending the allowed subject kind, not
by changing the fixed 68-byte layout:

```text
silence_subject/v1/source_stream/<device_key>/<source_stream_id>/state
```

Planned value:

```text
ExpectedSourceStateV1 with subject kind source_stream
```

Rules:

- existing device and tenant expected-source keys and encodings must not change
- source-stream stats are updated from source-stream observations derived during window
  finalization
- source-stream candidate evaluation must use pre-update stats for the current finalized
  window, matching the existing sharp-drop device/tenant integration pattern
- source-stream hard-silence scans must iterate source-stream expected-source state by
  prefix
- invalid or immature source-stream state suppresses candidates fail-closed

## Source-stream observations

Future runtime integration should derive source-stream observations from finalized-window
file spans. The recommended first approach is to group current-window spans by canonical
relative source path and compute:

- observed line count: number of file spans for that source stream
- observed byte count: sum of `offset_end - offset_start` for valid spans
- provenance: deterministic capped spans for that source stream

Rules:

- span grouping must be deterministic by canonical source path then source-stream id
- invalid span offsets suppress the invalid span from source-stream observation rather
  than producing negative byte counts
- source-stream observation must not change device-level row counts
- source-stream observation must not change feature emission or FeatureId assignment

## Hard-silence semantics

A source-stream hard-silence candidate exists only when:

- source-stream V_DROP policy is enabled
- source-stream expected-source state exists and is mature
- source-stream expected line volume for the current bucket is above the configured floor
- no current activity is observed for the source stream after the configured missed-window
  threshold
- no equivalent source-stream hard-silence interval is already open
- the source stream is not suppressed by invalid state, low expected activity, rotation,
  retirement, cold start, disabled tenant/device state, or policy controls

Hard silence is the full-drop case and takes priority over source-stream sharp drop for
the same source stream/window.

## Sharp-drop semantics

A source-stream sharp-drop candidate exists only when:

- source-stream V_DROP policy is enabled
- source-stream expected-source state exists and is mature
- expected line volume for the current bucket is above the configured floor
- current observed lines for the source stream are nonzero
- observed line volume satisfies the Phase 26/27 sharp-drop ratio gates
- no equivalent source-stream sharp-drop interval is already open
- no matching source-stream hard-silence interval is open

Ratio terms remain:

```text
observed_expected_ratio = observed_lines / expected_lines
drop_ratio = 1.0 - observed_expected_ratio
```

The first source-stream sharp-drop implementation should reuse the locked Phase 26/27
sharp-drop defaults unless a later contract changes them.

## State and dedup keys

Future source-stream state must use source-stream-specific key paths.

Planned keys:

```text
silence_open/v1/source_stream/<device_key>/<source_stream_id>
drop_open/v1/source_stream/<device_key>/<source_stream_id>
```

Planned values:

- `OpenSilenceStateV1` with subject kind source_stream for hard silence
- `OpenDropStateV1` with subject kind source_stream for sharp drop

Rules:

- source-stream open state must not reuse device or tenant open-state keys
- source-stream open state does not automatically suppress device or tenant aggregate state
- device or tenant aggregate open state does not automatically suppress source-stream state
- hard silence suppresses sharp drop only for the same source stream/window
- recovery closes matching source-stream open state only for the same source stream

## AlertV1 explanation and provenance

Future source-stream alerts must reuse the existing `AlertV1` schema.

Required reason details for source-stream alerts:

- `subject_kind=source_stream`
- `tenant_id=<tenant_id>`
- `device_key=<device_key>`
- `source_stream_id=<source_stream_id>`
- `source_path=<canonical_relative_source_path>` when safe and available
- `window_start_ts=<ts>`
- `window_end_ts=<ts>`
- `bucket=<bucket>`
- `expected_lines=<value>`
- `observed_lines=<value>`
- `drop_kind=hard_silence` for source-stream hard-silence alerts
- `drop_kind=sharp_drop` for source-stream sharp-drop alerts
- `drop_ratio=<value>` for sharp-drop alerts

Alert id construction must include `source_stream`, `source_stream_id`, the drop kind, and
the window bounds so source-stream alerts cannot collide with device or tenant aggregate
alerts.

Provenance rules:

- source-stream hard-silence alerts may have empty provenance
- source-stream sharp-drop alerts should include current source-stream spans when available
- span selection must be deterministic and capped
- `AlertV1.provenance` remains authoritative
- no `source_files` field or alternate drilldown model may be introduced

## Policy controls

Source-stream V_DROP should not be activated implicitly by the existing device/tenant
subject gates.

Recommended future global config field:

```text
[vdrop]
source_stream_enabled = false
```

Recommended future tenant-policy override field:

```text
vdrop_source_stream_enabled = inherit|true|false
```

Rules:

- `vdrop.enabled` must still be true before any source-stream V_DROP alert can emit
- `source_stream_enabled` defaults to false for first implementation safety
- tenant policy override takes precedence over global config
- source-stream-specific threshold knobs remain deferred; first implementation inherits
  existing V_DROP mature-window and expected-line floors plus the locked sharp-drop
  defaults
- invalid policy must fail closed for source-stream V_DROP evaluation

## Diagnostics boundary

Allowed future diagnostic concepts:

- aggregate source-stream subjects tracked
- aggregate source-stream subjects evaluated
- aggregate source-stream candidates suppressed
- aggregate source-stream alerts emitted
- aggregate open source-stream hard-silence intervals
- aggregate open source-stream sharp-drop intervals
- bounded per-tenant totals

Prohibited without a later contract:

- device-label Prometheus metrics
- source-path-label Prometheus metrics
- source-stream-id-label Prometheus metrics
- parser-class-label Prometheus metrics
- vendor-family-label Prometheus metrics
- per-subject Prometheus series
- suppression-reason label cardinality

## Recommended Phase 31 implementation split

Phase 30 recommends the following implementation sequence:

1. Phase 31a: source-stream identity, canonicalization, catalog, stats/state encodings, and
   storage helpers.
2. Phase 31b: source-stream evaluator primitives for hard silence and sharp drop.
3. Phase 31c: source-stream AlertV1 construction and source-stream open-state dedup
   primitives.
4. Phase 31d: source-stream policy/config gating behind the default-off source-stream gate. Complete.
5. Phase 31e: source-stream runtime integration behind the default-off source-stream gate.
6. Phase 31f: bounded diagnostics, deterministic validation, and checkpoint closeout.

Each implementation subphase should end with a checkpoint zip, summary, remaining work,
and updated progress checklist.

## Acceptance gates for later implementation

Before source-stream runtime activation is accepted:

- cargo fmt/check/test results must be provided by the user from a Rust environment
- source-stream identity tests must prove stable hash determinism and invalid path
  rejection
- stats/state codec tests must prove deterministic roundtrip and malformed rejection
- evaluator tests must cover hard silence, sharp drop, low expected activity, immature
  state, duplicate suppression, recovery, and hard-silence priority
- runtime tests must cover `oneshot` and `run` paths with source-stream gate disabled and
  enabled
- alert tests must prove source-stream alert id non-collision against device and tenant
  aggregate alerts
- diagnostics tests must prove no device/source-path/source-stream labels are emitted
- replay behavior must remain unchanged
- recovery behavior must remain unchanged
- `AlertV1.provenance` must remain authoritative
- `DeviceStatsV1` 68-byte layout must remain unchanged

## Phase 30 closeout

Phase 30 is complete when:

- the source-stream implementation plan is recorded
- Contract 39 is added
- docs, contracts, checklist, and phase history are updated
- no runtime source files or tests are changed
- a checkpoint zip is produced
