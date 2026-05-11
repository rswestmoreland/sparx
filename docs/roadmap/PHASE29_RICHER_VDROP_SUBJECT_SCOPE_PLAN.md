# Phase 29 Richer V_DROP Subject-Scope Plan

Status: complete as a documentation-only planning and contract checkpoint.

Phase 29 follows the Phase 28 post-sharp-drop review. It does not add runtime behavior,
tests, persisted keys, config fields, tenant-policy fields, metrics, replay behavior,
recovery behavior, or alert schema changes.

No local cargo/build/test/rustfmt run was performed in this sandbox.

## Purpose

Phase 29 scopes the next possible expansion of `V_DROP` beyond the currently active
subjects:

- device hard silence
- tenant aggregate hard silence
- device sharp drop
- tenant aggregate sharp drop

The goal is to decide which richer subject family should be considered first and to lock
the boundaries that any later implementation must preserve.

## Current active baseline

The following behavior is current truth and remains unchanged by Phase 29:

- hard-silence `V_DROP` is active for device and tenant aggregate subjects
- sharp-drop `V_DROP` is active for device and tenant aggregate subjects
- `silence_open/*` is hard-silence state
- `drop_open/*` is sharp-drop state
- `AlertV1.provenance: Vec<FileSpanV1>` is authoritative
- `DeviceStatsV1` remains the locked 68-byte device/bucket stats layout
- V_DROP policy controls remain active through the existing global config and tenant-policy
  override fields
- V_DROP diagnostics remain bounded and low-cardinality
- no device-label, source-path-label, parser-class-label, or vendor-family-label
  Prometheus fanout is active or approved

## Candidate subject families

Phase 29 compares three possible richer subject families.

### 1. Source stream / source path

Definition:

- a tenant/device-specific log source, normally derived from a canonical file path under
  the device directory
- represented internally by a stable source-stream subject id, not by raw path labels in
  metrics

Strengths:

- strong operator value: catches one important log stream stopping while the device still
  emits other logs
- good provenance fit: current finalized rows already retain file-span provenance
- clear drill/extract story for sharp-drop windows where current spans exist
- directly related to the ingest model and sparse window pipeline

Risks:

- higher cardinality than device or tenant aggregate subjects
- file rotation and rename behavior can create false positives if identity is not
  canonicalized carefully
- raw paths can leak environment details if exposed in metric labels or unstable keys
- expected-volume baselines require a separate source-stream stats model; the locked
  DeviceStatsV1 layout must not be changed

Required controls:

- use canonical relative source identity, never absolute host paths in persisted key
  components
- use the stable hash rule for internal `SourceStreamId` values
- keep any path display value out of Prometheus labels
- require maturity and minimum expected volume per source stream
- cap or policy-gate tracked source streams before implementation if needed
- close or suppress retired/rotated streams deterministically

Recommendation:

- selected as the first future richer subject to scope for implementation

Rationale:

- it is the most directly useful operational expansion after device/tenant V_DROP
- it stays closest to the existing sparse matrix ingest model
- it can use current file-span evidence for explanations without creating taxonomy or
  vendor-content dependencies

### 2. Parser class

Definition:

- a low-cardinality subject based on the parser or tokenizer route used for normalized
  events, such as syslog, key/value, JSON, CSV, CEF, or plaintext fallback

Strengths:

- low cardinality compared with source paths
- useful for detecting parser-pipeline regressions or format-specific dropouts
- no raw path disclosure risk if labels are kept aggregate-only

Risks:

- weaker operator actionability than source stream; a parser-class drop may not identify
  the real failed log source
- expected-volume quality depends on consistent parser classification for every line or
  event
- parser route changes can look like volume drops even when ingest is healthy

Decision:

- defer until after source-stream scope, or use as a later cross-check if source-stream
  cardinality is too costly

### 3. Vendor event family

Definition:

- a semantic family such as authentication, firewall traffic, endpoint process, DNS, or
  cloud identity activity, derived from normalized fields or content packs

Strengths:

- high analyst value when a family disappears but the device still emits unrelated logs
- aligns with future enrichment and taxonomy work

Risks:

- requires stable vendor/event-family classification that is not yet locked as a V_DROP
  subject identity
- more vulnerable to content-pack drift and vendor-specific behavior
- harder false-positive controls because business activity can naturally vary by family

Decision:

- defer until the taxonomy/content-pack model for these families is explicitly stable

## Phase 29 decision matrix

| Candidate | Operator value | Cardinality risk | Expected-volume quality | Provenance quality | Implementation dependency | Recommendation |
| --- | --- | --- | --- | --- | --- | --- |
| Source stream / source path | High | Medium to high | Good if source stats are mature | Strong for sharp-drop windows | Needs source-stream stats and catalog | First richer subject |
| Parser class | Medium | Low | Medium | Mixed | Needs line/event parser-class accounting | Later |
| Vendor event family | High | Medium | Medium until taxonomy matures | Mixed | Needs stable family classification/content packs | Later |

## Selected first richer subject: source stream

The recommended first richer subject is a **source stream**. A source stream is a stable,
tenant-scoped subject derived from a device and a canonical relative log source path.

A future implementation should use a subject identity like:

```text
source_stream_id = stable_hash("source_stream/v1" + tenant_id + device_key + canonical_source_path)
```

This uses the existing stable hash rule: BLAKE3, first 16 digest bytes, lowercase hex.
This is not a FeatureId and must not revive hashed-fallback FeatureId behavior.

Recommended subject detail fields for future AlertV1 reason details:

- `subject_kind=source_stream`
- `tenant_id=<tenant_id>`
- `device_key=<device_key>`
- `source_stream_id=<stable hash>`
- `source_path=<canonical relative path>` when safe and available
- existing hard-silence or sharp-drop fields already used by V_DROP

## Future source-stream expected-volume model

Source-stream V_DROP should not change the locked `DeviceStatsV1` 68-byte layout.

A later implementation should add separate source-stream expected-volume state or stats if
approved. The likely model is:

- per tenant
- per device
- per source stream
- per bucket
- line-count Welford baseline as primary signal
- byte-count Welford baseline as explanation-only unless later approved

The first implementation should not depend on external heartbeats, maintenance calendars,
or vendor taxonomy. It should derive source-stream observations from the ingest/finalized
window path already present in the sparse matrix pipeline.

## Future source-stream hard-silence semantics

A source-stream hard-silence candidate should mean:

- the source stream has mature expected activity for the current bucket
- the device or tenant still has enough context to evaluate the source stream
- the source stream has no observed activity for the configured missed-window threshold
- the source stream is not suppressed by retirement, rotation, cold start, low expected
  activity, disabled tenant/device state, or policy controls

Hard silence remains the full-drop case and must take priority over source-stream
sharp-drop for the same source stream/window.

## Future source-stream sharp-drop semantics

A source-stream sharp-drop candidate should mean:

- the source stream has mature expected activity for the current bucket
- the source stream has nonzero observed activity in the current finalized window
- observed line volume is far below expected line volume using the existing Phase 26/27
  ratio terminology
- `observed_expected_ratio = observed_lines / expected_lines`
- `drop_ratio = 1.0 - observed_expected_ratio`
- hard silence does not apply for the same source stream/window

The first source-stream sharp-drop implementation should reuse the existing sharp-drop
planning defaults unless an explicit later contract changes them.

## Future state and dedup direction

A later implementation may add source-stream variants of existing V_DROP state families,
but Phase 29 does not create keys or encodings.

Recommended future key direction, subject to a later encoding contract:

- `silence_subject/v1/source_stream/<device_key>/<source_stream_id>/state`
- `silence_open/v1/source_stream/<device_key>/<source_stream_id>`
- `drop_open/v1/source_stream/<device_key>/<source_stream_id>`

Rules:

- source-stream state must not alter existing device or tenant state encodings
- source-stream hard-silence state must not reuse device or tenant `silence_open/*` keys
- source-stream sharp-drop state must not reuse device or tenant `drop_open/*` keys
- open source-stream state must not automatically suppress device or tenant aggregate
  state, and device/tenant state must not automatically suppress source-stream state,
  except for explicitly documented hard-silence priority within the same subject

## Future AlertV1 and provenance direction

A future source-stream implementation should reuse the existing `AlertV1` schema.

Rules:

- reason remains `V_DROP`
- hard-silence detail should include `drop_kind=hard_silence` if the implementation needs
  disambiguation; otherwise it should preserve the existing hard-silence detail surface
- sharp-drop detail must include `drop_kind=sharp_drop`
- alert id inputs must include `source_stream` and `source_stream_id`
- `AlertV1.provenance` remains authoritative
- hard-silence source-stream alerts may have empty provenance when no current span exists
- sharp-drop source-stream alerts should include current source-stream spans when
  available and deterministic after capping

No `AlertV1` schema change is approved by Phase 29.

## Future diagnostics direction

Phase 29 keeps diagnostics scope conservative.

Allowed diagnostic concepts for a future implementation:

- aggregate number of tracked source streams
- aggregate number of eligible source streams evaluated
- aggregate source-stream hard-silence candidates
- aggregate source-stream sharp-drop candidates
- aggregate source-stream alerts emitted
- aggregate source-stream suppressions
- aggregate open source-stream silence/drop intervals
- bounded per-tenant totals

Prohibited diagnostics unless a later contract explicitly approves them:

- source-path labels
- source-stream-id labels
- device labels
- parser-class labels
- vendor-family labels
- per-subject Prometheus series
- suppression-reason label cardinality

## Deferred after Phase 29

Phase 29 does not implement source-stream V_DROP. It also keeps the following deferred:

- parser-class V_DROP subjects
- vendor-event-family V_DROP subjects
- external heartbeat checks
- maintenance-window calendars
- cross-tenant outage correlation
- source-stream-specific config or tenant-policy knobs
- AlertV1 schema changes
- DeviceStatsV1 layout changes
- replay behavior changes
- recovery behavior changes

## Recommended next phase

Recommended next phase:

- Phase 30: source-stream V_DROP implementation planning

Recommended Phase 30 sequence:

- 30a source-stream identity and catalog contract
- 30b source-stream stats/state encoding contract
- 30c source-stream evaluator primitives
- 30d source-stream AlertV1 and dedup primitives
- 30e source-stream runtime integration
- 30f diagnostics, tests, and closeout

Do not implement Phase 30 runtime behavior until the identity, state, cardinality, test,
and diagnostic contracts are approved.
