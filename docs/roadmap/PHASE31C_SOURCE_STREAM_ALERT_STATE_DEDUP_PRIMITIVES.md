# Phase 31c Source-Stream AlertV1 and Open-State/Dedup Primitives

Status: complete as an implementation-primitives checkpoint. Source-stream runtime V_DROP
remains inactive.

## Goal

Add source-stream AlertV1 construction and source-stream-specific open-state/dedup
primitives without activating source-stream runtime evaluation.

Phase 31c builds on Phase 31a identity/catalog/stats-state primitives and Phase 31b
evaluator primitives. It prepares the AlertV1 and open-state pieces needed by later
policy-gated runtime integration.

## Implemented

- fixed a small Phase 31b syntax typo in `src/alert/mod.rs` before adding Phase 31c
  primitives
- added source-stream hard-silence AlertV1 construction
- added source-stream sharp-drop AlertV1 construction
- reused the existing AlertV1 schema with no new fields
- preserved `AlertV1.provenance` authority for source-stream sharp-drop construction
- added deterministic source-stream alert id inputs that include:
  - tenant id
  - source_stream subject family
  - source_stream_id
  - window start
  - window end
  - V_DROP
  - drop kind
- added source-stream-specific open-silence construction helpers
- added source-stream-specific open-drop construction helpers
- added source-stream-specific open-state suppression helpers
- added source-stream hard-silence key builders under
  `silence_open/v1/source_stream/<device_key>/<source_stream_id>`
- added source-stream sharp-drop key builders under
  `drop_open/v1/source_stream/<device_key>/<source_stream_id>`
- added tenant DB read/write/list helpers for source-stream open-silence states
- added tenant DB read/write/list helpers for source-stream open-drop states
- added deterministic tests in:
  - `tests/alert_scoring.rs`
  - `tests/source_stream.rs`
  - `tests/db_keys.rs`
  - `tests/db_tenant.rs`

## Preserved boundaries

Phase 31c does not add:

- source-stream runtime evaluation
- source-stream run integration
- source-stream oneshot integration
- source-stream policy/config fields
- source-stream metrics
- source-stream status output
- source-stream health output
- replay behavior changes
- recovery behavior changes
- AlertV1 schema changes
- DeviceStatsV1 layout changes
- hashed-fallback FeatureId behavior
- parser-class or vendor-event-family subject behavior

## AlertV1 construction notes

Source-stream alert builders reuse existing AlertV1 fields.

For source-stream hard silence:

- `reason.code` remains `V_DROP`
- first reason detail is `drop_kind=hard_silence`
- the alert has empty provenance because no finalized source-stream window exists for the
  missing window
- `device_key` remains the source device key
- `device_path` is a deterministic source-stream display path

For source-stream sharp drop:

- `reason.code` remains `V_DROP`
- first reason detail remains `drop_kind=sharp_drop`
- source-stream provenance is accepted as `FileSpanV1` input and capped through the
  existing provenance cap helper
- score semantics remain `score_volume = clamp01(drop_ratio)` and `score_total = score_volume`

## Open-state notes

Phase 31c adds subject-specific helpers because `OpenSilenceStateV1` and
`OpenDropStateV1` intentionally remain compact state records. Source-stream identity is
bound by the key path and by the caller-provided `SourceStreamSubjectV1`.

Source-stream hard-silence open state uses:

- key family: `silence_open/v1/source_stream`
- subject kind: `SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1`
- alert id from the source-stream hard-silence AlertV1 builder

Source-stream sharp-drop open state uses:

- key family: `drop_open/v1/source_stream`
- subject kind: `SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1`
- alert id from the source-stream sharp-drop AlertV1 builder

## Validation performed in this sandbox

- ASCII-only scan
- path-length scan
- stale-marker scan for common unfinished-code markers
- checkpoint zip integrity check

No local cargo/build/test/rustfmt run was performed in this sandbox.

## Next phase

Phase 31d later completed source-stream policy/config gating behind the default-off
source-stream gate.

Runtime integration should remain Phase 31e. Diagnostics, validation, and closeout should
were completed in Phase 31f.
