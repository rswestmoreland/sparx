# V_DROP Richer Subject Scope Contract v0.1

This contract records the richer subject-scope decision for `V_DROP`.

## Current active subjects

- device hard silence
- tenant aggregate hard silence
- device sharp drop
- tenant aggregate sharp drop
- source-stream hard silence and sharp drop behind the default-off source-stream
  gate

## Subject-family decision

Source stream is the first richer subject family included in v1 scope.

Deferred subject families:

- parser class
- vendor event family
- external heartbeat subjects
- maintenance-calendar-aware subjects
- cross-tenant outage correlation subjects

## Source-stream subject model

A source stream is a tenant/device-scoped log source derived from a canonical
relative source path under the device directory. The subject uses a stable
internal identifier:

```text
source_stream_id = stable_hash_hex128_v1(source_stream_contract_input)
```

The identifier is not a `FeatureId` and must not revive hashed-fallback FeatureId
behavior.

## Source-stream rules

- source paths are canonical relative paths, not absolute host paths
- tenant id and device key are included in the identity input
- display paths must not become Prometheus labels
- source-stream state is separate from device and tenant state
- source-stream stats are separate from `DeviceStatsV1`
- source-stream hard silence uses `silence_open/v1/source_stream/*`
- source-stream sharp drop uses `drop_open/v1/source_stream/*`
- source-stream behavior is disabled by default unless policy enables it

## Diagnostics boundary

Allowed diagnostics are aggregate or bounded per-tenant counts. Metric label
fanout by device, source path, source-stream id, parser class, vendor family,
per-subject state, or suppression reason remains prohibited.

## Deferred rationale

Parser-class subjects are deferred until parser-class accounting and operator
semantics are explicitly scoped. Vendor-event-family subjects are deferred until
stable taxonomy/content-pack family classification is explicitly scoped.
