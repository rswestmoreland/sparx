# Phase 31a Source-Stream Identity, Catalog, Stats-State Primitives

Status: complete as an implementation-primitives checkpoint.

Phase 31a starts the source-stream `V_DROP` implementation sequence defined in Phase 30.
It adds identity, canonical path, catalog, source-stream stats, source-stream
expected-source state helpers, and deterministic tests. It does not activate
source-stream `V_DROP` runtime evaluation or alert emission.

No local cargo/build/test/rustfmt run was performed in this sandbox.

## Scope

Implemented in Phase 31a:

- source-stream path canonicalization and rejection helpers
- stable `source_stream_id` derivation from tenant id, device key, and canonical relative
  source path
- `SourceStreamCatalogV1` value and codec
- `SourceStreamStatsV1` value and codec
- source-stream stats update helper using Welford line-count and byte-count state
- source-stream subject kind support for `ExpectedSourceStateV1`
- source-stream tenant DB key builders
- tenant DB read/write/list helpers for source-stream catalog, stats, and expected-source
  state
- deterministic tests for identity, invalid path rejection, codec roundtrips,
  malformed values, key paths, and tenant DB persistence helpers

Out of scope for Phase 31a:

- source-stream runtime evaluation
- source-stream hard-silence candidate evaluation
- source-stream sharp-drop candidate evaluation
- source-stream AlertV1 construction
- source-stream open-state dedup
- source-stream policy/config fields
- source-stream diagnostics/metrics
- replay or recovery changes

## Identity contract

A source stream is represented by a canonical relative source path under a device
directory. Phase 31a adds the helper path:

```text
source_stream_id = stable_hash_hex128_v1(source_stream_contract_input)
```

The canonical contract input is:

```text
source_stream/v1
tenant_id=<tenant_id>
device_key=<device_key>
source_path=<canonical_relative_source_path>

```

The stable hash rule remains BLAKE3 first 16 digest bytes encoded as lowercase hex.
`source_stream_id` is not a FeatureId and is not stored in the feature dictionary.

## Canonical path rules

`canonicalize_source_stream_path_v1()` enforces the Phase 30 source-path rules:

- separators are normalized to `/`
- paths must be relative and non-empty
- absolute paths are rejected
- empty, `.`, and `..` components are rejected
- ASCII control characters, tab, newline, and NUL are rejected
- case is preserved

Invalid source paths fail closed at the primitive layer.

## Catalog primitive

Phase 31a adds `SourceStreamCatalogV1` under planned key builders for:

```text
source_stream/v1/<device_key>/<source_stream_id>/catalog
```

Fields:

- schema version
- source stream id
- device key
- canonical relative source path
- first seen timestamp
- last seen timestamp
- state flags
- reserved fields

Encoding is deterministic with a 28-byte fixed header plus variable UTF-8 bytes for the
source-stream id, device key, and canonical source path. The codec rejects unknown schema
versions, invalid ids, nonzero reserved fields, invalid timestamp bounds, malformed string
lengths, trailing bytes, and unsafe paths.

## Stats primitive

Phase 31a adds `SourceStreamStatsV1` under planned key builders for:

```text
source_stats/v1/<device_key>/<source_stream_id>/<bucket>
```

The encoded length is 68 bytes. It uses the same Welford-family field shape as
`DeviceStatsV1`, but it is a separate source-stream type and does not modify the locked
`DeviceStatsV1` layout.

Fields:

- line-count Welford state as the primary expected-volume signal
- byte-count Welford state as explanation-only
- score-total Welford state reserved and zeroed
- last update timestamp

The stats update helper updates line-count and byte-count Welford state from observed
source-stream lines and bytes. Score-total remains zeroed unless a later contract approves
its use.

## Expected-source state primitive

Phase 31a extends the accepted `ExpectedSourceStateV1` subject-kind values with:

```text
SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1 = 3
```

The existing 68-byte `ExpectedSourceStateV1` layout remains unchanged. Source-stream
expected-source state uses separate key builders and tenant DB helpers for:

```text
silence_subject/v1/source_stream/<device_key>/<source_stream_id>/state
```

Device and tenant expected-source keys and behavior are unchanged.

## Tenant DB helpers

Phase 31a adds tenant DB helpers for:

- read/write/list source-stream catalog records for a device
- read/write/list source-stream stats records by bucket
- read/write/update/list source-stream expected-source state records for a device

These helpers are storage primitives only. Runtime source-stream evaluation does not call
them yet.

## Preserved behavior

Phase 31a preserves:

- active device and tenant aggregate hard-silence `V_DROP`
- active device and tenant aggregate sharp-drop `V_DROP`
- `AlertV1` schema and provenance authority
- `DeviceStatsV1` 68-byte layout
- existing device/tenant `silence_subject/*` behavior
- existing `silence_open/*` and `drop_open/*` behavior
- config and tenant-policy schemas
- diagnostics/metrics behavior
- replay behavior
- recovery behavior

## Tests added or updated

Phase 31a adds deterministic test coverage for:

- source-stream path canonicalization
- unsafe source-path rejection
- source-stream id determinism
- catalog encoding/decoding and malformed rejection
- catalog first/last seen updates
- source-stream stats encoding/decoding and malformed rejection
- Welford source-stream stats updates
- source-stream tenant DB key paths
- source-stream expected-source subject kind acceptance
- tenant DB source-stream catalog/stats/expected-state persistence helpers

## Validation notes

Performed in this sandbox:

- source and test files were edited for Phase 31a primitives
- docs/contracts/history were updated
- ASCII-only scan passed for text/source/docs files
- repo-relative path length remained within the 260-character cap
- checkpoint zip integrity was verified

Not performed in this sandbox:

- cargo fmt
- cargo check
- cargo test
- rustfmt

## Next recommended phase

Recommended next phase: Phase 31b source-stream evaluator primitives.

Phase 31b should remain storage-agnostic evaluator work and should not add runtime
source-stream alert emission. It should use the Phase 31a identity/catalog/stats-state
primitives and produce deterministic evaluator tests for source-stream hard silence and
sharp drop.
