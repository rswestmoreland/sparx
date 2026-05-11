# Source-Stream V_DROP Contract v0.1

Status: implemented for v1 behind the default-off source-stream gate.

## Purpose

This contract defines source-stream `V_DROP`, the first richer subject family
after device and tenant aggregate hard-silence and sharp-drop detection.

## Preserved behavior

- device hard-silence `V_DROP` remains active
- tenant aggregate hard-silence `V_DROP` remains active
- device sharp-drop `V_DROP` remains active
- tenant aggregate sharp-drop `V_DROP` remains active
- `silence_open/*` remains hard-silence dedup state
- `drop_open/*` remains sharp-drop dedup state
- `AlertV1.provenance` remains authoritative
- `DeviceStatsV1` remains the locked 68-byte device/bucket stats layout
- active `alert_idx_*` persistence remains current truth
- diagnostics remain bounded and low-cardinality
- hashed-fallback FeatureId behavior remains absent

## Source-stream subject identity

A source stream is a tenant/device-specific log source represented by a canonical
relative source path under the device directory.

Subject kind value:

```text
SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1 = 3
```

Subject identifier:

```text
source_stream_id = stable_hash_hex128_v1(source_stream_contract_input)
```

Canonical contract input string:

```text
source_stream/v1
tenant_id=<tenant_id>
device_key=<device_key>
source_path=<canonical_relative_source_path>

```

The stable hash rule is BLAKE3 first 16 digest bytes encoded as lowercase hex.
`source_stream_id` is a subject identifier, not a `FeatureId`.

## Canonical relative source path

Rules:

- separators are `/`
- path is relative and non-empty
- absolute paths are rejected
- empty components are rejected
- `.` and `..` components are rejected
- ASCII control characters, tab, newline, and NUL are rejected
- case is preserved
- tenant root, absolute host paths, and DB paths are not included
- symlink handling follows existing ingest behavior

Invalid paths suppress source-stream state updates and candidate evaluation for
that path fail-closed.

## Source-stream storage

Source-stream state uses separate catalog, stats, expected-source, provenance,
and open-state key families. It must not change `DeviceStatsV1`.

Active tenant DB key families:

```text
source_stream/v1/<device_key>/<source_stream_id>/catalog
source_stats/v1/<device_key>/<source_stream_id>/<bucket>
source_prov/v1/<device_key>/<source_stream_id>/<window_start>
silence_subject/v1/source_stream/<device_key>/<source_stream_id>/state
silence_open/v1/source_stream/<device_key>/<source_stream_id>
drop_open/v1/source_stream/<device_key>/<source_stream_id>
```

## Observation derivation

Source-stream observations are derived from finalized-window file spans. For each
finalized window, spans are grouped by canonical relative source path.

Derived values:

- observed line count is the number of spans in the group
- observed byte count is the sum of valid `offset_end - offset_start` values
- provenance is deterministically capped from spans in the group

Invalid offsets must not produce negative values. Invalid source paths are
suppressed fail-closed for source-stream observation.

## Hard-silence semantics

A source-stream hard-silence candidate may be produced only when:

- global V_DROP is enabled
- source-stream V_DROP policy is enabled
- expected-source state exists for the source stream
- the source-stream subject is mature
- expected line volume meets the configured floor
- missed windows meet the hard-silence threshold
- no equivalent source-stream hard-silence interval is already open
- subject state is valid and not disabled or low expected activity

Hard silence is the full-drop case and suppresses sharp drop for the same source
stream/window.

## Sharp-drop semantics

A source-stream sharp-drop candidate may be produced only when:

- global V_DROP is enabled
- source-stream V_DROP policy is enabled
- source-stream stats exist for the bucket
- the source-stream baseline is mature
- expected line volume meets the configured floor
- observed line count is nonzero
- observed/expected ratio is below the configured sharp-drop threshold
- no equivalent source-stream sharp-drop interval is already open

Ratio semantics:

```text
observed_expected_ratio = observed_lines / expected_lines
drop_ratio = 1.0 - observed_expected_ratio
```

## Alert behavior

Source-stream alerts reuse the existing `AlertV1` schema. They include
source-stream subject details in deterministic reason details. Runtime source-
stream alerts are emitted only when the resolved source-stream gate is enabled.

## Diagnostics

Source-stream diagnostics are active and bounded. Aggregate and per-tenant values
may be surfaced through status, JSON status, Prometheus metrics, and health
output. Per-tenant metrics may use `tenant_id` only.

The following Prometheus labels remain prohibited:

- device
- source path
- source-stream id
- parser class
- vendor family
- per-subject state
- suppression reason

## Deferred scope

The following remain outside this contract:

- parser-class subjects
- vendor-event-family subjects
- heartbeat checks
- maintenance calendars
- cross-tenant outage correlation
- source-stream-specific threshold knobs
- AlertV1 schema changes
- replay or recovery semantic changes
