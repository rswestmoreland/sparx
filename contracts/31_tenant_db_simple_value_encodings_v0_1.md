# Tenant DB Simple Value Encodings Contract v0.1

This contract defines byte-level encodings for tenant DB keys that are not covered by:
- Open-Window Checkpoint Encoding v0.1 (win_active, win_row/*)
- Baseline Sketch Encoding v0.1 (df ring, centroid, stats)

Scope:
- `meta/*`
- `dev/*`
- `cursor/*`
- `feat_dict/*`
- `metrics/*` (if enabled)
- `silence_subject/*` and `silence_open/*` (`silence_subject/*` runtime writers active in the current release; `silence_open/*` runtime dedup writes active in the current release)
- `migrate/*`

All keys referenced here are defined by Tenant DB Key Prefix Map v0.1.

## Goals
- Every persisted value has a precise, versioned encoding.
- Encodings are deterministic and easy to test.
- Favor fixed structs for hot keys, varints for variable maps.

---

## 1) Common encoding rules

### 1.1 Endianness
- Fixed integers are little-endian.

### 1.2 Varints
- Unsigned LEB128 varints are used for:
  - lengths
  - u32 FeatureId
  - u32 counts

### 1.3 Strings
- UTF-8 bytes
- Encoding: `varint(len)` + bytes

---

## 2) `meta/*` values

### 2.1 Schema
- `meta/schema/v1/version` -> u32 LE
- `meta/schema/v1/created_ts` -> i64 LE
- `meta/schema/v1/last_migrate_ts` -> i64 LE

### 2.2 Ingest
- `meta/ingest/v1/last_flush_ts` -> i64 LE
- `meta/ingest/v1/worker_epoch` -> u64 LE

### 2.3 DF ring meta
- `meta/df_ring/v1/current_day_epoch` -> i64 LE
- `meta/df_ring/v1/day_slot_epoch/<slot>` -> i64 LE
- `meta/df_ring/v1/last_roll_epoch` -> i64 LE

### 2.4 Feature dict meta
- `feat_dict/v1/meta/next_id` -> u32 LE
- `feat_dict/v1/meta/entries` -> u32 LE
- `feat_dict/v1/meta/last_gc_ts` -> i64 LE

---

## 3) `dev/*` values

### 3.1 Device path
- `dev/v1/<device_key>/path` -> string

### 3.2 Device timestamps
- `dev/v1/<device_key>/created_ts` -> i64 LE
- `dev/v1/<device_key>/last_seen_ts` -> i64 LE

---

## 4) `cursor/*` values

Cursor values are stored as fixed-size fields per key (no struct packing across keys).

### 4.1 inode
- `cursor/v1/<device_key>/<file_key>/inode` -> u64 LE

### 4.2 mtime
- `cursor/v1/<device_key>/<file_key>/mtime` -> i64 LE

### 4.3 size
- `cursor/v1/<device_key>/<file_key>/size` -> u64 LE

### 4.4 offset
- `cursor/v1/<device_key>/<file_key>/offset` -> u64 LE

### 4.5 is_gzip
- `cursor/v1/<device_key>/<file_key>/is_gzip` -> u8 (0 or 1)

### 4.6 last_read_ts
- `cursor/v1/<device_key>/<file_key>/last_read_ts` -> i64 LE

Notes:
- Offsets for gzip are compressed-stream offsets.
- On inode change, offset resets to 0 and `cursor_resets_total` increments.

---

## 5) `feat_dict/*` values

### 5.1 String to id
- `feat_dict/v1/str/<feature_string>` -> u32 LE FeatureId

### 5.2 Id to string
- `feat_dict/v1/id/<feature_id_u32>` -> string feature_string

Notes:
- `feature_string` keys are the canonical emitted feature strings.
- Reverse-map is required for explainability.

---

## 6) `metrics/*` values (optional persistence)

### 6.1 Counters
- `metrics/v1/counter/<name>` -> u64 LE

### 6.2 Gauges
- `metrics/v1/gauge/<name>` -> f64 LE

---

## 7) `silence_subject/*` and `silence_open/*` values

the current release defines the value encodings for `V_DROP` / sudden loss-of-log detection.
the current release activates runtime writers for `silence_subject/*` expected-source state from
finalized windows. the current release activates runtime `silence_open/*` writes for duplicate
suppression when hard-silence `V_DROP` alerts are emitted.

### 7.1 Subject kinds

- `1` -> device
- `2` -> tenant aggregate

### 7.2 `ExpectedSourceStateV1`

- key: `silence_subject/v1/device/<device_key>/state` or `silence_subject/v1/tenant/state`
- encoded length: 68 bytes
- fixed little-endian fields, in order:
  - `schema_version_u16` -> u16 LE, value 1
  - `subject_kind_u8` -> u8
  - `state_flags_u8` -> u8
  - `window_size_s_u32` -> u32 LE
  - `observed_windows_total_u64` -> u64 LE
  - `mature_windows_total_u64` -> u64 LE
  - `last_seen_window_start_ts_i64` -> i64 LE
  - `last_seen_window_end_ts_i64` -> i64 LE
  - `last_observed_lines_u64` -> u64 LE
  - `last_observed_bytes_u64` -> u64 LE
  - `last_bucket_u8` -> u8
  - `reserved_u8_0` -> u8, value 0
  - `reserved_u16_0` -> u16 LE, value 0
  - `last_update_ts_i64` -> i64 LE

Decode rules:
- reject unknown schema versions
- reject unknown subject kinds
- reject nonzero reserved fields
- reject invalid lengths

Update rules active in the current release:
- `observed_windows_total_u64` increments for each finalized-window update
- `mature_windows_total_u64` increments only when the finalized window meets
  `scoring.min_lines_per_window`, or when that floor is zero
- last-seen window fields update only for non-regressive window-end timestamps
- older replayed windows may advance counters but must not move last-seen fields backward

### 7.3 `OpenSilenceStateV1`

- key: `silence_open/v1/device/<device_key>` or `silence_open/v1/tenant`
- fixed header length: 30 bytes
- fixed little-endian fields, in order:
  - `schema_version_u16` -> u16 LE, value 1
  - `subject_kind_u8` -> u8
  - `state_flags_u8` -> u8
  - `silence_start_ts_i64` -> i64 LE
  - `last_alert_window_start_ts_i64` -> i64 LE
  - `last_alert_window_end_ts_i64` -> i64 LE
  - `last_alert_id_len_u16` -> u16 LE
  - `last_alert_id_bytes` -> ASCII lowercase hex bytes of declared length

Flags:
- bit 0: open silence interval exists
- bit 1: interval closed by later observation

Decode rules:
- reject unknown schema versions
- reject unknown subject kinds
- reject malformed alert id lengths or trailing bytes
- reject alert id bytes outside ASCII lowercase hex


### 7.4 `OpenDropStateV1` encoding

`OpenDropStateV1` stores sharp-drop dedup state under `drop_open/*` keys.

Valid key families:

- `drop_open/v1/device/<device_key>`
- `drop_open/v1/tenant`
- `drop_open/v1/source_stream/<device_key>/<source_stream_id>`

Encoded shape:

- fixed header length: 30 bytes
- fixed little-endian fields, in order:
  - `schema_version_u16` -> u16 LE, value 1
  - `subject_kind_u8` -> u8
  - `state_flags_u8` -> u8
  - `drop_start_ts_i64` -> i64 LE
  - `last_alert_window_start_ts_i64` -> i64 LE
  - `last_alert_window_end_ts_i64` -> i64 LE
  - `last_alert_id_len_u16` -> u16 LE
  - `last_alert_id_bytes` -> ASCII lowercase hex bytes of declared length

Flags:

- bit 0: open sharp-drop interval exists
- bit 1: interval closed by recovery
- bit 2: interval closed because hard silence superseded sharp drop

Rules:

- reject unknown schema versions
- reject unknown subject kinds
- reject malformed alert id lengths or trailing bytes
- reject alert id bytes outside ASCII lowercase hex
- do not store floating-point expected/observed ratios in open-drop state

---

## 8) `migrate/*` values

### 8.1 Migration journal entries
- `migrate/v1/journal/<ts>/<name>` -> string (status line) OR bytes (implementation-defined)
v0.1 rule:
- store as string using encoding in 1.3.

---

## 9) Required tests
- roundtrip read/write for each primitive encoding
- string length varint correctness for long feature strings
- is_gzip accepts only 0/1
- schema version bump compatibility test scaffold (reads unknown version -> error)
- ExpectedSourceStateV1 fixed-width encode/decode roundtrip and validation failures
- ExpectedSourceStateV1 update-rule coverage for maturity and non-regressive last-seen behavior
- tenant DB read/write/update coverage for expected-source subject state
- OpenSilenceStateV1 variable-length encode/decode roundtrip and validation failures
- Future OpenDropStateV1 encode/decode roundtrip and validation failures when implemented


## Current release OpenSilenceStateV1 usage note

`OpenSilenceStateV1` was encoded in the current release and can be written/read through tenant DB helpers in the current release. The value remains the dedup marker for an open hard-silence alert interval. Runtime hard-silence evaluation and automatic open-state writes are active through the current release.


the current release runtime rule for `OpenSilenceStateV1`:

- write an open state when a hard-silence `V_DROP` alert is emitted
- suppress matching candidates while the open bit remains set
- mark the open state closed when a later finalized window is observed for that subject

## Current release OpenDropStateV1 planning note

the current release does not activate `drop_open/*` writes or codecs. It locks the future semantic
shape for `OpenDropStateV1` so the current release can implement sharp-drop duplicate suppression
without overloading `OpenSilenceStateV1` or changing active hard-silence behavior.

## Current release OpenDropStateV1 implementation note

the current release implements `OpenDropStateV1` as a separate semantic type from
`OpenSilenceStateV1`.

Implemented value encoding:

- 30-byte fixed header before variable alert id bytes
- schema version u16 LE
- subject kind u8
- state flags u8
- drop start i64 LE
- last alert window start i64 LE
- last alert window end i64 LE
- alert id length u16 LE
- lowercase ASCII hex alert id bytes

Implemented flags:

- bit 0: open sharp-drop interval exists
- bit 1: interval closed by recovery
- bit 2: interval closed by hard-silence supersession

the current release adds tenant DB read/write persistence for `drop_open/*`. the current release added the
codec and storage-neutral helpers only.

## Current release source-stream value encodings

the current release adds source-stream primitive value encodings. Source-stream runtime `V_DROP`
evaluation remains inactive.

### Source-stream subject kind

`ExpectedSourceStateV1` accepts an additional subject kind:

- `3` -> source stream

The `ExpectedSourceStateV1` encoded length remains 68 bytes. Existing device and tenant
subject keys and encodings are unchanged.

### SourceStreamCatalogV1

Key:

- `source_stream/v1/<device_key>/<source_stream_id>/catalog`

Encoding:

- fixed header length: 28 bytes
- `schema_version_u16` -> u16 LE, value 1
- `state_flags_u8` -> u8
- `reserved_u8_0` -> u8, value 0
- `reserved_u16_0` -> u16 LE, value 0
- `first_seen_ts_i64` -> i64 LE
- `last_seen_ts_i64` -> i64 LE
- `source_stream_id_len_u16` -> u16 LE
- `device_key_len_u16` -> u16 LE
- `canonical_source_path_len_u16` -> u16 LE
- `source_stream_id` -> UTF-8 bytes, lowercase hex, 32 bytes
- `device_key` -> UTF-8 bytes
- `canonical_source_path` -> UTF-8 bytes

Decode rules:

- reject unknown schema versions
- reject nonzero reserved fields
- reject malformed string lengths or trailing bytes
- reject invalid source-stream ids
- reject invalid timestamp bounds
- reject unsafe canonical source paths

### SourceStreamStatsV1

Key:

- `source_stats/v1/<device_key>/<source_stream_id>/<bucket>`

Encoding:

- encoded length: 68 bytes
- `line_count` -> WelfordF64V1, primary expected-volume signal
- `byte_count` -> WelfordF64V1, explanation-only signal
- `score_total` -> WelfordF64V1, reserved and zeroed
- `last_update_ts` -> i64 LE

`SourceStreamStatsV1` intentionally uses a separate type from `DeviceStatsV1`; it does
not change the locked `DeviceStatsV1` 68-byte layout.
