# Open-Window Checkpoint Encoding Contract v0.1

This contract locks the binary encodings for open-window checkpoint keys under `win_active/*` and `win_row/*` (Tenant DB Key Prefix Map v0.1).
These encodings must be deterministic and restart-safe.

## Goals
- Resume ingestion after restart without re-reading full files.
- Bound state size for high-volume tenants.
- Deterministic bytes for reproducible tests and easier migrations.
- Efficient incremental updates (append/merge friendly where possible).

## Non-goals (v0.1)
- Perfect exactly-once semantics across power loss mid-write. v0.1 targets at-least-once with idempotent window finalize.
- Cross-process concurrent writers to the same tenant DB (tenant workers are the single writer).

---

## 1) Common encoding rules

### 1.1 Endianness
- Fixed-size integers and floats are little-endian.

### 1.2 Varints
- Unsigned integers in variable-length form use unsigned LEB128 varints:
  - u32 FeatureId
  - u32 counts
  - lengths

### 1.3 Strings
- Strings are UTF-8 bytes.
- String encoding: `varint(len_bytes)` + raw bytes.
- No trailing NUL.

Normalization:
- UserId and Domain values are stored after normalization rules from Feature Emission Catalog v0.1.
- Src/Dst IP strings are stored as canonical textual forms:
  - IPv4 dotted quad
  - IPv6 canonical lower-case, compressed form (implementation-defined, but must be consistent).

### 1.4 Ordering
Where a list/map is written:
- If keyed by FeatureId, pairs must be sorted by increasing FeatureId.
- If keyed by string, entries must be sorted deterministically:
  - primary: count descending
  - tie-break 1: value bytes lex ascending
  - tie-break 2: not needed if bytes are unique; otherwise stable insertion order is NOT allowed.

---

## 2) `win_active` encoding

Key:
- `win_active/v1/<device_key>`

Value: fixed struct `WinActiveV1`:
- `active_window_start_ts` i64
- `active_window_id` u64
- `last_update_ts` i64

Total size: 8 + 8 + 8 = 24 bytes.

Semantics:
- `active_window_start_ts` is the window start (unix seconds) for the currently accumulating window.
- `active_window_id` is a monotonic per-device sequence used in win_row keys.
- `last_update_ts` is updated whenever any win_row state is modified.

---

## 3) `win_row/.../feat` sparse feature map encoding

Key:
- `win_row/v1/<device_key>/<window_id>/feat`

Value: `SparseCountsV1`

Format:
- `varint(pair_count)`
- repeated `pair_count` times:
  - `varint(feature_id_u32)`
  - `varint(count_u32)`

Constraints:
- Pairs are sorted by increasing feature_id.
- Counts are strictly positive; zero counts are not stored.
- On update, merge counts by addition.

Hard caps:
- Enforced at the window level by Feature Emission Catalog v0.1:
  - `MAX_FEATURES_PER_WINDOW`, `MAX_WORD_FEATURES_PER_WINDOW`, etc.
Dropped counts due to caps must increment the window meta drop counters (section 4).

---

## 4) `win_row/.../meta` fixed metadata struct encoding

Key:
- `win_row/v1/<device_key>/<window_id>/meta`

Value: fixed struct `WinMetaV1`:

- `window_start_ts` i64
- `window_end_ts` i64
- `lines` u32
- `bytes` u64
- `dropped_features` u32
- `dropped_words` u32
- `dropped_shapes` u32

Total size: 8 + 8 + 4 + 8 + 4 + 4 + 4 = 40 bytes.

Semantics:
- `window_end_ts` is inclusive end or exclusive end depending on implementation, but must be consistent.
  - Recommendation: store exclusive end ts = start + window_size_seconds.
- `bytes` is total raw bytes ingested for lines in this window.
- Dropped counters are cumulative for this window.

---

## 5) `win_row/.../ent/*` entity sketch encodings

Keys:
- `win_row/v1/<device_key>/<window_id>/ent/srcip`
- `win_row/v1/<device_key>/<window_id>/ent/dstip`
- `win_row/v1/<device_key>/<window_id>/ent/userid`
- `win_row/v1/<device_key>/<window_id>/ent/domain`
- `win_row/v1/<device_key>/<window_id>/ent/host`

Value: `TopKStringsV1`

Format:
- `varint(entry_count)`
- repeated `entry_count` times:
  - `varint(count_u32)`
  - `str(value_utf8_bytes)` where str is `varint(len)` + bytes

Ordering:
- Written in deterministic order using rules in section 1.4 (count desc, then bytes lex asc).

Update strategy:
- Implementation may keep an in-memory map for the active window and rewrite the full TopK list on flush/checkpoint.
- At checkpoint time, enforce top-K caps:
  - `MAX_SRCIPS`, `MAX_DSTIPS`, `MAX_USERIDS`, `MAX_DOMAINS`, `MAX_HOSTS`

Notes:
- These are metadata sketches for alert explainability and indexing, not part of the sparse scoring vector by default.

---

## 6) Flush/write rules

### 6.1 Atomicity and consistency
When checkpointing an active window:
- Write `feat`, `meta`, and `ent/*` first
- Write `win_active` last

This ensures `win_active` never points to a window_id without corresponding win_row data.

### 6.2 Delete rules on finalize
When a window is finalized into an alert row and baselines are updated:
- delete the open-window keys:
  - `win_row/v1/<device_key>/<window_id>/feat`
  - `win_row/v1/<device_key>/<window_id>/meta`
  - `win_row/v1/<device_key>/<window_id>/ent/*`
- update `win_active` to the next window (or remove it if idle)

MVP: deletes may be deferred until after alert persistence to avoid losing data on crash.

---

## 7) Must-have tests
- encoding roundtrip for SparseCountsV1 preserves ordering and counts
- WinMetaV1 fixed struct size is constant and matches expected byte length
- TopKStringsV1 ordering is deterministic for ties
- crash simulation: if win_active exists, referenced win_row keys exist (or worker recovers safely)
- finalize deletes win_row keys and advances win_active deterministically
