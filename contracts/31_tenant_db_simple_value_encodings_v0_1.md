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

## 7) `migrate/*` values

### 7.1 Migration journal entries
- `migrate/v1/journal/<ts>/<name>` -> string (status line) OR bytes (implementation-defined)
v0.1 rule:
- store as string using encoding in 1.3.

---

## 8) Required tests
- roundtrip read/write for each primitive encoding
- string length varint correctness for long feature strings
- is_gzip accepts only 0/1
- schema version bump compatibility test scaffold (reads unknown version -> error)
