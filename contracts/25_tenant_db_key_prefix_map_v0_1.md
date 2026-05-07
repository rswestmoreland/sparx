# Tenant DB Key Prefix Map Contract v0.1

This contract is the single authoritative list of all keys/prefixes in a tenant database.

## Goals
- Prevent implementation drift.
- Keep keys stable for migrations.
- Support efficient iteration by prefix.
- Keep all keys versioned to allow schema evolution.

## Conventions
- All keys are ASCII UTF-8 bytes.
- Prefix components are `/` separated.
- All logical groups include a `/v1/` marker in the prefix.
- Values are binary, little-endian for fixed structs, varint LEB128 where applicable.

---

## Stable hash rule
- v0.1 stable hashes use BLAKE3.
- Persist the first 16 digest bytes (128 bits) as lowercase hex.
- Hash exact UTF-8 bytes of the canonical input string.

---

## 1) Meta

### 1.1 Tenant schema
- `meta/schema/v1/version` -> u32
- `meta/schema/v1/created_ts` -> i64
- `meta/schema/v1/last_migrate_ts` -> i64

### 1.2 Ingest state
- `meta/ingest/v1/last_flush_ts` -> i64
- `meta/ingest/v1/worker_epoch` -> u64 (monotonic per restart)

---

## 2) Device registry and file cursors

### 2.1 Device registry
- `dev/v1/<device_key>/path` -> bytes (device directory relative path)
- `dev/v1/<device_key>/created_ts` -> i64
- `dev/v1/<device_key>/last_seen_ts` -> i64

### 2.2 File cursor state
Used to resume tailing per device file.

- `cursor/v1/<device_key>/<file_key>/inode` -> u64
- `cursor/v1/<device_key>/<file_key>/mtime` -> i64
- `cursor/v1/<device_key>/<file_key>/size` -> u64
- `cursor/v1/<device_key>/<file_key>/offset` -> u64
- `cursor/v1/<device_key>/<file_key>/is_gzip` -> u8 (0/1)
- `cursor/v1/<device_key>/<file_key>/last_read_ts` -> i64

Notes:
- `file_key` is a stable hash of the relative file path (not full path), using the Stable hash rule above.
- If inode changes, cursor resets to 0 and increments `cursor_resets_total`.

---

## 3) Window accumulator checkpoints (open windows)

Open-window state is required to emit alerts within latency bounds and survive restarts.

### 3.1 Active window index
- `win_active/v1/<device_key>` -> struct:
  - active_window_start_ts_i64
  - active_window_id_u64 (monotonic per device)
  - last_update_ts_i64

### 3.2 Window sparse counts (feature map)
- `win_row/v1/<device_key>/<window_id>/feat` -> encoded sparse map (FeatureId -> u32 count)

### 3.3 Window metadata
- `win_row/v1/<device_key>/<window_id>/meta` -> fixed struct:
  - window_start_ts_i64
  - window_end_ts_i64
  - lines_u32
  - bytes_u64
  - dropped_features_u32
  - dropped_words_u32
  - dropped_shapes_u32

### 3.4 Window entity sketches (top-K)
- `win_row/v1/<device_key>/<window_id>/ent/srcip` -> list (IP -> u32 count)
- `win_row/v1/<device_key>/<window_id>/ent/dstip` -> list (IP -> u32 count)
- `win_row/v1/<device_key>/<window_id>/ent/userid` -> list (string -> u32 count)
- `win_row/v1/<device_key>/<window_id>/ent/domain` -> list (string -> u32 count)
- `win_row/v1/<device_key>/<window_id>/ent/host` -> list (string -> u32 count)

Encoding:
- deterministic ordering by (count desc, then bytes asc) at write time.

---

## 4) Feature dictionary

### 4.1 Feature dictionary (string -> FeatureId)
- `feat_dict/v1/str/<feature_string>` -> u32 FeatureId

### 4.2 Feature reverse map (FeatureId -> string)
- `feat_dict/v1/id/<feature_id_u32>` -> bytes feature_string

### 4.3 Dictionary stats and caps
- `feat_dict/v1/meta/next_id` -> u32
- `feat_dict/v1/meta/entries` -> u32
- `feat_dict/v1/meta/last_gc_ts` -> i64

Notes:
- Dictionary is tenant-scoped.
- Dictionary entries remain tenant-scoped and dictionary-only in v0.1; no hashed fallback FeatureId namespace exists.

---

## 5) Baselines (DF ring + centroids + stats)

References Baseline Sketch Encoding v0.1.

### 5.1 DF ring meta
- `meta/df_ring/v1/current_day_epoch` -> i64
- `meta/df_ring/v1/day_slot_epoch/<slot>` -> i64
- `meta/df_ring/v1/last_roll_epoch` -> i64

### 5.2 DF window counts per slot/bucket
- `dfN/v1/<slot>/<bucket>` -> u32

### 5.3 DF maps per slot/bucket
- `dfM/v1/<slot>/<bucket>` -> encoded list of (FeatureId, u32 df_count)

### 5.4 Device centroids per bucket
- `centroid/v1/<device_key>/<bucket>` -> encoded list of (FeatureId, f32 value)

### 5.5 Device stats per bucket
- `stats/v1/<device_key>/<bucket>` -> fixed struct (Welford state)

---

## 6) Alerts and indexes

### 6.1 Alert record
- `alert/v1/<alert_id>` -> encoded AlertV1 object (see Alert contract)

Alert id:
- stable hash of (tenant, device_key, window_start_ts, top_feature_signature), using the Stable hash rule above

### 6.2 Time index (by device)
- `alert_idx_time/v1/<device_key>/<window_start_ts>/<alert_id>` -> empty

Notes:
- Phase 15c activates this index for list/search/export candidate selection when the tenant DB has complete time-index coverage for the current primary alert set.
- Query/export correctness still falls back to primary alert scans when the time index is absent or incomplete.

### 6.3 Category index
- `alert_idx_cat/v1/<category>/<window_start_ts>/<alert_id>` -> empty

Category values use the alert label categories: `outlier`, `noise_suspect`, `info`.

Notes:
- Phase 15d activates this index for structured category-filter candidate selection when the category index fully covers the tenant primary alert set.
- Structured category filters still fall back to primary-alert scans when the category index is absent or incomplete.

### 6.4 Entity index
- `alert_idx_ent/v1/<entity_kind>/<entity_value>/<window_start_ts>/<alert_id>` -> empty

Entity kinds use the canonical alert entity families: `srcip`, `dstip`, `userid`, `domain`, `host`.

Notes:
- Phase 15d activates this index for structured entity-filter candidate selection when the specific entity filter matches the primary alert set exactly.
- Structured entity filters still fall back to primary-alert scans when the relevant entity index coverage is absent or incomplete.

---

## 7) Metrics counters (optional persistence)

If enabled, persist selected counters for restart continuity:

- `metrics/v1/counter/<name>` -> u64
- `metrics/v1/gauge/<name>` -> f64

This is active in Phase 13a and later for the small persisted observability surface used by `status`, `/metrics`, and `/healthz` continuity across restarts.

---

## 8) Migrations

### 8.1 Migration journal
- `migrate/v1/journal/<ts>/<name>` -> bytes (result/status)

Rules:
- Any key/value encoding changes require schema version bump and migration steps.
