# Baseline Sketch Encoding Contract v0.1

This contract defines how DF (7-day ring) and device baselines are stored in each tenant embedded DB.

## Goals
- Compact, deterministic encoding.
- Efficient incremental updates.
- Fast reads for scoring (avoid scanning many keys).
- Clear caps to prevent unbounded growth.

---

## 1) Terminology
- `bucket`: time bucket index (0..47) for {weekday/weekend} x hour.
- `day_slot`: ring slot (0..6) representing a specific UTC day.
- `FeatureId`: u32 (dictionary or hashed ID), stable within the tenant.
- `DeviceKey`: stable hash of tenant_id + device_dir path using BLAKE3, persisted as first-16-byte lowercase hex.

---

## 2) Tenant DB keyspace layout

All keys are UTF-8 bytes with a `v1` marker in the key prefix.

### 2.1 DF ring meta
- `meta/df_ring/v1/current_day_epoch` -> i64 (UTC day number)
- `meta/df_ring/v1/day_slot_epoch/<slot>` -> i64
- `meta/df_ring/v1/last_roll_epoch` -> i64

### 2.2 DF ring window counts per slot/bucket
- `dfN/v1/<slot>/<bucket>` -> u32 (windows finalized in that slot/bucket)

Scoring uses:
- `N_bucket = sum_slot dfN[slot,bucket]`

### 2.3 DF maps per slot/bucket
- `dfM/v1/<slot>/<bucket>` -> encoded list of `(FeatureId, u32 df_count)`

Scoring uses:
- `df_bucket(f) = sum_slot dfM[slot,bucket].get(f)` (tenant worker may cache merged views)

### 2.4 Device centroid per bucket
- `centroid/v1/<device_key>/<bucket>` -> encoded list of `(FeatureId, f32 value)`

### 2.5 Device stats per bucket
- `stats/v1/<device_key>/<bucket>` -> fixed-size struct (volume and optional score stats)

---

## 3) Ring rollover rules

### 3.1 Day calculation
Compute:
- `day_epoch = floor(utc_timestamp_seconds / 86400)`

When finalizing a window, if `day_epoch != current_day_epoch`:
- roll ring and clear stale slots.

### 3.2 Slot selection
- `slot = day_epoch % 7`

If `day_slot_epoch[slot] != day_epoch`, clear the slot:
- delete keys with prefixes:
  - `dfN/v1/<slot>/`
  - `dfM/v1/<slot>/`
- set `day_slot_epoch[slot] = day_epoch`

Then set:
- `current_day_epoch = day_epoch`

Clearing strategy:
- v0.1: iterate and delete by prefix.
- future: delete-range if available.

---

## 4) DF update rules (presence per window)

For each finalized window row:
1) Determine `(bucket, slot)`.
2) Increment `dfN[slot,bucket]` by 1.
3) For each feature `f` present in the row (presence, not count):
   - increment `dfM[slot,bucket][f]` by 1.

Exact identity features like `SourceIp@...` are excluded from DF by default.

Caps:
- `DF_CAP` entries per `dfM[slot,bucket]` (start: 200k).
- On overflow:
  - keep top DF_CAP by df_count (deterministic)
  - drop the rest and increment `df_cap_hits_total`.

---

## 5) Device baseline encoding

### 5.1 Centroid update
EMA:
- `centroid_new = (1-a)*centroid_old + a*x`
Where `x` is the weighted row vector.

Cap:
- `CENTROID_CAP` entries per `(device,bucket)` (start: 50k).
- After update, keep top entries by absolute value (deterministic).

### 5.2 Stats struct
Store (little-endian fixed 68-byte struct):
- Welford state for line_count:
  - `n` (u32), `mean` (f64), `m2` (f64)
- Welford state for byte_count:
  - `n` (u32), `mean` (f64), `m2` (f64)
- Welford state for score_total:
  - `n` (u32), `mean` (f64), `m2` (f64)
- `last_update_ts` (i64)

Rules:
- `score_total` fields are always present in the encoded struct.
- `score_total.n == 0` means score stats are not populated yet for that device/bucket.
- No separate presence flag exists.
- Total size = `20 + 20 + 20 + 8 = 68` bytes.

---

## 6) Encoding formats

### 6.1 Varints
- `FeatureId` and u32 counts use unsigned LEB128 varints.

### 6.2 DF map value (`dfM`)
- `varint(count_pairs)`
- repeated:
  - `varint(feature_id)`
  - `varint(df_count)`

Pairs must be in increasing feature_id order.

### 6.3 Centroid value
- `varint(count_pairs)`
- repeated:
  - `varint(feature_id)`
  - `f32` little-endian

Pairs in increasing feature_id order.

Compression:
- rely on the embedded DB compression settings; keep encoding stable regardless.

---

## 7) Read path caching (tenant worker)
Maintain in-memory caches per active tenant slice:
- merged DF map for active bucket
- centroid map for active device/bucket

Invalidate:
- slot rollover invalidates merged DF for affected buckets.

---

## 8) Migration
Any changes to these keys or encodings require:
- `tenant_schema_version` bump
- explicit migration steps.

---

## 9) Must-have tests
- ring rollover clears stale slot deterministically
- N_bucket equals sum of dfN across slots
- presence increments once per window
- dfM and centroid encodings are deterministic (ordering)
- caps applied deterministically
