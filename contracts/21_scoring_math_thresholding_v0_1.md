# Scoring Math + Thresholding Contract v0.1

This contract defines the scoring formulas and deterministic labeling rules used to emit alerts.

## Inputs per finalized window row
For a finalized window row for `(tenant, device, time_bucket)`:

- Sparse feature counts: `f -> c_f`
- Weighted TF values (from Feature Weighting v0.1):
  - `tf_w(f) = w_family(f) * log1p(c_f)`
- Tenant DF sketch for the bucket:
  - `N_bucket`: number of windows observed in the 7-day ring for this bucket
  - `df_bucket(f)`: number of windows in the ring bucket where `f` appeared (presence)
- Device centroid for the bucket:
  - `centroid(f)`: EMA of weighted TF values
- Device volume stats for the bucket:
  - line_count stats and byte_count stats
- Optional device score stats for the bucket:
  - rolling stats on score_total and/or components

All state is tenant-scoped and persisted.

---

## 1) Rarity component

### 1.1 IDF
For each feature f:

- `idf(f) = ln((N_bucket + 1) / (df_bucket(f) + 1)) + 1`

### 1.2 Rarity mass
- `rarity_mass_raw = Σ_f ( tf_w(f) * idf(f) )`

Recommended normalization:
- `row_mass = Σ_f tf_w(f)`
- `rarity_mass = rarity_mass_raw / max(row_mass, eps)`

Bounded transform (preferred):
- `rarity = 1 - exp(-rarity_mass)`

---

## 2) Drift component (cosine distance)

Let `x(f) = tf_w(f)` for the row and `m(f) = centroid(f)` for the device/bucket.

- `cos = dot(x,m) / (||x|| * ||m||)` with eps guards
- `drift = 1 - clamp(cos, -1, 1)`

Cold start:
- `cold_start_days` is an active scoring config field in Phase 15a
- `N_MIN = cold_start_days * (3600 / window_size_s)` for the current bucket family
- if `cold_start_days = 0`, only an empty centroid forces `cold_start=true`
- otherwise centroid empty or `N_bucket < N_MIN` => `cold_start=true`

---

## 3) Volume component

Compute two z-scores for the bucket:

- `z_lines = (lines - mean_lines) / std_lines`
- `z_bytes = (bytes - mean_bytes) / std_bytes`

Bounded volume score:
- `volume = clamp01( max(z_lines, z_bytes) / Z_MAX )`

Defaults:
- `Z_MAX = 6.0`

If stddev is near zero, treat that term as 0 (or apply an epsilon floor).

---

## 4) Total score

Default blend:
- `score_total = 0.45 * rarity + 0.40 * drift01 + 0.15 * volume`

Where:
- `drift01 = clamp01(drift)` for blending

---

## 5) Labeling

### 5.1 Baseline maturity
- `cold_start_days` default: `2`
- `N_MIN` is derived from `cold_start_days` and `window_size_s`, not a fixed constant

If `cold_start=true`:
- only emit `info` (unless volume extreme; see below)
- still update baselines

### 5.1a Minimum alertable lines
- `min_lines_per_window` is an active scoring config field in Phase 15a
- default: `10`
- if `min_lines_per_window = 0`, the line floor is disabled
- if `lines < min_lines_per_window`, no alert is emitted for that window even if the score would otherwise qualify
- DF, centroid, volume stats, and score_total preview/update timing remain unchanged

### 5.2 Entity focus and blob ratio
- `blob_ratio` from Feature Weighting v0.1
- `entity_focus=true` if identities (SourceIp/DestIp/User/Host) captured with confidence >= 2 and appear in top_features

### 5.3 Thresholds (starting points)
- `T_outlier = 0.80`
- `T_noise  = 0.70`
- `T_info   = 0.60`
- `D_min    = 0.25`
- `B_high   = 0.60`

### 5.4 Label rules
- If `score_total >= T_outlier` and (`entity_focus` or `drift >= D_min`) and not cold start:
  - label = `outlier`
- Else if `score_total >= T_noise` and `blob_ratio >= B_high` and not entity_focus and not cold start:
  - label = `noise_suspect`
- Else if `score_total >= T_info`:
  - label = `info`
- Else:
  - no alert emitted

Volume extreme override:
- If `volume >= 0.90`, you may emit `info` even in cold start.

### 5.5 Confidence
- High: not cold start, outlier, >=2 reasons, entity_focus
- Medium: not cold start and score_total >= T_info
- Low: cold start or only one weak reason

---

## 6) Reasons (deterministic)
Rarity:
- `R_NEW_FEATURE` if any top feature has `df_bucket(f) == 0`
- `R_RARE_FEATURE` if `df_bucket(f)/N_bucket < 0.001`

Drift:
- `D_HIGH_DRIFT` if drift > 0.35
- `D_MED_DRIFT` if drift > 0.25

Volume:
- `V_SPIKE` if volume > 0.70
- `V_EXTREME` if volume > 0.90

Noise:
- `N_HIGH_CARDINALITY` if blob_ratio > 0.60 and not entity_focus

Outlier:
- `O_ENTITY_FOCUSED` if entity_focus and label is outlier

---

## 7) Baseline update timing
- Score using baseline as-of before this row.
- After scoring, update DF, centroid EMA, and volume stats.

DF update is window-presence based:
- for each feature present in the row, increment DF by 1 for the bucket/day-slot.

---

## 8) Must-have tests
- rarity normalization invariance to row scale
- drift near 0 when row matches centroid
- volume spike produces high volume
- blob_ratio drives noise_suspect without entity focus
- cold start suppresses outlier/noise_suspect using the active day-based maturity floor
- min_lines_per_window suppresses alert emission while preserving baseline updates
