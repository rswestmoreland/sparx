# Alert Object + Explanation Contract v0.1 (No Redaction)

## Key stance
- No redaction is applied. Analysts and customers have full access to identities.

## Alert object schema (fields)

### Identity + timing
- `alert_id` (BLAKE3 stable hash of tenant/device/window_start + top features; first 16 digest bytes as lowercase hex)
- `tenant_id`
- `device_path`
- `window_start_ts`, `window_end_ts`
- `ingest_watermark_ts`

### Scores
- `score_total`
- `score_components`:
  - `rarity_mass`
  - `drift`
  - `volume`
- `baseline_refs`:
  - `tenant_baseline_window_days` (e.g., 7)
  - `device_baseline_window_days` (e.g., 7)
  - `time_bucket` (weekday/weekend + hour bucket)

### Classification
- `label`: `outlier` | `noise_suspect` | `info`
- `confidence`: `low|med|high`
- `reasons`: list of `{ code, text }`

### Explainability
- `top_features`: list of
  - `feature` (string)
  - `observed`
  - `baseline_prevalence`
  - `contribution`

### Entities (exact, capped)
- `entities`:
  - `source_ips`, `dest_ips`, `users`, `hosts`
  - optional `processes`, `paths`, `domains`, `urls` (tighter caps)

### Context
- `line_count`, `byte_count`
- `provenance`: `Vec<FileSpanV1>`
  - `file_rel`
  - `file_key`
  - `inode`
  - `offset_start`
  - `offset_end`
  - `is_gzip`
- `samples`: pointers `{ path, offset, len?, ts_guess? }` (N capped)

Notes:
- `provenance` is the authoritative restart-safe drilldown model in v0.1.
- Older `source_files` wording is obsolete and must not be used for new implementation work.

## Contribution ranking
Rank features by deterministic contribution (e.g., family-weighted TF-IDF for rarity + drift weights), take top 10-30.

## Reason codes (examples)
- Rarity: `R_NEW_FEATURE`, `R_RARE_FEATURE`
- Drift: `D_HIGH_DRIFT`, `D_SUSTAINED_DRIFT`
- Volume: `V_SPIKE`, `V_DROP`
- Noise: `N_HIGH_CARDINALITY`, `N_ENV_WIDE`
- Outlier: `O_ENTITY_FOCUSED`, `O_LOCALIZED`

## Thresholding
Use per-device rolling stats per time bucket; page on SLA lag and error counters (see Metrics contract).
