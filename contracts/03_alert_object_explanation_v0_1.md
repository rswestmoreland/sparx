# Alert Object + Explanation Contract v0.1 (No Redaction)

## Key stance
- No redaction is applied. Analysts and customers have full access to identities.

## Alert object schema (fields)

### Identity + timing
- `alert_id` (BLAKE3 stable hash, first 16 digest bytes as lowercase hex; sparse-window alerts hash tenant/device/window_start + signature, while the current release `V_DROP` construction hashes tenant/subject/silence interval/reason/missed-window count)
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
- Volume/health: `V_SPIKE`, `V_DROP`
  - `V_SPIKE` is active through current volume scoring.
  - `V_DROP` is active for the first hard-silence runtime path as of the current release. It uses the expected-source missing-window model for device and tenant aggregate subjects.
  - The current release scopes sharp-drop detection under `V_DROP` with deterministic detail `drop_kind=sharp_drop`; the current release activates runtime sharp-drop emission through the existing V_DROP policy controls.
- Noise: `N_HIGH_CARDINALITY`, `N_ENV_WIDE`
- Outlier: `O_ENTITY_FOCUSED`, `O_LOCALIZED`

## Thresholding
Use per-device rolling stats per time bucket; page on SLA lag and error counters (see Metrics contract).


## Current release V_DROP explanation note

`V_DROP` alerts constructed by `build_vdrop_alert_v1(...)` preserve the existing `AlertV1` schema and use absence-of-data explainability:

- empty provenance
- empty top features
- zero lines and bytes
- one `V_DROP` reason with deterministic expected-vs-observed details

The current release calls this construction path from `run` and `oneshot` for mature hard-silence device and tenant aggregate candidates.


## Current release V_DROP runtime note

The current release activates runtime emission for hard-silence `V_DROP` alerts. The alert schema is unchanged. Alerts are persisted through the existing primary alert object path and surfaced through the existing alert workflows. `silence_open/*` state suppresses duplicates during an ongoing silence interval, and later observations close matching open-silence state.

Deferred after v1: parser-class subjects, vendor-event-family subjects, external heartbeat checks, maintenance-window calendars, cross-tenant outage correlation, source-stream-specific threshold knobs, and AlertV1 schema changes. Hard-silence, sharp-drop, and source-stream V_DROP use the existing AlertV1 schema and bounded diagnostics surface.


## Current release sharp-drop explanation planning note

Sharp-drop alerts keep the existing AlertV1 schema and use reason code
`V_DROP` with `drop_kind=sharp_drop`. The current release defines `observed_expected_ratio` and
`drop_ratio` as required explanation details, where `drop_ratio = 1.0 -
observed_expected_ratio`. The current release locks deterministic AlertV1 explanation behavior:
`score_volume = clamp01(drop_ratio)`, `score_total = score_volume`, and sharp-drop reason
details are emitted in stable vector order with ASCII string values. Device sharp-drop
alerts should include current finalized-row provenance when available. Tenant aggregate
sharp-drop alerts use empty provenance for the first implementation unless a later
implementation locks a deterministic capped aggregate-provenance rule. Hard-silence
empty-provenance behavior remains unchanged.

## Current release sharp-drop AlertV1 construction note

The current release implements deterministic sharp-drop AlertV1 construction without changing the
AlertV1 schema. Sharp-drop alerts use reason code `V_DROP` and first reason detail
`drop_kind=sharp_drop`. Device sharp-drop alerts preserve capped current-row provenance
when provided; tenant aggregate sharp-drop alerts use empty provenance for the first
implementation. Runtime sharp-drop emission is active as of the current release through the existing V_DROP
policy controls.
