# State + Retention Contract v0.1

## Storage split
- Raw logs remain on disk (1-year retention outside sparx).
- State stored in the embedded DB layer (global + per-tenant DBs).
- Optional Parquet/columnar archive deferred.

## Keys/partitioning
State keyed by:
- `tenant_id`
- `device_dir` (baseline unit)
- `window_start_minute` (UTC)

## Ingestion state
### Plain `.log`
- Tail incrementally from stored offset.
- Detect rotation by inode change / truncation (size < offset => reset to 0).
- Persist offsets frequently.

### `.gz`
- Immutable batch: stream once and mark processed by `(path, inode, size)`.

### Semantics
- At-least-once ingestion; duplicates tolerated at window level (stronger semantics optional later).

## Windowing
- 1-minute windows (configurable).
- Grace default 2 minutes.
- Emit detections within 10 minutes SLA.
- Keep open windows in memory; checkpoint open windows periodically.

## Baselines
- Retain baseline effect for ~7 days:
  - 7-day ring sketches, 48 buckets (weekday/weekend x hour).
- Tenant DF prevalence per bucket (top M dictionary FeatureIds only; no hashed fallback namespace in v0.1).
- Device centroid EMA per bucket + score distribution stats.

## Retention of window vectors
- Do not store all window vectors long-term (scale).
- Optionally store last 24h for debugging.

## Alert retention
- Store alerts for long term (aligned with policy).
- Index by time; optional entity indexes with TTL windows.

## Caps
- Features per window cap, identities top-K caps, centroid/sketch caps.
