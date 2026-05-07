# sparx Contracts v0.1 (Working Set)

This folder contains the locked contracts for **sparx** (Sparse Matrix Log Analyzer) as discussed in-chat.

- App/repo name: **sparx**
- Descriptive name: **Sparse Matrix Log Analyzer**
- Tenant terminology: use **tenant** (not customer)
- No redaction: analysts + customers have full access to identities

## Files
See `INDEX.md` for the full list and ordering.

## Notes
These are **v0.1** contracts intended to be deterministic and testable. Later revisions should bump versions and include migration notes where persistence formats change.

## Sparse matrix model

In `sparx`, a "sparse matrix" is the conceptual term-window representation of log activity:

- Rows: `(tenant_id, device_key, window_start_ts)`
- Columns: `FeatureId` (token/category/shape/identity features from the Feature Emission Catalog)
- Values: integer counts within the window (optionally weighted for scoring)

The matrix is sparse because each window contains only a small subset of all possible features, so each row is stored as a compact map of `FeatureId -> count` (not as a dense vector).

## Glossary

- **tenant_id**: The directory name under `tenant_root` that groups all devices and baselines for a tenant.
- **device_dir**: The directory name under a tenant that groups logs for a single device (used to derive `device_key`).
- **device_key**: The canonical stable identifier for a device within a tenant (derived from tenant_id + device_dir; used for DB keys and output paths). AlertV1 uses `device_path` as its field name for this relative path.
- **window**: A fixed-size time slice (default 60s) used to aggregate features into one sparse row. Windows align to UTC epoch boundaries.
- **row**: One sparse-matrix row for a `(tenant_id, device_key, window_start_ts)` triple.
- **Feature**: A canonical emitted string describing some log evidence (token/shape/category/identity). Features are mapped to `FeatureId`.
- **FeatureId**: A stable u32 integer assigned per-tenant to a feature string by the feature dictionary (`feat_dict/v1/*`).
- **Sparse matrix**: The conceptual matrix with rows = windows, columns = FeatureId, and values = counts (optionally weighted). Stored as sparse rows (`FeatureId -> count`).
- **DF (document frequency)**: For a feature, the number of window-rows in a baseline period that contained the feature at least once.
- **Baseline bucket**: One of N time-of-week buckets (N=48 in v0.1) used to maintain separate baselines for different time patterns.
- **Centroid**: A running average (EMA-like) sparse vector of feature weights for a bucket, used for distance-based scoring.
- **Open window**: The current in-progress window being filled before it is finalized and scored/emitted.
- **Checkpoint**: Persisted state used to recover open windows, cursors, and baseline sketches after restart.
- **Entity sketch**: Compact metadata extracted from a line (UserId, SrcIp, DstIp, Domain, Host, etc.) used for explainability and correlation.
- **Sink**: The output destination for alerts (JSONL files or stdout in v0.1).
- **Spool**: On-disk fallback storage for alerts that could not be written to the sink; replayed later for at-least-once delivery.
