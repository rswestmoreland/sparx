# Sparse Matrix Model

sparx models each finalized time window as a sparse row:

- row: tenant, device, and window slice
- column: canonical `FeatureId`
- value: observed feature count in the window

Only observed features are stored. Missing features are interpreted as zero.

## Baseline families

sparx maintains compact baseline structures for sparse-row scoring and volume
analysis:

- DF-ring style rarity estimates
- centroid comparison for drift scoring
- fixed-layout line and byte statistics
- source-stream line and byte statistics for per-source volume-loss detection

## Feature IDs

Feature IDs are deterministic. The removed hashed-fallback FeatureId behavior is
not active and must not be revived without explicit approval.

## Why sparse storage matters

Log environments often have millions of possible tokens, entities, and shape
features, but a single device window sees only a small subset. Sparse rows avoid
zero-filled storage and allow scoring to focus on observed signal.

## Explainability

Alert explanations are derived from the same sparse data and volume statistics
used by scoring. Alerts retain top contributing features where applicable and
provenance spans for drill/extract workflows.
