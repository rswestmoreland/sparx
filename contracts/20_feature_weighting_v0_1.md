# Feature Weighting Contract v0.1

## Goal
Default weights that reduce false positives from high-cardinality noise while preserving strong signals from semantic keys + shapes + bucketed identities.

Weights affect:
- rarity scoring (TF/IDF inputs)
- drift scoring (vector representation for cosine)
- top_features contribution ranking

Volume scoring is separate (line/byte stats) and not driven by feature weights.

## Feature families
Let each emitted feature be assigned a family and a multiplier `w_family`.

### Primary (highest value)
- `F_SHAPE`: `<Category>=<SHAPE>` (e.g., `SourceIp=<IPV4>`)  
  Default `w=1.0`
- `F_BUCKET`: bucketed identity (e.g., `SourceIp_net@10.2.3.0/24`)  
  Default `w=0.8`

### Structural (medium value)
- `F_KEYPRES`: `k=<norm_key>`  
  Default `w=0.5`
- `F_CANON`: `canon=<Category>`  
  Default `w=0.5`
- `F_SYSLOG`: `syslog_pri=...`, `syslog_app=...`  
  Default `w=0.2` (capped; host OFF by default)

### Textual (lower value)
- `F_WORD`: word tokens from plaintext/payload fields  
  Default `w=0.3`

### High-cardinality blob shapes (penalized)
- `F_BLOB`: `<UUID>`, `<HEX_N>`, `<B64_N>` and other request-id-like blobs  
  Default `w=0.1`

Rationale: blob shapes often dominate novelty but are usually noise without key context.

## Count transform (TF)
For a feature count `c` in a window row:
- `tf = log1p(c)` (deterministic, reduces burst dominance)
Alternative allowed: `sqrt(c)`.

Weighted TF:
- `tf_w = w_family * tf`

## Rarity scoring (baseline prevalence)
Use DF from the 7-day ring in the relevant time bucket.
A simple deterministic IDF:
- `idf = log((N + 1) / (df + 1)) + 1`

Rarity mass component (example):
- `rarity_mass = sum(tf_w * idf)` with optional normalization by row length.

## Drift scoring vector
For cosine distance:
- use the vector with `tf_w` values
- apply L2 normalization before cosine
- centroid vectors store weighted values (EMA)

## Noise heuristics
Compute a noise indicator:
- `blob_mass = sum(tf_w over F_BLOB)`
- `total_mass = sum(tf_w over all families)`
- `blob_ratio = blob_mass / max(total_mass, eps)`

If `blob_ratio` exceeds threshold (e.g., 0.6) and identities are not entity-focused, emit reason `N_HIGH_CARDINALITY`.

## Exact identity features
Exact identities like `SourceIp@1.2.3.4` are:
- stored as metadata for explanation/correlation
- excluded from scoring vector by default (or treated as weight 0)

## Tenant overrides
Tenant policy may override:
- family weights
- enable syslog_host feature
- blob penalty threshold
All overrides are explicit and deterministic.
