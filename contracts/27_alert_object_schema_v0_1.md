# Alert Object Schema Contract v0.1

This contract defines the persisted AlertV1 object stored under `alert/v1/<alert_id>` and the fields required to support analyst review and customer escalation.

## Goals
- Provide an alert object with explainability (top features + reasons).
- Preserve enough context to guide investigation (entities + small samples).
- Keep encoding deterministic and versioned.
- Avoid leaking internal implementation details that may change (store stable features/strings, not internal pointers unless needed).

## Non-goals (v0.1)
- Full per-line explainability (deferred).
- Cross-tenant correlation (tenant-scoped alerts).
- Storing entire window raw lines (too large).

---

## 1) Storage and encoding

### 1.1 Tenant DB key
- `alert/v1/<alert_id>` -> bytes (AlertV1)

### 1.2 Value encoding
- v0.1 uses `postcard` (Serde) encoding of the `AlertV1` struct.
- Determinism requirement: serializing the same AlertV1 values must produce identical bytes.

Export:
- CLI and sinks may also emit JSONL by converting AlertV1 to JSON at output time.

---

## 2) Top-level AlertV1 fields

All timestamps are unix seconds UTC.

### 2.1 Identity
- `schema_version: u16` (must be 1)
- `alert_id: String` (lowercase hex; BLAKE3 stable hash, first 16 digest bytes)
- `tenant_id: String` (from directory topology)
- `device_key: String` (stable hash of tenant_id + device dir path; BLAKE3 first-16-byte lowercase hex)
- `device_path: String` (device directory relative path, for explainability)

### 2.2 Window
- `window_start_ts: i64`
- `window_end_ts: i64` (recommended exclusive end = start + window_size_s)
- `window_size_s: u32`
- `bucket: u8` (0..47; weekday/weekend x hour; per Scoring contract)

### 2.3 Label and scores (Scoring Math + Thresholding v0.1)
- `label: String` in `{ "outlier", "noise_suspect", "info" }`
- `confidence: String` in `{ "high", "medium", "low" }`
- `cold_start: bool`
- `score_total: f32`
- `score_rarity: f32`
- `score_drift: f32`
- `score_volume: f32`

Optional debug (v0.1 default OFF):
- `baseline_n_bucket: u32`
- `baseline_centroid_norm: f32`

### 2.4 Reasons (deterministic codes)
- `reasons: Vec<ReasonV1>`

ReasonV1:
- `code: String` (example: `R_NEW_FEATURE`, `D_HIGH_DRIFT`, `V_SPIKE`, `N_HIGH_CARDINALITY`, `O_ENTITY_FOCUSED`)
- `msg: String` (short human-readable sentence)
- `details: Vec<(String,String)>` (small set of key/value details; ASCII keys)

Notes:
- Reason codes must match the deterministic reason rules in the Scoring contract.
- `msg` is allowed to evolve for UX, but `code` must remain stable.

---

## 3) Explainability payload

### 3.1 Top features
- `top_features: Vec<TopFeatureV1>` (capped; recommended 25)

TopFeatureV1:
- `feature: String` (canonical feature string, e.g. `k=src_ip`, `SourceIp=<IPV4>`, `w=failed`)
- `feature_id: u32` (optional 0 if not available at emit time)
- `count: u32` (window count)
- `family: String` (KEYPRES, CANON, SHAPE, BUCKET, SYSLOG, WORD)
- `tf_w: f32` (weighted tf used for scoring)
- `idf: f32` (idf used for scoring, if applicable; else 0)
- `contrib: f32` (per-feature contribution to rarity_mass_raw or overall score; implementation-defined but deterministic)

Capping:
- Choose top features by `contrib desc`, then `feature bytes asc`.

### 3.2 Summary text (for analyst and customer)
- `summary_analyst: String` (1-3 sentences)
- `summary_customer: String` (1-3 sentences; less jargon)

Notes:
- Summaries should be derived deterministically from reasons and top entities/features (no LLM in v0.1).
- If not implemented, summaries may be empty strings in v0.1, but the fields are reserved.

---

## 4) Entities (metadata, not sparse features)

Entities are stored as top-K sketches with counts, sourced from `win_row/.../ent/*` checkpoint state.

- `entities: EntitiesV1`

EntitiesV1:
- `src_ips: Vec<CountedStringV1>`
- `dst_ips: Vec<CountedStringV1>`
- `user_ids: Vec<CountedStringV1>` (canonical user_id per Feature Emission Catalog v0.1)
- `domains: Vec<CountedStringV1>` (domain/realm extracted separately)
- `hosts: Vec<CountedStringV1>`

CountedStringV1:
- `value: String`
- `count: u32`

Capping:
- Use the same top-K caps from Feature Emission Catalog v0.1.
- Ordering in the alert: `count desc`, then `value bytes asc`.

---

## 5) Window stats and provenance

### 5.1 Window stats
- `lines: u32`
- `bytes: u64`
- `dropped_features: u32`
- `dropped_words: u32`
- `dropped_shapes: u32`

### 5.2 Provenance pointers (restart-safe)
- `provenance: Vec<FileSpanV1>` (capped; recommended 8)

FileSpanV1:
- `file_rel: String` (relative path under device dir)
- `file_key: String` (stable hash of file_rel; BLAKE3 first-16-byte lowercase hex)
- `inode: u64` (0 if unavailable)
- `offset_start: u64`
- `offset_end: u64`
- `is_gzip: bool`

Notes:
- For gzip, offsets are in compressed byte stream.
- For zlg, offsets are archive chunk byte ranges and `is_gzip` remains false because the field is gzip-specific.
- These spans allow drilldown later without storing full raw lines.
- Future `V_DROP` absence-of-data alerts may have empty provenance because there may be no raw source span for missing data; drill/extract must fail closed with a clear explanation for that case.

---

## 6) Signature and id generation

Alert id inputs:
- `(tenant_id, device_key, window_start_ts, signature)`

Signature:
- `signature: String` (lowercase hex; BLAKE3 first 16 digest bytes)
- signature is a stable hash of the ordered list of top features:
  - For each TopFeatureV1, include `(feature string, count)` in order.

This ensures:
- identical windows with identical top features produce the same alert_id.

---

## 7) Required tests
- postcard roundtrip for AlertV1 preserves fields
- alert_id stable for identical input tuples
- top_features ordering deterministic on ties
- entities ordering deterministic on ties
- reasons include expected codes for synthetic scoring cases
- provenance spans capped deterministically


## Current release V_DROP construction note

the current release adds deterministic construction of `AlertV1` for hard-silence `V_DROP` candidates. The constructed alert keeps the v0.1 schema unchanged and uses absence-of-data fields:

- `reasons[0].code = "V_DROP"`
- `label = info`
- `confidence = medium`
- `lines = 0`
- `bytes = 0`
- `top_features = []`
- `provenance = []`

the current release calls this construction path from `run` and `oneshot` for mature hard-silence device and tenant aggregate subjects. The primary alert object path remains authoritative, and `silence_open/*` state suppresses duplicate alerts for an ongoing silence interval.


## Current release V_DROP runtime integration note

No schema fields changed in the current release. Runtime hard-silence `V_DROP` alerts use the existing `AlertV1` schema and may have empty provenance because the finding represents absence of expected data rather than a raw source span.


## Current release V_DROP validation closeout note

the current release adds validation hardening around the active hard-silence `V_DROP` alert path.
No AlertV1 schema fields changed. `V_DROP` alerts continue to use the existing schema,
reason code `V_DROP`, and empty provenance for absence-of-data findings.


## Sharp-drop schema note

Sharp-drop alerts reuse the existing AlertV1 schema. They use reason code `V_DROP` and
deterministic detail `drop_kind=sharp_drop`. The required ratio details are
`observed_expected_ratio` and `drop_ratio = 1.0 - observed_expected_ratio`.

Field usage:

- `label = info`
- `confidence = medium`
- `score_volume = clamp01(drop_ratio)`
- `score_total = score_volume`
- `score_rarity = 0.0`
- `score_drift = 0.0`
- `top_features = []`

Device and source-stream sharp-drop alerts should carry deterministic capped current
provenance when available. Tenant aggregate sharp-drop alerts may use empty provenance.
Alert id inputs include `sharp_drop` so hard-silence and sharp-drop V_DROP alerts do not
collide for the same subject/window.

## Source-stream V_DROP schema note

Source-stream V_DROP alerts reuse the existing AlertV1 schema. Reason details include
`subject_kind=source_stream`, `source_stream_id`, `device_key`, and safe source-path detail
when available. Source-stream hard-silence alerts may have empty provenance. Source-stream
sharp-drop alerts should include deterministic capped current-source spans when available.
No `source_files` field or alternate drilldown model may be introduced.
