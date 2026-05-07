# Feature Emission Catalog Contract v0.1

This contract locks the exact feature strings emitted into the sparse row (scoring vector) and what is stored as metadata only.

## Goals
- Stable, explainable feature strings across formats.
- Use semantic keys + shapes as primary signal.
- Allow limited standalone shape emission from plaintext only when structured parsing fails.
- Normalize user identifiers so `user@domain` and `DOMAIN\\user` align with bare `user` as the same canonical user_id when appropriate.

---

## 1) Canonical feature string formats

### 1.1 Key presence
- `k=<norm_key>`
  - Example: `k=src_ip`

### 1.2 Canonical category presence
- `canon=<Category>`
  - Example: `canon=SourceIp`

### 1.3 Category + shape (primary scoring features)
- `<Category>=<SHAPE>`
  - Examples:
    - `SourceIp=<IPV4>`
    - `User=<PRINCIPAL>`
    - `Path=<UNIX_PATH>`

`User=<PRINCIPAL>` embeds the canonical normalized user principal (see section 5) so user identity participates in scoring (Option B).

### 1.4 Bucketed identities (primary scoring features)
Only these bucket families are enabled in v0.1:

IPs:
- `<Category>_net@<cidr_bucket>`
  - Example: `SourceIp_net@10.2.3.0/24`

Other categories:
- Not bucketed by default in v0.1.

### 1.5 Syslog envelope structured features (capped)
- `syslog_pri=<INT>`
- `syslog_app=<WORD>`
- `syslog_ver=<INT>` (RFC5424)
- `syslog_host=<HOST>` (default OFF)

### 1.6 Plaintext word tokens
- `w=<word>`
  - Example: `w=failed`

### 1.7 Standalone shape features from plaintext (limited)
Only emitted when structured parsing produces zero structured pairs (no KV/JSON/CSV/CEF pairs; see section 4.3).

- `shape=<IPV4>`
- `shape=<IPV6>`
Optional (OFF by default, may enable later):
- `shape=<URL>`
- `shape=<MAC>`

Standalone blob shapes are NOT emitted from plaintext in v0.1:
- No `shape=<UUID>`, `shape=<HEX_N>`, `shape=<B64_N>`, `shape=<INT>`, etc.

---

## 2) Metadata only (not in scoring vector by default)

Stored on alert objects for correlation and explainability, but excluded from scoring unless explicitly enabled later:

- Exact identities:
  - `SourceIp@1.2.3.4`
  - `DestIp@5.6.7.8`
  - `UserRaw@alice` / `UserRaw@DOMAIN\\alice` / `UserRaw@alice@example.com`
  - `Host@dc01` (if captured)
- Canonical identities:
  - `UserId@alice` (see section 5)
- Realms/domains extracted from user identities:
  - `Domain@corp.example.com` or `Domain@CONTOSO` (see section 5)
- File pointers / offsets / samples (drilldown support)

Reason:
- Exact values are high-cardinality and can dominate rarity if included directly.

---

## 3) Feature family assignment (for weighting)
Each emitted feature belongs to one family:

- `k=` -> KEYPRES
- `canon=` -> CANON
- `<Category>=<SHAPE>` -> SHAPE
- bucketed identity -> BUCKET
- `syslog_*` -> SYSLOG
- `w=` -> WORD
- `shape=` -> SHAPE (plaintext-only limited set)

---

## 4) Emission rules by token event type

### 4.1 KV / JSONKV / CSVKV / CEF extension pairs
Given `(key_norm, value_raw)`:

Always emit:
- `k=<key_norm>`

If semantic classification returns `(Category, conf)`:
- emit `canon=<Category>`
- if `value_raw` matches a shape: emit `<Category>=<SHAPE>`

If value is an identity and conf >= 2 and category is allowlisted:
- store identity metadata (no redaction)
- emit bucketed identity feature only where defined:
  - for SourceIp/DestIp: emit CIDR bucket feature (see section 1.4)

If no category match:
- do not emit standalone `shape=` features from structured values in v0.1 (to reduce noise).

### 4.2 Word tokens (`Word`)
Emit:
- `w=<word>` (subject to dictionary promotion vs hashing)

Shape matching on word tokens:
- shape detection is performed for identity extraction metadata and for limited standalone `shape=` features (see 4.3),
  but does not emit general `shape=` for blobs.

### 4.3 Standalone `shape=` emission condition (plaintext fallback)
Only emit `shape=<IPV4>` and `shape=<IPV6>` when:
- structured parse found zero KV/JSON/CSV/CEF pairs for the line.

---

## 5) User identity normalization and domain extraction (metadata)

### 5.1 Canonical user_id extraction
When a user identity token is recognized:

Inputs:
- UPN/email form: `user@domain`
- Windows form: `DOMAIN\\user`
- Bare form: `user`

Extraction:
- UPN/email: `user_id = substring before @`, `domain = substring after @`
- Windows: `user_id = substring after \\`, `domain = substring before \\`
- Bare: `user_id = whole token`, no domain

Storage:
- Store `UserRaw@...` (original token string)
- Store `UserId@<user_id>` (canonical principal)
- Store `Domain@<domain>` if present
- Store `UserKind` as metadata enum: `bare|upn|win` (not a sparse feature)

### 5.2 Scoring features for users

In v0.1, user identity participates in scoring (Option B).

Emit:
- `canon=User` (when classified)
- `User=<PRINCIPAL>` where `<PRINCIPAL>` is the canonical normalized user_id:
  - domain removed if present (`user@domain` -> `user`)
  - Windows domain removed (`DOMAIN\user` -> `user`)
  - trim surrounding whitespace
  - lowercase
  - preserve only safe characters: `[a-z0-9._-]` (others replaced with `_`)

Store as metadata (no redaction):
- `UserRaw@...` (original token)
- `UserId@<user_id>` (canonical principal, same as `<PRINCIPAL>`)
- `Domain@<domain>` if present (separate entity for correlation)
- `UserKind` enum: `bare|upn|win`

Rationale:
- Per-tenant baselines make principal-level features meaningful and explainable to analysts and customers.
- Domain remains separate for correlation and avoids splitting identities across sources.

---

## 6) Caps (per line and per window)

Per line:
- `MAX_KV_PER_LINE = 64`
- `MAX_TOKENS_PER_LINE = 256`

Per window (starting points):
- `MAX_FEATURES_PER_WINDOW = 50000`
- `MAX_WORD_FEATURES_PER_WINDOW = 20000`
- `MAX_SHAPE_FEATURES_PER_WINDOW = 20000`
- `MAX_SYSLOG_FEATURES_PER_WINDOW = 2000` (syslog host OFF by default)

Identity metadata caps (top-K):
- `MAX_SRCIPS = 64`, `MAX_DSTIPS = 64`, `MAX_USERIDS = 128`, `MAX_DOMAINS = 128`

Deterministic window drop priority:
1) `<Category>=<SHAPE>` and bucketed identity
2) `k=` and `canon=`
3) `w=`
4) `shape=` plaintext-only
5) `syslog_*`

Counters:
- `window_feature_cap_hits_total`
- `window_word_drop_total`
- `window_shape_drop_total`

---

## 7) Required tests
- emits `k=` for all structured keys
- emits categorized shape features for IP/user/path in structured context
- exact identities stored as metadata only (not sparse features)
- canonical UserId extracted from UPN and DOMAIN\\user forms
- Domain extracted and stored separately when present
- emits only `shape=<IPV4/IPV6>` from plaintext when no structured pairs exist
- deterministic drop ordering at caps
