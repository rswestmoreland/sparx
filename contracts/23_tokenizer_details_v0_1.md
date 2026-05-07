# Tokenizer Details Contract v0.1

This contract locks the line-to-tokens behavior (fast, deterministic, format-flexible) so we can implement ingestion + sparse rows without drifting later.

## Goals
- Work across mixed formats without vendor/version parsers.
- Prefer extracting structured KV/JSON/CEF/CSV fields when present.
- Preserve the vendor payload (MSG/MESSAGE) as the primary signal.
- Bound CPU/memory with hard caps and deterministic drop rules.

---

## 1) Inputs and stages

For each raw line:

1) Syslog envelope peel (already locked)  
Output: `envelope` + `msg` (vendor payload string slice)

2) Tokenizer runs primarily on `msg` and returns a bounded list of token events:
- `Kv { key_norm, value_raw }`
- `Word { token_raw }`
- `JsonKv { key_path_norm, value_raw }` (flattened keys)
- `CsvKv { key_norm, value_raw }`
- `CefHeader { field, value }` (optional, low priority)
- `ResidualText { text_raw }` (optional helper for follow-on word tokenization)

3) Semantic key + shape pipeline consumes these token events (already locked).

Tokenizer never hard-fails; worst case it emits Word tokens from the whole `msg`.

---

## 2) Decoding and normalization

### 2.1 UTF-8 decoding
- Prefer UTF-8.
- On failure: deterministic lossy decode (replacement) and increment `utf8_decode_fallback_total`.

### 2.2 Line length cap
- `MAX_LINE_LEN = 16384` bytes (starting point).
- If longer: truncate deterministically and increment `lines_too_long_total`.

### 2.3 Whitespace
- Treat ASCII whitespace as separators.
- Do not normalize internal whitespace inside quoted values (preserve for later word split).

---

## 3) Format detection and precedence

Tokenizer attempts these in order (first match wins, but always fallback):

1) CEF: if `msg` starts with `CEF:` (after trimming leading whitespace)
   - parse header by `|` best effort
   - parse extension using reverse KV (already locked)
   - emit `Kv` events for extension pairs
   - optionally emit a few `CefHeader` events (capped; low weight later)

2) JSON object: if trimmed `msg` starts with `{` and ends with `}` and parses as object
   - flatten keys into `key_path_norm` (see section 6)
   - emit `JsonKv` events

3) CSV: only when file-level header detection says "this file is CSV with header"
   - emit `CsvKv` per column

4) Generic KV scan (streaming forward scan)
   - emit `Kv` events
   - also capture any leftover text segments into `ResidualText` (optional)

5) Plaintext word scan
   - emit `Word` events on `msg` (or on leftover segments if KV was found)

If JSON/CEF parsing fails, fall back to KV/plaintext and increment the relevant parse error counter (`json_parse_errors_total` or `cef_parse_errors_total`).

---

## 4) Generic KV parsing (forward scan)

### 4.1 Key detection
A candidate key is accepted only if:
- length 1..64
- chars are ASCII in `[A-Za-z0-9_.-]`
- preceded by start-of-string or whitespace or one of `; , ( [ {`
- followed by `=` or `:` (with rules below)

### 4.2 Separator rules
- `=` always allowed.
- `:` allowed only if the key token is immediately followed by `:` and the next char is not another `:` (avoids `http://` and time stamps).
- Additionally, do not treat `:` as KV if the key is all digits (avoids `12:34:56`).

### 4.3 Value parsing
After separator, skip one optional space.

Value forms:
- Quoted: `"..."` or `'...'`
  - capture until matching quote
  - support escapes `\`, `"`, `'`, `
` (best effort, deterministic)
- Bracketed: if value starts with `[`, `{`, or `(`, capture until matching closer with a small nesting cap (depth<=2). If cap exceeded, treat as unquoted.
- Unquoted: read until whitespace, but allow internal characters like `:/@.-_\` so paths/urls/users stay intact.

Trim trailing punctuation from unquoted values only if it is a single char from `, ; ) ] }`.

### 4.4 Pair separators
After a KV pair, skip any number of:
- whitespace
- commas/semicolons

Then continue scanning for the next key.

---

## 5) Word tokenization (plaintext)

This is intentionally fast and conservative (ASCII-first). Shapes/semantic keys do the heavy lifting.

### 5.1 Word character set
A word token is a maximal run of ASCII chars in:
- alnum plus `_ . - / : @ \`

Stop conditions:
- whitespace
- control chars
- most punctuation (except the allowed set above)

### 5.2 Token length / filtering
- `MIN_WORD_LEN = 2`
- `MAX_WORD_LEN = 64` (truncate deterministically)
- Drop tokens that are:
  - purely punctuation
  - empty after trimming

Do not do stopwords in v0.1; let DF + weighting handle common words.

### 5.3 Where words come from
- If no KV/JSON/CSV/CEF detected: tokenize the whole `msg`.
- If KV/JSON/CSV/CEF detected:
  - still tokenize the residual free text, plus
  - tokenize quoted string values that contain spaces (bounded; see caps)

---

## 6) JSON flattening rules

When JSON object parsing succeeds:

### 6.1 Key path normalization
- Flatten keys into a `_`-joined path: `userIdentity.accountId` -> `user_identity_account_id`
- Apply the same key normalization rules as semantic keys (lowercase, separators to `_`, collapse `_`).

### 6.2 Depth and array caps
- `JSON_MAX_DEPTH = 8`
- `JSON_MAX_KVS = 256` per line
- Arrays:
  - if scalar elements, emit up to `JSON_ARRAY_SCALAR_CAP = 16`
  - else emit a single `key=<ARRAY>` marker later (shape stage)

JSON parse failures increment `json_parse_errors_total` and fall back to KV/plaintext.

---

## 7) CSV with header rules (file-level)

Tokenizer only does CSV mapping when the file is in "CSV header mode":
- header stored per file path (reset on rotation/inode change)
- `CSV_MAX_COLS = 256`
- each row emits up to `CSV_MAX_KVS = 256`

If a row has wrong column count:
- map min(len(row), len(header))
- increment `csv_parse_errors_total` if mismatch is frequent (implementation detail)

---

## 8) Hard caps and deterministic drop policy

Caps (starting points):
- `MAX_TOKENS_PER_LINE = 256` (total token events)
- `MAX_KV_PER_LINE = 64` (across KV/CEF/JSON/CSV)
- `MAX_WORDS_FROM_QUOTED_VALUE = 32`

Drop priority:
1) CEF/JSON/CSV/KV pairs (structured)
2) Words from residual free text
3) Words from quoted values

When a cap is hit:
- stop emitting lower-priority tokens
- increment counters:
  - `token_cap_hits_total`
  - `kv_cap_hits_total`
  - `json_kv_cap_hits_total`
  - `word_cap_hits_total`

---

## 9) Required tests
- `kv_parses_quoted_values_with_spaces`
- `kv_does_not_split_http_urls_on_colon`
- `word_tokenizer_preserves_paths_and_domain_user`
- `cef_reverse_kv_handles_spaces_in_values`
- `json_flatten_depth_and_caps`
- `cap_drop_order_is_deterministic`
- `fallback_to_plaintext_on_parse_error`
