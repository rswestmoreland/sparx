# Format Handling Guarantees Contract v0.1

Universal:
- never reject unknown formats; fallback to plaintext tokenization
- deterministic tokenization; lossy UTF-8 fallback; line truncation with counters
- best-effort timestamp; fallback to ingest time

JSON:
- JSONL objects supported (depth cap, array element cap)
- JSON arrays in files best-effort (optional; may treat as plaintext in MVP)
- embedded JSON optional best-effort

KV:
- `key=value` and common variants supported; quoted values with spaces supported

CSV:
- header detection best-effort; with header supported; without header plaintext (or optional colN keys)

CEF:
- header by `|` best-effort
- extension parsed as KV with overrides; reverse parsing (see separate contract)

Multiline:
- v0.1 does not reconstruct multiline events; line-based processing only

Gzip:
- streamed; drilldown limitations as per drilldown contract

Zlg:
- `.zlg` archives are supported as line-oriented zstd-backed log archives
- zlg chunk payloads are decoded and fed through the same tokenization path as plain text
- zlg provenance offsets refer to archive chunk byte ranges rather than decoded-line byte positions
- zlg files are expected to be finalized archives; replace atomically rather than editing in place

Degrade gracefully:
- if specialized parse fails: fallback to plaintext + increment counters
