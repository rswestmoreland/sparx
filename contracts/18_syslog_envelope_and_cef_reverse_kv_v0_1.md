# Syslog Envelope + Timestamp Extraction + CEF Reverse KV Contract v0.1

## Syslog envelope goal
Extract:
- `pri? version? ts_guess? host? app? procid? msgid? structured_data?`
and return `msg` (vendor payload). Never hard-fail.

### Step 0: optional PRI
If starts with `<digits>` then `>` capture `pri` and advance.

### Step 1: RFC5424-like
Attempt: `VERSION TIMESTAMP HOST APP PROCID MSGID STRUCTURED-DATA MSG`
- TIMESTAMP is ISO-ish (RFC3339 variations allowed)
- STRUCTURED-DATA is `-` or `[ ... ]` (may include spaces)
If success, remainder is msg.

### Step 2: RFC3164/BSD
Attempt: `Mon dd hh:mm:ss HOST TAG: MSG` (TAG may include `[pid]:`)
Remainder is msg.

### Step 3: ISO timestamp without full RFC5424 fields
Attempt: `ISO_TS [HOST] [APP:] MSG` with heuristics.

### Step 4: Cisco ASA extra prefix peel
After 1-3, if msg begins with `<TOKEN>:` segments (incl timestamp tokens),
peel up to 2 segments to reach vendor payload (e.g., `%ASA-...`).
Capture peeled prefixes; if one is a timestamp, keep as embedded ts candidate.

## Timestamp selection for windowing
1) envelope ISO/RFC5424 timestamp
2) embedded timestamp (ASA-like)
3) BSD timestamp
4) ingest time

BSD year inference:
- use ingest year; if parsed time is >24h future vs ingest, subtract 1 year.

## CEF extension reverse KV parsing
Rationale: values may contain spaces even though pairs are space-delimited.

Algorithm:
- parse CEF header by `|` best-effort
- extension: consume pairs from the right
  - scan backward for `=`
  - validate key left of `=` as 1..32 chars in `[A-Za-z0-9_.-]` with whitespace boundary
  - value is slice from `eq+1..end` trimmed
  - strip one layer of quotes if present
  - unescape best-effort (\\, \|, \=, \n)
  - set `end = key_start` and continue
Stop when no valid pair remains.

Each parsed pair feeds semantic key + shape pipeline.

Degrade: if parsing fails, fall back to plaintext tokenization and increment counters.
