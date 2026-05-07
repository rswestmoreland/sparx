# Shape Catalog v0.1

## Goals
- Deterministic `<SHAPE>` classification across formats.
- Reduce high-cardinality noise from raw literals.
- Support explainable feature families.

## Shape types (v0.1)

### Network
- `<IPV4>`
- `<IPV6>`
- `<MAC>`
- `<PORT>` (only when key classified as Port)

### Time
- `<RFC3339_TS>`
- `<SYSLOG_TS>` (BSD)
- `<EPOCH_S>` / `<EPOCH_MS>` / `<EPOCH_NS>` (guarded)
- `<DATE>`
- `<TIME>`

### Identity-ish / OS
- `<SID>`
- `<WIN_USER>` (DOMAIN\user)
- `<UPN>` (user@domain) when user-like
- `<EMAIL>` (email-like)
- `<HOSTNAME>` (guarded)

### Paths / commands
- `<WIN_PATH>`
- `<UNIX_PATH>`
- `<URL>`
- `<REG_KEY>`
- `<CMDLIKE>` (cautious)

### Identifiers / blobs
- `<UUID>`
- `<HEX_N>` (bucketed by length ranges)
- `<B64_N>` (bucketed by length ranges)
- `<INT>` (guarded)
- `<FLOAT>`
- `<PERCENT>`
- `<SIZE>` (optional)

### Text fallback
- `<WORD>`
- `<ALNUM>`
- `<OTHER>` (usually not emitted)

## Priority rules (match order)
1) IP/MAC/SID/UUID/paths/URL/registry
2) timestamps/epochs
3) numeric
4) blob buckets (hex/b64)
5) hostname/user/email-like (key-context gated)
6) fallback word/alnum

## Emission guidance
- For SourceIp/DestIp with IP match: emit shape + bucket + store identity.
- For User: emit `<WIN_USER>/<UPN>/<EMAIL>/<NAME>` style subset and store identity.
