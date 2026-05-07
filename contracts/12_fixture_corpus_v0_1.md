# Fixture Corpus Contract v0.1

## Layout
fixtures/tenants/<tenant>/devices/<device>/{.log,.gz,.jsonl,.csv,.cef}
golden outputs and generator configs under fixtures/golden and fixtures/gen.

## Handwritten fixtures (small)
- linux syslog-ish
- windows-ish plaintext
- palo alto CSV with header
- fortigate KV
- CEF examples
- cloudtrail JSON (prefer JSONL)
- elastic JSONL with dotted keys
- mixed stress mini

## Gzip fixtures
- include at least one `.gz` per tenant; keep small (<100KB)

## Golden smoke outputs
- expected alerts subset (avoid strict floating compare)
- expected status snapshot

## Deterministic generator
- seed + scenario config
- format mix + anomaly injection
- outputs manifest with ground truth for tests
