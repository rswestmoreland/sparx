# Message Tokenization Boundary Contract v0.1

## Two-stage parse
1) Syslog envelope peel -> `envelope` + `msg`
2) Tokenize primarily `msg`

## Envelope features (structured, capped)
Default ON:
- `syslog_pri=<INT>`
- `syslog_app=<APP>`
Default OFF:
- `syslog_host=<HOST>` (high cardinality)

## Identity boundary
- High-confidence identities come from payload keys (KV/CEF/JSON/CSV).
- Envelope-derived identities are low confidence unless overridden.

## Double-wrapped syslog
- Optionally peel a second syslog header inside msg (depth <= 2).
- Prefer inner app for `syslog_app` feature.
