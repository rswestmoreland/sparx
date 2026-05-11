# EPS Benchmark Split Review

This checkpoint updates the tenant/device EPS benchmark so it reports two
separate performance signals instead of one ambiguous end-to-end value.

## Changes

- Reduced the default benchmark workload to 10000 events.
- Set the documented larger validation workload to 100000 events.
- Added `ingest_eps` for the ingestion path:
  - file scan and line read
  - syslog parse
  - tokenization
  - canonical feature emission
  - feature dictionary resolution
  - sparse window and row population
- Added detection metrics over finalized sparse rows:
  - `detection_event_eps`
  - `detection_row_eps`
  - `detection_alert_eps`
  - emitted alert count and encoded alert bytes
- Kept optional durable `oneshot` timing behind `SPARX_BENCH_DURABLE_ONESHOT=1`.

## Rationale

The earlier single `total_eps` value measured the durable runtime path. That is
useful for regression tracking, but it mixes ingestion, embedded DB writes,
cursor updates, recovery bookkeeping, baseline updates, scoring, and sink costs.
It can make Rust-level parser and sparse-row throughput look much slower than it
is.

The split benchmark makes the main measurements easier to interpret:

- ingestion EPS answers how fast sparx can read, parse, tokenize, and populate
  sparse matrices
- detection EPS answers how fast sparx can use finalized sparse rows to build
  alerts
- optional durable oneshot EPS remains available for storage-inclusive timing

## Validation status

The chat sandbox did not run the Rust toolchain. User-run validation with Rust
1.90 or newer is still required.
