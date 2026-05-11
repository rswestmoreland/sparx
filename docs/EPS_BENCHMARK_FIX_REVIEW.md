# EPS Benchmark Fix Review

This note records the bounded fixes made after Rust 1.90 validation showed that
the default tenant/device EPS benchmark completed, but reported low throughput.

## Findings

The original synthetic corpus advanced event time by one second for every log
line. That shape is useful for stressing window transitions, cursor writes, and
durable checkpoint/finalization behavior, but it is not a representative dense
high-EPS logging scenario.

The benchmark now supports event timestamp density through
`SPARX_BENCH_EVENTS_PER_TIMESTAMP`. The default is 100, which means 100 events
share each generated timestamp. Set the value to 1 to run the older sparse
event-time shape intentionally.

## Validation fixes included

The patch also keeps the previous Rust 1.90 validation fixes bounded:

- alert list fallback scans now tolerate incomplete or unreadable secondary
  indexes and decode valid primary alert records directly from the primary
  alert keyspace
- clippy-reported needless returns in the hot oneshot path were removed
- the EPS benchmark prints event timestamp density and approximate event-time
  span per file

## Boundaries preserved

No product behavior changes were intended for alert schema, provenance, storage
layout, replay semantics, metric labels, source-stream behavior, or deferred
parser-class/vendor-family scope.
