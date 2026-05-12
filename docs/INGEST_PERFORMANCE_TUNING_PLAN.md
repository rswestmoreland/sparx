# Ingest Performance Tuning Plan

This plan captures the next performance work for sparx. The goal is to make
polling, parsing, tokenization, sparse-row population, and alert evaluation
faster without changing public behavior or weakening safety boundaries.

## Current benchmark model

The tenant/device EPS benchmark reports separate metrics for:

- ingestion: file discovery, reading, parsing/tokenization, feature emission,
  and sparse-row population
- detection: scoring, alert construction, and alert encoding over finalized
  sparse rows
- optional durable oneshot total timing

The benchmark is the primary tool for measuring improvements.

## Current validated performance baseline

The current checkpoint has a retained Rust 1.90 validation report with green
formatting, check, test, and clippy results before benchmarks were recorded. The
current planning baseline is:

- default 10000-event split ingestion: about 58000 events per second
- 100000-event split ingestion: about 70000 events per second
- split-path detection: about 740000 to 1390000 events per second over finalized sparse rows
- default 10000-event durable oneshot: about 3100 events per second

These values are estimates from one modest single-node validation environment,
not guaranteed deployment throughput. Treat durable oneshot as the closest
storage-inclusive runtime estimate in the current benchmark target.

Further optimization should start from benchmark evidence and should keep the
current validation flow green before and after changes.

## Optimization candidates

### Parser hot path

- reduce temporary string allocation during tokenization
- reuse line buffers where safe
- avoid repeated lowercase/trim work when canonicalization has already happened
- keep lossy UTF-8 handling bounded and explicit
- keep malformed-readable-log stability behavior unchanged

### Sparse-row population

- reduce map lookups when multiple features are emitted for one line
- batch feature count updates within a finalized window
- avoid sorting until deterministic output or persisted order requires it
- preserve stable IDs and deterministic tie-breaks

### File polling and read path

- keep chunked plain-text reads
- tune `read_chunk_bytes` defaults using benchmark evidence
- preserve conservative gzip offset handling
- avoid following symlinks or unsafe paths

### Durable writes

- batch writes at window finalization boundaries where possible
- avoid opening additional DB owners in tests and runtime paths
- keep Fjall behind the adapter boundary
- preserve fail-closed behavior for unsafe or invalid storage state

### Pipeline strategies

- separate discovery/read, parse/tokenize, sparse-row aggregation, and detection
  as conceptual stages
- evaluate bounded worker partitioning by tenant or device only after the single
  threaded hot path is clean
- keep deterministic finalization order even if parallel workers are introduced
- prefer bounded queues and explicit flush points over unbounded async fanout

## Non-goals

- no high-cardinality metrics
- no behavior changes for replay ordering
- no AlertV1 schema changes
- no change to fixed stats layouts
- no unsafe Rust
- no broad refactor before benchmark evidence identifies the bottleneck

## Measurement rules

Each optimization should record:

- default ingest EPS
- default detection EPS
- 100000-event ingest EPS
- 100000-event detection EPS
- optional durable oneshot total EPS
- files changed
- behavior preserved

Do not publish performance claims from a failed validation run.

