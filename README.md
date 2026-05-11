# sparx

sparx is a Sparse Matrix Log Analyzer for Enterprise Linux. It processes large,
heterogeneous log collections across many tenants and devices, builds stable
behavioral baselines, and emits explainable alerts with retained source
provenance for analyst and customer review.

This project was inspired by a friend's love of sparse matrices and signal
processing. It is an intersection between data science, log management, and
cybersecurity.

## What a sparse matrix is

A matrix is a rectangular table of values. In log analysis, a useful model is:

- rows represent observations, such as a device during a time window
- columns represent possible features, such as event types, users, IPs,
  commands, paths, vendor fields, shape classes, or normalized tokens
- values represent measurements, usually counts or weighted counts

A dense matrix stores every row/column value, including zeros. A sparse matrix
stores only nonzero values. That distinction matters because log feature spaces
are usually very wide while each observation touches only a small subset of the
possible columns.

sparx treats each finalized time window as a sparse row:

- each row is a tenant/device/window slice
- each column is a canonical `FeatureId`
- each stored value is the count observed for that feature in the window

All omitted features are treated as zero without being stored. This keeps
multi-tenant telemetry practical while still supporting rarity scoring, drift
scoring, spike/extreme volume scoring, hard-silence detection, sharp-drop
detection, source-stream volume-loss detection, and explainable alerting.

## Why sparse rows help log analysis

Enterprise logs are wide and inconsistent. A single environment can contain
firewalls, endpoints, applications, identity systems, cloud services, and custom
collectors. Each product has its own fields, formats, and high-cardinality
values. sparx uses sparse rows so it can retain useful signal without building a
large zero-filled table.

Advantages include:

- storage grows with observed features, not every possible feature
- tenant dictionaries, baselines, and retention boundaries stay isolated
- deterministic feature IDs, row keys, tie-breaks, and explanations are possible
- sparse dot products and vector norms can compare windows to baselines
- top contributing features can be ranked from the same data used for scoring
- provenance can point alerts back to exact source file spans
- unknown or partially parsed formats can still emit deterministic token and
  shape features

## Supported input model

sparx reads per-tenant watch roots with per-device log directories. It supports
plain text and gzip where applicable, and tokenizes heterogeneous formats:

- syslog envelope variants
- key/value logs
- JSON logs
- CSV logs
- CEF with reverse parsing rules
- plaintext fallback

Parsers normalize what they can, emit canonical features and entity sketches,
and fall back to deterministic token/shape features when a format is only
partially known.

## Alerting and health scope

The active alert model writes `AlertV1` records for finalized sparse rows and
volume-loss conditions. `AlertV1.provenance: Vec<FileSpanV1>` is the only
authoritative drilldown field model.

Active alert and health signals include:

- rarity, drift, spike, and extreme volume components for sparse rows
- `V_DROP` hard-silence detection for device and tenant aggregate subjects
- `V_DROP` sharp-drop detection for reduced-but-nonzero activity
- `V_DROP` source-stream detection behind a default-off source-stream gate
- status, JSON status, Prometheus metrics, and health output for bounded
  operator diagnostics
- alert query/export/drill/extract workflows backed by persisted `AlertV1`
  records and secondary alert indexes

## Storage and runtime model

- Fjall is the active embedded DB backend.
- Fjall stays behind the internal adapter boundary under `src/db/`.
- sparx uses a single-owner embedded DB model.
- DB-backed CLI and runtime flows fail closed.
- `replay-spool` is filesystem/config based and does not open Fjall.
- `replay-spool` is valid only for replay-compatible file sinks; stdout fails
  closed.

## Operator workflows

The current CLI/runtime surface includes:

- `run`
- `oneshot`
- `status`
- `status --json`
- `/metrics`
- `/healthz`
- `tenant policy show`
- `tenant policy check`
- `purge`
- `migrate`
- `alerts query/search/show/export`
- `alert drill/extract`
- `replay-spool`

## Benchmarking

sparx includes a dependency-free tenant/device EPS benchmark:

```bash
cargo bench --bench tenant_device_eps
```

The benchmark generates a deterministic multi-tenant, multi-device corpus, runs
the existing `oneshot` runtime path, and prints total events per second as
`total_eps`. The default workload models dense high-EPS logging by grouping many
events under the same event timestamp; sparse event-time stress runs are also
available. See `docs/BENCHMARKING.md` for workload controls and interpretation
guidance.

## Repository guide

- `contracts/`: locked v0.1 contracts and behavior boundaries
- `docs/`: user-facing architecture, operations, configuration, alerting, and
  validation guidance
- `docs/roadmap/`: archived historical checkpoint notes, retained for project
  traceability
- `fixtures/`: minimal fixture corpus and expected outputs
- `src/`: Rust implementation
- `tests/`: deterministic unit and integration coverage
- `LICENSE` and `NOTICE.md`: MIT license and author/copyright metadata

## Current status

The current checkpoint includes source-stream `V_DROP` behavior for v1 behind the
default-off source-stream gate. Parser-class subjects, vendor-event-family
subjects, source-stream-specific threshold knobs, heartbeat checks, maintenance
calendars, cross-tenant outage correlation, and AlertV1 schema changes remain
outside the v1 scope unless explicitly approved.

Before v1 release, sparx requires Rust 1.90 or newer (see `rust-toolchain.toml`) and
user-run Rust toolchain validation logs for formatting, build, tests, clippy, and
benchmarks, followed by release packaging and operator documentation review.

## Current hardening status

The current checkpoint includes security, performance, consistency, and bad-data hardening. Covered areas include alert provenance path validation, spool path safety, symlink-resistant spool inventory, bounded ingest resource caps, chunked plain-text runtime reading, coherent source comments, explicit runtime invariant errors, and malformed-readable-log stability coverage. Final release validation still requires user-run Rust toolchain logs.

## License

sparx is open source under the MIT License.

Author: Richard S. Westmoreland  
Contact: dev@rswestmore.land  
Copyright (c) 2026 Richard S. Westmoreland.

See `LICENSE` for the full license text.
