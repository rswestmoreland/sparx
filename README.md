# sparx

sparx is a Sparse Matrix Log Analyzer for Enterprise Linux. Its objective is to process
large, heterogeneous log collections across many tenants and devices in near-real time,
build stable behavioral baselines, and emit explainable alerts that are useful to both
analysts and customers.

## How sparx uses a sparse matrix

sparx treats each finalized time window as a sparse row:

- each row represents a specific tenant/device/window slice
- each column is a canonical `FeatureId`
- each stored value is the count observed for that feature in the window

The representation stays sparse because sparx only persists features that were actually
observed in that window. It does not materialize a dense feature table full of zeros.
That makes the system practical for large multi-tenant log streams where each device and
window only touches a small fraction of the total feature space.

From those sparse rows, sparx:
- maintains per-tenant feature dictionaries
- updates DF-ring, centroid, and fixed-layout stats baselines
- scores outliers and noise suspects
- writes explainable `AlertV1` objects that retain provenance for drill/extract

## Operating model

- multi-tenant layout with per-tenant watch roots and per-device log directories
- tails actively written plain-text and gzip-compressed files where applicable
- supports heterogeneous input formats:
  - syslog envelope variants
  - key/value logs
  - JSON logs
  - CSV logs
  - CEF with reverse parsing rules
  - plaintext fallback
- emits canonical features and entity sketches from tokenized events
- aggregates events into time windows, persists open-window checkpoints, finalizes rows,
  updates baselines, scores results, and writes explainable alerts
- supports operator-facing workflows including purge, migrate, policy validation,
  alert query/export, alert drill/extract, replay-spool, status, oneshot, and run

## Storage and runtime model

- storage engine target for the real DB/runtime layer: Fjall
- Fjall remains behind a thin internal adapter boundary under `src/db/`
- sparx uses a single-owner embedded DB model
- DB-backed CLI/runtime flows must fail closed rather than pretending to succeed
- `AlertV1.provenance: Vec<FileSpanV1>` is the only authoritative drilldown field model

## Repository guide

- locked v0.1 contracts are in `contracts/`
- current planning and design notes are in `docs/`
- consolidated phase history is in `PHASE_HISTORY.txt`
- minimal fixture corpus is under `fixtures/`

## Current status

- Phases 0 through 12 are complete
- Phase 12e completed tenant lifecycle runtime reconciliation for the daemon path,
  including disable/terminating enforcement without restart, deterministic active-index
  reconciliation across discovered and known tenants, and tenant `last_seen_ts` updates
  during observed inventory cycles
- Phase 12.5 completed contract/config/docs closure before Phase 13
- Phase 12.5a completed the scope lock and checklist insertion for that closure work
- Phase 12.5b completed hashed-fallback retirement across contracts and stale config surface
- Phase 12.5c completed config contract reconciliation and validator hardening
- Phase 12.5d completed observability contract narrowing to the current status-centered v0.1 surface
- Phase 12.5e completed output sink and spool reconciliation against the narrowed active runtime/config surface
- Phase 12.5f completed Fjall note and doc closure, including removal of stale planning wording and dead doc artifacts
- Phase 12.5g completed the final consistency sweep and closeout across contracts, docs, config wording, and tests
- Phase 13a completed observability expansion
- Phase 13b completed release hardening and final operator ergonomics
- Phase 14a completed output recovery automation
- Phase 14b completed recovery visibility and tuning
- Phase 15a completed scoring policy activation
- Phase 15b completed secondary alert index persistence
- Phase 15c completed secondary alert index query activation
- Phase 15d completed structured alert filter activation
- next recommended phase: 16a replay cadence and spool-cap tuning

## Current implementation priorities

- Phase 15b activated deterministic secondary `alert_idx_*` persistence alongside the primary alert object
- Phase 15c activates the persisted `alert_idx_time` path for list/search/export candidate selection when coverage is complete
- Phase 15d activates structured category/entity alert filters on top of the persisted secondary indexes while preserving backward-safe fallback to primary scans
- keep the primary `AlertV1` object authoritative for show/export/drill flows
- next focus: carefully scoped replay cadence and spool-cap tuning for output recovery
