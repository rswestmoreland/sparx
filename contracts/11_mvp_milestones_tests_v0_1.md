# MVP Implementation Milestones + Tests Contract v0.1

Milestones:
0) Repo skeleton + config + global DB
1) Discovery + polling + offsets + gzip markers
2) Tokenization + semantic keys + shapes + identity capture
3) Windowing + accumulation + checkpoint restore
4) Baselines (7-day ring, 48 buckets) + scoring components
5) Alert object + explanation + storage + emit sinks
6) Worker partitioning + backpressure + metrics
7) Observability release hardening + operator ergonomics
8) Output recovery automation
9) Recovery visibility + bounded replay tuning

Definition of done:
- alerts emitted within 10 minutes
- restarts preserve offsets/windows/baselines
- tenant purge deletes tenant DB directory fully
- alert objects include scores, top_features, entities, file/offset pointers
- status and enabled endpoints expose the implemented runtime/process/schema and run-cycle metrics surface defined by Contract 10 v0.1
- `run`/`oneshot` with `output.sink=jsonl` automatically spool live write failures and attempt bounded deterministic replay passes without hiding unrecoverable delivery errors
- `status`, `/metrics`, and `/healthz` expose the active recovery backlog file/byte view and the configured automated replay max-files-per-pass value

## Phase 15a scoring policy activation
- activated `scoring.cold_start_days` as the day-based bucket maturity floor
- activated `scoring.min_lines_per_window` as the minimum alertable line-count floor
- added config validation coverage for active scoring thresholds
- added alert scoring coverage for cold-start maturity and min-lines suppression

## Phase 15b secondary alert index persistence
- activated secondary `alert_idx_time`, `alert_idx_cat`, and `alert_idx_ent` persistence on alert writes
- kept the primary `AlertV1` object authoritative for show/export/drill flows
- added tenant DB coverage for deterministic secondary index writes and stale-index replacement on alert rewrites


## Phase 15c secondary alert index query activation
- activate time-index-aware query/list/export candidate selection with backward-safe fallback to primary scans when secondary coverage is incomplete
- add tenant DB and alerts-query coverage proving complete-index selection and mixed-history fallback correctness

## Phase 15d structured alert filter activation
- activate structured `--category` and `--entity-kind/--entity-value` filters for alert list/search/export
- use secondary category/entity indexes for candidate selection only when the relevant index coverage is complete enough to preserve correctness
- keep the primary `AlertV1` object authoritative and require primary-scan fallback when structured-filter indexes are absent or incomplete
- add CLI-parse, tenant-DB, and alerts-query coverage for structured filter routing plus mixed-history fallback
