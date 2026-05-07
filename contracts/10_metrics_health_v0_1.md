# Metrics + Health Contract v0.1

## Scope in v0.1
The operator-facing observability surface in v0.1 now includes:
- `sparx status` in human-readable form
- `sparx status --json`
- a Prometheus text endpoint at `/metrics` during `run` when enabled
- a health endpoint at `/healthz` during `run` when enabled

These surfaces remain grounded in runtime/process/schema state and real
run-cycle totals that Sparx already computes.

## Status surface
`status` reports deterministic runtime and process metadata plus the persisted
run-cycle metrics that actually exist in the implementation:
- version string
- configured mode
- configured window size
- configured output sink
- resolved runtime roots and DB paths
- known and active tenant counts
- last run start/end/exit metadata when present
- global schema version and migration timestamps when present
- observability endpoint enable/bind settings
- derived endpoint URLs when enabled
- cumulative run-cycle totals
- most recent cycle summary when present
- current recovery backlog file/byte totals
- configured automated replay max-files-per-pass value

## Metrics endpoint
When `metrics.prometheus_enabled=true`, `run` binds `metrics.prometheus_bind`
and serves Prometheus text on `/metrics`.

The endpoint exports only metrics backed by real implementation data, including:
- tenant known/active counts
- process timestamps/exit code when present
- schema version/timestamps when present
- cumulative run-cycle totals
- most recent cycle summary values when present
- current recovery backlog file/byte gauges
- configured automated replay max-files-per-pass gauge

## Health endpoint
When `metrics.health_enabled=true`, `run` binds `metrics.health_bind` and
serves `/healthz`.

The health endpoint returns success when the daemon is alive and can produce the
current observability snapshot from the active runtime/global-DB state.
Its text body also includes the current recovery backlog file/byte totals and
the configured automated replay max-files-per-pass value.

## Endpoint behavior
- `/metrics` returns `404` for the wrong path and `405` for non-`GET` requests
- `/healthz` returns `404` for the wrong path and `405` for non-`GET` requests
- `status` reports derived endpoint URLs when the corresponding endpoint is enabled

## Failure model
- endpoint bind failures fail closed at `run` startup
- if one observability endpoint starts and the next bind fails, the already-started listener is shut down before `run` returns the startup error
- disabled endpoints are not bound
- endpoint snapshot failures return `503`
- `status` retains deterministic DB/runtime error handling
