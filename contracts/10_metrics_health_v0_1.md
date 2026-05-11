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
- current oldest spool-file timestamp/age when backlog exists
- current stale-backlog boolean plus stale-tenant count
- current per-tenant recovery backlog breakdown for tenants that presently have backlog, including oldest backlog age, stale state, per-tenant previous/last snapshot timestamps, per-tenant snapshot interval, per-tenant backlog file/byte deltas, per-tenant trend direction, per-tenant previous/last counter snapshot timestamps, per-tenant counter snapshot interval, per-tenant history-start counter snapshot timestamp, per-tenant long-window replay-rate fields, and per-tenant short-window replay-rate fields
- cumulative recovery counters for spool writes, replay successes, replay failures, and cap drops
- cumulative automated replay attempt count
- last automated replay attempt timestamp plus most recent replayed/failed file counts when present
- persisted global recovery trend snapshot timestamps, snapshot interval, backlog file/byte deltas, and trend direction when present
- persisted global recovery counter snapshot timestamps, counter snapshot interval, spool write rate, replay success rate, replay failure rate, and automated replay attempt rate when present
- persisted global recovery history-start counter snapshot timestamp plus derived long-window spool write rate, replay success rate, replay failure rate, and automated replay attempt rate when present
- configured automated replay max-files-per-pass value
- configured automated replay interval in seconds
- configured spool cap in megabytes
- active V_DROP policy state plus bounded global and per-tenant V_DROP diagnostics

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
- current oldest spool-file timestamp/age gauges when backlog exists
- current stale-backlog gauges
- current per-tenant recovery backlog gauges by tenant, including oldest age, stale state, per-tenant previous/last snapshot timestamps, per-tenant snapshot interval, per-tenant backlog file/byte deltas, per-tenant trend direction, per-tenant previous/last counter snapshot timestamps, per-tenant counter snapshot interval, per-tenant history-start counter snapshot timestamp, per-tenant long-window replay-rate gauges, and per-tenant short-window replay-rate gauges
- cumulative recovery counters for spool writes, replay successes, replay failures, and cap drops
- cumulative automated replay attempt counter
- most recent automated replay timestamp/replayed/failed gauges when present
- persisted global recovery trend gauges for previous/last snapshot timestamps, snapshot interval, backlog file/byte deltas, and trend direction
- persisted global recovery counter snapshot gauges for previous/last snapshot timestamps, counter snapshot interval, spool write rate, replay success rate, replay failure rate, and automated replay attempt rate
- persisted global recovery history-start counter snapshot gauge plus derived long-window replay-rate gauges when present
- persisted per-tenant recovery history-start counter snapshot gauges plus derived per-tenant long-window replay-rate gauges when present
- configured automated replay max-files-per-pass gauge
- configured automated replay interval gauge
- configured spool cap gauge
- active V_DROP policy gauges, diagnostic counters/gauges, and low-cardinality per-tenant diagnostic metrics

## Health endpoint
When `metrics.health_enabled=true`, `run` binds `metrics.health_bind` and
serves `/healthz`.

The health endpoint returns success when the daemon is alive and can produce the
current observability snapshot from the active runtime/global-DB state.
Its text body also includes the current recovery backlog file/byte totals,
the current oldest spool-file timestamp/age when backlog exists, the current
stale-backlog boolean plus stale-tenant count, the current per-tenant recovery
backlog breakdown including oldest age, stale state, per-tenant previous/last snapshot timestamps, per-tenant snapshot interval, per-tenant backlog file/byte deltas, per-tenant trend direction, per-tenant previous/last counter snapshot timestamps, per-tenant counter snapshot interval, per-tenant history-start counter snapshot timestamp, per-tenant short-window replay-rate fields, and per-tenant long-window replay-rate fields, cumulative recovery counters, persisted global and per-tenant recovery trend fields, persisted global recovery counter snapshot fields, persisted global and per-tenant recovery history-start fields, and derived short-window and long-window replay-rate fields,
the cumulative automated replay attempt count, the most recent automated replay
attempt timestamp/replayed/failed values when present, the configured automated
replay max-files-per-pass value, the configured automated replay interval in
seconds, the configured spool cap in megabytes, and bounded V_DROP policy/diagnostic values.

## V_DROP diagnostics
the current release adds bounded diagnostics for the active hard-silence `V_DROP` path; the current release hardens those diagnostics. the current release adds bounded open-drop diagnostics for the active sharp-drop path.

Status and health expose:
- configured global V_DROP enablement and subject enablement
- configured V_DROP missed-window, mature-window, and expected-line floors
- global tracked-subject and post-evaluation open-silence-subject gauges when known
- global evaluated-subject, candidate, suppressed-candidate, and emitted-alert counters
- global last evaluation timestamp when known
- per-tenant diagnostic values in the status surface

Prometheus exports low-cardinality global and per-tenant families only. Device labels are
not introduced in v0.1.

Active metric families include:
- `sparx_vdrop_enabled`
- `sparx_vdrop_device_enabled`
- `sparx_vdrop_tenant_enabled`
- `sparx_vdrop_min_expected_windows_missed`
- `sparx_vdrop_min_mature_windows` when configured
- `sparx_vdrop_min_expected_lines` when configured
- `sparx_vdrop_tracked_subjects` when known
- `sparx_vdrop_open_silence_subjects` when known
- `sparx_vdrop_evaluated_subjects_total`
- `sparx_vdrop_candidates_total`
- `sparx_vdrop_suppressed_candidates_total`
- `sparx_vdrop_alerts_emitted_total`
- `sparx_vdrop_last_evaluation_ts` when known
- matching `_by_tenant` families with only the `tenant_id` label

These diagnostics are observability-only and do not change alert semantics, replay
behavior, recovery behavior, or silence detection scope. Open-silence subject gauges represent
current open dedup state after each tenant evaluation pass writes newly emitted V_DROP dedup records.

## Recovery replay-rate derivation
Global short-window, global long-window, per-tenant short-window, and per-tenant long-window replay-rate fields use the same deterministic counter-rate rule:
- both endpoint snapshots must exist
- the timestamp interval must be positive
- the relevant counter delta must be nonnegative

If either endpoint is missing, the interval is not positive, or a counter delta would be negative, the derived rate is not emitted as a numeric value. Text and JSON status expose that field as `null`; Prometheus omits the optional rate gauge for that snapshot pair. This rule is analytics-only and does not change replay ordering, delivery semantics, replay cadence, spool cap behavior, or recovery control decisions.

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

## V_DROP diagnostics

Active V_DROP diagnostics include bounded aggregate and per-tenant values for:

- hard-silence subjects tracked, evaluated, suppressed, and emitted
- open hard-silence intervals
- sharp-drop subjects evaluated, suppressed, and emitted
- open sharp-drop intervals
- source-stream gate state
- source-stream subjects tracked and evaluated
- open source-stream hard-silence and sharp-drop intervals
- source-stream candidates, suppressions, emitted alerts, and last evaluation timestamp

Per-tenant Prometheus output may use only `tenant_id`. Device labels, source-path labels,
source-stream-id labels, parser-class labels, vendor-family labels, per-subject labels, and
suppression-reason labels remain prohibited.

The diagnostics surface changes no AlertV1 schema, DeviceStatsV1 layout, replay behavior,
or recovery behavior.
