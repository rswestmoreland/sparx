# Phase 25c V_DROP Diagnostics Counters and Operator Surfacing

Status: complete.

Phase 25c adds bounded diagnostics for the active hard-silence `V_DROP` path. The
implementation is observability-only: it does not add sharp-drop detection, new subject
scopes, recovery behavior, replay behavior, or new alert semantics.

## Active diagnostics

Runtime `run` and `oneshot` now persist deterministic diagnostics for each tenant V_DROP
runtime pass:

- tracked silence subjects
- open silence subjects
- evaluated subjects total
- candidates total
- suppressed candidates total
- emitted alerts total
- last evaluation timestamp

Tracked and open-silence subject counts are gauges. Evaluated, candidate, suppressed,
and emitted-alert counts are cumulative counters. The last evaluation timestamp is stored
as the most recent evaluation timestamp.

## Operator surfaces

The diagnostics are exposed through the existing operator surfaces:

- `sparx status`
- `sparx status --json`
- `/metrics`
- `/healthz`

The top-level status view includes global totals. Tenant diagnostics are also exposed in
status JSON and text output. Prometheus metrics remain low-cardinality: global values and
per-tenant values only. Device-level labels are intentionally not introduced.

## Runtime rules

- Diagnostics are updated only when a tenant's V_DROP runtime pass is evaluated.
- Disabled global V_DROP policy suppresses evaluation and therefore does not create new
  evaluation diagnostics for that pass.
- Tenant-policy overrides still control whether device and tenant-aggregate subjects are
  evaluated.
- Open-silence counts represent currently open dedup records after the evaluation
  pass writes any newly emitted V_DROP dedup records.
- Emitted-alert counters are incremented after successful sink emission.

## Tests updated

Phase 25c extends coverage for:

- `status` text defaults for V_DROP diagnostics
- `status --json` populated global and per-tenant diagnostics
- `/metrics` and `/healthz` V_DROP diagnostic surface
- `run` path V_DROP diagnostic persistence
- `oneshot` path candidate, suppression, emitted, open-state, and closure diagnostics

## Still deferred

- sharp-drop detection
- per-file/source-path silence subjects
- parser-class silence subjects
- vendor-event-family silence subjects
- external heartbeat checks
- maintenance-window calendars
- dedicated suppression-reason cardinality metrics
- recovery/replay behavior changes

## Next recommended phase

Phase 25d: V_DROP diagnostics validation hardening and closeout is complete.
