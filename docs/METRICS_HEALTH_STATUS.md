# Metrics, Health, and Status

sparx exposes bounded operator diagnostics through status output, JSON status,
Prometheus metrics, and health output.

## Status areas

- process state
- tenant discovery and lifecycle state
- output recovery and replay backlog
- recovery rate and trend analytics
- `V_DROP` hard-silence diagnostics
- `V_DROP` sharp-drop diagnostics
- source-stream `V_DROP` diagnostics when the source-stream gate is enabled

## Metrics label policy

Metrics must remain low-cardinality. Allowed per-tenant source-stream diagnostic
metrics use `tenant_id` only. The following Prometheus label fanout remains
prohibited:

- device labels
- source path labels
- source-stream id labels
- parser-class labels
- vendor-family labels
- per-subject labels
- suppression-reason labels

## Health output

Health output is an operator surface, not an alert object schema. It should
summarize service and recovery health without changing replay or recovery
semantics.
