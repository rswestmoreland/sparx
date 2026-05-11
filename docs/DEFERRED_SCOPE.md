# Deferred Scope

The following capabilities remain outside v1 unless explicitly approved.

## Deferred V_DROP subject families

- parser-class subjects
- vendor-event-family subjects
- external heartbeat checks
- maintenance-window calendars
- cross-tenant outage correlation

## Deferred alert/schema changes

- AlertV1 schema changes
- replacement of `AlertV1.provenance`
- legacy `source_files` drilldown behavior

## Deferred diagnostics expansions

- device-label metrics
- source-path or source-stream-id metric labels
- parser-class or vendor-family metric labels
- per-subject Prometheus fanout
- suppression-reason label cardinality

## Deferred policy refinements

- source-stream-specific threshold knobs
- maintenance-aware volume-loss suppression
- richer outage-correlation controls

These items may be revisited after v1 hardening and external validation.
