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

## Deferred signal-processing candidates

- autocorrelation-lite for repeated-interval, heartbeat-like, retry-loop, or beacon-like behavior
- DFT/FFT-style frequency-domain analysis for offline periodicity review

These candidates are recorded in `../contracts/41_deferred_signal_processing_candidates_v0_1.md` and require a separate contract before implementation.

These items may be revisited after v1 hardening and external validation.
