# Phase 25d V_DROP Diagnostics Validation Hardening and Closeout

Status: complete.

Phase 25d hardens and closes the bounded diagnostics work for the active hard-silence
`V_DROP` path. The phase keeps the detection scope unchanged: device and tenant aggregate
hard silence only.

## What changed

- tightened runtime diagnostics so `vdrop_open_silence_subjects` reflects the current
  post-evaluation open-silence state for that tenant pass
- ensured first-pass `V_DROP` emission updates the open-silence gauge immediately instead
  of waiting for a later duplicate pass
- extended `run` and `oneshot` coverage for the current open-silence diagnostic value
- closed the Phase 25 policy and diagnostics implementation sequence in docs and contracts

## Diagnostic semantics after closeout

- tracked-subject gauges represent the subjects known to the active hard-silence evaluator
  during the tenant pass
- open-silence gauges represent open dedup records after the tenant pass has evaluated
  subjects and written any newly emitted `V_DROP` dedup records
- evaluated, candidate, suppressed, and emitted-alert values remain cumulative counters
- last evaluation timestamp remains the most recent timestamp used by the tenant pass
- top-level tracked/open values are derived from per-tenant gauges
- Prometheus output remains low-cardinality: global and per-tenant only, no device labels

## Tests updated

Phase 25d updates focused runtime tests so:

- the first oneshot pass that emits device and tenant aggregate `V_DROP` alerts reports two
  currently open silence subjects
- the duplicate oneshot pass still reports two open silence subjects and emits no duplicate
  alerts
- a later observation closes both open-silence subjects and reports zero open silence subjects
- the run path also validates the post-emission open-silence diagnostic value

## Still deferred

- sharp-drop detection
- per-file/source-path silence subjects
- parser-class silence subjects
- vendor-event-family silence subjects
- external heartbeat checks
- maintenance-window calendars
- suppression-reason cardinality metrics
- recovery/replay behavior changes
- alert schema changes

## Next recommended phase

Phase 26: sharp-drop detection scope lock and planning.
