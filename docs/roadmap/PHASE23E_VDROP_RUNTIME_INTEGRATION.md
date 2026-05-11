# Phase 23e V_DROP Runtime Integration and Operator Surfacing

Phase 23e activates the first runtime path for hard-silence `V_DROP` detection.

This phase integrates the already-implemented expected-source state, candidate evaluator,
alert builder, and open-silence dedup state into `run` and `oneshot`. It keeps the scope
intentionally narrow: tenant/device silence and tenant aggregate silence only.

## Active behavior

After finalized-window processing for a tenant, sparx now evaluates mature expected-source
state for hard-silence candidates:

- device subject: `silence_subject/v1/device/<device_key>/state`
- tenant aggregate subject: `silence_subject/v1/tenant/state`

When a subject is mature, expected to be active, and has missed the configured minimum
number of windows, the runtime can emit a deterministic `AlertV1` with reason code
`V_DROP`.

The alert is persisted through the existing primary alert path and emitted through the
existing configured alert sink. This makes `V_DROP` visible through the existing operator
alert workflows such as search, show, export, and JSONL output.

## Deduplication and closure

When a `V_DROP` alert is emitted, sparx writes matching open-silence state:

- `silence_open/v1/device/<device_key>`
- `silence_open/v1/tenant`

A matching open-silence state suppresses duplicate alerts for the same ongoing silence
interval.

When a later finalized window is observed for that subject, the matching open-silence
state is marked closed. This allows a later independent silence interval to alert again
without duplicating the already-open interval.

## Determinism and false-positive controls

Phase 23e uses the Phase 23c evaluator, including fail-closed suppression for invalid or
immature state. The initial runtime integration keeps a conservative default missed-window
floor of three windows.

The first runtime implementation does not introduce a new public config knob. Future work
may expose a named silence policy once operator defaults are validated.

## Boundaries

Phase 23e does not add:

- sharp-drop detection for reduced-but-nonzero volume
- per-file/source-path silence detection
- parser-class silence detection
- vendor-event-family silence detection
- external heartbeat or reachability checks
- maintenance-window calendars
- cross-tenant outage correlation
- new metrics or health fields
- recovery, replay, or delivery semantic changes

## Tests

Phase 23e adds focused runtime coverage for:

- `run` emitting device and tenant aggregate `V_DROP` alerts from mature hard-silence state
- `oneshot` emitting device and tenant aggregate `V_DROP` alerts
- existing alert search surfacing `V_DROP`
- open-silence dedup suppressing duplicate `oneshot` emissions

## Next recommended phase

Phase 23f later completed V_DROP validation hardening and closeout. Phase 25a later added the V_DROP config and tenant-policy surfaces. Phase 25b through Phase 25d later completed policy resolution, diagnostics, and diagnostics validation. Phase 26a now scopes future sharp-drop detection planning.


## Phase 23f closeout note

Phase 23f adds validation hardening for the Phase 23e runtime behavior. It verifies that
closed open-silence state no longer suppresses later candidates and that later observations
close device and tenant aggregate open-silence state without adding duplicate `V_DROP`
alerts.


Phase 24 later locked V_DROP policy controls and diagnostics scope as planning-only work.
