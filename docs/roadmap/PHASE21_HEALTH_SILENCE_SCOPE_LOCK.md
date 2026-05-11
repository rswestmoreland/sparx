# Phase 21 - Health and Silence Detection Scope Lock

Phase 21 closes the planning gap around `V_DROP` / sudden loss-of-log detection. It is a
scope and contract phase only. It does not add runtime code, tests, persisted keys,
metrics, health fields, recovery behavior, replay behavior, or active scoring behavior.

## Why this phase exists

The active sparse-row scoring model detects rarity, drift, and high-volume spikes for
windows that exist. That works for `V_SPIKE` and `V_EXTREME`, because unusually high line
or byte volume appears inside a finalized row.

`V_DROP` is different. When a source stops logging, there may be no finalized sparse row
for the scorer to process. A correct implementation needs an expectation model that can
notice missing windows, mature expected sources, and sharp volume drops without relying
on rows that may not exist.

## Scope locked in Phase 21

The future `V_DROP` implementation should focus first on:

1. Tenant/device silence
   - one tenant/device subject stops producing expected windows
   - primary first implementation target

2. Tenant aggregate silence
   - many or all expected devices under one tenant stop producing logs
   - useful for collector, directory, or tenant-level outage patterns

The first implementation should use existing concepts where possible:

- active `window_size_s`
- existing time bucket model
- tenant/device identity and device key
- baseline maturity / cold-start-style guards
- line/event volume expectations
- deterministic alert id and reason details

## Deferred scope

The following are intentionally deferred:

- per-file or per-source-path silence
- parser-class-specific silence
- vendor-event-family-specific silence
- external heartbeat or reachability checks
- maintenance-window calendars
- cross-tenant outage correlation
- general host uptime monitoring

## Output decision

Future `V_DROP` findings should use the existing `AlertV1` path so query, export, show,
drill, and operator review remain consistent.

Because the signal is absence of expected data, a `V_DROP` alert may have:

- zero lines
- zero bytes
- empty top features
- empty provenance
- reason code `V_DROP`
- reason details that explain expected-vs-observed activity

Empty provenance is acceptable for absence-of-data alerts, but drill/extract must fail
closed with a clear message that no raw span exists because the alert is about missing
data.

## False-positive controls

The first implementation must suppress or avoid `V_DROP` when:

- the subject does not have enough history
- the current bucket is normally quiet
- expected volume is below a minimum activity floor
- the tenant is disabled or terminating
- the silence is inside a configured grace interval
- timestamps/counters are inconsistent
- an equivalent silence alert is already open or recently emitted

## Contract updates made in Phase 21

- Added `contracts/34_health_silence_detection_v0_1.md`
- Updated the alert explanation contract to point `V_DROP` to Contract 34
- Updated the scoring contract to make clear that `V_DROP` is planned, not active
- Updated the alert schema contract to allow future empty-provenance absence alerts
- Updated the contract consistency checklist with the Phase 21 boundary

## Active behavior after Phase 21

Still active:

- row-based anomaly scoring for finalized windows
- high-volume detection through `V_SPIKE` and `V_EXTREME`
- recovery/service health through status, `/metrics`, and `/healthz`

Still not active:

- `V_DROP` scoring
- expected-source silence detection
- tenant aggregate silence alerts
- per-file silence alerts
- maintenance-window suppression

## Phase 22 completion note

Phase 22 later converted this scope lock into a concrete implementation plan. See
`docs/PHASE22_EXPECTED_SOURCE_STATE_VDROP_PLAN.md` and
`contracts/35_expected_source_state_vdrop_plan_v0_1.md`. The current next recommended
phase is Phase 23a: expected-source state structs and encodings.
