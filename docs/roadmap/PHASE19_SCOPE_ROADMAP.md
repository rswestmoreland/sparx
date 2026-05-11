# Phase 19 Scope Lock and Roadmap Planning

Phase 19 is a planning-only checkpoint after the Phase 18e recovery observability
closeout. It does not add runtime behavior, persisted keys, CLI flags, metrics, health
fields, or recovery controls.

## Review basis

Reviewed the Phase 18e checkpoint tree at a documentation and source-map level:

- `README.md`
- `docs/CURRENT_PLAN_CHECKLIST.md`
- `docs/README.md`
- `PHASE_HISTORY.txt`
- `contracts/`
- `src/`
- `tests/`

No build, Cargo, or test execution was performed in this sandbox.

## Current locked state

The project is complete through Phase 18e.

The active v0.1 implementation includes:

- Fjall-backed global and per-tenant DB paths behind `src/db/`
- deterministic config loading, validation, and CLI routing
- tenant discovery, cursor reconciliation, plain/gzip reading, and ordered processing
- syslog, generic kv/json/csv, CEF reverse parsing, and plaintext fallback tokenization
- dictionary-backed canonical feature IDs with no hashed fallback namespace
- sparse window aggregation, open-window checkpointing, baseline updates, scoring, and
  `AlertV1` creation
- primary alert storage plus deterministic secondary alert indexes and backward-safe
  primary-scan fallback
- alert query/show/search/export and drill/extract surfaces using
  `AlertV1.provenance` as the authoritative file-span model
- runtime `run`, `oneshot`, `status`, `replay-spool`, migrate, tenant policy, and tenant
  purge flows
- output recovery automation and observability through status, status JSON, `/metrics`,
  and `/healthz`
- global and per-tenant recovery backlog, age, staleness, trend, replay-rate, and
  long-window history-anchor replay-rate analytics

## Scope locked by Phase 19

Phase 19 closes the recovery-observability expansion arc from Phases 16 through 18.

Do not add more recovery observability features before a release-readiness cleanup/audit
phase unless a test failure or contract inconsistency requires it.

The following remain explicitly out of scope for the immediate next implementation phase:

- richer multi-anchor or rolling replay-rate history stores
- replay ordering changes
- delivery semantic changes
- new recovery control knobs
- live multi-process DB administration
- optional Parquet or columnar archive storage
- long-term persisted dense window-vector history
- Kubernetes or multi-node deployment surfaces

## Drift and cleanup candidates identified by Phase 19

Phase 19 identified stale source comments and continuity-only storage naming as the
first Phase 20 cleanup targets. Phase 20a resolved those source-comment issues and
clarified the active Fjall / Embedded DB wording while preserving the old numbered
Contract 06 filename for continuity.

Remaining after Phase 20b:

- keep the old `contracts/06_rocksdb_topology_v0_1.md` filename only as a continuity
  artifact
- keep `V_DROP` documented as planned future silence detection, not active scoring
  behavior, until a missing-window expectation model is implemented
- create the Phase 20c release-readiness test matrix and acceptance checklist before
  any new feature phase

## Phase 20 recommendation

The next recommended phase is:

- Phase 20: release-readiness audit and cleanup

Recommended Phase 20 subphases:

1. Phase 20a: source comment and stale wording cleanup
   - remove or update phase-era comments that describe already-completed behavior as
     deferred or unimplemented
   - do not change runtime behavior
   - update tests only if comments/doc strings are asserted anywhere

2. Phase 20b: contract and docs final consistency pass
   - reconcile README, docs README, current checklist, phase history, and contracts
   - confirm all active behavior introduced through Phase 18e is represented exactly once
   - keep deferred features marked as deferred, not partially active

3. Phase 20c: release-readiness test matrix and acceptance checklist
   - create a compact operator-facing validation checklist
   - list the Cargo/test commands for external execution without claiming they were run
   - define acceptance gates for Windows local validation and Enterprise Linux target
     validation

4. Phase 20d: checkpoint closeout
   - package a clean checkpoint after the audit
   - record any remaining intentional deferrals before selecting the next feature phase

## Candidate later roadmap after Phase 20

These items should stay behind the Phase 20 cleanup gate:

- retention and maintenance operations for optional old-window/debug state
- deployment packaging and systemd/operator documentation expansion
- larger fixture corpus expansion for more vendor log shapes
- performance profiling and targeted hot-path optimization
- optional richer replay history analytics if a concrete operator need is identified
- health/silence detection scope lock for `V_DROP` and sudden expected-source loss

## Phase 19 completion criteria

Phase 19 is complete when:

- the current plan/checklist records Phase 19 as a planning-only checkpoint
- README and docs README point to Phase 20 as the next recommended phase
- Phase history records the Phase 19 scope lock
- Contract 11 records Phase 19 planning coverage
- no runtime code, tests, persisted keys, metrics, or recovery behavior are changed

Phase 20b note: this roadmap now explicitly carries `V_DROP` as future health/silence detection work and keeps the active global metrics key contract aligned to `metrics/v1/counter/<name>`. `V_DROP` is not active scoring behavior in the Phase 20b checkpoint.

Phase 20d note: Phase 20 is now closed. The release-readiness audit/checkpoint sequence
completed without runtime behavior, test, persisted-key, metrics, health, replay,
recovery, or scoring changes. The next recommended phase is Phase 21 health and silence
detection scope lock for `V_DROP` and sudden expected-source loss.

Phase 22 completion note: expected-source state and `V_DROP` implementation planning is locked in `docs/PHASE22_EXPECTED_SOURCE_STATE_VDROP_PLAN.md` and `contracts/35_expected_source_state_vdrop_plan_v0_1.md`. Phase 23a later implemented expected-source state structs, encodings, and key helpers. Phase 23b later activated finalized-window updates for `silence_subject/*` expected-source state. Phase 23c later added the pure `V_DROP` candidate evaluator. Phase 23d later added deterministic `V_DROP` `AlertV1` construction and open-silence dedup state helpers. Phase 23e later activated first runtime hard-silence V_DROP integration and operator surfacing. Phase 23f later closed the first hard-silence path with validation hardening. Phase 25a later added the V_DROP config and tenant-policy surfaces. Phase 25b through Phase 25d later completed policy resolution, diagnostics, and diagnostics validation. Phase 26a now scopes future sharp-drop detection planning.


Phase 24 later locked V_DROP policy controls and diagnostics scope in `docs/PHASE24_VDROP_POLICY_DIAGNOSTICS_SCOPE_LOCK.md` and `contracts/36_vdrop_policy_diagnostics_scope_v0_1.md`.
