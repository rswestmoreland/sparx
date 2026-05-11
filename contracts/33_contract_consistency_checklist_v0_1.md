# Contract Consistency Checklist v0.1

This checklist records the active v0.1 consistency gates. Historical checkpoint
notes are archived under `docs/roadmap/` and are not the active contract surface.

## Persistence coverage

Every active key family must have both:

- a key-prefix contract entry
- a byte-level value encoding contract when the value is persisted binary data

Required active coverage:

- global DB key prefixes
- tenant DB key prefixes
- open-window checkpoint values
- baseline sketch and stats values
- tenant simple values
- AlertV1 objects
- alert secondary indexes
- expected-source state
- hard-silence open state
- sharp-drop open state
- source-stream catalog, stats, expected-source state, provenance, and open state

## Feature pipeline coverage

Required contracts and docs must cover:

- syslog envelope handling
- CEF reverse extension parsing
- key/value, JSON, CSV, and plaintext fallback handling
- feature emission families
- feature caps and deterministic drop ordering
- entity sketches
- canonical feature IDs
- no active hashed-fallback FeatureId behavior

## Baseline and scoring coverage

Required contracts and docs must cover:

- window sizing and bucket scheme
- DF-ring sizing and retention
- centroid/stats persistence
- scoring components and thresholds
- cold-start and low-volume suppression
- hard-silence and sharp-drop ratio semantics
- source-stream expected-volume behavior

## Alerting coverage

Required contracts and docs must cover:

- AlertV1 schema
- deterministic alert IDs
- reason details
- top features
- entity sketches
- `AlertV1.provenance` as the authoritative drilldown field model
- primary alert objects and active `alert_idx_*` persistence
- query/export/show/drill/extract behavior
- drill/extract path validation and canonical tenant-root containment

## Operational coverage

Required contracts and docs must cover:

- config schema, defaults, bounds, resource caps, and environment overrides
- tenant policy schema and inherited defaults
- CLI commands, exit codes, and fail-closed behavior
- output sinks and replay-spool behavior
- filesystem component validation and symlink-resistant spool inventory
- malformed-readable-log handling remains bounded and stable
- source comments explain non-obvious safety and performance boundaries
- single-owner embedded DB behavior
- service/deployment expectations
- tenant purge and migration behavior

## Metrics and health coverage

Required contracts and docs must cover:

- status text fields
- JSON status fields
- Prometheus metric names and allowed labels
- health output
- recovery backlog and replay-rate diagnostics
- hard-silence, sharp-drop, and source-stream diagnostics
- explicit prohibition of high-cardinality metric labels

## Current release boundaries

The current v1 boundary includes:

- active device and tenant aggregate hard-silence `V_DROP`
- active device and tenant aggregate sharp-drop `V_DROP`
- source-stream `V_DROP` behind a default-off source-stream gate
- bounded diagnostics for active volume-loss subjects

The following remain deferred unless explicitly approved:

- parser-class volume-loss subjects
- vendor-event-family volume-loss subjects
- heartbeat checks
- maintenance-window calendars
- cross-tenant outage correlation
- source-stream-specific threshold knobs
- AlertV1 schema changes
- replay or recovery semantic changes

## No-drift gate

Do not merge implementation changes unless active docs and contracts remain
consistent with source, tests, and persisted encodings. If a change affects a
contract boundary, update the relevant contract and active user-facing docs in
the same checkpoint.

## Open-source metadata consistency

Active release artifacts must consistently identify sparx as MIT licensed and
attribute authorship to Richard S. Westmoreland with contact
`dev@rswestmore.land`. Rust source and test files should carry SPDX MIT
headers.
