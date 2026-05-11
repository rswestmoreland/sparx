# Storage and Retention

sparx uses Fjall as the active embedded DB backend. The storage engine remains
behind the internal adapter boundary in `src/db/`.

## Storage rules

- single-owner embedded DB model
- fail-closed DB-backed CLI and runtime flows
- deterministic key construction
- stable binary encodings for persisted values
- no storage-engine access outside the internal DB boundary

## Key families

Important tenant-scoped families include:

- cursors
- open-window checkpoints
- feature dictionaries
- sparse window rows
- baseline sketches and stats
- primary alert objects
- secondary alert indexes
- expected-source state
- hard-silence open state under `silence_open/*`
- sharp-drop open state under `drop_open/*`
- source-stream catalogs, stats, expected-source state, provenance, and open state

## Replay-spool

`replay-spool` is filesystem/config based and does not open Fjall. It is valid
only for replay-compatible file sinks. `stdout` fails closed for replay because a
stdout replay cannot safely guarantee durable delivery.

## Retention and cleanup

Retention and purge operations must preserve deterministic ordering, tenant
isolation, and fail-closed behavior. Alert drilldown remains based on
`AlertV1.provenance` rather than legacy source-file fields.
