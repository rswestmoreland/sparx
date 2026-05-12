# sparx Contracts v0.1

This folder contains the locked contracts for sparx, the Sparse Matrix Log
Analyzer.

Project terms:

- App/repo name: `sparx`
- Descriptive name: Sparse Matrix Log Analyzer
- Tenant terminology: use tenant, not customer
- Identity handling: no redaction by default; analysts and customers can access
  retained identities according to deployment policy

## Contract set

See `INDEX.md` for the ordered file list.

The contracts are deterministic and testable. Later revisions should bump
versions and include migration notes where persistence formats change.

## Current active scope

Current v1 scope includes:

- multi-tenant ingest from per-device directories
- syslog, key/value, JSON, CSV, CEF, and plaintext tokenization
- sparse row construction and deterministic feature emission
- DF-ring, centroid, and fixed-layout stats baselines
- AlertV1 scoring, persistence, indexing, query, export, drill, and extract
- output sinks, recovery spool, and replay-spool behavior
- status, JSON status, metrics, and health output
- hard-silence and sharp-drop V_DROP for device and tenant aggregate subjects
- source-stream V_DROP behind the default-off source-stream gate
- locked signal-processing MVP boundary for planned EWMA and hour-of-week periodic volume baselines

## Signal-processing baseline boundary

Signal-processing extensions are auxiliary baseline state over sampled window
signals. They must preserve sparse row encoding, AlertV1 schema, DeviceStatsV1,
SourceStreamStatsV1, and the Fjall adapter boundary. The planned MVP direction
is EWMA volume smoothing plus hour-of-week periodic volume baselines after
the validated performance checkpoint.
Autocorrelation-lite and DFT/FFT-style analysis are deferred.

## Sparse matrix model

Rows are `(tenant_id, device_key, window_start_ts)`. Columns are `FeatureId`
values from the tenant feature dictionary. Stored values are counts or weighted
counts in the window. Rows are stored sparsely as `FeatureId -> count` maps, not
as dense vectors.

## Glossary

- `tenant_id`: directory name under `tenant_root` that groups all devices and
  baselines for a tenant
- `device_dir`: directory name under a tenant that groups logs for one device
- `device_key`: canonical stable identifier for a device within a tenant
- `window`: fixed-size UTC-aligned time slice used for sparse rows
- `FeatureId`: stable per-tenant integer assigned to a feature string
- `AlertV1`: stable alert object schema
- `provenance`: authoritative `AlertV1` source-span list for drill/extract
- `silence_open/*`: hard-silence open-state family
- `drop_open/*`: sharp-drop open-state family
- `source_stream_id`: source-stream subject identifier, not a `FeatureId`

Security/performance hardening note: active contracts now require fail-closed drill/extract path resolution, validated output path components, symlink-resistant spool inventory, and bounded ingest resource caps.


## License and author

The sparx codebase and documentation are open source under the MIT License.

Author: Richard S. Westmoreland  
Contact: dev@rswestmore.land  
Copyright (c) 2026 Richard S. Westmoreland.

See `../LICENSE` for the full license text.
