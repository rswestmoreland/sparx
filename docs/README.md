# sparx Documentation

This directory contains active user-facing documentation for sparx. Task notes,
phase plans, validation fix reviews, and handoff prompts are archived under
`roadmap/` with phase-oriented filenames for traceability.

## Active guides

- `ARCHITECTURE.md`: high-level system model and data flow
- `INGEST_AND_TOKENIZATION.md`: supported log inputs and normalization behavior
- `SPARSE_MATRIX_MODEL.md`: how sparse rows, features, and baselines work
- `SPARSE_MATRIX_AND_SIGNAL_PROCESSING.md`: sparse rows as sampled signal frames, EWMA, and periodic volume baselines
- `SIGNAL_PROCESSING_MVP_PLAN.md`: lean public signal-processing MVP boundary
- `INGEST_PERFORMANCE_TUNING_PLAN.md`: public ingest and detection throughput tuning direction
- `STORAGE_AND_RETENTION.md`: Fjall storage, key families, retention, and replay
- `CONFIGURATION_AND_POLICY.md`: configuration, tenant policy, and fail-closed behavior
- `ALERTING_AND_EXPLANATIONS.md`: AlertV1, scoring, drilldown, and explanations
- `VDROP_VOLUME_LOSS_DETECTION.md`: hard-silence, sharp-drop, and source-stream volume-loss detection
- `OPERATIONS.md`: run, oneshot, status, policy, purge, migrate, alert, and replay workflows
- `METRICS_HEALTH_STATUS.md`: status, JSON status, Prometheus metrics, and health output
- `VALIDATION_AND_RELEASE_READINESS.md`: external validation and release gates
- `BENCHMARKING.md`: tenant/device ingestion and detection EPS benchmark and workload controls
- `OPEN_SOURCE_RELEASE_METADATA.md`: MIT license, author, copyright, and SPDX metadata
- `DEFERRED_SCOPE.md`: explicitly deferred capabilities and why they are out of v1 scope
- `CURRENT_PLAN_CHECKLIST.md`: current completion checklist and remaining work
- `FJALL_STORAGE_RUNTIME_DESIGN_NOTE.md`: storage/runtime design note

Contracts remain under `../contracts/`.

## Roadmap and task archive

Use `roadmap/` for phase notes, fix reviews, validation handoffs, and other
work-management documents. Files in `roadmap/` are retained for traceability and
are not the active public guide set.

## License and author

sparx is open source under the MIT License.

Author: Richard S. Westmoreland  
Contact: dev@rswestmore.land  
Copyright (c) 2026 Richard S. Westmoreland.

See `../LICENSE` for the full license text.
