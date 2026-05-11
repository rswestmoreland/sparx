# sparx Documentation

This directory contains active user-facing documentation for sparx. Historical
checkpoint notes are archived under `roadmap/` with their original filenames for
traceability.

## Active guides

- `ARCHITECTURE.md`: high-level system model and data flow
- `INGEST_AND_TOKENIZATION.md`: supported log inputs and normalization behavior
- `SPARSE_MATRIX_MODEL.md`: how sparse rows, features, and baselines work
- `STORAGE_AND_RETENTION.md`: Fjall storage, key families, retention, and replay
- `CONFIGURATION_AND_POLICY.md`: configuration, tenant policy, and fail-closed
  behavior
- `ALERTING_AND_EXPLANATIONS.md`: AlertV1, scoring, drilldown, and explanations
- `VDROP_VOLUME_LOSS_DETECTION.md`: hard-silence, sharp-drop, and source-stream
  volume-loss detection
- `OPERATIONS.md`: run, oneshot, status, policy, purge, migrate, alert, and
  replay workflows
- `METRICS_HEALTH_STATUS.md`: status, JSON status, Prometheus metrics, and
  health output
- `VALIDATION_AND_RELEASE_READINESS.md`: external validation and release gates
- `BENCHMARKING.md`: tenant/device EPS benchmark and workload controls
- `OPEN_SOURCE_RELEASE_METADATA.md`: MIT license, author, copyright, and SPDX metadata
- `SECURITY_PERFORMANCE_HARDENING_REVIEW.md`: filesystem, unsafe-data, and resource-use hardening review
- `CODEBASE_CONSISTENCY_AND_BAD_DATA_REVIEW.md`: maintainability comments and malformed-data stability review
- `RUST190_VALIDATION_FIX_REVIEW.md`: validation-failure fix review for Rust 1.90 and EPS benchmark rerun
- `DEFERRED_SCOPE.md`: explicitly deferred capabilities and why they are out of
  v1 scope
- `DOCUMENTATION_RECONCILIATION_REVIEW.md`: review note for the user-facing
  documentation reorganization
- `CURRENT_PLAN_CHECKLIST.md`: current completion checklist and remaining work
- `FJALL_STORAGE_RUNTIME_DESIGN_NOTE.md`: storage/runtime design note

Contracts remain under `../contracts/`.


## License and author

sparx is open source under the MIT License.

Author: Richard S. Westmoreland  
Contact: dev@rswestmore.land  
Copyright (c) 2026 Richard S. Westmoreland.

See `../LICENSE` for the full license text.
