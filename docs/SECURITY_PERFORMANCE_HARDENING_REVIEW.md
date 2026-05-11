# Security, Performance, and Test Coverage Hardening Review

This review records the active hardening pass performed before final validation packaging.
It focuses on unsafe data handling, filesystem boundaries, test coverage gaps, and bounded
runtime resource use.

## Scope reviewed

Reviewed areas:

- alert drill and extract provenance resolution
- output sink path construction
- spool inventory and replay helpers
- ingest file reading and line buffering
- config validation bounds for ingest resource controls
- tests and fixtures that exercise unsafe paths, spool inventory, and runtime-generated alert paths

No release toolchain results are claimed in this document. Final build, test, fmt, and clippy
validation must still be provided from the user-run Rust toolchain environment.

## Security hardening

### Alert provenance path resolution

Alert drill and extract now resolve `AlertV1.provenance` through a fail-closed helper that:

- rejects empty, absolute, traversal, control-character, and backslash-containing provenance paths
- validates tenant identifiers used as path components
- supports runtime-generated device paths that already include the tenant prefix
- supports source-stream display paths by resolving the physical device directory through `device_key`
- canonicalizes the tenant root and final provenance path
- rejects any resolved path that escapes the configured tenant root

This preserves the existing rule that `AlertV1.provenance` is authoritative while preventing
tampered alert records from reading arbitrary files through drill or extract.

### Output and spool path construction

Output and spool helpers now validate filesystem path components before building JSONL or spool paths.
Unsafe tenant, device, or alert identifiers containing separators, traversal components, or control
characters fail closed.

### CLI tenant identifiers

Tenant ids supplied to tenant-scoped commands are validated before they are used to derive tenant
policy paths, tenant DB paths, alert paths, or spool paths. Unsafe tenant ids fail closed with an
invalid-input route result instead of reaching filesystem path construction.

Spool writes use exclusive create semantics so an existing spool file is not silently overwritten.

### Spool replay inventory

Spool inventory now uses directory-entry file type checks and skips symlinked tenants and symlinked
spool files. This prevents replay and backlog code from following attacker-controlled links under the
spool tree.

## Performance and resource-use hardening

### Chunked plain-text reading

Plain-text runtime file processing now reads using the configured chunk size instead of one byte at a
time. Gzip processing remains conservative to preserve compressed-offset behavior.

### Bounded line buffering

Runtime line buffering now honors the configured maximum line length as a memory cap. A line that
exceeds the cap is processed up to the cap and the remainder of that physical line is discarded until
newline, keeping memory use bounded.

### Config resource bounds

Config validation now rejects unsafe ingest resource caps:

- `ingest.read_chunk_bytes` must be 1 through 16 MiB
- `ingest.max_line_len` must be 1 through 1 MiB
- `ingest.max_tokens_per_line` must be 1 through 4096
- `ingest.max_kv_per_line` must be 1 through 1024
- `ingest.max_words_from_quoted_value` must be 1 through 1024

These checks prevent accidental or malicious configuration from driving excessive CPU or memory use.

## Test coverage added

Added or extended tests for:

- runtime-generated alert `device_path` values that include the tenant prefix
- source-stream alert drill resolution through `device_key`
- provenance traversal rejection
- unsafe JSONL/spool filesystem component rejection
- unsafe CLI tenant component rejection
- symlinked spool-file inventory exclusion
- ingest resource cap validation

## Review result

The hardening pass did not intentionally change AlertV1 schema, DeviceStatsV1 layout, replay ordering,
source-stream metric labels, or recovery semantics. The changes are defensive and bounded around path
resolution, file inventory, and runtime resource caps.

Final validation still requires user-run Rust toolchain results.
