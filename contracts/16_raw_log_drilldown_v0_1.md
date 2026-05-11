# Raw Log Drilldown CLI Contract v0.1

Commands:
- `sparx alert drill <alert_id>`: print raw lines around stored provenance spans
- `sparx alert extract <alert_id>`: scan only relevant file ranges from `AlertV1.provenance`
- both commands accept `--max-bytes` and `--max-lines` caps in v0.1

Authoritative field model:
- drilldown/extract uses `AlertV1.provenance: Vec<FileSpanV1>` only
- older `source_files` wording is obsolete and must not be used for new implementation work

Caps:
- `--max-bytes`, `--max-lines` enforced

Gzip:
- `drill` no random seek in `.gz` (skip with message)
- `extract` may stream-decompress with limits

Paths:
- `FileSpanV1.file_rel` is the authoritative provenance-relative path field
- drill/extract reject empty, absolute, traversal, control-character, and backslash-containing provenance paths
- tenant identifiers used for path resolution must be safe single path components
- device alert paths may be device-relative or tenant-relative; both forms must resolve under the configured tenant root
- source-stream display paths resolve the physical device directory by `tenant_id` and `device_key`
- the final path is canonicalized and must remain under the configured tenant root
- `device_path` is the authoritative alert display field name, but it is not trusted blindly as a filesystem path
