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
- store tenant-relative paths in `FileSpanV1.file_rel`
- resolve via `<watch-root>/<tenant_id>/<device_path>/<file_rel>`
- `device_path` is the authoritative alert field name
