# Alerting and Explanations

sparx writes explainable `AlertV1` records for sparse-row anomalies and
volume-loss conditions.

## AlertV1 rules

- `AlertV1` schema remains stable for v1.
- `AlertV1.provenance: Vec<FileSpanV1>` is authoritative for drill/extract.
- Legacy `source_files` behavior must not be reintroduced.
- Alert IDs are deterministic and include subject-specific inputs that prevent
  hard-silence and sharp-drop collisions.
- Secondary `alert_idx_*` persistence is current truth and supports query/export
  workflows.

## Sparse-row alert reasons

The scoring model can represent rarity, drift, spike, and extreme volume
conditions for finalized sparse rows.

## V_DROP alert reasons

`V_DROP` represents volume-loss behavior:

- hard silence: mature expected source activity is fully missing
- sharp drop: activity is present but much lower than expected
- source stream: per-source-path subject behavior behind the default-off gate

Sharp-drop alerts use deterministic detail `drop_kind=sharp_drop`. Source-stream
alerts include deterministic source-stream subject details without adding new
AlertV1 fields.

## Drill and extract

Alert drill/extract uses retained provenance spans. This allows analysts to
review source log evidence while the sparse matrix remains compact.
