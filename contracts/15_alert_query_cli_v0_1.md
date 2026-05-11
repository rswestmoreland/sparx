# Alert Query CLI Contract v0.1

Commands:
- `sparx alerts list --tenant <tenant_id> [--since <ts>] [--until <ts>] [--category <outlier|noise_suspect|info>] [--entity-kind <srcip|dstip|userid|domain|host> --entity-value <value>] [--json]`
- `sparx alerts show --tenant <tenant_id> --alert-id <id> [--json]`
- `sparx alerts export --tenant <tenant_id> [--category <outlier|noise_suspect|info>] [--entity-kind <srcip|dstip|userid|domain|host> --entity-value <value>] --out <path> [--gzip]`
- `sparx alerts search --tenant <tenant_id> [--since <ts>] [--until <ts>] [--category <outlier|noise_suspect|info>] [--entity-kind <srcip|dstip|userid|domain|host> --entity-value <value>] --contains <text>`

Storage model assumptions:
- v0.1 persists primary alert objects under `alert/v1/<alert_id>`.
- the current release and later persist secondary `alert_idx_*` keys alongside the primary alert object.
- Query/export implementations must remain correct when only primary alert objects exist so older tenants and mixed-history DBs still work.

Time semantics:
- filter by `window_start_ts` UTC
- `since` is inclusive
- `until` is exclusive

Search scope:
- search only over fields actually persisted in `AlertV1`
- examples: summaries, reasons, top feature strings, entity values, device_path

Ordering:
- default list/search order: `window_start_ts desc`, then `alert_id asc`
- export order must be deterministic for the same filter set

Query strategy:
- the current release persists secondary indexes for deterministic restart continuity.
- the current release activates the `alert_idx_time` path for list/search/export candidate selection when the time index is complete for the tenant DB.
- If the time index is absent, incomplete, or mixed-history relative to primary alert objects, list/search/export fall back to primary-alert scans so correctness does not depend on index presence.
- the current release activates structured category/entity filters on the CLI surface.
- Category-filtered list/search/export may use `alert_idx_cat` candidate selection when the category index fully covers the tenant primary alert set; otherwise they fall back to primary-alert scans.
- Entity-filtered list/search/export may use `alert_idx_ent` candidate selection when the specific entity filter matches the primary alert set exactly; otherwise they fall back to primary-alert scans.
- When both category and entity filters are present, correctness still comes from the primary `AlertV1` object and any indexed candidate set is post-filtered before output.
