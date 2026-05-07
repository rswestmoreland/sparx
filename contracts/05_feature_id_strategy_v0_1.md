# Feature-ID Strategy Contract v0.1 (Dictionary-only stable IDs)

## Scope
This contract defines how `sparx` assigns and persists `FeatureId` values in v0.1.

The v0.1 implementation is dictionary-only. It does not implement a hashed fallback namespace, hashed Tier 2 IDs, or hashed promotion flow.

## Stable ID model
- Every emitted feature string resolves through the tenant-scoped feature dictionary.
- Each unique feature string receives one stable `FeatureId` (`u32`) within that tenant DB.
- Assigned IDs are monotonic and never reused within the same tenant DB.
- Reverse lookup must remain available for every persisted dictionary entry.

## Dictionary behavior
- Dictionary storage is tenant-scoped.
- Dictionary growth is bounded by `dict_max_entries`.
- Dictionary entries are not evicted in v0.1.
- If the dictionary is disabled, new feature insertion fails closed.
- If the dictionary reaches its cap, new feature insertion fails closed.
- `last_gc_ts` remains part of dictionary metadata for layout stability, but dictionary GC/eviction is not implemented in v0.1.

## Explainability rule
- All persisted `FeatureId` values must remain explainable through the reverse map.
- Alert explanation and drill/extract workflows rely on dictionary-backed reverse lookup.
- No hashed-only feature IDs may be emitted or persisted in v0.1.

## Non-goals for v0.1
- Hashed fallback IDs.
- Hybrid Tier 1/Tier 2 feature ID assignment.
- Dictionary promotion/demotion from a hashed long-tail pool.
- Collision-managed hashed feature spaces.

## Storage
- `feat_dict/v1/str/<feature_string>` -> `FeatureId` (`u32`)
- `feat_dict/v1/id/<feature_id_u32>` -> `feature_string`
- `feat_dict/v1/meta/next_id` -> next assignable `FeatureId`
- `feat_dict/v1/meta/entries` -> current dictionary entry count
- `feat_dict/v1/meta/last_gc_ts` -> reserved metadata field retained for layout stability

## Canonical storage prefixes (v0.1)
The tenant feature dictionary is stored under:
- `feat_dict/v1/str/<feature_string>` -> FeatureId (u32)
- `feat_dict/v1/id/<feature_id_u32>` -> feature_string

These prefixes are authoritative as defined by Tenant DB Key Prefix Map Contract v0.1 and Tenant DB Simple Value Encodings Contract v0.1.
