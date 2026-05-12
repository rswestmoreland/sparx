// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Feature dictionary persistence and deterministic feature id assignment.
// See: contracts/05_feature_id_strategy_v0_1.md and contracts/24_feature_emission_catalog_v0_1.md

use std::collections::BTreeMap;

use crate::config::FeaturesSectionV1;
use crate::db::keys::{
    key_tenant_feature_dict_entries_v1, key_tenant_feature_dict_id_v1,
    key_tenant_feature_dict_next_id_v1, key_tenant_feature_dict_str_v1, KeyBytes,
};
use crate::db::tenant_values::{
    encode_feat_dict_id_to_str_v1, encode_feat_dict_meta_entries_v1,
    encode_feat_dict_meta_next_id_v1, encode_feat_dict_str_to_id_v1,
};
use crate::types::FeatureId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeatureDictionaryConfigV1 {
    pub dict_enabled: bool,
    pub dict_max_entries: u32,
}

impl From<&FeaturesSectionV1> for FeatureDictionaryConfigV1 {
    fn from(value: &FeaturesSectionV1) -> Self {
        Self {
            dict_enabled: value.dict_enabled,
            dict_max_entries: value.dict_max_entries,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeatureDictionaryMetaV1 {
    pub next_id: u32,
    pub entries: u32,
    pub last_gc_ts: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeatureDictionaryKvV1 {
    pub key: KeyBytes,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeatureDictionaryResolveV1 {
    pub feature_id: FeatureId,
    pub inserted: bool,
    pub writes: Vec<FeatureDictionaryKvV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FeatureDictionaryErrorV1 {
    DictionaryDisabled,
    DictionaryFull {
        max_entries: u32,
    },
    NextIdExhausted,
    DuplicateFeatureString {
        feature_string: String,
    },
    DuplicateFeatureId {
        feature_id: FeatureId,
    },
    MetaEntriesMismatch {
        meta_entries: u32,
        actual_entries: u32,
    },
    ReverseEntriesMismatch {
        forward_entries: u32,
        reverse_entries: u32,
    },
    MissingReverseMap {
        feature_id: FeatureId,
        feature_string: String,
    },
    ReverseMapValueMismatch {
        feature_id: FeatureId,
        forward_feature_string: String,
        reverse_feature_string: String,
    },
}

#[derive(Clone, Debug)]
pub struct FeatureDictionaryV1 {
    cfg: FeatureDictionaryConfigV1,
    meta: FeatureDictionaryMetaV1,
    str_to_id: BTreeMap<String, FeatureId>,
    id_to_str: BTreeMap<FeatureId, String>,
}

impl FeatureDictionaryV1 {
    pub fn new_empty_v1(
        cfg: FeatureDictionaryConfigV1,
        next_id_seed: u32,
        last_gc_ts: i64,
    ) -> Self {
        Self {
            cfg,
            meta: FeatureDictionaryMetaV1 {
                next_id: next_id_seed,
                entries: 0,
                last_gc_ts,
            },
            str_to_id: BTreeMap::new(),
            id_to_str: BTreeMap::new(),
        }
    }

    pub fn load_persisted_v1(
        cfg: FeatureDictionaryConfigV1,
        meta: FeatureDictionaryMetaV1,
        forward_entries: Vec<(String, FeatureId)>,
        reverse_entries: Vec<(FeatureId, String)>,
    ) -> Result<Self, FeatureDictionaryErrorV1> {
        let mut str_to_id = BTreeMap::new();
        let mut id_to_str = BTreeMap::new();

        for (feature_string, feature_id) in forward_entries {
            if str_to_id
                .insert(feature_string.clone(), feature_id)
                .is_some()
            {
                return Err(FeatureDictionaryErrorV1::DuplicateFeatureString { feature_string });
            }
            if id_to_str.insert(feature_id, feature_string).is_some() {
                return Err(FeatureDictionaryErrorV1::DuplicateFeatureId { feature_id });
            }
        }

        let mut reverse_seen = BTreeMap::new();
        for (feature_id, feature_string) in reverse_entries {
            if reverse_seen.insert(feature_id, feature_string).is_some() {
                return Err(FeatureDictionaryErrorV1::DuplicateFeatureId { feature_id });
            }
        }

        let actual_entries = str_to_id.len() as u32;
        if meta.entries != actual_entries {
            return Err(FeatureDictionaryErrorV1::MetaEntriesMismatch {
                meta_entries: meta.entries,
                actual_entries,
            });
        }

        if reverse_seen.len() != str_to_id.len() {
            return Err(FeatureDictionaryErrorV1::ReverseEntriesMismatch {
                forward_entries: str_to_id.len() as u32,
                reverse_entries: reverse_seen.len() as u32,
            });
        }

        for (feature_string, feature_id) in str_to_id.iter() {
            match reverse_seen.get(feature_id) {
                Some(reverse_feature_string) if reverse_feature_string == feature_string => {}
                Some(reverse_feature_string) => {
                    return Err(FeatureDictionaryErrorV1::ReverseMapValueMismatch {
                        feature_id: *feature_id,
                        forward_feature_string: feature_string.clone(),
                        reverse_feature_string: reverse_feature_string.clone(),
                    });
                }
                None => {
                    return Err(FeatureDictionaryErrorV1::MissingReverseMap {
                        feature_id: *feature_id,
                        feature_string: feature_string.clone(),
                    });
                }
            }
        }

        Ok(Self {
            cfg,
            meta,
            str_to_id,
            id_to_str: reverse_seen,
        })
    }

    pub fn config_v1(&self) -> &FeatureDictionaryConfigV1 {
        &self.cfg
    }

    pub fn meta_v1(&self) -> &FeatureDictionaryMetaV1 {
        &self.meta
    }

    pub fn lookup_feature_id_v1(&self, feature_string: &str) -> Option<FeatureId> {
        self.str_to_id.get(feature_string).copied()
    }

    pub fn lookup_feature_string_v1(&self, feature_id: FeatureId) -> Option<&str> {
        self.id_to_str.get(&feature_id).map(|s| s.as_str())
    }

    pub fn forward_entries_v1(&self) -> Vec<(String, FeatureId)> {
        self.str_to_id
            .iter()
            .map(|(feature_string, feature_id)| (feature_string.clone(), *feature_id))
            .collect()
    }

    pub fn reverse_entries_v1(&self) -> Vec<(FeatureId, String)> {
        self.id_to_str
            .iter()
            .map(|(feature_id, feature_string)| (*feature_id, feature_string.clone()))
            .collect()
    }

    pub fn resolve_or_insert_batch_v1(
        &mut self,
        feature_strings: &[&str],
    ) -> Result<Vec<FeatureDictionaryResolveV1>, FeatureDictionaryErrorV1> {
        let mut planned_ids: BTreeMap<String, FeatureId> = BTreeMap::new();
        let mut insert_order: Vec<(String, FeatureId)> = Vec::new();
        let mut resolves = Vec::with_capacity(feature_strings.len());
        let mut next_id = self.meta.next_id;
        let mut entries = self.meta.entries;

        for feature_string in feature_strings {
            if let Some(feature_id) = self.lookup_feature_id_v1(feature_string) {
                resolves.push(FeatureDictionaryResolveV1 {
                    feature_id,
                    inserted: false,
                    writes: Vec::new(),
                });
                continue;
            }

            if let Some(feature_id) = planned_ids.get(*feature_string).copied() {
                resolves.push(FeatureDictionaryResolveV1 {
                    feature_id,
                    inserted: false,
                    writes: Vec::new(),
                });
                continue;
            }

            if !self.cfg.dict_enabled {
                return Err(FeatureDictionaryErrorV1::DictionaryDisabled);
            }

            if entries >= self.cfg.dict_max_entries {
                return Err(FeatureDictionaryErrorV1::DictionaryFull {
                    max_entries: self.cfg.dict_max_entries,
                });
            }

            let feature_id = next_id;
            next_id = feature_id
                .checked_add(1)
                .ok_or(FeatureDictionaryErrorV1::NextIdExhausted)?;
            entries += 1;

            let owned = (*feature_string).to_string();
            planned_ids.insert(owned.clone(), feature_id);
            insert_order.push((owned, feature_id));

            resolves.push(FeatureDictionaryResolveV1 {
                feature_id,
                inserted: true,
                writes: vec![
                    FeatureDictionaryKvV1 {
                        key: key_tenant_feature_dict_str_v1(*feature_string),
                        value: encode_feat_dict_str_to_id_v1(feature_id),
                    },
                    FeatureDictionaryKvV1 {
                        key: key_tenant_feature_dict_id_v1(feature_id),
                        value: encode_feat_dict_id_to_str_v1(*feature_string),
                    },
                    FeatureDictionaryKvV1 {
                        key: key_tenant_feature_dict_next_id_v1(),
                        value: encode_feat_dict_meta_next_id_v1(next_id),
                    },
                    FeatureDictionaryKvV1 {
                        key: key_tenant_feature_dict_entries_v1(),
                        value: encode_feat_dict_meta_entries_v1(entries),
                    },
                ],
            });
        }

        for (feature_string, feature_id) in insert_order {
            self.str_to_id.insert(feature_string.clone(), feature_id);
            self.id_to_str.insert(feature_id, feature_string);
        }
        self.meta.next_id = next_id;
        self.meta.entries = entries;

        Ok(resolves)
    }

    pub fn resolve_or_insert_v1(
        &mut self,
        feature_string: &str,
    ) -> Result<FeatureDictionaryResolveV1, FeatureDictionaryErrorV1> {
        if let Some(feature_id) = self.lookup_feature_id_v1(feature_string) {
            return Ok(FeatureDictionaryResolveV1 {
                feature_id,
                inserted: false,
                writes: Vec::new(),
            });
        }

        if !self.cfg.dict_enabled {
            return Err(FeatureDictionaryErrorV1::DictionaryDisabled);
        }

        if self.meta.entries >= self.cfg.dict_max_entries {
            return Err(FeatureDictionaryErrorV1::DictionaryFull {
                max_entries: self.cfg.dict_max_entries,
            });
        }

        let feature_id = self.meta.next_id;
        let next_id = feature_id
            .checked_add(1)
            .ok_or(FeatureDictionaryErrorV1::NextIdExhausted)?;

        self.meta.next_id = next_id;
        self.meta.entries += 1;
        self.str_to_id
            .insert(feature_string.to_string(), feature_id);
        self.id_to_str
            .insert(feature_id, feature_string.to_string());

        Ok(FeatureDictionaryResolveV1 {
            feature_id,
            inserted: true,
            writes: vec![
                FeatureDictionaryKvV1 {
                    key: key_tenant_feature_dict_str_v1(feature_string),
                    value: encode_feat_dict_str_to_id_v1(feature_id),
                },
                FeatureDictionaryKvV1 {
                    key: key_tenant_feature_dict_id_v1(feature_id),
                    value: encode_feat_dict_id_to_str_v1(feature_string),
                },
                FeatureDictionaryKvV1 {
                    key: key_tenant_feature_dict_next_id_v1(),
                    value: encode_feat_dict_meta_next_id_v1(self.meta.next_id),
                },
                FeatureDictionaryKvV1 {
                    key: key_tenant_feature_dict_entries_v1(),
                    value: encode_feat_dict_meta_entries_v1(self.meta.entries),
                },
            ],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_cfg() -> FeatureDictionaryConfigV1 {
        FeatureDictionaryConfigV1 {
            dict_enabled: true,
            dict_max_entries: 4,
        }
    }

    #[test]
    fn empty_dictionary_starts_with_requested_seed() {
        let dict = FeatureDictionaryV1::new_empty_v1(base_cfg(), 100, 1234);
        assert_eq!(dict.meta_v1().next_id, 100);
        assert_eq!(dict.meta_v1().entries, 0);
        assert_eq!(dict.meta_v1().last_gc_ts, 1234);
    }

    #[test]
    fn resolve_existing_feature_is_stable_and_side_effect_free() {
        let mut dict = FeatureDictionaryV1::new_empty_v1(base_cfg(), 1, 0);
        let first = dict.resolve_or_insert_v1("k=src_ip").unwrap();
        let second = dict.resolve_or_insert_v1("k=src_ip").unwrap();
        assert_eq!(first.feature_id, 1);
        assert!(first.inserted);
        assert!(!second.inserted);
        assert_eq!(second.feature_id, 1);
        assert!(second.writes.is_empty());
        assert_eq!(dict.meta_v1().entries, 1);
        assert_eq!(dict.meta_v1().next_id, 2);
    }

    #[test]
    fn batch_resolve_inserts_new_features_in_input_order() {
        let mut dict = FeatureDictionaryV1::new_empty_v1(base_cfg(), 10, 0);
        let resolved = dict
            .resolve_or_insert_batch_v1(&["k=src_ip", "k=dst_ip", "k=src_ip"])
            .unwrap();
        assert_eq!(resolved.len(), 3);
        assert_eq!(resolved[0].feature_id, 10);
        assert!(resolved[0].inserted);
        assert_eq!(resolved[1].feature_id, 11);
        assert!(resolved[1].inserted);
        assert_eq!(resolved[2].feature_id, 10);
        assert!(!resolved[2].inserted);
        assert!(resolved[2].writes.is_empty());
        assert_eq!(dict.meta_v1().entries, 2);
        assert_eq!(dict.meta_v1().next_id, 12);
    }

    #[test]
    fn batch_resolve_is_atomic_on_capacity_error() {
        let mut dict = FeatureDictionaryV1::new_empty_v1(base_cfg(), 1, 0);
        let err = dict
            .resolve_or_insert_batch_v1(&["k=one", "k=two", "k=three", "k=four", "k=five"])
            .unwrap_err();
        assert_eq!(err, FeatureDictionaryErrorV1::DictionaryFull { max_entries: 4 });
        assert_eq!(dict.meta_v1().entries, 0);
        assert_eq!(dict.meta_v1().next_id, 1);
        assert!(dict.lookup_feature_id_v1("k=one").is_none());
    }
}
