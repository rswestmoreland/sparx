// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// DF ring update planning helpers.
// See: contracts/22_baseline_sketch_encoding_v0_1.md
//   and contracts/21_scoring_math_thresholding_v0_1.md
// Updates per-slot DF counts from finalized windows using deterministic
// mutations. Centroid, stats, and scoring helpers live in sibling modules.

use std::collections::{BTreeMap, BTreeSet};

use crate::config::BaselineSectionV1;
use crate::db::baseline_sketch::{
    encode_dfm_v1, encode_dfn_v1, BaselineSketchErrorV1, DfCountPairV1,
};
use crate::db::keys::{
    key_prefix_tenant_dfm_slot_v1, key_prefix_tenant_dfn_slot_v1,
    key_tenant_df_ring_current_day_epoch_v1, key_tenant_df_ring_day_slot_epoch_v1,
    key_tenant_dfm_v1, key_tenant_dfn_v1, KeyBytes,
};
use crate::db::tenant_values::{
    encode_meta_df_ring_current_day_epoch_v1, encode_meta_df_ring_day_slot_epoch_v1,
};
use crate::types::{BaselineBucket, UnixSec};
use crate::window::FinalizedWindowRowV1;

pub const DF_RING_SLOTS_DEFAULT_V1: u32 = 7;
pub const DF_BUCKET_COUNT_DEFAULT_V1: u32 = 48;
pub const DF_MAP_CAP_DEFAULT_V1: usize = 200_000;
const SECONDS_PER_DAY_V1: i64 = 86_400;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DfRingConfigV1 {
    pub df_ring_slots: u32,
    pub df_bucket_count: u32,
    pub df_map_cap: usize,
}

impl Default for DfRingConfigV1 {
    fn default() -> Self {
        Self {
            df_ring_slots: DF_RING_SLOTS_DEFAULT_V1,
            df_bucket_count: DF_BUCKET_COUNT_DEFAULT_V1,
            df_map_cap: DF_MAP_CAP_DEFAULT_V1,
        }
    }
}

impl From<&BaselineSectionV1> for DfRingConfigV1 {
    fn from(value: &BaselineSectionV1) -> Self {
        Self {
            df_ring_slots: value.df_ring_slots,
            df_bucket_count: value.df_bucket_count,
            df_map_cap: DF_MAP_CAP_DEFAULT_V1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DfRingMetaStateV1 {
    pub current_day_epoch: Option<i64>,
    pub day_slot_epochs: Vec<Option<i64>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DfRingSlotBucketStateV1 {
    pub window_count: u32,
    pub df_pairs: Vec<DfCountPairV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DfRingKvV1 {
    pub key: KeyBytes,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DfRingMutationV1 {
    Put(DfRingKvV1),
    Delete(KeyBytes),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DfRingUpdatePlanV1 {
    pub day_epoch: i64,
    pub slot: u8,
    pub bucket: BaselineBucket,
    pub cleared_stale_slot: bool,
    pub next_window_count: u32,
    pub next_df_pairs: Vec<DfCountPairV1>,
    pub mutations: Vec<DfRingMutationV1>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DfRingErrorV1 {
    InvalidDfRingSlots {
        df_ring_slots: u32,
    },
    InvalidDfBucketCount {
        df_bucket_count: u32,
    },
    DaySlotEpochLenMismatch {
        expected: usize,
        actual: usize,
    },
    InvalidBucket {
        bucket: BaselineBucket,
        df_bucket_count: u32,
    },
    StaleSlotKeyOutsidePrefixes {
        key: String,
        slot: u8,
    },
    BaselineSketch(BaselineSketchErrorV1),
}

impl From<BaselineSketchErrorV1> for DfRingErrorV1 {
    fn from(value: BaselineSketchErrorV1) -> Self {
        Self::BaselineSketch(value)
    }
}

pub fn day_epoch_for_ts_v1(ts: UnixSec) -> i64 {
    ts.div_euclid(SECONDS_PER_DAY_V1)
}

pub fn slot_for_day_epoch_v1(day_epoch: i64, df_ring_slots: u32) -> Result<u8, DfRingErrorV1> {
    validate_df_ring_slots_v1(df_ring_slots)?;
    let slot = day_epoch.rem_euclid(i64::from(df_ring_slots));
    Ok(u8::try_from(slot).unwrap())
}

pub fn plan_df_ring_update_v1(
    row: &FinalizedWindowRowV1,
    cfg: &DfRingConfigV1,
    meta: &DfRingMetaStateV1,
    current_slot_bucket: &DfRingSlotBucketStateV1,
    stale_slot_keys: &[KeyBytes],
) -> Result<DfRingUpdatePlanV1, DfRingErrorV1> {
    validate_df_ring_config_v1(cfg)?;

    let expected_slots = usize::try_from(cfg.df_ring_slots).unwrap();
    if meta.day_slot_epochs.len() != expected_slots {
        return Err(DfRingErrorV1::DaySlotEpochLenMismatch {
            expected: expected_slots,
            actual: meta.day_slot_epochs.len(),
        });
    }
    if u32::from(row.key.bucket) >= cfg.df_bucket_count {
        return Err(DfRingErrorV1::InvalidBucket {
            bucket: row.key.bucket,
            df_bucket_count: cfg.df_bucket_count,
        });
    }

    let day_epoch = day_epoch_for_ts_v1(row.key.window_start_ts);
    let slot = slot_for_day_epoch_v1(day_epoch, cfg.df_ring_slots)?;
    let slot_index = usize::from(slot);
    let stale_slot = meta.day_slot_epochs[slot_index] != Some(day_epoch);

    let mut mutations = Vec::new();
    if stale_slot {
        let delete_keys = normalize_stale_slot_keys_v1(slot, stale_slot_keys)?;
        for key in delete_keys {
            mutations.push(DfRingMutationV1::Delete(key));
        }
        mutations.push(DfRingMutationV1::Put(DfRingKvV1 {
            key: key_tenant_df_ring_day_slot_epoch_v1(slot),
            value: encode_meta_df_ring_day_slot_epoch_v1(day_epoch),
        }));
    }

    if meta.current_day_epoch != Some(day_epoch) {
        mutations.push(DfRingMutationV1::Put(DfRingKvV1 {
            key: key_tenant_df_ring_current_day_epoch_v1(),
            value: encode_meta_df_ring_current_day_epoch_v1(day_epoch),
        }));
    }

    let next_window_count = if stale_slot {
        1
    } else {
        current_slot_bucket.window_count.saturating_add(1)
    };

    let mut df_counts = if stale_slot {
        BTreeMap::new()
    } else {
        df_pairs_to_map_v1(&current_slot_bucket.df_pairs)
    };

    let present_feature_ids: BTreeSet<u32> = row
        .sparse_counts
        .iter()
        .map(|pair| pair.feature_id)
        .collect();
    for feature_id in present_feature_ids {
        let entry = df_counts.entry(feature_id).or_insert(0);
        *entry = (*entry).saturating_add(1);
    }

    let next_df_pairs = cap_df_pairs_v1(map_to_df_pairs_v1(&df_counts), cfg.df_map_cap);
    mutations.push(DfRingMutationV1::Put(DfRingKvV1 {
        key: key_tenant_dfn_v1(slot, row.key.bucket),
        value: encode_dfn_v1(next_window_count),
    }));
    mutations.push(DfRingMutationV1::Put(DfRingKvV1 {
        key: key_tenant_dfm_v1(slot, row.key.bucket),
        value: encode_dfm_v1(&next_df_pairs)?,
    }));

    Ok(DfRingUpdatePlanV1 {
        day_epoch,
        slot,
        bucket: row.key.bucket,
        cleared_stale_slot: stale_slot,
        next_window_count,
        next_df_pairs,
        mutations,
    })
}

fn validate_df_ring_config_v1(cfg: &DfRingConfigV1) -> Result<(), DfRingErrorV1> {
    validate_df_ring_slots_v1(cfg.df_ring_slots)?;
    validate_df_bucket_count_v1(cfg.df_bucket_count)?;
    Ok(())
}

fn validate_df_ring_slots_v1(df_ring_slots: u32) -> Result<(), DfRingErrorV1> {
    if df_ring_slots == 0 || df_ring_slots > u32::from(u8::MAX) + 1 {
        return Err(DfRingErrorV1::InvalidDfRingSlots { df_ring_slots });
    }
    Ok(())
}

fn validate_df_bucket_count_v1(df_bucket_count: u32) -> Result<(), DfRingErrorV1> {
    if df_bucket_count == 0 || df_bucket_count > u32::from(u8::MAX) + 1 {
        return Err(DfRingErrorV1::InvalidDfBucketCount { df_bucket_count });
    }
    Ok(())
}

fn normalize_stale_slot_keys_v1(
    slot: u8,
    stale_slot_keys: &[KeyBytes],
) -> Result<Vec<KeyBytes>, DfRingErrorV1> {
    let dfn_prefix = key_prefix_tenant_dfn_slot_v1(slot);
    let dfm_prefix = key_prefix_tenant_dfm_slot_v1(slot);
    let dfn_prefix_bytes = dfn_prefix.as_bytes();
    let dfm_prefix_bytes = dfm_prefix.as_bytes();
    let mut keys = stale_slot_keys.to_vec();

    for key in &keys {
        let is_dfn = key.bytes.starts_with(dfn_prefix_bytes);
        let is_dfm = key.bytes.starts_with(dfm_prefix_bytes);
        if !is_dfn && !is_dfm {
            return Err(DfRingErrorV1::StaleSlotKeyOutsidePrefixes {
                key: String::from_utf8_lossy(&key.bytes).into_owned(),
                slot,
            });
        }
    }

    keys.sort_by(|a, b| {
        let a_parts = stale_slot_key_sort_parts_v1(&a.bytes, dfm_prefix_bytes, dfn_prefix_bytes);
        let b_parts = stale_slot_key_sort_parts_v1(&b.bytes, dfm_prefix_bytes, dfn_prefix_bytes);
        a_parts.cmp(&b_parts).then(a.bytes.cmp(&b.bytes))
    });
    keys.dedup_by(|a, b| a.bytes == b.bytes);
    Ok(keys)
}

fn stale_slot_key_sort_parts_v1(
    bytes: &[u8],
    dfm_prefix: &[u8],
    dfn_prefix: &[u8],
) -> (u8, Option<u32>) {
    if let Some(bucket) = stale_slot_bucket_suffix_v1(bytes, dfm_prefix) {
        return (0, Some(bucket));
    }
    if let Some(bucket) = stale_slot_bucket_suffix_v1(bytes, dfn_prefix) {
        return (1, Some(bucket));
    }
    (2, None)
}

fn stale_slot_bucket_suffix_v1(bytes: &[u8], prefix: &[u8]) -> Option<u32> {
    let suffix = bytes.strip_prefix(prefix)?;
    let suffix = suffix.strip_prefix(b"/")?;
    let text = std::str::from_utf8(suffix).ok()?;
    text.parse::<u32>().ok()
}

fn df_pairs_to_map_v1(pairs: &[DfCountPairV1]) -> BTreeMap<u32, u32> {
    let mut out = BTreeMap::new();
    for pair in pairs {
        let entry = out.entry(pair.feature_id).or_insert(0u32);
        *entry = (*entry).saturating_add(pair.df_count);
    }
    out
}

fn map_to_df_pairs_v1(map: &BTreeMap<u32, u32>) -> Vec<DfCountPairV1> {
    map.iter()
        .filter_map(|(feature_id, df_count)| {
            if *df_count == 0 {
                None
            } else {
                Some(DfCountPairV1 {
                    feature_id: *feature_id,
                    df_count: *df_count,
                })
            }
        })
        .collect()
}

fn cap_df_pairs_v1(mut pairs: Vec<DfCountPairV1>, df_map_cap: usize) -> Vec<DfCountPairV1> {
    if pairs.len() <= df_map_cap {
        return pairs;
    }

    pairs.sort_by(|a, b| {
        b.df_count
            .cmp(&a.df_count)
            .then(a.feature_id.cmp(&b.feature_id))
    });
    pairs.truncate(df_map_cap);
    pairs.sort_by_key(|pair| pair.feature_id);
    pairs
}
