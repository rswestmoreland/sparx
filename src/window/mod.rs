// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Window aggregation and checkpoint helpers.
// See: contracts/24_feature_emission_catalog_v0_1.md
//   and contracts/26_open_window_checkpoint_encoding_v0_1.md
// Aggregates one active sparse window per device, emits deterministic checkpoint
// mutations, and builds deterministic finalize plans. Baseline and scoring
// modules consume the finalized rows produced here.

use std::collections::BTreeMap;

use chrono::{Datelike, TimeZone, Timelike, Utc};

use crate::config::CapsSectionV1;
use crate::db::keys::{
    key_tenant_active_window_v1, key_tenant_window_row_feat_v1, key_tenant_window_row_meta_v1,
    KeyBytes,
};
use crate::db::open_window::{
    encode_win_active_v1, encode_win_row_feat_v1, encode_win_row_meta_v1, OpenWindowErrorV1,
    SparseCountPairV1, TopKStringEntryV1, WinActiveV1, WinMetaV1,
};
use crate::features::{
    EntitySketchCapsV1, EntitySketchSnapshotV1, EntitySketchesV1, FeatureDictionaryErrorV1,
    FeatureDictionaryKvV1, FeatureDictionaryV1, FeatureEmissionLineV1,
};
use crate::types::{BaselineBucket, DeviceKey, FeatureFamilyV1, FeatureId, UnixSec};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowKeyV1 {
    pub device_key: DeviceKey,
    pub window_start_ts: UnixSec,
    pub window_end_ts: UnixSec,
    pub bucket: BaselineBucket,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowMetaV1 {
    pub lines: u32,
    pub bytes: u64,
    pub dropped_features: u32,
    pub dropped_words: u32,
    pub dropped_shapes: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowCapsV1 {
    pub max_features_per_window: u32,
    pub max_word_features_per_window: u32,
    pub max_shape_features_per_window: u32,
    pub max_syslog_features_per_window: u32,
    pub entity_sketch_caps: EntitySketchCapsV1,
}

impl From<&CapsSectionV1> for WindowCapsV1 {
    fn from(value: &CapsSectionV1) -> Self {
        Self {
            max_features_per_window: value.max_features_per_window,
            max_word_features_per_window: value.max_word_features_per_window,
            max_shape_features_per_window: value.max_shape_features_per_window,
            max_syslog_features_per_window: value.max_syslog_features_per_window,
            entity_sketch_caps: EntitySketchCapsV1::from(value),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowCheckpointKvV1 {
    pub key: KeyBytes,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizedWindowRowV1 {
    pub key: WindowKeyV1,
    pub window_id: u64,
    pub meta: WinMetaV1,
    pub sparse_counts: Vec<SparseCountPairV1>,
    pub entity_snapshot: EntitySketchSnapshotV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowFinalizeMutationV1 {
    Put(WindowCheckpointKvV1),
    Delete(KeyBytes),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowFinalizePlanV1 {
    pub finalized_row: FinalizedWindowRowV1,
    pub mutations: Vec<WindowFinalizeMutationV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowIngestResultV1 {
    pub dict_writes: Vec<FeatureDictionaryKvV1>,
    pub dropped_features: u32,
    pub dropped_words: u32,
    pub dropped_shapes: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowApplyLineResultV1 {
    Applied(WindowIngestResultV1),
    DifferentWindow { line_window_start_ts: UnixSec },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowErrorV1 {
    InvalidWindowSize {
        window_size_s: u32,
    },
    InvalidWindowBounds {
        window_start_ts: UnixSec,
        window_end_ts: UnixSec,
    },
    MisalignedWindowStart {
        window_start_ts: UnixSec,
        aligned_window_start_ts: UnixSec,
        window_size_s: u32,
    },
    InvalidTimestamp {
        ts: UnixSec,
    },
    ActiveMetaStartMismatch {
        active_window_start_ts: UnixSec,
        meta_window_start_ts: UnixSec,
    },
    InvalidNextWindowStart {
        current_window_start_ts: UnixSec,
        current_window_end_ts: UnixSec,
        next_window_start_ts: UnixSec,
    },
    WindowIdOverflow {
        current_window_id: u64,
    },
    MissingFeatureString {
        feature_id: FeatureId,
    },
    FeatureDictionary(FeatureDictionaryErrorV1),
    OpenWindow(OpenWindowErrorV1),
}

impl From<FeatureDictionaryErrorV1> for WindowErrorV1 {
    fn from(value: FeatureDictionaryErrorV1) -> Self {
        Self::FeatureDictionary(value)
    }
}

impl From<OpenWindowErrorV1> for WindowErrorV1 {
    fn from(value: OpenWindowErrorV1) -> Self {
        Self::OpenWindow(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowAccumulatorV1 {
    device_key: DeviceKey,
    window_size_s: u32,
    bucket: BaselineBucket,
    active: WinActiveV1,
    meta: WinMetaV1,
    counts: BTreeMap<FeatureId, u32>,
    caps: WindowCapsV1,
    entity_sketches: EntitySketchesV1,
    unique_features: u32,
    unique_word_features: u32,
    unique_shape_features: u32,
    unique_syslog_features: u32,
}

impl WindowAccumulatorV1 {
    pub fn new_v1(
        device_key: &str,
        window_start_ts: UnixSec,
        window_id: u64,
        window_size_s: u32,
        last_update_ts: UnixSec,
        caps: WindowCapsV1,
    ) -> Result<Self, WindowErrorV1> {
        let key = compute_window_key_v1(device_key, window_start_ts, window_size_s)?;
        Ok(Self {
            device_key: device_key.to_string(),
            window_size_s,
            bucket: key.bucket,
            active: WinActiveV1 {
                active_window_start_ts: window_start_ts,
                active_window_id: window_id,
                last_update_ts,
            },
            meta: WinMetaV1 {
                window_start_ts,
                window_end_ts: key.window_end_ts,
                lines: 0,
                bytes: 0,
                dropped_features: 0,
                dropped_words: 0,
                dropped_shapes: 0,
            },
            counts: BTreeMap::new(),
            caps: caps.clone(),
            entity_sketches: EntitySketchesV1::new_v1(caps.entity_sketch_caps.clone()),
            unique_features: 0,
            unique_word_features: 0,
            unique_shape_features: 0,
            unique_syslog_features: 0,
        })
    }

    pub fn from_checkpoint_v1(
        device_key: &str,
        caps: WindowCapsV1,
        active: WinActiveV1,
        meta: WinMetaV1,
        feat_pairs: &[SparseCountPairV1],
        entity_snapshot: &EntitySketchSnapshotV1,
        dict: &FeatureDictionaryV1,
    ) -> Result<Self, WindowErrorV1> {
        if active.active_window_start_ts != meta.window_start_ts {
            return Err(WindowErrorV1::ActiveMetaStartMismatch {
                active_window_start_ts: active.active_window_start_ts,
                meta_window_start_ts: meta.window_start_ts,
            });
        }
        let delta = meta.window_end_ts - meta.window_start_ts;
        if delta <= 0 {
            return Err(WindowErrorV1::InvalidWindowBounds {
                window_start_ts: meta.window_start_ts,
                window_end_ts: meta.window_end_ts,
            });
        }
        let window_size_s =
            u32::try_from(delta).map_err(|_| WindowErrorV1::InvalidWindowBounds {
                window_start_ts: meta.window_start_ts,
                window_end_ts: meta.window_end_ts,
            })?;
        let mut counts = BTreeMap::new();
        let mut unique_features = 0u32;
        let mut unique_word_features = 0u32;
        let mut unique_shape_features = 0u32;
        let mut unique_syslog_features = 0u32;

        for pair in feat_pairs {
            counts.insert(pair.feature_id, pair.count);
            unique_features = unique_features.saturating_add(1);
            let feature_string = dict.lookup_feature_string_v1(pair.feature_id).ok_or(
                WindowErrorV1::MissingFeatureString {
                    feature_id: pair.feature_id,
                },
            )?;
            match feature_cap_class_v1(feature_string) {
                FeatureCapClassV1::Word => {
                    unique_word_features = unique_word_features.saturating_add(1);
                }
                FeatureCapClassV1::Shape => {
                    unique_shape_features = unique_shape_features.saturating_add(1);
                }
                FeatureCapClassV1::Syslog => {
                    unique_syslog_features = unique_syslog_features.saturating_add(1);
                }
                FeatureCapClassV1::General => {}
            }
        }

        let key = compute_window_key_v1(device_key, meta.window_start_ts, window_size_s)?;

        Ok(Self {
            device_key: device_key.to_string(),
            window_size_s,
            bucket: key.bucket,
            active,
            meta,
            counts,
            caps: caps.clone(),
            entity_sketches: EntitySketchesV1::from_snapshot_v1(
                caps.entity_sketch_caps.clone(),
                entity_snapshot,
            ),
            unique_features,
            unique_word_features,
            unique_shape_features,
            unique_syslog_features,
        })
    }

    pub fn device_key_v1(&self) -> &str {
        &self.device_key
    }

    pub fn window_key_v1(&self) -> WindowKeyV1 {
        WindowKeyV1 {
            device_key: self.device_key.clone(),
            window_start_ts: self.meta.window_start_ts,
            window_end_ts: self.meta.window_end_ts,
            bucket: self.bucket,
        }
    }

    pub fn active_v1(&self) -> &WinActiveV1 {
        &self.active
    }

    pub fn meta_v1(&self) -> &WinMetaV1 {
        &self.meta
    }

    pub fn sparse_counts_v1(&self) -> Vec<SparseCountPairV1> {
        self.counts
            .iter()
            .map(|(feature_id, count)| SparseCountPairV1 {
                feature_id: *feature_id,
                count: *count,
            })
            .collect()
    }

    pub fn entity_snapshot_v1(&self) -> EntitySketchSnapshotV1 {
        self.entity_sketches.snapshot_v1()
    }

    pub fn apply_line_v1(
        &mut self,
        line_ts: UnixSec,
        update_ts: UnixSec,
        line_bytes: usize,
        line: &FeatureEmissionLineV1,
        dict: &mut FeatureDictionaryV1,
    ) -> Result<WindowApplyLineResultV1, WindowErrorV1> {
        let line_window_start_ts = align_window_start_ts_v1(line_ts, self.window_size_s)?;
        if line_window_start_ts != self.meta.window_start_ts {
            return Ok(WindowApplyLineResultV1::DifferentWindow {
                line_window_start_ts,
            });
        }

        let mut next = self.clone();
        let mut dict_work = dict.clone();

        next.meta.lines = next.meta.lines.saturating_add(1);
        let line_bytes_u64 = u64::try_from(line_bytes).unwrap_or(u64::MAX);
        next.meta.bytes = next.meta.bytes.saturating_add(line_bytes_u64);
        next.entity_sketches.ingest_line_v1(line);

        let mut result = WindowIngestResultV1 {
            dict_writes: Vec::new(),
            dropped_features: 0,
            dropped_words: 0,
            dropped_shapes: 0,
        };

        for emitted in &line.features {
            if emitted.count == 0 {
                continue;
            }

            let resolved = dict_work.resolve_or_insert_v1(&emitted.feature.s)?;
            result.dict_writes.extend(resolved.writes.into_iter());

            if let Some(existing) = next.counts.get_mut(&resolved.feature_id) {
                *existing = existing.saturating_add(emitted.count);
                continue;
            }

            if next.can_admit_new_feature_v1(&emitted.feature.s, emitted.family) {
                next.note_admitted_feature_v1(&emitted.feature.s, emitted.family);
                next.counts.insert(resolved.feature_id, emitted.count);
            } else {
                next.note_dropped_feature_v1(&emitted.feature.s, emitted.count, &mut result);
            }
        }

        next.active.last_update_ts = update_ts;
        *self = next;
        *dict = dict_work;
        Ok(WindowApplyLineResultV1::Applied(result))
    }

    pub fn checkpoint_writes_v1(&self) -> Result<Vec<WindowCheckpointKvV1>, WindowErrorV1> {
        let mut writes = Vec::with_capacity(8);
        writes.push(WindowCheckpointKvV1 {
            key: key_tenant_window_row_feat_v1(&self.device_key, self.active.active_window_id),
            value: encode_win_row_feat_v1(&self.sparse_counts_v1())?,
        });
        writes.push(WindowCheckpointKvV1 {
            key: key_tenant_window_row_meta_v1(&self.device_key, self.active.active_window_id),
            value: encode_win_row_meta_v1(&self.meta),
        });
        for write in self
            .entity_sketches
            .checkpoint_writes_v1(&self.device_key, self.active.active_window_id)?
        {
            writes.push(WindowCheckpointKvV1 {
                key: write.key,
                value: write.value,
            });
        }
        writes.push(WindowCheckpointKvV1 {
            key: key_tenant_active_window_v1(&self.device_key),
            value: encode_win_active_v1(&self.active),
        });
        Ok(writes)
    }

    pub fn finalized_row_v1(&self) -> FinalizedWindowRowV1 {
        FinalizedWindowRowV1 {
            key: self.window_key_v1(),
            window_id: self.active.active_window_id,
            meta: self.meta.clone(),
            sparse_counts: self.sparse_counts_v1(),
            entity_snapshot: self.entity_snapshot_v1(),
        }
    }

    pub fn open_window_delete_keys_v1(&self) -> Vec<KeyBytes> {
        vec![
            key_tenant_window_row_feat_v1(&self.device_key, self.active.active_window_id),
            key_tenant_window_row_meta_v1(&self.device_key, self.active.active_window_id),
            crate::db::keys::key_tenant_window_row_ent_srcip_v1(
                &self.device_key,
                self.active.active_window_id,
            ),
            crate::db::keys::key_tenant_window_row_ent_dstip_v1(
                &self.device_key,
                self.active.active_window_id,
            ),
            crate::db::keys::key_tenant_window_row_ent_userid_v1(
                &self.device_key,
                self.active.active_window_id,
            ),
            crate::db::keys::key_tenant_window_row_ent_domain_v1(
                &self.device_key,
                self.active.active_window_id,
            ),
            crate::db::keys::key_tenant_window_row_ent_host_v1(
                &self.device_key,
                self.active.active_window_id,
            ),
        ]
    }

    pub fn finalize_idle_v1(&self) -> WindowFinalizePlanV1 {
        let mut mutations = Vec::with_capacity(8);
        for key in self.open_window_delete_keys_v1() {
            mutations.push(WindowFinalizeMutationV1::Delete(key));
        }
        mutations.push(WindowFinalizeMutationV1::Delete(
            key_tenant_active_window_v1(&self.device_key),
        ));
        WindowFinalizePlanV1 {
            finalized_row: self.finalized_row_v1(),
            mutations,
        }
    }

    pub fn finalize_and_advance_v1(
        &self,
        next_window_start_ts: UnixSec,
        last_update_ts: UnixSec,
    ) -> Result<(WindowFinalizePlanV1, Self), WindowErrorV1> {
        let next = self.new_follow_on_window_v1(next_window_start_ts, last_update_ts)?;
        let mut mutations = Vec::with_capacity(8);
        for key in self.open_window_delete_keys_v1() {
            mutations.push(WindowFinalizeMutationV1::Delete(key));
        }
        mutations.push(WindowFinalizeMutationV1::Put(WindowCheckpointKvV1 {
            key: key_tenant_active_window_v1(&self.device_key),
            value: encode_win_active_v1(next.active_v1()),
        }));
        Ok((
            WindowFinalizePlanV1 {
                finalized_row: self.finalized_row_v1(),
                mutations,
            },
            next,
        ))
    }

    fn new_follow_on_window_v1(
        &self,
        next_window_start_ts: UnixSec,
        last_update_ts: UnixSec,
    ) -> Result<Self, WindowErrorV1> {
        let aligned_next_window_start_ts =
            align_window_start_ts_v1(next_window_start_ts, self.window_size_s)?;
        if aligned_next_window_start_ts != next_window_start_ts
            || next_window_start_ts < self.meta.window_end_ts
        {
            return Err(WindowErrorV1::InvalidNextWindowStart {
                current_window_start_ts: self.meta.window_start_ts,
                current_window_end_ts: self.meta.window_end_ts,
                next_window_start_ts,
            });
        }
        let next_window_id =
            self.active
                .active_window_id
                .checked_add(1)
                .ok_or(WindowErrorV1::WindowIdOverflow {
                    current_window_id: self.active.active_window_id,
                })?;
        Self::new_v1(
            &self.device_key,
            next_window_start_ts,
            next_window_id,
            self.window_size_s,
            last_update_ts,
            self.caps.clone(),
        )
    }

    fn can_admit_new_feature_v1(&self, feature: &str, _family: FeatureFamilyV1) -> bool {
        if self.unique_features >= self.caps.max_features_per_window {
            return false;
        }
        match feature_cap_class_v1(feature) {
            FeatureCapClassV1::Word => {
                self.unique_word_features < self.caps.max_word_features_per_window
            }
            FeatureCapClassV1::Shape => {
                self.unique_shape_features < self.caps.max_shape_features_per_window
            }
            FeatureCapClassV1::Syslog => {
                self.unique_syslog_features < self.caps.max_syslog_features_per_window
            }
            FeatureCapClassV1::General => true,
        }
    }

    fn note_admitted_feature_v1(&mut self, feature: &str, family: FeatureFamilyV1) {
        self.unique_features = self.unique_features.saturating_add(1);
        match (feature_cap_class_v1(feature), family) {
            (FeatureCapClassV1::Word, _) => {
                self.unique_word_features = self.unique_word_features.saturating_add(1);
            }
            (FeatureCapClassV1::Shape, _) => {
                self.unique_shape_features = self.unique_shape_features.saturating_add(1);
            }
            (FeatureCapClassV1::Syslog, _) => {
                self.unique_syslog_features = self.unique_syslog_features.saturating_add(1);
            }
            (FeatureCapClassV1::General, _) => {}
        }
    }

    fn note_dropped_feature_v1(
        &mut self,
        feature: &str,
        count: u32,
        result: &mut WindowIngestResultV1,
    ) {
        match feature_drop_counter_v1(feature) {
            DropCounterV1::Features => {
                self.meta.dropped_features = self.meta.dropped_features.saturating_add(count);
                result.dropped_features = result.dropped_features.saturating_add(count);
            }
            DropCounterV1::Words => {
                self.meta.dropped_words = self.meta.dropped_words.saturating_add(count);
                result.dropped_words = result.dropped_words.saturating_add(count);
            }
            DropCounterV1::Shapes => {
                self.meta.dropped_shapes = self.meta.dropped_shapes.saturating_add(count);
                result.dropped_shapes = result.dropped_shapes.saturating_add(count);
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FeatureCapClassV1 {
    General,
    Word,
    Shape,
    Syslog,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DropCounterV1 {
    Features,
    Words,
    Shapes,
}

fn feature_cap_class_v1(feature: &str) -> FeatureCapClassV1 {
    if feature.starts_with("w=") {
        FeatureCapClassV1::Word
    } else if feature.starts_with("syslog_") {
        FeatureCapClassV1::Syslog
    } else if feature.starts_with("shape=") || is_categorized_shape_feature_v1(feature) {
        FeatureCapClassV1::Shape
    } else {
        FeatureCapClassV1::General
    }
}

fn feature_drop_counter_v1(feature: &str) -> DropCounterV1 {
    if feature.starts_with("w=") {
        DropCounterV1::Words
    } else if feature.starts_with("shape=") {
        DropCounterV1::Shapes
    } else {
        DropCounterV1::Features
    }
}

fn is_categorized_shape_feature_v1(feature: &str) -> bool {
    if feature.starts_with("k=") || feature.starts_with("canon=") || feature.starts_with("syslog_")
    {
        return false;
    }
    if feature.contains("_net@") {
        return false;
    }
    feature.contains("=<") && feature.ends_with('>')
}

pub fn align_window_start_ts_v1(ts: UnixSec, window_size_s: u32) -> Result<UnixSec, WindowErrorV1> {
    if window_size_s == 0 {
        return Err(WindowErrorV1::InvalidWindowSize { window_size_s });
    }
    let width = i64::from(window_size_s);
    Ok(ts.div_euclid(width) * width)
}

pub fn bucket_for_window_start_ts_v1(
    window_start_ts: UnixSec,
) -> Result<BaselineBucket, WindowErrorV1> {
    let dt =
        Utc.timestamp_opt(window_start_ts, 0)
            .single()
            .ok_or(WindowErrorV1::InvalidTimestamp {
                ts: window_start_ts,
            })?;
    let weekend_group = match dt.weekday() {
        chrono::Weekday::Sat | chrono::Weekday::Sun => 1u8,
        _ => 0u8,
    };
    let hour = u8::try_from(dt.hour()).unwrap_or(0);
    Ok(weekend_group.saturating_mul(24).saturating_add(hour))
}

pub fn compute_window_key_v1(
    device_key: &str,
    window_start_ts: UnixSec,
    window_size_s: u32,
) -> Result<WindowKeyV1, WindowErrorV1> {
    if window_size_s == 0 {
        return Err(WindowErrorV1::InvalidWindowSize { window_size_s });
    }
    let aligned_window_start_ts = align_window_start_ts_v1(window_start_ts, window_size_s)?;
    if aligned_window_start_ts != window_start_ts {
        return Err(WindowErrorV1::MisalignedWindowStart {
            window_start_ts,
            aligned_window_start_ts,
            window_size_s,
        });
    }
    let window_end_ts = window_start_ts
        .checked_add(i64::from(window_size_s))
        .ok_or(WindowErrorV1::InvalidWindowBounds {
            window_start_ts,
            window_end_ts: window_start_ts,
        })?;
    let bucket = bucket_for_window_start_ts_v1(window_start_ts)?;
    Ok(WindowKeyV1 {
        device_key: device_key.to_string(),
        window_start_ts,
        window_end_ts,
        bucket,
    })
}

pub fn topk_snapshot_to_counted_pairs_v1(entries: &[TopKStringEntryV1]) -> Vec<(String, u32)> {
    entries
        .iter()
        .map(|entry| (entry.value.clone(), entry.count))
        .collect()
}
