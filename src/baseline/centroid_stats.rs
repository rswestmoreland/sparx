// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Device centroid and stats update planning helpers.
// See: contracts/20_feature_weighting_v0_1.md
//   and contracts/22_baseline_sketch_encoding_v0_1.md
// Computes weighted row vectors, applies centroid EMA updates, and updates
// per-device stats with deterministic persistence mutations.

use std::collections::BTreeMap;

use crate::db::baseline_sketch::{
    encode_centroid_v1, encode_stats_v1, BaselineSketchErrorV1, CentroidValuePairV1, DeviceStatsV1,
    WelfordF64V1,
};
use crate::db::keys::{key_tenant_centroid_v1, key_tenant_stats_v1, KeyBytes};
use crate::features::FeatureDictionaryV1;
use crate::window::FinalizedWindowRowV1;

pub const CENTROID_CAP_DEFAULT_V1: usize = 50_000;

const FEATURE_WEIGHT_SHAPE_V1: f64 = 1.0;
const FEATURE_WEIGHT_BUCKET_V1: f64 = 0.8;
const FEATURE_WEIGHT_KEYPRES_V1: f64 = 0.5;
const FEATURE_WEIGHT_CANON_V1: f64 = 0.5;
const FEATURE_WEIGHT_SYSLOG_V1: f64 = 0.2;
const FEATURE_WEIGHT_WORD_V1: f64 = 0.3;

#[derive(Clone, Debug, PartialEq)]
pub struct CentroidStatsConfigV1 {
    pub centroid_alpha: f32,
    pub centroid_cap: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CentroidStatsKvV1 {
    pub key: KeyBytes,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CentroidStatsMutationV1 {
    Put(CentroidStatsKvV1),
}

#[derive(Clone, Debug, PartialEq)]
pub struct CentroidStatsUpdatePlanV1 {
    pub weighted_row_pairs: Vec<CentroidValuePairV1>,
    pub next_centroid_pairs: Vec<CentroidValuePairV1>,
    pub next_stats: DeviceStatsV1,
    pub score_total_updated: bool,
    pub mutations: Vec<CentroidStatsMutationV1>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CentroidStatsErrorV1 {
    InvalidCentroidAlpha { centroid_alpha: f32 },
    InvalidCentroidCap { centroid_cap: usize },
    InvalidBucket { bucket: u8 },
    MissingFeatureString { feature_id: u32 },
    StatsCountOverflow { field: &'static str, current_n: u32 },
    BaselineSketch(BaselineSketchErrorV1),
}

impl From<BaselineSketchErrorV1> for CentroidStatsErrorV1 {
    fn from(value: BaselineSketchErrorV1) -> Self {
        Self::BaselineSketch(value)
    }
}

pub fn weighted_row_vector_v1(
    row: &FinalizedWindowRowV1,
    dict: &FeatureDictionaryV1,
) -> Result<Vec<CentroidValuePairV1>, CentroidStatsErrorV1> {
    let mut pairs = Vec::with_capacity(row.sparse_counts.len());
    for pair in &row.sparse_counts {
        let feature_string = dict.lookup_feature_string_v1(pair.feature_id).ok_or(
            CentroidStatsErrorV1::MissingFeatureString {
                feature_id: pair.feature_id,
            },
        )?;
        let weight = feature_weight_v1(feature_string);
        let tf = f64::from(pair.count).ln_1p();
        let value = (weight * tf) as f32;
        if value != 0.0 {
            pairs.push(CentroidValuePairV1 {
                feature_id: pair.feature_id,
                value,
            });
        }
    }
    pairs.sort_by_key(|pair| pair.feature_id);
    Ok(pairs)
}

pub fn plan_centroid_stats_update_v1(
    row: &FinalizedWindowRowV1,
    dict: &FeatureDictionaryV1,
    cfg: &CentroidStatsConfigV1,
    current_centroid_pairs: &[CentroidValuePairV1],
    current_stats: Option<&DeviceStatsV1>,
    score_total: Option<f32>,
    last_update_ts: i64,
) -> Result<CentroidStatsUpdatePlanV1, CentroidStatsErrorV1> {
    validate_centroid_stats_config_v1(cfg)?;
    validate_bucket_v1(row.key.bucket)?;

    let weighted_row_pairs = weighted_row_vector_v1(row, dict)?;
    let next_centroid_pairs =
        apply_centroid_ema_v1(current_centroid_pairs, &weighted_row_pairs, cfg);
    let next_stats = apply_stats_updates_v1(current_stats, row, score_total, last_update_ts)?;

    let mutations = vec![
        CentroidStatsMutationV1::Put(CentroidStatsKvV1 {
            key: key_tenant_centroid_v1(&row.key.device_key, row.key.bucket),
            value: encode_centroid_v1(&next_centroid_pairs)?,
        }),
        CentroidStatsMutationV1::Put(CentroidStatsKvV1 {
            key: key_tenant_stats_v1(&row.key.device_key, row.key.bucket),
            value: encode_stats_v1(&next_stats),
        }),
    ];

    Ok(CentroidStatsUpdatePlanV1 {
        weighted_row_pairs,
        next_centroid_pairs,
        next_stats,
        score_total_updated: score_total.is_some(),
        mutations,
    })
}

fn validate_centroid_stats_config_v1(
    cfg: &CentroidStatsConfigV1,
) -> Result<(), CentroidStatsErrorV1> {
    if !cfg.centroid_alpha.is_finite() || cfg.centroid_alpha <= 0.0 || cfg.centroid_alpha > 1.0 {
        return Err(CentroidStatsErrorV1::InvalidCentroidAlpha {
            centroid_alpha: cfg.centroid_alpha,
        });
    }
    if cfg.centroid_cap == 0 {
        return Err(CentroidStatsErrorV1::InvalidCentroidCap {
            centroid_cap: cfg.centroid_cap,
        });
    }
    Ok(())
}

fn validate_bucket_v1(bucket: u8) -> Result<(), CentroidStatsErrorV1> {
    if bucket >= 48 {
        return Err(CentroidStatsErrorV1::InvalidBucket { bucket });
    }
    Ok(())
}

fn feature_weight_v1(feature: &str) -> f64 {
    if is_exact_identity_feature_v1(feature) {
        0.0
    } else if feature.starts_with("k=") {
        FEATURE_WEIGHT_KEYPRES_V1
    } else if feature.starts_with("canon=") {
        FEATURE_WEIGHT_CANON_V1
    } else if feature.starts_with("syslog_") {
        FEATURE_WEIGHT_SYSLOG_V1
    } else if feature.starts_with("w=") {
        FEATURE_WEIGHT_WORD_V1
    } else if feature.contains("_net@") {
        FEATURE_WEIGHT_BUCKET_V1
    } else {
        FEATURE_WEIGHT_SHAPE_V1
    }
}

fn is_exact_identity_feature_v1(feature: &str) -> bool {
    feature.starts_with("SourceIp@")
        || feature.starts_with("DestIp@")
        || feature.starts_with("UserRaw@")
        || feature.starts_with("UserId@")
        || feature.starts_with("Domain@")
        || feature.starts_with("Host@")
}

fn apply_centroid_ema_v1(
    current_centroid_pairs: &[CentroidValuePairV1],
    weighted_row_pairs: &[CentroidValuePairV1],
    cfg: &CentroidStatsConfigV1,
) -> Vec<CentroidValuePairV1> {
    let alpha = f64::from(cfg.centroid_alpha);
    let one_minus_alpha = 1.0 - alpha;
    let mut values = BTreeMap::new();

    for pair in current_centroid_pairs {
        let decayed = f64::from(pair.value) * one_minus_alpha;
        if decayed != 0.0 {
            values.insert(pair.feature_id, decayed);
        }
    }
    for pair in weighted_row_pairs {
        let updated = values.entry(pair.feature_id).or_insert(0.0);
        *updated += alpha * f64::from(pair.value);
    }

    let mut pairs: Vec<CentroidValuePairV1> = values
        .into_iter()
        .filter_map(|(feature_id, value)| {
            if value == 0.0 {
                None
            } else {
                Some(CentroidValuePairV1 {
                    feature_id,
                    value: value as f32,
                })
            }
        })
        .collect();

    if pairs.len() > cfg.centroid_cap {
        pairs.sort_by(|a, b| {
            b.value
                .abs()
                .total_cmp(&a.value.abs())
                .then(a.feature_id.cmp(&b.feature_id))
        });
        pairs.truncate(cfg.centroid_cap);
    }
    pairs.sort_by_key(|pair| pair.feature_id);
    pairs
}

fn apply_stats_updates_v1(
    current_stats: Option<&DeviceStatsV1>,
    row: &FinalizedWindowRowV1,
    score_total: Option<f32>,
    last_update_ts: i64,
) -> Result<DeviceStatsV1, CentroidStatsErrorV1> {
    let mut next = current_stats.cloned().unwrap_or_else(|| DeviceStatsV1 {
        line_count: WelfordF64V1 {
            n: 0,
            mean: 0.0,
            m2: 0.0,
        },
        byte_count: WelfordF64V1 {
            n: 0,
            mean: 0.0,
            m2: 0.0,
        },
        score_total: WelfordF64V1 {
            n: 0,
            mean: 0.0,
            m2: 0.0,
        },
        last_update_ts,
    });

    next.line_count = welford_update_v1(&next.line_count, f64::from(row.meta.lines), "line_count")?;
    next.byte_count = welford_update_v1(&next.byte_count, row.meta.bytes as f64, "byte_count")?;
    if let Some(score_total) = score_total {
        next.score_total =
            welford_update_v1(&next.score_total, f64::from(score_total), "score_total")?;
    }
    next.last_update_ts = last_update_ts;
    Ok(next)
}

fn welford_update_v1(
    state: &WelfordF64V1,
    value: f64,
    field: &'static str,
) -> Result<WelfordF64V1, CentroidStatsErrorV1> {
    let next_n = state
        .n
        .checked_add(1)
        .ok_or(CentroidStatsErrorV1::StatsCountOverflow {
            field,
            current_n: state.n,
        })?;
    let delta = value - state.mean;
    let next_mean = state.mean + (delta / f64::from(next_n));
    let delta2 = value - next_mean;
    let next_m2 = state.m2 + (delta * delta2);
    Ok(WelfordF64V1 {
        n: next_n,
        mean: next_mean,
        m2: next_m2,
    })
}
