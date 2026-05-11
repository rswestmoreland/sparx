// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Alert scoring, explainability, and persistence helpers.
// See: contracts/03_alert_object_explanation_v0_1.md
//   and contracts/21_scoring_math_thresholding_v0_1.md
//   and contracts/27_alert_object_schema_v0_1.md

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::baseline::{weighted_row_vector_v1, BucketBaselineV1};
use crate::config::ScoringSectionV1;
use crate::db::baseline_sketch::{DeviceStatsV1, WelfordF64V1};
use crate::db::keys::{key_tenant_alert_v1, KeyBytes};
use crate::db::silence::{
    open_drop_state_from_candidate_v1, OpenDropStateV1, OpenSilenceStateV1, SharpDropCandidateV1,
    VDropCandidateV1, OPEN_SILENCE_FLAG_OPEN_V1, SILENCE_SCHEMA_VERSION_V1,
    SILENCE_SUBJECT_KIND_DEVICE_V1, SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1,
    SILENCE_SUBJECT_KIND_TENANT_V1,
};
use crate::db::source_stream::{
    source_stream_open_drop_state_from_candidate_v1,
    source_stream_open_silence_state_from_candidate_v1,
    validate_source_stream_subject_v1, SourceStreamSubjectV1,
};
use crate::features::{EntitySketchSnapshotV1, FeatureDictionaryV1};
use crate::stable_hash::stable_hash_hex128_v1;
use crate::types::{AlertId, ConfidenceV1, DeviceKey, FeatureFamilyV1, LabelV1, TenantId, UnixSec};
use crate::window::FinalizedWindowRowV1;

pub const ALERT_SCHEMA_VERSION_V1: u16 = 1;
pub const TOP_FEATURES_CAP_DEFAULT_V1: usize = 25;
pub const PROVENANCE_CAP_DEFAULT_V1: usize = 8;
pub const INFO_THRESHOLD_DEFAULT_V1: f32 = 0.60;
pub const DRIFT_MIN_DEFAULT_V1: f32 = 0.25;
pub const DRIFT_MED_REASON_THRESHOLD_V1: f32 = 0.25;
pub const DRIFT_HIGH_REASON_THRESHOLD_V1: f32 = 0.35;
pub const BLOB_RATIO_HIGH_DEFAULT_V1: f32 = 0.60;
pub const RARE_FEATURE_RATIO_THRESHOLD_V1: f32 = 0.001;
pub const VOLUME_SPIKE_THRESHOLD_V1: f32 = 0.70;
pub const VOLUME_EXTREME_THRESHOLD_V1: f32 = 0.90;
pub const VOLUME_Z_MAX_DEFAULT_V1: f32 = 6.0;
pub const COLD_START_DAYS_DEFAULT_V1: u32 = 2;
pub const MIN_LINES_PER_WINDOW_DEFAULT_V1: u32 = 10;
pub const VDROP_REASON_CODE_V1: &str = "V_DROP";
pub const VDROP_TENANT_AGGREGATE_DEVICE_KEY_V1: &str = "__tenant__";

const EPSILON_F64_V1: f64 = 1.0e-12;
const ALERT_KIND_NONE_V1: u8 = 0;
const ALERT_KIND_INFO_V1: u8 = 1;
const ALERT_KIND_NOISE_V1: u8 = 2;
const ALERT_KIND_OUTLIER_V1: u8 = 3;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AlertV1 {
    pub schema_version: u16,
    pub alert_id: AlertId,
    pub tenant_id: TenantId,
    pub device_key: DeviceKey,
    pub device_path: String,

    pub window_start_ts: UnixSec,
    pub window_end_ts: UnixSec,
    pub window_size_s: u32,
    pub bucket: u8,

    pub label: LabelV1,
    pub confidence: ConfidenceV1,
    pub cold_start: bool,

    pub score_total: f32,
    pub score_rarity: f32,
    pub score_drift: f32,
    pub score_volume: f32,

    pub baseline_n_bucket: Option<u32>,
    pub baseline_centroid_norm: Option<f32>,

    pub reasons: Vec<ReasonV1>,
    pub top_features: Vec<TopFeatureV1>,

    pub summary_analyst: String,
    pub summary_customer: String,

    pub entities: EntitiesV1,

    pub lines: u32,
    pub bytes: u64,
    pub dropped_features: u32,
    pub dropped_words: u32,
    pub dropped_shapes: u32,

    pub provenance: Vec<FileSpanV1>,

    pub signature: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReasonV1 {
    pub code: String,
    pub msg: String,
    pub details: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TopFeatureV1 {
    pub feature: String,
    pub feature_id: u32,
    pub count: u32,
    pub family: FeatureFamilyV1,
    pub tf_w: f32,
    pub idf: f32,
    pub contrib: f32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntitiesV1 {
    pub src_ips: Vec<CountedStringV1>,
    pub dst_ips: Vec<CountedStringV1>,
    pub user_ids: Vec<CountedStringV1>,
    pub domains: Vec<CountedStringV1>,
    pub hosts: Vec<CountedStringV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountedStringV1 {
    pub value: String,
    pub count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileSpanV1 {
    pub file_rel: String,
    pub file_key: String,
    pub inode: u64,
    pub offset_start: u64,
    pub offset_end: u64,
    pub is_gzip: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlertKvV1 {
    pub key: KeyBytes,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AlertScoringConfigV1 {
    pub outlier_threshold: f32,
    pub noise_threshold: f32,
    pub info_threshold: f32,
    pub drift_min: f32,
    pub blob_ratio_high: f32,
    pub volume_z_max: f32,
    pub cold_start_days: u32,
    pub cold_start_min_windows: u32,
    pub min_lines_per_window: u32,
    pub top_features_cap: usize,
    pub include_debug_fields: bool,
}

impl Default for AlertScoringConfigV1 {
    fn default() -> Self {
        Self {
            outlier_threshold: 0.85,
            noise_threshold: 0.65,
            info_threshold: INFO_THRESHOLD_DEFAULT_V1,
            drift_min: DRIFT_MIN_DEFAULT_V1,
            blob_ratio_high: BLOB_RATIO_HIGH_DEFAULT_V1,
            volume_z_max: VOLUME_Z_MAX_DEFAULT_V1,
            cold_start_days: COLD_START_DAYS_DEFAULT_V1,
            cold_start_min_windows: compute_cold_start_min_windows_v1(COLD_START_DAYS_DEFAULT_V1, 60),
            min_lines_per_window: MIN_LINES_PER_WINDOW_DEFAULT_V1,
            top_features_cap: TOP_FEATURES_CAP_DEFAULT_V1,
            include_debug_fields: false,
        }
    }
}

impl AlertScoringConfigV1 {
    pub fn from_sections_v1(value: &ScoringSectionV1, window_size_s: u32) -> Self {
        let cold_start_min_windows = compute_cold_start_min_windows_v1(value.cold_start_days, window_size_s);
        Self {
            outlier_threshold: value.outlier_threshold,
            noise_threshold: value.noise_threshold,
            cold_start_days: value.cold_start_days,
            cold_start_min_windows,
            min_lines_per_window: value.min_lines_per_window,
            ..Self::default()
        }
    }
}

impl From<&ScoringSectionV1> for AlertScoringConfigV1 {
    fn from(value: &ScoringSectionV1) -> Self {
        Self::from_sections_v1(value, 60)
    }
}

fn compute_cold_start_min_windows_v1(cold_start_days: u32, window_size_s: u32) -> u32 {
    if cold_start_days == 0 || window_size_s == 0 {
        return 0;
    }
    let windows_per_bucket_day = 3600u32 / window_size_s;
    if windows_per_bucket_day == 0 {
        return 0;
    }
    cold_start_days.saturating_mul(windows_per_bucket_day)
}

#[derive(Clone, Debug, PartialEq)]
pub struct AlertBuildResultV1 {
    pub rarity: f32,
    pub drift: f32,
    pub volume: f32,
    pub score_total: f32,
    pub cold_start: bool,
    pub below_min_lines: bool,
    pub entity_focus: bool,
    pub blob_ratio: f32,
    pub reasons: Vec<ReasonV1>,
    pub top_features: Vec<TopFeatureV1>,
    pub alert: Option<AlertV1>,
    pub primary_put: Option<AlertKvV1>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VDropAlertBuildResultV1 {
    pub alert: AlertV1,
    pub primary_put: AlertKvV1,
    pub open_silence: OpenSilenceStateV1,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SharpDropAlertBuildResultV1 {
    pub alert: AlertV1,
    pub primary_put: AlertKvV1,
    pub open_drop: OpenDropStateV1,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AlertErrorV1 {
    InvalidOutlierThreshold { value: f32 },
    InvalidNoiseThreshold { value: f32 },
    InvalidInfoThreshold { value: f32 },
    InvalidDriftMin { value: f32 },
    InvalidBlobRatioHigh { value: f32 },
    InvalidVolumeZMax { value: f32 },
    InvalidColdStartMinWindows { value: u32 },
    InvalidTopFeaturesCap { value: usize },
    BucketMismatch { row_bucket: u8, baseline_bucket: u8 },
    MissingFeatureString { feature_id: u32 },
    Postcard { msg: String },
    InvalidVDropCandidate { msg: String },
    InvalidSharpDropCandidate { msg: String },
}

pub fn build_alert_v1(
    tenant_id: &str,
    device_path: &str,
    row: &FinalizedWindowRowV1,
    dict: &FeatureDictionaryV1,
    baseline: &BucketBaselineV1,
    device_stats: Option<&DeviceStatsV1>,
    cfg: &AlertScoringConfigV1,
    provenance: &[FileSpanV1],
) -> Result<AlertBuildResultV1, AlertErrorV1> {
    validate_alert_scoring_config_v1(cfg)?;
    if row.key.bucket != baseline.bucket {
        return Err(AlertErrorV1::BucketMismatch {
            row_bucket: row.key.bucket,
            baseline_bucket: baseline.bucket,
        });
    }

    let weighted_pairs = weighted_row_vector_v1(row, dict).map_err(map_centroid_error_v1)?;
    let weighted_map = centroid_value_pairs_to_map_v1(&weighted_pairs);
    let df_map = df_pairs_to_map_v1(&baseline.df);
    let centroid_map = baseline_centroid_pairs_to_map_v1(&baseline.centroid);
    let centroid_norm = l2_norm_v1(&centroid_map);
    let row_norm = l2_norm_v1(&weighted_map);

    let mut feature_scores = Vec::with_capacity(row.sparse_counts.len());
    let n_bucket = baseline.n_bucket;
    let mut rarity_mass_raw = 0.0f64;
    let mut row_mass = 0.0f64;
    let mut blob_mass = 0.0f64;

    for pair in &row.sparse_counts {
        let feature_string = dict
            .lookup_feature_string_v1(pair.feature_id)
            .ok_or(AlertErrorV1::MissingFeatureString {
                feature_id: pair.feature_id,
            })?;
        let tf_w = f64::from(*weighted_map.get(&pair.feature_id).unwrap_or(&0.0f32));
        let df_count = *df_map.get(&pair.feature_id).unwrap_or(&0u32);
        let idf = ((f64::from(n_bucket) + 1.0) / (f64::from(df_count) + 1.0)).ln() + 1.0;
        let contrib = tf_w * idf;
        let is_blob = is_blob_feature_v1(feature_string);
        rarity_mass_raw += contrib;
        row_mass += tf_w;
        if is_blob {
            blob_mass += tf_w;
        }
        feature_scores.push(FeatureScoreV1 {
            feature_id: pair.feature_id,
            feature: feature_string.to_string(),
            count: pair.count,
            family: family_from_feature_v1(feature_string),
            tf_w: tf_w as f32,
            idf: idf as f32,
            contrib: contrib as f32,
            df_count,
        });
    }

    let rarity_mass = if row_mass > EPSILON_F64_V1 {
        rarity_mass_raw / row_mass
    } else {
        0.0
    };
    let rarity = (1.0 - (-rarity_mass).exp()) as f32;

    let drift = cosine_drift_v1(&weighted_map, row_norm, &centroid_map, centroid_norm);
    let volume = volume_score_v1(row, device_stats, cfg.volume_z_max);
    let drift01 = drift.clamp(0.0, 1.0);
    let score_total = (0.45 * rarity) + (0.40 * drift01) + (0.15 * volume);
    let cold_start = centroid_map.is_empty()
        || (cfg.cold_start_min_windows > 0 && n_bucket < cfg.cold_start_min_windows);
    let below_min_lines = cfg.min_lines_per_window > 0 && row.meta.lines < cfg.min_lines_per_window;
    let blob_ratio = if row_mass > EPSILON_F64_V1 {
        (blob_mass / row_mass) as f32
    } else {
        0.0
    };

    let top_feature_scores = select_top_feature_scores_v1(&feature_scores, cfg.top_features_cap);
    let top_features = top_feature_scores
        .iter()
        .map(|score| TopFeatureV1 {
            feature: score.feature.clone(),
            feature_id: score.feature_id,
            count: score.count,
            family: score.family,
            tf_w: score.tf_w,
            idf: score.idf,
            contrib: score.contrib,
        })
        .collect::<Vec<TopFeatureV1>>();
    let entities = build_entities_v1(&row.entity_snapshot);
    let entity_focus = compute_entity_focus_v1(&top_features, &entities);
    let scored_label = choose_label_v1(score_total, drift, volume, cold_start, entity_focus, blob_ratio, cfg);
    let label = if below_min_lines {
        ALERT_KIND_NONE_V1
    } else {
        scored_label
    };
    let reasons = build_reasons_v1(&top_feature_scores, n_bucket, drift, volume, blob_ratio, entity_focus, scored_label);

    let alert = if label == ALERT_KIND_NONE_V1 {
        None
    } else {
        let label_enum = label_from_kind_v1(label);
        let confidence = choose_confidence_v1(label_enum, cold_start, score_total, reasons.len(), entity_focus, cfg);
        let capped_provenance = cap_provenance_v1(provenance, PROVENANCE_CAP_DEFAULT_V1);
        let summary_analyst = build_summary_analyst_v1(label_enum, score_total, &reasons, &top_features);
        let summary_customer = build_summary_customer_v1(label_enum, &reasons, &top_features, &entities);
        let baseline_n_bucket = if cfg.include_debug_fields {
            Some(n_bucket)
        } else {
            None
        };
        let baseline_centroid_norm = if cfg.include_debug_fields {
            Some(centroid_norm as f32)
        } else {
            None
        };
        let signature = compute_alert_signature_v1(&top_features);
        let alert_id = compute_alert_id_v1(tenant_id, &row.key.device_key, row.key.window_start_ts, &signature);
        let alert = AlertV1 {
            schema_version: ALERT_SCHEMA_VERSION_V1,
            alert_id,
            tenant_id: tenant_id.to_string(),
            device_key: row.key.device_key.clone(),
            device_path: device_path.to_string(),
            window_start_ts: row.key.window_start_ts,
            window_end_ts: row.key.window_end_ts,
            window_size_s: u32::try_from(row.key.window_end_ts - row.key.window_start_ts).unwrap_or(0),
            bucket: row.key.bucket,
            label: label_enum,
            confidence,
            cold_start,
            score_total,
            score_rarity: rarity,
            score_drift: drift,
            score_volume: volume,
            baseline_n_bucket,
            baseline_centroid_norm,
            reasons: reasons.clone(),
            top_features: top_features.clone(),
            summary_analyst,
            summary_customer,
            entities,
            lines: row.meta.lines,
            bytes: row.meta.bytes,
            dropped_features: row.meta.dropped_features,
            dropped_words: row.meta.dropped_words,
            dropped_shapes: row.meta.dropped_shapes,
            provenance: capped_provenance,
            signature,
        };
        Some(alert)
    };

    let primary_put = match &alert {
        Some(alert) => Some(alert_primary_put_v1(alert)?),
        None => None,
    };

    Ok(AlertBuildResultV1 {
        rarity,
        drift,
        volume,
        score_total,
        cold_start,
        below_min_lines,
        entity_focus,
        blob_ratio,
        reasons,
        top_features,
        alert,
        primary_put,
    })
}

pub fn alert_primary_put_v1(alert: &AlertV1) -> Result<AlertKvV1, AlertErrorV1> {
    Ok(AlertKvV1 {
        key: key_tenant_alert_v1(&alert.alert_id),
        value: encode_alert_v1(alert)?,
    })
}

pub fn build_vdrop_alert_v1(candidate: &VDropCandidateV1) -> Result<VDropAlertBuildResultV1, AlertErrorV1> {
    validate_vdrop_candidate_for_alert_v1(candidate)?;

    let reason = ReasonV1 {
        code: VDROP_REASON_CODE_V1.to_string(),
        msg: "expected log activity was not observed for this subject".to_string(),
        details: candidate.reason_details.clone(),
    };
    let signature = compute_vdrop_signature_v1(candidate);
    let alert_id = compute_vdrop_alert_id_v1(candidate);
    let device_key = vdrop_alert_device_key_v1(candidate);
    let device_path = vdrop_alert_device_path_v1(candidate);
    let window_size_s = u32::try_from(candidate.window_end_ts_i64 - candidate.window_start_ts_i64).unwrap_or(0);
    let score_volume = candidate.drop_ratio_f32.clamp(0.0, 1.0);

    let alert = AlertV1 {
        schema_version: ALERT_SCHEMA_VERSION_V1,
        alert_id,
        tenant_id: candidate.tenant_id.clone(),
        device_key,
        device_path,
        window_start_ts: candidate.window_start_ts_i64,
        window_end_ts: candidate.window_end_ts_i64,
        window_size_s,
        bucket: candidate.bucket_u8,
        label: LabelV1::Info,
        confidence: ConfidenceV1::Medium,
        cold_start: false,
        score_total: score_volume,
        score_rarity: 0.0,
        score_drift: 0.0,
        score_volume,
        baseline_n_bucket: None,
        baseline_centroid_norm: None,
        reasons: vec![reason],
        top_features: Vec::new(),
        summary_analyst: build_vdrop_summary_analyst_v1(candidate),
        summary_customer: build_vdrop_summary_customer_v1(candidate),
        entities: EntitiesV1 {
            src_ips: Vec::new(),
            dst_ips: Vec::new(),
            user_ids: Vec::new(),
            domains: Vec::new(),
            hosts: Vec::new(),
        },
        lines: 0,
        bytes: 0,
        dropped_features: 0,
        dropped_words: 0,
        dropped_shapes: 0,
        provenance: Vec::new(),
        signature,
    };
    let primary_put = alert_primary_put_v1(&alert)?;
    let open_silence = OpenSilenceStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: candidate.subject_kind_u8,
        state_flags_u8: OPEN_SILENCE_FLAG_OPEN_V1,
        silence_start_ts_i64: candidate.window_start_ts_i64,
        last_alert_window_start_ts_i64: candidate.window_start_ts_i64,
        last_alert_window_end_ts_i64: candidate.window_end_ts_i64,
        last_alert_id: alert.alert_id.clone(),
    };

    Ok(VDropAlertBuildResultV1 {
        alert,
        primary_put,
        open_silence,
    })
}

pub fn build_sharp_drop_alert_v1(
    candidate: &SharpDropCandidateV1,
    provenance: &[FileSpanV1],
) -> Result<SharpDropAlertBuildResultV1, AlertErrorV1> {
    validate_sharp_drop_candidate_for_alert_v1(candidate)?;

    let reason = ReasonV1 {
        code: VDROP_REASON_CODE_V1.to_string(),
        msg: "log volume dropped sharply but did not stop for this subject".to_string(),
        details: candidate.reason_details.clone(),
    };
    let signature = compute_reason_signature_v1(VDROP_REASON_CODE_V1, &candidate.reason_details);
    let alert_id = compute_sharp_drop_alert_id_v1(candidate);
    let device_key = sharp_drop_alert_device_key_v1(candidate);
    let device_path = sharp_drop_alert_device_path_v1(candidate);
    let window_size_s = u32::try_from(candidate.window_end_ts_i64 - candidate.window_start_ts_i64).unwrap_or(0);
    let score_volume = candidate.drop_ratio_f32.clamp(0.0, 1.0);
    let capped_provenance = if candidate.subject_kind_u8 == SILENCE_SUBJECT_KIND_DEVICE_V1 {
        cap_provenance_v1(provenance, PROVENANCE_CAP_DEFAULT_V1)
    } else {
        Vec::new()
    };

    let alert = AlertV1 {
        schema_version: ALERT_SCHEMA_VERSION_V1,
        alert_id,
        tenant_id: candidate.tenant_id.clone(),
        device_key,
        device_path,
        window_start_ts: candidate.window_start_ts_i64,
        window_end_ts: candidate.window_end_ts_i64,
        window_size_s,
        bucket: candidate.bucket_u8,
        label: LabelV1::Info,
        confidence: ConfidenceV1::Medium,
        cold_start: false,
        score_total: score_volume,
        score_rarity: 0.0,
        score_drift: 0.0,
        score_volume,
        baseline_n_bucket: None,
        baseline_centroid_norm: None,
        reasons: vec![reason],
        top_features: Vec::new(),
        summary_analyst: build_sharp_drop_summary_analyst_v1(candidate),
        summary_customer: build_sharp_drop_summary_customer_v1(candidate),
        entities: EntitiesV1 {
            src_ips: Vec::new(),
            dst_ips: Vec::new(),
            user_ids: Vec::new(),
            domains: Vec::new(),
            hosts: Vec::new(),
        },
        lines: u32::try_from(candidate.observed_lines_u64).unwrap_or(u32::MAX),
        bytes: candidate.observed_bytes_u64,
        dropped_features: 0,
        dropped_words: 0,
        dropped_shapes: 0,
        provenance: capped_provenance,
        signature,
    };
    let primary_put = alert_primary_put_v1(&alert)?;
    let open_drop = open_drop_state_from_candidate_v1(candidate, &alert.alert_id);

    Ok(SharpDropAlertBuildResultV1 {
        alert,
        primary_put,
        open_drop,
    })
}


pub fn build_source_stream_vdrop_alert_v1(
    subject: &SourceStreamSubjectV1,
    candidate: &VDropCandidateV1,
) -> Result<VDropAlertBuildResultV1, AlertErrorV1> {
    validate_source_stream_vdrop_candidate_for_alert_v1(subject, candidate)?;

    let reason_details = source_stream_hard_silence_reason_details_v1(subject, candidate);
    let reason = ReasonV1 {
        code: VDROP_REASON_CODE_V1.to_string(),
        msg: "expected log activity was not observed for this source stream".to_string(),
        details: reason_details.clone(),
    };
    let signature = compute_reason_signature_v1(VDROP_REASON_CODE_V1, &reason_details);
    let alert_id = compute_source_stream_vdrop_alert_id_v1(candidate);
    let window_size_s = u32::try_from(candidate.window_end_ts_i64 - candidate.window_start_ts_i64).unwrap_or(0);
    let score_volume = candidate.drop_ratio_f32.clamp(0.0, 1.0);

    let alert = AlertV1 {
        schema_version: ALERT_SCHEMA_VERSION_V1,
        alert_id,
        tenant_id: candidate.tenant_id.clone(),
        device_key: subject.device_key.clone(),
        device_path: source_stream_alert_device_path_v1(subject),
        window_start_ts: candidate.window_start_ts_i64,
        window_end_ts: candidate.window_end_ts_i64,
        window_size_s,
        bucket: candidate.bucket_u8,
        label: LabelV1::Info,
        confidence: ConfidenceV1::Medium,
        cold_start: false,
        score_total: score_volume,
        score_rarity: 0.0,
        score_drift: 0.0,
        score_volume,
        baseline_n_bucket: None,
        baseline_centroid_norm: None,
        reasons: vec![reason],
        top_features: Vec::new(),
        summary_analyst: build_source_stream_vdrop_summary_analyst_v1(subject, candidate),
        summary_customer: build_source_stream_vdrop_summary_customer_v1(subject, candidate),
        entities: EntitiesV1 {
            src_ips: Vec::new(),
            dst_ips: Vec::new(),
            user_ids: Vec::new(),
            domains: Vec::new(),
            hosts: Vec::new(),
        },
        lines: 0,
        bytes: 0,
        dropped_features: 0,
        dropped_words: 0,
        dropped_shapes: 0,
        provenance: Vec::new(),
        signature,
    };
    let primary_put = alert_primary_put_v1(&alert)?;
    let open_silence = source_stream_open_silence_state_from_candidate_v1(subject, candidate, &alert.alert_id)
        .map_err(|e| AlertErrorV1::InvalidVDropCandidate {
            msg: format!("source-stream open-silence construction failed: {:?}", e),
        })?;

    Ok(VDropAlertBuildResultV1 {
        alert,
        primary_put,
        open_silence,
    })
}

pub fn build_source_stream_sharp_drop_alert_v1(
    subject: &SourceStreamSubjectV1,
    candidate: &SharpDropCandidateV1,
    provenance: &[FileSpanV1],
) -> Result<SharpDropAlertBuildResultV1, AlertErrorV1> {
    validate_source_stream_sharp_drop_candidate_for_alert_v1(subject, candidate)?;

    let reason = ReasonV1 {
        code: VDROP_REASON_CODE_V1.to_string(),
        msg: "log volume dropped sharply but did not stop for this source stream".to_string(),
        details: candidate.reason_details.clone(),
    };
    let signature = compute_reason_signature_v1(VDROP_REASON_CODE_V1, &candidate.reason_details);
    let alert_id = compute_source_stream_sharp_drop_alert_id_v1(candidate);
    let window_size_s = u32::try_from(candidate.window_end_ts_i64 - candidate.window_start_ts_i64).unwrap_or(0);
    let score_volume = candidate.drop_ratio_f32.clamp(0.0, 1.0);
    let capped_provenance = cap_provenance_v1(provenance, PROVENANCE_CAP_DEFAULT_V1);

    let alert = AlertV1 {
        schema_version: ALERT_SCHEMA_VERSION_V1,
        alert_id,
        tenant_id: candidate.tenant_id.clone(),
        device_key: subject.device_key.clone(),
        device_path: source_stream_alert_device_path_v1(subject),
        window_start_ts: candidate.window_start_ts_i64,
        window_end_ts: candidate.window_end_ts_i64,
        window_size_s,
        bucket: candidate.bucket_u8,
        label: LabelV1::Info,
        confidence: ConfidenceV1::Medium,
        cold_start: false,
        score_total: score_volume,
        score_rarity: 0.0,
        score_drift: 0.0,
        score_volume,
        baseline_n_bucket: None,
        baseline_centroid_norm: None,
        reasons: vec![reason],
        top_features: Vec::new(),
        summary_analyst: build_source_stream_sharp_drop_summary_analyst_v1(subject, candidate),
        summary_customer: build_source_stream_sharp_drop_summary_customer_v1(subject, candidate),
        entities: EntitiesV1 {
            src_ips: Vec::new(),
            dst_ips: Vec::new(),
            user_ids: Vec::new(),
            domains: Vec::new(),
            hosts: Vec::new(),
        },
        lines: u32::try_from(candidate.observed_lines_u64).unwrap_or(u32::MAX),
        bytes: candidate.observed_bytes_u64,
        dropped_features: 0,
        dropped_words: 0,
        dropped_shapes: 0,
        provenance: capped_provenance,
        signature,
    };
    let primary_put = alert_primary_put_v1(&alert)?;
    let open_drop = source_stream_open_drop_state_from_candidate_v1(subject, candidate, &alert.alert_id)
        .map_err(|e| AlertErrorV1::InvalidSharpDropCandidate {
            msg: format!("source-stream open-drop construction failed: {:?}", e),
        })?;

    Ok(SharpDropAlertBuildResultV1 {
        alert,
        primary_put,
        open_drop,
    })
}

fn validate_sharp_drop_candidate_for_alert_v1(candidate: &SharpDropCandidateV1) -> Result<(), AlertErrorV1> {
    match candidate.subject_kind_u8 {
        SILENCE_SUBJECT_KIND_DEVICE_V1 | SILENCE_SUBJECT_KIND_TENANT_V1 => {}
        other => {
            return Err(AlertErrorV1::InvalidSharpDropCandidate {
                msg: format!("invalid subject kind: {}", other),
            })
        }
    }
    if candidate.tenant_id.is_empty() {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "tenant_id must not be empty".to_string(),
        });
    }
    if candidate.subject_key.is_empty() {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "subject_key must not be empty".to_string(),
        });
    }
    if candidate.window_end_ts_i64 <= candidate.window_start_ts_i64 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "window end must be after window start".to_string(),
        });
    }
    if candidate.bucket_u8 > 47 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: format!("invalid bucket: {}", candidate.bucket_u8),
        });
    }
    if candidate.observed_lines_u64 == 0 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "observed_lines_u64 must be positive for sharp drop".to_string(),
        });
    }
    if !candidate.expected_lines_f64.is_finite() || candidate.expected_lines_f64 <= 0.0 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "expected_lines_f64 must be finite and positive".to_string(),
        });
    }
    if !candidate.expected_bytes_f64.is_finite() || candidate.expected_bytes_f64 < 0.0 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "expected_bytes_f64 must be finite and nonnegative".to_string(),
        });
    }
    if !candidate.observed_expected_ratio_f32.is_finite()
        || candidate.observed_expected_ratio_f32 < 0.0
        || candidate.observed_expected_ratio_f32 > 1.0
    {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: format!("invalid observed/expected ratio: {}", candidate.observed_expected_ratio_f32),
        });
    }
    if !candidate.drop_ratio_f32.is_finite() || candidate.drop_ratio_f32 < 0.0 || candidate.drop_ratio_f32 > 1.0 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: format!("invalid drop ratio: {}", candidate.drop_ratio_f32),
        });
    }
    if !candidate.absolute_drop_lines_f64.is_finite() || candidate.absolute_drop_lines_f64 <= 0.0 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "absolute_drop_lines_f64 must be finite and positive".to_string(),
        });
    }
    if candidate.maturity_count_u32 == 0 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "maturity_count_u32 must be positive".to_string(),
        });
    }
    match candidate.reason_details.first() {
        Some((key, value)) if key == "drop_kind" && value == "sharp_drop" => {}
        _ => {
            return Err(AlertErrorV1::InvalidSharpDropCandidate {
                msg: "first reason detail must be drop_kind=sharp_drop".to_string(),
            })
        }
    }
    Ok(())
}

fn validate_vdrop_candidate_for_alert_v1(candidate: &VDropCandidateV1) -> Result<(), AlertErrorV1> {
    match candidate.subject_kind_u8 {
        SILENCE_SUBJECT_KIND_DEVICE_V1 | SILENCE_SUBJECT_KIND_TENANT_V1 => {}
        other => {
            return Err(AlertErrorV1::InvalidVDropCandidate {
                msg: format!("invalid subject kind: {}", other),
            })
        }
    }
    if candidate.tenant_id.is_empty() {
        return Err(AlertErrorV1::InvalidVDropCandidate {
            msg: "tenant_id must not be empty".to_string(),
        });
    }
    if candidate.subject_key.is_empty() {
        return Err(AlertErrorV1::InvalidVDropCandidate {
            msg: "subject_key must not be empty".to_string(),
        });
    }
    if candidate.window_end_ts_i64 <= candidate.window_start_ts_i64 {
        return Err(AlertErrorV1::InvalidVDropCandidate {
            msg: "window end must be after window start".to_string(),
        });
    }
    if candidate.expected_windows_missed_u64 == 0 {
        return Err(AlertErrorV1::InvalidVDropCandidate {
            msg: "expected_windows_missed_u64 must be positive".to_string(),
        });
    }
    if candidate.bucket_u8 > 47 {
        return Err(AlertErrorV1::InvalidVDropCandidate {
            msg: format!("invalid bucket: {}", candidate.bucket_u8),
        });
    }
    if !candidate.drop_ratio_f32.is_finite()
        || candidate.drop_ratio_f32 < 0.0
        || candidate.drop_ratio_f32 > 1.0
    {
        return Err(AlertErrorV1::InvalidVDropCandidate {
            msg: format!("invalid drop ratio: {}", candidate.drop_ratio_f32),
        });
    }
    Ok(())
}

fn vdrop_alert_device_key_v1(candidate: &VDropCandidateV1) -> String {
    if candidate.subject_kind_u8 == SILENCE_SUBJECT_KIND_DEVICE_V1 {
        candidate.subject_key.clone()
    } else {
        VDROP_TENANT_AGGREGATE_DEVICE_KEY_V1.to_string()
    }
}

fn vdrop_alert_device_path_v1(candidate: &VDropCandidateV1) -> String {
    if candidate.subject_kind_u8 == SILENCE_SUBJECT_KIND_DEVICE_V1 {
        candidate.subject_key.clone()
    } else {
        format!("tenant:{}", candidate.tenant_id)
    }
}

fn compute_reason_signature_v1(code: &str, details: &[(String, String)]) -> String {
    let mut input = String::new();
    input.push_str(code);
    input.push('\n');
    for (key, value) in details {
        input.push_str(key);
        input.push('\t');
        input.push_str(value);
        input.push('\n');
    }
    stable_hash_hex128_v1(&input)
}

fn compute_vdrop_signature_v1(candidate: &VDropCandidateV1) -> String {
    compute_reason_signature_v1(VDROP_REASON_CODE_V1, &candidate.reason_details)
}

fn compute_vdrop_alert_id_v1(candidate: &VDropCandidateV1) -> String {
    let input = format!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}",
        candidate.tenant_id,
        subject_kind_alert_id_part_v1(candidate.subject_kind_u8),
        candidate.subject_key,
        candidate.window_start_ts_i64,
        candidate.window_end_ts_i64,
        VDROP_REASON_CODE_V1,
        candidate.expected_windows_missed_u64
    );
    stable_hash_hex128_v1(&input)
}

fn compute_sharp_drop_alert_id_v1(candidate: &SharpDropCandidateV1) -> String {
    let input = format!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}",
        candidate.tenant_id,
        subject_kind_alert_id_part_v1(candidate.subject_kind_u8),
        candidate.subject_key,
        candidate.window_start_ts_i64,
        candidate.window_end_ts_i64,
        VDROP_REASON_CODE_V1,
        "sharp_drop"
    );
    stable_hash_hex128_v1(&input)
}

fn sharp_drop_alert_device_key_v1(candidate: &SharpDropCandidateV1) -> String {
    if candidate.subject_kind_u8 == SILENCE_SUBJECT_KIND_DEVICE_V1 {
        candidate.subject_key.clone()
    } else {
        VDROP_TENANT_AGGREGATE_DEVICE_KEY_V1.to_string()
    }
}

fn sharp_drop_alert_device_path_v1(candidate: &SharpDropCandidateV1) -> String {
    if candidate.subject_kind_u8 == SILENCE_SUBJECT_KIND_DEVICE_V1 {
        candidate.subject_key.clone()
    } else {
        format!("tenant:{}", candidate.tenant_id)
    }
}

fn subject_kind_alert_id_part_v1(subject_kind: u8) -> &'static str {
    match subject_kind {
        SILENCE_SUBJECT_KIND_DEVICE_V1 => "device",
        SILENCE_SUBJECT_KIND_TENANT_V1 => "tenant",
        SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1 => "source_stream",
        _ => "unknown",
    }
}


fn validate_source_stream_vdrop_candidate_for_alert_v1(
    subject: &SourceStreamSubjectV1,
    candidate: &VDropCandidateV1,
) -> Result<(), AlertErrorV1> {
    validate_source_stream_subject_v1(subject).map_err(|e| AlertErrorV1::InvalidVDropCandidate {
        msg: format!("invalid source-stream subject: {:?}", e),
    })?;
    if candidate.subject_kind_u8 != SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1 {
        return Err(AlertErrorV1::InvalidVDropCandidate {
            msg: format!("invalid source-stream subject kind: {}", candidate.subject_kind_u8),
        });
    }
    if candidate.tenant_id.as_str() != subject.tenant_id.as_str() {
        return Err(AlertErrorV1::InvalidVDropCandidate {
            msg: "candidate tenant_id does not match source-stream subject".to_string(),
        });
    }
    if candidate.subject_key.as_str() != subject.source_stream_id.as_str() {
        return Err(AlertErrorV1::InvalidVDropCandidate {
            msg: "candidate subject_key does not match source_stream_id".to_string(),
        });
    }
    validate_vdrop_candidate_core_for_alert_v1(candidate)?;
    require_reason_detail_v1(&candidate.reason_details, "subject_kind", "source_stream", "vdrop")?;
    require_reason_detail_v1(&candidate.reason_details, "tenant_id", &subject.tenant_id, "vdrop")?;
    require_reason_detail_v1(&candidate.reason_details, "device_key", &subject.device_key, "vdrop")?;
    require_reason_detail_v1(&candidate.reason_details, "source_stream_id", &subject.source_stream_id, "vdrop")?;
    require_reason_detail_v1(&candidate.reason_details, "source_path", &subject.canonical_source_path, "vdrop")?;
    Ok(())
}

fn validate_source_stream_sharp_drop_candidate_for_alert_v1(
    subject: &SourceStreamSubjectV1,
    candidate: &SharpDropCandidateV1,
) -> Result<(), AlertErrorV1> {
    validate_source_stream_subject_v1(subject).map_err(|e| AlertErrorV1::InvalidSharpDropCandidate {
        msg: format!("invalid source-stream subject: {:?}", e),
    })?;
    if candidate.subject_kind_u8 != SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: format!("invalid source-stream subject kind: {}", candidate.subject_kind_u8),
        });
    }
    if candidate.tenant_id.as_str() != subject.tenant_id.as_str() {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "candidate tenant_id does not match source-stream subject".to_string(),
        });
    }
    if candidate.subject_key.as_str() != subject.source_stream_id.as_str() {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "candidate subject_key does not match source_stream_id".to_string(),
        });
    }
    validate_sharp_drop_candidate_core_for_alert_v1(candidate)?;
    require_reason_detail_v1(&candidate.reason_details, "subject_kind", "source_stream", "sharp_drop")?;
    require_reason_detail_v1(&candidate.reason_details, "tenant_id", &subject.tenant_id, "sharp_drop")?;
    require_reason_detail_v1(&candidate.reason_details, "device_key", &subject.device_key, "sharp_drop")?;
    require_reason_detail_v1(&candidate.reason_details, "source_stream_id", &subject.source_stream_id, "sharp_drop")?;
    require_reason_detail_v1(&candidate.reason_details, "source_path", &subject.canonical_source_path, "sharp_drop")?;
    Ok(())
}

fn validate_vdrop_candidate_core_for_alert_v1(candidate: &VDropCandidateV1) -> Result<(), AlertErrorV1> {
    if candidate.tenant_id.is_empty() {
        return Err(AlertErrorV1::InvalidVDropCandidate {
            msg: "tenant_id must not be empty".to_string(),
        });
    }
    if candidate.subject_key.is_empty() {
        return Err(AlertErrorV1::InvalidVDropCandidate {
            msg: "subject_key must not be empty".to_string(),
        });
    }
    if candidate.window_end_ts_i64 <= candidate.window_start_ts_i64 {
        return Err(AlertErrorV1::InvalidVDropCandidate {
            msg: "window end must be after window start".to_string(),
        });
    }
    if candidate.expected_windows_missed_u64 == 0 {
        return Err(AlertErrorV1::InvalidVDropCandidate {
            msg: "expected_windows_missed_u64 must be positive".to_string(),
        });
    }
    if candidate.bucket_u8 > 47 {
        return Err(AlertErrorV1::InvalidVDropCandidate {
            msg: format!("invalid bucket: {}", candidate.bucket_u8),
        });
    }
    if !candidate.drop_ratio_f32.is_finite()
        || candidate.drop_ratio_f32 < 0.0
        || candidate.drop_ratio_f32 > 1.0
    {
        return Err(AlertErrorV1::InvalidVDropCandidate {
            msg: format!("invalid drop ratio: {}", candidate.drop_ratio_f32),
        });
    }
    Ok(())
}

fn validate_sharp_drop_candidate_core_for_alert_v1(candidate: &SharpDropCandidateV1) -> Result<(), AlertErrorV1> {
    if candidate.tenant_id.is_empty() {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "tenant_id must not be empty".to_string(),
        });
    }
    if candidate.subject_key.is_empty() {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "subject_key must not be empty".to_string(),
        });
    }
    if candidate.window_end_ts_i64 <= candidate.window_start_ts_i64 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "window end must be after window start".to_string(),
        });
    }
    if candidate.bucket_u8 > 47 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: format!("invalid bucket: {}", candidate.bucket_u8),
        });
    }
    if candidate.observed_lines_u64 == 0 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "observed_lines_u64 must be positive for sharp drop".to_string(),
        });
    }
    if !candidate.expected_lines_f64.is_finite() || candidate.expected_lines_f64 <= 0.0 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "expected_lines_f64 must be finite and positive".to_string(),
        });
    }
    if !candidate.expected_bytes_f64.is_finite() || candidate.expected_bytes_f64 < 0.0 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "expected_bytes_f64 must be finite and nonnegative".to_string(),
        });
    }
    if !candidate.observed_expected_ratio_f32.is_finite()
        || candidate.observed_expected_ratio_f32 < 0.0
        || candidate.observed_expected_ratio_f32 > 1.0
    {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: format!("invalid observed/expected ratio: {}", candidate.observed_expected_ratio_f32),
        });
    }
    if !candidate.drop_ratio_f32.is_finite() || candidate.drop_ratio_f32 < 0.0 || candidate.drop_ratio_f32 > 1.0 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: format!("invalid drop ratio: {}", candidate.drop_ratio_f32),
        });
    }
    if !candidate.absolute_drop_lines_f64.is_finite() || candidate.absolute_drop_lines_f64 <= 0.0 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "absolute_drop_lines_f64 must be finite and positive".to_string(),
        });
    }
    if candidate.maturity_count_u32 == 0 {
        return Err(AlertErrorV1::InvalidSharpDropCandidate {
            msg: "maturity_count_u32 must be positive".to_string(),
        });
    }
    match candidate.reason_details.first() {
        Some((key, value)) if key == "drop_kind" && value == "sharp_drop" => {}
        _ => {
            return Err(AlertErrorV1::InvalidSharpDropCandidate {
                msg: "first reason detail must be drop_kind=sharp_drop".to_string(),
            })
        }
    }
    Ok(())
}

fn require_reason_detail_v1(
    details: &[(String, String)],
    key: &str,
    expected_value: &str,
    context: &'static str,
) -> Result<(), AlertErrorV1> {
    if details.iter().any(|(detail_key, detail_value)| detail_key == key && detail_value == expected_value) {
        return Ok(());
    }
    let msg = format!("missing reason detail {}={} for {}", key, expected_value, context);
    if context == "sharp_drop" {
        Err(AlertErrorV1::InvalidSharpDropCandidate { msg })
    } else {
        Err(AlertErrorV1::InvalidVDropCandidate { msg })
    }
}

fn source_stream_hard_silence_reason_details_v1(
    subject: &SourceStreamSubjectV1,
    candidate: &VDropCandidateV1,
) -> Vec<(String, String)> {
    let mut details = Vec::with_capacity(candidate.reason_details.len() + 1);
    details.push(("drop_kind".to_string(), "hard_silence".to_string()));
    details.extend(candidate.reason_details.clone());
    if !details.iter().any(|(key, _)| key == "device_key") {
        details.push(("device_key".to_string(), subject.device_key.clone()));
    }
    if !details.iter().any(|(key, _)| key == "source_stream_id") {
        details.push(("source_stream_id".to_string(), subject.source_stream_id.clone()));
    }
    if !details.iter().any(|(key, _)| key == "source_path") {
        details.push(("source_path".to_string(), subject.canonical_source_path.clone()));
    }
    details
}

fn source_stream_alert_device_path_v1(subject: &SourceStreamSubjectV1) -> String {
    format!("source_stream:{}/{}", subject.device_key, subject.canonical_source_path)
}

fn compute_source_stream_vdrop_alert_id_v1(candidate: &VDropCandidateV1) -> String {
    let input = format!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}",
        candidate.tenant_id,
        "source_stream",
        candidate.subject_key,
        candidate.window_start_ts_i64,
        candidate.window_end_ts_i64,
        VDROP_REASON_CODE_V1,
        "hard_silence"
    );
    stable_hash_hex128_v1(&input)
}

fn compute_source_stream_sharp_drop_alert_id_v1(candidate: &SharpDropCandidateV1) -> String {
    let input = format!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}",
        candidate.tenant_id,
        "source_stream",
        candidate.subject_key,
        candidate.window_start_ts_i64,
        candidate.window_end_ts_i64,
        VDROP_REASON_CODE_V1,
        "sharp_drop"
    );
    stable_hash_hex128_v1(&input)
}

fn build_vdrop_summary_analyst_v1(candidate: &VDropCandidateV1) -> String {
    format!(
        "V_DROP score {:.3}. Expected {} lines over {} missed windows for {} {}; observed 0 lines since {}.",
        candidate.drop_ratio_f32,
        candidate.expected_lines_u64,
        candidate.expected_windows_missed_u64,
        subject_kind_alert_id_part_v1(candidate.subject_kind_u8),
        candidate.subject_key,
        candidate.last_seen_ts_i64
    )
}

fn build_vdrop_summary_customer_v1(candidate: &VDropCandidateV1) -> String {
    format!(
        "Expected log activity was not observed for {} {}. The last observed window ended at {}.",
        subject_kind_alert_id_part_v1(candidate.subject_kind_u8),
        candidate.subject_key,
        candidate.last_seen_ts_i64
    )
}

fn build_sharp_drop_summary_analyst_v1(candidate: &SharpDropCandidateV1) -> String {
    format!(
        "V_DROP sharp_drop score {:.3}. Expected {:.6} lines for {} {}; observed {} lines, drop ratio {:.6}.",
        candidate.drop_ratio_f32,
        candidate.expected_lines_f64,
        subject_kind_alert_id_part_v1(candidate.subject_kind_u8),
        candidate.subject_key,
        candidate.observed_lines_u64,
        candidate.drop_ratio_f32
    )
}

fn build_sharp_drop_summary_customer_v1(candidate: &SharpDropCandidateV1) -> String {
    format!(
        "Log activity dropped sharply but did not stop for {} {} during this window.",
        subject_kind_alert_id_part_v1(candidate.subject_kind_u8),
        candidate.subject_key
    )
}

fn build_source_stream_vdrop_summary_analyst_v1(
    subject: &SourceStreamSubjectV1,
    candidate: &VDropCandidateV1,
) -> String {
    format!(
        "V_DROP hard_silence score {:.3}. Expected {} lines over {} missed windows for source stream {} on device {}; observed 0 lines since {}.",
        candidate.drop_ratio_f32,
        candidate.expected_lines_u64,
        candidate.expected_windows_missed_u64,
        subject.source_stream_id,
        subject.device_key,
        candidate.last_seen_ts_i64
    )
}

fn build_source_stream_vdrop_summary_customer_v1(
    subject: &SourceStreamSubjectV1,
    candidate: &VDropCandidateV1,
) -> String {
    format!(
        "Expected log activity was not observed for source stream {} on device {}. The last observed window ended at {}.",
        subject.canonical_source_path,
        subject.device_key,
        candidate.last_seen_ts_i64
    )
}

fn build_source_stream_sharp_drop_summary_analyst_v1(
    subject: &SourceStreamSubjectV1,
    candidate: &SharpDropCandidateV1,
) -> String {
    format!(
        "V_DROP sharp_drop score {:.3}. Expected {:.6} lines for source stream {} on device {}; observed {} lines, drop ratio {:.6}.",
        candidate.drop_ratio_f32,
        candidate.expected_lines_f64,
        subject.source_stream_id,
        subject.device_key,
        candidate.observed_lines_u64,
        candidate.drop_ratio_f32
    )
}

fn build_source_stream_sharp_drop_summary_customer_v1(
    subject: &SourceStreamSubjectV1,
    _candidate: &SharpDropCandidateV1,
) -> String {
    format!(
        "Log activity dropped sharply but did not stop for source stream {} on device {} during this window.",
        subject.canonical_source_path,
        subject.device_key
    )
}

pub fn encode_alert_v1(alert: &AlertV1) -> Result<Vec<u8>, AlertErrorV1> {
    postcard::to_stdvec(alert).map_err(|e| AlertErrorV1::Postcard { msg: e.to_string() })
}

pub fn decode_alert_v1(bytes: &[u8]) -> Result<AlertV1, AlertErrorV1> {
    postcard::from_bytes(bytes).map_err(|e| AlertErrorV1::Postcard { msg: e.to_string() })
}

fn validate_alert_scoring_config_v1(cfg: &AlertScoringConfigV1) -> Result<(), AlertErrorV1> {
    if !cfg.outlier_threshold.is_finite() || cfg.outlier_threshold < 0.0 || cfg.outlier_threshold > 1.0 {
        return Err(AlertErrorV1::InvalidOutlierThreshold {
            value: cfg.outlier_threshold,
        });
    }
    if !cfg.noise_threshold.is_finite() || cfg.noise_threshold < 0.0 || cfg.noise_threshold > 1.0 {
        return Err(AlertErrorV1::InvalidNoiseThreshold {
            value: cfg.noise_threshold,
        });
    }
    if !cfg.info_threshold.is_finite() || cfg.info_threshold < 0.0 || cfg.info_threshold > 1.0 {
        return Err(AlertErrorV1::InvalidInfoThreshold {
            value: cfg.info_threshold,
        });
    }
    if !cfg.drift_min.is_finite() || cfg.drift_min < 0.0 || cfg.drift_min > 1.0 {
        return Err(AlertErrorV1::InvalidDriftMin { value: cfg.drift_min });
    }
    if !cfg.blob_ratio_high.is_finite() || cfg.blob_ratio_high < 0.0 || cfg.blob_ratio_high > 1.0 {
        return Err(AlertErrorV1::InvalidBlobRatioHigh {
            value: cfg.blob_ratio_high,
        });
    }
    if !cfg.volume_z_max.is_finite() || cfg.volume_z_max <= 0.0 {
        return Err(AlertErrorV1::InvalidVolumeZMax {
            value: cfg.volume_z_max,
        });
    }
    if cfg.cold_start_days > 0 && cfg.cold_start_min_windows == 0 {
        return Err(AlertErrorV1::InvalidColdStartMinWindows {
            value: cfg.cold_start_min_windows,
        });
    }
    if cfg.top_features_cap == 0 {
        return Err(AlertErrorV1::InvalidTopFeaturesCap {
            value: cfg.top_features_cap,
        });
    }
    Ok(())
}

fn map_centroid_error_v1(value: crate::baseline::CentroidStatsErrorV1) -> AlertErrorV1 {
    match value {
        crate::baseline::CentroidStatsErrorV1::MissingFeatureString { feature_id } => {
            AlertErrorV1::MissingFeatureString { feature_id }
        }
        other => AlertErrorV1::Postcard {
            msg: format!("unexpected centroid weighting error: {:?}", other),
        },
    }
}

fn choose_label_v1(
    score_total: f32,
    drift: f32,
    volume: f32,
    cold_start: bool,
    entity_focus: bool,
    blob_ratio: f32,
    cfg: &AlertScoringConfigV1,
) -> u8 {
    if !cold_start
        && score_total >= cfg.outlier_threshold
        && (entity_focus || drift >= cfg.drift_min)
    {
        ALERT_KIND_OUTLIER_V1
    } else if !cold_start
        && score_total >= cfg.noise_threshold
        && blob_ratio >= cfg.blob_ratio_high
        && !entity_focus
    {
        ALERT_KIND_NOISE_V1
    } else if score_total >= cfg.info_threshold || (cold_start && volume >= VOLUME_EXTREME_THRESHOLD_V1) {
        ALERT_KIND_INFO_V1
    } else {
        ALERT_KIND_NONE_V1
    }
}

fn label_from_kind_v1(kind: u8) -> LabelV1 {
    match kind {
        ALERT_KIND_OUTLIER_V1 => LabelV1::Outlier,
        ALERT_KIND_NOISE_V1 => LabelV1::NoiseSuspect,
        ALERT_KIND_INFO_V1 => LabelV1::Info,
        _ => unreachable!(),
    }
}

fn choose_confidence_v1(
    label: LabelV1,
    cold_start: bool,
    score_total: f32,
    reason_count: usize,
    entity_focus: bool,
    cfg: &AlertScoringConfigV1,
) -> ConfidenceV1 {
    if !cold_start && label == LabelV1::Outlier && reason_count >= 2 && entity_focus {
        ConfidenceV1::High
    } else if !cold_start && score_total >= cfg.info_threshold {
        ConfidenceV1::Medium
    } else {
        ConfidenceV1::Low
    }
}

fn build_reasons_v1(
    top_feature_scores: &[FeatureScoreV1],
    n_bucket: u32,
    drift: f32,
    volume: f32,
    blob_ratio: f32,
    entity_focus: bool,
    label_kind: u8,
) -> Vec<ReasonV1> {
    let mut reasons = Vec::new();

    if top_feature_scores.iter().any(|feature| feature.df_count == 0) {
        reasons.push(ReasonV1 {
            code: "R_NEW_FEATURE".to_string(),
            msg: "one or more top features are new in this time bucket".to_string(),
            details: vec![("n_bucket".to_string(), n_bucket.to_string())],
        });
    }

    if n_bucket > 0
        && top_feature_scores.iter().any(|feature| {
            let df_ratio = f64::from(feature.df_count) / f64::from(n_bucket);
            df_ratio > 0.0 && df_ratio < f64::from(RARE_FEATURE_RATIO_THRESHOLD_V1)
        })
    {
        reasons.push(ReasonV1 {
            code: "R_RARE_FEATURE".to_string(),
            msg: "one or more top features are rare for this time bucket".to_string(),
            details: vec![("n_bucket".to_string(), n_bucket.to_string())],
        });
    }

    if drift > DRIFT_HIGH_REASON_THRESHOLD_V1 {
        reasons.push(ReasonV1 {
            code: "D_HIGH_DRIFT".to_string(),
            msg: "the current feature mix is far from the device baseline".to_string(),
            details: vec![("drift".to_string(), format_float3_v1(drift))],
        });
    } else if drift > DRIFT_MED_REASON_THRESHOLD_V1 {
        reasons.push(ReasonV1 {
            code: "D_MED_DRIFT".to_string(),
            msg: "the current feature mix differs from the device baseline".to_string(),
            details: vec![("drift".to_string(), format_float3_v1(drift))],
        });
    }

    if volume > VOLUME_EXTREME_THRESHOLD_V1 {
        reasons.push(ReasonV1 {
            code: "V_EXTREME".to_string(),
            msg: "window volume is extremely high for this device and bucket".to_string(),
            details: vec![("volume".to_string(), format_float3_v1(volume))],
        });
    } else if volume > VOLUME_SPIKE_THRESHOLD_V1 {
        reasons.push(ReasonV1 {
            code: "V_SPIKE".to_string(),
            msg: "window volume is elevated for this device and bucket".to_string(),
            details: vec![("volume".to_string(), format_float3_v1(volume))],
        });
    }

    if blob_ratio > BLOB_RATIO_HIGH_DEFAULT_V1 && !entity_focus {
        reasons.push(ReasonV1 {
            code: "N_HIGH_CARDINALITY".to_string(),
            msg: "high-cardinality blob-like features dominate this window".to_string(),
            details: vec![("blob_ratio".to_string(), format_float3_v1(blob_ratio))],
        });
    }

    if label_kind == ALERT_KIND_OUTLIER_V1 && entity_focus {
        reasons.push(ReasonV1 {
            code: "O_ENTITY_FOCUSED".to_string(),
            msg: "top features align with captured entities for this window".to_string(),
            details: Vec::new(),
        });
    }

    reasons
}

fn select_top_feature_scores_v1(feature_scores: &[FeatureScoreV1], cap: usize) -> Vec<FeatureScoreV1> {
    let mut scores = feature_scores.to_vec();
    scores.sort_by(|a, b| {
        b.contrib
            .total_cmp(&a.contrib)
            .then_with(|| a.feature.as_bytes().cmp(b.feature.as_bytes()))
    });
    if scores.len() > cap {
        scores.truncate(cap);
    }
    scores
}

fn build_entities_v1(snapshot: &EntitySketchSnapshotV1) -> EntitiesV1 {
    EntitiesV1 {
        src_ips: counted_strings_sorted_v1(&snapshot.srcips),
        dst_ips: counted_strings_sorted_v1(&snapshot.dstips),
        user_ids: counted_strings_sorted_v1(&snapshot.userids),
        domains: counted_strings_sorted_v1(&snapshot.domains),
        hosts: counted_strings_sorted_v1(&snapshot.hosts),
    }
}

fn counted_strings_sorted_v1(entries: &[crate::db::open_window::TopKStringEntryV1]) -> Vec<CountedStringV1> {
    let mut out: Vec<CountedStringV1> = entries
        .iter()
        .map(|entry| CountedStringV1 {
            value: entry.value.clone(),
            count: entry.count,
        })
        .collect();
    out.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.value.as_bytes().cmp(b.value.as_bytes())));
    out
}

fn compute_entity_focus_v1(top_features: &[TopFeatureV1], entities: &EntitiesV1) -> bool {
    top_features.iter().any(|feature| {
        if is_source_ip_feature_v1(&feature.feature) {
            !entities.src_ips.is_empty()
        } else if is_dest_ip_feature_v1(&feature.feature) {
            !entities.dst_ips.is_empty()
        } else if is_user_feature_v1(&feature.feature) {
            !entities.user_ids.is_empty()
        } else if is_host_feature_v1(&feature.feature) {
            !entities.hosts.is_empty()
        } else {
            false
        }
    })
}

fn build_summary_analyst_v1(
    label: LabelV1,
    score_total: f32,
    reasons: &[ReasonV1],
    top_features: &[TopFeatureV1],
) -> String {
    let label_text = label_text_v1(label);
    let top_list = feature_list_text_v1(top_features, 3);
    let reason_list = reason_code_list_text_v1(reasons, 3);
    if top_list.is_empty() && reason_list.is_empty() {
        format!("{} score {:.3}.", label_text, score_total)
    } else if reason_list.is_empty() {
        format!("{} score {:.3}. Top features: {}.", label_text, score_total, top_list)
    } else if top_list.is_empty() {
        format!("{} score {:.3}. Reasons: {}.", label_text, score_total, reason_list)
    } else {
        format!(
            "{} score {:.3}. Reasons: {}. Top features: {}.",
            label_text, score_total, reason_list, top_list
        )
    }
}

fn build_summary_customer_v1(
    label: LabelV1,
    reasons: &[ReasonV1],
    top_features: &[TopFeatureV1],
    entities: &EntitiesV1,
) -> String {
    let label_text = match label {
        LabelV1::Outlier => "An unusual pattern was observed in this log window",
        LabelV1::NoiseSuspect => "A likely noisy pattern was observed in this log window",
        LabelV1::Info => "A notable pattern was observed in this log window",
    };
    let lead_entity = primary_entity_text_v1(entities);
    let lead_feature = top_features.first().map(|feature| feature.feature.as_str()).unwrap_or("");
    let lead_reason = reasons.first().map(|reason| reason.msg.as_str()).unwrap_or("");

    let mut parts = vec![label_text.to_string()];
    if !lead_reason.is_empty() {
        parts.push(lead_reason.to_string());
    }
    if !lead_entity.is_empty() {
        parts.push(format!("key entity: {}", lead_entity));
    } else if !lead_feature.is_empty() {
        parts.push(format!("top signal: {}", lead_feature));
    }
    parts.join(". ") + "."
}

fn feature_list_text_v1(top_features: &[TopFeatureV1], cap: usize) -> String {
    top_features
        .iter()
        .take(cap)
        .map(|feature| feature.feature.clone())
        .collect::<Vec<String>>()
        .join(", ")
}

fn reason_code_list_text_v1(reasons: &[ReasonV1], cap: usize) -> String {
    reasons
        .iter()
        .take(cap)
        .map(|reason| reason.code.clone())
        .collect::<Vec<String>>()
        .join(", ")
}

fn primary_entity_text_v1(entities: &EntitiesV1) -> String {
    if let Some(value) = entities.user_ids.first() {
        return value.value.clone();
    }
    if let Some(value) = entities.src_ips.first() {
        return value.value.clone();
    }
    if let Some(value) = entities.dst_ips.first() {
        return value.value.clone();
    }
    if let Some(value) = entities.hosts.first() {
        return value.value.clone();
    }
    if let Some(value) = entities.domains.first() {
        return value.value.clone();
    }
    String::new()
}

fn label_text_v1(label: LabelV1) -> &'static str {
    match label {
        LabelV1::Outlier => "outlier",
        LabelV1::NoiseSuspect => "noise_suspect",
        LabelV1::Info => "info",
    }
}

fn cap_provenance_v1(spans: &[FileSpanV1], cap: usize) -> Vec<FileSpanV1> {
    let mut out = spans.to_vec();
    out.sort_by(|a, b| {
        a.file_rel
            .as_bytes()
            .cmp(b.file_rel.as_bytes())
            .then_with(|| a.offset_start.cmp(&b.offset_start))
            .then_with(|| a.offset_end.cmp(&b.offset_end))
            .then_with(|| a.file_key.as_bytes().cmp(b.file_key.as_bytes()))
            .then_with(|| a.inode.cmp(&b.inode))
            .then_with(|| a.is_gzip.cmp(&b.is_gzip))
    });
    out.dedup_by(|a, b| {
        a.file_rel == b.file_rel
            && a.file_key == b.file_key
            && a.inode == b.inode
            && a.offset_start == b.offset_start
            && a.offset_end == b.offset_end
            && a.is_gzip == b.is_gzip
    });
    if out.len() > cap {
        out.truncate(cap);
    }
    out
}

fn compute_alert_signature_v1(top_features: &[TopFeatureV1]) -> String {
    let mut input = String::new();
    for feature in top_features {
        input.push_str(&feature.feature);
        input.push('\t');
        input.push_str(&feature.count.to_string());
        input.push('\n');
    }
    stable_hash_hex128_v1(&input)
}

fn compute_alert_id_v1(tenant_id: &str, device_key: &str, window_start_ts: i64, signature: &str) -> String {
    let input = format!("{}\t{}\t{}\t{}", tenant_id, device_key, window_start_ts, signature);
    stable_hash_hex128_v1(&input)
}

fn volume_score_v1(row: &FinalizedWindowRowV1, stats: Option<&DeviceStatsV1>, z_max: f32) -> f32 {
    let Some(stats) = stats else {
        return 0.0;
    };
    let z_lines = positive_z_score_v1(f64::from(row.meta.lines), &stats.line_count);
    let z_bytes = positive_z_score_v1(row.meta.bytes as f64, &stats.byte_count);
    let max_z = z_lines.max(z_bytes);
    (max_z / f64::from(z_max)).clamp(0.0, 1.0) as f32
}

fn positive_z_score_v1(value: f64, state: &WelfordF64V1) -> f64 {
    if state.n < 2 {
        return 0.0;
    }
    let variance = state.m2 / f64::from(state.n.saturating_sub(1));
    if !variance.is_finite() || variance <= EPSILON_F64_V1 {
        return 0.0;
    }
    let stddev = variance.sqrt();
    if stddev <= EPSILON_F64_V1 {
        return 0.0;
    }
    ((value - state.mean) / stddev).max(0.0)
}

fn cosine_drift_v1(
    row_map: &BTreeMap<u32, f32>,
    row_norm: f64,
    centroid_map: &BTreeMap<u32, f32>,
    centroid_norm: f64,
) -> f32 {
    if row_norm <= EPSILON_F64_V1 || centroid_norm <= EPSILON_F64_V1 {
        return 0.0;
    }
    let mut dot = 0.0f64;
    for (feature_id, row_value) in row_map {
        if let Some(centroid_value) = centroid_map.get(feature_id) {
            dot += f64::from(*row_value) * f64::from(*centroid_value);
        }
    }
    let cos = (dot / (row_norm * centroid_norm)).clamp(-1.0, 1.0);
    (1.0 - cos) as f32
}

fn centroid_value_pairs_to_map_v1(
    pairs: &[crate::db::baseline_sketch::CentroidValuePairV1],
) -> BTreeMap<u32, f32> {
    let mut out = BTreeMap::new();
    for pair in pairs {
        out.insert(pair.feature_id, pair.value);
    }
    out
}

fn baseline_centroid_pairs_to_map_v1(
    pairs: &[crate::baseline::CentroidPairV1],
) -> BTreeMap<u32, f32> {
    let mut out = BTreeMap::new();
    for pair in pairs {
        out.insert(pair.feature_id, pair.value);
    }
    out
}

fn df_pairs_to_map_v1(pairs: &[crate::baseline::DfPairV1]) -> BTreeMap<u32, u32> {
    let mut out = BTreeMap::new();
    for pair in pairs {
        out.insert(pair.feature_id, pair.df_count);
    }
    out
}

fn l2_norm_v1(map: &BTreeMap<u32, f32>) -> f64 {
    map.values()
        .map(|value| {
            let v = f64::from(*value);
            v * v
        })
        .sum::<f64>()
        .sqrt()
}


fn family_from_feature_v1(feature: &str) -> FeatureFamilyV1 {
    if feature.starts_with("k=") {
        FeatureFamilyV1::KeyPres
    } else if feature.starts_with("canon=") {
        FeatureFamilyV1::Canon
    } else if feature.starts_with("syslog_") {
        FeatureFamilyV1::Syslog
    } else if feature.starts_with("w=") {
        FeatureFamilyV1::Word
    } else if feature.contains("_net@") {
        FeatureFamilyV1::Bucket
    } else {
        FeatureFamilyV1::Shape
    }
}

fn is_blob_feature_v1(feature: &str) -> bool {
    feature.contains("<UUID>") || feature.contains("<HEX_") || feature.contains("<B64_")
}

fn is_source_ip_feature_v1(feature: &str) -> bool {
    feature.starts_with("SourceIp") || feature == "canon=SourceIp"
}

fn is_dest_ip_feature_v1(feature: &str) -> bool {
    feature.starts_with("DestIp") || feature == "canon=DestIp"
}

fn is_user_feature_v1(feature: &str) -> bool {
    feature.starts_with("User=") || feature == "canon=User"
}

fn is_host_feature_v1(feature: &str) -> bool {
    feature.starts_with("SourceHost")
        || feature.starts_with("DestHost")
        || feature == "canon=SourceHost"
        || feature == "canon=DestHost"
}

fn format_float3_v1(value: f32) -> String {
    format!("{:.3}", value)
}

#[derive(Clone, Debug)]
struct FeatureScoreV1 {
    feature_id: u32,
    feature: String,
    count: u32,
    family: FeatureFamilyV1,
    tf_w: f32,
    idf: f32,
    contrib: f32,
    df_count: u32,
}
