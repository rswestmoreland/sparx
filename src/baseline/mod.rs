// Baseline state types and DF ring helpers.
// See: contracts/22_baseline_sketch_encoding_v0_1.md and contracts/21_scoring_math_thresholding_v0_1.md

pub mod centroid_stats;
pub mod df_ring;

use crate::types::{BaselineBucket, FeatureId};

pub use centroid_stats::{
    plan_centroid_stats_update_v1, weighted_row_vector_v1, CentroidStatsConfigV1,
    CentroidStatsErrorV1, CentroidStatsKvV1, CentroidStatsMutationV1, CentroidStatsUpdatePlanV1,
    CENTROID_CAP_DEFAULT_V1,
};

pub use df_ring::{
    day_epoch_for_ts_v1, plan_df_ring_update_v1, slot_for_day_epoch_v1, DfRingConfigV1,
    DfRingErrorV1, DfRingKvV1, DfRingMetaStateV1, DfRingMutationV1, DfRingSlotBucketStateV1,
    DfRingUpdatePlanV1, DF_BUCKET_COUNT_DEFAULT_V1, DF_MAP_CAP_DEFAULT_V1,
    DF_RING_SLOTS_DEFAULT_V1,
};

#[derive(Clone, Debug)]
pub struct DfPairV1 {
    pub feature_id: FeatureId,
    pub df_count: u32,
}

#[derive(Clone, Debug)]
pub struct CentroidPairV1 {
    pub feature_id: FeatureId,
    pub value: f32,
}

#[derive(Clone, Debug)]
pub struct BucketBaselineV1 {
    pub bucket: BaselineBucket,
    pub n_bucket: u32,
    pub df: Vec<DfPairV1>,
    pub centroid: Vec<CentroidPairV1>,
}
