// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Feature emission and sparse row types.
// See: contracts/24_feature_emission_catalog_v0_1.md and contracts/05_feature_id_strategy_v0_1.md

pub mod dict;
pub mod emit;
pub mod sketch;

use crate::types::{FeatureFamilyV1, FeatureId};

pub use dict::{
    FeatureDictionaryConfigV1, FeatureDictionaryErrorV1, FeatureDictionaryKvV1,
    FeatureDictionaryMetaV1, FeatureDictionaryResolveV1, FeatureDictionaryV1,
};

pub use emit::{
    classify_key_v1, emit_line_features_v1, normalize_key_v1, normalize_user_identity_v1,
    normalize_word_feature_v1, FeatureEmissionLineV1, MetadataIdentityKindV1, MetadataIdentityV1,
    SemanticCategoryV1, SemanticMatchV1, UserIdentityV1, UserKindV1,
};
pub use sketch::{
    EntitySketchCapsV1, EntitySketchKindV1, EntitySketchKvV1, EntitySketchSnapshotV1,
    EntitySketchesV1,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeatureStringV1 {
    pub s: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeatureCountV1 {
    pub feature_id: FeatureId,
    pub count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SparseRowV1 {
    // Sparse counts by FeatureId; ordering and encoding rules are defined in contracts.
    pub counts: Vec<FeatureCountV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmittedFeatureV1 {
    pub feature: FeatureStringV1,
    pub family: FeatureFamilyV1,
    pub count: u32,
}
