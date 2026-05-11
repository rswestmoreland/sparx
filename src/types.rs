// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Shared primitives and small enums used across the crate.
// Shared types only; no runtime logic in this module.

pub type UnixSec = i64;
pub type DeviceKey = String;
pub type TenantId = String;
pub type AlertId = String;
pub type FeatureId = u32;

// Time bucket index (0..47) for weekday/weekend x hour.
pub type BaselineBucket = u8;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LabelV1 {
    Outlier,
    NoiseSuspect,
    Info,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfidenceV1 {
    High,
    Medium,
    Low,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeatureFamilyV1 {
    #[serde(rename = "KEYPRES")]
    KeyPres,
    #[serde(rename = "CANON")]
    Canon,
    #[serde(rename = "SHAPE")]
    Shape,
    #[serde(rename = "BUCKET")]
    Bucket,
    #[serde(rename = "SYSLOG")]
    Syslog,
    #[serde(rename = "WORD")]
    Word,
}
