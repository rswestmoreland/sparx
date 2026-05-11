// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// CLI command model.
// See: contracts/07_cli_contract_v0_1.md
// Defines the command shapes consumed by the manual parser and routing layer.
// ASCII-only.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrateModeV1 {
    Auto,
    Off,
    Require,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlertCategoryFilterV1 {
    Outlier,
    NoiseSuspect,
    Info,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlertEntityKindFilterV1 {
    SrcIp,
    DstIp,
    UserId,
    Domain,
    Host,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandV1 {
    Run {
        migrate: MigrateModeV1,
    },
    OneShot {
        tenant_id: String,
        since: Option<i64>,
        until: Option<i64>,
        device_path: Option<String>,
        migrate: MigrateModeV1,
    },
    Status {
        json: bool,
    },
    Version,
    TenantPurge {
        tenant_id: String,
        force: bool,
    },
    ConfigCheck,
    ReplaySpool {
        tenant_id: Option<String>,
    },
    ValidateFixtures {
        fixture_root: String,
    },

    // Additional subcommands (detailed behavior in their contracts):
    TenantPolicyShow {
        tenant_id: String,
    },
    TenantPolicyCheck {
        tenant_id: String,
    },

    // Migrations
    MigrateTenant {
        tenant_id: String,
    },
    MigrateAll,

    // Alerts query/export
    AlertsList {
        tenant_id: String,
        since: Option<i64>,
        until: Option<i64>,
        category: Option<AlertCategoryFilterV1>,
        entity_kind: Option<AlertEntityKindFilterV1>,
        entity_value: Option<String>,
        json: bool,
    },
    AlertsShow {
        tenant_id: String,
        alert_id: String,
        json: bool,
    },
    AlertsSearch {
        tenant_id: String,
        since: Option<i64>,
        until: Option<i64>,
        category: Option<AlertCategoryFilterV1>,
        entity_kind: Option<AlertEntityKindFilterV1>,
        entity_value: Option<String>,
        contains: String,
    },
    AlertsExport {
        tenant_id: String,
        category: Option<AlertCategoryFilterV1>,
        entity_kind: Option<AlertEntityKindFilterV1>,
        entity_value: Option<String>,
        out_path: String,
        gzip: bool,
    },

    // Drilldown
    AlertExtract {
        tenant_id: String,
        alert_id: String,
        out_path: String,
        max_bytes: Option<u64>,
        max_lines: Option<u64>,
    },
    AlertDrill {
        tenant_id: String,
        alert_id: String,
        max_bytes: Option<u64>,
        max_lines: Option<u64>,
    },
}

pub mod parse;
pub mod route;
