// Canonical filesystem layout helpers.
//
// Phase 10b introduces one authoritative path derivation layer for runtime
// commands and later DB wiring. Paths are derived only from the effective
// config plus tenant_id where applicable.
//
// See:
// - contracts/06_rocksdb_topology_v0_1.md
// - contracts/13_overrides_tenant_policy_v0_1.md
// - contracts/28_config_schema_v0_1.md
// - contracts/29_output_sink_contract_v0_1.md
// - contracts/32_service_and_deployment_contract_v0_1.md

use std::path::PathBuf;

use crate::config::ConfigV1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilesystemLayoutV1 {
    data_root: PathBuf,
    tenant_root: PathBuf,
    global_db_path: PathBuf,
    tenant_db_root: PathBuf,
    alert_out_root: PathBuf,
}

impl FilesystemLayoutV1 {
    pub fn from_config_v1(cfg: &ConfigV1) -> Self {
        Self {
            data_root: PathBuf::from(&cfg.sparx.data_root),
            tenant_root: PathBuf::from(&cfg.sparx.tenant_root),
            global_db_path: PathBuf::from(&cfg.sparx.global_db_path),
            tenant_db_root: PathBuf::from(&cfg.sparx.tenant_db_root),
            alert_out_root: PathBuf::from(&cfg.sparx.alert_out_root),
        }
    }

    pub fn data_root_v1(&self) -> PathBuf {
        self.data_root.clone()
    }

    pub fn tenant_root_v1(&self) -> PathBuf {
        self.tenant_root.clone()
    }

    pub fn global_db_path_v1(&self) -> PathBuf {
        self.global_db_path.clone()
    }

    pub fn tenant_db_root_v1(&self) -> PathBuf {
        self.tenant_db_root.clone()
    }

    pub fn alert_out_root_v1(&self) -> PathBuf {
        self.alert_out_root.clone()
    }

    pub fn spool_root_v1(&self) -> PathBuf {
        self.data_root.join("spool").join("alerts")
    }

    pub fn tenant_db_dir_v1(&self, tenant_id: &str) -> PathBuf {
        self.tenant_db_root
            .join(format!("tenant={}", tenant_id))
            .join("tenant.db")
    }

    pub fn tenant_alert_dir_v1(&self, tenant_id: &str) -> PathBuf {
        self.alert_out_root.join(format!("tenant={}", tenant_id))
    }

    pub fn tenant_spool_dir_v1(&self, tenant_id: &str) -> PathBuf {
        self.spool_root_v1().join(format!("tenant={}", tenant_id))
    }

    pub fn tenant_policy_path_v1(&self, tenant_id: &str) -> PathBuf {
        self.tenant_root
            .join(tenant_id)
            .join(".sparx")
            .join("policy.toml")
    }
}

pub fn filesystem_layout_v1(cfg: &ConfigV1) -> FilesystemLayoutV1 {
    FilesystemLayoutV1::from_config_v1(cfg)
}
