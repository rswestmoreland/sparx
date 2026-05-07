// CLI routing.
// Phase 10a hardens dispatch so config-free commands bypass config load
// and unimplemented operational commands fail closed.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flate2::write::GzEncoder;
use flate2::Compression;
use crate::alert::{AlertScoringConfigV1, AlertV1, FileSpanV1, build_alert_v1};
use crate::drilldown::{drill_alert_v1, extract_alert_v1};
use crate::config::ConfigV1;
use crate::db::layout::filesystem_layout_v1;
use crate::db::tenant_values::{
    decode_feat_dict_id_to_str_v1, decode_feat_dict_meta_entries_v1, decode_feat_dict_meta_last_gc_ts_v1,
    decode_feat_dict_meta_next_id_v1, decode_feat_dict_str_to_id_v1,
    decode_meta_df_ring_current_day_epoch_v1, decode_meta_df_ring_day_slot_epoch_v1,
};
use crate::db::DbErrorV1;
use crate::policy::{
    load_tenant_policy_v1, tenant_policy_path_parts_v1, TenantPolicyLoadErrorKindV1, TenantPolicyV1,
};
use crate::features::{emit_line_features_v1, FeatureDictionaryConfigV1, FeatureDictionaryMetaV1, FeatureDictionaryV1};
use crate::ingest::{
    apply_cursor_read_progress_v1, discover_device_inventory_v1, open_file_reader_v1, reconcile_cursor_v1,
    CursorPlanV1, DiscoveredFileV1, FileCursorV1, ObservedFileStateV1, TenantDeviceV1,
};
use crate::runtime::{
    GlobalSchemaMigrateResultV1, MigrateAllResultV1, SchemaMigrateOutcomeKindV1, SparxRuntimeV1,
    TenantPurgeOutcomeKindV1, TenantPurgeResultV1, TenantSchemaMigrateResultV1,
};
use crate::observability::{
    build_status_snapshot_from_runtime_v1, format_status_text_v1, ObservabilityServersV1,
    METRIC_RUN_ALERTS_EMITTED_TOTAL_V1, METRIC_RUN_CYCLES_COMPLETED_TOTAL_V1,
    METRIC_RUN_DEVICES_FAILED_TOTAL_V1, METRIC_RUN_DEVICES_PROCESSED_TOTAL_V1,
    METRIC_RUN_LAST_CYCLE_ALERTS_EMITTED_V1, METRIC_RUN_LAST_CYCLE_COMPLETED_TS_V1,
    METRIC_RUN_LAST_CYCLE_DEVICES_FAILED_V1, METRIC_RUN_LAST_CYCLE_DEVICES_PROCESSED_V1,
    METRIC_RUN_LAST_CYCLE_TENANTS_PROCESSED_V1, METRIC_RUN_LAST_CYCLE_TENANTS_SKIPPED_V1,
    METRIC_RUN_LAST_CYCLE_TENANTS_TOTAL_V1, METRIC_RUN_TENANTS_PROCESSED_TOTAL_V1,
    METRIC_RUN_TENANTS_SKIPPED_TOTAL_V1, METRIC_RUN_TENANTS_TOTAL_V1,
};
use crate::sink::{
    read_spooled_alert_v1, sorted_spool_files_for_replay_v1, JsonlAlertSinkV1,
    JsonlSinkConfigV1, SpoolConfigV1, SpoolReplayReportV1, SpoolingJsonlAlertSinkV1,
    StdoutAlertSinkV1, SPOOL_MAX_MB_DEFAULT_V1,
};
use crate::tokenize::{parse_syslog_envelope_v1, tokenize_message_v1};
use crate::window::{align_window_start_ts_v1, WindowAccumulatorV1, WindowApplyLineResultV1, WindowCapsV1};
use crate::baseline::{
    BucketBaselineV1, CentroidPairV1, CentroidStatsConfigV1, DfPairV1, DfRingConfigV1, DfRingMetaStateV1,
    DfRingMutationV1, DfRingSlotBucketStateV1, plan_centroid_stats_update_v1, plan_df_ring_update_v1,
};
use super::{
    AlertCategoryFilterV1, AlertEntityKindFilterV1, CommandV1, MigrateModeV1,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct AlertEntityFilterV1 {
    kind: AlertEntityKindFilterV1,
    value: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
struct AlertQueryFiltersV1 {
    since: Option<i64>,
    until: Option<i64>,
    category: Option<AlertCategoryFilterV1>,
    entity: Option<AlertEntityFilterV1>,
}

#[derive(Clone, Debug)]
pub struct RouteResultV1 {
    pub exit_code: i32,
    pub msg_stdout: Option<String>,
    pub msg_stderr: Option<String>,
}

type RunTestCycleHookV1 = Arc<dyn Fn(u32, &mut SparxRuntimeV1, &ConfigV1) + Send + Sync>;

static RUN_TEST_CYCLE_HOOK_V1: OnceLock<Mutex<Option<RunTestCycleHookV1>>> = OnceLock::new();

fn build_alert_entity_filter_from_parts_v1(
    entity_kind: &Option<AlertEntityKindFilterV1>,
    entity_value: &Option<String>,
) -> Option<AlertEntityFilterV1> {
    match (entity_kind, entity_value) {
        (Some(kind), Some(value)) => Some(AlertEntityFilterV1 {
            kind: *kind,
            value: value.clone(),
        }),
        _ => None,
    }
}

#[doc(hidden)]
pub fn install_run_test_cycle_hook_v1<F>(hook: F)
where
    F: Fn(u32, &mut SparxRuntimeV1, &ConfigV1) + Send + Sync + 'static,
{
    let cell = RUN_TEST_CYCLE_HOOK_V1.get_or_init(|| Mutex::new(None));
    *cell.lock().expect("run test cycle hook lock") = Some(Arc::new(hook));
}

#[doc(hidden)]
pub fn clear_run_test_cycle_hook_v1() {
    if let Some(cell) = RUN_TEST_CYCLE_HOOK_V1.get() {
        *cell.lock().expect("run test cycle hook lock") = None;
    }
}

fn maybe_call_run_test_cycle_hook_v1(cycle_completed: u32, runtime: &mut SparxRuntimeV1, cfg: &ConfigV1) {
    let hook = RUN_TEST_CYCLE_HOOK_V1
        .get()
        .and_then(|cell| cell.lock().ok().and_then(|guard| guard.as_ref().map(Arc::clone)));
    if let Some(hook) = hook {
        hook(cycle_completed, runtime, cfg);
    }
}

pub fn command_requires_config_v1(cmd: &CommandV1) -> bool {
    match cmd {
        CommandV1::Version => false,
        CommandV1::ValidateFixtures { .. } => false,
        _ => true,
    }
}

pub fn route_command_no_config_v1(cmd: &CommandV1) -> RouteResultV1 {
    match cmd {
        CommandV1::Version => RouteResultV1 {
            exit_code: 0,
            msg_stdout: Some("sparx 0.0.0".to_string()),
            msg_stderr: None,
        },
        CommandV1::ValidateFixtures { fixture_root } => route_validate_fixtures_v1(fixture_root),
        _ => RouteResultV1 {
            exit_code: 5,
            msg_stdout: None,
            msg_stderr: Some(format!(
                "internal invariant violation: command requires config: {}",
                command_label_v1(cmd)
            )),
        },
    }
}

pub fn route_command_v1(cmd: &CommandV1, cfg: &ConfigV1) -> RouteResultV1 {
    match cmd {
        CommandV1::Run { migrate } => route_run_v1(cfg, *migrate),
        CommandV1::OneShot { tenant_id, since, until, device_path, migrate } => route_oneshot_v1(cfg, tenant_id, *since, *until, device_path.as_deref(), *migrate),
        CommandV1::Status { json } => route_status_v1(cfg, *json),
        CommandV1::ReplaySpool { tenant_id } => route_replay_spool_v1(cfg, tenant_id.as_deref()),
        CommandV1::TenantPurge { tenant_id, force } => route_tenant_purge_v1(cfg, tenant_id, *force),
        CommandV1::TenantPolicyShow { tenant_id } => route_tenant_policy_show_v1(cfg, tenant_id),
        CommandV1::TenantPolicyCheck { tenant_id } => route_tenant_policy_check_v1(cfg, tenant_id),
        CommandV1::MigrateTenant { tenant_id } => route_migrate_tenant_v1(cfg, tenant_id),
        CommandV1::MigrateAll => route_migrate_all_v1(cfg),
        CommandV1::AlertsList {
            tenant_id,
            since,
            until,
            category,
            entity_kind,
            entity_value,
            json,
        } => route_alerts_list_v1(
            cfg,
            tenant_id,
            *since,
            *until,
            *category,
            build_alert_entity_filter_from_parts_v1(entity_kind, entity_value),
            *json,
        ),
        CommandV1::AlertsShow {
            tenant_id,
            alert_id,
            json,
        } => route_alerts_show_v1(cfg, tenant_id, alert_id, *json),
        CommandV1::AlertsSearch {
            tenant_id,
            since,
            until,
            category,
            entity_kind,
            entity_value,
            contains,
        } => route_alerts_search_v1(
            cfg,
            tenant_id,
            *since,
            *until,
            *category,
            build_alert_entity_filter_from_parts_v1(entity_kind, entity_value),
            contains,
        ),
        CommandV1::AlertsExport {
            tenant_id,
            category,
            entity_kind,
            entity_value,
            out_path,
            gzip,
        } => route_alerts_export_v1(
            cfg,
            tenant_id,
            *category,
            build_alert_entity_filter_from_parts_v1(entity_kind, entity_value),
            out_path,
            *gzip,
        ),
        CommandV1::AlertExtract {
            tenant_id,
            alert_id,
            out_path,
            max_bytes,
            max_lines,
        } => route_alert_extract_v1(cfg, tenant_id, alert_id, out_path, *max_bytes, *max_lines),
        CommandV1::AlertDrill {
            tenant_id,
            alert_id,
            max_bytes,
            max_lines,
        } => route_alert_drill_v1(cfg, tenant_id, alert_id, *max_bytes, *max_lines),
        CommandV1::ConfigCheck => route_config_check_v1(cfg),
        CommandV1::Version | CommandV1::ValidateFixtures { .. } => route_command_no_config_v1(cmd),
    }
}

fn command_label_v1(cmd: &CommandV1) -> &'static str {
    match cmd {
        CommandV1::Run { .. } => "run",
        CommandV1::OneShot { .. } => "oneshot",
        CommandV1::Status { .. } => "status",
        CommandV1::Version => "version",
        CommandV1::TenantPurge { .. } => "tenant purge",
        CommandV1::ConfigCheck => "config check",
        CommandV1::ReplaySpool { .. } => "replay-spool",
        CommandV1::ValidateFixtures { .. } => "validate-fixtures",
        CommandV1::TenantPolicyShow { .. } => "tenant policy show",
        CommandV1::TenantPolicyCheck { .. } => "tenant policy check",
        CommandV1::MigrateTenant { .. } => "migrate --tenant",
        CommandV1::MigrateAll => "migrate --all",
        CommandV1::AlertsList { .. } => "alerts list",
        CommandV1::AlertsShow { .. } => "alerts show",
        CommandV1::AlertsSearch { .. } => "alerts search",
        CommandV1::AlertsExport { .. } => "alerts export",
        CommandV1::AlertExtract { .. } => "alert extract",
        CommandV1::AlertDrill { .. } => "alert drill",
    }
}


fn route_tenant_purge_v1(cfg: &ConfigV1, tenant_id: &str, force: bool) -> RouteResultV1 {
    let mut runtime = match SparxRuntimeV1::open_from_config_v1(cfg) {
        Ok(runtime) => runtime,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("tenant purge db error: {}", e)),
            };
        }
    };

    let now_ts = current_unix_ts_v1();
    let result = match runtime.purge_tenant_v1(tenant_id, force, now_ts) {
        Ok(result) => result,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("tenant purge db error: {}", e)),
            };
        }
    };

    route_tenant_purge_result_v1(&result)
}


fn route_tenant_policy_show_v1(cfg: &ConfigV1, tenant_id: &str) -> RouteResultV1 {
    let layout = filesystem_layout_v1(cfg);
    let (tenant_dir, policy_path) = tenant_policy_path_parts_v1(&layout.tenant_root_v1(), tenant_id);
    match load_tenant_policy_v1(&tenant_dir, &policy_path) {
        Ok(policy) => RouteResultV1 {
            exit_code: 0,
            msg_stdout: Some(format_tenant_policy_show_v1(tenant_id, &policy_path, &policy)),
            msg_stderr: None,
        },
        Err(err) => route_tenant_policy_error_v1("show", tenant_id, &policy_path, err.kind, err.details),
    }
}

fn route_tenant_policy_check_v1(cfg: &ConfigV1, tenant_id: &str) -> RouteResultV1 {
    let layout = filesystem_layout_v1(cfg);
    let (tenant_dir, policy_path) = tenant_policy_path_parts_v1(&layout.tenant_root_v1(), tenant_id);
    match load_tenant_policy_v1(&tenant_dir, &policy_path) {
        Ok(policy) => RouteResultV1 {
            exit_code: 0,
            msg_stdout: Some(format_tenant_policy_check_v1(tenant_id, &policy_path, &policy)),
            msg_stderr: None,
        },
        Err(err) => route_tenant_policy_error_v1("check", tenant_id, &policy_path, err.kind, err.details),
    }
}


fn route_alerts_list_v1(
    cfg: &ConfigV1,
    tenant_id: &str,
    since: Option<i64>,
    until: Option<i64>,
    category: Option<AlertCategoryFilterV1>,
    entity: Option<AlertEntityFilterV1>,
    json: bool,
) -> RouteResultV1 {
    let mut runtime = match SparxRuntimeV1::open_from_config_v1(cfg) {
        Ok(runtime) => runtime,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("alerts list db error: {}", e)),
            };
        }
    };

    let filters = AlertQueryFiltersV1 {
        since,
        until,
        category,
        entity,
    };

    let alerts = match load_filtered_alerts_v1(&mut runtime, tenant_id, &filters) {
        Ok(alerts) => alerts,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("alerts list db error: {}", e)),
            };
        }
    };

    let stdout = if json {
        match serde_json::to_string(&serde_json::json!({
            "tenant_id": tenant_id,
            "count": alerts.len(),
            "filters": alert_query_filters_json_v1(&filters),
            "alerts": alerts,
        })) {
            Ok(s) => s,
            Err(e) => {
                return RouteResultV1 {
                    exit_code: 4,
                    msg_stdout: None,
                    msg_stderr: Some(format!("alerts list json error: {}", e)),
                };
            }
        }
    } else {
        format_alert_list_text_v1("alerts list", tenant_id, &alerts, &filters)
    };

    RouteResultV1 {
        exit_code: 0,
        msg_stdout: Some(stdout),
        msg_stderr: None,
    }
}

fn route_alerts_show_v1(cfg: &ConfigV1, tenant_id: &str, alert_id: &str, json: bool) -> RouteResultV1 {
    let mut runtime = match SparxRuntimeV1::open_from_config_v1(cfg) {
        Ok(runtime) => runtime,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("alerts show db error: {}", e)),
            };
        }
    };

    let alert = match load_alert_by_id_v1(&mut runtime, tenant_id, alert_id) {
        Ok(Some(alert)) => alert,
        Ok(None) => {
            return RouteResultV1 {
                exit_code: 1,
                msg_stdout: None,
                msg_stderr: Some(format!("alert not found: {}", alert_id)),
            };
        }
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("alerts show db error: {}", e)),
            };
        }
    };

    let stdout = if json {
        match serde_json::to_string(&serde_json::json!({
            "tenant_id": tenant_id,
            "alert": alert,
        })) {
            Ok(s) => s,
            Err(e) => {
                return RouteResultV1 {
                    exit_code: 4,
                    msg_stdout: None,
                    msg_stderr: Some(format!("alerts show json error: {}", e)),
                };
            }
        }
    } else {
        format_alert_show_text_v1(tenant_id, &alert)
    };

    RouteResultV1 {
        exit_code: 0,
        msg_stdout: Some(stdout),
        msg_stderr: None,
    }
}

fn route_alerts_search_v1(
    cfg: &ConfigV1,
    tenant_id: &str,
    since: Option<i64>,
    until: Option<i64>,
    category: Option<AlertCategoryFilterV1>,
    entity: Option<AlertEntityFilterV1>,
    contains: &str,
) -> RouteResultV1 {
    let mut runtime = match SparxRuntimeV1::open_from_config_v1(cfg) {
        Ok(runtime) => runtime,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("alerts search db error: {}", e)),
            };
        }
    };

    let filters = AlertQueryFiltersV1 {
        since,
        until,
        category,
        entity,
    };

    let mut alerts = match load_filtered_alerts_v1(&mut runtime, tenant_id, &filters) {
        Ok(alerts) => alerts,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("alerts search db error: {}", e)),
            };
        }
    };
    alerts.retain(|alert| alert_contains_text_v1(alert, contains));

    RouteResultV1 {
        exit_code: 0,
        msg_stdout: Some(format_alert_search_text_v1(tenant_id, contains, &alerts, &filters)),
        msg_stderr: None,
    }
}

fn route_alerts_export_v1(
    cfg: &ConfigV1,
    tenant_id: &str,
    category: Option<AlertCategoryFilterV1>,
    entity: Option<AlertEntityFilterV1>,
    out_path: &str,
    gzip: bool,
) -> RouteResultV1 {
    let mut runtime = match SparxRuntimeV1::open_from_config_v1(cfg) {
        Ok(runtime) => runtime,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("alerts export db error: {}", e)),
            };
        }
    };

    let filters = AlertQueryFiltersV1 {
        since: None,
        until: None,
        category,
        entity,
    };

    let alerts = match load_filtered_alerts_v1(&mut runtime, tenant_id, &filters) {
        Ok(alerts) => alerts,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("alerts export db error: {}", e)),
            };
        }
    };

    let path = std::path::Path::new(out_path);
    if let Some(parent) = path.parent() {
        if let Err(e) = fs::create_dir_all(parent) {
            return RouteResultV1 {
                exit_code: 3,
                msg_stdout: None,
                msg_stderr: Some(format!("alerts export io error: {}", e)),
            };
        }
    }

    let file = match fs::File::create(path) {
        Ok(file) => file,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 3,
                msg_stdout: None,
                msg_stderr: Some(format!("alerts export io error: {}", e)),
            };
        }
    };

    let write_result = if gzip {
        let mut encoder = GzEncoder::new(file, Compression::default());
        write_alert_jsonl_v1(&mut encoder, &alerts).and_then(|_| encoder.finish().map(|_| ()).map_err(|e| e.to_string()))
    } else {
        let mut writer = std::io::BufWriter::new(file);
        write_alert_jsonl_v1(&mut writer, &alerts)
    };

    if let Err(e) = write_result {
        return RouteResultV1 {
            exit_code: 3,
            msg_stdout: None,
            msg_stderr: Some(format!("alerts export io error: {}", e)),
        };
    }

    RouteResultV1 {
        exit_code: 0,
        msg_stdout: Some(format!(
            "alerts export
tenant_id: {}
out_path: {}
gzip: {}
count: {}
",
            tenant_id,
            path.display(),
            gzip,
            alerts.len(),
        )),
        msg_stderr: None,
    }
}

fn route_alert_extract_v1(
    cfg: &ConfigV1,
    tenant_id: &str,
    alert_id: &str,
    out_path: &str,
    max_bytes: Option<u64>,
    max_lines: Option<u64>,
) -> RouteResultV1 {
    let mut runtime = match SparxRuntimeV1::open_from_config_v1(cfg) {
        Ok(runtime) => runtime,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("alert extract db error: {}", e)),
            };
        }
    };

    let alert = match load_alert_by_id_v1(&mut runtime, tenant_id, alert_id) {
        Ok(Some(alert)) => alert,
        Ok(None) => {
            return RouteResultV1 {
                exit_code: 1,
                msg_stdout: None,
                msg_stderr: Some(format!("alert not found: {}", alert_id)),
            };
        }
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("alert extract db error: {}", e)),
            };
        }
    };

    let out_path_obj = Path::new(out_path);
    let result = match extract_alert_v1(cfg, &alert, out_path_obj, max_bytes, max_lines) {
        Ok(result) => result,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 3,
                msg_stdout: None,
                msg_stderr: Some(format!("alert extract io error: {}", e)),
            };
        }
    };

    RouteResultV1 {
        exit_code: 0,
        msg_stdout: Some(format!(
            "alert extract
tenant_id: {}
alert_id: {}
out_path: {}
spans_written: {}
bytes_written: {}
lines_written: {}
max_bytes: {}
max_lines: {}
",
            tenant_id,
            alert_id,
            result.out_path,
            result.spans_written,
            result.bytes_written,
            result.lines_written,
            format_cap_v1(result.max_bytes),
            format_cap_v1(result.max_lines),
        )),
        msg_stderr: None,
    }
}

fn route_alert_drill_v1(
    cfg: &ConfigV1,
    tenant_id: &str,
    alert_id: &str,
    max_bytes: Option<u64>,
    max_lines: Option<u64>,
) -> RouteResultV1 {
    let mut runtime = match SparxRuntimeV1::open_from_config_v1(cfg) {
        Ok(runtime) => runtime,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("alert drill db error: {}", e)),
            };
        }
    };

    let alert = match load_alert_by_id_v1(&mut runtime, tenant_id, alert_id) {
        Ok(Some(alert)) => alert,
        Ok(None) => {
            return RouteResultV1 {
                exit_code: 1,
                msg_stdout: None,
                msg_stderr: Some(format!("alert not found: {}", alert_id)),
            };
        }
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("alert drill db error: {}", e)),
            };
        }
    };

    let result = match drill_alert_v1(cfg, &alert, max_bytes, max_lines) {
        Ok(result) => result,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 3,
                msg_stdout: None,
                msg_stderr: Some(format!("alert drill io error: {}", e)),
            };
        }
    };

    RouteResultV1 {
        exit_code: 0,
        msg_stdout: Some(format_alert_drill_text_v1(tenant_id, alert_id, &result)),
        msg_stderr: None,
    }
}

fn load_filtered_alerts_v1(
    runtime: &mut SparxRuntimeV1,
    tenant_id: &str,
    filters: &AlertQueryFiltersV1,
) -> Result<Vec<AlertV1>, crate::db::DbErrorV1> {
    let tenant_db_dir = runtime.layout_v1().tenant_db_dir_v1(tenant_id);
    if !tenant_db_dir.exists() {
        return Ok(Vec::new());
    }

    let mut alerts = runtime.with_tenant_db_v1(tenant_id, current_unix_ts_v1(), |db| {
        let mut out = Vec::new();
        let indexed_ids = if let Some(entity) = &filters.entity {
            db.select_alert_ids_via_entity_index_if_complete_v1(
                alert_entity_kind_filter_name_v1(entity.kind),
                &entity.value,
                filters.since,
                filters.until,
            )?
        } else if let Some(category) = filters.category {
            db.select_alert_ids_via_category_index_if_complete_v1(
                alert_category_filter_name_v1(category),
                filters.since,
                filters.until,
            )?
        } else {
            db.select_alert_ids_via_time_index_if_complete_v1(filters.since, filters.until)?
        };

        if let Some(alert_ids) = indexed_ids {
            for alert_id in alert_ids {
                if let Some(alert) = db.read_primary_alert_v1(&alert_id)? {
                    out.push(alert);
                }
            }
            return Ok(out);
        }

        for alert_id in db.list_primary_alert_ids_v1()? {
            if let Some(alert) = db.read_primary_alert_v1(&alert_id)? {
                if alert_matches_query_filters_v1(&alert, filters) {
                    out.push(alert);
                }
            }
        }
        Ok(out)
    })?;

    alerts.retain(|alert| alert_matches_query_filters_v1(alert, filters));
    alerts.sort_by(|a, b| {
        b.window_start_ts
            .cmp(&a.window_start_ts)
            .then_with(|| a.alert_id.cmp(&b.alert_id))
    });
    Ok(alerts)
}

fn load_alert_by_id_v1(
    runtime: &mut SparxRuntimeV1,
    tenant_id: &str,
    alert_id: &str,
) -> Result<Option<AlertV1>, crate::db::DbErrorV1> {
    let tenant_db_dir = runtime.layout_v1().tenant_db_dir_v1(tenant_id);
    if !tenant_db_dir.exists() {
        return Ok(None);
    }
    runtime.with_tenant_db_v1(tenant_id, current_unix_ts_v1(), |db| db.read_primary_alert_v1(alert_id))
}

fn alert_matches_query_filters_v1(alert: &AlertV1, filters: &AlertQueryFiltersV1) -> bool {
    if !alert_matches_time_filter_v1(alert, filters.since, filters.until) {
        return false;
    }
    if let Some(category) = filters.category {
        if !alert_matches_category_filter_v1(alert, category) {
            return false;
        }
    }
    if let Some(entity) = &filters.entity {
        if !alert_matches_entity_filter_v1(alert, entity) {
            return false;
        }
    }
    true
}

fn alert_matches_time_filter_v1(alert: &AlertV1, since: Option<i64>, until: Option<i64>) -> bool {
    if let Some(since_ts) = since {
        if alert.window_start_ts < since_ts {
            return false;
        }
    }
    if let Some(until_ts) = until {
        if alert.window_start_ts >= until_ts {
            return false;
        }
    }
    true
}

fn alert_matches_category_filter_v1(alert: &AlertV1, category: AlertCategoryFilterV1) -> bool {
    match category {
        AlertCategoryFilterV1::Outlier => matches!(alert.label, crate::types::LabelV1::Outlier),
        AlertCategoryFilterV1::NoiseSuspect => matches!(alert.label, crate::types::LabelV1::NoiseSuspect),
        AlertCategoryFilterV1::Info => matches!(alert.label, crate::types::LabelV1::Info),
    }
}

fn alert_matches_entity_filter_v1(alert: &AlertV1, entity: &AlertEntityFilterV1) -> bool {
    let needle = entity.value.as_str();
    match entity.kind {
        AlertEntityKindFilterV1::SrcIp => alert.entities.src_ips.iter().any(|entry| entry.value == needle),
        AlertEntityKindFilterV1::DstIp => alert.entities.dst_ips.iter().any(|entry| entry.value == needle),
        AlertEntityKindFilterV1::UserId => alert.entities.user_ids.iter().any(|entry| entry.value == needle),
        AlertEntityKindFilterV1::Domain => alert.entities.domains.iter().any(|entry| entry.value == needle),
        AlertEntityKindFilterV1::Host => alert.entities.hosts.iter().any(|entry| entry.value == needle),
    }
}

fn alert_contains_text_v1(alert: &AlertV1, contains: &str) -> bool {
    let needle = contains.to_ascii_lowercase();
    if needle.is_empty() {
        return true;
    }

    let mut haystacks: Vec<&str> = vec![
        alert.alert_id.as_str(),
        alert.device_key.as_str(),
        alert.device_path.as_str(),
        alert.summary_analyst.as_str(),
        alert.summary_customer.as_str(),
        alert.signature.as_str(),
    ];
    for reason in &alert.reasons {
        haystacks.push(reason.code.as_str());
        haystacks.push(reason.msg.as_str());
        for (k, v) in &reason.details {
            haystacks.push(k.as_str());
            haystacks.push(v.as_str());
        }
    }
    for feature in &alert.top_features {
        haystacks.push(feature.feature.as_str());
    }
    for value in &alert.entities.src_ips {
        haystacks.push(value.value.as_str());
    }
    for value in &alert.entities.dst_ips {
        haystacks.push(value.value.as_str());
    }
    for value in &alert.entities.user_ids {
        haystacks.push(value.value.as_str());
    }
    for value in &alert.entities.domains {
        haystacks.push(value.value.as_str());
    }
    for value in &alert.entities.hosts {
        haystacks.push(value.value.as_str());
    }

    haystacks.into_iter().any(|s| s.to_ascii_lowercase().contains(&needle))
}

fn format_alert_list_text_v1(
    title: &str,
    tenant_id: &str,
    alerts: &[AlertV1],
    filters: &AlertQueryFiltersV1,
) -> String {
    let mut out = String::new();
    out.push_str(title);
    out.push('\n');
    out.push_str(&format!("tenant_id: {}
", tenant_id));
    append_alert_query_filter_lines_v1(&mut out, filters);
    out.push_str(&format!("count: {}
", alerts.len()));
    for alert in alerts {
        out.push_str(&format!(
            "- alert_id: {} window_start_ts: {} label: {} confidence: {} score_total: {:.3} device_path: {}
",
            alert.alert_id,
            alert.window_start_ts,
            format_label_v1(alert),
            format_confidence_v1(alert),
            alert.score_total,
            alert.device_path,
        ));
    }
    out
}

fn format_alert_search_text_v1(
    tenant_id: &str,
    contains: &str,
    alerts: &[AlertV1],
    filters: &AlertQueryFiltersV1,
) -> String {
    let mut out = String::new();
    out.push_str("alerts search
");
    out.push_str(&format!("tenant_id: {}
", tenant_id));
    append_alert_query_filter_lines_v1(&mut out, filters);
    out.push_str(&format!("contains: {}
", contains));
    out.push_str(&format!("count: {}
", alerts.len()));
    for alert in alerts {
        out.push_str(&format!(
            "- alert_id: {} window_start_ts: {} label: {} score_total: {:.3} summary_analyst: {}
",
            alert.alert_id,
            alert.window_start_ts,
            format_label_v1(alert),
            alert.score_total,
            alert.summary_analyst,
        ));
    }
    out
}

fn append_alert_query_filter_lines_v1(out: &mut String, filters: &AlertQueryFiltersV1) {
    if let Some(since) = filters.since {
        out.push_str(&format!("since: {}
", since));
    }
    if let Some(until) = filters.until {
        out.push_str(&format!("until: {}
", until));
    }
    if let Some(category) = filters.category {
        out.push_str(&format!("category: {}
", alert_category_filter_name_v1(category)));
    }
    if let Some(entity) = &filters.entity {
        out.push_str(&format!("entity_kind: {}
", alert_entity_kind_filter_name_v1(entity.kind)));
        out.push_str(&format!("entity_value: {}
", entity.value));
    }
}

fn alert_query_filters_json_v1(filters: &AlertQueryFiltersV1) -> serde_json::Value {
    serde_json::json!({
        "since": filters.since,
        "until": filters.until,
        "category": filters.category.map(alert_category_filter_name_v1),
        "entity_kind": filters.entity.as_ref().map(|entity| alert_entity_kind_filter_name_v1(entity.kind)),
        "entity_value": filters.entity.as_ref().map(|entity| entity.value.clone()),
    })
}

fn alert_category_filter_name_v1(category: AlertCategoryFilterV1) -> &'static str {
    match category {
        AlertCategoryFilterV1::Outlier => "outlier",
        AlertCategoryFilterV1::NoiseSuspect => "noise_suspect",
        AlertCategoryFilterV1::Info => "info",
    }
}

fn alert_entity_kind_filter_name_v1(kind: AlertEntityKindFilterV1) -> &'static str {
    match kind {
        AlertEntityKindFilterV1::SrcIp => "srcip",
        AlertEntityKindFilterV1::DstIp => "dstip",
        AlertEntityKindFilterV1::UserId => "userid",
        AlertEntityKindFilterV1::Domain => "domain",
        AlertEntityKindFilterV1::Host => "host",
    }
}

fn format_alert_show_text_v1(tenant_id: &str, alert: &AlertV1) -> String {
    let mut out = String::new();
    out.push_str("alerts show
");
    out.push_str(&format!("tenant_id: {}
", tenant_id));
    out.push_str(&format!("alert_id: {}
", alert.alert_id));
    out.push_str(&format!("device_key: {}
", alert.device_key));
    out.push_str(&format!("device_path: {}
", alert.device_path));
    out.push_str(&format!("window_start_ts: {}
", alert.window_start_ts));
    out.push_str(&format!("window_end_ts: {}
", alert.window_end_ts));
    out.push_str(&format!("label: {}
", format_label_v1(alert)));
    out.push_str(&format!("confidence: {}
", format_confidence_v1(alert)));
    out.push_str(&format!("score_total: {:.3}
", alert.score_total));
    out.push_str(&format!("summary_analyst: {}
", alert.summary_analyst));
    out.push_str(&format!("summary_customer: {}
", alert.summary_customer));
    out.push_str(&format!("reasons_count: {}
", alert.reasons.len()));
    out.push_str(&format!("top_features_count: {}
", alert.top_features.len()));
    out.push_str(&format!("provenance_count: {}
", alert.provenance.len()));
    out
}

fn format_alert_drill_text_v1(
    tenant_id: &str,
    alert_id: &str,
    result: &crate::drilldown::DrillAlertResultV1,
) -> String {
    let mut out = String::new();
    out.push_str("alert drill
");
    out.push_str(&format!("tenant_id: {}
", tenant_id));
    out.push_str(&format!("alert_id: {}
", alert_id));
    out.push_str(&format!("spans_total: {}
", result.spans.len()));
    out.push_str(&format!("spans_emitted: {}
", result.spans_emitted));
    out.push_str(&format!("gzip_spans_skipped: {}
", result.gzip_spans_skipped));
    out.push_str(&format!("bytes_emitted: {}
", result.bytes_emitted));
    out.push_str(&format!("lines_emitted: {}
", result.lines_emitted));
    out.push_str(&format!("max_bytes: {}
", format_cap_v1(result.max_bytes)));
    out.push_str(&format!("max_lines: {}
", format_cap_v1(result.max_lines)));
    for span in &result.spans {
        out.push_str(&format!(
            "- span_index: {} path: {} offset_start: {} offset_end: {} gzip_skipped: {} bytes_emitted: {} lines_emitted: {}
",
            span.span_index,
            span.path,
            span.offset_start,
            span.offset_end,
            span.gzip_skipped,
            span.bytes_emitted,
            span.lines_emitted,
        ));
        for line in &span.lines {
            out.push_str("  ");
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

fn format_cap_v1(value: Option<u64>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "none".to_string(),
    }
}

fn write_alert_jsonl_v1<W: Write>(writer: &mut W, alerts: &[AlertV1]) -> Result<(), String> {
    for alert in alerts {
        let line = serde_json::to_vec(alert).map_err(|e| e.to_string())?;
        writer.write_all(&line).map_err(|e| e.to_string())?;
        writer.write_all(b"\n").map_err(|e| e.to_string())?;
    }
    writer.flush().map_err(|e| e.to_string())
}

fn format_label_v1(alert: &AlertV1) -> &'static str {
    match alert.label {
        crate::types::LabelV1::Outlier => "outlier",
        crate::types::LabelV1::NoiseSuspect => "noise_suspect",
        crate::types::LabelV1::Info => "info",
    }
}

fn format_confidence_v1(alert: &AlertV1) -> &'static str {
    match alert.confidence {
        crate::types::ConfidenceV1::High => "high",
        crate::types::ConfidenceV1::Medium => "medium",
        crate::types::ConfidenceV1::Low => "low",
    }
}

fn route_tenant_policy_error_v1(
    action: &str,
    tenant_id: &str,
    policy_path: &std::path::Path,
    kind: TenantPolicyLoadErrorKindV1,
    details: Vec<String>,
) -> RouteResultV1 {
    let mut stderr = String::new();
    stderr.push_str("tenant policy ");
    stderr.push_str(action);
    stderr.push_str(" failed
");
    stderr.push_str(&format!("tenant_id: {}
", tenant_id));
    stderr.push_str(&format!("path: {}
", policy_path.display()));
    for detail in details {
        stderr.push_str("- ");
        stderr.push_str(&detail);
        stderr.push('\n');
    }
    RouteResultV1 {
        exit_code: match kind {
            TenantPolicyLoadErrorKindV1::Io => 3,
            _ => 1,
        },
        msg_stdout: None,
        msg_stderr: Some(stderr),
    }
}

fn format_tenant_policy_show_v1(
    tenant_id: &str,
    policy_path: &std::path::Path,
    policy: &TenantPolicyV1,
) -> String {
    let mut out = String::new();
    out.push_str("tenant policy show
");
    out.push_str(&format!("tenant_id: {}
", tenant_id));
    out.push_str(&format!("path: {}
", policy_path.display()));
    out.push_str(&format!("policy_version: {}
", policy.policy_version));
    out.push_str(&format!(
        "min_identity_confidence: {}
",
        policy.min_identity_confidence
    ));
    out.push_str(&format!(
        "ip_bucket: {}
",
        policy.ip_bucket.as_deref().unwrap_or("none")
    ));
    out.push_str(&format!("key_overrides_count: {}
", policy.key_overrides.len()));
    out.push_str("key_overrides:
");
    for (norm_key, category) in &policy.key_overrides {
        out.push_str(&format!("- {} => {}
", norm_key, category));
    }
    out
}

fn format_tenant_policy_check_v1(
    tenant_id: &str,
    policy_path: &std::path::Path,
    policy: &TenantPolicyV1,
) -> String {
    format!(
        "tenant policy ok
tenant_id: {}
path: {}
policy_version: {}
min_identity_confidence: {}
ip_bucket: {}
key_overrides_count: {}
",
        tenant_id,
        policy_path.display(),
        policy.policy_version,
        policy.min_identity_confidence,
        policy.ip_bucket.as_deref().unwrap_or("none"),
        policy.key_overrides.len(),
    )
}

fn route_migrate_tenant_v1(cfg: &ConfigV1, tenant_id: &str) -> RouteResultV1 {
    let mut runtime = match SparxRuntimeV1::open_from_config_v1(cfg) {
        Ok(runtime) => runtime,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("migrate db error: {}", e)),
            };
        }
    };

    let now_ts = current_unix_ts_v1();
    let global = match runtime.migrate_global_schema_v1(now_ts) {
        Ok(result) => result,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("migrate db error: {}", e)),
            };
        }
    };

    if global.outcome == SchemaMigrateOutcomeKindV1::RefusedDowngrade {
        return route_migrate_global_refused_v1(&global);
    }

    let tenant = match runtime.migrate_tenant_schema_v1(tenant_id, now_ts + 1000) {
        Ok(result) => result,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("migrate db error: {}", e)),
            };
        }
    };

    route_migrate_tenant_result_v1(tenant_id, &global, &tenant)
}

fn route_migrate_all_v1(cfg: &ConfigV1) -> RouteResultV1 {
    let mut runtime = match SparxRuntimeV1::open_from_config_v1(cfg) {
        Ok(runtime) => runtime,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("migrate db error: {}", e)),
            };
        }
    };

    let now_ts = current_unix_ts_v1();
    let result = match runtime.migrate_all_schemas_v1(now_ts) {
        Ok(result) => result,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("migrate db error: {}", e)),
            };
        }
    };

    route_migrate_all_result_v1(&result)
}

fn route_migrate_global_refused_v1(global: &GlobalSchemaMigrateResultV1) -> RouteResultV1 {
    RouteResultV1 {
        exit_code: 1,
        msg_stdout: Some(format!(
            "migrate global
outcome: {}
version_before: {}
version_after: {}
journal_entries: {}
",
            format_schema_migrate_outcome_v1(&global.outcome),
            format_optional_u32_v1(global.version_before),
            format_optional_u32_v1(global.version_after),
            format_list_v1(&global.journal_entries),
        )),
        msg_stderr: Some(global.failure_details.join("\n")),
    }
}

fn route_migrate_tenant_result_v1(
    tenant_id: &str,
    global: &GlobalSchemaMigrateResultV1,
    tenant: &TenantSchemaMigrateResultV1,
) -> RouteResultV1 {
    let stdout = format!(
        "migrate tenant
tenant_id: {}
global_outcome: {}
global_version_before: {}
global_version_after: {}
global_journal_entries: {}
tenant_outcome: {}
tenant_status_before: {}
tenant_status_after: {}
tenant_version_before: {}
tenant_version_after: {}
tenant_journal_entries: {}
",
        tenant_id,
        format_schema_migrate_outcome_v1(&global.outcome),
        format_optional_u32_v1(global.version_before),
        format_optional_u32_v1(global.version_after),
        format_list_v1(&global.journal_entries),
        format_schema_migrate_outcome_v1(&tenant.outcome),
        format_optional_status_v1(tenant.status_before),
        format_optional_status_v1(tenant.status_after),
        format_optional_u32_v1(tenant.version_before),
        format_optional_u32_v1(tenant.version_after),
        format_list_v1(&tenant.journal_entries),
    );

    match tenant.outcome {
        SchemaMigrateOutcomeKindV1::NoopCurrent
        | SchemaMigrateOutcomeKindV1::Initialized
        | SchemaMigrateOutcomeKindV1::Upgraded => RouteResultV1 {
            exit_code: 0,
            msg_stdout: Some(stdout),
            msg_stderr: None,
        },
        SchemaMigrateOutcomeKindV1::TenantNotFound
        | SchemaMigrateOutcomeKindV1::RefusedDowngrade
        | SchemaMigrateOutcomeKindV1::SkippedTerminated => RouteResultV1 {
            exit_code: 1,
            msg_stdout: Some(stdout),
            msg_stderr: Some(tenant.failure_details.join("\n")),
        },
    }
}

fn route_migrate_all_result_v1(result: &MigrateAllResultV1) -> RouteResultV1 {
    if result.global.outcome == SchemaMigrateOutcomeKindV1::RefusedDowngrade {
        return route_migrate_global_refused_v1(&result.global);
    }

    let mut stdout = String::new();
    stdout.push_str("migrate all\n");
    stdout.push_str(&format!(
        "global_outcome: {}
global_version_before: {}
global_version_after: {}
global_journal_entries: {}
",
        format_schema_migrate_outcome_v1(&result.global.outcome),
        format_optional_u32_v1(result.global.version_before),
        format_optional_u32_v1(result.global.version_after),
        format_list_v1(&result.global.journal_entries),
    ));
    stdout.push_str("tenants:\n");

    let mut stderr_lines: Vec<String> = Vec::new();
    let mut has_failures = false;
    for tenant in &result.tenants {
        stdout.push_str(&format!(
            "- tenant_id: {} outcome: {} status_before: {} status_after: {} version_before: {} version_after: {} journal_entries: {}
",
            tenant.tenant_id,
            format_schema_migrate_outcome_v1(&tenant.outcome),
            format_optional_status_v1(tenant.status_before),
            format_optional_status_v1(tenant.status_after),
            format_optional_u32_v1(tenant.version_before),
            format_optional_u32_v1(tenant.version_after),
            format_list_v1(&tenant.journal_entries),
        ));
        if tenant.outcome == SchemaMigrateOutcomeKindV1::RefusedDowngrade
            || tenant.outcome == SchemaMigrateOutcomeKindV1::TenantNotFound
        {
            has_failures = true;
        }
        for detail in &tenant.failure_details {
            stderr_lines.push(format!("{}: {}", tenant.tenant_id, detail));
        }
    }

    RouteResultV1 {
        exit_code: if has_failures { 6 } else { 0 },
        msg_stdout: Some(stdout),
        msg_stderr: if stderr_lines.is_empty() {
            None
        } else {
            Some(stderr_lines.join("\n"))
        },
    }
}

fn route_tenant_purge_result_v1(result: &TenantPurgeResultV1) -> RouteResultV1 {
    match result.outcome {
        TenantPurgeOutcomeKindV1::Success => RouteResultV1 {
            exit_code: 0,
            msg_stdout: Some(format!(
                "tenant purge ok\ntenant_id: {}\nstatus_before: {}\nstatus_after: {}\nclosed_tenant_handle: {}\ndeleted_db_dir: {}\ndeleted_alert_dir: {}\ndeleted_spool_dir: {}\n",
                result.tenant_id,
                format_optional_status_v1(result.status_before),
                format_optional_status_v1(result.status_after),
                result.closed_tenant_handle,
                result.deleted_db_dir,
                result.deleted_alert_dir,
                result.deleted_spool_dir,
            )),
            msg_stderr: None,
        },
        TenantPurgeOutcomeKindV1::TenantNotFound => RouteResultV1 {
            exit_code: 1,
            msg_stdout: None,
            msg_stderr: Some(format!("tenant purge failed: tenant not found: {}", result.tenant_id)),
        },
        TenantPurgeOutcomeKindV1::RejectedStatus => RouteResultV1 {
            exit_code: 1,
            msg_stdout: None,
            msg_stderr: Some(format!(
                "tenant purge rejected: {}",
                result
                    .failure_details
                    .first()
                    .cloned()
                    .unwrap_or_else(|| format!("tenant {} is not terminating", result.tenant_id))
            )),
        },
        TenantPurgeOutcomeKindV1::Partial => {
            let deleted_count = [
                result.deleted_db_dir,
                result.deleted_alert_dir,
                result.deleted_spool_dir,
            ]
            .into_iter()
            .filter(|v| *v)
            .count();
            let exit_code = if deleted_count > 0 { 6 } else { 3 };
            let mut stderr = String::new();
            stderr.push_str("tenant purge incomplete\n");
            stderr.push_str(&format!("tenant_id: {}\n", result.tenant_id));
            for detail in &result.failure_details {
                stderr.push_str("- ");
                stderr.push_str(detail);
                stderr.push('\n');
            }
            RouteResultV1 {
                exit_code,
                msg_stdout: Some(format!(
                    "tenant purge partial\ntenant_id: {}\nstatus_before: {}\nstatus_after: {}\nclosed_tenant_handle: {}\ndeleted_db_dir: {}\ndeleted_alert_dir: {}\ndeleted_spool_dir: {}\n",
                    result.tenant_id,
                    format_optional_status_v1(result.status_before),
                    format_optional_status_v1(result.status_after),
                    result.closed_tenant_handle,
                    result.deleted_db_dir,
                    result.deleted_alert_dir,
                    result.deleted_spool_dir,
                )),
                msg_stderr: Some(stderr),
            }
        }
    }
}

fn current_unix_ts_v1() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        Err(_) => 0,
    }
}

fn format_optional_status_v1(status: Option<u8>) -> String {
    match status {
        Some(v) => v.to_string(),
        None => "none".to_string(),
    }
}

fn format_optional_u32_v1(value: Option<u32>) -> String {
    match value {
        Some(v) => v.to_string(),
        None => "none".to_string(),
    }
}

fn format_list_v1(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_string()
    } else {
        values.join(",")
    }
}

fn format_schema_migrate_outcome_v1(outcome: &SchemaMigrateOutcomeKindV1) -> &'static str {
    match outcome {
        SchemaMigrateOutcomeKindV1::NoopCurrent => "noop_current",
        SchemaMigrateOutcomeKindV1::Initialized => "initialized",
        SchemaMigrateOutcomeKindV1::Upgraded => "upgraded",
        SchemaMigrateOutcomeKindV1::RefusedDowngrade => "refused_downgrade",
        SchemaMigrateOutcomeKindV1::TenantNotFound => "tenant_not_found",
        SchemaMigrateOutcomeKindV1::SkippedTerminated => "skipped_terminated",
    }
}

fn route_replay_spool_v1(cfg: &ConfigV1, tenant_id: Option<&str>) -> RouteResultV1 {
    if cfg.output.sink != "jsonl" {
        return RouteResultV1 {
            exit_code: 1,
            msg_stdout: None,
            msg_stderr: Some(format!(
                "replay-spool requires output.sink=jsonl; current sink={}",
                cfg.output.sink
            )),
        };
    }

    let layout = filesystem_layout_v1(cfg);
    let data_root = layout.data_root_v1().display().to_string();
    let alert_out_root = layout.alert_out_root_v1().display().to_string();

    let mut spool_files = match sorted_spool_files_for_replay_v1(&data_root) {
        Ok(files) => files,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 3,
                msg_stdout: None,
                msg_stderr: Some(format!("replay-spool enumerate error: {}", e.msg)),
            };
        }
    };

    if let Some(requested_tenant_id) = tenant_id {
        let tenant_spool_dir = layout.tenant_spool_dir_v1(requested_tenant_id);
        spool_files.retain(|path| path.starts_with(&tenant_spool_dir));
    }

    let mut sink = JsonlAlertSinkV1::new(JsonlSinkConfigV1 {
        alert_out_root,
        jsonl_rotate_mb: cfg.output.jsonl_rotate_mb,
        jsonl_flush_interval_s: cfg.output.jsonl_flush_interval_s,
    });

    let mut replayed_paths: Vec<PathBuf> = Vec::new();
    let mut failed_paths: Vec<PathBuf> = Vec::new();

    for path in spool_files {
        let alert = match read_spooled_alert_v1(&path) {
            Ok(alert) => alert,
            Err(_) => {
                failed_paths.push(path);
                continue;
            }
        };

        if let Some(requested_tenant_id) = tenant_id {
            if alert.tenant_id != requested_tenant_id {
                failed_paths.push(path);
                continue;
            }
        }

        if sink.emit_at_v1(&alert, alert.window_end_ts).is_err() {
            failed_paths.push(path);
            continue;
        }

        if sink.flush_v1().is_err() {
            failed_paths.push(path);
            continue;
        }

        if fs::remove_file(&path).is_err() {
            failed_paths.push(path);
            continue;
        }

        replayed_paths.push(path);
    }

    if let Err(e) = sink.shutdown_v1() {
        return RouteResultV1 {
            exit_code: 3,
            msg_stdout: None,
            msg_stderr: Some(format!("replay-spool shutdown error: {}", e.msg)),
        };
    }

    RouteResultV1 {
        exit_code: if failed_paths.is_empty() { 0 } else { 6 },
        msg_stdout: Some(format_replay_spool_summary_v1(tenant_id, replayed_paths.len(), failed_paths.len())),
        msg_stderr: format_replay_spool_failures_v1(tenant_id, &failed_paths),
    }
}

fn format_replay_spool_summary_v1(tenant_id: Option<&str>, replayed_count: usize, failed_count: usize) -> String {
    let scope = tenant_id.unwrap_or("all");
    format!(
        "replay-spool\nscope: {}\nsink: jsonl\nreplayed: {}\nfailed: {}\n",
        scope, replayed_count, failed_count
    )
}

fn format_replay_spool_failures_v1(tenant_id: Option<&str>, failed_paths: &[PathBuf]) -> Option<String> {
    if failed_paths.is_empty() {
        return None;
    }

    let scope = tenant_id.unwrap_or("all");
    let mut s = String::new();
    s.push_str("replay-spool partial failure\n");
    s.push_str("scope: ");
    s.push_str(scope);
    s.push('\n');
    s.push_str("failed_paths:\n");
    for path in failed_paths {
        s.push_str("- ");
        s.push_str(&path.display().to_string());
        s.push('\n');
    }
    Some(s)
}


#[derive(Clone, Debug, PartialEq, Eq)]
struct ActiveSpanStateV1 {
    file_rel: String,
    file_key: String,
    inode: u64,
    offset_start: u64,
    offset_end: u64,
    is_gzip: bool,
}

#[derive(Debug)]
enum OneShotSinkV1 {
    Jsonl(SpoolingJsonlAlertSinkV1),
    Stdout(StdoutAlertSinkV1<Vec<u8>>),
}

impl OneShotSinkV1 {
    fn emit_v1(&mut self, alert: &AlertV1) -> Result<(), String> {
        match self {
            OneShotSinkV1::Jsonl(sink) => sink.emit_at_v1(alert, alert.window_end_ts).map(|_| ()).map_err(|e| e.msg),
            OneShotSinkV1::Stdout(sink) => sink.emit_line_v1(alert).map_err(|e| e.msg),
        }
    }

    fn replay_automated_v1(&mut self, now_ts: i64, max_files: usize) -> Result<Option<SpoolReplayReportV1>, String> {
        match self {
            OneShotSinkV1::Jsonl(sink) => sink
                .replay_spooled_alerts_limited_v1(now_ts, max_files)
                .map(Some)
                .map_err(|e| e.msg),
            OneShotSinkV1::Stdout(_) => Ok(None),
        }
    }

    fn shutdown_v1(&mut self) -> Result<(), String> {
        match self {
            OneShotSinkV1::Jsonl(sink) => sink.shutdown_v1().map_err(|e| e.msg),
            OneShotSinkV1::Stdout(_) => Ok(()),
        }
    }

    fn into_stdout_v1(self) -> Result<Option<String>, String> {
        match self {
            OneShotSinkV1::Jsonl(_) => Ok(None),
            OneShotSinkV1::Stdout(sink) => String::from_utf8(sink.into_inner())
                .map(Some)
                .map_err(|e| format!("stdout sink utf8 error: {}", e)),
        }
    }
}

static RUN_STOP_REQUESTED_V1: AtomicBool = AtomicBool::new(false);
static RUN_SIGNAL_HANDLER_INIT_V1: OnceLock<Result<(), String>> = OnceLock::new();

#[derive(Clone, Debug, Default)]
struct RunCycleSummaryV1 {
    tenants_total: usize,
    tenants_processed: usize,
    tenants_skipped: usize,
    devices_processed: usize,
    devices_failed: usize,
    alerts_emitted: usize,
}

impl RunCycleSummaryV1 {
    fn add_v1(&mut self, other: &RunCycleSummaryV1) {
        self.tenants_total = self.tenants_total.saturating_add(other.tenants_total);
        self.tenants_processed = self.tenants_processed.saturating_add(other.tenants_processed);
        self.tenants_skipped = self.tenants_skipped.saturating_add(other.tenants_skipped);
        self.devices_processed = self.devices_processed.saturating_add(other.devices_processed);
        self.devices_failed = self.devices_failed.saturating_add(other.devices_failed);
        self.alerts_emitted = self.alerts_emitted.saturating_add(other.alerts_emitted);
    }
}


fn read_persisted_run_cycle_summary_v1(runtime: &SparxRuntimeV1) -> Result<RunCycleSummaryV1, DbErrorV1> {
    let db = runtime.global_db_v1();
    Ok(RunCycleSummaryV1 {
        tenants_total: db.read_metric_counter_v1(METRIC_RUN_TENANTS_TOTAL_V1)?.unwrap_or(0) as usize,
        tenants_processed: db.read_metric_counter_v1(METRIC_RUN_TENANTS_PROCESSED_TOTAL_V1)?.unwrap_or(0) as usize,
        tenants_skipped: db.read_metric_counter_v1(METRIC_RUN_TENANTS_SKIPPED_TOTAL_V1)?.unwrap_or(0) as usize,
        devices_processed: db.read_metric_counter_v1(METRIC_RUN_DEVICES_PROCESSED_TOTAL_V1)?.unwrap_or(0) as usize,
        devices_failed: db.read_metric_counter_v1(METRIC_RUN_DEVICES_FAILED_TOTAL_V1)?.unwrap_or(0) as usize,
        alerts_emitted: db.read_metric_counter_v1(METRIC_RUN_ALERTS_EMITTED_TOTAL_V1)?.unwrap_or(0) as usize,
    })
}

fn persist_run_metrics_v1(
    runtime: &SparxRuntimeV1,
    totals: &RunCycleSummaryV1,
    last_cycle: &RunCycleSummaryV1,
    cycle_completed_ts: i64,
) -> Result<(), DbErrorV1> {
    let db = runtime.global_db_v1();
    db.write_metric_counter_v1(METRIC_RUN_CYCLES_COMPLETED_TOTAL_V1, db.read_metric_counter_v1(METRIC_RUN_CYCLES_COMPLETED_TOTAL_V1)?.unwrap_or(0).saturating_add(1))?;
    db.write_metric_counter_v1(METRIC_RUN_TENANTS_TOTAL_V1, totals.tenants_total as u64)?;
    db.write_metric_counter_v1(METRIC_RUN_TENANTS_PROCESSED_TOTAL_V1, totals.tenants_processed as u64)?;
    db.write_metric_counter_v1(METRIC_RUN_TENANTS_SKIPPED_TOTAL_V1, totals.tenants_skipped as u64)?;
    db.write_metric_counter_v1(METRIC_RUN_DEVICES_PROCESSED_TOTAL_V1, totals.devices_processed as u64)?;
    db.write_metric_counter_v1(METRIC_RUN_DEVICES_FAILED_TOTAL_V1, totals.devices_failed as u64)?;
    db.write_metric_counter_v1(METRIC_RUN_ALERTS_EMITTED_TOTAL_V1, totals.alerts_emitted as u64)?;
    db.write_metric_gauge_v1(METRIC_RUN_LAST_CYCLE_TENANTS_TOTAL_V1, last_cycle.tenants_total as f64)?;
    db.write_metric_gauge_v1(METRIC_RUN_LAST_CYCLE_TENANTS_PROCESSED_V1, last_cycle.tenants_processed as f64)?;
    db.write_metric_gauge_v1(METRIC_RUN_LAST_CYCLE_TENANTS_SKIPPED_V1, last_cycle.tenants_skipped as f64)?;
    db.write_metric_gauge_v1(METRIC_RUN_LAST_CYCLE_DEVICES_PROCESSED_V1, last_cycle.devices_processed as f64)?;
    db.write_metric_gauge_v1(METRIC_RUN_LAST_CYCLE_DEVICES_FAILED_V1, last_cycle.devices_failed as f64)?;
    db.write_metric_gauge_v1(METRIC_RUN_LAST_CYCLE_ALERTS_EMITTED_V1, last_cycle.alerts_emitted as f64)?;
    db.write_metric_counter_v1(METRIC_RUN_LAST_CYCLE_COMPLETED_TS_V1, cycle_completed_ts as u64)?;
    Ok(())
}

fn ensure_run_signal_handler_v1() -> Result<(), String> {
    RUN_SIGNAL_HANDLER_INIT_V1
        .get_or_init(|| {
            ctrlc::set_handler(|| {
                RUN_STOP_REQUESTED_V1.store(true, Ordering::SeqCst);
            })
            .map_err(|e| format!("failed to install signal handler: {}", e))
        })
        .as_ref()
        .map(|_| ())
        .map_err(|e| e.clone())
}

fn is_nonfatal_run_warning_v1(item: &str) -> bool {
    item.starts_with("automated spool replay warning:")
}

fn format_automated_spool_replay_warning_v1(scope: &str, report: &SpoolReplayReportV1, max_files: usize) -> Option<String> {
    if report.failed_paths.is_empty() && report.replayed_paths.is_empty() {
        return None;
    }
    if report.failed_paths.is_empty() {
        return None;
    }
    Some(format!(
        "automated spool replay warning: scope={} replayed={} failed={} max_files_per_pass={}",
        scope,
        report.replayed_paths.len(),
        report.failed_paths.len(),
        max_files
    ))
}

fn test_run_max_cycles_v1() -> Option<u32> {
    std::env::var("SPARX_TEST_RUN_MAX_CYCLES")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| *v > 0)
}

fn route_run_v1(cfg: &ConfigV1, migrate: MigrateModeV1) -> RouteResultV1 {
    if let Err(e) = ensure_run_signal_handler_v1() {
        return RouteResultV1 {
            exit_code: 1,
            msg_stdout: None,
            msg_stderr: Some(format!("run startup error: {}", e)),
        };
    }
    RUN_STOP_REQUESTED_V1.store(false, Ordering::SeqCst);

    let mut runtime = match SparxRuntimeV1::open_from_config_v1(cfg) {
        Ok(runtime) => runtime,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("run db error: {}", e)),
            };
        }
    };

    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string());
    if let Err(e) = runtime.mark_process_start_v1(current_unix_ts_v1(), &host) {
        return RouteResultV1 {
            exit_code: 4,
            msg_stdout: None,
            msg_stderr: Some(format!("run db error: {}", e)),
        };
    }

    let result = run_daemon_inner_v1(&mut runtime, cfg, migrate);
    let _ = runtime.mark_process_end_v1(current_unix_ts_v1(), result.exit_code);
    result
}

fn run_daemon_inner_v1(
    runtime: &mut SparxRuntimeV1,
    cfg: &ConfigV1,
    migrate: MigrateModeV1,
) -> RouteResultV1 {
    let now_ts = current_unix_ts_v1();
    if let Err(e) = ensure_run_global_schema_mode_v1(runtime, migrate, now_ts) {
        return RouteResultV1 {
            exit_code: 4,
            msg_stdout: None,
            msg_stderr: Some(format!("run schema error: {}", e)),
        };
    }

    let mut sink = match cfg.output.sink.as_str() {
        "jsonl" => OneShotSinkV1::Jsonl(SpoolingJsonlAlertSinkV1::new(
            JsonlSinkConfigV1 {
                alert_out_root: cfg.sparx.alert_out_root.clone(),
                jsonl_rotate_mb: cfg.output.jsonl_rotate_mb,
                jsonl_flush_interval_s: cfg.output.jsonl_flush_interval_s,
            },
            SpoolConfigV1 {
                data_root: cfg.sparx.data_root.clone(),
                spool_max_mb: SPOOL_MAX_MB_DEFAULT_V1,
            },
        )),
        "stdout" => OneShotSinkV1::Stdout(StdoutAlertSinkV1::new(Vec::<u8>::new())),
        other => {
            return RouteResultV1 {
                exit_code: 1,
                msg_stdout: None,
                msg_stderr: Some(format!("run unsupported output.sink={}", other)),
            };
        }
    };

    let mut observability = match ObservabilityServersV1::start_from_runtime_v1(cfg, runtime) {
        Ok(servers) => servers,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 1,
                msg_stdout: None,
                msg_stderr: Some(format!("run observability startup error: {}", e)),
            };
        }
    };

    let max_cycles = test_run_max_cycles_v1();
    let mut cycles_completed = 0u32;
    let mut summary = RunCycleSummaryV1::default();
    let mut failures: Vec<String> = Vec::new();
    let mut metric_totals = match read_persisted_run_cycle_summary_v1(runtime) {
        Ok(summary) => summary,
        Err(e) => {
            observability.shutdown_v1();
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("run metrics error: {}", e)),
            };
        }
    };

    let automated_replay_max_files = cfg.output.automated_replay_max_files_per_pass as usize;

    loop {
        let cycle_now_ts = current_unix_ts_v1();
        match sink.replay_automated_v1(cycle_now_ts, automated_replay_max_files) {
            Ok(Some(report)) => {
                if let Some(warning) = format_automated_spool_replay_warning_v1(
                    "run",
                    &report,
                    automated_replay_max_files,
                ) {
                    failures.push(warning);
                }
            }
            Ok(None) => {}
            Err(e) => failures.push(format!("automated spool replay warning: scope=run error={}", e)),
        }
        match run_single_cycle_v1(runtime, cfg, migrate, &mut sink, cycle_now_ts) {
            Ok((cycle_summary, cycle_failures)) => {
                summary.add_v1(&cycle_summary);
                metric_totals.add_v1(&cycle_summary);
                if let Err(e) = persist_run_metrics_v1(runtime, &metric_totals, &cycle_summary, current_unix_ts_v1()) {
                    observability.shutdown_v1();
                    let _ = sink.shutdown_v1();
                    return RouteResultV1 {
                        exit_code: 4,
                        msg_stdout: None,
                        msg_stderr: Some(format!("run metrics error: {}", e)),
                    };
                }
                failures.extend(cycle_failures);
            }
            Err(route) => {
                observability.shutdown_v1();
                let _ = sink.shutdown_v1();
                return route;
            }
        }
        cycles_completed = cycles_completed.saturating_add(1);
        maybe_call_run_test_cycle_hook_v1(cycles_completed, runtime, cfg);

        if RUN_STOP_REQUESTED_V1.load(Ordering::SeqCst) {
            break;
        }
        if let Some(max_cycles) = max_cycles {
            if cycles_completed >= max_cycles {
                break;
            }
        }
        thread::sleep(Duration::from_millis(u64::from(cfg.ingest.poll_interval_ms)));
    }

    match sink.replay_automated_v1(current_unix_ts_v1(), automated_replay_max_files) {
        Ok(Some(report)) => {
            if let Some(warning) = format_automated_spool_replay_warning_v1(
                "run-final",
                &report,
                automated_replay_max_files,
            ) {
                failures.push(warning);
            }
        }
        Ok(None) => {}
        Err(e) => failures.push(format!("automated spool replay warning: scope=run-final error={}", e)),
    }

    if let Err(e) = sink.shutdown_v1() {
        observability.shutdown_v1();
        return RouteResultV1 {
            exit_code: 3,
            msg_stdout: None,
            msg_stderr: Some(format!("run sink shutdown error: {}", e)),
        };
    }

    let stdout_from_sink = match sink.into_stdout_v1() {
        Ok(v) => v,
        Err(e) => {
            observability.shutdown_v1();
            return RouteResultV1 {
                exit_code: 3,
                msg_stdout: None,
                msg_stderr: Some(e),
            };
        }
    };


    let stdout = match stdout_from_sink {
        Some(s) => Some(s),
        None => Some(format!(
            "run
cycles_completed: {}
tenants_total: {}
tenants_processed: {}
tenants_skipped: {}
devices_processed: {}
devices_failed: {}
alerts_emitted: {}
",
            cycles_completed,
            summary.tenants_total,
            summary.tenants_processed,
            summary.tenants_skipped,
            summary.devices_processed,
            summary.devices_failed,
            summary.alerts_emitted,
        )),
    };

    let stderr = if failures.is_empty() {
        None
    } else {
        let mut s = String::new();
        s.push_str("run cycle issues\n");
        for item in &failures {
            s.push_str("- ");
            s.push_str(item);
            s.push('\n');
        }
        Some(s)
    };

    observability.shutdown_v1();

    RouteResultV1 {
        exit_code: if failures.iter().any(|item| !is_nonfatal_run_warning_v1(item)) { 3 } else { 0 },
        msg_stdout: stdout,
        msg_stderr: stderr,
    }
}

fn run_single_cycle_v1(
    runtime: &mut SparxRuntimeV1,
    cfg: &ConfigV1,
    migrate: MigrateModeV1,
    sink: &mut OneShotSinkV1,
    now_ts: i64,
) -> Result<(RunCycleSummaryV1, Vec<String>), RouteResultV1> {
    let inventory = match discover_device_inventory_v1(Path::new(&cfg.sparx.tenant_root), cfg.ingest.follow_symlinks) {
        Ok(inventory) => inventory,
        Err(e) => {
            return Err(RouteResultV1 {
                exit_code: 3,
                msg_stdout: None,
                msg_stderr: Some(format!("run discovery error: {}", e)),
            });
        }
    };

    let mut by_tenant: BTreeMap<String, Vec<crate::ingest::DeviceInventoryV1>> = BTreeMap::new();
    for item in inventory {
        by_tenant
            .entry(item.device.tenant_id.clone())
            .or_default()
            .push(item);
    }

    let known_tenant_ids = match runtime.list_known_tenant_ids_v1() {
        Ok(ids) => ids,
        Err(e) => {
            return Err(RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("run db error: {}", e)),
            });
        }
    };

    let mut tenant_ids: BTreeSet<String> = BTreeSet::new();
    for tenant_id in known_tenant_ids {
        tenant_ids.insert(tenant_id);
    }
    for tenant_id in by_tenant.keys() {
        tenant_ids.insert(tenant_id.clone());
    }

    let mut summary = RunCycleSummaryV1 {
        tenants_total: tenant_ids.len(),
        ..RunCycleSummaryV1::default()
    };
    let mut failures: Vec<String> = Vec::new();

    for tenant_id in tenant_ids {
        let inventories = by_tenant.remove(&tenant_id).unwrap_or_default();
        let tenant_seen_this_cycle = !inventories.is_empty();

        let record = if tenant_seen_this_cycle {
            match ensure_tenant_record_for_run_v1(runtime, &tenant_id, now_ts) {
                Ok(record) => record,
                Err(e) => {
                    return Err(RouteResultV1 {
                        exit_code: 4,
                        msg_stdout: None,
                        msg_stderr: Some(format!("run db error: {}", e)),
                    });
                }
            }
        } else {
            match runtime.read_tenant_record_v1(&tenant_id) {
                Ok(Some(record)) => record,
                Ok(None) => {
                    if let Err(e) = runtime.set_tenant_active_index_v1(&tenant_id, false) {
                        return Err(RouteResultV1 {
                            exit_code: 4,
                            msg_stdout: None,
                            msg_stderr: Some(format!("run db error: {}", e)),
                        });
                    }
                    runtime.close_tenant_v1(&tenant_id);
                    summary.tenants_skipped = summary.tenants_skipped.saturating_add(1);
                    continue;
                }
                Err(e) => {
                    return Err(RouteResultV1 {
                        exit_code: 4,
                        msg_stdout: None,
                        msg_stderr: Some(format!("run db error: {}", e)),
                    });
                }
            }
        };

        if tenant_seen_this_cycle {
            if let Err(e) = runtime.set_tenant_last_seen_ts_v1(&tenant_id, now_ts) {
                return Err(RouteResultV1 {
                    exit_code: 4,
                    msg_stdout: None,
                    msg_stderr: Some(format!("run db error: {}", e)),
                });
            }
        }

        if record.status != 0 || !tenant_seen_this_cycle {
            if let Err(e) = runtime.set_tenant_active_index_v1(&tenant_id, false) {
                return Err(RouteResultV1 {
                    exit_code: 4,
                    msg_stdout: None,
                    msg_stderr: Some(format!("run db error: {}", e)),
                });
            }
            runtime.close_tenant_v1(&tenant_id);
            summary.tenants_skipped = summary.tenants_skipped.saturating_add(1);
            continue;
        }

        if let Err(e) = runtime.set_tenant_active_index_v1(&tenant_id, true) {
            return Err(RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("run db error: {}", e)),
            });
        }
        if let Err(e) = ensure_run_tenant_schema_mode_v1(runtime, &tenant_id, migrate, now_ts) {
            return Err(RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("run schema error: {}", e)),
            });
        }

        summary.tenants_processed = summary.tenants_processed.saturating_add(1);
        let mut inventories_sorted = inventories;
        inventories_sorted.sort_by(|a, b| {
            a.device
                .device_dir_rel
                .cmp(&b.device.device_dir_rel)
                .then(a.device.device_key.cmp(&b.device.device_key))
        });

        for inventory in &inventories_sorted {
            match process_device_oneshot_v1(runtime, cfg, inventory, None, None, sink, now_ts) {
                Ok(alert_count) => {
                    summary.devices_processed = summary.devices_processed.saturating_add(1);
                    summary.alerts_emitted = summary.alerts_emitted.saturating_add(alert_count);
                }
                Err(e) => {
                    summary.devices_failed = summary.devices_failed.saturating_add(1);
                    failures.push(format!("{}/{}: {}", inventory.device.tenant_id, inventory.device.device_dir_rel, e));
                }
            }
        }
    }

    Ok((summary, failures))
}

fn ensure_run_global_schema_mode_v1(
    runtime: &mut SparxRuntimeV1,
    migrate: MigrateModeV1,
    now_ts: i64,
) -> Result<(), DbErrorV1> {
    match migrate {
        MigrateModeV1::Auto | MigrateModeV1::Require => {
            let global = runtime.migrate_global_schema_v1(now_ts)?;
            if global.outcome == SchemaMigrateOutcomeKindV1::RefusedDowngrade {
                return Err(DbErrorV1::new_v1(format!(
                    "global schema refusal: {}",
                    global.failure_details.join("; ")
                )));
            }
            Ok(())
        }
        MigrateModeV1::Off => {
            let global = runtime.read_global_schema_state_v1()?;
            match global {
                Some(state) if state.version == crate::runtime::GLOBAL_SCHEMA_VERSION_CURRENT_V1 => Ok(()),
                Some(state) => Err(DbErrorV1::new_v1(format!(
                    "global schema version {} does not match required {} with --migrate off",
                    state.version,
                    crate::runtime::GLOBAL_SCHEMA_VERSION_CURRENT_V1
                ))),
                None => Err(DbErrorV1::new_v1(
                    "global schema missing with --migrate off".to_string(),
                )),
            }
        }
    }
}

fn ensure_run_tenant_schema_mode_v1(
    runtime: &mut SparxRuntimeV1,
    tenant_id: &str,
    migrate: MigrateModeV1,
    now_ts: i64,
) -> Result<(), DbErrorV1> {
    match migrate {
        MigrateModeV1::Auto | MigrateModeV1::Require => {
            let tenant = runtime.migrate_tenant_schema_v1(tenant_id, now_ts + 10)?;
            match tenant.outcome {
                SchemaMigrateOutcomeKindV1::RefusedDowngrade
                | SchemaMigrateOutcomeKindV1::TenantNotFound
                | SchemaMigrateOutcomeKindV1::SkippedTerminated => Err(DbErrorV1::new_v1(format!(
                    "tenant schema refusal: {}",
                    tenant.failure_details.join("; ")
                ))),
                _ => Ok(()),
            }
        }
        MigrateModeV1::Off => {
            let tenant_schema = runtime.with_tenant_db_v1(tenant_id, now_ts, |db| db.read_schema_state_v1())?;
            match tenant_schema {
                Some(state) if state.version == crate::runtime::TENANT_SCHEMA_VERSION_CURRENT_V1 => Ok(()),
                Some(state) => Err(DbErrorV1::new_v1(format!(
                    "tenant schema version {} does not match required {} with --migrate off",
                    state.version,
                    crate::runtime::TENANT_SCHEMA_VERSION_CURRENT_V1
                ))),
                None => Err(DbErrorV1::new_v1(
                    "tenant schema missing with --migrate off".to_string(),
                )),
            }
        }
    }
}

fn ensure_tenant_record_for_run_v1(
    runtime: &mut SparxRuntimeV1,
    tenant_id: &str,
    now_ts: i64,
) -> Result<crate::db::GlobalTenantRecordV1, DbErrorV1> {
    let paths = runtime.tenant_paths_v1(tenant_id);
    let mut record = match runtime.read_tenant_record_v1(tenant_id)? {
        Some(record) => record,
        None => crate::db::GlobalTenantRecordV1 {
            tenant_id: tenant_id.to_string(),
            created_ts: now_ts,
            last_seen_ts: now_ts,
            status: 0,
            tenant_root_rel: Some(tenant_id.to_string()),
            tenant_db_path: Some(paths.tenant_db_dir.clone()),
            alert_out_root: Some(paths.alert_out_dir.clone()),
        },
    };
    record.tenant_root_rel = Some(tenant_id.to_string());
    record.tenant_db_path = Some(paths.tenant_db_dir);
    record.alert_out_root = Some(paths.alert_out_dir);
    runtime.upsert_tenant_record_v1(&record)?;
    Ok(record)
}



fn route_oneshot_v1(
    cfg: &ConfigV1,
    tenant_id: &str,
    since: Option<i64>,
    until: Option<i64>,
    device_path: Option<&str>,
    migrate: MigrateModeV1,
) -> RouteResultV1 {
    let mut runtime = match SparxRuntimeV1::open_from_config_v1(cfg) {
        Ok(runtime) => runtime,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("oneshot db error: {}", e)),
            };
        }
    };

    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string());
    if let Err(e) = runtime.mark_process_start_v1(current_unix_ts_v1(), &host) {
        return RouteResultV1 {
            exit_code: 4,
            msg_stdout: None,
            msg_stderr: Some(format!("oneshot db error: {}", e)),
        };
    }

    let result = run_oneshot_inner_v1(&mut runtime, cfg, tenant_id, since, until, device_path, migrate);
    let _ = runtime.mark_process_end_v1(current_unix_ts_v1(), result.exit_code);
    result
}

fn run_oneshot_inner_v1(
    runtime: &mut SparxRuntimeV1,
    cfg: &ConfigV1,
    tenant_id: &str,
    since: Option<i64>,
    until: Option<i64>,
    device_path: Option<&str>,
    migrate: MigrateModeV1,
) -> RouteResultV1 {
    let now_ts = current_unix_ts_v1();
    if let Err(e) = ensure_tenant_record_for_oneshot_v1(runtime, tenant_id, now_ts) {
        return RouteResultV1 {
            exit_code: 4,
            msg_stdout: None,
            msg_stderr: Some(format!("oneshot db error: {}", e)),
        };
    }

    if let Err(e) = ensure_oneshot_schema_mode_v1(runtime, tenant_id, migrate, now_ts) {
        return RouteResultV1 {
            exit_code: 4,
            msg_stdout: None,
            msg_stderr: Some(format!("oneshot schema error: {}", e)),
        };
    }

    let inventories = match build_oneshot_inventory_v1(cfg, tenant_id, device_path) {
        Ok(inventories) => inventories,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 3,
                msg_stdout: None,
                msg_stderr: Some(format!("oneshot discovery error: {}", e)),
            };
        }
    };

    let mut sink = match cfg.output.sink.as_str() {
        "jsonl" => OneShotSinkV1::Jsonl(SpoolingJsonlAlertSinkV1::new(
            JsonlSinkConfigV1 {
                alert_out_root: cfg.sparx.alert_out_root.clone(),
                jsonl_rotate_mb: cfg.output.jsonl_rotate_mb,
                jsonl_flush_interval_s: cfg.output.jsonl_flush_interval_s,
            },
            SpoolConfigV1 {
                data_root: cfg.sparx.data_root.clone(),
                spool_max_mb: SPOOL_MAX_MB_DEFAULT_V1,
            },
        )),
        "stdout" => OneShotSinkV1::Stdout(StdoutAlertSinkV1::new(Vec::<u8>::new())),
        other => {
            return RouteResultV1 {
                exit_code: 1,
                msg_stdout: None,
                msg_stderr: Some(format!("oneshot unsupported output.sink={}", other)),
            };
        }
    };

    let mut total_alerts = 0usize;
    let mut failed_devices: Vec<String> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    let automated_replay_max_files = cfg.output.automated_replay_max_files_per_pass as usize;

    match sink.replay_automated_v1(current_unix_ts_v1(), automated_replay_max_files) {
        Ok(Some(report)) => {
            if let Some(warning) = format_automated_spool_replay_warning_v1(
                "oneshot-pre",
                &report,
                automated_replay_max_files,
            ) {
                warnings.push(warning);
            }
        }
        Ok(None) => {}
        Err(e) => warnings.push(format!("automated spool replay warning: scope=oneshot-pre error={}", e)),
    }

    for inventory in &inventories {
        match process_device_oneshot_v1(runtime, cfg, inventory, since, until, &mut sink, now_ts) {
            Ok(alert_count) => {
                total_alerts = total_alerts.saturating_add(alert_count);
            }
            Err(e) => {
                failed_devices.push(format!("{}/{}: {}", inventory.device.tenant_id, inventory.device.device_dir_rel, e));
            }
        }
    }

    match sink.replay_automated_v1(current_unix_ts_v1(), automated_replay_max_files) {
        Ok(Some(report)) => {
            if let Some(warning) = format_automated_spool_replay_warning_v1(
                "oneshot-post",
                &report,
                automated_replay_max_files,
            ) {
                warnings.push(warning);
            }
        }
        Ok(None) => {}
        Err(e) => warnings.push(format!("automated spool replay warning: scope=oneshot-post error={}", e)),
    }

    if let Err(e) = sink.shutdown_v1() {
        return RouteResultV1 {
            exit_code: 3,
            msg_stdout: None,
            msg_stderr: Some(format!("oneshot sink shutdown error: {}", e)),
        };
    }

    let stdout_from_sink = match sink.into_stdout_v1() {
        Ok(v) => v,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 3,
                msg_stdout: None,
                msg_stderr: Some(e),
            };
        }
    };

    let _ = runtime.set_tenant_last_seen_ts_v1(tenant_id, now_ts);

    let summary_stdout = match stdout_from_sink {
        Some(s) => Some(s),
        None => Some(format!(
            "oneshot
tenant_id: {}
devices_total: {}
devices_failed: {}
alerts_emitted: {}
",
            tenant_id,
            inventories.len(),
            failed_devices.len(),
            total_alerts,
        )),
    };

    let stderr = if failed_devices.is_empty() && warnings.is_empty() {
        None
    } else {
        let mut s = String::new();
        if !failed_devices.is_empty() {
            s.push_str("oneshot partial failure
");
            s.push_str(&format!("tenant_id: {}
", tenant_id));
            for item in &failed_devices {
                s.push_str("- ");
                s.push_str(item);
                s.push('\n');
            }
        }
        for item in &warnings {
            s.push_str("- ");
            s.push_str(item);
            s.push('\n');
        }
        Some(s)
    };

    RouteResultV1 {
        exit_code: if failed_devices.is_empty() { 0 } else { 6 },
        msg_stdout: summary_stdout,
        msg_stderr: stderr,
    }
}

fn ensure_tenant_record_for_oneshot_v1(
    runtime: &mut SparxRuntimeV1,
    tenant_id: &str,
    now_ts: i64,
) -> Result<(), DbErrorV1> {
    if runtime.read_tenant_record_v1(tenant_id)?.is_some() {
        return Ok(());
    }
    let paths = runtime.tenant_paths_v1(tenant_id);
    runtime.upsert_tenant_record_v1(&crate::db::GlobalTenantRecordV1 {
        tenant_id: tenant_id.to_string(),
        created_ts: now_ts,
        last_seen_ts: now_ts,
        status: 0,
        tenant_root_rel: Some(tenant_id.to_string()),
        tenant_db_path: Some(paths.tenant_db_dir),
        alert_out_root: Some(paths.alert_out_dir),
    })?;
    runtime.set_tenant_active_index_v1(tenant_id, true)?;
    Ok(())
}

fn ensure_oneshot_schema_mode_v1(
    runtime: &mut SparxRuntimeV1,
    tenant_id: &str,
    migrate: MigrateModeV1,
    now_ts: i64,
) -> Result<(), DbErrorV1> {
    match migrate {
        MigrateModeV1::Auto | MigrateModeV1::Require => {
            let global = runtime.migrate_global_schema_v1(now_ts)?;
            if global.outcome == SchemaMigrateOutcomeKindV1::RefusedDowngrade {
                return Err(DbErrorV1::new_v1(format!(
                    "global schema refusal: {}",
                    global.failure_details.join("; ")
                )));
            }
            let tenant = runtime.migrate_tenant_schema_v1(tenant_id, now_ts + 10)?;
            match tenant.outcome {
                SchemaMigrateOutcomeKindV1::RefusedDowngrade
                | SchemaMigrateOutcomeKindV1::TenantNotFound
                | SchemaMigrateOutcomeKindV1::SkippedTerminated => {
                    return Err(DbErrorV1::new_v1(format!(
                        "tenant schema refusal: {}",
                        tenant.failure_details.join("; ")
                    )));
                }
                _ => {}
            }
            Ok(())
        }
        MigrateModeV1::Off => {
            let global = runtime.read_global_schema_state_v1()?;
            match global {
                Some(state) if state.version == crate::runtime::GLOBAL_SCHEMA_VERSION_CURRENT_V1 => {}
                Some(state) => {
                    return Err(DbErrorV1::new_v1(format!(
                        "global schema version {} does not match required {} with --migrate off",
                        state.version,
                        crate::runtime::GLOBAL_SCHEMA_VERSION_CURRENT_V1
                    )));
                }
                None => {
                    return Err(DbErrorV1::new_v1(
                        "global schema missing with --migrate off".to_string(),
                    ));
                }
            }

            let tenant_schema = runtime.with_tenant_db_v1(tenant_id, now_ts, |db| db.read_schema_state_v1())?;
            match tenant_schema {
                Some(state) if state.version == crate::runtime::TENANT_SCHEMA_VERSION_CURRENT_V1 => Ok(()),
                Some(state) => Err(DbErrorV1::new_v1(format!(
                    "tenant schema version {} does not match required {} with --migrate off",
                    state.version,
                    crate::runtime::TENANT_SCHEMA_VERSION_CURRENT_V1
                ))),
                None => Err(DbErrorV1::new_v1(
                    "tenant schema missing with --migrate off".to_string(),
                )),
            }
        }
    }
}

fn build_oneshot_inventory_v1(
    cfg: &ConfigV1,
    tenant_id: &str,
    device_path: Option<&str>,
) -> Result<Vec<crate::ingest::DeviceInventoryV1>, std::io::Error> {
    let watch_root = Path::new(&cfg.sparx.tenant_root);
    let mut inventories: Vec<crate::ingest::DeviceInventoryV1> = discover_device_inventory_v1(
        watch_root,
        cfg.ingest.follow_symlinks,
    )?
    .into_iter()
    .filter(|inventory| inventory.device.tenant_id == tenant_id)
    .collect();

    if let Some(device_path) = device_path {
        inventories.retain(|inventory| inventory.device.device_dir_rel == device_path);
    }
    inventories.sort_by(|a, b| {
        a.device
            .tenant_id
            .cmp(&b.device.tenant_id)
            .then(a.device.device_dir_rel.cmp(&b.device.device_dir_rel))
            .then(a.device.device_key.cmp(&b.device.device_key))
    });
    Ok(inventories)
}

fn process_device_oneshot_v1(
    runtime: &mut SparxRuntimeV1,
    cfg: &ConfigV1,
    inventory: &crate::ingest::DeviceInventoryV1,
    since: Option<i64>,
    until: Option<i64>,
    sink: &mut OneShotSinkV1,
    now_ts: i64,
) -> Result<usize, String> {
    let dict_cfg = FeatureDictionaryConfigV1::from(&cfg.features);
    let caps = WindowCapsV1::from(&cfg.caps);
    let df_cfg = DfRingConfigV1::from(&cfg.baseline);
    let centroid_cfg = CentroidStatsConfigV1 {
        centroid_alpha: 0.5,
        centroid_cap: 10_000,
    };
    let mut alert_cfg = AlertScoringConfigV1::from_sections_v1(&cfg.scoring, cfg.ingest.window_size_s);
    alert_cfg.include_debug_fields = cfg.output.include_debug_fields;

    let tenant_id = inventory.device.tenant_id.clone();
    let device = &inventory.device;

    let mut dict = runtime
        .with_tenant_db_v1(&tenant_id, now_ts, |db| load_dict_from_tenant_db_v1(db, dict_cfg.clone()))
        .map_err(|e| e.to_string())?;
    let mut acc = runtime
        .with_tenant_db_v1(&tenant_id, now_ts, |db| restore_active_window_from_tenant_db_v1(db, &device.device_key, caps.clone(), &dict))
        .map_err(|e| e.to_string())?;
    let mut active_spans: Vec<ActiveSpanStateV1> = Vec::new();
    let mut alerts_emitted = 0usize;
    let mut ordered_files = inventory.files.clone();
    ordered_files.sort_by(|a, b| {
        a.is_gzip
            .cmp(&b.is_gzip)
            .then(a.file_rel.cmp(&b.file_rel))
            .then(a.file_key.cmp(&b.file_key))
    });

    for file in &ordered_files {
        let file_path = Path::new(&cfg.sparx.tenant_root)
            .join(&device.tenant_id)
            .join(&device.device_dir_rel)
            .join(&file.file_rel);
        let observed = observed_file_state_for_path_v1(&file_path, file.is_gzip).map_err(|e| e.to_string())?;
        let previous_cursor = runtime
            .with_tenant_db_v1(&tenant_id, now_ts, |db| db.read_cursor_v1(&device.device_key, &file.file_key))
            .map_err(|e| e.to_string())?;
        let cursor_plan = reconcile_cursor_v1(previous_cursor.as_ref(), &observed);
        let mut cursor = cursor_plan.cursor.clone();
        runtime
            .with_tenant_db_v1(&tenant_id, now_ts, |db| db.write_cursor_v1(&device.device_key, &file.file_key, &cursor))
            .map_err(|e| e.to_string())?;
        if !cursor_plan.should_read {
            continue;
        }
        alerts_emitted = alerts_emitted.saturating_add(process_file_oneshot_v1(
            runtime,
            cfg,
            &mut dict,
            &mut acc,
            &mut active_spans,
            &mut cursor,
            sink,
            device,
            file,
            &file_path,
            &cursor_plan,
            since,
            until,
            &df_cfg,
            &centroid_cfg,
            &alert_cfg,
            now_ts,
        )?);
    }

    if let Some(acc_final) = acc.take() {
        let finalized = acc_final.finalize_idle_v1();
        alerts_emitted = alerts_emitted.saturating_add(
            runtime
                .with_tenant_db_v1(&tenant_id, now_ts, |db| {
                    finalize_window_oneshot_v1(
                        db,
                        &tenant_id,
                        device,
                        &dict,
                        take_file_spans_from_active_v1(&mut active_spans),
                        sink,
                        finalized.finalized_row.clone(),
                        &df_cfg,
                        &centroid_cfg,
                        &alert_cfg,
                    )
                })
                .map_err(|e| e.to_string())?,
        );
        runtime
            .with_tenant_db_v1(&tenant_id, now_ts, |db| apply_window_finalize_mutations_to_db_v1(db, &finalized.mutations))
            .map_err(|e| e.to_string())?;
    }

    Ok(alerts_emitted)
}

#[allow(clippy::too_many_arguments)]
fn process_file_oneshot_v1(
    runtime: &mut SparxRuntimeV1,
    cfg: &ConfigV1,
    dict: &mut FeatureDictionaryV1,
    acc_opt: &mut Option<WindowAccumulatorV1>,
    active_spans: &mut Vec<ActiveSpanStateV1>,
    cursor: &mut FileCursorV1,
    sink: &mut OneShotSinkV1,
    device: &TenantDeviceV1,
    file: &DiscoveredFileV1,
    file_path: &Path,
    cursor_plan: &CursorPlanV1,
    since: Option<i64>,
    until: Option<i64>,
    df_cfg: &DfRingConfigV1,
    centroid_cfg: &CentroidStatsConfigV1,
    alert_cfg: &AlertScoringConfigV1,
    now_ts: i64,
) -> Result<usize, String> {
    let mut reader = open_file_reader_v1(file_path, file.is_gzip, cursor_plan.start_offset, 1)
        .map_err(|e| format!("open file reader failed: {}", e))?;
    let mut line_buf = Vec::new();
    let mut line_start_offset = cursor_plan.start_offset;
    let mut current_offset = cursor_plan.start_offset;
    let mut alerts_emitted = 0usize;

    loop {
        let chunk = reader.read_chunk_v1().map_err(|e| format!("read file chunk failed: {}", e))?;
        let Some(chunk) = chunk else {
            if !line_buf.is_empty() {
                let line_end = current_offset;
                let line = String::from_utf8_lossy(&line_buf).into_owned();
                alerts_emitted = alerts_emitted.saturating_add(process_line_oneshot_v1(
                    runtime,
                    cfg,
                    dict,
                    acc_opt,
                    active_spans,
                    cursor,
                    sink,
                    device,
                    file,
                    line.trim_end_matches('\n').trim_end_matches('\r'),
                    line_start_offset,
                    line_end,
                    line.as_bytes().len() as u64,
                    since,
                    until,
                    df_cfg,
                    centroid_cfg,
                    alert_cfg,
                    now_ts,
                )?);
                line_buf.clear();
            }
            break;
        };

        current_offset = chunk.offset_end;
        if line_buf.is_empty() {
            line_start_offset = chunk.offset_start;
        }
        line_buf.extend_from_slice(&chunk.data);
        if chunk.data.len() == 1 && chunk.data[0] == b'\n' {
            let line_end = current_offset;
            let line = String::from_utf8_lossy(&line_buf).into_owned();
            alerts_emitted = alerts_emitted.saturating_add(process_line_oneshot_v1(
                runtime,
                cfg,
                dict,
                acc_opt,
                active_spans,
                cursor,
                sink,
                device,
                file,
                line.trim_end_matches('\n').trim_end_matches('\r'),
                line_start_offset,
                line_end,
                line.as_bytes().len() as u64,
                since,
                until,
                df_cfg,
                centroid_cfg,
                alert_cfg,
                now_ts,
            )?);
            line_buf.clear();
        }
    }

    let final_offset = reader.current_source_offset_v1();
    if final_offset > cursor.offset {
        *cursor = apply_cursor_read_progress_v1(cursor, final_offset, cursor.last_read_ts);
        runtime
            .with_tenant_db_v1(&device.tenant_id, now_ts, |db| db.write_cursor_v1(&device.device_key, &file.file_key, cursor))
            .map_err(|e| e.to_string())?;
    }
    Ok(alerts_emitted)
}

#[allow(clippy::too_many_arguments)]
fn process_line_oneshot_v1(
    runtime: &mut SparxRuntimeV1,
    cfg: &ConfigV1,
    dict: &mut FeatureDictionaryV1,
    acc_opt: &mut Option<WindowAccumulatorV1>,
    active_spans: &mut Vec<ActiveSpanStateV1>,
    cursor: &mut FileCursorV1,
    sink: &mut OneShotSinkV1,
    device: &TenantDeviceV1,
    file: &DiscoveredFileV1,
    line: &str,
    offset_start: u64,
    offset_end: u64,
    byte_len: u64,
    since: Option<i64>,
    until: Option<i64>,
    df_cfg: &DfRingConfigV1,
    centroid_cfg: &CentroidStatsConfigV1,
    alert_cfg: &AlertScoringConfigV1,
    now_ts: i64,
) -> Result<usize, String> {
    let parsed = parse_syslog_envelope_v1(line, 0);
    let line_ts = parsed.envelope.ts_guess.unwrap_or(0);

    if since.map(|v| line_ts < v).unwrap_or(false) || until.map(|v| line_ts > v).unwrap_or(false) {
        *cursor = apply_cursor_read_progress_v1(cursor, offset_end, line_ts);
        runtime
            .with_tenant_db_v1(&device.tenant_id, now_ts, |db| db.write_cursor_v1(&device.device_key, &file.file_key, cursor))
            .map_err(|e| e.to_string())?;
        return Ok(0);
    }

    let tokenized = tokenize_message_v1(&parsed.msg, None);
    let emitted = emit_line_features_v1(&parsed.envelope, &tokenized.events);
    let window_start_ts = align_window_start_ts_v1(line_ts, cfg.ingest.window_size_s)
        .map_err(|e| format!("align window failed: {:?}", e))?;

    if acc_opt.is_none() {
        *acc_opt = Some(
            WindowAccumulatorV1::new_v1(
                &device.device_key,
                window_start_ts,
                1,
                cfg.ingest.window_size_s,
                line_ts,
                WindowCapsV1::from(&cfg.caps),
            )
            .map_err(|e| format!("create window accumulator failed: {:?}", e))?,
        );
    }

    loop {
        let acc = acc_opt.as_mut().expect("accumulator present");
        let result = acc
            .apply_line_v1(line_ts, line_ts, usize::try_from(byte_len).unwrap_or(usize::MAX), &emitted, dict)
            .map_err(|e| format!("apply line failed: {:?}", e))?;
        match result {
            WindowApplyLineResultV1::Applied(applied) => {
                runtime
                    .with_tenant_db_v1(&device.tenant_id, now_ts, |db| {
                        apply_feature_dict_writes_to_db_v1(db, &applied.dict_writes)?;
                        apply_window_checkpoint_writes_to_db_v1(db, &acc.checkpoint_writes_v1().map_err(|e| DbErrorV1::new_v1(format!("window checkpoint failed: {:?}", e)))?)
                    })
                    .map_err(|e| e.to_string())?;
                update_active_spans_v1(active_spans, file, cursor.inode, offset_start, offset_end);
                *cursor = apply_cursor_read_progress_v1(cursor, offset_end, line_ts);
                runtime
                    .with_tenant_db_v1(&device.tenant_id, now_ts, |db| db.write_cursor_v1(&device.device_key, &file.file_key, cursor))
                    .map_err(|e| e.to_string())?;
                return Ok(0);
            }
            WindowApplyLineResultV1::DifferentWindow { line_window_start_ts } => {
                let (plan, next) = acc
                    .finalize_and_advance_v1(line_window_start_ts, line_ts)
                    .map_err(|e| format!("finalize and advance failed: {:?}", e))?;
                let finalized_row = plan.finalized_row.clone();
                let alerts = runtime
                    .with_tenant_db_v1(&device.tenant_id, now_ts, |db| {
                        let alerts = finalize_window_oneshot_v1(
                            db,
                            &device.tenant_id,
                            device,
                            dict,
                            take_file_spans_from_active_v1(active_spans),
                            sink,
                            finalized_row,
                            df_cfg,
                            centroid_cfg,
                            alert_cfg,
                        )?;
                        apply_window_finalize_mutations_to_db_v1(db, &plan.mutations)?;
                        Ok(alerts)
                    })
                    .map_err(|e| e.to_string())?;
                *acc_opt = Some(next);
                return Ok(alerts.saturating_add(process_line_oneshot_v1(
                    runtime,
                    cfg,
                    dict,
                    acc_opt,
                    active_spans,
                    cursor,
                    sink,
                    device,
                    file,
                    line,
                    offset_start,
                    offset_end,
                    byte_len,
                    since,
                    until,
                    df_cfg,
                    centroid_cfg,
                    alert_cfg,
                    now_ts,
                )?));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn finalize_window_oneshot_v1(
    db: &crate::db::TenantDbV1,
    tenant_id: &str,
    device: &TenantDeviceV1,
    dict: &FeatureDictionaryV1,
    spans: Vec<FileSpanV1>,
    sink: &mut OneShotSinkV1,
    row: crate::window::FinalizedWindowRowV1,
    df_cfg: &DfRingConfigV1,
    centroid_cfg: &CentroidStatsConfigV1,
    alert_cfg: &AlertScoringConfigV1,
) -> Result<usize, DbErrorV1> {
    let baseline_before = build_bucket_baseline_from_tenant_db_v1(db, &device.device_key, row.key.bucket, df_cfg)?;
    let current_stats = db
        .read_device_baseline_state_v1(&device.device_key, row.key.bucket)?
        .and_then(|state| state.stats);
    let df_meta = load_df_meta_from_tenant_db_v1(db, df_cfg)?;
    let day_epoch = crate::baseline::day_epoch_for_ts_v1(row.key.window_start_ts);
    let slot = crate::baseline::slot_for_day_epoch_v1(day_epoch, df_cfg.df_ring_slots)
        .map_err(|e| DbErrorV1::new_v1(format!("df slot compute failed: {:?}", e)))?;
    let current_slot_bucket = match db.read_df_slot_bucket_state_v1(slot, row.key.bucket)? {
        Some(state) => DfRingSlotBucketStateV1 {
            window_count: state.window_count,
            df_pairs: state.df_pairs,
        },
        None => DfRingSlotBucketStateV1 {
            window_count: 0,
            df_pairs: Vec::new(),
        },
    };
    let stale_slot_keys = collect_stale_slot_keys_from_tenant_db_v1(db, slot)?;
    let preview = build_alert_v1(
        tenant_id,
        &format!("{}/{}", device.tenant_id, device.device_dir_rel),
        &row,
        dict,
        &baseline_before,
        current_stats.as_ref(),
        alert_cfg,
        &spans,
    )
    .map_err(|e| DbErrorV1::new_v1(format!("build alert failed: {:?}", e)))?;

    let df_plan = plan_df_ring_update_v1(&row, df_cfg, &df_meta, &current_slot_bucket, &stale_slot_keys)
        .map_err(|e| DbErrorV1::new_v1(format!("df update failed: {:?}", e)))?;
    apply_df_mutations_to_db_v1(db, &df_plan.mutations)?;

    let centroid_pairs_before = load_centroid_pairs_from_tenant_db_v1(db, &device.device_key, row.key.bucket)?;
    let centroid_plan = plan_centroid_stats_update_v1(
        &row,
        dict,
        centroid_cfg,
        &centroid_pairs_before,
        current_stats.as_ref(),
        Some(preview.score_total),
        row.key.window_end_ts,
    )
    .map_err(|e| DbErrorV1::new_v1(format!("centroid update failed: {:?}", e)))?;
    apply_centroid_mutations_to_db_v1(db, &centroid_plan.mutations)?;

    if let Some(alert) = preview.alert {
        db.write_primary_alert_v1(&alert)?;
        sink.emit_v1(&alert)
            .map_err(|e| DbErrorV1::new_v1(format!("oneshot sink emit failed: {}", e)))?;
        Ok(1)
    } else {
        Ok(0)
    }
}

fn observed_file_state_for_path_v1(path: &Path, is_gzip: bool) -> Result<ObservedFileStateV1, std::io::Error> {
    let md = fs::metadata(path)?;
    #[cfg(unix)]
    let inode = {
        use std::os::unix::fs::MetadataExt;
        md.ino()
    };
    #[cfg(not(unix))]
    let inode = 0u64;
    let mtime = md
        .modified()
        .ok()
        .and_then(|ts| ts.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    Ok(ObservedFileStateV1 {
        inode,
        mtime,
        size: md.len(),
        is_gzip,
    })
}

fn load_dict_from_tenant_db_v1(
    db: &crate::db::TenantDbV1,
    cfg: FeatureDictionaryConfigV1,
) -> Result<FeatureDictionaryV1, DbErrorV1> {
    let next_id = db.get_raw_v1(crate::db::keys::key_tenant_feature_dict_next_id_v1().as_bytes())?;
    if next_id.is_none() {
        return Ok(FeatureDictionaryV1::new_empty_v1(cfg, 1, 0));
    }
    let entries = db
        .get_raw_v1(crate::db::keys::key_tenant_feature_dict_entries_v1().as_bytes())?
        .ok_or_else(|| DbErrorV1::new_v1("feature dict missing entries"))?;
    let last_gc_ts = db.get_raw_v1(crate::db::keys::key_tenant_feature_dict_last_gc_ts_v1().as_bytes())?;
    let meta = FeatureDictionaryMetaV1 {
        next_id: decode_feat_dict_meta_next_id_v1(&next_id.expect("checked above"))
            .map_err(|e| DbErrorV1::new_v1(format!("feature dict next_id decode failed: {:?}", e)))?,
        entries: decode_feat_dict_meta_entries_v1(&entries)
            .map_err(|e| DbErrorV1::new_v1(format!("feature dict entries decode failed: {:?}", e)))?,
        last_gc_ts: match last_gc_ts {
            Some(bytes) => decode_feat_dict_meta_last_gc_ts_v1(&bytes)
                .map_err(|e| DbErrorV1::new_v1(format!("feature dict last_gc_ts decode failed: {:?}", e)))?,
            None => 0,
        },
    };
    let mut forward_entries = Vec::new();
    let mut reverse_entries = Vec::new();
    for (key, value) in db.scan_prefix_raw_v1(b"feat_dict/v1/str/")? {
        let key_text = String::from_utf8(key).map_err(|e| DbErrorV1::new_v1(format!("feature dict key utf8 failed: {}", e)))?;
        let prefix = "feat_dict/v1/str/";
        let feature_string = key_text
            .strip_prefix(prefix)
            .ok_or_else(|| DbErrorV1::new_v1("feature dict str key missing prefix"))?;
        let feature_id = decode_feat_dict_str_to_id_v1(&value)
            .map_err(|e| DbErrorV1::new_v1(format!("feature dict str->id decode failed: {:?}", e)))?;
        forward_entries.push((feature_string.to_string(), feature_id));
    }
    for (key, value) in db.scan_prefix_raw_v1(b"feat_dict/v1/id/")? {
        let key_text = String::from_utf8(key).map_err(|e| DbErrorV1::new_v1(format!("feature dict key utf8 failed: {}", e)))?;
        let prefix = "feat_dict/v1/id/";
        let feature_id_text = key_text
            .strip_prefix(prefix)
            .ok_or_else(|| DbErrorV1::new_v1("feature dict id key missing prefix"))?;
        let feature_id = feature_id_text
            .parse::<u32>()
            .map_err(|e| DbErrorV1::new_v1(format!("feature dict id parse failed: {}", e)))?;
        let feature_string = decode_feat_dict_id_to_str_v1(&value)
            .map_err(|e| DbErrorV1::new_v1(format!("feature dict id->str decode failed: {:?}", e)))?;
        reverse_entries.push((feature_id, feature_string));
    }
    forward_entries.sort_by(|a, b| a.0.cmp(&b.0));
    reverse_entries.sort_by_key(|(feature_id, _)| *feature_id);
    FeatureDictionaryV1::load_persisted_v1(cfg, meta, forward_entries, reverse_entries)
        .map_err(|e| DbErrorV1::new_v1(format!("feature dict load failed: {:?}", e)))
}

fn restore_active_window_from_tenant_db_v1(
    db: &crate::db::TenantDbV1,
    device_key: &str,
    caps: WindowCapsV1,
    dict: &FeatureDictionaryV1,
) -> Result<Option<WindowAccumulatorV1>, DbErrorV1> {
    match db.read_open_window_state_v1(device_key)? {
        Some(state) => WindowAccumulatorV1::from_checkpoint_v1(
            device_key,
            caps,
            state.active,
            state.meta,
            &state.sparse_counts,
            &state.entity_snapshot,
            dict,
        )
        .map(Some)
        .map_err(|e| DbErrorV1::new_v1(format!("restore open window failed: {:?}", e))),
        None => Ok(None),
    }
}

fn load_df_meta_from_tenant_db_v1(
    db: &crate::db::TenantDbV1,
    cfg: &DfRingConfigV1,
) -> Result<DfRingMetaStateV1, DbErrorV1> {
    let mut day_slot_epochs = vec![None; usize::try_from(cfg.df_ring_slots).unwrap()];
    for slot in 0..cfg.df_ring_slots {
        if let Some(value) = db.get_raw_v1(crate::db::keys::key_tenant_df_ring_day_slot_epoch_v1(slot as u8).as_bytes())? {
            day_slot_epochs[slot as usize] = Some(
                decode_meta_df_ring_day_slot_epoch_v1(&value)
                    .map_err(|e| DbErrorV1::new_v1(format!("df day slot epoch decode failed: {:?}", e)))?,
            );
        }
    }
    let current_day_epoch = match db.get_raw_v1(crate::db::keys::key_tenant_df_ring_current_day_epoch_v1().as_bytes())? {
        Some(value) => Some(
            decode_meta_df_ring_current_day_epoch_v1(&value)
                .map_err(|e| DbErrorV1::new_v1(format!("df current day epoch decode failed: {:?}", e)))?,
        ),
        None => None,
    };
    Ok(DfRingMetaStateV1 {
        current_day_epoch,
        day_slot_epochs,
    })
}

fn collect_stale_slot_keys_from_tenant_db_v1(
    db: &crate::db::TenantDbV1,
    slot: u8,
) -> Result<Vec<crate::db::keys::KeyBytes>, DbErrorV1> {
    let mut keys = Vec::new();
    for (key, _) in db.scan_prefix_raw_v1(crate::db::keys::key_prefix_tenant_dfm_slot_v1(slot).as_bytes())? {
        keys.push(crate::db::keys::KeyBytes { bytes: key });
    }
    for (key, _) in db.scan_prefix_raw_v1(crate::db::keys::key_prefix_tenant_dfn_slot_v1(slot).as_bytes())? {
        keys.push(crate::db::keys::KeyBytes { bytes: key });
    }
    keys.sort_by(|a, b| a.bytes.cmp(&b.bytes));
    Ok(keys)
}

fn build_bucket_baseline_from_tenant_db_v1(
    db: &crate::db::TenantDbV1,
    device_key: &str,
    bucket: u8,
    df_cfg: &DfRingConfigV1,
) -> Result<BucketBaselineV1, DbErrorV1> {
    let mut n_bucket = 0u32;
    let mut df_totals: BTreeMap<u32, u32> = BTreeMap::new();
    for slot in 0..df_cfg.df_ring_slots {
        if let Some(state) = db.read_df_slot_bucket_state_v1(slot as u8, bucket)? {
            n_bucket = n_bucket.saturating_add(state.window_count);
            for pair in state.df_pairs {
                let entry = df_totals.entry(pair.feature_id).or_insert(0);
                *entry = (*entry).saturating_add(pair.df_count);
            }
        }
    }
    let baseline_state = db.read_device_baseline_state_v1(device_key, bucket)?;
    let centroid = baseline_state
        .as_ref()
        .map(|state| {
            state
                .centroid
                .iter()
                .map(|pair| CentroidPairV1 {
                    feature_id: pair.feature_id,
                    value: pair.value,
                })
                .collect::<Vec<CentroidPairV1>>()
        })
        .unwrap_or_default();
    let df = df_totals
        .into_iter()
        .map(|(feature_id, df_count)| DfPairV1 { feature_id, df_count })
        .collect::<Vec<DfPairV1>>();
    Ok(BucketBaselineV1 {
        bucket,
        n_bucket,
        df,
        centroid,
    })
}

fn load_centroid_pairs_from_tenant_db_v1(
    db: &crate::db::TenantDbV1,
    device_key: &str,
    bucket: u8,
) -> Result<Vec<crate::db::baseline_sketch::CentroidValuePairV1>, DbErrorV1> {
    Ok(db
        .read_device_baseline_state_v1(device_key, bucket)?
        .map(|state| state.centroid)
        .unwrap_or_default())
}

fn apply_feature_dict_writes_to_db_v1(
    db: &crate::db::TenantDbV1,
    writes: &[crate::features::FeatureDictionaryKvV1],
) -> Result<(), DbErrorV1> {
    for write in writes {
        db.put_raw_v1(write.key.as_bytes(), &write.value)?;
    }
    Ok(())
}

fn apply_window_checkpoint_writes_to_db_v1(
    db: &crate::db::TenantDbV1,
    writes: &[crate::window::WindowCheckpointKvV1],
) -> Result<(), DbErrorV1> {
    for write in writes {
        db.put_raw_v1(write.key.as_bytes(), &write.value)?;
    }
    Ok(())
}

fn apply_window_finalize_mutations_to_db_v1(
    db: &crate::db::TenantDbV1,
    mutations: &[crate::window::WindowFinalizeMutationV1],
) -> Result<(), DbErrorV1> {
    for mutation in mutations {
        match mutation {
            crate::window::WindowFinalizeMutationV1::Put(kv) => db.put_raw_v1(kv.key.as_bytes(), &kv.value)?,
            crate::window::WindowFinalizeMutationV1::Delete(key) => db.delete_raw_v1(key.as_bytes())?,
        }
    }
    Ok(())
}

fn apply_df_mutations_to_db_v1(
    db: &crate::db::TenantDbV1,
    mutations: &[DfRingMutationV1],
) -> Result<(), DbErrorV1> {
    for mutation in mutations {
        match mutation {
            DfRingMutationV1::Put(kv) => db.put_raw_v1(kv.key.as_bytes(), &kv.value)?,
            DfRingMutationV1::Delete(key) => db.delete_raw_v1(key.as_bytes())?,
        }
    }
    Ok(())
}

fn apply_centroid_mutations_to_db_v1(
    db: &crate::db::TenantDbV1,
    mutations: &[crate::baseline::CentroidStatsMutationV1],
) -> Result<(), DbErrorV1> {
    for mutation in mutations {
        match mutation {
            crate::baseline::CentroidStatsMutationV1::Put(kv) => db.put_raw_v1(kv.key.as_bytes(), &kv.value)?,
        }
    }
    Ok(())
}

fn update_active_spans_v1(
    spans: &mut Vec<ActiveSpanStateV1>,
    file: &DiscoveredFileV1,
    inode: u64,
    offset_start: u64,
    offset_end: u64,
) {
    if let Some(last) = spans.last_mut() {
        if last.file_rel == file.file_rel && last.file_key == file.file_key && last.inode == inode && last.is_gzip == file.is_gzip {
            last.offset_end = offset_end;
            return;
        }
    }
    spans.push(ActiveSpanStateV1 {
        file_rel: file.file_rel.clone(),
        file_key: file.file_key.clone(),
        inode,
        offset_start,
        offset_end,
        is_gzip: file.is_gzip,
    });
}

fn take_file_spans_from_active_v1(spans: &mut Vec<ActiveSpanStateV1>) -> Vec<FileSpanV1> {
    let mut out = Vec::with_capacity(spans.len());
    for span in spans.drain(..) {
        out.push(FileSpanV1 {
            file_rel: span.file_rel,
            file_key: span.file_key,
            inode: span.inode,
            offset_start: span.offset_start,
            offset_end: span.offset_end,
            is_gzip: span.is_gzip,
        });
    }
    out
}


fn route_status_v1(cfg: &ConfigV1, json: bool) -> RouteResultV1 {
    let runtime = match SparxRuntimeV1::open_from_config_v1(cfg) {
        Ok(runtime) => runtime,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("status db error: {}", e)),
            };
        }
    };

    let snapshot = match build_status_snapshot_from_runtime_v1(cfg, &runtime) {
        Ok(snapshot) => snapshot,
        Err(e) => {
            return RouteResultV1 {
                exit_code: 4,
                msg_stdout: None,
                msg_stderr: Some(format!("status db error: {}", e)),
            };
        }
    };

    let stdout = if json {
        match serde_json::to_string(&snapshot) {
            Ok(s) => s,
            Err(e) => {
                return RouteResultV1 {
                    exit_code: 4,
                    msg_stdout: None,
                    msg_stderr: Some(format!("status json error: {}", e)),
                };
            }
        }
    } else {
        format_status_text_v1(&snapshot)
    };

    RouteResultV1 {
        exit_code: 0,
        msg_stdout: Some(stdout),
        msg_stderr: None,
    }
}

fn route_config_check_v1(cfg: &ConfigV1) -> RouteResultV1 {
    // Validate that required directories exist or are creatable.
    // This remains a best-effort filesystem check; real DB open checks land in
    // later phases, but canonical path derivation now comes from db::layout.
    let mut errs: Vec<String> = Vec::new();
    let layout = filesystem_layout_v1(cfg);

    // Tenant root must exist and be readable.
    let tenant_root = layout.tenant_root_v1();
    if let Err(e) = fs::read_dir(&tenant_root) {
        errs.push(format!(
            "tenant_root not readable: {}: {}",
            tenant_root.display(),
            e
        ));
    }

    let data_root = layout.data_root_v1();
    if let Err(e) = ensure_dir(data_root.as_path()) {
        errs.push(format!(
            "data_root not writable/creatable: {}: {}",
            data_root.display(),
            e
        ));
    }

    let global_db_path = layout.global_db_path_v1();
    if let Err(e) = ensure_dir(global_db_path.as_path()) {
        errs.push(format!(
            "global_db_path not writable/creatable: {}: {}",
            global_db_path.display(),
            e
        ));
    }

    let tenant_db_root = layout.tenant_db_root_v1();
    if let Err(e) = ensure_dir(tenant_db_root.as_path()) {
        errs.push(format!(
            "tenant_db_root not writable/creatable: {}: {}",
            tenant_db_root.display(),
            e
        ));
    }

    let alert_out_root = layout.alert_out_root_v1();
    if let Err(e) = ensure_dir(alert_out_root.as_path()) {
        errs.push(format!(
            "alert_out_root not writable/creatable: {}: {}",
            alert_out_root.display(),
            e
        ));
    }

    let spool_root = layout.spool_root_v1();
    if let Err(e) = ensure_dir(spool_root.as_path()) {
        errs.push(format!(
            "spool_root not writable/creatable: {}: {}",
            spool_root.display(),
            e
        ));
    }

    if errs.is_empty() {
        return RouteResultV1 {
            exit_code: 0,
            msg_stdout: Some("config check ok".to_string()),
            msg_stderr: None,
        };
    }

    let mut s = String::new();
    s.push_str("config check failed\n");
    for e in errs {
        s.push_str("- ");
        s.push_str(&e);
        s.push('\n');
    }

    RouteResultV1 {
        exit_code: 3, // IO error
        msg_stdout: None,
        msg_stderr: Some(s),
    }
}

fn ensure_dir(path: &std::path::Path) -> Result<(), String> {
    match fs::metadata(path) {
        Ok(md) => {
            if md.is_dir() {
                Ok(())
            } else {
                Err("exists but is not a directory".to_string())
            }
        }
        Err(_) => fs::create_dir_all(path).map_err(|e| e.to_string()),
    }
}

fn route_validate_fixtures_v1(fixture_root: &str) -> RouteResultV1 {
    match crate::fixture_validate::validate_fixture_root_v1(std::path::Path::new(fixture_root)) {
        Ok(report) if report.is_ok() => RouteResultV1 {
            exit_code: 0,
            msg_stdout: Some(format!(
                "fixture validation ok\nroot: {}\ntenants: {}\ndevice_files: {}\ngolden_files: {}\ngen_files: {}\n",
                report.root,
                report.tenant_count,
                report.device_file_count,
                report.golden_file_count,
                report.gen_file_count,
            )),
            msg_stderr: None,
        },
        Ok(report) => {
            let mut s = String::new();
            s.push_str("fixture validation failed\n");
            s.push_str(&format!("root: {}\n", report.root));
            for err in report.errors {
                s.push_str("- ");
                s.push_str(&err);
                s.push('\n');
            }
            RouteResultV1 {
                exit_code: 1,
                msg_stdout: None,
                msg_stderr: Some(s),
            }
        }
        Err(e) => RouteResultV1 {
            exit_code: 3,
            msg_stdout: None,
            msg_stderr: Some(format!("fixture validation IO error: {}", e)),
        },
    }
}
