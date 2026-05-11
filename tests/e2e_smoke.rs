// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use sparx::alert::{build_alert_v1, AlertScoringConfigV1, AlertV1, FileSpanV1};
use sparx::baseline::{
    plan_centroid_stats_update_v1, plan_df_ring_update_v1, BucketBaselineV1, CentroidPairV1,
    CentroidStatsConfigV1, DfPairV1, DfRingConfigV1, DfRingMetaStateV1, DfRingMutationV1,
    DfRingSlotBucketStateV1,
};
use sparx::db::baseline_sketch::{
    decode_centroid_v1, decode_dfm_v1, decode_dfn_v1, decode_stats_v1, CentroidValuePairV1,
    DeviceStatsV1,
};
use sparx::db::keys::{
    key_tenant_active_window_v1, key_tenant_centroid_v1, key_tenant_cursor_inode_v1,
    key_tenant_cursor_is_gzip_v1, key_tenant_cursor_last_read_ts_v1, key_tenant_cursor_mtime_v1,
    key_tenant_cursor_offset_v1, key_tenant_cursor_size_v1,
    key_tenant_df_ring_current_day_epoch_v1, key_tenant_df_ring_day_slot_epoch_v1,
    key_tenant_dfm_v1, key_tenant_dfn_v1, key_tenant_feature_dict_entries_v1,
    key_tenant_feature_dict_last_gc_ts_v1, key_tenant_feature_dict_next_id_v1, key_tenant_stats_v1,
    key_tenant_window_row_ent_domain_v1, key_tenant_window_row_ent_dstip_v1,
    key_tenant_window_row_ent_host_v1, key_tenant_window_row_ent_srcip_v1,
    key_tenant_window_row_ent_userid_v1, key_tenant_window_row_feat_v1,
    key_tenant_window_row_meta_v1,
};
use sparx::db::open_window::{
    decode_win_active_v1, decode_win_row_ent_domain_v1, decode_win_row_ent_dstip_v1,
    decode_win_row_ent_host_v1, decode_win_row_ent_srcip_v1, decode_win_row_ent_userid_v1,
    decode_win_row_feat_v1, decode_win_row_meta_v1,
};
use sparx::db::tenant_values::{
    decode_cursor_inode_v1, decode_cursor_is_gzip_v1, decode_cursor_last_read_ts_v1,
    decode_cursor_mtime_v1, decode_cursor_offset_v1, decode_cursor_size_v1,
    decode_feat_dict_id_to_str_v1, decode_feat_dict_meta_entries_v1,
    decode_feat_dict_meta_last_gc_ts_v1, decode_feat_dict_meta_next_id_v1,
    decode_feat_dict_str_to_id_v1, decode_meta_df_ring_current_day_epoch_v1,
    decode_meta_df_ring_day_slot_epoch_v1, encode_cursor_inode_v1, encode_cursor_is_gzip_v1,
    encode_cursor_last_read_ts_v1, encode_cursor_mtime_v1, encode_cursor_offset_v1,
    encode_cursor_size_v1,
};
use sparx::features::{
    emit_line_features_v1, EntitySketchSnapshotV1, FeatureDictionaryConfigV1,
    FeatureDictionaryMetaV1, FeatureDictionaryV1,
};
use sparx::fixture_validate::validate_fixture_root_v1;
use sparx::ingest::{
    apply_cursor_read_progress_v1, discover_device_inventory_v1, open_file_reader_v1,
    reconcile_cursor_v1, CursorPlanV1, DiscoveredFileV1, FileCursorV1, ObservedFileStateV1,
    TenantDeviceV1,
};
use sparx::sink::{JsonlAlertSinkV1, JsonlSinkConfigV1};
use sparx::tokenize::{parse_syslog_envelope_v1, tokenize_message_v1};
use sparx::window::{
    align_window_start_ts_v1, WindowAccumulatorV1, WindowApplyLineResultV1, WindowCapsV1,
};

static NEXT_TMP_ID_V1: AtomicU64 = AtomicU64::new(1);
const FIXTURE_TENANT_V1: &str = "smoke";
const FIXTURE_LOG_V1: &str = "edge01.log";
const DEVICE_DIR_V1: &str = "edge01";
const READ_CHUNK_BYTES_V1: usize = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
struct ActiveSpanStateV1 {
    file_rel: String,
    file_key: String,
    inode: u64,
    offset_start: u64,
    offset_end: u64,
    is_gzip: bool,
}

#[derive(Clone, Debug)]
struct PipelineSnapshotV1 {
    tenant_db: BTreeMap<String, Vec<u8>>,
    active_span: Option<ActiveSpanStateV1>,
}

#[derive(Clone, Debug)]
struct RunResultV1 {
    snapshot: PipelineSnapshotV1,
    emitted_alerts: Vec<AlertV1>,
    final_cursor: FileCursorV1,
}

#[test]
fn repo_fixture_corpus_validates_and_smoke_pipeline_emits_alert_with_provenance() {
    let fixture_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    let report = validate_fixture_root_v1(&fixture_root).unwrap();
    assert!(
        report.is_ok(),
        "fixture validation errors: {:?}",
        report.errors
    );
    assert!(report.device_file_count >= 2);

    let run = run_full_fixture_v1();
    assert_eq!(run.final_cursor.offset, run.final_cursor.size);
    assert!(
        !run.emitted_alerts.is_empty(),
        "expected at least one alert"
    );

    let alert = &run.emitted_alerts[0];
    assert_eq!(alert.tenant_id, FIXTURE_TENANT_V1);
    assert_eq!(
        alert.device_path,
        format!("{}/{}", FIXTURE_TENANT_V1, DEVICE_DIR_V1)
    );
    assert!(!alert.provenance.is_empty());
    assert_eq!(alert.provenance[0].file_rel, "auth.log");
    assert!(alert.provenance[0].offset_end > alert.provenance[0].offset_start);
}

#[test]
fn restart_recovery_matches_single_pass_alerts_and_cursor() {
    let expected = run_full_fixture_v1();

    let partial = run_fixture_v1(None, Some(3));
    assert!(
        partial.snapshot.active_span.is_some(),
        "expected active window state after partial run"
    );
    assert!(partial.final_cursor.offset < partial.final_cursor.size);

    let resumed = run_fixture_v1(Some(partial.snapshot.clone()), None);

    let mut actual_alerts = partial.emitted_alerts;
    actual_alerts.extend(resumed.emitted_alerts);

    assert_eq!(actual_alerts, expected.emitted_alerts);
    assert_eq!(resumed.final_cursor.offset, expected.final_cursor.offset);
    assert_eq!(resumed.final_cursor.size, expected.final_cursor.size);
}

fn run_full_fixture_v1() -> RunResultV1 {
    run_fixture_v1(None, None)
}

fn run_fixture_v1(
    snapshot: Option<PipelineSnapshotV1>,
    stop_after_lines: Option<usize>,
) -> RunResultV1 {
    let temp_root = make_temp_root_v1("e2e_smoke");
    let watch_root = temp_root.join("watch_root");
    let alerts_root = temp_root.join("alerts_out");
    let state_root = temp_root.join("state_root");
    build_watch_root_from_fixture_v1(&watch_root);

    let inventory = discover_device_inventory_v1(&watch_root, false).unwrap();
    assert_eq!(inventory.len(), 1);
    let device_inventory = &inventory[0];
    assert_eq!(device_inventory.files.len(), 1);

    let device = &device_inventory.device;
    let file = &device_inventory.files[0];
    let file_path = watch_root
        .join(&device.tenant_id)
        .join(&device.device_dir_rel)
        .join(&file.file_rel);
    let observed = observed_file_state_v1(&file_path, file.is_gzip);

    let mut tenant_db = snapshot
        .as_ref()
        .map(|snap| snap.tenant_db.clone())
        .unwrap_or_default();
    let mut active_span = snapshot.and_then(|snap| snap.active_span);

    let mut sink = JsonlAlertSinkV1::new(JsonlSinkConfigV1 {
        alert_out_root: alerts_root.to_string_lossy().to_string(),
        jsonl_rotate_mb: 256,
        jsonl_flush_interval_s: 0,
    });

    let previous_cursor = load_cursor_from_db_v1(&tenant_db, &device.device_key, &file.file_key);
    let cursor_plan = reconcile_cursor_v1(previous_cursor.as_ref(), &observed);
    let emitted_alerts = process_file_v1(
        &mut tenant_db,
        &mut active_span,
        device,
        file,
        &file_path,
        &cursor_plan,
        stop_after_lines,
        &mut sink,
    );
    sink.shutdown_v1().unwrap();

    let final_cursor =
        load_cursor_from_db_v1(&tenant_db, &device.device_key, &file.file_key).unwrap();
    let alerts_from_sink = read_alerts_from_jsonl_root_v1(&alerts_root);
    assert_eq!(alerts_from_sink, emitted_alerts);

    let _ = fs::create_dir_all(&state_root);

    RunResultV1 {
        snapshot: PipelineSnapshotV1 {
            tenant_db,
            active_span,
        },
        emitted_alerts: alerts_from_sink,
        final_cursor,
    }
}

#[allow(clippy::too_many_arguments)]
fn process_file_v1(
    tenant_db: &mut BTreeMap<String, Vec<u8>>,
    active_span: &mut Option<ActiveSpanStateV1>,
    device: &TenantDeviceV1,
    file: &DiscoveredFileV1,
    file_path: &Path,
    cursor_plan: &CursorPlanV1,
    stop_after_lines: Option<usize>,
    sink: &mut JsonlAlertSinkV1,
) -> Vec<AlertV1> {
    let dict_cfg = FeatureDictionaryConfigV1 {
        dict_enabled: true,
        dict_max_entries: 50_000,
    };
    let caps = base_window_caps_v1();
    let df_cfg = DfRingConfigV1 {
        df_ring_slots: 7,
        df_bucket_count: 48,
        df_map_cap: 50_000,
    };
    let centroid_cfg = CentroidStatsConfigV1 {
        centroid_alpha: 0.5,
        centroid_cap: 10_000,
    };
    let alert_cfg = AlertScoringConfigV1 {
        outlier_threshold: 0.20,
        noise_threshold: 0.75,
        cold_start_min_windows: 1,
        include_debug_fields: true,
        ..Default::default()
    };

    let mut dict = load_dict_from_db_v1(tenant_db, dict_cfg.clone());
    let mut acc =
        restore_active_window_from_db_v1(tenant_db, &device.device_key, caps.clone(), &dict);
    let mut cursor = cursor_plan.cursor.clone();
    write_cursor_to_db_v1(tenant_db, &device.device_key, &file.file_key, &cursor);

    let mut reader = open_file_reader_v1(
        file_path,
        file.is_gzip,
        cursor_plan.start_offset,
        READ_CHUNK_BYTES_V1,
    )
    .unwrap();
    let mut emitted_alerts = Vec::new();
    let mut line_buf = Vec::new();
    let mut line_start_offset = cursor_plan.start_offset;
    let mut current_offset = cursor_plan.start_offset;
    let mut lines_processed = 0usize;

    loop {
        if stop_after_lines == Some(lines_processed) {
            break;
        }

        let chunk = reader.read_chunk_v1().unwrap();
        let Some(chunk) = chunk else {
            if !line_buf.is_empty() {
                let line_end = current_offset;
                let line = String::from_utf8_lossy(&line_buf).into_owned();
                process_line_v1(
                    tenant_db,
                    &mut dict,
                    &mut acc,
                    active_span,
                    &mut cursor,
                    &mut emitted_alerts,
                    sink,
                    device,
                    file,
                    line.trim_end_matches('\n').trim_end_matches('\r'),
                    line_start_offset,
                    line_end,
                    line.len() as u64,
                    &df_cfg,
                    &centroid_cfg,
                    &alert_cfg,
                );
                lines_processed += 1;
                line_buf.clear();
                if stop_after_lines == Some(lines_processed) {
                    break;
                }
            }
            break;
        };

        current_offset = chunk.offset_end;
        if line_buf.is_empty() {
            line_start_offset = chunk.offset_start;
        }
        line_buf.extend_from_slice(&chunk.data);
        if chunk.data[0] == b'\n' {
            let line_end = current_offset;
            let line = String::from_utf8_lossy(&line_buf).into_owned();
            process_line_v1(
                tenant_db,
                &mut dict,
                &mut acc,
                active_span,
                &mut cursor,
                &mut emitted_alerts,
                sink,
                device,
                file,
                line.trim_end_matches('\n').trim_end_matches('\r'),
                line_start_offset,
                line_end,
                line.len() as u64,
                &df_cfg,
                &centroid_cfg,
                &alert_cfg,
            );
            lines_processed += 1;
            line_buf.clear();
        }
    }

    if stop_after_lines != Some(lines_processed) {
        if let Some(acc) = acc.take() {
            finalize_window_v1(
                tenant_db,
                &dict,
                active_span.take(),
                &mut emitted_alerts,
                sink,
                device,
                acc.finalize_idle_v1().finalized_row,
                &df_cfg,
                &centroid_cfg,
                &alert_cfg,
            );
            apply_finalize_idle_deletes_v1(
                tenant_db,
                &device.device_key,
                acc.active_v1().active_window_id,
            );
        }
    }

    emitted_alerts
}

#[allow(clippy::too_many_arguments)]
fn process_line_v1(
    tenant_db: &mut BTreeMap<String, Vec<u8>>,
    dict: &mut FeatureDictionaryV1,
    acc_opt: &mut Option<WindowAccumulatorV1>,
    active_span: &mut Option<ActiveSpanStateV1>,
    cursor: &mut FileCursorV1,
    emitted_alerts: &mut Vec<AlertV1>,
    sink: &mut JsonlAlertSinkV1,
    device: &TenantDeviceV1,
    file: &DiscoveredFileV1,
    line: &str,
    offset_start: u64,
    offset_end: u64,
    byte_len: u64,
    df_cfg: &DfRingConfigV1,
    centroid_cfg: &CentroidStatsConfigV1,
    alert_cfg: &AlertScoringConfigV1,
) {
    let parsed = parse_syslog_envelope_v1(line, 0);
    let line_ts = parsed.envelope.ts_guess.unwrap_or(0);
    let tokenized = tokenize_message_v1(&parsed.msg, None);
    let emitted = emit_line_features_v1(&parsed.envelope, &tokenized.events);
    let window_start_ts = align_window_start_ts_v1(line_ts, 60).unwrap();

    if acc_opt.is_none() {
        *acc_opt = Some(
            WindowAccumulatorV1::new_v1(
                &device.device_key,
                window_start_ts,
                next_window_id_v1(tenant_db, &device.device_key),
                60,
                line_ts,
                base_window_caps_v1(),
            )
            .unwrap(),
        );
    }

    loop {
        let acc = acc_opt.as_mut().unwrap();
        let result = acc
            .apply_line_v1(
                line_ts,
                line_ts,
                usize::try_from(byte_len).unwrap(),
                &emitted,
                dict,
            )
            .unwrap();
        match result {
            WindowApplyLineResultV1::Applied(applied) => {
                for kv in applied.dict_writes {
                    insert_kv_v1(tenant_db, kv.key.bytes, kv.value);
                }
                for kv in acc.checkpoint_writes_v1().unwrap() {
                    insert_kv_v1(tenant_db, kv.key.bytes, kv.value);
                }
                update_active_span_v1(active_span, file, 1, offset_start, offset_end);
                *cursor = apply_cursor_read_progress_v1(cursor, offset_end, line_ts);
                write_cursor_to_db_v1(tenant_db, &device.device_key, &file.file_key, cursor);
                break;
            }
            WindowApplyLineResultV1::DifferentWindow {
                line_window_start_ts,
            } => {
                let (plan, next) = acc
                    .finalize_and_advance_v1(line_window_start_ts, line_ts)
                    .unwrap();
                let finalized_row = plan.finalized_row.clone();
                for mutation in plan.mutations {
                    match mutation {
                        sparx::window::WindowFinalizeMutationV1::Put(kv) => {
                            insert_kv_v1(tenant_db, kv.key.bytes, kv.value)
                        }
                        sparx::window::WindowFinalizeMutationV1::Delete(key) => {
                            tenant_db.remove(&String::from_utf8(key.bytes).unwrap());
                        }
                    }
                }
                finalize_window_v1(
                    tenant_db,
                    dict,
                    active_span.take(),
                    emitted_alerts,
                    sink,
                    device,
                    finalized_row,
                    df_cfg,
                    centroid_cfg,
                    alert_cfg,
                );
                *acc_opt = Some(next);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn finalize_window_v1(
    tenant_db: &mut BTreeMap<String, Vec<u8>>,
    dict: &FeatureDictionaryV1,
    span: Option<ActiveSpanStateV1>,
    emitted_alerts: &mut Vec<AlertV1>,
    sink: &mut JsonlAlertSinkV1,
    device: &TenantDeviceV1,
    row: sparx::window::FinalizedWindowRowV1,
    df_cfg: &DfRingConfigV1,
    centroid_cfg: &CentroidStatsConfigV1,
    alert_cfg: &AlertScoringConfigV1,
) {
    let baseline_before =
        build_bucket_baseline_from_db_v1(tenant_db, &device.device_key, row.key.bucket, df_cfg);
    let current_stats = load_device_stats_v1(tenant_db, &device.device_key, row.key.bucket);

    let df_meta = load_df_meta_v1(tenant_db, df_cfg);
    let day_epoch = sparx::baseline::day_epoch_for_ts_v1(row.key.window_start_ts);
    let slot = sparx::baseline::slot_for_day_epoch_v1(day_epoch, df_cfg.df_ring_slots).unwrap();
    let current_slot_bucket = load_df_slot_bucket_state_v1(tenant_db, slot, row.key.bucket);
    let stale_slot_keys = collect_stale_slot_keys_v1(tenant_db, slot);
    let df_plan = plan_df_ring_update_v1(
        &row,
        df_cfg,
        &df_meta,
        &current_slot_bucket,
        &stale_slot_keys,
    )
    .unwrap();
    let preview = build_alert_v1(
        &device.tenant_id,
        &format!("{}/{}", device.tenant_id, device.device_dir_rel),
        &row,
        dict,
        &baseline_before,
        current_stats.as_ref(),
        alert_cfg,
        &span
            .clone()
            .into_iter()
            .map(file_span_from_active_v1)
            .collect::<Vec<FileSpanV1>>(),
    )
    .unwrap();

    apply_df_mutations_v1(tenant_db, &df_plan.mutations);

    let centroid_pairs_before =
        load_centroid_pairs_v1(tenant_db, &device.device_key, row.key.bucket);
    let centroid_plan = plan_centroid_stats_update_v1(
        &row,
        dict,
        centroid_cfg,
        &centroid_pairs_before,
        current_stats.as_ref(),
        Some(preview.score_total),
        row.key.window_end_ts,
    )
    .unwrap();
    apply_centroid_mutations_v1(tenant_db, &centroid_plan.mutations);

    if let Some(alert) = preview.alert {
        sink.emit_at_v1(&alert, alert.window_end_ts).unwrap();
        emitted_alerts.push(alert);
    }
}

fn apply_finalize_idle_deletes_v1(
    db: &mut BTreeMap<String, Vec<u8>>,
    device_key: &str,
    window_id: u64,
) {
    let keys = vec![
        key_tenant_window_row_feat_v1(device_key, window_id),
        key_tenant_window_row_meta_v1(device_key, window_id),
        key_tenant_window_row_ent_srcip_v1(device_key, window_id),
        key_tenant_window_row_ent_dstip_v1(device_key, window_id),
        key_tenant_window_row_ent_userid_v1(device_key, window_id),
        key_tenant_window_row_ent_domain_v1(device_key, window_id),
        key_tenant_window_row_ent_host_v1(device_key, window_id),
        key_tenant_active_window_v1(device_key),
    ];
    for key in keys {
        db.remove(&String::from_utf8(key.bytes).unwrap());
    }
}

fn observed_file_state_v1(path: &Path, is_gzip: bool) -> ObservedFileStateV1 {
    let md = fs::metadata(path).unwrap();
    ObservedFileStateV1 {
        inode: 1,
        mtime: 1_704_460_800,
        size: md.len(),
        is_gzip,
    }
}

fn load_cursor_from_db_v1(
    db: &BTreeMap<String, Vec<u8>>,
    device_key: &str,
    file_key: &str,
) -> Option<FileCursorV1> {
    let inode = db
        .get(&key_string_v1(key_tenant_cursor_inode_v1(
            device_key, file_key,
        )))
        .map(|v| decode_cursor_inode_v1(v).unwrap())?;
    let mtime = decode_cursor_mtime_v1(
        db.get(&key_string_v1(key_tenant_cursor_mtime_v1(
            device_key, file_key,
        )))
        .unwrap(),
    )
    .unwrap();
    let size = decode_cursor_size_v1(
        db.get(&key_string_v1(key_tenant_cursor_size_v1(
            device_key, file_key,
        )))
        .unwrap(),
    )
    .unwrap();
    let offset = decode_cursor_offset_v1(
        db.get(&key_string_v1(key_tenant_cursor_offset_v1(
            device_key, file_key,
        )))
        .unwrap(),
    )
    .unwrap();
    let is_gzip = decode_cursor_is_gzip_v1(
        db.get(&key_string_v1(key_tenant_cursor_is_gzip_v1(
            device_key, file_key,
        )))
        .unwrap(),
    )
    .unwrap();
    let last_read_ts = decode_cursor_last_read_ts_v1(
        db.get(&key_string_v1(key_tenant_cursor_last_read_ts_v1(
            device_key, file_key,
        )))
        .unwrap(),
    )
    .unwrap();
    Some(FileCursorV1 {
        inode,
        mtime,
        size,
        offset,
        is_gzip,
        last_read_ts,
    })
}

fn write_cursor_to_db_v1(
    db: &mut BTreeMap<String, Vec<u8>>,
    device_key: &str,
    file_key: &str,
    cursor: &FileCursorV1,
) {
    insert_kv_v1(
        db,
        key_tenant_cursor_inode_v1(device_key, file_key).bytes,
        encode_cursor_inode_v1(cursor.inode),
    );
    insert_kv_v1(
        db,
        key_tenant_cursor_mtime_v1(device_key, file_key).bytes,
        encode_cursor_mtime_v1(cursor.mtime),
    );
    insert_kv_v1(
        db,
        key_tenant_cursor_size_v1(device_key, file_key).bytes,
        encode_cursor_size_v1(cursor.size),
    );
    insert_kv_v1(
        db,
        key_tenant_cursor_offset_v1(device_key, file_key).bytes,
        encode_cursor_offset_v1(cursor.offset),
    );
    insert_kv_v1(
        db,
        key_tenant_cursor_is_gzip_v1(device_key, file_key).bytes,
        encode_cursor_is_gzip_v1(cursor.is_gzip),
    );
    insert_kv_v1(
        db,
        key_tenant_cursor_last_read_ts_v1(device_key, file_key).bytes,
        encode_cursor_last_read_ts_v1(cursor.last_read_ts),
    );
}

fn load_dict_from_db_v1(
    db: &BTreeMap<String, Vec<u8>>,
    cfg: FeatureDictionaryConfigV1,
) -> FeatureDictionaryV1 {
    let next_id_key = key_string_v1(key_tenant_feature_dict_next_id_v1());
    if !db.contains_key(&next_id_key) {
        return FeatureDictionaryV1::new_empty_v1(cfg, 1, 0);
    }

    let meta = FeatureDictionaryMetaV1 {
        next_id: decode_feat_dict_meta_next_id_v1(db.get(&next_id_key).unwrap()).unwrap(),
        entries: decode_feat_dict_meta_entries_v1(
            db.get(&key_string_v1(key_tenant_feature_dict_entries_v1()))
                .unwrap(),
        )
        .unwrap(),
        last_gc_ts: db
            .get(&key_string_v1(key_tenant_feature_dict_last_gc_ts_v1()))
            .map(|v| decode_feat_dict_meta_last_gc_ts_v1(v).unwrap())
            .unwrap_or(0),
    };

    let mut forward_entries = Vec::new();
    let mut reverse_entries = Vec::new();
    for (key, value) in db {
        if let Some(feature_string) = key.strip_prefix("feat_dict/v1/str/") {
            forward_entries.push((
                feature_string.to_string(),
                decode_feat_dict_str_to_id_v1(value).unwrap(),
            ));
        } else if key.starts_with("feat_dict/v1/id/") {
            let feature_id: u32 = key.rsplit('/').next().unwrap().parse().unwrap();
            reverse_entries.push((feature_id, decode_feat_dict_id_to_str_v1(value).unwrap()));
        }
    }
    forward_entries.sort_by(|a, b| a.0.cmp(&b.0));
    reverse_entries.sort_by_key(|(feature_id, _)| *feature_id);

    FeatureDictionaryV1::load_persisted_v1(cfg, meta, forward_entries, reverse_entries).unwrap()
}

fn restore_active_window_from_db_v1(
    db: &BTreeMap<String, Vec<u8>>,
    device_key: &str,
    caps: WindowCapsV1,
    dict: &FeatureDictionaryV1,
) -> Option<WindowAccumulatorV1> {
    let active = db.get(&key_string_v1(key_tenant_active_window_v1(device_key)))?;
    let active = decode_win_active_v1(active).unwrap();
    let feat = decode_win_row_feat_v1(
        db.get(&key_string_v1(key_tenant_window_row_feat_v1(
            device_key,
            active.active_window_id,
        )))
        .unwrap(),
    )
    .unwrap();
    let meta = decode_win_row_meta_v1(
        db.get(&key_string_v1(key_tenant_window_row_meta_v1(
            device_key,
            active.active_window_id,
        )))
        .unwrap(),
    )
    .unwrap();
    let snapshot = EntitySketchSnapshotV1 {
        srcips: decode_win_row_ent_srcip_v1(
            db.get(&key_string_v1(key_tenant_window_row_ent_srcip_v1(
                device_key,
                active.active_window_id,
            )))
            .unwrap(),
        )
        .unwrap(),
        dstips: decode_win_row_ent_dstip_v1(
            db.get(&key_string_v1(key_tenant_window_row_ent_dstip_v1(
                device_key,
                active.active_window_id,
            )))
            .unwrap(),
        )
        .unwrap(),
        userids: decode_win_row_ent_userid_v1(
            db.get(&key_string_v1(key_tenant_window_row_ent_userid_v1(
                device_key,
                active.active_window_id,
            )))
            .unwrap(),
        )
        .unwrap(),
        domains: decode_win_row_ent_domain_v1(
            db.get(&key_string_v1(key_tenant_window_row_ent_domain_v1(
                device_key,
                active.active_window_id,
            )))
            .unwrap(),
        )
        .unwrap(),
        hosts: decode_win_row_ent_host_v1(
            db.get(&key_string_v1(key_tenant_window_row_ent_host_v1(
                device_key,
                active.active_window_id,
            )))
            .unwrap(),
        )
        .unwrap(),
    };
    Some(
        WindowAccumulatorV1::from_checkpoint_v1(
            device_key, caps, active, meta, &feat, &snapshot, dict,
        )
        .unwrap(),
    )
}

fn load_df_meta_v1(db: &BTreeMap<String, Vec<u8>>, cfg: &DfRingConfigV1) -> DfRingMetaStateV1 {
    let mut day_slot_epochs = vec![None; usize::try_from(cfg.df_ring_slots).unwrap()];
    for slot in 0..cfg.df_ring_slots {
        let key = key_string_v1(key_tenant_df_ring_day_slot_epoch_v1(slot as u8));
        if let Some(value) = db.get(&key) {
            day_slot_epochs[slot as usize] =
                Some(decode_meta_df_ring_day_slot_epoch_v1(value).unwrap());
        }
    }
    DfRingMetaStateV1 {
        current_day_epoch: db
            .get(&key_string_v1(key_tenant_df_ring_current_day_epoch_v1()))
            .map(|value| decode_meta_df_ring_current_day_epoch_v1(value).unwrap()),
        day_slot_epochs,
    }
}

fn load_df_slot_bucket_state_v1(
    db: &BTreeMap<String, Vec<u8>>,
    slot: u8,
    bucket: u8,
) -> DfRingSlotBucketStateV1 {
    let window_count = db
        .get(&key_string_v1(key_tenant_dfn_v1(slot, bucket)))
        .map(|value| decode_dfn_v1(value).unwrap())
        .unwrap_or(0);
    let df_pairs = db
        .get(&key_string_v1(key_tenant_dfm_v1(slot, bucket)))
        .map(|value| decode_dfm_v1(value).unwrap())
        .unwrap_or_default();
    DfRingSlotBucketStateV1 {
        window_count,
        df_pairs,
    }
}

fn collect_stale_slot_keys_v1(
    db: &BTreeMap<String, Vec<u8>>,
    slot: u8,
) -> Vec<sparx::db::keys::KeyBytes> {
    let dfm_prefix = format!("dfM/v1/{}/", slot);
    let dfn_prefix = format!("dfN/v1/{}/", slot);
    let mut keys = Vec::new();
    for key in db.keys() {
        if key.starts_with(&dfm_prefix) || key.starts_with(&dfn_prefix) {
            keys.push(sparx::db::keys::KeyBytes {
                bytes: key.as_bytes().to_vec(),
            });
        }
    }
    keys
}

fn apply_df_mutations_v1(db: &mut BTreeMap<String, Vec<u8>>, mutations: &[DfRingMutationV1]) {
    for mutation in mutations {
        match mutation {
            DfRingMutationV1::Put(kv) => insert_kv_v1(db, kv.key.bytes.clone(), kv.value.clone()),
            DfRingMutationV1::Delete(key) => {
                db.remove(&String::from_utf8(key.bytes.clone()).unwrap());
            }
        }
    }
}

fn build_bucket_baseline_from_db_v1(
    db: &BTreeMap<String, Vec<u8>>,
    device_key: &str,
    bucket: u8,
    df_cfg: &DfRingConfigV1,
) -> BucketBaselineV1 {
    let mut n_bucket = 0u32;
    let mut df_totals: BTreeMap<u32, u32> = BTreeMap::new();
    for slot in 0..df_cfg.df_ring_slots {
        let slot = slot as u8;
        if let Some(value) = db.get(&key_string_v1(key_tenant_dfn_v1(slot, bucket))) {
            n_bucket = n_bucket.saturating_add(decode_dfn_v1(value).unwrap());
        }
        if let Some(value) = db.get(&key_string_v1(key_tenant_dfm_v1(slot, bucket))) {
            for pair in decode_dfm_v1(value).unwrap() {
                let entry = df_totals.entry(pair.feature_id).or_insert(0u32);
                *entry = (*entry).saturating_add(pair.df_count);
            }
        }
    }
    let df = df_totals
        .into_iter()
        .map(|(feature_id, df_count)| DfPairV1 {
            feature_id,
            df_count,
        })
        .collect();
    let centroid = load_centroid_pairs_v1(db, device_key, bucket)
        .into_iter()
        .map(|pair| CentroidPairV1 {
            feature_id: pair.feature_id,
            value: pair.value,
        })
        .collect();
    BucketBaselineV1 {
        bucket,
        n_bucket,
        df,
        centroid,
    }
}

fn load_centroid_pairs_v1(
    db: &BTreeMap<String, Vec<u8>>,
    device_key: &str,
    bucket: u8,
) -> Vec<CentroidValuePairV1> {
    db.get(&key_string_v1(key_tenant_centroid_v1(device_key, bucket)))
        .map(|value| decode_centroid_v1(value).unwrap())
        .unwrap_or_default()
}

fn load_device_stats_v1(
    db: &BTreeMap<String, Vec<u8>>,
    device_key: &str,
    bucket: u8,
) -> Option<DeviceStatsV1> {
    db.get(&key_string_v1(key_tenant_stats_v1(device_key, bucket)))
        .map(|value| decode_stats_v1(value).unwrap())
}

fn apply_centroid_mutations_v1(
    db: &mut BTreeMap<String, Vec<u8>>,
    mutations: &[sparx::baseline::CentroidStatsMutationV1],
) {
    for mutation in mutations {
        match mutation {
            sparx::baseline::CentroidStatsMutationV1::Put(kv) => {
                insert_kv_v1(db, kv.key.bytes.clone(), kv.value.clone())
            }
        }
    }
}

fn update_active_span_v1(
    active_span: &mut Option<ActiveSpanStateV1>,
    file: &DiscoveredFileV1,
    inode: u64,
    offset_start: u64,
    offset_end: u64,
) {
    match active_span {
        Some(span) => {
            span.offset_end = offset_end;
        }
        None => {
            *active_span = Some(ActiveSpanStateV1 {
                file_rel: file.file_rel.clone(),
                file_key: file.file_key.clone(),
                inode,
                offset_start,
                offset_end,
                is_gzip: file.is_gzip,
            });
        }
    }
}

fn file_span_from_active_v1(span: ActiveSpanStateV1) -> FileSpanV1 {
    FileSpanV1 {
        file_rel: span.file_rel,
        file_key: span.file_key,
        inode: span.inode,
        offset_start: span.offset_start,
        offset_end: span.offset_end,
        is_gzip: span.is_gzip,
    }
}

fn next_window_id_v1(db: &BTreeMap<String, Vec<u8>>, device_key: &str) -> u64 {
    db.get(&key_string_v1(key_tenant_active_window_v1(device_key)))
        .map(|value| {
            decode_win_active_v1(value)
                .unwrap()
                .active_window_id
                .saturating_add(1)
        })
        .unwrap_or(1)
}

fn read_alerts_from_jsonl_root_v1(root: &Path) -> Vec<AlertV1> {
    let mut files = Vec::new();
    collect_files_v1(root, &mut files);
    files.sort();
    let mut alerts = Vec::new();
    for path in files {
        let content = fs::read_to_string(path).unwrap();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            alerts.push(serde_json::from_str::<AlertV1>(line).unwrap());
        }
    }
    alerts
}

fn collect_files_v1(dir: &Path, out: &mut Vec<PathBuf>) {
    if !dir.exists() {
        return;
    }
    let mut entries: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect_files_v1(&path, out);
        } else {
            out.push(path);
        }
    }
}

fn build_watch_root_from_fixture_v1(watch_root: &Path) {
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("tenants")
        .join(FIXTURE_TENANT_V1)
        .join("devices")
        .join(FIXTURE_LOG_V1);
    let target_dir = watch_root.join(FIXTURE_TENANT_V1).join(DEVICE_DIR_V1);
    fs::create_dir_all(&target_dir).unwrap();
    fs::copy(source, target_dir.join("auth.log")).unwrap();
}

fn base_window_caps_v1() -> WindowCapsV1 {
    WindowCapsV1 {
        max_features_per_window: 50_000,
        max_word_features_per_window: 20_000,
        max_shape_features_per_window: 20_000,
        max_syslog_features_per_window: 2_000,
        entity_sketch_caps: sparx::features::EntitySketchCapsV1 {
            max_srcips: 64,
            max_dstips: 64,
            max_userids: 128,
            max_domains: 128,
            max_hosts: 128,
        },
    }
}

fn insert_kv_v1(db: &mut BTreeMap<String, Vec<u8>>, key: Vec<u8>, value: Vec<u8>) {
    db.insert(String::from_utf8(key).unwrap(), value);
}

fn key_string_v1(key: sparx::db::keys::KeyBytes) -> String {
    String::from_utf8(key.bytes).unwrap()
}

fn make_temp_root_v1(label: &str) -> PathBuf {
    let id = NEXT_TMP_ID_V1.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!("sparx_{}_{}_{}", label, std::process::id(), id));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    root
}
