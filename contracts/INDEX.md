# sparx Contracts v0.1 Index

1. `README.md`
2. `01_semantic_keys_v0_1.md`
3. `02_shape_catalog_v0_1.md`
4. `03_alert_object_explanation_v0_1.md`
5. `04_state_retention_v0_1.md`
6. `05_feature_id_strategy_v0_1.md`
7. `06_rocksdb_topology_v0_1.md`
8. `07_cli_contract_v0_1.md`
9. `08_directory_discovery_v0_1.md`
10. `09_worker_partitioning_v0_1.md`
11. `10_metrics_health_v0_1.md`
12. `11_mvp_milestones_tests_v0_1.md`
13. `12_fixture_corpus_v0_1.md`
14. `13_overrides_tenant_policy_v0_1.md`
15. `14_schema_migrations_v0_1.md`
16. `15_alert_query_cli_v0_1.md`
17. `16_raw_log_drilldown_v0_1.md`
18. `17_format_handling_v0_1.md`
19. `18_syslog_envelope_and_cef_reverse_kv_v0_1.md`
20. `19_tokenization_boundary_v0_1.md`
21. `20_feature_weighting_v0_1.md`
22. `21_scoring_math_thresholding_v0_1.md`
23. `22_baseline_sketch_encoding_v0_1.md`
24. `23_tokenizer_details_v0_1.md`
25. `24_feature_emission_catalog_v0_1.md`
26. `25_tenant_db_key_prefix_map_v0_1.md`
27. `26_open_window_checkpoint_encoding_v0_1.md`
28. `27_alert_object_schema_v0_1.md`
29. `28_config_schema_v0_1.md`
30. `29_output_sink_contract_v0_1.md`
31. `30_global_db_key_prefix_map_v0_1.md`
32. `31_tenant_db_simple_value_encodings_v0_1.md`
33. `32_service_and_deployment_contract_v0_1.md`
34. `33_contract_consistency_checklist_v0_1.md`
35. `34_health_silence_detection_v0_1.md`
36. `35_expected_source_state_vdrop_plan_v0_1.md`
37. `36_vdrop_policy_diagnostics_scope_v0_1.md`
38. `37_sharp_drop_detection_scope_v0_1.md`
39. `38_vdrop_richer_subject_scope_v0_1.md`
40. `39_source_stream_vdrop_implementation_plan_v0_1.md`
41. `40_signal_processing_baselines_v0_1.md`
42. `41_deferred_signal_processing_candidates_v0_1.md`

## Current release boundary

Source-stream V_DROP is included in v1 scope behind the default-off source-stream
gate. Parser-class and vendor-event-family V_DROP subjects remain deferred.
External Rust validation logs are still required before release closure.

Security/performance hardening note: active contracts now require fail-closed drill/extract path resolution, validated output path components, symlink-resistant spool inventory, and bounded ingest resource caps.

## Signal-processing baseline contract note

The lean signal-processing baseline direction is locked in Contract 40. The MVP
adds EWMA volume smoothing and hour-of-week periodic volume baselines as compact
auxiliary state. It must not change sparse row encoding, AlertV1, DeviceStatsV1,
or SourceStreamStatsV1. Autocorrelation-lite and frequency-domain analysis are
recorded as deferred candidates in Contract 41.
