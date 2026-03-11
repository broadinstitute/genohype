use crate::distributed::coordinator::services::catalog::load_catalog;
use crate::distributed::coordinator::state::{JobExecutionState, SharedState};
use crate::distributed::message::{CatalogEntry, JobSpec, LoadCatalogRequest, ProcessCatalogRequest};
use axum::{extract::State, Json};
use crate::manhattan::batch::BatchConfig;

pub(crate) async fn load_catalog_api(
    State(state): State<SharedState>,
    Json(req): Json<LoadCatalogRequest>,
) -> Json<serde_json::Value> {
    match load_catalog(&req.config_path) {
        Ok(catalog) => {
            let mut data = state.lock().unwrap();
            data.catalog = Some(catalog);
            Json(serde_json::json!({ "success": true }))
        }
        Err(e) => {
            Json(serde_json::json!({ "success": false, "error": e.to_string() }))
        }
    }
}

pub(crate) async fn get_catalog_api(
    State(state): State<SharedState>,
) -> Json<Vec<CatalogEntry>> {
    let data = state.lock().unwrap();
    if let Some(catalog) = &data.catalog {
        let mut entries = catalog.entries.clone();

        // Dynamically compute statuses based on current JobExecutionState
        if let JobExecutionState::Batch(batch) = &data.job_state {
            for entry in entries.iter_mut() {
                // Determine output path to match batch phenotype ID
                let output_path = format!("{}/{}/{}", catalog.config.job.output_dir.as_deref().unwrap_or(""), entry.ancestry, entry.id);
                if let Some(status) = batch.phenotype_statuses.get(&output_path) {
                    entry.status = status.stage.clone();
                }
            }
        }
        Json(entries)
    } else {
        Json(vec![])
    }
}

pub(crate) async fn process_catalog_api(
    State(state): State<SharedState>,
    Json(req): Json<ProcessCatalogRequest>,
) -> Json<serde_json::Value> {
    let mut data = state.lock().unwrap();

    let catalog = if let Some(cat) = &data.catalog {
        cat.clone()
    } else {
        return Json(serde_json::json!({ "success": false, "error": "Catalog not loaded" }));
    };

    // Extract selected inputs
    let mut selected_inputs = Vec::new();
    for pheno in &req.phenotypes {
        if let Some(input) = catalog.inputs.get(pheno) {
            selected_inputs.push(input.clone());
        }
    }

    if selected_inputs.is_empty() {
        return Json(serde_json::json!({ "success": false, "error": "No valid phenotypes selected" }));
    }

    // Build the BatchConfig
    let batch_config = BatchConfig {
        output_dir: catalog.config.job.output_dir.clone().unwrap_or_default(),
        threshold: catalog.config.job.threshold,
        gene_threshold: catalog.config.job.gene_threshold,
        locus_threshold: catalog.config.job.locus_threshold,
        locus_window: catalog.config.job.locus_window,
        locus_plots: catalog.config.job.locus_plots,
        min_variants_per_locus: catalog.config.job.min_variants_per_locus,
        width: catalog.config.job.width,
        height: catalog.config.job.height,
        y_field: catalog.config.job.y_field.clone(),
        genes_path: catalog.config.job.genes.clone(),
        exome_annotations: catalog.config.job.exome_annotations.clone(),
        genome_annotations: catalog.config.job.genome_annotations.clone(),
        styling: catalog.config.styling(),
    };

    let specs = crate::manhattan::batch::create_specs(selected_inputs, &batch_config);
    let mode = if catalog.config.job.scan_only {
        crate::distributed::message::ExecutionMode::ScanOnly
    } else if catalog.config.job.aggregate_only {
        crate::distributed::message::ExecutionMode::AggregateOnly
    } else {
        crate::distributed::message::ExecutionMode::Full
    };

    if data.idle || !matches!(data.job_state, JobExecutionState::Batch(_)) {
        let primary_input = specs.first().and_then(|s| s.primary_input_path()).unwrap_or("batch").to_string();
        let job_spec = JobSpec::ManhattanBatch { specs, mode };

        data.config.input_path = primary_input;
        data.config.job_spec = Some(job_spec.clone());
        data.idle = false;
        data.job_state = JobExecutionState::Standard; // Will be overwritten below

        let batch_state = crate::distributed::coordinator::services::init_batch_state(
            match &job_spec { JobSpec::ManhattanBatch { specs, .. } => specs, _ => unreachable!() },
            mode
        );
        data.job_state = JobExecutionState::Batch(batch_state);

        let job_id = uuid::Uuid::new_v4().to_string();
        data.current_job_id = Some(job_id.clone());
        let _ = data.metrics_db.insert_job(&crate::distributed::message::JobRecord {
            job_id,
            status: "running".to_string(),
            start_time_ms: crate::distributed::coordinator::state::CoordinatorData::now_ms(),
            end_time_ms: None,
            job_spec_json: serde_json::to_value(&job_spec).ok(),
            input_path: data.config.input_path.clone(),
            total_tasks: 0,
            job_type: Some("manhattan batch (catalog)".to_string()),
        });

        Json(serde_json::json!({ "success": true, "message": "Started new batch job" }))
    } else {
        // Append to existing batch job
        if let JobExecutionState::Batch(ref mut batch) = data.job_state {
            for spec in specs {
                let id = spec.output_path.clone();
                if !batch.phenotype_statuses.contains_key(&id) {
                    batch.phenotype_statuses.insert(id.clone(), crate::distributed::message::PhenotypeStatus {
                        id: id.clone(),
                        stage: "queued".to_string(),
                        partitions_done: 0,
                        partitions_total: 0,
                        result: None,
                        error: None,
                        duration_secs: None,
                        cpu_core_secs: None,
                    });
                    batch.pending_queue.push_back(spec);
                    batch.total_phenotypes += 1;
                }
            }
            crate::distributed::coordinator::scheduler::assignment::activate_next_phenotypes(batch);
        }
        Json(serde_json::json!({ "success": true, "message": "Appended to running batch job" }))
    }
}

pub(crate) async fn ingest_catalog_api(
    State(state): State<SharedState>,
    Json(req): Json<ProcessCatalogRequest>,
) -> Json<serde_json::Value> {
    let mut data = state.lock().unwrap();

    if !data.idle {
        return Json(serde_json::json!({ "success": false, "error": "Coordinator is busy. Wait for job to finish." }));
    }

    let catalog = if let Some(cat) = &data.catalog {
        cat.clone()
    } else {
        return Json(serde_json::json!({ "success": false, "error": "Catalog not loaded" }));
    };

    let input_dir = catalog.config.ingest_input_dir().unwrap_or_default();
    let clickhouse_url = catalog.config.ingest.clickhouse_url.clone().unwrap_or_default();

    if input_dir.is_empty() || clickhouse_url.is_empty() {
        return Json(serde_json::json!({ "success": false, "error": "ingest.input_dir and clickhouse_url must be set in config" }));
    }

    let init_strategy = match catalog.config.ingest.init_strategy.to_lowercase().as_str() {
        "replace" => crate::distributed::message::InitStrategy::Replace,
        "append" => crate::distributed::message::InitStrategy::Append,
        _ => crate::distributed::message::InitStrategy::Create,
    };

    let job_spec = JobSpec::IngestManhattan {
        input_dir: input_dir.clone(),
        clickhouse_url: clickhouse_url.clone(),
        database: catalog.config.ingest.database.clone(),
        init_strategy,
        phenotypes: Some(req.phenotypes),
    };

    // Execute DDL based on init_strategy
    #[cfg(feature = "clickhouse")]
    {
        if let Err(e) = crate::distributed::coordinator::services::init_clickhouse_tables(&clickhouse_url, &init_strategy) {
            return Json(serde_json::json!({ "success": false, "error": format!("DDL failed: {}", e) }));
        }
    }

    let phenotype_filter = job_spec.phenotypes().unwrap_or_default();
    let phenotypes = match crate::distributed::coordinator::services::discover_phenotypes_for_ingestion(&input_dir, Some(&phenotype_filter)) {
        Ok(p) => p,
        Err(e) => return Json(serde_json::json!({ "success": false, "error": format!("Discovery failed: {}", e) })),
    };

    if phenotypes.is_empty() {
        return Json(serde_json::json!({ "success": false, "error": "No matching phenotypes found in output directory" }));
    }

    data.config.input_path = input_dir;
    data.config.job_spec = Some(job_spec.clone());
    data.idle = false;

    #[cfg(feature = "clickhouse")]
    {
        data.job_state = JobExecutionState::Ingestion(
            crate::distributed::coordinator::services::create_ingestion_state(
                phenotypes,
                &clickhouse_url,
                &catalog.config.ingest.database,
            )
        );
    }

    let job_id = uuid::Uuid::new_v4().to_string();
    data.current_job_id = Some(job_id.clone());
    let _ = data.metrics_db.insert_job(&crate::distributed::message::JobRecord {
        job_id,
        status: "running".to_string(),
        start_time_ms: crate::distributed::coordinator::state::CoordinatorData::now_ms(),
        end_time_ms: None,
        job_spec_json: serde_json::to_value(&job_spec).ok(),
        input_path: data.config.input_path.clone(),
        total_tasks: 0,
        job_type: Some("ingest manhattan (catalog)".to_string()),
    });

    Json(serde_json::json!({ "success": true, "message": "Started ingestion job" }))
}
