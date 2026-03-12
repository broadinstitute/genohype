use crate::distributed::coordinator::state::{CoordinatorData, JobExecutionState, SharedState};
use crate::distributed::message::{CatalogEntry, JobEvent, JobSpec, LoadCatalogRequest, ProcessCatalogRequest};
use axum::{extract::State, Json};
use crate::manhattan::batch::BatchConfig;

pub(crate) async fn load_catalog_api(
    State(state): State<SharedState>,
    Json(req): Json<LoadCatalogRequest>,
) -> Json<serde_json::Value> {
    use crate::distributed::coordinator::services::catalog;

    let result = if let Some(ref config_path) = req.config_path {
        catalog::load_catalog(config_path)
    } else if let Some(ref assets_json) = req.assets_json {
        catalog::load_catalog_from_assets(assets_json)
    } else {
        return Json(serde_json::json!({ "success": false, "error": "Provide config_path or assets_json" }));
    };

    match result {
        Ok((cat, completed, ingested)) => {
            let count = cat.entries.len();
            let mut data = state.lock().expect("state lock poisoned");
            data.completed_phenotypes.extend(completed);
            data.ingested_phenotypes.extend(ingested);
            data.catalog = Some(cat);
            Json(serde_json::json!({ "success": true, "count": count }))
        }
        Err(e) => {
            Json(serde_json::json!({ "success": false, "error": e.to_string() }))
        }
    }
}

pub(crate) async fn get_catalog_api(
    State(state): State<SharedState>,
) -> Json<Vec<CatalogEntry>> {
    // Phase 1: check if we need to load, and grab the config (lock released after this block)
    let pending_config = {
        let data = state.lock().expect("state lock poisoned");
        if data.catalog.is_none() {
            if let Some(JobSpec::ManhattanBatch { config: Some(ref cfg), .. }) = data.config.job_spec {
                Some(cfg.clone())
            } else {
                None
            }
        } else {
            None
        }
    };

    // Phase 2: do the expensive I/O outside the lock
    let loaded_catalog = if let Some(cfg) = pending_config {
        match crate::distributed::coordinator::services::catalog::load_catalog_from_config(cfg) {
            Ok(res) => {
                println!("Lazy-loaded catalog with {} phenotypes", res.0.entries.len());
                Some(res)
            }
            Err(e) => {
                println!("Warning: Failed to lazy-load catalog: {}", e);
                None
            }
        }
    } else {
        None
    };

    // Phase 3: re-acquire lock to update state and formulate response
    let mut data = state.lock().expect("state lock poisoned");
    if let Some((cat, completed, ingested)) = loaded_catalog {
        data.completed_phenotypes.extend(completed);
        data.ingested_phenotypes.extend(ingested);
        data.catalog = Some(cat);
    }

    if let Some(catalog) = &data.catalog {
        let mut entries = catalog.entries.clone();

        // Dynamically compute statuses based on current JobExecutionState
        for entry in entries.iter_mut() {
            // First check if it's currently processing in a batch
            let mut is_processing = false;
            if let JobExecutionState::Batch(batch) = &data.job_state {
                let output_path = format!("{}/{}/{}", catalog.config.job.output_dir.as_deref().unwrap_or(""), entry.ancestry, entry.id);
                if let Some(status) = batch.phenotype_statuses.get(&output_path) {
                    entry.status = status.stage.clone();
                    is_processing = true;
                }
            }

            // If not actively processing, use our pre-loaded states
            if !is_processing {
                if data.ingested_phenotypes.contains(&(entry.id.clone(), entry.ancestry.clone())) {
                    entry.status = "ingested".to_string();
                } else if data.completed_phenotypes.contains(&(entry.id.clone(), entry.ancestry.clone())) {
                    entry.status = "completed".to_string();
                }
            }
        }
        Json(entries)
    } else {
        // No catalog loaded - synthesize entries from the running batch job's specs + statuses
        synthesize_catalog_from_batch(&data)
    }
}

/// Build catalog entries from whatever the coordinator already has in memory:
/// the ManhattanBatch specs and the BatchState phenotype statuses.
fn synthesize_catalog_from_batch(
    data: &crate::distributed::coordinator::state::CoordinatorData,
) -> Json<Vec<CatalogEntry>> {
    let mut entries = Vec::new();

    // Extract specs from the job spec
    if let Some(JobSpec::ManhattanBatch { ref specs, .. }) = data.config.job_spec {
        for spec in specs {
            let id = spec.phenotype.clone().unwrap_or_default();
            let ancestry = spec.ancestry.clone().unwrap_or_default();
            let mut status = "idle".to_string();

            // Look up live status from batch state
            if let JobExecutionState::Batch(ref batch) = data.job_state {
                if let Some(ps) = batch.phenotype_statuses.get(&spec.output_path) {
                    status = ps.stage.clone();
                }
            }

            let final_status = if status == "idle" {
                if data.ingested_phenotypes.contains(&(id.clone(), ancestry.clone())) {
                    "ingested".to_string()
                } else if data.completed_phenotypes.contains(&(id.clone(), ancestry.clone())) {
                    "completed".to_string()
                } else {
                    status
                }
            } else {
                status
            };

            entries.push(CatalogEntry {
                id: id.clone(),
                ancestry: ancestry.clone(),
                description: None,
                category: None,
                trait_type: None,
                n_cases: None,
                n_controls: None,
                has_exome: spec.exome.is_some(),
                has_genome: spec.genome.is_some(),
                has_gene_burden: spec.gene_burden.is_some(),
                status: final_status,
            });
        }
    }

    entries.sort_by(|a, b| a.id.cmp(&b.id).then(a.ancestry.cmp(&b.ancestry)));
    Json(entries)
}

/// Serve the embedded job config as JSON (for the config panel).
pub(crate) async fn get_config_api(
    State(state): State<SharedState>,
) -> Json<serde_json::Value> {
    let data = state.lock().expect("state lock poisoned");

    // Try catalog config first, then fall back to job spec's embedded config
    if let Some(ref catalog) = data.catalog {
        return Json(serde_json::to_value(&catalog.config).unwrap_or(serde_json::json!(null)));
    }
    if let Some(JobSpec::ManhattanBatch { config: Some(ref cfg), .. }) = data.config.job_spec {
        return Json(serde_json::to_value(cfg).unwrap_or(serde_json::json!(null)));
    }
    Json(serde_json::json!(null))
}

pub(crate) async fn process_catalog_api(
    State(state): State<SharedState>,
    Json(req): Json<ProcessCatalogRequest>,
) -> Json<serde_json::Value> {
    let mut data = state.lock().expect("state lock poisoned");

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
        let job_spec = JobSpec::ManhattanBatch { specs, mode, config: None };

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

        let pheno_names: Vec<String> = req.phenotypes.iter().map(|(id, anc)| format!("{}/{}", anc, id)).collect();
        data.log_event(JobEvent {
            timestamp_ms: CoordinatorData::now_ms(),
            event_type: "submitted".to_string(),
            worker_id: None,
            phenotype_id: None,
            details: format!("Started batch from catalog: {} phenotypes ({})", pheno_names.len(), pheno_names.join(", ")),
        });

        Json(serde_json::json!({ "success": true, "message": "Started new batch job" }))
    } else {
        // Append to existing batch job
        let mut appended = Vec::new();
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
                    appended.push(spec.phenotype.clone().unwrap_or_default());
                    batch.pending_queue.push_back(spec);
                    batch.total_phenotypes += 1;
                }
            }
            crate::distributed::coordinator::scheduler::assignment::activate_next_phenotypes(batch);
        }
        if !appended.is_empty() {
            data.log_event(JobEvent {
                timestamp_ms: CoordinatorData::now_ms(),
                event_type: "submitted".to_string(),
                worker_id: None,
                phenotype_id: None,
                details: format!("Appended {} phenotypes to batch: {}", appended.len(), appended.join(", ")),
            });
        }
        Json(serde_json::json!({ "success": true, "message": "Appended to running batch job" }))
    }
}

pub(crate) async fn ingest_catalog_api(
    State(state): State<SharedState>,
    Json(req): Json<ProcessCatalogRequest>,
) -> Json<serde_json::Value> {
    let mut data = state.lock().expect("state lock poisoned");

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
