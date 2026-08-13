//! Bounded local safe-export coordinator and publication fault campaign.
//!
//! These tests deliberately use authority-shaped `gs://` specs with an injected
//! versioned store. Production URI resolution remains unchanged.

use super::monitor::{check_stuck_job, check_timeouts, check_worker_liveness};
use super::services::safe_export;
use super::state::{CoordinatorData, JobExecutionState, SafeExportTestRepository, SharedState};
use super::{api, complete_work, get_work, open_and_reconcile_database};
use crate::distributed::message::{
    AssignmentLease, CompleteRequest, CompleteResponse, JobConfigRequest, JobConfigResponse,
    JobRecord, JobSpec, WorkRequest, WorkResponse, SAFE_EXPORT_WORKER_PROTOCOL_VERSION,
};
use crate::distributed::metrics_db::MetricsDb;
use axum::routing::{get, post};
use axum::Router;
use bytes::Bytes;
use futures::stream::BoxStream;
use genohype_core::codec::{ETypeParser, EncodedValue, LosslessValue};
use genohype_core::io::{
    build_attempt_staging_location, content_addressed_path, lease_identity_sha256, SourceManifest,
    SourceObject,
};
use genohype_core::metadata::BufferSpec;
use genohype_core::parquet::ParquetWriter;
use genohype_core::release::{
    ComponentDescriptor, DecodedRowsHasher, ExportPartReport, GlobalsPayload, KeyDescriptor,
    KeyOrdering, ParquetDescriptor, PhysicalTypeDescriptor, SafeExportSpec,
    SafeExportWorkerIdentity, SourcePartition, TableDescriptor, ToolIdentity,
    WriterPropertiesDescriptor, SAFE_EXPORT_SCHEMA_VERSION, SAFE_EXPORT_SHARDING_RULE_V1,
};
use genohype_core::schema::{ArrowSchemaDescriptor, PairedSchema, VTypeParser};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOpts,
    PutOptions, PutPayload, PutResult,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tower::ServiceExt;

#[derive(Debug)]
struct VersionedFaultStore {
    inner: InMemory,
    next_generation: AtomicU64,
    identities: Mutex<HashMap<Path, (String, String)>>,
    operations: Mutex<Vec<(String, Path)>>,
    list_calls: AtomicUsize,
    lose_success_response_once: AtomicBool,
}

impl VersionedFaultStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            next_generation: AtomicU64::new(0),
            identities: Mutex::new(HashMap::new()),
            operations: Mutex::new(Vec::new()),
            list_calls: AtomicUsize::new(0),
            lose_success_response_once: AtomicBool::new(false),
        }
    }

    fn fresh_identity(&self, path: &Path) -> PutResult {
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst) + 1;
        let version = format!("generation-{generation}");
        let etag = format!("etag-{generation}");
        self.identities
            .lock()
            .unwrap()
            .insert(path.clone(), (version.clone(), etag.clone()));
        PutResult {
            e_tag: Some(etag),
            version: Some(version),
        }
    }

    fn operation_count_beneath(&self, prefix: &Path) -> usize {
        self.operations
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, path)| path.as_ref().starts_with(prefix.as_ref()))
            .count()
    }

    fn clear_operations(&self) {
        self.operations.lock().unwrap().clear();
    }

    fn operations(&self) -> Vec<(String, Path)> {
        self.operations.lock().unwrap().clone()
    }

    fn lose_next_success_response(&self) {
        self.lose_success_response_once
            .store(true, Ordering::SeqCst);
    }

    async fn put_bytes(&self, path: &Path, bytes: Bytes) -> SourceObject {
        let result = self
            .put_opts(path, bytes.clone().into(), PutMode::Overwrite.into())
            .await
            .unwrap();
        SourceObject {
            uri: String::new(),
            version: result.version.unwrap(),
            etag: result.e_tag.unwrap(),
            size: bytes.len() as u64,
            crc32c: None,
            sha256: Some(format!("{:x}", Sha256::digest(&bytes))),
        }
    }

    fn check_identity(&self, location: &Path, options: &GetOptions) -> object_store::Result<()> {
        let identities = self.identities.lock().unwrap();
        let Some((version, etag)) = identities.get(location) else {
            return Ok(());
        };
        if options
            .version
            .as_deref()
            .is_some_and(|value| value != version)
            || options
                .if_match
                .as_deref()
                .is_some_and(|value| value != "*" && value != etag)
        {
            return Err(object_store::Error::Precondition {
                path: location.to_string(),
                source: "fault-store identity mismatch".into(),
            });
        }
        Ok(())
    }

    fn patch_meta(&self, meta: &mut ObjectMeta) {
        if let Some((version, etag)) = self.identities.lock().unwrap().get(&meta.location) {
            meta.version = Some(version.clone());
            meta.e_tag = Some(etag.clone());
        }
    }
}

impl std::fmt::Display for VersionedFaultStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "safe-export-versioned-fault-store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for VersionedFaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await?;
        self.operations
            .lock()
            .unwrap()
            .push(("PUT".to_string(), location.clone()));
        let identity = self.fresh_identity(location);
        if location.as_ref().ends_with("/_SUCCESS")
            && self
                .lose_success_response_once
                .swap(false, Ordering::SeqCst)
        {
            return Err(object_store::Error::Generic {
                store: "fault-store",
                source: "injected lost response after successful _SUCCESS create".into(),
            });
        }
        Ok(identity)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        mut options: GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.operations.lock().unwrap().push((
            if options.head { "HEAD" } else { "GET" }.to_string(),
            location.clone(),
        ));
        self.check_identity(location, &options)?;
        options.version = None;
        options.if_match = None;
        let mut result = self.inner.get_opts(location, options).await?;
        self.patch_meta(&mut result.meta);
        Ok(result)
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.operations
            .lock()
            .unwrap()
            .push(("DELETE".to_string(), location.clone()));
        self.inner.delete(location).await?;
        self.identities.lock().unwrap().remove(location);
        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.operations
            .lock()
            .unwrap()
            .push(("COPY".to_string(), to.clone()));
        self.inner.copy(from, to).await?;
        self.fresh_identity(to);
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.operations
            .lock()
            .unwrap()
            .push(("COPY_CREATE".to_string(), to.clone()));
        self.inner.copy_if_not_exists(from, to).await?;
        self.fresh_identity(to);
        Ok(())
    }
}

struct Fixture {
    store: Arc<VersionedFaultStore>,
    store_dyn: Arc<dyn ObjectStore>,
    prefix: Path,
    manifest: SourceManifest,
    spec: SafeExportSpec,
}

fn component(part_files: Vec<String>) -> ComponentDescriptor {
    let physical = ETypeParser::parse("+EBaseStruct{x:+EBinary}").unwrap();
    ComponentDescriptor {
        name: "OrderedRVDComponentSpec".to_string(),
        raw_e_type: "+EBaseStruct{x:+EBinary}".to_string(),
        physical_type: PhysicalTypeDescriptor::from(&physical),
        raw_v_type: "Struct{x:String}".to_string(),
        logical_type: VTypeParser::parse("Struct{x:String}").unwrap(),
        buffer_spec: BufferSpec::StreamBlock,
        part_files,
        attrs: serde_json::json!({}),
        range_bounds: Vec::new(),
        index: None,
    }
}

async fn fixture() -> Fixture {
    let store = Arc::new(VersionedFaultStore::new());
    let store_dyn: Arc<dyn ObjectStore> = store.clone();
    let prefix = Path::from("release");
    let mut sources = Vec::new();
    for index in 0..2 {
        let path = Path::from(format!("source/table.ht/rows/parts/part-{index}"));
        let bytes = Bytes::from(format!("source-{index}"));
        let mut source = store.put_bytes(&path, bytes).await;
        source.uri = format!("gs://source/table.ht/rows/parts/part-{index}");
        source.crc32c = Some("Nks/tw==".to_string());
        sources.push(source);
    }
    let manifest = SourceManifest::new(sources.clone()).unwrap();
    let arrow = PairedSchema::from_raw_vtype(
        ETypeParser::parse("+EBaseStruct{x:+EBinary}").unwrap(),
        "Struct{x:String}",
    )
    .unwrap()
    .arrow_plan()
    .unwrap();
    let worker_sha = "b".repeat(64);
    let spec = SafeExportSpec {
        schema_version: SAFE_EXPORT_SCHEMA_VERSION,
        export_id: String::new(),
        canonical_table_uri: "gs://source/table.ht".to_string(),
        source_manifest_uri: "gs://authority/manifest.json".to_string(),
        source_manifest_sha256: manifest.digest_sha256().unwrap(),
        source_snapshot_timestamp: "2026-08-12T12:00:00Z".to_string(),
        output_root_uri: "gs://output/release".to_string(),
        source_parts: sources,
        parquet: ParquetDescriptor {
            arrow_schema: serde_json::to_value(arrow.descriptor()).unwrap(),
            schema_fingerprint_sha256: arrow.fingerprint_sha256().unwrap(),
            writer_properties: WriterPropertiesDescriptor {
                compression: "zstd".to_string(),
                compression_level: None,
                max_row_group_size: 100_000,
                converter_batch_size: 4096,
                dictionary_enabled: true,
                statistics_enabled: true,
                data_page_version: "v1".to_string(),
                writer_version: "v1".to_string(),
            },
            sharding_rule: SAFE_EXPORT_SHARDING_RULE_V1.to_string(),
            projection: Vec::new(),
            filters: Vec::new(),
            output_ordering: vec!["x".to_string()],
        },
        expected_worker: SafeExportWorkerIdentity {
            git_commit: "commit-a".to_string(),
            build_identity: "release:gcp".to_string(),
            binary_sha256: worker_sha.clone(),
        },
        table: TableDescriptor {
            file_version: 1,
            hail_version: "0.2".to_string(),
            references_rel_path: "references".to_string(),
            table_type: "Table".to_string(),
            top_metadata: serde_json::json!({"file_version": 1}),
            key: KeyDescriptor {
                fields: vec!["x".to_string()],
                ordering: KeyOrdering::LexicographicAscending,
                distinctly_keyed: Some(true),
            },
            rows: component(vec!["part-0".to_string(), "part-1".to_string()]),
            globals: component(vec!["part-0".to_string()]),
            partitions: vec![
                SourcePartition {
                    index: 0,
                    part_file: "part-0".to_string(),
                    range_bound: None,
                    row_count: Some(2),
                    index_objects: Vec::new(),
                    output_part_ids: vec!["part-00000000".to_string()],
                },
                SourcePartition {
                    index: 1,
                    part_file: "part-1".to_string(),
                    range_bound: None,
                    row_count: Some(1),
                    index_objects: Vec::new(),
                    output_part_ids: vec!["part-00000001".to_string()],
                },
            ],
        },
        globals: GlobalsPayload::from_value(&EncodedValue::Struct(Vec::new())),
        tool: ToolIdentity {
            package_version: "0.1".to_string(),
            git_commit: "commit-a".to_string(),
            dirty_build: false,
            build_provenance: "release:gcp".to_string(),
            binary_name: "genohype-worker".to_string(),
            binary_uri: Some("gs://binaries/genohype-worker".to_string()),
            binary_sha256: worker_sha,
            target_triple: "x86_64-unknown-linux-gnu".to_string(),
            enabled_features: vec!["gcp".to_string(), "validation".to_string()],
            arrow_crate_version: "53.3".to_string(),
            parquet_crate_version: "53.3".to_string(),
        },
        receipt_schema_version: 1,
        completion_schema_version: 1,
    }
    .with_computed_export_id()
    .unwrap();
    store.clear_operations();
    Fixture {
        store,
        store_dyn,
        prefix,
        manifest,
        spec,
    }
}

fn empty_state(db: MetricsDb, fixture: &Fixture, session: &str) -> SharedState {
    Arc::new(Mutex::new(CoordinatorData {
        safe_export_test_repository: Some(SafeExportTestRepository {
            store: fixture.store_dyn.clone(),
            output_prefix: fixture.prefix.clone(),
            manifest: fixture.manifest.clone(),
        }),
        pending_partitions: VecDeque::new(),
        processing_partitions: HashMap::new(),
        completed_tasks: HashSet::new(),
        config: super::state::CoordinatorConfig::default(),
        total_rows: 0,
        scan_cpu_secs: 0.0,
        aggregate_cpu_secs: 0.0,
        wasted_cpu_secs: 0.0,
        retry_counts: HashMap::new(),
        custom_assignment_attempts: HashMap::new(),
        custom_assignments: HashMap::new(),
        safe_export_assignment_attempts: HashMap::new(),
        safe_export_assignments: HashMap::new(),
        failed_partitions: HashSet::new(),
        worker_registry: HashMap::new(),
        job_start_time: Instant::now(),
        last_progress_time: Instant::now(),
        idle: true,
        metrics_db: db,
        aggregated_results: Vec::new(),
        job_state: JobExecutionState::Standard,
        active_tasks: HashMap::new(),
        last_error: None,
        events: VecDeque::new(),
        failures: VecDeque::new(),
        events_since_backup: 0,
        last_backup_at: None,
        update_fleet_url: None,
        updated_workers: HashSet::new(),
        current_job_id: None,
        session_id: session.to_string(),
        catalog: None,
        ingested_phenotypes: HashSet::new(),
        completed_phenotypes: HashSet::new(),
        last_completed_batch: None,
        cached_vms: None,
        deleted_workers: HashSet::new(),
    }))
}

fn request(spec: &SafeExportSpec) -> JobConfigRequest {
    JobConfigRequest {
        input_path: spec.canonical_table_uri.clone(),
        job_spec: JobSpec::SafeExport(spec.clone()),
        total_tasks: spec.table.partitions.len(),
        batch_size: Some(1),
        force: false,
        filters: Vec::new(),
        intervals: Vec::new(),
        memory_weight_mb: None,
    }
}

fn worker_request(spec: &SafeExportSpec, worker_id: &str) -> WorkRequest {
    WorkRequest {
        worker_id: worker_id.to_string(),
        hardware: None,
        build_version: Some(spec.expected_worker.git_commit.clone()),
        protocol_version: Some(SAFE_EXPORT_WORKER_PROTOCOL_VERSION),
        build_identity: Some(spec.expected_worker.build_identity.clone()),
        binary_sha256: Some(spec.expected_worker.binary_sha256.clone()),
        capabilities: vec!["safe_export_v1".to_string()],
    }
}

struct HttpCoordinator {
    base_url: String,
    server: tokio::task::JoinHandle<()>,
}

impl HttpCoordinator {
    async fn start(state: SharedState) -> Self {
        let app = Router::new()
            .route("/api/job", post(api::jobs::submit_job))
            .route("/work", post(get_work))
            .route("/complete", post(complete_work))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            base_url: format!("http://{address}"),
            server,
        }
    }

    async fn work(&self, request: &WorkRequest) -> WorkResponse {
        reqwest::Client::new()
            .post(format!("{}/work", self.base_url))
            .json(request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn submit(&self, request: &JobConfigRequest) -> JobConfigResponse {
        reqwest::Client::new()
            .post(format!("{}/api/job", self.base_url))
            .json(request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn complete(&self, request: &CompleteRequest) -> CompleteResponse {
        reqwest::Client::new()
            .post(format!("{}/complete", self.base_url))
            .json(request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn stop(self) {
        self.server.abort();
        let _ = self.server.await;
    }
}

async fn submit_and_assign(
    state: &SharedState,
    spec: &SafeExportSpec,
    worker: &str,
) -> (String, genohype_pool::distributed::TaskDescriptor, String) {
    assert!(matches!(
        get_work(
            axum::extract::State(state.clone()),
            axum::Json(worker_request(spec, worker)),
        )
        .await
        .0,
        WorkResponse::Wait
    ));
    let response = api::jobs::submit_job(
        axum::extract::State(state.clone()),
        axum::Json(request(spec)),
    )
    .await
    .0;
    assert!(response.acknowledged, "{:?}", response.error);
    let job_id = response.job_id.unwrap();
    let assigned = get_work(
        axum::extract::State(state.clone()),
        axum::Json(worker_request(spec, worker)),
    )
    .await
    .0;
    match assigned {
        WorkResponse::Task {
            mut tasks,
            session_id,
            ..
        } => (job_id, tasks.remove(0), session_id.unwrap()),
        other => panic!("expected safe task, got {other:?}"),
    }
}

fn parquet_part(index: usize) -> (Bytes, u64, String, LosslessValue, LosslessValue) {
    let arrow = PairedSchema::from_raw_vtype(
        ETypeParser::parse("+EBaseStruct{x:+EBinary}").unwrap(),
        "Struct{x:String}",
    )
    .unwrap()
    .arrow_plan()
    .unwrap();
    let values = if index == 0 {
        vec!["a", "b"]
    } else {
        vec!["c"]
    };
    let mut digest = DecodedRowsHasher::new();
    let mut writer = ParquetWriter::from_writer(Cursor::new(Vec::new()), Arc::new(arrow)).unwrap();
    for value in &values {
        let row = EncodedValue::Struct(vec![(
            "x".to_string(),
            EncodedValue::Binary(value.as_bytes().to_vec()),
        )]);
        digest.update(&row).unwrap();
        writer.write_row(&row).unwrap();
    }
    let (cursor, stats) = writer.finish_into_inner().unwrap();
    assert_eq!(stats.row_count, values.len() as u64);
    (
        Bytes::from(cursor.into_inner()),
        stats.row_group_count,
        digest.finalize(),
        LosslessValue::from(&EncodedValue::Binary(
            values.first().unwrap().as_bytes().to_vec(),
        )),
        LosslessValue::from(&EncodedValue::Binary(
            values.last().unwrap().as_bytes().to_vec(),
        )),
    )
}

async fn report_for(
    fixture: &Fixture,
    descriptor: &genohype_pool::distributed::TaskDescriptor,
    session: &str,
    worker: &str,
) -> (CompleteRequest, Path) {
    let task_value = descriptor
        .payload
        .get("task")
        .cloned()
        .unwrap_or_else(|| descriptor.payload.clone());
    let task: genohype_core::release::SafeExportPartTask =
        serde_json::from_value(task_value).unwrap();
    let lease = descriptor.assignment_lease().unwrap();
    let staging = build_attempt_staging_location(
        &fixture.spec.output_root_uri,
        &fixture.spec.export_id,
        &task.task_id,
        lease.assignment_attempt,
        &lease.lease_token,
        "parquet",
    )
    .unwrap();
    let staging_path =
        super::safe_export_test_path(&fixture.spec.output_root_uri, &staging.uri).unwrap();
    let (bytes, row_groups, decoded_rows_sha256, first_key, last_key) =
        parquet_part(task.partition_index);
    let mut uploaded = fixture.store.put_bytes(&staging_path, bytes.clone()).await;
    uploaded.uri = staging.uri;
    let report = ExportPartReport {
        schema_version: SAFE_EXPORT_SCHEMA_VERSION,
        export_id: fixture.spec.export_id.clone(),
        task_id: task.task_id.clone(),
        partition_index: task.partition_index,
        source_manifest_sha256: task.source_manifest_sha256.clone(),
        source_part_file: task.source_part_file.clone(),
        source_object: task.source_object.clone(),
        schema_fingerprint_sha256: task.schema_fingerprint_sha256.clone(),
        staging_object: uploaded,
        byte_size: bytes.len() as u64,
        row_count: if task.partition_index == 0 { 2 } else { 1 },
        row_group_count: row_groups,
        decoded_rows_sha256,
        first_key: Some(first_key),
        last_key: Some(last_key),
        worker_id: worker.to_string(),
        worker_git_commit: fixture.spec.expected_worker.git_commit.clone(),
        worker_binary_sha256: fixture.spec.expected_worker.binary_sha256.clone(),
        assignment_attempt: lease.assignment_attempt,
        lease_identity_sha256: lease_identity_sha256(&lease.lease_token).unwrap(),
        started_at: "2026-08-12T12:00:00Z".to_string(),
        completed_at: "2026-08-12T12:00:01Z".to_string(),
    };
    report
        .validate_against(
            &fixture.spec,
            &task,
            worker,
            lease.assignment_attempt,
            &lease_identity_sha256(&lease.lease_token).unwrap(),
        )
        .unwrap();
    (
        CompleteRequest {
            worker_id: worker.to_string(),
            tasks: vec![task.task_id],
            items_processed: report.row_count as usize,
            result_json: Some(serde_json::to_value(report).unwrap()),
            error: None,
            session_id: Some(session.to_string()),
            assignments: vec![lease],
        },
        staging_path,
    )
}

async fn assign_next(
    state: &SharedState,
    spec: &SafeExportSpec,
    worker: &str,
) -> (genohype_pool::distributed::TaskDescriptor, String) {
    match get_work(
        axum::extract::State(state.clone()),
        axum::Json(worker_request(spec, worker)),
    )
    .await
    .0
    {
        WorkResponse::Task {
            mut tasks,
            session_id,
            ..
        } => (tasks.remove(0), session_id.unwrap()),
        other => panic!("expected safe task, got {other:?}"),
    }
}

#[tokio::test]
async fn timeout_requeues_for_replacement_before_same_loop_stuck_check() {
    let fixture = fixture().await;
    let state = empty_state(MetricsDb::in_memory().unwrap(), &fixture, "session-timeout");
    let (job_id, first, _) = submit_and_assign(&state, &fixture.spec, "worker-old").await;
    {
        let mut data = state.lock().unwrap();
        data.config.timeout_secs = 1;
        data.config.stuck_timeout_secs = 1;
        data.processing_partitions.get_mut(&0).unwrap().1 =
            Instant::now() - Duration::from_secs(10);
        data.last_progress_time = Instant::now() - Duration::from_secs(10);
    }

    check_timeouts(&state);
    check_stuck_job(&state);
    {
        let data = state.lock().unwrap();
        assert!(!data.idle, "same-loop stuck check terminated a fresh retry");
        assert_eq!(data.pending_partitions, VecDeque::from([0, 1]));
        assert!(data.processing_partitions.is_empty());
        let status = data.metrics_db.get_safe_export_status(&job_id).unwrap();
        assert_eq!(status.phase.as_deref(), Some("running"));
        assert_eq!(status.current_assignment_count, 0);
        assert!(status.error.is_none());
    }

    let (replacement, session) = assign_next(&state, &fixture.spec, "worker-new").await;
    assert_eq!(replacement.id, first.id);
    assert_eq!(replacement.assignment_attempt, Some(2));
    let (stale, _) = report_for(&fixture, &first, &session, "worker-old").await;
    fixture.store.clear_operations();
    let rejected = complete_work(axum::extract::State(state), axum::Json(stale))
        .await
        .0;
    assert!(!rejected.acknowledged);
    assert_eq!(fixture.store.operation_count_beneath(&fixture.prefix), 0);
    assert_eq!(fixture.store.list_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn suspected_worker_death_requeues_safe_export_for_replacement() {
    let fixture = fixture().await;
    let state = empty_state(MetricsDb::in_memory().unwrap(), &fixture, "session-death");
    let (job_id, first, _) = submit_and_assign(&state, &fixture.spec, "worker-dead").await;
    {
        let mut data = state.lock().unwrap();
        data.config.stuck_timeout_secs = 1;
        data.worker_registry
            .get_mut("worker-dead")
            .unwrap()
            .last_seen =
            Instant::now() - Duration::from_secs(super::state::WORKER_SUSPECT_TIMEOUT_SECS + 1);
        data.last_progress_time = Instant::now() - Duration::from_secs(10);
    }

    check_worker_liveness(&state);
    check_stuck_job(&state);
    {
        let data = state.lock().unwrap();
        assert!(!data.idle, "same-loop stuck check terminated a death retry");
        assert_eq!(data.pending_partitions, VecDeque::from([0, 1]));
        assert_eq!(
            data.metrics_db
                .get_safe_export_status(&job_id)
                .unwrap()
                .phase
                .as_deref(),
            Some("running")
        );
    }

    let (replacement, session) = assign_next(&state, &fixture.spec, "worker-replacement").await;
    assert_eq!(replacement.id, first.id);
    assert_eq!(replacement.assignment_attempt, Some(2));
    let (stale, _) = report_for(&fixture, &first, &session, "worker-dead").await;
    fixture.store.clear_operations();
    let rejected = complete_work(axum::extract::State(state), axum::Json(stale))
        .await
        .0;
    assert!(!rejected.acknowledged);
    assert_eq!(fixture.store.operation_count_beneath(&fixture.prefix), 0);
    assert_eq!(fixture.store.list_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn stuck_safe_export_terminalizes_job_phase_and_exact_status_atomically() {
    let fixture = fixture().await;
    let state = empty_state(MetricsDb::in_memory().unwrap(), &fixture, "session-stuck");
    let (job_id, _, _) = submit_and_assign(&state, &fixture.spec, "worker-stuck").await;
    {
        let mut data = state.lock().unwrap();
        data.config.stuck_timeout_secs = 1;
        data.last_progress_time = Instant::now() - Duration::from_secs(10);
    }

    check_stuck_job(&state);

    let data = state.lock().unwrap();
    assert!(data.idle);
    assert!(data.safe_export_assignments.is_empty());
    assert_eq!(
        data.metrics_db.get_job(&job_id).unwrap().unwrap().status,
        "failed"
    );
    let status = data.metrics_db.get_safe_export_status(&job_id).unwrap();
    assert_eq!(status.phase.as_deref(), Some("failed"));
    assert!(!status.complete);
    assert_eq!(status.current_assignment_count, 0);
    assert!(
        status.error.as_deref().is_some_and(
            |error| error.starts_with("safe export stuck with zero completed partitions")
        )
    );
}

#[tokio::test]
async fn source_mutation_and_stale_attempt_cannot_publish() {
    let fixture = fixture().await;
    let state = empty_state(MetricsDb::in_memory().unwrap(), &fixture, "session-a");
    let (_job, first, session) = submit_and_assign(&state, &fixture.spec, "worker-a").await;
    assert_eq!(first.index, Some(0));

    fixture.store.clear_operations();
    let source = fixture.spec.source_parts[0].clone();
    let source_path = Path::from("source/table.ht/rows/parts/part-0");
    fixture
        .store
        .put_bytes(&source_path, Bytes::from_static(b"mutated-source"))
        .await;
    let conditioned = fixture
        .store
        .get_opts(
            &source_path,
            GetOptions {
                version: Some(source.version),
                if_match: Some(source.etag),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(
        conditioned,
        Err(object_store::Error::Precondition { .. })
    ));
    assert_eq!(fixture.store.operation_count_beneath(&fixture.prefix), 0);

    let first_lease = first.assignment_lease().unwrap();
    let failed = complete_work(
        axum::extract::State(state.clone()),
        axum::Json(CompleteRequest {
            worker_id: "worker-a".to_string(),
            tasks: vec![first.id.clone()],
            items_processed: 0,
            result_json: None,
            error: Some("conditioned source mutation".to_string()),
            session_id: Some(session.clone()),
            assignments: vec![first_lease.clone()],
        }),
    )
    .await
    .0;
    assert!(failed.acknowledged);

    let (second, session) = assign_next(&state, &fixture.spec, "worker-b").await;
    assert_eq!(second.id, first.id);
    assert_eq!(second.assignment_attempt, Some(2));
    let (mut stale, _) = report_for(&fixture, &first, &session, "worker-a").await;
    stale.assignments = vec![first_lease];
    fixture.store.clear_operations();
    let rejected = complete_work(axum::extract::State(state.clone()), axum::Json(stale))
        .await
        .0;
    assert!(!rejected.acknowledged);
    assert_eq!(fixture.store.operation_count_beneath(&fixture.prefix), 0);

    let (fresh, _) = report_for(&fixture, &second, &session, "worker-b").await;
    let accepted = complete_work(axum::extract::State(state.clone()), axum::Json(fresh))
        .await
        .0;
    assert!(accepted.acknowledged);
    let job_id = {
        let data = state.lock().unwrap();
        assert_eq!(data.completed_tasks, HashSet::from([0]));
        let job_id = data.current_job_id.clone().unwrap();
        assert_eq!(
            data.metrics_db
                .get_safe_export_status(&job_id)
                .unwrap()
                .failed_attempt_count,
            1
        );
        job_id
    };
    let app = Router::new()
        .route(
            "/api/jobs/:job_id/safe-export",
            get(api::jobs::get_safe_export_status),
        )
        .with_state(state.clone());
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri(format!("/api/jobs/{job_id}/safe-export"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let status: crate::distributed::message::SafeExportStatusResponse =
        serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status.job_id, job_id);
    assert_eq!(
        status.export_id.as_deref(),
        Some(fixture.spec.export_id.as_str())
    );
    assert_eq!(status.phase.as_deref(), Some("running"));
    assert_eq!(status.accepted_part_count, 1);
    assert_eq!(status.failed_attempt_count, 1);
    assert!(!status.complete);
    assert_eq!(fixture.store.list_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn coordinator_replacement_and_same_export_resubmit_dispatch_only_missing() {
    let fixture = fixture().await;
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("ops.db");
    let state = empty_state(MetricsDb::open(&db_path).unwrap(), &fixture, "old-session");
    let old_http = HttpCoordinator::start(state.clone()).await;
    assert!(matches!(
        old_http
            .work(&worker_request(&fixture.spec, "worker-a"))
            .await,
        WorkResponse::Wait
    ));
    let accepted = old_http.submit(&request(&fixture.spec)).await;
    assert!(accepted.acknowledged, "{:?}", accepted.error);
    let (first, old_session) = match old_http
        .work(&worker_request(&fixture.spec, "worker-a"))
        .await
    {
        WorkResponse::Task {
            mut tasks,
            session_id,
            ..
        } => (tasks.remove(0), session_id.unwrap()),
        other => panic!("expected first HTTP assignment, got {other:?}"),
    };
    let (completion, _) = report_for(&fixture, &first, &old_session, "worker-a").await;
    assert!(old_http.complete(&completion).await.acknowledged);
    old_http.stop().await;
    drop(state);

    let reopened = open_and_reconcile_database(db_path.to_str().unwrap()).unwrap();
    let replacement = empty_state(reopened, &fixture, "new-session");
    let new_http = HttpCoordinator::start(replacement.clone()).await;
    assert!(matches!(
        new_http
            .work(&worker_request(&fixture.spec, "worker-b"))
            .await,
        WorkResponse::Wait
    ));
    let accepted = new_http.submit(&request(&fixture.spec)).await;
    assert!(accepted.acknowledged, "{:?}", accepted.error);
    let (assigned, new_session) = match new_http
        .work(&worker_request(&fixture.spec, "worker-b"))
        .await
    {
        WorkResponse::Task {
            mut tasks,
            session_id,
            ..
        } => (tasks.remove(0), session_id.unwrap()),
        other => panic!("expected replacement HTTP assignment, got {other:?}"),
    };
    assert_eq!(assigned.index, Some(1));
    {
        let data = replacement.lock().unwrap();
        assert_eq!(data.completed_tasks, HashSet::from([0]));
        assert_eq!(data.pending_partitions, VecDeque::new());
        assert_eq!(
            data.processing_partitions
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    fixture.store.clear_operations();
    let stale = new_http.complete(&completion).await;
    assert!(!stale.acknowledged);
    assert_eq!(fixture.store.operation_count_beneath(&fixture.prefix), 0);
    assert_ne!(old_session, new_session);
    assert_eq!(assigned.assignment_attempt, Some(1));
    assert_eq!(fixture.store.list_calls.load(Ordering::SeqCst), 0);
    new_http.stop().await;
}

async fn prepare_registration(
    fixture: &Fixture,
) -> Result<safe_export::SafeExportRegistrationPlan, String> {
    let spec = fixture.spec.clone();
    let store = fixture.store_dyn.clone();
    let prefix = fixture.prefix.clone();
    let manifest = fixture.manifest.clone();
    tokio::task::spawn_blocking(move || {
        safe_export::prepare_registration_with_store(&spec, store, prefix, &manifest)
    })
    .await
    .unwrap()
}

async fn finalize(fixture: &Fixture) -> Result<safe_export::FinalizedSafeExport, String> {
    let spec = fixture.spec.clone();
    let store = fixture.store_dyn.clone();
    let prefix = fixture.prefix.clone();
    let manifest = fixture.manifest.clone();
    tokio::task::spawn_blocking(move || {
        safe_export::finalize_with_store(&spec, store, prefix, &manifest)
    })
    .await
    .unwrap()
}

async fn verify_graph(
    fixture: &Fixture,
) -> Result<safe_export::VerifiedSafeExportSuccessGraph, String> {
    let spec = fixture.spec.clone();
    let store = fixture.store_dyn.clone();
    let prefix = fixture.prefix.clone();
    tokio::task::spawn_blocking(move || {
        safe_export::verify_success_graph_with_store(&spec, store, prefix)
    })
    .await
    .unwrap()
}

async fn publish_direct(fixture: &Fixture, partition: usize) {
    let task = fixture.spec.sealed_part_tasks().unwrap().remove(partition);
    let lease = AssignmentLease {
        task_id: task.task_id.clone(),
        assignment_attempt: 1,
        lease_token: format!("lease-token-{partition}"),
    };
    let descriptor = genohype_pool::distributed::TaskDescriptor {
        id: task.task_id.clone(),
        task_type: "safe_export_part".to_string(),
        label: None,
        index: Some(partition),
        total: Some(2),
        payload: serde_json::to_value(&task).unwrap(),
        assignment_attempt: Some(1),
        lease_token: Some(lease.lease_token),
    };
    let (completion, staging_path) = report_for(fixture, &descriptor, "direct", "worker").await;
    let report: ExportPartReport = serde_json::from_value(completion.result_json.unwrap()).unwrap();
    let receipt_uri = fixture.spec.part_receipt_uri(partition).unwrap();
    let receipt_path =
        super::safe_export_test_path(&fixture.spec.output_root_uri, &receipt_uri).unwrap();
    let store = fixture.store_dyn.clone();
    let prefix = fixture.prefix.clone();
    let spec = fixture.spec.clone();
    tokio::task::spawn_blocking(move || {
        safe_export::publish_claimed_part_with_store(
            store,
            prefix,
            staging_path,
            receipt_path,
            receipt_uri,
            &spec,
            &task,
            &report,
        )
    })
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn resume_rejects_wrong_generation_and_missing_output_without_listing() {
    for delete in [false, true] {
        let fixture = fixture().await;
        publish_direct(&fixture, 0).await;
        let plan = prepare_registration(&fixture).await.unwrap();
        let output_uri = &plan.imported[0].envelope.part.output_object.uri;
        let output_path =
            super::safe_export_test_path(&fixture.spec.output_root_uri, output_uri).unwrap();
        if delete {
            fixture.store.delete(&output_path).await.unwrap();
        } else {
            fixture
                .store
                .put_bytes(&output_path, Bytes::from_static(b"wrong generation"))
                .await;
        }
        let state = empty_state(MetricsDb::in_memory().unwrap(), &fixture, "invalid-resume");
        assert!(matches!(
            get_work(
                axum::extract::State(state.clone()),
                axum::Json(worker_request(&fixture.spec, "worker")),
            )
            .await
            .0,
            WorkResponse::Wait
        ));
        let rejected = api::jobs::submit_job(
            axum::extract::State(state.clone()),
            axum::Json(request(&fixture.spec)),
        )
        .await
        .0;
        assert!(!rejected.acknowledged);
        let error = rejected.error.unwrap();
        assert!(
            error.contains("conditioned output") || error.contains("NotFound"),
            "{error}"
        );
        let data = state.lock().unwrap();
        assert!(data.idle);
        assert!(data.current_job_id.is_none());
        assert!(data.pending_partitions.is_empty());
        assert_eq!(fixture.store.list_calls.load(Ordering::SeqCst), 0);
    }

    let fixture = fixture().await;
    let orphan_sha = "f".repeat(64);
    let orphan = content_addressed_path(&fixture.prefix, &orphan_sha, "parquet").unwrap();
    fixture
        .store
        .put_bytes(&orphan, Bytes::from_static(b"orphan without receipt"))
        .await;
    let plan = prepare_registration(&fixture).await.unwrap();
    assert_eq!(plan.missing_partitions, vec![0, 1]);
    assert_eq!(fixture.store.list_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn success_is_last_and_graph_is_independently_readable() {
    let fixture = fixture().await;
    publish_direct(&fixture, 0).await;
    let before_complete = finalize(&fixture).await.unwrap_err();
    assert!(before_complete.contains("exact complete receipt coverage"));
    let success_path = super::safe_export_test_path(
        &fixture.spec.output_root_uri,
        &fixture.spec.success_uri().unwrap(),
    )
    .unwrap();
    assert!(fixture.store.head(&success_path).await.is_err());

    publish_direct(&fixture, 1).await;
    let plan = prepare_registration(&fixture).await.unwrap();
    let db = MetricsDb::in_memory().unwrap();
    let job = JobRecord {
        job_id: "finalize-job".to_string(),
        status: "running".to_string(),
        start_time_ms: 1,
        end_time_ms: None,
        job_spec_json: serde_json::to_value(JobSpec::SafeExport(fixture.spec.clone())).ok(),
        input_path: fixture.spec.canonical_table_uri.clone(),
        total_tasks: 2,
        job_type: Some("safe export parquet".to_string()),
    };
    db.register_safe_export_job(&job, &fixture.spec, &plan)
        .unwrap();
    db.claim_safe_export_finalization("finalize-job", &fixture.spec.export_id, 2)
        .unwrap();
    assert!(!db.get_safe_export_status("finalize-job").unwrap().complete);

    let receipt_paths: Vec<_> = (0..2)
        .map(|index| {
            super::safe_export_test_path(
                &fixture.spec.output_root_uri,
                &fixture.spec.part_receipt_uri(index).unwrap(),
            )
            .unwrap()
        })
        .collect();
    let prior_operations = fixture.store.operations();
    assert!(receipt_paths.iter().all(|receipt| {
        prior_operations
            .iter()
            .any(|(kind, path)| kind == "PUT" && path == receipt)
    }));
    fixture.store.clear_operations();
    fixture.store.lose_next_success_response();
    let lost = finalize(&fixture).await.unwrap_err();
    assert!(lost.contains("_SUCCESS publication failed"), "{lost}");
    assert!(!db.get_safe_export_status("finalize-job").unwrap().complete);
    let finalized = finalize(&fixture).await.unwrap();

    let writes: Vec<_> = fixture
        .store
        .operations()
        .into_iter()
        .filter(|(kind, _)| kind == "PUT")
        .map(|(_, path)| path)
        .collect();
    let release_path = super::safe_export_test_path(
        &fixture.spec.output_root_uri,
        &fixture.spec.release_uri().unwrap(),
    )
    .unwrap();
    assert_eq!(writes.first(), Some(&release_path));
    assert!(writes.iter().skip(1).all(|path| path == &success_path));
    assert_eq!(writes.last(), Some(&success_path));
    let operations = fixture.store.operations();
    let success_put = operations
        .iter()
        .position(|(kind, path)| kind == "PUT" && path == &success_path)
        .unwrap();
    assert!(operations
        .iter()
        .skip(success_put + 1)
        .any(|(kind, path)| (kind == "HEAD" || kind == "GET") && path == &success_path));

    let graph = verify_graph(&fixture).await.unwrap();
    assert_eq!(graph.output_objects.len(), 2);
    let mut total_rows = 0_u64;
    for (path, expected) in &graph.output_objects {
        let bytes = fixture
            .store
            .get(path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.len() as u64, expected.size);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let observed_descriptor =
            ArrowSchemaDescriptor::from_arrow_schema(builder.schema().as_ref()).unwrap();
        assert_eq!(
            observed_descriptor.fingerprint_sha256().unwrap(),
            graph.sidecar.parquet.schema_fingerprint_sha256,
            "Parquet bytes at {path} do not carry the sealed release schema"
        );
        assert_eq!(
            serde_json::to_value(&observed_descriptor).unwrap(),
            graph.sidecar.parquet.arrow_schema
        );
        let mut reader = builder.build().unwrap();
        total_rows += reader
            .by_ref()
            .map(|batch| batch.unwrap().num_rows() as u64)
            .sum::<u64>();
    }
    assert_eq!(total_rows, graph.sidecar.totals.output_row_count);
    assert_eq!(
        graph.sidecar.parquet.schema_fingerprint_sha256,
        fixture.spec.parquet.schema_fingerprint_sha256
    );
    assert!(!db.get_safe_export_status("finalize-job").unwrap().complete);
    db.complete_safe_export_finalization("finalize-job", &fixture.spec.export_id, &finalized, 2)
        .unwrap();
    assert!(db.get_safe_export_status("finalize-job").unwrap().complete);
    assert_eq!(fixture.store.list_calls.load(Ordering::SeqCst), 0);
}
