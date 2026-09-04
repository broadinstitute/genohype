use crate::association_server::{serve_dataset_viewer, ViewerOptions};
use crate::association_session::{
    AssociationSessionRegistry, BuildClaim, CleanupPolicy, CleanupSummary,
    CompletedAssociationSession, NormalizedSessionRequestV1, SessionCompletion, SessionLookup,
    SessionPolicy,
};
use crate::association_sources::delimited::DelimitedAssociationSource;
use crate::association_sources::hail::HailAssociationSource;
use crate::association_sources::mapping::load_mapping;
use crate::association_sources::vcf::VcfAssociationSource;
use crate::association_sources::{detect_source, DetectedAssociationSource};
use crate::cli::{
    association_retention_duration, validate_association_raster_dimensions, AssociationBuildArg,
    AssociationViewArgs,
};
use crate::{HailError, Result};
use genohype_association::{
    build_association_session, canonical_contig_lengths, Assembly, AssociationBuildConfig,
    AssociationBuildOptionsV1, AssociationBuildProduct, AssociationSource, AssociationThresholdV1,
    Background, BuildCancellation, GenomeLayoutV1, NormalizedFieldMappingV1, ResourceDescriptorV1,
    SessionBackgroundV1, SqliteAssociationStore, SCHEMA_VERSION_V1, STORE_SCHEMA_VERSION_V1,
};
use genohype_core::io::{inventory_hail_table_path, InventoryOptions, SourceSnapshot};
use genohype_core::query::QueryEngine;
use std::fs::File;
use std::future::Future;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CONTIG_GAP: u32 = 4;
const POINT_RADIUS: f32 = 2.5;
const POINT_ALPHA: u8 = 178;
const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;
const MAX_PRINTED_REJECTION_REASONS: usize = 10;
const THRESHOLD_NAME: &str = "genome_wide_significance";
const TITLE: &str = "Association dataset";
const RENDERER_VERSION: &str = env!("GENOHYPE_VERSION");

#[derive(Debug)]
struct PreparedAssociationView {
    args: AssociationViewArgs,
    detected: DetectedAssociationSource,
    mapping: NormalizedFieldMappingV1,
    layout: GenomeLayoutV1,
    request: NormalizedSessionRequestV1,
    retention: Duration,
}

struct AcquiredAssociationView {
    session: CompletedAssociationSession,
    state: AcquisitionState,
    cleanup: Option<CleanupSummary>,
    port: u16,
    open: bool,
}

enum AcquisitionState {
    Warm,
    Cold(ColdBuildSummary),
}

struct ColdBuildSummary {
    rows_seen: u64,
    valid_rows: u64,
    rejected_rows: u64,
    rendered_rows: u64,
    significant_rows: u64,
    rejection_reasons: Vec<(String, u64)>,
}

pub fn run(args: AssociationViewArgs) -> Result<()> {
    let prepared = prepare(args)?;
    let cache_root = association_cache_root()?;
    print_storage_notice(&prepared.args, &cache_root)?;
    // Resolve and open the exact printed XDG cache root on this thread. On macOS,
    // platform directory lookup can otherwise disagree after crossing a worker thread.
    let registry = AssociationSessionRegistry::open_default().map_err(invalid)?;

    let cancellation = BuildCancellation::default();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(HailError::Io)?;
    let acquired = runtime.block_on(acquire_with_cancellation(prepared, registry, cancellation))?;
    drop(runtime);

    print_acquisition_summary(&acquired)?;
    let options = ViewerOptions {
        port: acquired.port,
        open: acquired.open,
    };
    // CompletedAssociationSession owns the active generation locks. In ephemeral
    // mode its final drop, after graceful server shutdown, removes the generation.
    serve_dataset_viewer(acquired.session, options)
}

fn prepare(args: AssociationViewArgs) -> Result<PreparedAssociationView> {
    validate_association_raster_dimensions(args.width, args.height).map_err(invalid)?;
    let retention = association_retention_duration(args.retention_hours).map_err(invalid)?;
    if !args.threshold.is_finite() || args.threshold <= 0.0 || args.threshold > 1.0 {
        return Err(HailError::InvalidFormat(
            "--threshold must be finite and in 0 < P <= 1".into(),
        ));
    }
    let detected = detect_source(&args.source).map_err(invalid)?;
    let mapping = load_mapping(&args.mapping, Some(args.build))
        .and_then(|mapping| mapping.complete())
        .map_err(invalid)?;
    let assembly = assembly(args.build);
    let layout = GenomeLayoutV1::new(
        canonical_contig_lengths(assembly),
        args.width,
        args.height,
        CONTIG_GAP,
    )
    .map_err(invalid)?;
    let thresholds = vec![AssociationThresholdV1 {
        name: THRESHOLD_NAME.into(),
        pvalue: args.threshold,
    }];
    let request = NormalizedSessionRequestV1 {
        association_schema_version: SCHEMA_VERSION_V1.into(),
        store_schema_version: STORE_SCHEMA_VERSION_V1,
        mapping: mapping.clone(),
        assembly: args.build.as_str().into(),
        thresholds,
        significance_threshold: args.threshold,
        top_hits: args.top_hits,
        width: args.width,
        height: args.height,
        layout_hash: layout.layout_hash(args.build.as_str()),
        point_radius: POINT_RADIUS,
        point_alpha: POINT_ALPHA,
        background: SessionBackgroundV1::White,
        renderer_version: RENDERER_VERSION.into(),
    };

    Ok(PreparedAssociationView {
        args,
        detected,
        mapping,
        layout,
        request,
        retention,
    })
}

async fn acquire_with_cancellation(
    prepared: PreparedAssociationView,
    registry: AssociationSessionRegistry,
    cancellation: BuildCancellation,
) -> Result<AcquiredAssociationView> {
    let port = prepared.args.port.unwrap_or(0);
    let open = !prepared.args.no_open;
    let task = tokio::task::spawn_blocking({
        let cancellation = cancellation.clone();
        move || acquire_or_build(prepared, registry, cancellation, port, open)
    });
    await_acquisition_or_signal(task, tokio::signal::ctrl_c(), cancellation).await
}

async fn await_acquisition_or_signal<T, S>(
    mut task: tokio::task::JoinHandle<Result<T>>,
    signal: S,
    cancellation: BuildCancellation,
) -> Result<T>
where
    S: Future<Output = std::io::Result<()>>,
{
    tokio::pin!(signal);
    tokio::select! {
        // Give an observed signal deterministic precedence at the boundary. If
        // publication already committed, awaiting the worker below still returns
        // its successful completed-session handle rather than reporting a false
        // cancellation for a registered generation.
        biased;
        signal = &mut signal => {
            signal.map_err(HailError::Io)?;
            cancellation.cancel();
            // The blocking worker owns the claim and source. Await it so Drop can
            // roll back SQLite and remove staging before this process returns.
            match task.await.map_err(join_error)? {
                Ok(acquired) => Ok(acquired),
                Err(_) => Err(cancelled_build()),
            }
        }
        result = &mut task => result.map_err(join_error)?,
    }
}

fn cancelled_build() -> HailError {
    HailError::InvalidFormat("association view build was cancelled; no viewer was started".into())
}

fn join_error(error: tokio::task::JoinError) -> HailError {
    HailError::InvalidFormat(format!("association view worker failed: {error}"))
}

fn acquire_or_build(
    prepared: PreparedAssociationView,
    registry: AssociationSessionRegistry,
    cancellation: BuildCancellation,
    port: u16,
    open: bool,
) -> Result<AcquiredAssociationView> {
    cancellation.check().map_err(invalid)?;
    let cleanup = if prepared.args.cleanup_expired {
        Some(
            registry
                .cleanup(CleanupPolicy::default())
                .map_err(invalid)?,
        )
    } else {
        None
    };
    cancellation.check().map_err(invalid)?;

    let policy = SessionPolicy {
        rebuild: prepared.args.rebuild,
        ephemeral: prepared.args.ephemeral,
        retention: prepared.retention,
    };
    let allow_reuse = !policy.rebuild && !policy.ephemeral;

    let acquired = match prepared.detected {
        DetectedAssociationSource::Hail => {
            let inventory =
                inventory_hail_table_path(&prepared.args.source, &InventoryOptions::default())?;
            cancellation.check().map_err(invalid)?;
            // Local inventory canonicalizes its root (for example /var ->
            // /private/var on macOS), and its object URIs are rooted there. Open
            // the engine at that same canonical authority so every read resolves
            // to an identity present in the snapshot.
            let inventory_root = inventory.root.clone();
            let snapshot = SourceSnapshot::new(inventory.manifest, &inventory.digest_sha256)?;
            // SourceSnapshot keeps the inventory authority in its canonical bare
            // form. Session observations use the checksum wire form required by
            // every other registry digest.
            let stable_observation = format!("sha256:{}", snapshot.digest_sha256());
            let lookup = SessionLookup::observe_stable(
                "hail-inventory-v1",
                &stable_observation,
                prepared.request.clone(),
            )
            .map_err(invalid)?;
            if allow_reuse {
                if let Some(session) = registry
                    .lookup_valid(&lookup, SystemTime::now())
                    .map_err(invalid)?
                {
                    return Ok(with_runtime_options(
                        session,
                        AcquisitionState::Warm,
                        cleanup,
                        port,
                        open,
                    ));
                }
            }
            let claim = registry.claim(lookup).map_err(invalid)?;
            if allow_reuse {
                if let Some(session) = claim.recheck_valid(SystemTime::now()).map_err(invalid)? {
                    return Ok(with_runtime_options(
                        session,
                        AcquisitionState::Warm,
                        cleanup,
                        port,
                        open,
                    ));
                }
            }
            cancellation.check().map_err(invalid)?;
            let engine = QueryEngine::open_path_with_snapshot(&inventory_root, snapshot)?;
            let mut source = HailAssociationSource::from_engine(
                engine,
                prepared.mapping.clone(),
                prepared.args.build.as_str(),
            )
            .map_err(invalid)?;
            build_claim(
                claim,
                &mut source,
                &prepared,
                &policy,
                &cancellation,
                cleanup,
                port,
                open,
            )?
        }
        detected @ DetectedAssociationSource::Delimited { .. } => {
            let path = Path::new(&prepared.args.source);
            let lookup =
                SessionLookup::observe_local(path, prepared.request.clone()).map_err(invalid)?;
            if allow_reuse {
                if let Some(session) = registry
                    .lookup_valid(&lookup, SystemTime::now())
                    .map_err(invalid)?
                {
                    return Ok(with_runtime_options(
                        session,
                        AcquisitionState::Warm,
                        cleanup,
                        port,
                        open,
                    ));
                }
            }
            let claim = registry.claim(lookup).map_err(invalid)?;
            if allow_reuse {
                if let Some(session) = claim.recheck_valid(SystemTime::now()).map_err(invalid)? {
                    return Ok(with_runtime_options(
                        session,
                        AcquisitionState::Warm,
                        cleanup,
                        port,
                        open,
                    ));
                }
            }
            cancellation.check().map_err(invalid)?;
            let mut source = DelimitedAssociationSource::open(
                path,
                detected,
                prepared.mapping.clone(),
                prepared.args.build.as_str(),
            )
            .map_err(invalid)?;
            build_claim(
                claim,
                &mut source,
                &prepared,
                &policy,
                &cancellation,
                cleanup,
                port,
                open,
            )?
        }
        detected @ DetectedAssociationSource::Vcf => {
            let path = Path::new(&prepared.args.source);
            let lookup =
                SessionLookup::observe_local(path, prepared.request.clone()).map_err(invalid)?;
            if allow_reuse {
                if let Some(session) = registry
                    .lookup_valid(&lookup, SystemTime::now())
                    .map_err(invalid)?
                {
                    return Ok(with_runtime_options(
                        session,
                        AcquisitionState::Warm,
                        cleanup,
                        port,
                        open,
                    ));
                }
            }
            let claim = registry.claim(lookup).map_err(invalid)?;
            if allow_reuse {
                if let Some(session) = claim.recheck_valid(SystemTime::now()).map_err(invalid)? {
                    return Ok(with_runtime_options(
                        session,
                        AcquisitionState::Warm,
                        cleanup,
                        port,
                        open,
                    ));
                }
            }
            cancellation.check().map_err(invalid)?;
            let mut source = VcfAssociationSource::open(
                path,
                detected,
                prepared.mapping.clone(),
                prepared.args.build.as_str(),
            )
            .map_err(invalid)?;
            build_claim(
                claim,
                &mut source,
                &prepared,
                &policy,
                &cancellation,
                cleanup,
                port,
                open,
            )?
        }
    };
    Ok(acquired)
}

fn build_claim<S: AssociationSource>(
    claim: BuildClaim,
    source: &mut S,
    prepared: &PreparedAssociationView,
    policy: &SessionPolicy,
    cancellation: &BuildCancellation,
    cleanup: Option<CleanupSummary>,
    port: u16,
    open: bool,
) -> Result<AcquiredAssociationView> {
    cancellation.check().map_err(invalid)?;
    let created_at = SystemTime::now();
    let created_at_text = format_system_time(created_at)?;
    let dataset_id = claim.dataset_id().clone();
    let route_base = format!("datasets/{dataset_id}");
    let config = AssociationBuildConfig {
        dataset_id,
        layout: prepared.layout.clone(),
        thresholds: prepared.request.thresholds.clone(),
        options: AssociationBuildOptionsV1 {
            top_hits: prepared.args.top_hits,
            insert_batch_size: genohype_association::MAX_INSERT_BATCH_SIZE,
            significance_threshold: prepared.args.threshold,
        },
        point_radius: prepared.request.point_radius,
        point_alpha: prepared.request.point_alpha,
        background: Background::White,
        title: TITLE.into(),
        genohype_version: prepared.request.renderer_version.clone(),
        created_at: created_at_text,
        table_resource: json_resource(format!("{route_base}/table")),
        region_resource: json_resource(format!("{route_base}/region")),
    };

    let mut store = SqliteAssociationStore::create(claim.database_path()).map_err(invalid)?;
    let product =
        build_association_session(source, &mut store, &config, cancellation).map_err(invalid)?;
    // SQLite must be fully closed before lifecycle code hashes, syncs, validates,
    // renames, and publishes the generation.
    drop(store);
    cancellation.check().map_err(invalid)?;
    claim
        .write_raster(&product.png_bytes, cancellation)
        .map_err(invalid)?;
    claim
        .write_manifest(&product.manifest, cancellation)
        .map_err(invalid)?;
    let summary = cold_summary(&product);
    let completion = SessionCompletion {
        source_fingerprint: product.source_fingerprint,
        created_at,
        expires_at: policy.expires_at(created_at).map_err(invalid)?,
        ephemeral: policy.ephemeral,
    };
    let session = claim.publish(completion, cancellation).map_err(invalid)?;
    Ok(with_runtime_options(
        session,
        AcquisitionState::Cold(summary),
        cleanup,
        port,
        open,
    ))
}

fn json_resource(url: String) -> ResourceDescriptorV1 {
    ResourceDescriptorV1 {
        url,
        media_type: "application/json".into(),
        byte_size: None,
        checksum: None,
        expires_at: None,
    }
}

fn cold_summary(product: &AssociationBuildProduct) -> ColdBuildSummary {
    ColdBuildSummary {
        rows_seen: product.progress.rows_seen,
        valid_rows: product.summary.valid_row_count,
        rejected_rows: product.summary.skipped_row_count,
        rendered_rows: product.summary.rendered_point_count,
        significant_rows: product.summary.significant_hit_count,
        rejection_reasons: product
            .rejection_diagnostics
            .reasons
            .iter()
            .take(MAX_PRINTED_REJECTION_REASONS)
            .map(|(reason, diagnostics)| (reason.clone(), diagnostics.count))
            .collect(),
    }
}

fn with_runtime_options(
    session: CompletedAssociationSession,
    state: AcquisitionState,
    cleanup: Option<CleanupSummary>,
    port: u16,
    open: bool,
) -> AcquiredAssociationView {
    AcquiredAssociationView {
        session,
        state,
        cleanup,
        port,
        open,
    }
}

fn print_storage_notice(args: &AssociationViewArgs, cache_root: &Path) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    writeln!(
        stdout,
        "Association derivatives are private local plaintext."
    )?;
    writeln!(stdout, "Association cache root: {}", cache_root.display())?;
    if args.ephemeral {
        writeln!(
            stdout,
            "Retention: --ephemeral; this generation will be deleted after the viewer shuts down."
        )?;
    } else {
        writeln!(
            stdout,
            "Retention: {} hours after creation; use --ephemeral to delete a generation after the viewer shuts down.",
            args.retention_hours
        )?;
    }
    stdout.flush()?;
    Ok(())
}

fn print_acquisition_summary(acquired: &AcquiredAssociationView) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    if let Some(cleanup) = &acquired.cleanup {
        writeln!(
            stdout,
            "Cleanup: staging_removed={} sessions_removed={} retained_locked={} refused={}",
            cleanup.staging_removed,
            cleanup.sessions_removed,
            cleanup.retained_locked,
            cleanup.refused_entries
        )?;
    }
    match &acquired.state {
        AcquisitionState::Warm => {
            let manifest = read_manifest(&acquired.session)?;
            writeln!(
                stdout,
                "Association session: warm reuse; dataset_id={}",
                acquired.session.dataset_id()
            )?;
            writeln!(
                stdout,
                "Rows: seen={} valid={} rejected={} rendered={} significant={}",
                manifest.row_count,
                manifest.valid_row_count,
                manifest.skipped_row_count,
                manifest.rendered_point_count,
                manifest.significant_hit_count
            )?;
            writeln!(
                stdout,
                "Rejections: {} (reason counts are reported only by the cold build that observed them)",
                manifest.skipped_row_count
            )?;
        }
        AcquisitionState::Cold(summary) => {
            writeln!(
                stdout,
                "Association session: cold build; dataset_id={}",
                acquired.session.dataset_id()
            )?;
            writeln!(
                stdout,
                "Rows: seen={} valid={} rejected={} rendered={} significant={}",
                summary.rows_seen,
                summary.valid_rows,
                summary.rejected_rows,
                summary.rendered_rows,
                summary.significant_rows
            )?;
            if summary.rejection_reasons.is_empty() {
                writeln!(stdout, "Rejections: 0")?;
            } else {
                let reasons = summary
                    .rejection_reasons
                    .iter()
                    .map(|(reason, count)| format!("{reason}={count}"))
                    .collect::<Vec<_>>()
                    .join(",");
                writeln!(stdout, "Rejections: {} ({reasons})", summary.rejected_rows)?;
            }
        }
    }
    stdout.flush()?;
    Ok(())
}

fn read_manifest(
    session: &CompletedAssociationSession,
) -> Result<genohype_association::AssociationVisualizationManifestV1> {
    let mut file = File::open(session.manifest_path())?;
    if file.metadata()?.len() > MAX_MANIFEST_BYTES {
        return Err(invalid("completed association manifest is too large"));
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(MAX_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(invalid("completed association manifest is too large"));
    }
    serde_json::from_slice(&bytes).map_err(invalid)
}

fn association_cache_root() -> Result<PathBuf> {
    dirs::cache_dir()
        .map(|cache| cache.join("genohype").join("association").join("v1"))
        .ok_or_else(|| invalid("association cache directory is unavailable"))
}

fn format_system_time(time: SystemTime) -> Result<String> {
    let seconds = time.duration_since(UNIX_EPOCH).map_err(invalid)?.as_secs();
    Ok(format!("unix:{seconds}"))
}

const fn assembly(build: AssociationBuildArg) -> Assembly {
    match build {
        AssociationBuildArg::Grch37 => Assembly::Grch37,
        AssociationBuildArg::Grch38 => Assembly::Grch38,
    }
}

fn invalid(error: impl ToString) -> HailError {
    HailError::InvalidFormat(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::AssociationMappingArgs;

    fn args(source: String) -> AssociationViewArgs {
        AssociationViewArgs {
            source,
            build: AssociationBuildArg::Grch38,
            mapping: AssociationMappingArgs {
                mappings: vec![
                    "contig=CHR".into(),
                    "position=POS".into(),
                    "pvalue=P".into(),
                ],
                mapping_file: None,
            },
            threshold: 5e-8,
            top_hits: 100,
            width: 3000,
            height: 800,
            no_open: true,
            port: Some(0),
            rebuild: false,
            ephemeral: false,
            retention_hours: 168,
            cleanup_expired: false,
        }
    }

    #[test]
    fn preparation_uses_canonical_layout_and_matching_build_identity() {
        let prepared = prepare(args("input.tsv".into())).unwrap();
        assert_eq!(prepared.layout.contigs.len(), 25);
        assert_eq!(prepared.layout.contigs[0].name, "1");
        assert_eq!(prepared.layout.contigs.last().unwrap().name, "MT");
        assert_eq!(
            prepared.request.layout_hash,
            prepared.layout.layout_hash("GRCh38")
        );
        assert_eq!(
            prepared.request.thresholds[0].pvalue,
            prepared.args.threshold
        );
        assert_eq!(
            prepared.request.significance_threshold,
            prepared.args.threshold
        );
        assert_eq!(prepared.request.point_radius, POINT_RADIUS);
        assert_eq!(prepared.request.point_alpha, POINT_ALPHA);
        assert_eq!(prepared.request.background, SessionBackgroundV1::White);
    }

    #[tokio::test]
    async fn published_worker_success_wins_a_simultaneous_signal() {
        let cancellation = BuildCancellation::default();
        let task = tokio::spawn(async { Ok::<_, HailError>(17_u8) });
        tokio::task::yield_now().await;

        let value = await_acquisition_or_signal(task, async { Ok(()) }, cancellation.clone())
            .await
            .unwrap();

        assert_eq!(value, 17);
        assert!(cancellation.is_cancelled());
    }

    #[tokio::test]
    async fn signal_still_cancels_an_incomplete_worker() {
        let cancellation = BuildCancellation::default();
        let worker_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            while !worker_cancellation.is_cancelled() {
                tokio::task::yield_now().await;
            }
            Err::<(), _>(invalid("worker observed cancellation"))
        });

        let error = await_acquisition_or_signal(task, async { Ok(()) }, cancellation)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("build was cancelled"));
    }

    #[test]
    fn invalid_mapping_threshold_dimensions_or_retention_fail_before_acquisition() {
        let mut invalid_threshold = args("input.tsv".into());
        invalid_threshold.threshold = 0.0;
        assert!(prepare(invalid_threshold)
            .unwrap_err()
            .to_string()
            .contains("threshold"));

        let mut invalid_dimensions = args("input.tsv".into());
        invalid_dimensions.width = 16_384;
        invalid_dimensions.height = 16_384;
        assert!(prepare(invalid_dimensions)
            .unwrap_err()
            .to_string()
            .contains("pixels"));

        let mut invalid_width = args("input.tsv".into());
        invalid_width.width = genohype_association::MAX_RASTER_DIMENSION + 1;
        invalid_width.height = 1;
        assert!(prepare(invalid_width)
            .unwrap_err()
            .to_string()
            .contains("width and height"));

        let mut invalid_retention = args("input.tsv".into());
        invalid_retention.retention_hours = 0;
        assert!(prepare(invalid_retention)
            .unwrap_err()
            .to_string()
            .contains("retention"));

        let mut invalid_mapping = args("input.tsv".into());
        invalid_mapping.mapping.mappings.clear();
        assert!(prepare(invalid_mapping)
            .unwrap_err()
            .to_string()
            .contains("mapping requires"));
    }
}
