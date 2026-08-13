use crate::cli::{SafeExportCommands, SafeExportSealArgs, SafeExportValidateArgs};
use crate::{HailError, Result};
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use genohype_core::codec::ETypeParser;
use genohype_core::io::{join_path, SourceManifest, SourceSnapshot};
use genohype_core::query::QueryEngine;
use genohype_core::release::{
    GlobalsPayload, ParquetDescriptor, SafeExportSpec, SafeExportWorkerIdentity, TableDescriptor,
    ToolIdentity, WriterPropertiesDescriptor, SAFE_EXPORT_COMPLETION_SCHEMA_VERSION,
    SAFE_EXPORT_RECEIPT_SCHEMA_VERSION, SAFE_EXPORT_SCHEMA_VERSION, SAFE_EXPORT_SHARDING_RULE_V1,
};
use genohype_core::schema::{PairedSchema, PhysicalBinaryEncoding};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutOptions,
    PutPayload, PutResult,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Mutex};
use url::Url;

pub fn run_safe_export_command(command: SafeExportCommands) -> Result<()> {
    match command {
        SafeExportCommands::Seal(args) => seal(args),
        SafeExportCommands::Validate(args) => validate(args),
    }
}

fn seal(args: SafeExportSealArgs) -> Result<()> {
    require_local_input(&args.manifest, "manifest")?;
    require_local_input(&args.worker_binary, "worker binary")?;
    let output = local_output(&args.output, false)?.expect("stdout is forbidden");
    preflight_create_new(&output)?;

    let manifest_bytes = std::fs::read(&args.manifest)?;
    let manifest = SourceManifest::from_json(&manifest_bytes)?;
    let manifest_sha256 = manifest.digest_sha256()?;
    let snapshot = SourceSnapshot::new(manifest.clone(), &manifest_sha256)?;

    let worker_bytes = std::fs::read(&args.worker_binary)?;
    validate_stock_worker_identity(&args, &worker_bytes)?;
    let worker_sha256 = format!("{:x}", Sha256::digest(&worker_bytes));

    // Production deliberately opens the canonical GCS table under the
    // digest-checked snapshot. The optional decoded bundle is an explicit
    // local/mock qualification seam: it still binds the exact manifest digest,
    // canonical gs:// identities, and project-owned schema descriptor, but it
    // performs no cloud access and is never an alternate production authority.
    let (mut table, globals, arrow_descriptor) =
        if let Some(bundle_path) = args.metadata_evidence_bundle.as_deref() {
            require_local_input(bundle_path, "metadata evidence bundle")?;
            let bundle: SealMetadataEvidenceBundle =
                serde_json::from_slice(&std::fs::read(bundle_path)?)?;
            if bundle.schema_version != 1
                || bundle.canonical_table_uri != args.table
                || bundle.source_manifest_sha256 != manifest_sha256
            {
                return invalid(
                    "metadata evidence bundle does not match the exact table/manifest authority",
                );
            }
            bundle.globals.validate_digest()?;
            let arrow = PairedSchema::from_raw_vtype(
                ETypeParser::parse(&bundle.table.rows.raw_e_type)?,
                &bundle.table.rows.raw_v_type,
            )?
            .arrow_plan()?
            .descriptor();
            (bundle.table, bundle.globals, arrow)
        } else {
            let engine = QueryEngine::open_path_with_snapshot(&args.table, snapshot.clone())?;
            (
                engine.table_descriptor()?,
                GlobalsPayload::from_value(&engine.globals()?),
                engine
                    .parquet_plan(PhysicalBinaryEncoding::Utf8)?
                    .descriptor(),
            )
        };
    let mut source_parts = Vec::with_capacity(table.partitions.len());
    for partition in &mut table.partitions {
        partition.output_part_ids = vec![format!("part-{:08}", partition.index)];
        let expected_uri = join_path(
            &join_path(&join_path(&args.table, "rows"), "parts"),
            &partition.part_file,
        );
        let source = snapshot.object(&expected_uri)?.clone();
        if source.crc32c.as_deref().is_none_or(str::is_empty) {
            return invalid(format!(
                "GCS row source lacks CRC32C identity: {expected_uri}"
            ));
        }
        source_parts.push(source);
    }

    let schema_fingerprint_sha256 = arrow_descriptor.fingerprint_sha256()?;
    let expected_worker = SafeExportWorkerIdentity {
        git_commit: args.worker_git_commit.clone(),
        build_identity: args.worker_build_identity.clone(),
        binary_sha256: worker_sha256.clone(),
    };
    let mut features = args.worker_features.clone();
    features.sort();
    if features.iter().any(String::is_empty) || features.windows(2).any(|pair| pair[0] == pair[1]) {
        return invalid("worker features must be non-empty and unique");
    }
    let parquet = ParquetDescriptor {
        arrow_schema: serde_json::to_value(arrow_descriptor)?,
        schema_fingerprint_sha256,
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
        output_ordering: table.key.fields.clone(),
    };
    let tool = ToolIdentity {
        package_version: env!("CARGO_PKG_VERSION").to_string(),
        git_commit: args.worker_git_commit.clone(),
        dirty_build: false,
        build_provenance: args.worker_build_identity.clone(),
        binary_name: "genohype".to_string(),
        binary_uri: Some(args.worker_binary_uri),
        binary_sha256: worker_sha256,
        target_triple: "x86_64-unknown-linux-gnu".to_string(),
        enabled_features: features,
        arrow_crate_version: "53.3".to_string(),
        parquet_crate_version: "53.3".to_string(),
    };
    let spec = SafeExportSpec {
        schema_version: SAFE_EXPORT_SCHEMA_VERSION,
        export_id: String::new(),
        canonical_table_uri: args.table,
        source_manifest_uri: args.manifest_uri,
        source_manifest_sha256: manifest_sha256,
        source_snapshot_timestamp: args.snapshot_timestamp,
        output_root_uri: args.output_root,
        source_parts,
        parquet,
        expected_worker,
        table,
        globals,
        tool,
        receipt_schema_version: SAFE_EXPORT_RECEIPT_SCHEMA_VERSION,
        completion_schema_version: SAFE_EXPORT_COMPLETION_SCHEMA_VERSION,
    }
    .with_computed_export_id()?
    .canonicalized_for_publication()?;

    let mut bytes = serde_json::to_vec(&spec)?;
    bytes.push(b'\n');
    write_create_new(&output, &bytes)?;
    eprintln!("safe-export specification sealed");
    eprintln!("  export_id: {}", spec.export_id);
    eprintln!("  manifest_sha256: {}", spec.source_manifest_sha256);
    eprintln!("  partitions: {}", spec.table.partitions.len());
    eprintln!("  output: {}", output.display());
    Ok(())
}

fn validate(args: SafeExportValidateArgs) -> Result<()> {
    require_local_input(&args.spec, "spec")?;
    let output = local_output(&args.output, true)?;
    if let Some(path) = &output {
        preflight_create_new(path)?;
    }
    let spec = SafeExportSpec::from_json(&std::fs::read(&args.spec)?)?;
    let verify_result = if let Some(bundle_path) = args.evidence_bundle {
        require_local_input(&bundle_path, "evidence bundle")?;
        let bundle: EvidenceBundle = serde_json::from_slice(&std::fs::read(bundle_path)?)?;
        let store = Arc::new(BundleStore::from_bundle(bundle)?) as Arc<dyn ObjectStore>;
        let output_prefix = output_prefix(&spec.output_root_uri)?;
        crate::distributed::coordinator::services::safe_export::verify_success_graph_with_store(
            &spec,
            store,
            output_prefix,
        )
    } else {
        crate::distributed::coordinator::services::safe_export::verify_success_graph(&spec)
    };
    let verified = match verify_result {
        Ok(verified) => verified,
        Err(error) => {
            if let Some(path) = &output {
                let _ = std::fs::remove_file(path);
            }
            return Err(HailError::InvalidFormat(format!(
                "safe-export validation failed: {error}"
            )));
        }
    };
    let report = SafeExportValidationReport {
        schema_version: 1,
        valid: true,
        export_id: spec.export_id.clone(),
        success_uri: spec.success_uri()?,
        release_id: verified.sidecar.release_id,
        receipt_set_sha256: verified.sidecar.receipt_set_sha256,
        expected_partition_count: verified.sidecar.totals.expected_partition_count,
        output_part_count: verified.output_objects.len(),
        output_row_count: verified.sidecar.totals.output_row_count,
        output_bytes: verified.sidecar.totals.output_bytes,
        schema_fingerprint_sha256: verified.sidecar.parquet.schema_fingerprint_sha256,
        worker_git_commit: verified.sidecar.tool.git_commit,
        worker_binary_sha256: verified.sidecar.tool.binary_sha256,
    };
    let mut bytes = serde_json::to_vec(&report)?;
    bytes.push(b'\n');
    if let Some(path) = output {
        write_create_new(&path, &bytes)?;
    } else {
        std::io::stdout().lock().write_all(&bytes)?;
    }
    Ok(())
}

fn validate_stock_worker_identity(args: &SafeExportSealArgs, bytes: &[u8]) -> Result<()> {
    if args.worker_git_commit.len() != 7
        || !args
            .worker_git_commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return invalid(
            "stock worker git commit must be exactly seven lowercase hexadecimal characters",
        );
    }
    let expected = format!("{}:{}", env!("CARGO_PKG_VERSION"), args.worker_git_commit);
    if args.worker_build_identity != expected
        || args.worker_build_identity.contains("dirty")
        || args.worker_build_identity.contains("unknown")
    {
        return invalid(format!(
            "stock worker build identity must exactly match {expected:?}"
        ));
    }
    if bytes.len() < 20
        || &bytes[..4] != b"\x7fELF"
        || bytes[4] != 2
        || bytes[5] != 1
        || u16::from_le_bytes([bytes[18], bytes[19]]) != 62
    {
        return invalid("worker binary must be a 64-bit little-endian x86_64 Linux ELF");
    }
    if !bytes
        .windows(args.worker_git_commit.len())
        .any(|window| window == args.worker_git_commit.as_bytes())
    {
        return invalid("worker binary does not embed its advertised git commit");
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SealMetadataEvidenceBundle {
    schema_version: u32,
    canonical_table_uri: String,
    source_manifest_sha256: String,
    table: TableDescriptor,
    globals: GlobalsPayload,
}

#[derive(Debug, Serialize)]
struct SafeExportValidationReport {
    schema_version: u32,
    valid: bool,
    export_id: String,
    success_uri: String,
    release_id: String,
    receipt_set_sha256: String,
    expected_partition_count: usize,
    output_part_count: usize,
    output_row_count: u64,
    output_bytes: u64,
    schema_fingerprint_sha256: String,
    worker_git_commit: String,
    worker_binary_sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceBundle {
    schema_version: u32,
    objects: Vec<EvidenceObject>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceObject {
    /// Object-store path below the bucket, without a leading slash.
    path: String,
    version: String,
    etag: String,
    bytes_hex: String,
}

#[derive(Debug)]
struct BundleStore {
    inner: InMemory,
    identities: Mutex<HashMap<Path, (String, String)>>,
}

impl BundleStore {
    fn from_bundle(bundle: EvidenceBundle) -> Result<Self> {
        if bundle.schema_version != 1 {
            return invalid("unsupported safe-export evidence bundle schema version");
        }
        let store = Self {
            inner: InMemory::new(),
            identities: Mutex::new(HashMap::new()),
        };
        let mut paths = HashSet::with_capacity(bundle.objects.len());
        for object in bundle.objects {
            if object.path.is_empty()
                || object.path.starts_with('/')
                || object.path.contains('\\')
                || object
                    .path
                    .split('/')
                    .any(|part| matches!(part, "" | "." | ".."))
                || object.version.is_empty()
                || object.etag.is_empty()
            {
                return invalid("evidence bundle contains an invalid path or identity");
            }
            let path = Path::parse(&object.path).map_err(|error| {
                HailError::InvalidFormat(format!("invalid evidence path: {error}"))
            })?;
            if !paths.insert(path.clone()) {
                return invalid("evidence bundle contains a duplicate path");
            }
            let bytes = decode_hex(&object.bytes_hex)?;
            futures::executor::block_on(store.inner.put_opts(
                &path,
                Bytes::from(bytes).into(),
                PutOptions::from(PutMode::Overwrite),
            ))
            .map_err(HailError::ObjectStore)?;
            store
                .identities
                .lock()
                .unwrap()
                .insert(path, (object.version, object.etag));
        }
        Ok(store)
    }

    fn patch_meta(&self, meta: &mut ObjectMeta) {
        if let Some((version, etag)) = self.identities.lock().unwrap().get(&meta.location) {
            meta.version = Some(version.clone());
            meta.e_tag = Some(etag.clone());
        }
    }
}

impl std::fmt::Display for BundleStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "safe-export-read-only-evidence-bundle")
    }
}

#[async_trait::async_trait]
impl ObjectStore for BundleStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _options: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(read_only_store_error())
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _options: object_store::PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(read_only_store_error())
    }

    async fn get_opts(
        &self,
        location: &Path,
        mut options: GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let identity = self.identities.lock().unwrap().get(location).cloned();
        let Some((version, etag)) = identity else {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "object absent from evidence bundle".into(),
            });
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
                source: "evidence identity mismatch".into(),
            });
        }
        options.version = None;
        options.if_match = None;
        let mut result = self.inner.get_opts(location, options).await?;
        self.patch_meta(&mut result.meta);
        Ok(result)
    }

    async fn delete(&self, _location: &Path) -> object_store::Result<()> {
        Err(read_only_store_error())
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        stream::once(async { Err(read_only_store_error()) }).boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        Err(read_only_store_error())
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(read_only_store_error())
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(read_only_store_error())
    }
}

fn read_only_store_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "safe-export-evidence-bundle",
        source: "evidence bundle is exact-path read-only; listing and mutation are forbidden"
            .into(),
    }
}

fn output_prefix(root: &str) -> Result<Path> {
    let url = Url::parse(root)
        .map_err(|error| HailError::InvalidFormat(format!("invalid output root URI: {error}")))?;
    Path::from_url_path(url.path())
        .map_err(|error| HailError::InvalidFormat(format!("invalid output root path: {error}")))
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    if value.len() % 2 != 0 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return invalid("evidence bytes_hex must contain an even number of hexadecimal characters");
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16)
                .map_err(|_| HailError::InvalidFormat("invalid evidence bytes_hex".to_string()))
        })
        .collect()
}

fn require_local_input(value: &str, label: &str) -> Result<()> {
    if value.is_empty() || value.contains("://") || value == "-" {
        return invalid(format!("{label} must be a local file"));
    }
    Ok(())
}

fn local_output(value: &str, allow_stdout: bool) -> Result<Option<PathBuf>> {
    if value == "-" && allow_stdout {
        return Ok(None);
    }
    if value.is_empty() || value.contains("://") || value == "-" {
        return invalid("safe-export output must be a local file");
    }
    Ok(Some(PathBuf::from(value)))
}

fn preflight_create_new(path: &FsPath) -> Result<()> {
    if path.exists() {
        return invalid(format!(
            "refusing to overwrite existing output: {}",
            path.display()
        ));
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            return invalid(format!(
                "output parent directory does not exist: {}",
                parent.display()
            ));
        }
    }
    Ok(())
}

fn write_create_new(path: &FsPath, bytes: &[u8]) -> Result<()> {
    let mut file = create_new(path)?;
    if let Err(error) = file.write_all(bytes).and_then(|()| file.sync_all()) {
        let _ = std::fs::remove_file(path);
        return Err(HailError::Io(error));
    }
    Ok(())
}

fn create_new(path: &FsPath) -> Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(HailError::Io)
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(HailError::InvalidFormat(message.into()))
}
