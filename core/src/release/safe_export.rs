//! Strict, storage-independent safe-export protocol contracts.
//!
//! Workers produce only attempt-private staging evidence. Publication and
//! durable completion authority remain coordinator concerns.

use super::{
    ComponentDescriptor, GlobalsPayload, ParquetDescriptor, PartReceipt, PartStatus,
    PhysicalTypeDescriptor, SourcePartition, TableDescriptor, ToolIdentity,
};
use crate::codec::{canonical_bytes, ETypeParser, EncodedValue};
use crate::io::{join_path, SourceManifest, SourceObject};
use crate::schema::{ArrowSchemaDescriptor, PairedSchema, VTypeParser};
use crate::{HailError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use url::Url;
use uuid::Uuid;

pub const SAFE_EXPORT_SCHEMA_VERSION: u32 = 1;
pub const SAFE_EXPORT_RECEIPT_SCHEMA_VERSION: u32 = 1;
pub const SAFE_EXPORT_COMPLETION_SCHEMA_VERSION: u32 = 1;
pub const SAFE_EXPORT_SHARDING_RULE_V1: &str = "one_hail_row_partition_per_parquet_part_v1";
const EXPORT_ID_DOMAIN: &[u8] = b"genohype.safe-export.identity.sha256.v1\0";
const DECODED_ROWS_DOMAIN: &[u8] = b"genohype.safe-export.decoded-rows.sha256.v1\0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeExportWorkerIdentity {
    pub git_commit: String,
    pub build_identity: String,
    pub binary_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeExportSpec {
    pub schema_version: u32,
    pub export_id: String,
    pub canonical_table_uri: String,
    pub source_manifest_uri: String,
    pub source_manifest_sha256: String,
    /// Stable inventory/snapshot time sealed into the export identity. It must
    /// never be synthesized by a finalizer.
    pub source_snapshot_timestamp: String,
    pub output_root_uri: String,
    /// Exact row-part object identities in source partition order.
    pub source_parts: Vec<SourceObject>,
    pub parquet: ParquetDescriptor,
    pub expected_worker: SafeExportWorkerIdentity,
    pub table: TableDescriptor,
    pub globals: GlobalsPayload,
    pub tool: ToolIdentity,
    pub receipt_schema_version: u32,
    pub completion_schema_version: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeExportPartTask {
    pub schema_version: u32,
    pub export_id: String,
    pub task_id: String,
    pub partition_index: usize,
    pub source_manifest_sha256: String,
    pub source_part_file: String,
    pub source_object: SourceObject,
    pub output_part_id: String,
    pub schema_fingerprint_sha256: String,
}

/// Export-bound immutable publication evidence for one canonical partition.
///
/// The envelope identity prevents a valid-looking [`PartReceipt`] from being
/// copied between exports, manifests, or tasks. The nested receipt remains the
/// release-sidecar representation so finalization does not need a lossy
/// conversion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeExportPartReceipt {
    pub schema_version: u32,
    pub export_id: String,
    pub source_manifest_sha256: String,
    pub task_id: String,
    pub partition_index: usize,
    pub part: PartReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeExportReceiptIdentity {
    pub partition_index: usize,
    pub receipt_object: SourceObject,
}

/// Deterministic root of trust published only after `release.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeExportSuccess {
    pub schema_version: u32,
    pub export_id: String,
    pub release_object: SourceObject,
    pub receipts: Vec<SafeExportReceiptIdentity>,
    pub receipt_set_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportPartReport {
    pub schema_version: u32,
    pub export_id: String,
    pub task_id: String,
    pub partition_index: usize,
    pub source_manifest_sha256: String,
    pub source_part_file: String,
    pub source_object: SourceObject,
    pub schema_fingerprint_sha256: String,
    pub staging_object: SourceObject,
    pub byte_size: u64,
    pub row_count: u64,
    pub row_group_count: u64,
    pub decoded_rows_sha256: String,
    pub first_key: Option<crate::codec::LosslessValue>,
    pub last_key: Option<crate::codec::LosslessValue>,
    pub worker_id: String,
    pub worker_git_commit: String,
    pub worker_binary_sha256: String,
    pub assignment_attempt: u64,
    pub lease_identity_sha256: String,
    pub started_at: String,
    pub completed_at: String,
}

impl SafeExportSpec {
    /// Strictly decode and validate a submitted safe-export intent.
    pub fn from_json(data: &[u8]) -> Result<Self> {
        let value: Self = serde_json::from_slice(data)
            .map_err(|error| invalid(format!("invalid safe-export spec JSON: {error}")))?;
        value.validate()?;
        Ok(value)
    }

    /// Return a copy sealed with its stable content identity.
    pub fn with_computed_export_id(mut self) -> Result<Self> {
        self.validate_intent()?;
        self.export_id = self.computed_export_id()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_intent()?;
        validate_sha256("export ID", &self.export_id)?;
        let computed = self.computed_export_id()?;
        if computed != self.export_id {
            return Err(invalid(format!(
                "safe-export ID mismatch: declared {}, computed {computed}",
                self.export_id
            )));
        }
        Ok(())
    }

    /// Stable identity excludes manifest location and all runtime assignment
    /// state. JSON object keys are recursively sorted before hashing.
    pub fn computed_export_id(&self) -> Result<String> {
        self.validate_intent()?;
        let mut identity = serde_json::to_value(self)
            .map_err(|error| invalid(format!("failed to encode safe-export identity: {error}")))?;
        let object = identity
            .as_object_mut()
            .ok_or_else(|| invalid("safe-export identity must encode as an object"))?;
        object.remove("export_id");
        object.remove("source_manifest_uri");

        // Feature declarations and index object lists are semantically
        // unordered. Source partition identities deliberately remain ordered.
        if let Some(features) = object
            .get_mut("tool")
            .and_then(Value::as_object_mut)
            .and_then(|tool| tool.get_mut("enabled_features"))
            .and_then(Value::as_array_mut)
        {
            features.sort_by(|left, right| {
                left.as_str()
                    .unwrap_or_default()
                    .cmp(right.as_str().unwrap_or_default())
            });
        }
        if let Some(partitions) = object
            .get_mut("table")
            .and_then(Value::as_object_mut)
            .and_then(|table| table.get_mut("partitions"))
            .and_then(Value::as_array_mut)
        {
            for partition in partitions {
                if let Some(index_objects) = partition
                    .as_object_mut()
                    .and_then(|part| part.get_mut("index_objects"))
                    .and_then(Value::as_array_mut)
                {
                    index_objects.sort_by(|left, right| {
                        left.as_str()
                            .unwrap_or_default()
                            .cmp(right.as_str().unwrap_or_default())
                    });
                }
            }
        }

        let canonical = canonical_json(identity);
        let bytes = serde_json::to_vec(&canonical).map_err(|error| {
            invalid(format!(
                "failed to encode canonical safe-export identity: {error}"
            ))
        })?;
        Ok(domain_sha256(EXPORT_ID_DOMAIN, &bytes))
    }

    /// Derive one task from the digest-bound manifest. The source identity is
    /// looked up by its exact row-part URI; suffix or basename matching is
    /// deliberately forbidden.
    pub fn task_for_partition(
        &self,
        partition_index: usize,
        manifest: &SourceManifest,
    ) -> Result<SafeExportPartTask> {
        self.validate()?;
        let digest = manifest.digest_sha256()?;
        if digest != self.source_manifest_sha256 {
            return Err(invalid(format!(
                "task source manifest mismatch: expected {}, got {digest}",
                self.source_manifest_sha256
            )));
        }
        let partition = self.table.partitions.get(partition_index).ok_or_else(|| {
            invalid(format!(
                "safe-export partition {partition_index} is out of bounds"
            ))
        })?;
        let expected_uri = row_part_uri(&self.canonical_table_uri, &partition.part_file);
        let source_object = self
            .source_parts
            .get(partition_index)
            .cloned()
            .ok_or_else(|| {
                invalid(format!(
                    "safe-export spec has no source identity for partition {partition_index}"
                ))
            })?;
        if source_object.uri != expected_uri
            || manifest
                .objects
                .iter()
                .find(|object| object.uri == expected_uri)
                != Some(&source_object)
        {
            return Err(invalid(format!(
                "source manifest does not exactly match row part identity {expected_uri}"
            )));
        }
        let task = SafeExportPartTask {
            schema_version: SAFE_EXPORT_SCHEMA_VERSION,
            export_id: self.export_id.clone(),
            task_id: canonical_part_id(partition_index),
            partition_index,
            source_manifest_sha256: self.source_manifest_sha256.clone(),
            source_part_file: partition.part_file.clone(),
            source_object,
            output_part_id: canonical_part_id(partition_index),
            schema_fingerprint_sha256: self.parquet.schema_fingerprint_sha256.clone(),
        };
        task.validate_against_manifest(self, manifest)?;
        Ok(task)
    }

    pub fn part_tasks(&self, manifest: &SourceManifest) -> Result<Vec<SafeExportPartTask>> {
        self.validate()?;
        (0..self.table.partitions.len())
            .map(|index| self.task_for_partition(index, manifest))
            .collect()
    }

    /// Deterministic task plan from the source identities already sealed into
    /// this spec. Submission and workers must still validate those identities
    /// against the digest-bound manifest before any source read.
    pub fn sealed_part_tasks(&self) -> Result<Vec<SafeExportPartTask>> {
        self.validate()?;
        self.table
            .partitions
            .iter()
            .zip(&self.source_parts)
            .map(|(partition, source_object)| {
                let task = SafeExportPartTask {
                    schema_version: SAFE_EXPORT_SCHEMA_VERSION,
                    export_id: self.export_id.clone(),
                    task_id: canonical_part_id(partition.index),
                    partition_index: partition.index,
                    source_manifest_sha256: self.source_manifest_sha256.clone(),
                    source_part_file: partition.part_file.clone(),
                    source_object: source_object.clone(),
                    output_part_id: canonical_part_id(partition.index),
                    schema_fingerprint_sha256: self.parquet.schema_fingerprint_sha256.clone(),
                };
                task.validate_against(self)?;
                Ok(task)
            })
            .collect()
    }

    /// Exact receipt URI for a partition, derived without listing storage.
    pub fn part_receipt_uri(&self, partition_index: usize) -> Result<String> {
        self.validate()?;
        if partition_index >= self.table.partitions.len() {
            return Err(invalid(format!(
                "safe-export receipt partition {partition_index} is out of bounds"
            )));
        }
        safe_export_part_receipt_uri(&self.output_root_uri, &self.export_id, partition_index)
    }

    pub fn release_uri(&self) -> Result<String> {
        self.validate()?;
        safe_export_release_uri(&self.output_root_uri, &self.export_id)
    }

    pub fn success_uri(&self) -> Result<String> {
        self.validate()?;
        safe_export_success_uri(&self.output_root_uri, &self.export_id)
    }

    fn validate_intent(&self) -> Result<()> {
        require_version("safe-export spec", self.schema_version)?;
        if self.receipt_schema_version != SAFE_EXPORT_RECEIPT_SCHEMA_VERSION
            || self.completion_schema_version != SAFE_EXPORT_COMPLETION_SCHEMA_VERSION
        {
            return Err(invalid(
                "unsupported safe-export receipt/completion schema version",
            ));
        }
        validate_canonical_gcs_uri("canonical table URI", &self.canonical_table_uri, true)?;
        validate_canonical_gcs_uri("source manifest URI", &self.source_manifest_uri, false)?;
        validate_canonical_gcs_uri("output root URI", &self.output_root_uri, false)?;
        validate_sha256("source manifest SHA-256", &self.source_manifest_sha256)?;
        validate_canonical_timestamp("source snapshot timestamp", &self.source_snapshot_timestamp)?;
        validate_worker(&self.expected_worker)?;
        validate_tool(&self.tool)?;
        if self.expected_worker.git_commit != self.tool.git_commit
            || self.expected_worker.build_identity != self.tool.build_provenance
            || self.expected_worker.binary_sha256 != self.tool.binary_sha256
        {
            return Err(invalid(
                "expected worker identity must exactly match the release tool identity",
            ));
        }
        self.globals.validate_digest()?;
        validate_parquet(&self.parquet, &self.table)?;
        validate_table(&self.table)?;
        if self.source_parts.len() != self.table.partitions.len() {
            return Err(invalid(
                "safe-export source identity count does not match table partitions",
            ));
        }
        let mut source_uris = HashSet::with_capacity(self.source_parts.len());
        for (index, (partition, source)) in self
            .table
            .partitions
            .iter()
            .zip(&self.source_parts)
            .enumerate()
        {
            validate_source_object("safe-export source part", source, true)?;
            let expected_uri = row_part_uri(&self.canonical_table_uri, &partition.part_file);
            if source.uri != expected_uri || !source_uris.insert(source.uri.as_str()) {
                return Err(invalid(format!(
                    "source identity {index} is duplicate or does not match {expected_uri}"
                )));
            }
        }
        Ok(())
    }
}

impl SafeExportPartTask {
    pub fn from_json(data: &[u8], spec: &SafeExportSpec) -> Result<Self> {
        let value: Self = serde_json::from_slice(data)
            .map_err(|error| invalid(format!("invalid safe-export task JSON: {error}")))?;
        value.validate_against(spec)?;
        Ok(value)
    }

    pub fn validate_against_manifest(
        &self,
        spec: &SafeExportSpec,
        manifest: &SourceManifest,
    ) -> Result<()> {
        self.validate_against(spec)?;
        let digest = manifest.digest_sha256()?;
        if digest != spec.source_manifest_sha256
            || manifest
                .objects
                .iter()
                .find(|object| object.uri == self.source_object.uri)
                != Some(&self.source_object)
        {
            return Err(invalid(
                "safe-export task source identity is absent from its digest-bound manifest",
            ));
        }
        Ok(())
    }

    pub fn validate_against(&self, spec: &SafeExportSpec) -> Result<()> {
        spec.validate()?;
        require_version("safe-export task", self.schema_version)?;
        let partition = spec
            .table
            .partitions
            .get(self.partition_index)
            .ok_or_else(|| {
                invalid(format!(
                    "task partition {} is out of bounds",
                    self.partition_index
                ))
            })?;
        let expected_part = canonical_part_id(self.partition_index);
        if self.export_id != spec.export_id
            || self.task_id != expected_part
            || self.output_part_id != expected_part
            || self.source_manifest_sha256 != spec.source_manifest_sha256
            || self.source_part_file != partition.part_file
            || self.schema_fingerprint_sha256 != spec.parquet.schema_fingerprint_sha256
        {
            return Err(invalid(
                "safe-export task does not match its immutable spec",
            ));
        }
        validate_source_object("task source object", &self.source_object, true)?;
        let expected_uri = row_part_uri(&spec.canonical_table_uri, &partition.part_file);
        if self.source_object.uri != expected_uri {
            return Err(invalid(format!(
                "task source object URI mismatch: expected {expected_uri}, got {}",
                self.source_object.uri
            )));
        }
        Ok(())
    }
}

impl SafeExportPartReceipt {
    /// Construct the only valid receipt for an accepted report and its
    /// conditioned, content-addressed publication identity.
    pub fn from_publication(
        spec: &SafeExportSpec,
        task: &SafeExportPartTask,
        report: &ExportPartReport,
        output_object: SourceObject,
    ) -> Result<Self> {
        let part = part_from_publication(spec, task, report, &output_object)?;
        let receipt = Self {
            schema_version: SAFE_EXPORT_RECEIPT_SCHEMA_VERSION,
            export_id: spec.export_id.clone(),
            source_manifest_sha256: spec.source_manifest_sha256.clone(),
            task_id: task.task_id.clone(),
            partition_index: task.partition_index,
            part,
        };
        receipt.validate_against_publication(spec, task, report, &output_object)?;
        Ok(receipt)
    }

    /// Strictly decode receipt bytes and validate them without requiring the
    /// original attempt report. This is the registration/finalization import
    /// path; the published object identity must come from a conditioned read.
    pub fn from_json(
        data: &[u8],
        spec: &SafeExportSpec,
        task: &SafeExportPartTask,
        output_object: &SourceObject,
    ) -> Result<Self> {
        let receipt: Self = serde_json::from_slice(data)
            .map_err(|error| invalid(format!("invalid safe-export receipt JSON: {error}")))?;
        receipt.validate_against(spec, task, output_object)?;
        Ok(receipt)
    }

    /// Validate the export envelope, one-partition coverage, release fields,
    /// and exact immutable output identity.
    pub fn validate_against(
        &self,
        spec: &SafeExportSpec,
        task: &SafeExportPartTask,
        output_object: &SourceObject,
    ) -> Result<()> {
        spec.validate()?;
        task.validate_against(spec)?;
        if self.schema_version != SAFE_EXPORT_RECEIPT_SCHEMA_VERSION
            || self.schema_version != spec.receipt_schema_version
        {
            return Err(invalid(format!(
                "unsupported safe-export part receipt schema version {}",
                self.schema_version
            )));
        }
        if self.export_id != spec.export_id
            || self.export_id != task.export_id
            || self.source_manifest_sha256 != spec.source_manifest_sha256
            || self.source_manifest_sha256 != task.source_manifest_sha256
            || self.task_id != task.task_id
            || self.partition_index != task.partition_index
        {
            return Err(invalid(
                "safe-export part receipt envelope does not match its immutable task",
            ));
        }

        validate_canonical_part_receipt(&self.part, spec, task, output_object)
    }

    /// Additionally prove that every nested receipt field is the canonical
    /// projection of the exact accepted worker report.
    pub fn validate_against_publication(
        &self,
        spec: &SafeExportSpec,
        task: &SafeExportPartTask,
        report: &ExportPartReport,
        output_object: &SourceObject,
    ) -> Result<()> {
        self.validate_against(spec, task, output_object)?;
        let expected = part_from_publication(spec, task, report, output_object)?;
        if self.part != expected {
            return Err(invalid(
                "safe-export part receipt does not exactly match its accepted report",
            ));
        }
        Ok(())
    }

    /// Canonical compact JSON for immutable receipt publication.
    pub fn canonical_bytes(
        &self,
        spec: &SafeExportSpec,
        task: &SafeExportPartTask,
        output_object: &SourceObject,
    ) -> Result<Vec<u8>> {
        self.validate_against(spec, task, output_object)?;
        serde_json::to_vec(self)
            .map_err(|error| invalid(format!("failed to encode safe-export receipt: {error}")))
    }
}

impl SafeExportSuccess {
    pub fn validate_against(&self, spec: &SafeExportSpec) -> Result<()> {
        spec.validate()?;
        if self.schema_version != SAFE_EXPORT_COMPLETION_SCHEMA_VERSION
            || self.schema_version != spec.completion_schema_version
            || self.export_id != spec.export_id
        {
            return Err(invalid("safe-export success identity/version mismatch"));
        }
        validate_sha256("success receipt-set SHA-256", &self.receipt_set_sha256)?;
        validate_source_object("release object", &self.release_object, false)?;
        if self.release_object.uri != spec.release_uri()?
            || self.release_object.sha256.as_deref().is_none()
        {
            return Err(invalid("safe-export success release object mismatch"));
        }
        if self.receipts.len() != spec.table.partitions.len() {
            return Err(invalid("safe-export success receipt count mismatch"));
        }
        for (expected, identity) in self.receipts.iter().enumerate() {
            validate_source_object("success receipt object", &identity.receipt_object, false)?;
            if identity.partition_index != expected
                || identity.receipt_object.uri != spec.part_receipt_uri(expected)?
                || identity.receipt_object.sha256.as_deref().is_none()
            {
                return Err(invalid(
                    "safe-export success receipts are not complete and canonically ordered",
                ));
            }
        }
        Ok(())
    }

    pub fn canonical_bytes(&self, spec: &SafeExportSpec) -> Result<Vec<u8>> {
        self.validate_against(spec)?;
        serde_json::to_vec(self)
            .map_err(|error| invalid(format!("failed to encode safe-export success: {error}")))
    }

    pub fn from_json(data: &[u8], spec: &SafeExportSpec) -> Result<Self> {
        let value: Self = serde_json::from_slice(data)
            .map_err(|error| invalid(format!("invalid safe-export success JSON: {error}")))?;
        value.validate_against(spec)?;
        Ok(value)
    }
}

impl ExportPartReport {
    pub fn from_json(data: &[u8]) -> Result<Self> {
        let value: Self = serde_json::from_slice(data)
            .map_err(|error| invalid(format!("invalid safe-export report JSON: {error}")))?;
        value.validate_basic()?;
        Ok(value)
    }

    pub fn validate_against(
        &self,
        spec: &SafeExportSpec,
        task: &SafeExportPartTask,
        assigned_worker_id: &str,
        assignment_attempt: u64,
        lease_identity_sha256: &str,
    ) -> Result<()> {
        spec.validate()?;
        task.validate_against(spec)?;
        self.validate_basic()?;
        validate_sha256("assigned lease identity", lease_identity_sha256)?;

        if self.export_id != task.export_id
            || self.task_id != task.task_id
            || self.partition_index != task.partition_index
            || self.source_manifest_sha256 != task.source_manifest_sha256
            || self.source_part_file != task.source_part_file
            || self.source_object != task.source_object
            || self.schema_fingerprint_sha256 != task.schema_fingerprint_sha256
        {
            return Err(invalid(
                "safe-export report does not match its assigned task",
            ));
        }
        if self.worker_id != assigned_worker_id
            || self.worker_git_commit != spec.expected_worker.git_commit
            || self.worker_binary_sha256 != spec.expected_worker.binary_sha256
        {
            return Err(invalid("safe-export report worker identity mismatch"));
        }
        if self.row_count > 0
            && !spec.table.key.fields.is_empty()
            && (self.first_key.is_none() || self.last_key.is_none())
        {
            return Err(invalid(
                "non-empty keyed export report requires first and last keys",
            ));
        }
        if self.assignment_attempt != assignment_attempt
            || self.lease_identity_sha256 != lease_identity_sha256
        {
            return Err(invalid("safe-export report assignment identity mismatch"));
        }
        validate_staging_uri(
            &self.staging_object.uri,
            &spec.output_root_uri,
            &self.export_id,
            &self.task_id,
            self.assignment_attempt,
            &self.lease_identity_sha256,
        )?;
        Ok(())
    }

    fn validate_basic(&self) -> Result<()> {
        require_version("safe-export report", self.schema_version)?;
        validate_sha256("report export ID", &self.export_id)?;
        validate_sha256("report manifest SHA-256", &self.source_manifest_sha256)?;
        validate_sha256("report schema fingerprint", &self.schema_fingerprint_sha256)?;
        validate_sha256("decoded rows SHA-256", &self.decoded_rows_sha256)?;
        validate_sha256("lease identity SHA-256", &self.lease_identity_sha256)?;
        validate_source_object("report source object", &self.source_object, true)?;
        validate_source_object("report staging object", &self.staging_object, false)?;
        if self.staging_object.sha256.is_none() {
            return Err(invalid("report staging object requires a SHA-256"));
        }
        if self.byte_size == 0 || self.byte_size != self.staging_object.size {
            return Err(invalid("report byte size does not match staging object"));
        }
        if self.row_count == 0 {
            if self.row_group_count != 0 || self.first_key.is_some() || self.last_key.is_some() {
                return Err(invalid("empty report has row groups or boundary keys"));
            }
        } else if self.row_group_count == 0 || self.first_key.is_some() != self.last_key.is_some() {
            return Err(invalid(
                "non-empty report has inconsistent row groups or boundary keys",
            ));
        }
        if self.worker_id.is_empty() || self.worker_git_commit.is_empty() {
            return Err(invalid("report worker identity is incomplete"));
        }
        validate_sha256("report worker binary SHA-256", &self.worker_binary_sha256)?;
        validate_canonical_timestamp("report start timestamp", &self.started_at)?;
        validate_canonical_timestamp("report completion timestamp", &self.completed_at)?;
        if self.completed_at < self.started_at {
            return Err(invalid(
                "report completion timestamp precedes start timestamp",
            ));
        }
        Ok(())
    }
}

fn part_from_publication(
    spec: &SafeExportSpec,
    task: &SafeExportPartTask,
    report: &ExportPartReport,
    output_object: &SourceObject,
) -> Result<PartReceipt> {
    report.validate_against(
        spec,
        task,
        &report.worker_id,
        report.assignment_attempt,
        &report.lease_identity_sha256,
    )?;
    validate_published_output(spec, report, output_object)?;
    Ok(PartReceipt {
        part_id: task.output_part_id.clone(),
        output_uri: output_object.uri.clone(),
        source_partition_indices: vec![task.partition_index],
        output_object: output_object.clone(),
        byte_size: report.byte_size,
        sha256: output_object
            .sha256
            .clone()
            .expect("published output SHA-256 validated above"),
        row_count: report.row_count,
        row_group_count: report.row_group_count,
        schema_fingerprint_sha256: report.schema_fingerprint_sha256.clone(),
        decoded_rows_sha256: report.decoded_rows_sha256.clone(),
        first_key: report.first_key.clone(),
        last_key: report.last_key.clone(),
        worker_id: report.worker_id.clone(),
        worker_git_commit: report.worker_git_commit.clone(),
        worker_binary_sha256: report.worker_binary_sha256.clone(),
        assignment_attempt: report.assignment_attempt,
        lease_identity_sha256: report.lease_identity_sha256.clone(),
        started_at: report.started_at.clone(),
        completed_at: report.completed_at.clone(),
        status: PartStatus::Complete,
        error: None,
    })
}

fn validate_canonical_part_receipt(
    part: &PartReceipt,
    spec: &SafeExportSpec,
    task: &SafeExportPartTask,
    output_object: &SourceObject,
) -> Result<()> {
    validate_source_object("published output object", output_object, false)?;
    validate_sha256("part SHA-256", &part.sha256)?;
    validate_sha256("part decoded rows SHA-256", &part.decoded_rows_sha256)?;
    validate_sha256("part worker binary SHA-256", &part.worker_binary_sha256)?;
    validate_sha256("part lease identity SHA-256", &part.lease_identity_sha256)?;
    validate_canonical_timestamp("part start timestamp", &part.started_at)?;
    validate_canonical_timestamp("part completion timestamp", &part.completed_at)?;

    let expected_output_uri = content_addressed_parquet_uri(&spec.output_root_uri, &part.sha256)?;
    if part.part_id != task.output_part_id
        || part.source_partition_indices.as_slice() != [task.partition_index]
        || part.status != PartStatus::Complete
        || part.error.is_some()
        || part.schema_fingerprint_sha256 != task.schema_fingerprint_sha256
        || part.schema_fingerprint_sha256 != spec.parquet.schema_fingerprint_sha256
        || part.worker_id.is_empty()
        || part.worker_git_commit != spec.expected_worker.git_commit
        || part.worker_binary_sha256 != spec.expected_worker.binary_sha256
        || part.assignment_attempt == 0
        || part.output_object != *output_object
        || part.output_uri != output_object.uri
        || part.output_uri != expected_output_uri
        || part.byte_size == 0
        || part.byte_size != output_object.size
        || output_object.sha256.as_deref() != Some(part.sha256.as_str())
        || spec.table.partitions[task.partition_index]
            .row_count
            .is_some_and(|expected| expected != part.row_count)
        || part.completed_at < part.started_at
    {
        return Err(invalid(
            "safe-export part receipt does not match its task, build, or published object",
        ));
    }
    if part.row_count == 0 {
        if part.row_group_count != 0 || part.first_key.is_some() || part.last_key.is_some() {
            return Err(invalid(
                "empty safe-export part receipt has row groups or boundary keys",
            ));
        }
    } else if part.row_group_count == 0
        || part.first_key.is_some() != part.last_key.is_some()
        || (!spec.table.key.fields.is_empty()
            && (part.first_key.is_none() || part.last_key.is_none()))
    {
        return Err(invalid(
            "non-empty safe-export part receipt has inconsistent row groups or boundary keys",
        ));
    }
    Ok(())
}

fn validate_published_output(
    spec: &SafeExportSpec,
    report: &ExportPartReport,
    output_object: &SourceObject,
) -> Result<()> {
    validate_source_object("published output object", output_object, false)?;
    let sha256 = report
        .staging_object
        .sha256
        .as_deref()
        .ok_or_else(|| invalid("safe-export report staging object lacks SHA-256"))?;
    let expected_uri = content_addressed_parquet_uri(&spec.output_root_uri, sha256)?;
    if output_object.uri != expected_uri
        || output_object.size != report.byte_size
        || output_object.sha256.as_deref() != Some(sha256)
    {
        return Err(invalid(
            "published output does not exactly match the report's content identity",
        ));
    }
    Ok(())
}

fn validate_canonical_timestamp(label: &str, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    if bytes.len() != 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'Z'
        || bytes.iter().enumerate().any(|(index, byte)| {
            !matches!(index, 4 | 7 | 10 | 13 | 16 | 19) && !byte.is_ascii_digit()
        })
    {
        return Err(invalid(format!(
            "{label} must be canonical UTC YYYY-MM-DDTHH:MM:SSZ"
        )));
    }
    let number = |range: std::ops::Range<usize>| -> u32 {
        value[range].parse().expect("digits checked above")
    };
    let month = number(5..7);
    let day = number(8..10);
    let hour = number(11..13);
    let minute = number(14..16);
    let second = number(17..19);
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(invalid(format!("{label} has an out-of-range component")));
    }
    Ok(())
}

/// Streaming, row-boundary-preserving digest for decoded Hail rows.
pub struct DecodedRowsHasher {
    hasher: Sha256,
    row_count: u64,
}

impl DecodedRowsHasher {
    pub fn new() -> Self {
        let mut hasher = Sha256::new();
        hasher.update(DECODED_ROWS_DOMAIN);
        Self {
            hasher,
            row_count: 0,
        }
    }

    pub fn update(&mut self, row: &EncodedValue) -> Result<()> {
        let bytes = canonical_bytes(row);
        let length = u64::try_from(bytes.len())
            .map_err(|_| invalid("canonical decoded row length exceeds u64"))?;
        self.hasher.update(length.to_be_bytes());
        self.hasher.update(bytes);
        self.row_count = self
            .row_count
            .checked_add(1)
            .ok_or_else(|| invalid("decoded row count overflow"))?;
        Ok(())
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn finalize(self) -> String {
        format!("{:x}", self.hasher.finalize())
    }
}

impl Default for DecodedRowsHasher {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_worker(worker: &SafeExportWorkerIdentity) -> Result<()> {
    if worker.git_commit.is_empty() || worker.build_identity.is_empty() {
        return Err(invalid("expected worker build identity is incomplete"));
    }
    validate_sha256("expected worker binary SHA-256", &worker.binary_sha256)
}

fn validate_tool(tool: &ToolIdentity) -> Result<()> {
    if tool.package_version.is_empty()
        || tool.git_commit.is_empty()
        || tool.build_provenance.is_empty()
        || tool.binary_name.is_empty()
        || tool.target_triple.is_empty()
        || tool.arrow_crate_version.is_empty()
        || tool.parquet_crate_version.is_empty()
    {
        return Err(invalid("safe-export tool identity is incomplete"));
    }
    validate_sha256("tool binary SHA-256", &tool.binary_sha256)?;
    let mut features = HashSet::with_capacity(tool.enabled_features.len());
    if tool
        .enabled_features
        .iter()
        .any(|feature| feature.is_empty() || !features.insert(feature))
    {
        return Err(invalid(
            "tool enabled features must be non-empty and unique",
        ));
    }
    if let Some(uri) = &tool.binary_uri {
        validate_canonical_gcs_uri("tool binary URI", uri, false)?;
    }
    Ok(())
}

fn validate_parquet(parquet: &ParquetDescriptor, table: &TableDescriptor) -> Result<()> {
    validate_sha256(
        "Parquet schema fingerprint",
        &parquet.schema_fingerprint_sha256,
    )?;
    let descriptor: ArrowSchemaDescriptor = serde_json::from_value(parquet.arrow_schema.clone())
        .map_err(|error| {
            invalid(format!(
                "invalid canonical Arrow schema descriptor: {error}"
            ))
        })?;
    descriptor.validate()?;
    let physical = ETypeParser::parse(&table.rows.raw_e_type)?;
    let logical = VTypeParser::parse(&table.rows.raw_v_type)?;
    let planned = PairedSchema::new(physical, logical)?
        .arrow_plan()?
        .descriptor();
    if descriptor != planned {
        return Err(invalid(
            "canonical Arrow schema descriptor does not match the Hail row schema",
        ));
    }
    let fingerprint = descriptor.fingerprint_sha256()?;
    if fingerprint != parquet.schema_fingerprint_sha256 {
        return Err(invalid(format!(
            "Parquet schema fingerprint mismatch: declared {}, computed {fingerprint}",
            parquet.schema_fingerprint_sha256
        )));
    }
    if parquet.sharding_rule != SAFE_EXPORT_SHARDING_RULE_V1
        || !parquet.projection.is_empty()
        || !parquet.filters.is_empty()
        || parquet.output_ordering != table.key.fields
    {
        return Err(invalid(
            "safe-export v1 forbids projection/filter/remapping and requires key ordering",
        ));
    }
    let writer = &parquet.writer_properties;
    if !writer.compression.eq_ignore_ascii_case("zstd")
        || writer.compression_level.is_some()
        || writer.max_row_group_size != 100_000
        || writer.converter_batch_size != 4096
        || !writer.dictionary_enabled
        || !writer.statistics_enabled
        || writer.data_page_version != "v1"
        || writer.writer_version != "v1"
    {
        return Err(invalid(
            "safe-export v1 requires the fixed qualified Parquet writer policy",
        ));
    }
    Ok(())
}

fn validate_table(table: &TableDescriptor) -> Result<()> {
    validate_component("rows", &table.rows)?;
    validate_component("globals", &table.globals)?;
    if table.partitions.is_empty() || table.rows.part_files.len() != table.partitions.len() {
        return Err(invalid(
            "safe-export table has empty or mismatched row partitions",
        ));
    }
    if !table.rows.range_bounds.is_empty()
        && table.rows.range_bounds.len() != table.partitions.len()
    {
        return Err(invalid(
            "safe-export table range bounds do not match partitions",
        ));
    }
    let mut files = HashSet::with_capacity(table.partitions.len());
    for (index, partition) in table.partitions.iter().enumerate() {
        validate_partition(index, partition)?;
        if partition.part_file != table.rows.part_files[index]
            || !files.insert(partition.part_file.as_str())
        {
            return Err(invalid(
                "safe-export source partition mapping is inconsistent",
            ));
        }
    }
    Ok(())
}

fn validate_component(label: &str, component: &ComponentDescriptor) -> Result<()> {
    if component.name.is_empty() || component.part_files.is_empty() {
        return Err(invalid(format!("{label} component is incomplete")));
    }
    let physical = ETypeParser::parse(&component.raw_e_type)?;
    let logical = VTypeParser::parse(&component.raw_v_type)?;
    PairedSchema::new(physical.clone(), logical.clone())?;
    if component.physical_type != PhysicalTypeDescriptor::from(&physical)
        || component.logical_type != logical
    {
        return Err(invalid(format!(
            "{label} component schema descriptor mismatch"
        )));
    }
    Ok(())
}

fn validate_partition(index: usize, partition: &SourcePartition) -> Result<()> {
    let output = canonical_part_id(index);
    if partition.index != index
        || !safe_component(&partition.part_file)
        || partition.output_part_ids.as_slice() != [output.as_str()]
    {
        return Err(invalid(format!(
            "partition {index} is not a one-to-one canonical safe-export mapping"
        )));
    }
    Ok(())
}

fn validate_source_object(
    label: &str,
    object: &SourceObject,
    require_gcs_crc32c: bool,
) -> Result<()> {
    SourceManifest::new(vec![object.clone()])?;
    if require_gcs_crc32c
        && object.uri.starts_with("gs://")
        && object.crc32c.as_deref().is_none_or(str::is_empty)
    {
        return Err(invalid(format!("{label} requires GCS CRC32C identity")));
    }
    Ok(())
}

fn validate_staging_uri(
    uri: &str,
    output_root_uri: &str,
    export_id: &str,
    task_id: &str,
    assignment_attempt: u64,
    lease_identity_sha256: &str,
) -> Result<()> {
    validate_sha256("staging lease identity", lease_identity_sha256)?;
    let prefix = format!(
        "{output_root_uri}/_genohype/staging/{export_id}/{task_id}/attempt-{assignment_attempt:020}/lease-{lease_identity_sha256}/upload-"
    );
    let remainder = uri.strip_prefix(&prefix).ok_or_else(|| {
        invalid("staging object URI is outside the assigned attempt-private path")
    })?;
    let upload = remainder.strip_suffix("/part.parquet").ok_or_else(|| {
        invalid("staging object URI does not name the canonical Parquet attempt object")
    })?;
    let parsed = Uuid::parse_str(upload)
        .map_err(|error| invalid(format!("invalid staging upload identity: {error}")))?;
    if parsed.to_string() != upload {
        return Err(invalid(
            "staging upload identity is not canonical lowercase UUID",
        ));
    }
    validate_canonical_gcs_uri("staging object URI", uri, false)
}

fn validate_canonical_gcs_uri(label: &str, value: &str, require_path: bool) -> Result<()> {
    if value.contains('\\')
        || value.to_ascii_lowercase().contains("%2f")
        || value.to_ascii_lowercase().contains("%5c")
        || value.to_ascii_lowercase().contains("%2e")
        || value.ends_with('/')
    {
        return Err(invalid(format!("{label} is not canonical")));
    }
    let parsed = Url::parse(value).map_err(|error| invalid(format!("invalid {label}: {error}")))?;
    let path = parsed.path();
    if parsed.scheme() != "gs"
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.port().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || (require_path && (path.is_empty() || path == "/"))
        || path.contains("//")
        || path.split('/').any(|part| matches!(part, "." | ".."))
    {
        return Err(invalid(format!("{label} must be a canonical gs:// URI")));
    }
    let canonical = if path == "/" || path.is_empty() {
        format!("gs://{}", parsed.host_str().unwrap())
    } else {
        format!("gs://{}{}", parsed.host_str().unwrap(), path)
    };
    if canonical != value {
        return Err(invalid(format!("{label} is not in canonical form")));
    }
    Ok(())
}

/// Derive the exact receipt URI without listing storage.
pub fn safe_export_part_receipt_uri(
    output_root_uri: &str,
    export_id: &str,
    partition_index: usize,
) -> Result<String> {
    validate_canonical_gcs_uri("output root URI", output_root_uri, false)?;
    validate_sha256("receipt export ID", export_id)?;
    Ok(format!(
        "{output_root_uri}/_genohype/exports/{export_id}/receipts/part-{partition_index:020}.json"
    ))
}

pub fn safe_export_release_uri(output_root_uri: &str, export_id: &str) -> Result<String> {
    validate_canonical_gcs_uri("output root URI", output_root_uri, false)?;
    validate_sha256("release export ID", export_id)?;
    Ok(format!(
        "{output_root_uri}/_genohype/exports/{export_id}/release.json"
    ))
}

pub fn safe_export_success_uri(output_root_uri: &str, export_id: &str) -> Result<String> {
    validate_canonical_gcs_uri("output root URI", output_root_uri, false)?;
    validate_sha256("success export ID", export_id)?;
    Ok(format!(
        "{output_root_uri}/_genohype/exports/{export_id}/_SUCCESS"
    ))
}

fn content_addressed_parquet_uri(output_root_uri: &str, sha256: &str) -> Result<String> {
    validate_canonical_gcs_uri("output root URI", output_root_uri, false)?;
    validate_sha256("published output SHA-256", sha256)?;
    Ok(format!(
        "{output_root_uri}/_genohype/objects/sha256/{}/{sha256}.parquet",
        &sha256[..2]
    ))
}

fn row_part_uri(table_uri: &str, part_file: &str) -> String {
    join_path(
        &join_path(&join_path(table_uri, "rows"), "parts"),
        part_file,
    )
}

fn canonical_part_id(index: usize) -> String {
    format!("part-{index:08}")
}

fn safe_component(value: &str) -> bool {
    !value.is_empty()
        && !matches!(value, "." | "..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn require_version(label: &str, version: u32) -> Result<()> {
    if version != SAFE_EXPORT_SCHEMA_VERSION {
        return Err(invalid(format!(
            "unsupported {label} schema version {version}"
        )));
    }
    Ok(())
}

fn validate_sha256(label: &str, value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(format!(
            "{label} must be 64 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

fn canonical_json(value: Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut entries: Vec<_> = object.into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json(value)))
                    .collect(),
            )
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonical_json).collect()),
        other => other,
    }
}

fn domain_sha256(domain: &[u8], bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn invalid(message: impl Into<String>) -> HailError {
    HailError::InvalidFormat(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::LosslessValue;
    use crate::metadata::BufferSpec;
    use crate::release::{KeyDescriptor, KeyOrdering, WriterPropertiesDescriptor};

    fn object(uri: &str, size: u64, sha: Option<&str>) -> SourceObject {
        SourceObject {
            uri: uri.into(),
            version: "7".into(),
            etag: "etag-7".into(),
            size,
            crc32c: Some("Nks/tw==".into()),
            sha256: sha.map(str::to_string),
        }
    }

    fn component(part_files: Vec<String>) -> ComponentDescriptor {
        let physical = ETypeParser::parse("+EBaseStruct{x:+EBinary}").unwrap();
        ComponentDescriptor {
            name: "OrderedRVDComponentSpec".into(),
            raw_e_type: "+EBaseStruct{x:+EBinary}".into(),
            physical_type: PhysicalTypeDescriptor::from(&physical),
            raw_v_type: "Struct{x:String}".into(),
            logical_type: VTypeParser::parse("Struct{x:String}").unwrap(),
            buffer_spec: BufferSpec::StreamBlock,
            part_files,
            attrs: Value::Object(Default::default()),
            range_bounds: vec![],
            index: None,
        }
    }

    fn source_manifest() -> SourceManifest {
        SourceManifest::new(vec![object(
            "gs://source/table.ht/rows/parts/part-0",
            10,
            None,
        )])
        .unwrap()
    }

    fn unsealed_spec() -> SafeExportSpec {
        let schema = PairedSchema::from_raw_vtype(
            ETypeParser::parse("+EBaseStruct{x:+EBinary}").unwrap(),
            "Struct{x:String}",
        )
        .unwrap()
        .arrow_plan()
        .unwrap();
        let fingerprint = schema.fingerprint_sha256().unwrap();
        SafeExportSpec {
            schema_version: 1,
            export_id: String::new(),
            canonical_table_uri: "gs://source/table.ht".into(),
            source_manifest_uri: "gs://authority/manifests/a.json".into(),
            source_manifest_sha256: source_manifest().digest_sha256().unwrap(),
            source_snapshot_timestamp: "2026-08-12T12:00:00Z".into(),
            output_root_uri: "gs://output/release".into(),
            source_parts: source_manifest().objects,
            parquet: ParquetDescriptor {
                arrow_schema: serde_json::to_value(schema.descriptor()).unwrap(),
                schema_fingerprint_sha256: fingerprint,
                writer_properties: WriterPropertiesDescriptor {
                    compression: "zstd".into(),
                    compression_level: None,
                    max_row_group_size: 100_000,
                    converter_batch_size: 4096,
                    dictionary_enabled: true,
                    statistics_enabled: true,
                    data_page_version: "v1".into(),
                    writer_version: "v1".into(),
                },
                sharding_rule: SAFE_EXPORT_SHARDING_RULE_V1.into(),
                projection: vec![],
                filters: vec![],
                output_ordering: vec!["x".into()],
            },
            expected_worker: SafeExportWorkerIdentity {
                git_commit: "commit-a".into(),
                build_identity: "release:gcp".into(),
                binary_sha256: "b".repeat(64),
            },
            table: TableDescriptor {
                file_version: 1,
                hail_version: "0.2".into(),
                references_rel_path: "references".into(),
                table_type: "Table".into(),
                top_metadata: serde_json::json!({"z": 1, "a": {"b": 2}}),
                key: KeyDescriptor {
                    fields: vec!["x".into()],
                    ordering: KeyOrdering::LexicographicAscending,
                    distinctly_keyed: Some(true),
                },
                rows: component(vec!["part-0".into()]),
                globals: component(vec!["part-0".into()]),
                partitions: vec![SourcePartition {
                    index: 0,
                    part_file: "part-0".into(),
                    range_bound: None,
                    row_count: Some(2),
                    index_objects: vec!["z".into(), "a".into()],
                    output_part_ids: vec!["part-00000000".into()],
                }],
            },
            globals: GlobalsPayload::from_value(&EncodedValue::Struct(Vec::new())),
            tool: ToolIdentity {
                package_version: "0.1".into(),
                git_commit: "commit-a".into(),
                dirty_build: false,
                build_provenance: "release:gcp".into(),
                binary_name: "genohype-worker".into(),
                binary_uri: Some("gs://binaries/genohype-worker".into()),
                binary_sha256: "b".repeat(64),
                target_triple: "x86_64-unknown-linux-gnu".into(),
                enabled_features: vec!["validation".into(), "gcp".into()],
                arrow_crate_version: "53.3".into(),
                parquet_crate_version: "53.3".into(),
            },
            receipt_schema_version: 1,
            completion_schema_version: 1,
        }
    }

    fn spec() -> SafeExportSpec {
        unsealed_spec().with_computed_export_id().unwrap()
    }

    fn task(spec: &SafeExportSpec) -> SafeExportPartTask {
        spec.task_for_partition(0, &source_manifest()).unwrap()
    }

    fn report(spec: &SafeExportSpec, task: &SafeExportPartTask) -> ExportPartReport {
        let lease = "c".repeat(64);
        ExportPartReport {
            schema_version: 1,
            export_id: spec.export_id.clone(),
            task_id: task.task_id.clone(),
            partition_index: 0,
            source_manifest_sha256: spec.source_manifest_sha256.clone(),
            source_part_file: "part-0".into(),
            source_object: task.source_object.clone(),
            schema_fingerprint_sha256: spec.parquet.schema_fingerprint_sha256.clone(),
            staging_object: object(
                &format!(
                    "gs://output/release/_genohype/staging/{}/part-00000000/attempt-00000000000000000002/lease-{lease}/upload-00000000-0000-4000-8000-000000000000/part.parquet",
                    spec.export_id
                ),
                100,
                Some(&"d".repeat(64)),
            ),
            byte_size: 100,
            row_count: 2,
            row_group_count: 1,
            decoded_rows_sha256: "e".repeat(64),
            first_key: Some(LosslessValue::Int32 { value: 1 }),
            last_key: Some(LosslessValue::Int32 { value: 2 }),
            worker_id: "worker-1".into(),
            worker_git_commit: spec.expected_worker.git_commit.clone(),
            worker_binary_sha256: spec.expected_worker.binary_sha256.clone(),
            assignment_attempt: 2,
            lease_identity_sha256: lease,
            started_at: "2026-08-12T12:00:00Z".into(),
            completed_at: "2026-08-12T12:00:01Z".into(),
        }
    }

    fn published_object(spec: &SafeExportSpec, report: &ExportPartReport) -> SourceObject {
        let sha256 = report.staging_object.sha256.as_deref().unwrap();
        object(
            &format!(
                "{}/_genohype/objects/sha256/{}/{sha256}.parquet",
                spec.output_root_uri,
                &sha256[..2]
            ),
            report.byte_size,
            Some(sha256),
        )
    }

    #[test]
    fn export_identity_is_stable_canonical_and_excludes_manifest_location() {
        let first = spec();
        first.validate().unwrap();
        let encoded = serde_json::to_vec(&first).unwrap();
        assert_eq!(
            SafeExportSpec::from_json(&encoded).unwrap().export_id,
            first.export_id
        );

        let mut equivalent = first.clone();
        equivalent.source_manifest_uri = "gs://other/location/manifest.json".into();
        equivalent.table.partitions[0].index_objects.reverse();
        equivalent.tool.enabled_features.reverse();
        assert_eq!(equivalent.computed_export_id().unwrap(), first.export_id);

        let mut changed = first.clone();
        changed.canonical_table_uri = "gs://source/other.ht".into();
        changed.source_parts[0].uri = "gs://source/other.ht/rows/parts/part-0".into();
        assert_ne!(changed.computed_export_id().unwrap(), first.export_id);
        let mut changed = first.clone();
        changed.output_root_uri = "gs://output/other".into();
        assert_ne!(changed.computed_export_id().unwrap(), first.export_id);
        let mut changed = first.clone();
        changed.parquet.writer_properties.max_row_group_size += 1;
        assert!(changed.computed_export_id().is_err());
        let mut changed = first.clone();
        changed.expected_worker.binary_sha256 = "f".repeat(64);
        changed.tool.binary_sha256 = "f".repeat(64);
        assert_ne!(changed.computed_export_id().unwrap(), first.export_id);
        let mut changed = first.clone();
        changed.source_snapshot_timestamp = "2026-08-12T12:00:01Z".into();
        assert_ne!(changed.computed_export_id().unwrap(), first.export_id);
        let mut changed = first;
        changed.source_parts[0].version = "8".into();
        assert_ne!(changed.computed_export_id().unwrap(), changed.export_id);
    }

    #[test]
    fn strict_spec_rejects_unknown_versions_fields_and_unsafe_policy() {
        let valid = spec();
        let mut value = serde_json::to_value(&valid).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("limit".into(), Value::from(1));
        assert!(serde_json::from_value::<SafeExportSpec>(value).is_err());

        let mut broken = valid.clone();
        broken.schema_version = 2;
        assert!(broken.validate().is_err());
        let mut broken = valid.clone();
        broken.output_root_uri = "/tmp/export".into();
        assert!(broken.computed_export_id().is_err());
        let mut broken = valid.clone();
        broken.parquet.filters.push("x > 0".into());
        assert!(broken.computed_export_id().is_err());
        let mut broken = valid;
        broken.table.partitions[0].output_part_ids = vec!["custom".into()];
        assert!(broken.computed_export_id().is_err());
    }

    #[test]
    fn task_and_report_validation_are_assignment_exact() {
        let spec = spec();
        let task = task(&spec);
        assert_eq!(spec.sealed_part_tasks().unwrap(), vec![task.clone()]);
        let valid = report(&spec, &task);
        valid
            .validate_against(&spec, &task, "worker-1", 2, &"c".repeat(64))
            .unwrap();

        let mut broken = valid.clone();
        broken.source_object.version = "8".into();
        assert!(broken
            .validate_against(&spec, &task, "worker-1", 2, &"c".repeat(64))
            .is_err());
        let mut broken = valid.clone();
        broken.assignment_attempt = 3;
        assert!(broken
            .validate_against(&spec, &task, "worker-1", 2, &"c".repeat(64))
            .is_err());
        let mut broken = valid.clone();
        broken.staging_object.uri = broken.staging_object.uri.replace(
            "attempt-00000000000000000002",
            "attempt-00000000000000000003",
        );
        assert!(broken
            .validate_against(&spec, &task, "worker-1", 2, &"c".repeat(64))
            .is_err());
        let mut broken = valid.clone();
        broken.byte_size += 1;
        assert!(broken
            .validate_against(&spec, &task, "worker-1", 2, &"c".repeat(64))
            .is_err());
        let mut broken = valid;
        broken.completed_at = "2026-08-12T11:59:59Z".into();
        assert!(broken
            .validate_against(&spec, &task, "worker-1", 2, &"c".repeat(64))
            .is_err());
    }

    #[test]
    fn safe_export_part_receipt_is_strict_export_bound_and_canonical() {
        let spec = spec();
        let task = task(&spec);
        let report = report(&spec, &task);
        let output = published_object(&spec, &report);
        let receipt =
            SafeExportPartReceipt::from_publication(&spec, &task, &report, output.clone()).unwrap();

        assert_eq!(receipt.schema_version, SAFE_EXPORT_RECEIPT_SCHEMA_VERSION);
        assert_eq!(receipt.export_id, spec.export_id);
        assert_eq!(receipt.source_manifest_sha256, spec.source_manifest_sha256);
        assert_eq!(receipt.task_id, task.task_id);
        assert_eq!(receipt.partition_index, 0);
        assert_eq!(receipt.part.source_partition_indices, vec![0]);
        assert_eq!(receipt.part.output_object, output);
        assert_eq!(
            spec.part_receipt_uri(0).unwrap(),
            format!(
                "{}/_genohype/exports/{}/receipts/part-00000000000000000000.json",
                spec.output_root_uri, spec.export_id
            )
        );

        let bytes = receipt.canonical_bytes(&spec, &task, &output).unwrap();
        assert_eq!(
            SafeExportPartReceipt::from_json(&bytes, &spec, &task, &output).unwrap(),
            receipt
        );
        let mut value: Value = serde_json::from_slice(&bytes).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("unknown".into(), Value::Null);
        assert!(serde_json::from_value::<SafeExportPartReceipt>(value).is_err());
    }

    #[test]
    fn safe_export_success_is_deterministic_strict_and_timestamp_free() {
        let spec = spec();
        let task = task(&spec);
        let report = report(&spec, &task);
        let output = published_object(&spec, &report);
        let receipt =
            SafeExportPartReceipt::from_publication(&spec, &task, &report, output).unwrap();
        let receipt_bytes = serde_json::to_vec(&receipt).unwrap();
        let receipt_sha = format!("{:x}", Sha256::digest(&receipt_bytes));
        let release_sha = "9".repeat(64);
        let success = SafeExportSuccess {
            schema_version: SAFE_EXPORT_COMPLETION_SCHEMA_VERSION,
            export_id: spec.export_id.clone(),
            release_object: object(&spec.release_uri().unwrap(), 123, Some(&release_sha)),
            receipts: vec![SafeExportReceiptIdentity {
                partition_index: 0,
                receipt_object: object(
                    &spec.part_receipt_uri(0).unwrap(),
                    receipt_bytes.len() as u64,
                    Some(&receipt_sha),
                ),
            }],
            receipt_set_sha256: "8".repeat(64),
        };
        let bytes = success.canonical_bytes(&spec).unwrap();
        assert_eq!(
            SafeExportSuccess::from_json(&bytes, &spec).unwrap(),
            success
        );
        assert!(!String::from_utf8(bytes).unwrap().contains("timestamp"));
        assert_eq!(
            spec.success_uri().unwrap(),
            format!(
                "{}/_genohype/exports/{}/_SUCCESS",
                spec.output_root_uri, spec.export_id
            )
        );

        let mut broken = success;
        broken.receipts[0].partition_index = 1;
        assert!(broken.validate_against(&spec).is_err());
    }

    #[test]
    fn safe_export_part_receipt_rejects_wrong_envelope_build_report_and_output() {
        let spec = spec();
        let task = task(&spec);
        let report = report(&spec, &task);
        let output = published_object(&spec, &report);
        let valid =
            SafeExportPartReceipt::from_publication(&spec, &task, &report, output.clone()).unwrap();

        let mut broken = valid.clone();
        broken.schema_version += 1;
        assert!(broken.validate_against(&spec, &task, &output).is_err());
        let mut broken = valid.clone();
        broken.export_id = "f".repeat(64);
        assert!(broken.validate_against(&spec, &task, &output).is_err());
        let mut broken = valid.clone();
        broken.source_manifest_sha256 = "f".repeat(64);
        assert!(broken.validate_against(&spec, &task, &output).is_err());
        let mut broken = valid.clone();
        broken.task_id = "part-00000001".into();
        assert!(broken.validate_against(&spec, &task, &output).is_err());
        let mut broken = valid.clone();
        broken.partition_index = 1;
        assert!(broken.validate_against(&spec, &task, &output).is_err());
        let mut broken = valid.clone();
        broken.part.worker_git_commit = "other-build".into();
        assert!(broken.validate_against(&spec, &task, &output).is_err());
        let mut broken = valid.clone();
        broken.part.output_uri = "gs://output/wrong.parquet".into();
        assert!(broken.validate_against(&spec, &task, &output).is_err());

        let mut changed_report = report.clone();
        changed_report.row_count += 1;
        assert!(valid
            .validate_against_publication(&spec, &task, &changed_report, &output)
            .is_err());
        let mut changed_output = output.clone();
        changed_output.version = "8".into();
        assert!(valid
            .validate_against(&spec, &task, &changed_output)
            .is_err());
    }

    #[test]
    fn report_wire_rejects_raw_lease_field() {
        let spec = spec();
        let task = task(&spec);
        let mut value = serde_json::to_value(report(&spec, &task)).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("lease_token".into(), Value::String("raw-secret".into()));
        assert!(serde_json::from_value::<ExportPartReport>(value).is_err());
    }

    #[test]
    fn decoded_rows_digest_preserves_row_boundaries() {
        let mut left = DecodedRowsHasher::new();
        left.update(&EncodedValue::Binary(vec![1, 2])).unwrap();
        left.update(&EncodedValue::Binary(vec![3])).unwrap();
        let mut right = DecodedRowsHasher::new();
        right.update(&EncodedValue::Binary(vec![1])).unwrap();
        right.update(&EncodedValue::Binary(vec![2, 3])).unwrap();
        assert_eq!(left.row_count(), 2);
        assert_ne!(left.finalize(), right.finalize());
    }
}
