//! Typed, versioned release-sidecar descriptors and completion validation.
//!
//! This module deliberately models publication evidence without performing
//! cloud writes. Immutable output naming and `_SUCCESS` publication remain a
//! later stage; a sidecar cannot be marked complete unless this validator can
//! prove its receipts and source manifest agree.

use crate::codec::{canonical_digest, ETypeParser, EncodedType, LosslessValue, ValueDigest};
use crate::io::{SourceManifest, SourceObject};
use crate::metadata::{BufferSpec, CodecSpec, IndexSpec, Interval, RVDComponentSpec};
use crate::schema::{LogicalType, VTypeParser};
use crate::{HailError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashSet};

pub const RELEASE_SIDECAR_SCHEMA_VERSION: u32 = 1;
const RECEIPT_DIGEST_DOMAIN: &[u8] = b"genohype.release.receipt-set.sha256.v1\0";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableReleaseSidecar {
    pub schema_version: u32,
    pub release_id: String,
    pub status: ReleaseStatus,
    pub source: ReleaseSourceSnapshot,
    pub table: TableDescriptor,
    pub globals: GlobalsPayload,
    pub tool: ToolIdentity,
    pub parquet: ParquetDescriptor,
    pub parts: Vec<PartReceipt>,
    pub totals: ReleaseTotals,
    pub receipt_set_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseStatus {
    Incomplete,
    Complete,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseSourceSnapshot {
    pub canonical_table_uri: String,
    pub snapshot_timestamp: String,
    pub manifest: SourceManifest,
    pub manifest_sha256: String,
}

impl ReleaseSourceSnapshot {
    pub fn validate(&self) -> Result<()> {
        self.manifest.validate()?;
        if self.canonical_table_uri.is_empty() || self.snapshot_timestamp.is_empty() {
            return Err(invalid(
                "source table URI and snapshot timestamp are required",
            ));
        }
        let digest = self.manifest.digest_sha256()?;
        if digest != self.manifest_sha256 {
            return Err(invalid(format!(
                "source manifest digest mismatch: declared {}, computed {digest}",
                self.manifest_sha256
            )));
        }
        for object in &self.manifest.objects {
            if object.uri.starts_with("gs://") && object.crc32c.as_deref().is_none_or(str::is_empty)
            {
                return Err(invalid(format!(
                    "GCS source object lacks CRC32C: {}",
                    object.uri
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableDescriptor {
    pub file_version: u32,
    pub hail_version: String,
    pub references_rel_path: String,
    pub table_type: String,
    pub top_metadata: Value,
    pub key: KeyDescriptor,
    pub rows: ComponentDescriptor,
    pub globals: ComponentDescriptor,
    pub partitions: Vec<SourcePartition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyDescriptor {
    pub fields: Vec<String>,
    pub ordering: KeyOrdering,
    pub distinctly_keyed: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyOrdering {
    LexicographicAscending,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentDescriptor {
    pub name: String,
    pub raw_e_type: String,
    pub physical_type: PhysicalTypeDescriptor,
    pub raw_v_type: String,
    pub logical_type: LogicalType,
    pub buffer_spec: BufferSpec,
    pub part_files: Vec<String>,
    pub attrs: Value,
    pub range_bounds: Vec<Interval>,
    pub index: Option<IndexDescriptor>,
}

impl ComponentDescriptor {
    pub fn from_spec(spec: &RVDComponentSpec) -> Result<Self> {
        let physical = ETypeParser::parse(&spec.codec_spec.e_type)?;
        let logical = VTypeParser::parse(&spec.codec_spec.v_type)?;
        crate::schema::PairedSchema::new(physical.clone(), logical.clone())?;
        Ok(Self {
            name: spec.name.clone(),
            raw_e_type: spec.codec_spec.e_type.clone(),
            physical_type: PhysicalTypeDescriptor::from(&physical),
            raw_v_type: spec.codec_spec.v_type.clone(),
            logical_type: logical,
            buffer_spec: spec.codec_spec.buffer_spec.clone(),
            part_files: spec.part_files.clone(),
            attrs: spec.attrs.clone(),
            range_bounds: spec.range_bounds.clone(),
            index: spec
                .index_spec
                .as_ref()
                .map(IndexDescriptor::from_spec)
                .transpose()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PhysicalTypeDescriptor {
    Int32 {
        required: bool,
    },
    Int64 {
        required: bool,
    },
    Float32 {
        required: bool,
    },
    Float64 {
        required: bool,
    },
    Boolean {
        required: bool,
    },
    Binary {
        required: bool,
    },
    Array {
        required: bool,
        element: Box<PhysicalTypeDescriptor>,
    },
    Struct {
        required: bool,
        fields: Vec<PhysicalFieldDescriptor>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalFieldDescriptor {
    pub name: String,
    pub index: usize,
    pub physical_type: PhysicalTypeDescriptor,
}

impl From<&EncodedType> for PhysicalTypeDescriptor {
    fn from(value: &EncodedType) -> Self {
        match value {
            EncodedType::EInt32 { required } => Self::Int32 {
                required: *required,
            },
            EncodedType::EInt64 { required } => Self::Int64 {
                required: *required,
            },
            EncodedType::EFloat32 { required } => Self::Float32 {
                required: *required,
            },
            EncodedType::EFloat64 { required } => Self::Float64 {
                required: *required,
            },
            EncodedType::EBoolean { required } => Self::Boolean {
                required: *required,
            },
            EncodedType::EBinary { required } => Self::Binary {
                required: *required,
            },
            EncodedType::EArray { required, element } => Self::Array {
                required: *required,
                element: Box::new(Self::from(element.as_ref())),
            },
            EncodedType::EBaseStruct { required, fields } => Self::Struct {
                required: *required,
                fields: fields
                    .iter()
                    .map(|field| PhysicalFieldDescriptor {
                        name: field.name.clone(),
                        index: field.index,
                        physical_type: Self::from(&field.encoded_type),
                    })
                    .collect(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexDescriptor {
    pub rel_path: String,
    pub key_type: String,
    pub annotation_type: String,
    pub leaf: IndexCodecDescriptor,
    pub internal_node: IndexCodecDescriptor,
}

impl IndexDescriptor {
    fn from_spec(spec: &IndexSpec) -> Result<Self> {
        Ok(Self {
            rel_path: spec.rel_path.clone(),
            key_type: spec.key_type.clone(),
            annotation_type: spec.annotation_type.clone(),
            leaf: IndexCodecDescriptor::from_codec(&spec.leaf_codec)?,
            internal_node: IndexCodecDescriptor::from_codec(&spec.internal_node_codec)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexCodecDescriptor {
    pub raw_e_type: String,
    pub physical_type: PhysicalTypeDescriptor,
    pub raw_v_type: String,
    pub logical_type: LogicalType,
    pub buffer_spec: BufferSpec,
}

impl IndexCodecDescriptor {
    fn from_codec(spec: &CodecSpec) -> Result<Self> {
        let physical = ETypeParser::parse(&spec.e_type)?;
        let logical = VTypeParser::parse(&spec.v_type)?;
        crate::schema::PairedSchema::new(physical.clone(), logical.clone())?;
        Ok(Self {
            raw_e_type: spec.e_type.clone(),
            physical_type: PhysicalTypeDescriptor::from(&physical),
            raw_v_type: spec.v_type.clone(),
            logical_type: logical,
            buffer_spec: spec.buffer_spec.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourcePartition {
    pub index: usize,
    pub part_file: String,
    pub range_bound: Option<Interval>,
    pub row_count: Option<u64>,
    pub index_objects: Vec<String>,
    pub output_part_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum GlobalsPayload {
    Empty {
        digest: ValueDigest,
    },
    Null {
        digest: ValueDigest,
    },
    Value {
        value: LosslessValue,
        digest: ValueDigest,
    },
    Error {
        message: String,
    },
}

impl GlobalsPayload {
    pub fn from_value(value: &crate::codec::EncodedValue) -> Self {
        let digest = canonical_digest(value);
        match value {
            crate::codec::EncodedValue::Null => Self::Null { digest },
            crate::codec::EncodedValue::Struct(fields) if fields.is_empty() => {
                Self::Empty { digest }
            }
            _ => Self::Value {
                value: LosslessValue::from(value),
                digest,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolIdentity {
    pub package_version: String,
    pub git_commit: String,
    pub dirty_build: bool,
    pub build_provenance: String,
    pub binary_name: String,
    pub binary_uri: Option<String>,
    pub binary_sha256: String,
    pub target_triple: String,
    pub enabled_features: Vec<String>,
    pub arrow_crate_version: String,
    pub parquet_crate_version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetDescriptor {
    pub arrow_schema: Value,
    pub schema_fingerprint_sha256: String,
    pub writer_properties: WriterPropertiesDescriptor,
    pub sharding_rule: String,
    pub projection: Vec<String>,
    pub filters: Vec<String>,
    pub output_ordering: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriterPropertiesDescriptor {
    pub compression: String,
    pub compression_level: Option<i32>,
    pub max_row_group_size: usize,
    pub converter_batch_size: usize,
    pub dictionary_enabled: bool,
    pub statistics_enabled: bool,
    pub data_page_version: String,
    pub writer_version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartReceipt {
    pub part_id: String,
    pub output_uri: String,
    pub source_partition_indices: Vec<usize>,
    pub output_object: SourceObject,
    pub byte_size: u64,
    pub sha256: String,
    pub row_count: u64,
    pub row_group_count: u64,
    pub schema_fingerprint_sha256: String,
    pub decoded_rows_sha256: String,
    pub first_key: Option<LosslessValue>,
    pub last_key: Option<LosslessValue>,
    pub worker_id: String,
    pub worker_git_commit: String,
    pub worker_binary_sha256: String,
    pub assignment_attempt: String,
    pub lease_id: String,
    pub started_at: String,
    pub completed_at: String,
    pub status: PartStatus,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartStatus {
    Incomplete,
    Complete,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseTotals {
    pub expected_partition_count: usize,
    pub completed_partition_count: usize,
    pub failed_partition_count: usize,
    pub expected_part_count: usize,
    pub completed_part_count: usize,
    pub failed_part_count: usize,
    pub source_row_count: Option<u64>,
    pub output_row_count: u64,
    pub output_bytes: u64,
    pub aggregate_sha256: String,
}

impl TableReleaseSidecar {
    pub fn computed_receipt_set_sha256(&self) -> Result<String> {
        let mut receipts = self.parts.clone();
        receipts.sort_by(|a, b| a.part_id.cmp(&b.part_id));
        let bytes = serde_json::to_vec(&receipts)
            .map_err(|error| invalid(format!("failed to encode receipts: {error}")))?;
        let mut hasher = Sha256::new();
        hasher.update(RECEIPT_DIGEST_DOMAIN);
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
        Ok(format!("{:x}", hasher.finalize()))
    }

    /// Validate every invariant required before a sidecar may be published as
    /// complete. This does not claim remote objects were re-HEADed; callers must
    /// construct receipts from conditioned reads and closed-object identities.
    pub fn validate_complete(&self) -> Result<()> {
        if self.schema_version != RELEASE_SIDECAR_SCHEMA_VERSION {
            return Err(invalid(format!(
                "unsupported release sidecar schema version {}",
                self.schema_version
            )));
        }
        if self.status != ReleaseStatus::Complete || self.release_id.is_empty() {
            return Err(invalid(
                "release must have a non-empty ID and complete status",
            ));
        }
        self.source.validate()?;
        if matches!(self.globals, GlobalsPayload::Error { .. }) {
            return Err(invalid("complete release cannot contain a globals error"));
        }

        let expected: BTreeSet<_> = self
            .table
            .partitions
            .iter()
            .map(|part| part.index)
            .collect();
        if expected.len() != self.table.partitions.len()
            || expected.len() != self.totals.expected_partition_count
        {
            return Err(invalid(
                "source partition descriptor count/indices are inconsistent",
            ));
        }
        let mut covered = HashSet::new();
        let mut rows = 0u64;
        let mut bytes = 0u64;
        let mut part_ids = HashSet::new();
        for receipt in &self.parts {
            if !part_ids.insert(receipt.part_id.as_str()) {
                return Err(invalid(format!(
                    "duplicate part receipt {}",
                    receipt.part_id
                )));
            }
            if receipt.status != PartStatus::Complete || receipt.error.is_some() {
                return Err(invalid(format!("part {} is not complete", receipt.part_id)));
            }
            if receipt.schema_fingerprint_sha256 != self.parquet.schema_fingerprint_sha256 {
                return Err(invalid(format!(
                    "part {} schema fingerprint mismatch",
                    receipt.part_id
                )));
            }
            if receipt.output_uri != receipt.output_object.uri
                || receipt.byte_size != receipt.output_object.size
                || receipt.sha256.len() != 64
                || receipt.output_object.sha256.as_deref() != Some(receipt.sha256.as_str())
            {
                return Err(invalid(format!(
                    "part {} output identity mismatch",
                    receipt.part_id
                )));
            }
            for partition in &receipt.source_partition_indices {
                if !expected.contains(partition) || !covered.insert(*partition) {
                    return Err(invalid(format!(
                        "source partition {partition} is absent or covered more than once"
                    )));
                }
            }
            rows = rows
                .checked_add(receipt.row_count)
                .ok_or_else(|| invalid("row count overflow"))?;
            bytes = bytes
                .checked_add(receipt.byte_size)
                .ok_or_else(|| invalid("byte count overflow"))?;
        }
        if covered.len() != expected.len() {
            return Err(invalid(
                "not every source partition is covered exactly once",
            ));
        }
        let completed_parts = self.parts.len();
        if self.totals.completed_partition_count != expected.len()
            || self.totals.failed_partition_count != 0
            || self.totals.expected_part_count != completed_parts
            || self.totals.completed_part_count != completed_parts
            || self.totals.failed_part_count != 0
            || self.totals.output_row_count != rows
            || self.totals.output_bytes != bytes
            || self
                .totals
                .source_row_count
                .is_some_and(|source| source != rows)
        {
            return Err(invalid(
                "release totals do not agree with complete receipts",
            ));
        }
        let digest = self.computed_receipt_set_sha256()?;
        if digest != self.receipt_set_sha256 || digest != self.totals.aggregate_sha256 {
            return Err(invalid("receipt-set/aggregate digest mismatch"));
        }
        Ok(())
    }
}

fn invalid(message: impl Into<String>) -> HailError {
    HailError::InvalidFormat(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{ETypeParser, EncodedValue};
    use crate::io::SourceManifest;
    use crate::metadata::BufferSpec;

    fn source_object(uri: &str, sha256: Option<&str>) -> SourceObject {
        SourceObject {
            uri: uri.into(),
            version: "1".into(),
            etag: "etag".into(),
            size: 10,
            crc32c: Some("Nks/tw==".into()),
            sha256: sha256.map(str::to_string),
        }
    }

    fn component() -> ComponentDescriptor {
        let physical = ETypeParser::parse("+EBaseStruct{x:+EBinary}").unwrap();
        ComponentDescriptor {
            name: "OrderedRVDComponentSpec".into(),
            raw_e_type: "+EBaseStruct{x:+EBinary}".into(),
            physical_type: PhysicalTypeDescriptor::from(&physical),
            raw_v_type: "Struct{x:String}".into(),
            logical_type: VTypeParser::parse("Struct{x:String}").unwrap(),
            buffer_spec: BufferSpec::StreamBlock,
            part_files: vec!["part-0".into()],
            attrs: Value::Null,
            range_bounds: vec![],
            index: None,
        }
    }

    fn sidecar() -> TableReleaseSidecar {
        let manifest =
            SourceManifest::new(vec![source_object("gs://bucket/table/metadata", None)]).unwrap();
        let manifest_sha256 = manifest.digest_sha256().unwrap();
        let sha = "a".repeat(64);
        let receipt = PartReceipt {
            part_id: "part-0".into(),
            output_uri: "gs://out/part-0.parquet".into(),
            source_partition_indices: vec![0],
            output_object: source_object("gs://out/part-0.parquet", Some(&sha)),
            byte_size: 10,
            sha256: sha,
            row_count: 2,
            row_group_count: 1,
            schema_fingerprint_sha256: "schema".into(),
            decoded_rows_sha256: "rows".into(),
            first_key: None,
            last_key: None,
            worker_id: "worker".into(),
            worker_git_commit: "commit".into(),
            worker_binary_sha256: "binary".into(),
            assignment_attempt: "attempt".into(),
            lease_id: "lease".into(),
            started_at: "start".into(),
            completed_at: "end".into(),
            status: PartStatus::Complete,
            error: None,
        };
        let mut value = TableReleaseSidecar {
            schema_version: 1,
            release_id: "release".into(),
            status: ReleaseStatus::Complete,
            source: ReleaseSourceSnapshot {
                canonical_table_uri: "gs://bucket/table".into(),
                snapshot_timestamp: "now".into(),
                manifest,
                manifest_sha256,
            },
            table: TableDescriptor {
                file_version: 1,
                hail_version: "0.2".into(),
                references_rel_path: "references".into(),
                table_type: "Table".into(),
                top_metadata: Value::Null,
                key: KeyDescriptor {
                    fields: vec!["x".into()],
                    ordering: KeyOrdering::LexicographicAscending,
                    distinctly_keyed: None,
                },
                rows: component(),
                globals: component(),
                partitions: vec![SourcePartition {
                    index: 0,
                    part_file: "part-0".into(),
                    range_bound: None,
                    row_count: Some(2),
                    index_objects: vec![],
                    output_part_ids: vec!["part-0".into()],
                }],
            },
            globals: GlobalsPayload::from_value(&EncodedValue::Struct(vec![])),
            tool: ToolIdentity {
                package_version: "0.1".into(),
                git_commit: "commit".into(),
                dirty_build: false,
                build_provenance: "cargo".into(),
                binary_name: "genohype".into(),
                binary_uri: None,
                binary_sha256: "binary".into(),
                target_triple: "target".into(),
                enabled_features: vec![],
                arrow_crate_version: "53.3".into(),
                parquet_crate_version: "53.3".into(),
            },
            parquet: ParquetDescriptor {
                arrow_schema: Value::Null,
                schema_fingerprint_sha256: "schema".into(),
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
                sharding_rule: "one partition".into(),
                projection: vec![],
                filters: vec![],
                output_ordering: vec![],
            },
            parts: vec![receipt],
            totals: ReleaseTotals {
                expected_partition_count: 1,
                completed_partition_count: 1,
                failed_partition_count: 0,
                expected_part_count: 1,
                completed_part_count: 1,
                failed_part_count: 0,
                source_row_count: Some(2),
                output_row_count: 2,
                output_bytes: 10,
                aggregate_sha256: String::new(),
            },
            receipt_set_sha256: String::new(),
        };
        let digest = value.computed_receipt_set_sha256().unwrap();
        value.receipt_set_sha256 = digest.clone();
        value.totals.aggregate_sha256 = digest;
        value
    }

    #[test]
    fn descriptor_json_is_strict_and_preserves_logical_distinctions() {
        let sidecar = sidecar();
        let json = serde_json::to_string(&sidecar).unwrap();
        let decoded: TableReleaseSidecar = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, sidecar);
        let mut value: Value = serde_json::from_str(&json).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("unknown".into(), Value::Null);
        assert!(serde_json::from_value::<TableReleaseSidecar>(value).is_err());
    }

    #[test]
    fn completion_validation_rejects_coverage_schema_rows_globals_and_digest() {
        let valid = sidecar();
        valid.validate_complete().unwrap();

        let mut broken = valid.clone();
        broken.parts[0].source_partition_indices.clear();
        assert!(broken.validate_complete().is_err());

        let mut broken = valid.clone();
        broken.parts[0].schema_fingerprint_sha256 = "other".into();
        assert!(broken.validate_complete().is_err());

        let mut broken = valid.clone();
        broken.totals.output_row_count = 3;
        assert!(broken.validate_complete().is_err());

        let mut broken = valid.clone();
        broken.globals = GlobalsPayload::Error {
            message: "no".into(),
        };
        assert!(broken.validate_complete().is_err());

        let mut broken = valid;
        broken.receipt_set_sha256 = "bad".into();
        assert!(broken.validate_complete().is_err());
    }
}
