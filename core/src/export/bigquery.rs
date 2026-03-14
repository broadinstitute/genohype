//! BigQuery export functionality
//!
//! This module provides functionality to export Hail tables to Google BigQuery
//! using Parquet as an intermediate format staged in Google Cloud Storage.

use crate::codec::EncodedType;
use google_cloud_bigquery::client::{Client as BqClient, ClientConfig as BqClientConfig};
use google_cloud_bigquery::http::job::get::GetJobRequest;
use google_cloud_bigquery::http::job::{
    Job, JobConfiguration, JobConfigurationLoad, JobReference, JobState, JobType, WriteDisposition,
};
use google_cloud_bigquery::http::table::{
    SourceFormat, TableFieldMode, TableFieldSchema, TableFieldType, TableReference, TableSchema,
};
use google_cloud_storage::client::{Client as StorageClient, ClientConfig as StorageConfig};
use google_cloud_storage::http::objects::delete::DeleteObjectRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use std::path::Path;
use std::time::Duration;
use thiserror::Error;
use tokio::time::sleep;
use uuid::Uuid;

/// Errors that can occur during BigQuery export
#[derive(Error, Debug)]
pub enum BigQueryError {
    #[error("BigQuery error: {0}")]
    BigQuery(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid schema: {0}")]
    InvalidSchema(String),

    #[error("Job failed: {0}")]
    JobFailed(String),

    #[error("Authentication error: {0}")]
    Auth(String),
}

pub type Result<T> = std::result::Result<T, BigQueryError>;

/// Client for interacting with BigQuery and GCS
pub struct BigQueryClient {
    project_id: String,
    bq_client: BqClient,
    storage_client: StorageClient,
    staging_bucket: String,
}

impl BigQueryClient {
    /// Create a new BigQuery client
    ///
    /// This will use Application Default Credentials.
    pub async fn new(project_id: &str, bucket: &str) -> Result<Self> {
        // Create BQ client with auth
        let (bq_config, _project) = BqClientConfig::new_with_auth()
            .await
            .map_err(|e| BigQueryError::Auth(e.to_string()))?;
        let bq_client = BqClient::new(bq_config)
            .await
            .map_err(|e| BigQueryError::BigQuery(e.to_string()))?;

        // Create GCS client with auth
        let storage_config = StorageConfig::default()
            .with_auth()
            .await
            .map_err(|e| BigQueryError::Storage(e.to_string()))?;
        let storage_client = StorageClient::new(storage_config);

        Ok(Self {
            project_id: project_id.to_string(),
            bq_client,
            storage_client,
            staging_bucket: bucket.to_string(),
        })
    }

    /// Upload a local Parquet file to the staging GCS bucket
    ///
    /// Returns the gs:// URI of the uploaded object.
    pub async fn upload_parquet(&self, local_path: &Path) -> Result<String> {
        let file_name = local_path
            .file_name()
            .ok_or_else(|| {
                BigQueryError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid file path",
                ))
            })?
            .to_string_lossy();

        // Generate a unique object name to avoid collisions
        let object_name = format!("staging/{}_{}", Uuid::new_v4(), file_name);

        let file_content = tokio::fs::read(local_path).await?;

        let upload_type = UploadType::Simple(Media::new(object_name.clone()));

        self.storage_client
            .upload_object(
                &UploadObjectRequest {
                    bucket: self.staging_bucket.clone(),
                    ..Default::default()
                },
                file_content,
                &upload_type,
            )
            .await
            .map_err(|e| BigQueryError::Storage(e.to_string()))?;

        Ok(format!("gs://{}/{}", self.staging_bucket, object_name))
    }

    /// Delete an object from GCS (cleanup)
    pub async fn delete_object(&self, gcs_uri: &str) -> Result<()> {
        if !gcs_uri.starts_with("gs://") {
            return Ok(());
        }

        let parts: Vec<&str> = gcs_uri
            .trim_start_matches("gs://")
            .splitn(2, '/')
            .collect();
        if parts.len() != 2 {
            return Ok(());
        }

        let bucket = parts[0];
        let object = parts[1];

        self.storage_client
            .delete_object(&DeleteObjectRequest {
                bucket: bucket.to_string(),
                object: object.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|e| BigQueryError::Storage(e.to_string()))?;

        Ok(())
    }

    /// Load a Parquet file from GCS into a BigQuery table
    pub async fn load_parquet(
        &self,
        dataset_id: &str,
        table_id: &str,
        gcs_uri: &str,
        row_type: &EncodedType,
    ) -> Result<()> {
        // Generate schema
        let schema = generate_bq_schema(row_type)?;

        // Configure Load Job
        let job_config = JobConfigurationLoad {
            source_uris: vec![gcs_uri.to_string()],
            source_format: Some(SourceFormat::Parquet),
            destination_table: TableReference {
                project_id: self.project_id.clone(),
                dataset_id: dataset_id.to_string(),
                table_id: table_id.to_string(),
            },
            schema: Some(schema),
            write_disposition: Some(WriteDisposition::WriteAppend),
            ..Default::default()
        };

        let job = Job {
            job_reference: JobReference {
                project_id: self.project_id.clone(),
                job_id: format!("hail_export_{}", Uuid::new_v4()),
                location: None,
            },
            configuration: JobConfiguration {
                job: JobType::Load(job_config),
                ..Default::default()
            },
            ..Default::default()
        };

        // Submit Job
        let created_job = self
            .bq_client
            .job()
            .create(&job)
            .await
            .map_err(|e| BigQueryError::BigQuery(e.to_string()))?;

        // Poll for completion
        let job_ref = &created_job.job_reference;
        let job_id = &job_ref.job_id;

        loop {
            let job_status = self
                .bq_client
                .job()
                .get(
                    &self.project_id,
                    job_id,
                    &GetJobRequest {
                        location: job_ref.location.clone(),
                    },
                )
                .await
                .map_err(|e| BigQueryError::BigQuery(e.to_string()))?;

            match job_status.status.state {
                JobState::Done => {
                    if let Some(error) = job_status.status.error_result {
                        return Err(BigQueryError::JobFailed(format!(
                            "Job failed: {}",
                            error.message.unwrap_or_default()
                        )));
                    }
                    // Check for errors array even if error_result is None
                    if let Some(errors) = job_status.status.errors {
                        if !errors.is_empty() {
                            return Err(BigQueryError::JobFailed(format!(
                                "Job finished with errors: {}",
                                errors[0].message.clone().unwrap_or_default()
                            )));
                        }
                    }
                    break;
                }
                JobState::Pending | JobState::Running => {
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }

        Ok(())
    }
}

/// Generate BigQuery TableSchema from Hail EncodedType
pub fn generate_bq_schema(etype: &EncodedType) -> Result<TableSchema> {
    match etype {
        EncodedType::EBaseStruct { fields, .. } => {
            let bq_fields = fields
                .iter()
                .map(|f| hail_to_bq_field(&f.name, &f.encoded_type))
                .collect();
            Ok(TableSchema { fields: bq_fields })
        }
        _ => Err(BigQueryError::InvalidSchema(
            "Root type must be a struct".to_string(),
        )),
    }
}

/// Recursive helper to map Hail types to BigQuery fields
fn hail_to_bq_field(name: &str, etype: &EncodedType) -> TableFieldSchema {
    // Determine mode (NULLABLE vs REQUIRED vs REPEATED)
    // BigQuery ARRAYs use REPEATED mode
    let mode = if matches!(etype, EncodedType::EArray { .. }) {
        Some(TableFieldMode::Repeated)
    } else if etype.is_required() {
        Some(TableFieldMode::Required)
    } else {
        Some(TableFieldMode::Nullable)
    };

    match etype {
        EncodedType::EInt32 { .. } | EncodedType::EInt64 { .. } => TableFieldSchema {
            name: name.to_string(),
            data_type: TableFieldType::Int64,
            mode,
            ..Default::default()
        },
        EncodedType::EFloat32 { .. } | EncodedType::EFloat64 { .. } => TableFieldSchema {
            name: name.to_string(),
            data_type: TableFieldType::Float,
            mode,
            ..Default::default()
        },
        EncodedType::EBoolean { .. } => TableFieldSchema {
            name: name.to_string(),
            data_type: TableFieldType::Boolean,
            mode,
            ..Default::default()
        },
        EncodedType::EBinary { .. } => {
            // Default to STRING, could be BYTES if needed but Hail EBinary usually UTF-8 strings
            TableFieldSchema {
                name: name.to_string(),
                data_type: TableFieldType::String,
                mode,
                ..Default::default()
            }
        }
        EncodedType::EArray { element, .. } => {
            // For arrays, the field is REPEATED, and the type comes from the element
            // Note: BQ Arrays cannot have NULL elements. Hail Arrays CAN.
            let inner = hail_to_bq_field(name, element);
            TableFieldSchema {
                name: name.to_string(),
                data_type: inner.data_type,
                mode: Some(TableFieldMode::Repeated),
                fields: inner.fields,
                ..Default::default()
            }
        }
        EncodedType::EBaseStruct { fields, .. } => {
            let sub_fields: Vec<TableFieldSchema> = fields
                .iter()
                .map(|f| hail_to_bq_field(&f.name, &f.encoded_type))
                .collect();
            TableFieldSchema {
                name: name.to_string(),
                data_type: TableFieldType::Record,
                mode,
                fields: Some(sub_fields),
                ..Default::default()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::EncodedField;

    #[test]
    fn test_primitive_type_mapping() {
        let field = hail_to_bq_field("test", &EncodedType::EInt32 { required: true });
        assert_eq!(field.data_type, TableFieldType::Int64);
        assert_eq!(field.mode, Some(TableFieldMode::Required));

        let field = hail_to_bq_field("test", &EncodedType::EInt64 { required: false });
        assert_eq!(field.data_type, TableFieldType::Int64);
        assert_eq!(field.mode, Some(TableFieldMode::Nullable));

        let field = hail_to_bq_field("test", &EncodedType::EFloat64 { required: true });
        assert_eq!(field.data_type, TableFieldType::Float);

        let field = hail_to_bq_field("test", &EncodedType::EBoolean { required: true });
        assert_eq!(field.data_type, TableFieldType::Boolean);

        let field = hail_to_bq_field("test", &EncodedType::EBinary { required: true });
        assert_eq!(field.data_type, TableFieldType::String);
    }

    #[test]
    fn test_array_type_mapping() {
        let array_type = EncodedType::EArray {
            required: true,
            element: Box::new(EncodedType::EInt32 { required: true }),
        };
        let field = hail_to_bq_field("test", &array_type);
        assert_eq!(field.data_type, TableFieldType::Int64);
        assert_eq!(field.mode, Some(TableFieldMode::Repeated));
    }

    #[test]
    fn test_struct_type_mapping() {
        let struct_type = EncodedType::EBaseStruct {
            required: true,
            fields: vec![
                EncodedField {
                    name: "x".to_string(),
                    encoded_type: EncodedType::EInt32 { required: true },
                    index: 0,
                },
                EncodedField {
                    name: "y".to_string(),
                    encoded_type: EncodedType::EBinary { required: false },
                    index: 1,
                },
            ],
        };
        let field = hail_to_bq_field("test", &struct_type);
        assert_eq!(field.data_type, TableFieldType::Record);
        assert_eq!(field.mode, Some(TableFieldMode::Required));
        assert!(field.fields.is_some());
        let fields = field.fields.unwrap();
        assert_eq!(fields.len(), 2);
    }

    #[test]
    fn test_generate_bq_schema() {
        let schema = EncodedType::EBaseStruct {
            required: true,
            fields: vec![
                EncodedField {
                    name: "id".to_string(),
                    encoded_type: EncodedType::EInt64 { required: true },
                    index: 0,
                },
                EncodedField {
                    name: "name".to_string(),
                    encoded_type: EncodedType::EBinary { required: true },
                    index: 1,
                },
            ],
        };

        let bq_schema = generate_bq_schema(&schema).unwrap();
        assert!(bq_schema.fields.is_some());
        let fields = bq_schema.fields.unwrap();
        assert_eq!(fields.len(), 2);
    }
}
