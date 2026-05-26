//! AnnotatingDataSource: transparent VEP annotation wrapper for any DataSource.
//!
//! Wraps an inner DataSource and annotates each row with VEP consequence predictions
//! by calling fastVEP's AnnotationContext. The annotation context is lazily initialized
//! on first use via OnceCell.

use crate::codec::{EncodedType, EncodedValue};
use crate::datasource::DataSource;
use crate::projection::ProjectionTree;
use crate::query::{IntervalList, KeyRange};
use crate::vep::mapper::append_vep_to_row;
use crate::vep::row_to_variation_feature;
use crate::vep::schema::vep_field;
use crate::Result;
use fastvep_annotate::AnnotationContext;
use once_cell::sync::OnceCell;
use std::sync::Arc;

/// Options for initializing the VEP AnnotationContext.
#[derive(Clone)]
pub struct VepInitOptions {
    pub gff3: String,
    pub fasta: Option<String>,
    pub sa_dir: Option<String>,
    pub distance: u64,
    pub pick: bool,
}

/// A DataSource wrapper that adds VEP annotations to every row.
///
/// On first access, it lazily initializes an `AnnotationContext` from GFF3/FASTA files.
/// Each row is annotated via `row_to_variation_feature` → `annotate_variant` → `append_vep_to_row`.
///
/// Because `AnnotationContext` is `Send + Sync`, multiple partition threads can
/// safely share it for concurrent annotation.
pub struct AnnotatingDataSource {
    inner: Box<dyn DataSource>,
    options: VepInitOptions,
    context: Arc<OnceCell<AnnotationContext>>,
    row_type: EncodedType,
}

impl AnnotatingDataSource {
    /// Create a new AnnotatingDataSource wrapping the given inner source.
    ///
    /// The schema is computed immediately by appending the `vep` field to the inner schema.
    /// The AnnotationContext is NOT loaded until rows are actually consumed.
    pub fn new(inner: Box<dyn DataSource>, options: VepInitOptions) -> Result<Self> {
        let row_type = build_augmented_schema(inner.row_type())?;

        Ok(Self {
            inner,
            options,
            context: Arc::new(OnceCell::new()),
            row_type,
        })
    }

    /// Get or initialize the annotation context.
    fn get_context(&self) -> Result<&AnnotationContext> {
        self.context
            .get_or_try_init(|| {
                AnnotationContext::new(
                    Some(self.options.gff3.as_str()),
                    self.options.fasta.as_deref(),
                    self.options.sa_dir.as_deref(),
                    self.options.distance,
                )
                .map_err(|e| {
                    crate::HailError::InvalidFormat(format!(
                        "Failed to initialize VEP context: {}",
                        e
                    ))
                })
            })
    }

    /// Wrap an iterator to annotate each row.
    fn annotate_iter(
        &self,
        iter: Box<dyn Iterator<Item = Result<EncodedValue>> + Send>,
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        // Eagerly initialize context so errors surface before iteration starts
        let _ = self.get_context()?;
        let ctx_arc = Arc::clone(&self.context);
        let pick = self.options.pick;

        Ok(Box::new(iter.map(move |row_result| {
            let row = row_result?;
            let ctx = ctx_arc.get().expect("context already initialized");
            let mut vf = row_to_variation_feature(&row)?;
            ctx.annotate_variant(&mut vf, pick, &[]).map_err(|e| {
                crate::HailError::InvalidFormat(format!("VEP annotation error: {}", e))
            })?;
            append_vep_to_row(row, &vf)
        })))
    }
}

impl DataSource for AnnotatingDataSource {
    fn row_type(&self) -> &EncodedType {
        &self.row_type
    }

    fn globals(&self) -> Result<EncodedValue> {
        self.inner.globals()
    }

    fn key_fields(&self) -> &[String] {
        self.inner.key_fields()
    }

    fn num_partitions(&self) -> usize {
        self.inner.num_partitions()
    }

    fn scan_partition_stream(
        &self,
        partition_idx: usize,
        ranges: &[KeyRange],
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        let inner_iter = self.inner.scan_partition_stream(partition_idx, ranges)?;
        self.annotate_iter(inner_iter)
    }

    fn query_stream_with_intervals(
        &self,
        ranges: &[KeyRange],
        intervals: Option<Arc<IntervalList>>,
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        let inner_iter = self.inner.query_stream_with_intervals(ranges, intervals)?;
        self.annotate_iter(inner_iter)
    }

    fn query_stream_with_projection(
        &self,
        ranges: &[KeyRange],
        intervals: Option<Arc<IntervalList>>,
        decode_projection: Option<Arc<ProjectionTree>>,
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        // Ignore the decode projection for the inner source since we need all fields
        // for annotation. The vep field is always appended.
        let inner_iter = self
            .inner
            .query_stream_with_projection(ranges, intervals, decode_projection)?;
        self.annotate_iter(inner_iter)
    }

    fn query_stream_sorted(
        &self,
        ranges: &[KeyRange],
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        let inner_iter = self.inner.query_stream_sorted(ranges)?;
        self.annotate_iter(inner_iter)
    }

    fn lookup(&self, key: &EncodedValue) -> Result<Option<EncodedValue>> {
        match self.inner.lookup(key)? {
            Some(row) => {
                let ctx = self.get_context()?;
                let mut vf = row_to_variation_feature(&row)?;
                ctx.annotate_variant(&mut vf, self.options.pick, &[])
                    .map_err(|e| {
                        crate::HailError::InvalidFormat(format!("VEP annotation error: {}", e))
                    })?;
                Ok(Some(append_vep_to_row(row, &vf)?))
            }
            None => Ok(None),
        }
    }

    fn total_rows(&self) -> Option<usize> {
        self.inner.total_rows()
    }

    fn sample_random(&self, sample_size: usize) -> Result<Vec<EncodedValue>> {
        self.inner.sample_random(sample_size)
    }
}

/// Build an augmented schema by appending the `vep` field to the inner row type.
pub fn build_augmented_schema(inner_type: &EncodedType) -> Result<EncodedType> {
    match inner_type {
        EncodedType::EBaseStruct { required, fields } => {
            let mut new_fields = fields.clone();
            let next_idx = new_fields.len();
            let mut vf = vep_field();
            vf.index = next_idx;
            new_fields.push(vf);
            Ok(EncodedType::EBaseStruct {
                required: *required,
                fields: new_fields,
            })
        }
        _ => Err(crate::HailError::InvalidFormat(
            "Expected struct row type for VEP annotation".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::DataSource;

    #[test]
    fn test_annotating_datasource_with_vcf() {
        let vcf_path = "../data/test/pcsk9_test.vcf";
        let gff3_path = "../data/test/pcsk9_transcripts.gff3";

        let vcf_source = crate::vcf::VcfDataSource::new(vcf_path).unwrap();
        let options = VepInitOptions {
            gff3: gff3_path.to_string(),
            fasta: None,
            sa_dir: None,
            distance: 5000,
            pick: false,
        };

        let annotating = AnnotatingDataSource::new(Box::new(vcf_source), options).unwrap();

        // Schema test: row_type should include the vep field
        let row_type = annotating.row_type();
        match row_type {
            EncodedType::EBaseStruct { fields, .. } => {
                let vep_field = fields.iter().find(|f| f.name == "vep");
                assert!(vep_field.is_some(), "row_type should contain a 'vep' field");
                let vep_type = &vep_field.unwrap().encoded_type;
                match vep_type {
                    EncodedType::EArray { element, .. } => match element.as_ref() {
                        EncodedType::EBaseStruct { fields, .. } => {
                            assert!(fields.iter().any(|f| f.name == "consequence"));
                            assert!(fields.iter().any(|f| f.name == "gene_symbol"));
                            assert!(fields.iter().any(|f| f.name == "transcript_id"));
                        }
                        _ => panic!("vep array element should be a struct"),
                    },
                    _ => panic!("vep field should be an array"),
                }
            }
            _ => panic!("row_type should be a struct"),
        }

        // Data test: query rows and check vep field is populated
        let rows: Vec<_> = annotating
            .query_stream(&[])
            .unwrap()
            .take(5)
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert!(!rows.is_empty(), "should have rows from VCF");

        for row in &rows {
            let fields = match row {
                EncodedValue::Struct(f) => f,
                _ => panic!("expected struct row"),
            };

            let vep_field = fields.iter().find(|(name, _)| name == "vep");
            assert!(vep_field.is_some(), "row should have vep field");

            let vep_array = match &vep_field.unwrap().1 {
                EncodedValue::Array(a) => a,
                _ => panic!("vep should be an array"),
            };

            // Every variant in the PCSK9 test VCF should get at least one annotation
            // (either genic or intergenic)
            assert!(
                !vep_array.is_empty(),
                "vep array should have at least one entry"
            );

            // Check the first entry has the expected fields
            let first = match &vep_array[0] {
                EncodedValue::Struct(f) => f,
                _ => panic!("vep element should be a struct"),
            };

            let consequence = first.iter().find(|(name, _)| name == "consequence");
            assert!(consequence.is_some(), "should have consequence field");
            let csq_str = consequence.unwrap().1.as_string().unwrap();
            assert!(!csq_str.is_empty(), "consequence should be non-empty");
        }
    }

    #[test]
    fn test_annotating_datasource_schema_augmentation() {
        let inner_schema = EncodedType::EBaseStruct {
            required: true,
            fields: vec![
                crate::codec::EncodedField {
                    name: "locus".to_string(),
                    encoded_type: EncodedType::EBinary { required: true },
                    index: 0,
                },
                crate::codec::EncodedField {
                    name: "alleles".to_string(),
                    encoded_type: EncodedType::EBinary { required: true },
                    index: 1,
                },
            ],
        };

        let augmented = build_augmented_schema(&inner_schema).unwrap();
        match augmented {
            EncodedType::EBaseStruct { fields, .. } => {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].name, "locus");
                assert_eq!(fields[1].name, "alleles");
                assert_eq!(fields[2].name, "vep");
                assert_eq!(fields[2].index, 2);
            }
            _ => panic!("expected struct"),
        }
    }
}
