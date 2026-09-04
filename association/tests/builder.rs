use genohype_association::{
    build_association_session, AssociationBuildConfig, AssociationBuildOptionsV1,
    AssociationBuildV1, AssociationPageV1, AssociationPointV1, AssociationRegionRequestV1,
    AssociationSource, AssociationSourceError, AssociationSourceFieldV1, AssociationSourceItem,
    AssociationSourceKindV1, AssociationSourceMetadataV1, AssociationSourceRejectionV1,
    AssociationStore, AssociationStoreError, AssociationTablePageRequestV1, AssociationThresholdV1,
    Background, BuildCancellation, DatasetIdV1, GenomeLayoutV1, NormalizedFieldMappingV1,
    ResourceDescriptorV1, WarningCode,
};
use std::collections::VecDeque;

fn id() -> DatasetIdV1 {
    DatasetIdV1::new("123e4567-e89b-42d3-a456-426614174000").unwrap()
}
fn fingerprint() -> String {
    format!("sha256:{}", "a".repeat(64))
}
fn metadata() -> AssociationSourceMetadataV1 {
    AssociationSourceMetadataV1 {
        kind: AssociationSourceKindV1::Tsv,
        observed_fields: vec![
            AssociationSourceFieldV1 {
                name: "CHR".into(),
                data_type: Some("string".into()),
            },
            AssociationSourceFieldV1 {
                name: "POS".into(),
                data_type: Some("integer".into()),
            },
            AssociationSourceFieldV1 {
                name: "P".into(),
                data_type: Some("number".into()),
            },
        ],
        normalized_mapping: NormalizedFieldMappingV1 {
            contig: "CHR".into(),
            position: "POS".into(),
            ref_allele: None,
            alt_allele: None,
            pvalue: Some("P".into()),
            neg_log10_p: None,
            beta: None,
            se: None,
            af: None,
            ac: None,
            gene: None,
            consequence: None,
            series: None,
        },
        assembly: "GRCh38".into(),
        display_name: Some("Association dataset".into()),
    }
}
fn point(ordinal: u64, contig: &str, position: i64, pvalue: f64, logp: f64) -> AssociationPointV1 {
    AssociationPointV1 {
        contig: contig.into(),
        position,
        ref_allele: None,
        alt_allele: None,
        pvalue,
        neg_log10_p: logp,
        beta: None,
        standard_error: None,
        allele_frequency: None,
        allele_count: None,
        gene_symbol: None,
        consequence: None,
        series: None,
        source_ordinal: ordinal,
    }
}
fn config(batch: usize) -> AssociationBuildConfig {
    let layout = GenomeLayoutV1::new([("1", 100), ("2", 100)], 100, 50, 1).unwrap();
    AssociationBuildConfig {
        dataset_id: id(),
        layout,
        thresholds: vec![AssociationThresholdV1 {
            name: "genome_wide_significance".into(),
            pvalue: 5e-8,
        }],
        options: AssociationBuildOptionsV1 {
            top_hits: 3,
            insert_batch_size: batch,
            significance_threshold: 5e-8,
        },
        point_radius: 2.5,
        point_alpha: 178,
        background: Background::White,
        title: "Association dataset".into(),
        genohype_version: "test".into(),
        created_at: "2026-09-04T00:00:00Z".into(),
        table_resource: ResourceDescriptorV1 {
            url: format!("datasets/{}/table", id()),
            media_type: "application/json".into(),
            byte_size: None,
            checksum: None,
            expires_at: None,
        },
        region_resource: ResourceDescriptorV1 {
            url: format!("datasets/{}/region", id()),
            media_type: "application/json".into(),
            byte_size: None,
            checksum: None,
            expires_at: None,
        },
    }
}

struct FakeSource {
    metadata: AssociationSourceMetadataV1,
    items: VecDeque<AssociationSourceItem>,
    fingerprint: String,
    eof_count: usize,
    calls: usize,
    exhausted: bool,
    fatal_after: Option<usize>,
}
impl FakeSource {
    fn new(items: Vec<AssociationSourceItem>) -> Self {
        Self {
            metadata: metadata(),
            items: items.into(),
            fingerprint: fingerprint(),
            eof_count: 0,
            calls: 0,
            exhausted: false,
            fatal_after: None,
        }
    }
}
impl AssociationSource for FakeSource {
    fn metadata(&self) -> &AssociationSourceMetadataV1 {
        &self.metadata
    }
    fn next_item(&mut self) -> Result<Option<AssociationSourceItem>, AssociationSourceError> {
        assert!(!self.exhausted, "builder traversed the source after EOF");
        self.calls += 1;
        if self.fatal_after == Some(self.calls) {
            return Err(AssociationSourceError::new(
                "injected",
                "fatal source error",
            ));
        }
        if let Some(item) = self.items.pop_front() {
            Ok(Some(item))
        } else {
            self.exhausted = true;
            self.eof_count += 1;
            Ok(None)
        }
    }
    fn finalized_source_fingerprint(&self) -> Option<&str> {
        self.exhausted.then_some(self.fingerprint.as_str())
    }
}

#[derive(Default)]
struct FakeStore {
    building: bool,
    complete: bool,
    aborted: bool,
    points: Vec<AssociationPointV1>,
    top: Vec<AssociationPointV1>,
    batches: Vec<usize>,
    fingerprint: Option<String>,
    fail_batch: Option<usize>,
    cancel_after_batch: Option<(usize, BuildCancellation)>,
}
impl AssociationStore for FakeStore {
    fn begin_build(&mut self, _: &AssociationBuildV1) -> Result<(), AssociationStoreError> {
        self.building = true;
        Ok(())
    }
    fn insert_points(
        &mut self,
        _: &DatasetIdV1,
        points: &[AssociationPointV1],
    ) -> Result<(), AssociationStoreError> {
        let number = self.batches.len() + 1;
        if self.fail_batch == Some(number) {
            return Err(AssociationStoreError::Backend("injected".into()));
        }
        self.batches.push(points.len());
        self.points.extend_from_slice(points);
        if self
            .cancel_after_batch
            .as_ref()
            .is_some_and(|(batch, _)| *batch == number)
        {
            self.cancel_after_batch.as_ref().unwrap().1.cancel();
        }
        Ok(())
    }
    fn replace_top_hits(
        &mut self,
        _: &DatasetIdV1,
        points: &[AssociationPointV1],
    ) -> Result<(), AssociationStoreError> {
        self.top = points.to_vec();
        Ok(())
    }
    fn complete_build(
        &mut self,
        _: &DatasetIdV1,
        fingerprint: &str,
    ) -> Result<(), AssociationStoreError> {
        self.fingerprint = Some(fingerprint.into());
        self.building = false;
        self.complete = true;
        Ok(())
    }
    fn abort_build(&mut self, _: &DatasetIdV1) -> Result<(), AssociationStoreError> {
        self.aborted = true;
        self.building = false;
        self.complete = false;
        self.points.clear();
        self.top.clear();
        Ok(())
    }
    fn is_complete(&self, _: &DatasetIdV1) -> Result<bool, AssociationStoreError> {
        Ok(self.complete)
    }
    fn top_hits(
        &self,
        _: &DatasetIdV1,
        limit: u32,
    ) -> Result<Vec<AssociationPointV1>, AssociationStoreError> {
        Ok(self.top[..self.top.len().min(limit as usize)].to_vec())
    }
    fn table_page(
        &self,
        _: &AssociationTablePageRequestV1,
    ) -> Result<AssociationPageV1, AssociationStoreError> {
        unreachable!()
    }
    fn region_page(
        &self,
        _: &AssociationRegionRequestV1,
    ) -> Result<AssociationPageV1, AssociationStoreError> {
        unreachable!()
    }
}

fn canonical_items() -> Vec<AssociationSourceItem> {
    vec![
        AssociationSourceItem::Point {
            point: point(0, "1", 1, 1e-9, 9.0),
            warnings: vec![],
        },
        AssociationSourceItem::Rejected(AssociationSourceRejectionV1::new(
            1,
            "bad_pvalue",
            Some("P".into()),
            Some("not-a-number"),
        )),
        AssociationSourceItem::Point {
            point: point(2, "2", 10, 1e-3, 3.0),
            warnings: vec![],
        },
        AssociationSourceItem::Point {
            point: point(3, "GL1", 1, 0.0, 400.0),
            warnings: vec![
                WarningCode::PvalueUnderflow,
                WarningCode::NegLog10PDisplayCapped,
            ],
        },
        AssociationSourceItem::Point {
            point: point(4, "1", 50, 1e-10, 10.0),
            warnings: vec![],
        },
        AssociationSourceItem::Point {
            point: point(5, "2", 100, 5e-8, -5e-8_f64.log10()),
            warnings: vec![],
        },
    ]
}

#[test]
fn one_pass_build_keeps_every_product_and_batch_count_consistent() {
    let mut source = FakeSource::new(canonical_items());
    let mut store = FakeStore::default();
    let product = build_association_session(
        &mut source,
        &mut store,
        &config(2),
        &BuildCancellation::default(),
    )
    .unwrap();

    assert_eq!(source.eof_count, 1);
    assert_eq!(source.calls, 7);
    assert_eq!(store.batches, vec![2, 2, 1]);
    assert!(store.batches.iter().all(|size| *size <= 1_000));
    assert_eq!(store.points.len(), 5);
    assert_eq!(product.progress.rows_persisted, 5);
    assert_eq!(product.summary.row_count, 6);
    assert_eq!(product.summary.valid_row_count, 5);
    assert_eq!(product.summary.skipped_row_count, 1);
    assert_eq!(product.summary.rendered_point_count, 4);
    assert_eq!(product.summary.significant_hit_count, 3);
    assert_eq!(product.rejection_diagnostics.total, 1);
    assert_eq!(product.top_hits, store.top);
    assert_eq!(product.top_hits, product.manifest.top_hits);
    assert_eq!(product.manifest.title, "Association dataset");
    assert!(product.png_bytes.starts_with(b"\x89PNG\r\n\x1a\n"));
    let expected_fingerprint = fingerprint();
    assert_eq!(
        store.fingerprint.as_deref(),
        Some(expected_fingerprint.as_str())
    );
}

#[test]
fn maximum_batch_size_has_exact_high_water_and_bounded_top_k() {
    let items = (0..2_501)
        .map(|ordinal| AssociationSourceItem::Point {
            point: point(
                ordinal,
                "1",
                (ordinal % 100 + 1) as i64,
                10_f64.powf(-(ordinal as f64 / 100.0)),
                ordinal as f64 / 100.0,
            ),
            warnings: vec![],
        })
        .collect();
    let mut source = FakeSource::new(items);
    let mut store = FakeStore::default();
    let product = build_association_session(
        &mut source,
        &mut store,
        &config(1_000),
        &BuildCancellation::default(),
    )
    .unwrap();
    assert_eq!(store.batches, vec![1_000, 1_000, 501]);
    assert_eq!(product.top_hits.len(), 3);
    assert_eq!(product.summary.valid_row_count, 2_501);
    assert_eq!(source.eof_count, 1);
}

#[test]
fn store_failure_and_cancellation_rollback_partial_builds() {
    let mut source = FakeSource::new(canonical_items());
    let mut store = FakeStore {
        fail_batch: Some(2),
        ..FakeStore::default()
    };
    assert!(build_association_session(
        &mut source,
        &mut store,
        &config(2),
        &BuildCancellation::default(),
    )
    .is_err());
    assert!(store.aborted);
    assert!(store.points.is_empty());

    let mut source = FakeSource::new(canonical_items());
    source.fatal_after = Some(4);
    let mut store = FakeStore::default();
    assert!(build_association_session(
        &mut source,
        &mut store,
        &config(2),
        &BuildCancellation::default(),
    )
    .is_err());
    assert!(store.aborted);
    assert!(store.points.is_empty());

    let cancellation = BuildCancellation::default();
    let mut source = FakeSource::new(canonical_items());
    let mut store = FakeStore {
        cancel_after_batch: Some((1, cancellation.clone())),
        ..FakeStore::default()
    };
    assert!(
        build_association_session(&mut source, &mut store, &config(2), &cancellation,).is_err()
    );
    assert!(store.aborted);
    assert!(store.points.is_empty());

    let cancellation = BuildCancellation::default();
    cancellation.cancel();
    let mut source = FakeSource::new(canonical_items());
    let mut store = FakeStore::default();
    assert!(
        build_association_session(&mut source, &mut store, &config(2), &cancellation,).is_err()
    );
    assert!(!store.building && !store.aborted);
    assert_eq!(source.calls, 0);
}

#[test]
fn public_rejection_fields_cannot_bypass_builder_bounds() {
    let mut source = FakeSource::new(vec![AssociationSourceItem::Rejected(
        AssociationSourceRejectionV1 {
            source_ordinal: 0,
            reason_code: "x".repeat(129),
            field: None,
            bounded_example: None,
        },
    )]);
    let mut store = FakeStore::default();
    assert!(build_association_session(
        &mut source,
        &mut store,
        &config(2),
        &BuildCancellation::default(),
    )
    .is_err());
    assert!(store.aborted);
}
