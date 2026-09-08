//! Bounded local profiling harness. Usage: association_build_profile SOURCE.ht OUT_DIR [scan|render|build|build-batched]
//! OUT_DIR must be new. Uses the production projected adapter, builder and SQLite store.
//! Timers include wall time, not CPU time; source includes decode/normalization/semantic hash.
#![allow(dead_code)]
#[path = "../src/association_sources/mod.rs"]
mod association_sources;
use association_sources::hail::HailAssociationSource;
use genohype_association::*;
pub use genohype_cli::cli;
use std::time::{Duration, Instant};

struct TimedSource {
    inner: HailAssociationSource,
    elapsed: Duration,
}
impl AssociationSource for TimedSource {
    fn metadata(&self) -> &AssociationSourceMetadataV1 {
        self.inner.metadata()
    }
    fn next_item(&mut self) -> Result<Option<AssociationSourceItem>, AssociationSourceError> {
        let t = Instant::now();
        let result = self.inner.next_item();
        self.elapsed += t.elapsed();
        result
    }
    fn finalized_source_fingerprint(&self) -> Option<&str> {
        self.inner.finalized_source_fingerprint()
    }
}
struct TimedStore {
    inner: SqliteAssociationStore,
    inserts: Duration,
    completion: Duration,
}
impl AssociationStore for TimedStore {
    fn begin_build(&mut self, b: &AssociationBuildV1) -> Result<(), AssociationStoreError> {
        self.inner.begin_build(b)
    }
    fn insert_points(
        &mut self,
        id: &DatasetIdV1,
        p: &[AssociationPointV1],
    ) -> Result<(), AssociationStoreError> {
        let t = Instant::now();
        let r = self.inner.insert_points(id, p);
        self.inserts += t.elapsed();
        r
    }
    fn replace_top_hits(
        &mut self,
        id: &DatasetIdV1,
        p: &[AssociationPointV1],
    ) -> Result<(), AssociationStoreError> {
        self.inner.replace_top_hits(id, p)
    }
    fn complete_build(&mut self, id: &DatasetIdV1, f: &str) -> Result<(), AssociationStoreError> {
        let t = Instant::now();
        let r = self.inner.complete_build(id, f);
        self.completion += t.elapsed();
        r
    }
    fn abort_build(&mut self, id: &DatasetIdV1) -> Result<(), AssociationStoreError> {
        self.inner.abort_build(id)
    }
    fn is_complete(&self, id: &DatasetIdV1) -> Result<bool, AssociationStoreError> {
        self.inner.is_complete(id)
    }
    fn top_hits(
        &self,
        id: &DatasetIdV1,
        n: u32,
    ) -> Result<Vec<AssociationPointV1>, AssociationStoreError> {
        self.inner.top_hits(id, n)
    }
    fn table_page(
        &self,
        r: &AssociationTablePageRequestV1,
    ) -> Result<AssociationPageV1, AssociationStoreError> {
        self.inner.table_page(r)
    }
    fn region_page(
        &self,
        r: &AssociationRegionRequestV1,
    ) -> Result<AssociationPageV1, AssociationStoreError> {
        self.inner.region_page(r)
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(3).map(String::as_str).unwrap_or("build");
    assert!(["scan", "render", "build", "build-batched"].contains(&mode));
    let out = std::path::Path::new(&args[2]);
    std::fs::create_dir(out)?;
    let start = Instant::now();
    let mut source = TimedSource {
        inner: HailAssociationSource::open(
            &args[1],
            NormalizedFieldMappingV1 {
                contig: "locus.contig".into(),
                position: "locus.position".into(),
                pvalue: Some("Pvalue".into()),
                neg_log10_p: Some("Pvalue_log10".into()),
                beta: Some("BETA".into()),
                se: Some("SE".into()),
                ref_allele: None,
                alt_allele: None,
                af: None,
                ac: None,
                gene: None,
                consequence: None,
                series: None,
            },
            "GRCh38",
        )?,
        elapsed: Duration::ZERO,
    };
    let layout = GenomeLayoutV1::new(canonical_contig_lengths(Assembly::Grch38), 3000, 800, 4)?;
    let mut report;
    if mode.starts_with("build") {
        let id = DatasetIdV1::new("123e4567-e89b-42d3-a456-426614174000")?;
        let path = out.join("associations.sqlite3");
        let inner = if mode == "build-batched" {
            SqliteAssociationStore::create(&path)?
        } else {
            SqliteAssociationStore::create_for_bulk_build(&path)?
        };
        let mut store = TimedStore {
            inner,
            inserts: Duration::ZERO,
            completion: Duration::ZERO,
        };
        let resource = |url: &str| ResourceDescriptorV1 {
            url: url.into(),
            media_type: "application/json".into(),
            byte_size: None,
            checksum: None,
            expires_at: None,
        };
        let product = build_association_session(
            &mut source,
            &mut store,
            &AssociationBuildConfig {
                dataset_id: id.clone(),
                layout,
                thresholds: vec![AssociationThresholdV1 {
                    name: "genome_wide_significance".into(),
                    pvalue: 5e-8,
                }],
                options: AssociationBuildOptionsV1::default(),
                point_radius: 2.5,
                point_alpha: 178,
                background: Background::White,
                title: "Association profile".into(),
                genohype_version: "profile".into(),
                created_at: "2026-09-08T00:00:00Z".into(),
                table_resource: resource("table"),
                region_resource: resource("region"),
            },
            &NeverCancel,
        )?;
        assert!(store.is_complete(&id)?);
        assert_eq!(store.top_hits(&id, 100)?, product.top_hits);
        std::fs::write(out.join("raster.png"), &product.png_bytes)?;
        std::fs::write(
            out.join("manifest.json"),
            serde_json::to_vec_pretty(&product.manifest)?,
        )?;
        report = serde_json::json!({"mode": mode, "progress": product.progress, "summary": product.summary, "fingerprint": product.source_fingerprint,
            "insert_seconds": store.inserts.as_secs_f64(), "complete_seconds": store.completion.as_secs_f64(),
            "database_bytes": std::fs::metadata(out.join("associations.sqlite3"))?.len()});
    } else {
        let mut raster = AssociationRasterizer::from_layout(&layout, Background::White)?;
        let mut rows = 0_u64;
        let mut valid = 0_u64;
        let mut rendered = 0_u64;
        let mut render_time = Duration::ZERO;
        while let Some(item) = source.next_item()? {
            rows += 1;
            if let AssociationSourceItem::Point { point, .. } = item {
                valid += 1;
                if mode == "render" {
                    let t = Instant::now();
                    rendered +=
                        u64::from(raster.render_association_point(&layout, &point, 2.5, 178)?);
                    render_time += t.elapsed();
                }
            }
        }
        report = serde_json::json!({"mode": mode, "rows": rows, "valid": valid, "rendered": rendered, "render_seconds": render_time.as_secs_f64(), "fingerprint": source.finalized_source_fingerprint()});
    }
    report["elapsed_seconds"] = start.elapsed().as_secs_f64().into();
    report["source_seconds"] = source.elapsed.as_secs_f64().into();
    let text = serde_json::to_string_pretty(&report)?;
    println!("{text}");
    std::fs::write(out.join("profile.json"), text)?;
    Ok(())
}
