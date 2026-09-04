use super::server::{self, DatasetService, ServerState};
use super::*;
use crate::association_session::{
    AssociationSessionRegistry, CompletedAssociationSession, NormalizedSessionRequestV1,
    SessionCompletion, SessionLookup,
};
use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use genohype_association::{
    AssociationBuildV1, AssociationPageV1, AssociationPointV1, AssociationRasterizer,
    AssociationStore, AssociationThresholdV1, AssociationVisualizationManifestV1,
    AssociationVisualizationResourcesV1, Background, DatasetIdV1, GenomeLayoutV1, NeverCancel,
    NormalizedFieldMappingV1, PlotKindV1, ResourceDescriptorV1, SessionBackgroundV1,
    SqliteAssociationStore, COORDINATE_CONVENTION_V1, SCHEMA_VERSION_V1, STORE_SCHEMA_VERSION_V1,
};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};
use tempfile::TempDir;
use tower::ServiceExt;

fn artifact_bytes() -> (AssociationVisualizationManifestV1, Vec<u8>) {
    let layout = GenomeLayoutV1::new(vec![("1".to_string(), 100)], 32, 16, 0).unwrap();
    let pixmap = tiny_skia::Pixmap::new(32, 16).unwrap();
    let raster = pixmap.encode_png().unwrap();
    let manifest = AssociationVisualizationManifestV1 {
        schema_version: SCHEMA_VERSION_V1.into(),
        plot_id: "test".into(),
        kind: PlotKindV1::Manhattan,
        title: "Test plot".into(),
        assembly: "GRCh38".into(),
        coordinate_convention: COORDINATE_CONVENTION_V1.into(),
        source_fingerprint: "sha256:test".into(),
        genohype_version: "test".into(),
        created_at: "2026-09-03T00:00:00Z".into(),
        layout_hash: layout.layout_hash("GRCh38"),
        layout,
        thresholds: vec![AssociationThresholdV1 {
            name: "genome_wide_significance".into(),
            pvalue: 5e-8,
        }],
        row_count: 0,
        valid_row_count: 0,
        skipped_row_count: 0,
        rendered_point_count: 0,
        significant_hit_count: 0,
        top_hits: vec![],
        warnings: vec![],
        resources: AssociationVisualizationResourcesV1 {
            raster: ResourceDescriptorV1 {
                url: "test.png".into(),
                media_type: "image/png".into(),
                byte_size: Some(raster.len() as u64),
                checksum: Some(format!("sha256:{:x}", Sha256::digest(&raster))),
                expires_at: None,
            },
            region_api: None,
            table_api: None,
        },
    };
    manifest.validate().unwrap();
    (manifest, raster)
}

fn write_artifacts(directory: &Path) -> ViewerArtifacts {
    let (manifest, raster) = artifact_bytes();
    let manifest_path = directory.join("plot.manifest.json");
    let raster_path = directory.join("plot.png");
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    fs::write(&raster_path, raster).unwrap();
    ViewerArtifacts::new(manifest_path, raster_path)
}

fn router(token: &str) -> axum::Router {
    let (mut manifest, raster) = artifact_bytes();
    manifest.resources.raster.url = "raster".into();
    server::build_router(ServerState::new(
        token.into(),
        serde_json::to_vec(&manifest).unwrap(),
        raster,
    ))
}

fn dataset_id() -> DatasetIdV1 {
    DatasetIdV1::new("123e4567-e89b-42d3-a456-426614174000").unwrap()
}

fn dataset_router(token: &str, database_path: PathBuf) -> axum::Router {
    let (mut manifest, raster) = artifact_bytes();
    manifest.resources.raster.url = "raster".into();
    let dataset = DatasetService::for_test(dataset_id(), database_path);
    server::build_router(
        ServerState::new(token.into(), serde_json::to_vec(&manifest).unwrap(), raster)
            .with_dataset(dataset),
    )
}

fn point(contig: &str, position: i64, pvalue: f64, gene: &str, ordinal: u64) -> AssociationPointV1 {
    AssociationPointV1 {
        contig: contig.into(),
        position,
        ref_allele: Some("A".into()),
        alt_allele: Some("G".into()),
        pvalue,
        neg_log10_p: -pvalue.log10(),
        beta: None,
        standard_error: None,
        allele_frequency: None,
        allele_count: None,
        gene_symbol: Some(gene.into()),
        consequence: None,
        series: None,
        source_ordinal: ordinal,
    }
}

fn write_dataset(path: &Path) {
    let id = dataset_id();
    let mut store = SqliteAssociationStore::create(path).unwrap();
    store
        .begin_build(&AssociationBuildV1 {
            dataset_id: id.clone(),
            created_at: "2026-09-03T00:00:00Z".into(),
        })
        .unwrap();
    store
        .insert_points(
            &id,
            &[
                point("1", 10, 0.05, "GENE1", 0),
                point("1", 20, 1e-8, "GENE2", 1),
                point("2", 1, 0.01, "GENE1", 2),
            ],
        )
        .unwrap();
    store
        .complete_build(&id, &format!("sha256:{}", "a".repeat(64)))
        .unwrap();
}

fn completed_ephemeral_session() -> (TempDir, CompletedAssociationSession, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let registry = AssociationSessionRegistry::open_for_test(temp.path().join("registry")).unwrap();
    let source = temp.path().join("source.tsv");
    fs::write(&source, b"CHR\tPOS\tP\n1\t10\t1e-8\n").unwrap();
    let layout = GenomeLayoutV1::new(vec![("1".to_string(), 100)], 32, 16, 0).unwrap();
    let request = NormalizedSessionRequestV1 {
        association_schema_version: SCHEMA_VERSION_V1.into(),
        store_schema_version: STORE_SCHEMA_VERSION_V1,
        mapping: NormalizedFieldMappingV1 {
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
        thresholds: vec![AssociationThresholdV1 {
            name: "genome_wide_significance".into(),
            pvalue: 5e-8,
        }],
        significance_threshold: 5e-8,
        top_hits: 1,
        width: layout.width,
        height: layout.height,
        layout_hash: layout.layout_hash("GRCh38"),
        point_radius: 2.5,
        point_alpha: 178,
        background: SessionBackgroundV1::White,
        renderer_version: "test".into(),
    };
    let lookup = SessionLookup::observe_local(&source, request).unwrap();
    let claim = registry.claim(lookup).unwrap();
    let id = claim.dataset_id().clone();
    let fingerprint = format!("sha256:{}", "a".repeat(64));
    let association_point = point("1", 10, 1e-8, "GENE1", 0);

    let mut store = SqliteAssociationStore::create(claim.database_path()).unwrap();
    store
        .begin_build(&AssociationBuildV1 {
            dataset_id: id.clone(),
            created_at: "unix:1".into(),
        })
        .unwrap();
    store
        .insert_points(&id, std::slice::from_ref(&association_point))
        .unwrap();
    store
        .replace_top_hits(&id, std::slice::from_ref(&association_point))
        .unwrap();
    store.complete_build(&id, &fingerprint).unwrap();
    drop(store);

    let mut rasterizer = AssociationRasterizer::from_layout(&layout, Background::White).unwrap();
    rasterizer
        .render_association_point(&layout, &association_point, 2.5, 178)
        .unwrap();
    let raster = rasterizer.png_bytes().unwrap();
    let manifest = AssociationVisualizationManifestV1 {
        schema_version: SCHEMA_VERSION_V1.into(),
        plot_id: id.to_string(),
        kind: PlotKindV1::Manhattan,
        title: "Ephemeral production-path fixture".into(),
        assembly: "GRCh38".into(),
        coordinate_convention: COORDINATE_CONVENTION_V1.into(),
        source_fingerprint: fingerprint.clone(),
        genohype_version: "test".into(),
        created_at: "unix:1".into(),
        layout: layout.clone(),
        layout_hash: layout.layout_hash("GRCh38"),
        thresholds: vec![AssociationThresholdV1 {
            name: "genome_wide_significance".into(),
            pvalue: 5e-8,
        }],
        row_count: 1,
        valid_row_count: 1,
        skipped_row_count: 0,
        rendered_point_count: 1,
        significant_hit_count: 1,
        top_hits: vec![association_point],
        warnings: vec![],
        resources: AssociationVisualizationResourcesV1 {
            raster: ResourceDescriptorV1 {
                url: "raster.png".into(),
                media_type: "image/png".into(),
                byte_size: Some(raster.len() as u64),
                checksum: Some(format!("sha256:{:x}", Sha256::digest(&raster))),
                expires_at: None,
            },
            region_api: Some(ResourceDescriptorV1 {
                url: format!("datasets/{id}/region"),
                media_type: "application/json".into(),
                byte_size: None,
                checksum: None,
                expires_at: None,
            }),
            table_api: Some(ResourceDescriptorV1 {
                url: format!("datasets/{id}/table"),
                media_type: "application/json".into(),
                byte_size: None,
                checksum: None,
                expires_at: None,
            }),
        },
    };
    claim.write_raster(&raster, &NeverCancel).unwrap();
    claim.write_manifest(&manifest, &NeverCancel).unwrap();
    let session = claim
        .publish(
            SessionCompletion {
                source_fingerprint: fingerprint,
                created_at: UNIX_EPOCH + Duration::from_secs(1),
                expires_at: None,
                ephemeral: true,
            },
            &NeverCancel,
        )
        .unwrap();
    let generation = session.database_path().parent().unwrap().to_path_buf();
    assert!(generation.is_dir());
    (temp, session, generation)
}

fn refresh_database_completion_marker(session: &mut CompletedAssociationSession) {
    let database = fs::read(session.database_path()).unwrap();
    {
        let marker = session.completion_mut_for_test();
        marker.database.byte_size = database.len() as u64;
        marker.database.checksum = format!("sha256:{:x}", Sha256::digest(&database));
    }
    let mut marker = serde_json::to_vec_pretty(session.completion()).unwrap();
    marker.push(b'\n');
    fs::write(
        session
            .database_path()
            .parent()
            .unwrap()
            .join("complete.json"),
        marker,
    )
    .unwrap();
}

async fn router_response(
    router: axum::Router,
    method: Method,
    path: &str,
) -> axum::response::Response {
    router
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn response(path: &str) -> axum::response::Response {
    router("correct-token")
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

fn assert_security_headers(response: &axum::response::Response) {
    let headers = response.headers();
    assert!(headers.contains_key("content-security-policy"));
    assert_eq!(headers["x-content-type-options"], "nosniff");
    assert_eq!(headers["referrer-policy"], "no-referrer");
    assert_eq!(headers["x-frame-options"], "DENY");
    assert_eq!(headers["cross-origin-resource-policy"], "same-origin");
    assert!(!headers.contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN));
}

#[tokio::test]
async fn serves_only_token_scoped_allowlisted_resources() {
    let root = response("/correct-token/").await;
    assert_eq!(root.status(), StatusCode::OK);
    assert_eq!(
        root.headers()[header::CONTENT_TYPE],
        "text/html; charset=utf-8"
    );
    assert_eq!(root.headers()[header::CACHE_CONTROL], "no-store");
    assert_security_headers(&root);
    let root_html = String::from_utf8(
        to_bytes(root.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    if let Some(start) = root_html.find("./assets/") {
        let tail = &root_html[start + "./assets/".len()..];
        let end = tail.find(['\"', '\'']).unwrap();
        let asset_path = &tail[..end];
        let asset = response(&format!("/correct-token/assets/{asset_path}")).await;
        assert_eq!(asset.status(), StatusCode::OK);
        assert!(asset.headers()[header::CACHE_CONTROL]
            .to_str()
            .unwrap()
            .contains("immutable"));
        assert_security_headers(&asset);
    }

    let manifest = response("/correct-token/api/manifest").await;
    assert_eq!(manifest.status(), StatusCode::OK);
    assert_eq!(manifest.headers()[header::CONTENT_TYPE], "application/json");
    assert_eq!(manifest.headers()[header::CACHE_CONTROL], "no-store");
    assert_security_headers(&manifest);
    let body = to_bytes(manifest.into_body(), usize::MAX).await.unwrap();
    let parsed: AssociationVisualizationManifestV1 = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed.resources.raster.url, "raster");

    let raster = response("/correct-token/api/raster").await;
    assert_eq!(raster.status(), StatusCode::OK);
    assert_eq!(raster.headers()[header::CONTENT_TYPE], "image/png");
    assert_eq!(raster.headers()[header::CACHE_CONTROL], "no-store");
    assert_security_headers(&raster);
    assert_eq!(
        to_bytes(raster.into_body(), usize::MAX)
            .await
            .unwrap()
            .as_ref(),
        artifact_bytes().1
    );

    let method_not_allowed = router("correct-token")
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/correct-token/api/manifest")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(method_not_allowed.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        method_not_allowed.headers()[header::CACHE_CONTROL],
        "no-store"
    );
    assert_security_headers(&method_not_allowed);

    for path in [
        "/wrong-token/",
        "/wrong-token/api/manifest",
        "/correct-token/api/unknown",
        "/correct-token/missing.js",
        "/correct-token/assets/missing.js",
        "/correct-token/assets/../index.html",
        "/correct-token/assets/%2e%2e/index.html",
        "/correct-token/assets/%2Fetc",
        "/correct-token/assets/a%5cb.js",
    ] {
        let not_found = response(path).await;
        assert_eq!(not_found.status(), StatusCode::NOT_FOUND, "{path}");
        assert_security_headers(&not_found);
        let body = to_bytes(not_found.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"not found\n", "no SPA fallback for {path}");
    }
}

#[test]
fn validates_manifest_raster_contract_before_serving() {
    let temp = tempfile::tempdir().unwrap();
    let artifacts = write_artifacts(temp.path());
    let prepared = prepare_artifacts(&artifacts).unwrap();
    let on_disk: AssociationVisualizationManifestV1 =
        serde_json::from_slice(&fs::read(&artifacts.manifest_path).unwrap()).unwrap();
    let served: AssociationVisualizationManifestV1 =
        serde_json::from_slice(&prepared.manifest).unwrap();
    assert_eq!(on_disk.resources.raster.url, "test.png");
    assert_eq!(served.resources.raster.url, "raster");

    fs::write(&artifacts.raster_path, b"not a PNG").unwrap();
    assert!(prepare_artifacts(&artifacts)
        .unwrap_err()
        .to_string()
        .contains("byte size"));

    let artifacts = write_artifacts(temp.path());
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&artifacts.manifest_path).unwrap()).unwrap();
    manifest["resources"]["raster"]["media_type"] = serde_json::json!("text/html");
    fs::write(
        &artifacts.manifest_path,
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    assert!(prepare_artifacts(&artifacts)
        .unwrap_err()
        .to_string()
        .contains("media type"));

    let artifacts = write_artifacts(temp.path());
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&artifacts.manifest_path).unwrap()).unwrap();
    manifest["resources"]["raster"]["url"] = serde_json::json!("../secret.png");
    fs::write(
        &artifacts.manifest_path,
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    assert!(prepare_artifacts(&artifacts).is_err());

    let artifacts = write_artifacts(temp.path());
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&artifacts.manifest_path).unwrap()).unwrap();
    manifest["resources"]["raster"]["byte_size"] = serde_json::Value::Null;
    manifest["resources"]["raster"]["checksum"] = serde_json::json!("sha256:deadbeef");
    fs::write(
        &artifacts.manifest_path,
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    assert!(prepare_artifacts(&artifacts)
        .unwrap_err()
        .to_string()
        .contains("checksum"));

    let artifacts = write_artifacts(temp.path());
    let smaller = tiny_skia::Pixmap::new(8, 8).unwrap().encode_png().unwrap();
    fs::write(&artifacts.raster_path, &smaller).unwrap();
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&artifacts.manifest_path).unwrap()).unwrap();
    manifest["resources"]["raster"]["byte_size"] = serde_json::json!(smaller.len());
    manifest["resources"]["raster"]["checksum"] =
        serde_json::json!(format!("sha256:{:x}", Sha256::digest(&smaller)));
    fs::write(
        &artifacts.manifest_path,
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    assert!(prepare_artifacts(&artifacts)
        .unwrap_err()
        .to_string()
        .contains("dimensions"));
}

#[test]
fn manifest_rebase_preserves_unknown_compatible_v1_fields() {
    let (mut manifest, raster) = artifact_bytes();
    manifest.resources.table_api = Some(ResourceDescriptorV1 {
        url: "datasets/old/table".into(),
        media_type: "application/json".into(),
        byte_size: None,
        checksum: None,
        expires_at: None,
    });
    manifest.resources.region_api = Some(ResourceDescriptorV1 {
        url: "datasets/old/region".into(),
        media_type: "application/json".into(),
        byte_size: None,
        checksum: None,
        expires_at: None,
    });
    let mut value = serde_json::to_value(manifest).unwrap();
    value["future_root"] = serde_json::json!({"kept": [1, 2, 3]});
    value["resources"]["future_resource"] = serde_json::json!({"kind": "future"});
    value["resources"]["raster"]["future_checksum_kind"] = serde_json::json!(7);
    value["resources"]["table_api"]["future_table_field"] = serde_json::json!(true);
    value["resources"]["region_api"]["future_region_field"] = serde_json::json!("kept");

    let prepared = prepare_manifest_and_raster(
        serde_json::to_vec(&value).unwrap(),
        raster,
        Some((
            "datasets/123e4567-e89b-42d3-a456-426614174000/table",
            "datasets/123e4567-e89b-42d3-a456-426614174000/region",
        )),
    )
    .unwrap();
    let served: serde_json::Value = serde_json::from_slice(&prepared.manifest).unwrap();
    assert_eq!(served["future_root"], value["future_root"]);
    assert_eq!(
        served["resources"]["future_resource"],
        value["resources"]["future_resource"]
    );
    assert_eq!(
        served["resources"]["raster"]["future_checksum_kind"],
        serde_json::json!(7)
    );
    assert_eq!(
        served["resources"]["table_api"]["future_table_field"],
        serde_json::json!(true)
    );
    assert_eq!(
        served["resources"]["region_api"]["future_region_field"],
        serde_json::json!("kept")
    );
    assert_eq!(served["resources"]["raster"]["url"], "raster");
    assert_eq!(
        served["resources"]["table_api"]["url"],
        "datasets/123e4567-e89b-42d3-a456-426614174000/table"
    );
    assert_eq!(
        served["resources"]["region_api"]["url"],
        "datasets/123e4567-e89b-42d3-a456-426614174000/region"
    );
    let projection: AssociationVisualizationManifestV1 = serde_json::from_value(served).unwrap();
    projection.validate().unwrap();
}

#[tokio::test]
async fn dataset_routes_page_sort_search_and_use_inclusive_regions() {
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("private-associations.sqlite3");
    write_dataset(&database);
    DatasetService::for_test(dataset_id(), database.clone())
        .validate_complete()
        .await
        .unwrap();
    let base = "/correct-token/api/datasets/123e4567-e89b-42d3-a456-426614174000";

    let default = router_response(
        dataset_router("correct-token", database.clone()),
        Method::GET,
        &format!("{base}/table"),
    )
    .await;
    assert_eq!(default.status(), StatusCode::OK);
    assert_eq!(default.headers()[header::CONTENT_TYPE], "application/json");
    assert_eq!(default.headers()[header::CACHE_CONTROL], "no-store");
    assert_security_headers(&default);
    let default: AssociationPageV1 =
        serde_json::from_slice(&to_bytes(default.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(default.offset, 0);
    assert_eq!(default.limit, 100);
    assert_eq!(default.total, 3);
    assert_eq!(default.points[0].position, 20);
    assert_eq!(default.points[1].contig, "2");
    assert_eq!(default.points[2].position, 10);

    let page = router_response(
        dataset_router("correct-token", database.clone()),
        Method::GET,
        &format!("{base}/table?offset=1&limit=1&sort=locus&direction=descending"),
    )
    .await;
    let page: AssociationPageV1 =
        serde_json::from_slice(&to_bytes(page.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(page.total, 3);
    assert_eq!(page.points.len(), 1);
    assert_eq!(
        (page.points[0].contig.as_str(), page.points[0].position),
        ("1", 20)
    );

    let search = router_response(
        dataset_router("correct-token", database.clone()),
        Method::GET,
        &format!("{base}/table?search=GENE2"),
    )
    .await;
    let search: AssociationPageV1 =
        serde_json::from_slice(&to_bytes(search.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(search.total, 1);
    assert_eq!(search.points[0].gene_symbol.as_deref(), Some("GENE2"));

    let region = router_response(
        dataset_router("correct-token", database),
        Method::GET,
        &format!("{base}/region?contig=chr1&start=10&end=20"),
    )
    .await;
    assert_eq!(region.status(), StatusCode::OK);
    assert_eq!(region.headers()[header::CACHE_CONTROL], "no-store");
    assert_security_headers(&region);
    let region: AssociationPageV1 =
        serde_json::from_slice(&to_bytes(region.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(region.offset, 0);
    assert_eq!(region.limit, 100);
    assert_eq!(region.total, 2);
    assert_eq!(
        region
            .points
            .iter()
            .map(|point| point.position)
            .collect::<Vec<_>>(),
        vec![10, 20]
    );

    let region_page = router_response(
        dataset_router(
            "correct-token",
            temp.path().join("private-associations.sqlite3"),
        ),
        Method::GET,
        &format!("{base}/region?contig=1&start=10&end=20&offset=1&limit=1"),
    )
    .await;
    let region_page: AssociationPageV1 =
        serde_json::from_slice(&to_bytes(region_page.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(region_page.total, 2);
    assert_eq!(region_page.points.len(), 1);
    assert_eq!(region_page.points[0].position, 20);
}

#[tokio::test]
async fn dataset_route_errors_are_generic_exact_and_hardened() {
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("do-not-leak-this.sqlite3");
    write_dataset(&database);
    let id = dataset_id().to_string();
    let base = format!("/correct-token/api/datasets/{id}");

    for path in [
        format!("/wrong-token/api/datasets/{id}/table"),
        "/correct-token/api/datasets/not-a-dataset/table".into(),
        "/correct-token/api/datasets/123e4567-e89b-42d3-a456-426614174001/table".into(),
        format!("{base}/table/"),
        format!("{base}/unknown"),
    ] {
        let response = router_response(
            dataset_router("correct-token", database.clone()),
            Method::GET,
            &path,
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        assert_security_headers(&response);
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .as_ref(),
            b"not found\n"
        );
    }

    let absent = router_response(
        router("correct-token"),
        Method::GET,
        &format!("{base}/table"),
    )
    .await;
    assert_eq!(absent.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        to_bytes(absent.into_body(), usize::MAX)
            .await
            .unwrap()
            .as_ref(),
        b"not found\n"
    );

    for path in [
        format!("{base}/table?limit=0"),
        format!("{base}/table?limit=1001"),
        format!("{base}/table?offset=9007199254740992"),
        format!("{base}/table?sort=sql"),
        format!("{base}/table?direction=sideways"),
        format!("{base}/table?search={}", "x".repeat(257)),
        format!("{base}/table?search=line%0Abreak"),
        format!("{base}/table?unknown=1"),
        format!("{base}/region?contig=1&start=0&end=1"),
        format!("{base}/region?contig=1&start=2&end=1"),
        format!("{base}/region?contig=%20&start=1&end=2"),
        format!("{base}/region?contig=1&start=1"),
        format!("{base}/region?contig=1&start=1&end=2&limit=1001"),
    ] {
        let response = router_response(
            dataset_router("correct-token", database.clone()),
            Method::GET,
            &path,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert_security_headers(&response);
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .as_ref(),
            b"bad request\n",
            "{path}"
        );
    }

    for route in ["table", "region"] {
        let response = router_response(
            dataset_router("correct-token", database.clone()),
            Method::POST,
            &format!("{base}/{route}"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert_security_headers(&response);
    }

    let failing_router = dataset_router("correct-token", database.clone());
    fs::remove_file(&database).unwrap();
    let failure = router_response(failing_router, Method::GET, &format!("{base}/table")).await;
    assert_eq!(failure.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(failure.headers()[header::CACHE_CONTROL], "no-store");
    assert_security_headers(&failure);
    let body = to_bytes(failure.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), b"internal server error\n");
    assert!(!String::from_utf8_lossy(&body).contains("do-not-leak-this"));
}

#[test]
fn browser_open_failure_keeps_manual_url_available() {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    announce_and_open(
        "http://127.0.0.1:1234/token/",
        true,
        &mut stdout,
        &mut stderr,
        |_| Err(io::Error::new(io::ErrorKind::Other, "browser unavailable")),
    )
    .unwrap();
    assert_eq!(
        String::from_utf8(stdout).unwrap(),
        "Viewer URL: http://127.0.0.1:1234/token/\n"
    );
    let stderr = String::from_utf8(stderr).unwrap();
    assert!(stderr.contains("browser unavailable"));
    assert!(stderr.contains("http://127.0.0.1:1234/token/"));

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    announce_and_open("url", false, &mut stdout, &mut stderr, |_| {
        panic!("opener must not run")
    })
    .unwrap();
    assert!(stderr.is_empty());
}

#[test]
fn production_dataset_serve_fails_closed_for_changed_completed_session_state() {
    let (_temp, session, generation) = completed_ephemeral_session();
    let marker_path = generation.join("complete.json");
    let mut marker: serde_json::Value =
        serde_json::from_slice(&fs::read(&marker_path).unwrap()).unwrap();
    marker["created_at"] = serde_json::json!("unix:2");
    fs::write(&marker_path, serde_json::to_vec_pretty(&marker).unwrap()).unwrap();
    let error = serve_dataset_viewer(
        session,
        ViewerOptions {
            port: 0,
            open: false,
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("session marker changed"));
    assert!(!generation.exists());

    let (_temp, session, generation) = completed_ephemeral_session();
    fs::write(session.raster_path(), b"not the completed raster").unwrap();
    let error = serve_dataset_viewer(
        session,
        ViewerOptions {
            port: 0,
            open: false,
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("raster"));
    assert!(!generation.exists());

    let (_temp, mut session, generation) = completed_ephemeral_session();
    let connection = rusqlite::Connection::open(session.database_path()).unwrap();
    connection
        .execute("UPDATE association_datasets SET status='building'", [])
        .unwrap();
    drop(connection);
    refresh_database_completion_marker(&mut session);
    let error = serve_dataset_viewer(
        session,
        ViewerOptions {
            port: 0,
            open: false,
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("required build state"));
    assert!(!generation.exists());

    let (_temp, mut session, generation) = completed_ephemeral_session();
    let (_other_temp, other_session, other_generation) = completed_ephemeral_session();
    fs::copy(other_session.database_path(), session.database_path()).unwrap();
    refresh_database_completion_marker(&mut session);
    let error = serve_dataset_viewer(
        session,
        ViewerOptions {
            port: 0,
            open: false,
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("required build state"));
    assert!(!generation.exists());
    assert!(other_generation.exists());
    drop(other_session);
    assert!(!other_generation.exists());
}

#[tokio::test]
async fn production_dataset_router_retains_ephemeral_session_until_shutdown_drop() {
    let (_temp, session, generation) = completed_ephemeral_session();
    let dataset_id = session.dataset_id().clone();
    let prepared = prepare_dataset_artifacts(&session).unwrap();
    let dataset = DatasetService::from_session(session);
    dataset.validate_complete().await.unwrap();
    let token = server::generate_url_token();
    let app = server::build_router(
        ServerState::new(token.clone(), prepared.manifest, prepared.raster).with_dataset(dataset),
    );
    assert!(
        generation.exists(),
        "router state must retain the generation"
    );

    let listener = server::bind_loopback(0).await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(server::serve_with_shutdown(listener, app, async move {
        let _ = shutdown_rx.await;
    }));
    let url = format!("http://{address}/{token}/api/datasets/{dataset_id}/table?limit=1");
    let response = reqwest::get(url).await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let page: AssociationPageV1 = response.json().await.unwrap();
    assert_eq!(page.total, 1);
    assert!(
        generation.exists(),
        "live server state dropped the generation"
    );

    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("server did not shut down")
        .unwrap()
        .unwrap();
    assert!(
        !generation.exists(),
        "final server-state drop must delete the ephemeral generation"
    );
}

#[tokio::test]
async fn ephemeral_listener_is_loopback_and_shutdown_is_injected() {
    let token = server::generate_url_token();
    assert_ne!(token, server::generate_url_token());
    let listener = server::bind_loopback(0).await.unwrap();
    let address = listener.local_addr().unwrap();
    assert_eq!(address.ip().to_string(), "127.0.0.1");
    assert_ne!(address.port(), 0);
    let url = server::viewer_url(address, &token).unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(server::serve_with_shutdown(
        listener,
        router(&token),
        async move {
            let _ = shutdown_rx.await;
        },
    ));

    let manifest_url = reqwest::Url::parse(&format!("{url}api/manifest")).unwrap();
    let manifest_response = reqwest::get(manifest_url.clone()).await.unwrap();
    assert_eq!(manifest_response.status(), reqwest::StatusCode::OK);
    let manifest: AssociationVisualizationManifestV1 = manifest_response.json().await.unwrap();
    let raster_response = reqwest::get(manifest_url.join(&manifest.resources.raster.url).unwrap())
        .await
        .unwrap();
    assert_eq!(raster_response.status(), reqwest::StatusCode::OK);
    assert!(!raster_response.bytes().await.unwrap().is_empty());

    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("server did not shut down")
        .unwrap()
        .unwrap();
}
