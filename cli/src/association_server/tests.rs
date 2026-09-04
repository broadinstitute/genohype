use super::server::{self, ServerState};
use super::*;
use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use genohype_association::{
    AssociationThresholdV1, AssociationVisualizationManifestV1,
    AssociationVisualizationResourcesV1, GenomeLayoutV1, PlotKindV1, ResourceDescriptorV1,
    COORDINATE_CONVENTION_V1, SCHEMA_VERSION_V1,
};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::time::Duration;
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
