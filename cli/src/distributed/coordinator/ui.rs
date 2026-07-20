//! UI serving for the embedded dashboard SPA.
//!
//! This module handles serving the embedded React dashboard from static files
//! compiled into the binary using rust-embed.

use axum::body::Body;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use rust_embed::Embed;

/// Embedded static files for the pool-dashboard SPA.
///
/// A dashboard build is served from `static/dist` when present. The tracked
/// `static/index.html` remains a fallback so a clean Cargo checkout can build
/// without requiring Node.js.
#[derive(Embed)]
#[folder = "static"]
pub(crate) struct DashboardAssets;

/// Handler for serving embedded dashboard assets.
/// Serves files from the embedded SPA, falling back to index.html for SPA routing.
pub(crate) async fn serve_dashboard_asset(
    axum::extract::Path(path): axum::extract::Path<String>,
) -> impl IntoResponse {
    serve_embedded_file(&path)
}

/// Handler for the dashboard root - serves index.html.
pub(crate) async fn serve_dashboard_index() -> impl IntoResponse {
    serve_embedded_file("index.html")
}

/// Serve an embedded file by path, with SPA fallback to index.html.
pub(crate) fn serve_embedded_file(path: &str) -> Response {
    let get_asset = |asset_path: &str| {
        let built_path = format!("dist/{asset_path}");
        DashboardAssets::get(&built_path).or_else(|| DashboardAssets::get(asset_path))
    };

    // Prefer the compiled React app, then use the tracked fallback dashboard.
    let file = get_asset(path).or_else(|| {
        // For SPA routing: if the path doesn't exist, serve index.html
        // (unless it looks like a file request with an extension)
        if !path.contains('.') {
            get_asset("index.html")
        } else {
            None
        }
    });

    match file {
        Some(content) => {
            // Determine content type from file extension
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime.as_ref())
                .body(Body::from(content.data.into_owned()))
                .unwrap()
        }
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("Not found"))
            .unwrap(),
    }
}
