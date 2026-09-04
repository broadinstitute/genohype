use super::assets::{viewer_asset, viewer_index};
use super::security::security_headers;
use axum::body::{Body, Bytes};
use axum::extract::{OriginalUri, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpListener;
use uuid::Uuid;

const NO_STORE: &str = "no-store";
const IMMUTABLE: &str = "public, max-age=31536000, immutable";

#[derive(Clone)]
pub(crate) struct ServerState {
    pub(crate) token: Arc<str>,
    pub(crate) manifest: Bytes,
    pub(crate) raster: Bytes,
}

impl ServerState {
    pub(crate) fn new(token: String, manifest: Vec<u8>, raster: Vec<u8>) -> Self {
        Self {
            token: token.into(),
            manifest: Bytes::from(manifest),
            raster: Bytes::from(raster),
        }
    }
}

pub(crate) fn generate_url_token() -> String {
    Uuid::new_v4().to_string()
}

pub(crate) fn build_router(state: ServerState) -> Router {
    let root = format!("/{}/", state.token);
    let assets = format!("/{}/assets/*path", state.token);
    let manifest = format!("/{}/api/manifest", state.token);
    let raster = format!("/{}/api/raster", state.token);

    Router::new()
        .route(&root, get(index))
        .route(&assets, get(asset))
        .route(&manifest, get(manifest_response))
        .route(&raster, get(raster_response))
        .fallback(not_found)
        .layer(middleware::from_fn(security_headers))
        .with_state(state)
}

pub(crate) async fn bind_loopback(port: u16) -> io::Result<TcpListener> {
    TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)).await
}

pub(crate) fn viewer_url(address: SocketAddr, token: &str) -> io::Result<String> {
    if address.ip() != IpAddr::V4(Ipv4Addr::LOCALHOST) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "association viewer must use the IPv4 loopback address",
        ));
    }
    Ok(format!("http://127.0.0.1:{}/{token}/", address.port()))
}

pub(crate) async fn serve_with_shutdown<F>(
    listener: TcpListener,
    app: Router,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
}

pub(crate) async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        eprintln!("Unable to install Ctrl-C handler: {error}");
    }
}

async fn index() -> Response {
    match viewer_index() {
        Some(asset) => response(
            StatusCode::OK,
            "text/html; charset=utf-8",
            NO_STORE,
            asset.bytes,
        ),
        None => not_found().await,
    }
}

async fn asset(State(state): State<ServerState>, OriginalUri(uri): OriginalUri) -> Response {
    let prefix = format!("/{}/assets/", state.token);
    let Some(path) = uri.path().strip_prefix(&prefix) else {
        return not_found().await;
    };
    match viewer_asset(path) {
        Some(asset) => response(
            StatusCode::OK,
            &asset.media_type,
            if asset.immutable { IMMUTABLE } else { NO_STORE },
            asset.bytes,
        ),
        None => not_found().await,
    }
}

async fn manifest_response(State(state): State<ServerState>) -> Response {
    response(
        StatusCode::OK,
        "application/json",
        NO_STORE,
        state.manifest.clone(),
    )
}

async fn raster_response(State(state): State<ServerState>) -> Response {
    response(StatusCode::OK, "image/png", NO_STORE, state.raster.clone())
}

async fn not_found() -> Response {
    response(
        StatusCode::NOT_FOUND,
        "text/plain; charset=utf-8",
        NO_STORE,
        b"not found\n".to_vec(),
    )
}

fn response(
    status: StatusCode,
    media_type: &str,
    cache_control: &'static str,
    bytes: impl Into<Body>,
) -> Response {
    let mut response = Response::new(bytes.into());
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(media_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(cache_control),
    );
    response
}
