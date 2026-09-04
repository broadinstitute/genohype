use super::assets::{viewer_asset, viewer_index};
use super::security::security_headers;
use crate::association_session::CompletedAssociationSession;
use axum::body::{Body, Bytes};
use axum::extract::rejection::QueryRejection;
use axum::extract::{OriginalUri, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use genohype_association::{
    AssociationPageV1, AssociationRegionRequestV1, AssociationSortDirectionV1, AssociationStore,
    AssociationStoreError, AssociationTablePageRequestV1, AssociationTableSortV1, DatasetIdV1,
    SqliteAssociationStore, DEFAULT_PAGE_SIZE,
};
use serde::Deserialize;
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
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
    dataset: Option<DatasetService>,
}

impl ServerState {
    pub(crate) fn new(token: String, manifest: Vec<u8>, raster: Vec<u8>) -> Self {
        Self {
            token: token.into(),
            manifest: Bytes::from(manifest),
            raster: Bytes::from(raster),
            dataset: None,
        }
    }

    pub(crate) fn with_dataset(mut self, dataset: DatasetService) -> Self {
        self.dataset = Some(dataset);
        self
    }
}

#[derive(Clone)]
pub(crate) struct DatasetService {
    dataset_id: DatasetIdV1,
    database_path: Arc<PathBuf>,
    // This is lifecycle state, not just metadata: retaining the final clone keeps
    // ephemeral session artifacts and their locks alive until server shutdown.
    _session: Option<CompletedAssociationSession>,
}

impl DatasetService {
    pub(crate) fn from_session(session: CompletedAssociationSession) -> Self {
        Self {
            dataset_id: session.dataset_id().clone(),
            database_path: Arc::new(session.database_path()),
            _session: Some(session),
        }
    }

    pub(crate) async fn validate_complete(&self) -> Result<(), AssociationStoreError> {
        let path = self.database_path.as_ref().clone();
        let dataset_id = self.dataset_id.clone();
        run_blocking(move || {
            let store = SqliteAssociationStore::open_read_only(path)?;
            if store.is_complete(&dataset_id)? {
                Ok(())
            } else {
                Err(AssociationStoreError::InvalidBuildState)
            }
        })
        .await
    }

    async fn table_page(
        &self,
        request: AssociationTablePageRequestV1,
    ) -> Result<AssociationPageV1, AssociationStoreError> {
        let path = self.database_path.as_ref().clone();
        run_blocking(move || SqliteAssociationStore::open_read_only(path)?.table_page(&request))
            .await
    }

    async fn region_page(
        &self,
        request: AssociationRegionRequestV1,
    ) -> Result<AssociationPageV1, AssociationStoreError> {
        let path = self.database_path.as_ref().clone();
        run_blocking(move || SqliteAssociationStore::open_read_only(path)?.region_page(&request))
            .await
    }

    #[cfg(test)]
    pub(crate) fn for_test(dataset_id: DatasetIdV1, database_path: PathBuf) -> Self {
        Self {
            dataset_id,
            database_path: Arc::new(database_path),
            _session: None,
        }
    }
}

async fn run_blocking<T, F>(operation: F) -> Result<T, AssociationStoreError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, AssociationStoreError> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|_| AssociationStoreError::Backend("blocking association query failed".into()))?
}

pub(crate) fn generate_url_token() -> String {
    Uuid::new_v4().to_string()
}

pub(crate) fn build_router(state: ServerState) -> Router {
    let root = format!("/{}/", state.token);
    let assets = format!("/{}/assets/*path", state.token);
    let manifest = format!("/{}/api/manifest", state.token);
    let raster = format!("/{}/api/raster", state.token);

    let mut router = Router::new()
        .route(&root, get(index))
        .route(&assets, get(asset))
        .route(&manifest, get(manifest_response))
        .route(&raster, get(raster_response));
    if let Some(dataset) = &state.dataset {
        let table = format!("/{}/api/datasets/{}/table", state.token, dataset.dataset_id);
        let region = format!(
            "/{}/api/datasets/{}/region",
            state.token, dataset.dataset_id
        );
        router = router
            .route(&table, get(table_response))
            .route(&region, get(region_response));
    }

    router
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableQueryV1 {
    #[serde(default)]
    offset: Option<u64>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    sort: Option<AssociationTableSortV1>,
    #[serde(default)]
    direction: Option<AssociationSortDirectionV1>,
    #[serde(default)]
    search: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegionQueryV1 {
    contig: String,
    start: i64,
    end: i64,
    #[serde(default)]
    offset: Option<u64>,
    #[serde(default)]
    limit: Option<u32>,
}

async fn table_response(
    State(state): State<ServerState>,
    query: Result<Query<TableQueryV1>, QueryRejection>,
) -> Response {
    let Some(dataset) = state.dataset else {
        return not_found().await;
    };
    let Ok(Query(query)) = query else {
        return bad_request();
    };
    let request = AssociationTablePageRequestV1 {
        dataset_id: dataset.dataset_id.clone(),
        offset: query.offset.unwrap_or(0),
        limit: query.limit.unwrap_or(DEFAULT_PAGE_SIZE),
        sort: query.sort.unwrap_or(AssociationTableSortV1::Pvalue),
        direction: query
            .direction
            .unwrap_or(AssociationSortDirectionV1::Ascending),
        search: query.search,
    };
    if request.validate().is_err() {
        return bad_request();
    }
    match dataset.table_page(request).await {
        Ok(page) => json_response(&page),
        Err(_) => internal_error(),
    }
}

async fn region_response(
    State(state): State<ServerState>,
    query: Result<Query<RegionQueryV1>, QueryRejection>,
) -> Response {
    let Some(dataset) = state.dataset else {
        return not_found().await;
    };
    let Ok(Query(query)) = query else {
        return bad_request();
    };
    let request = AssociationRegionRequestV1 {
        dataset_id: dataset.dataset_id.clone(),
        contig: query.contig,
        start: query.start,
        end: query.end,
        offset: query.offset.unwrap_or(0),
        limit: query.limit.unwrap_or(DEFAULT_PAGE_SIZE),
    };
    if request.validate().is_err() {
        return bad_request();
    }
    match dataset.region_page(request).await {
        Ok(page) => json_response(&page),
        Err(_) => internal_error(),
    }
}

fn json_response(page: &AssociationPageV1) -> Response {
    match serde_json::to_vec(page) {
        Ok(bytes) => response(StatusCode::OK, "application/json", NO_STORE, bytes),
        Err(_) => internal_error(),
    }
}

fn bad_request() -> Response {
    response(
        StatusCode::BAD_REQUEST,
        "text/plain; charset=utf-8",
        NO_STORE,
        b"bad request\n".to_vec(),
    )
}

fn internal_error() -> Response {
    response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "text/plain; charset=utf-8",
        NO_STORE,
        b"internal server error\n".to_vec(),
    )
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
