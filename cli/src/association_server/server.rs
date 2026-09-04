use super::assets::{viewer_asset, viewer_index};
use super::detail::{valid_resource_id, DetailError, DetailService, RegionRequest};
use super::security::security_headers;
use crate::association_session::CompletedAssociationSession;
use axum::body::{Body, Bytes};
use axum::extract::rejection::QueryRejection;
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
#[cfg(test)]
use genohype_association::GeneReferenceProvider;
use genohype_association::{
    AssociationPageV1, AssociationRegionRequestV1, AssociationSortDirectionV1, AssociationStore,
    AssociationStoreError, AssociationTablePageRequestV1, AssociationTableSortV1,
    AssociationVisualizationManifestV1, DatasetIdV1, SqliteAssociationStore, DEFAULT_PAGE_SIZE,
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
    detail: Option<DetailService>,
    // This is lifecycle state, not just metadata: retaining the final clone keeps
    // ephemeral session artifacts and their locks alive until server shutdown.
    _session: Option<CompletedAssociationSession>,
}

impl DatasetService {
    pub(crate) fn from_session(
        session: CompletedAssociationSession,
        root: AssociationVisualizationManifestV1,
    ) -> Self {
        let dataset_id = session.dataset_id().clone();
        let database_path = session.database_path();
        Self {
            detail: Some(DetailService::new(
                dataset_id.clone(),
                database_path.clone(),
                root,
            )),
            dataset_id,
            database_path: Arc::new(database_path),
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
            detail: None,
            _session: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test_with_manifest(
        dataset_id: DatasetIdV1,
        database_path: PathBuf,
        root: AssociationVisualizationManifestV1,
    ) -> Self {
        let detail = DetailService::new(dataset_id.clone(), database_path.clone(), root);
        Self {
            dataset_id,
            database_path: Arc::new(database_path),
            detail: Some(detail),
            _session: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_gene_provider(mut self, provider: Arc<dyn GeneReferenceProvider>) -> Self {
        self.detail = self
            .detail
            .map(|detail| detail.with_gene_provider(provider));
        self
    }

    #[cfg(test)]
    pub(crate) fn with_cache_limits(mut self, entries: usize, bytes: usize) -> Self {
        self.detail = self
            .detail
            .map(|detail| detail.with_cache_limits(entries, bytes));
        self
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
    let chromosome_ui = format!("/{}/chromosome/:contig", state.token);
    let locus_ui = format!("/{}/locus/:contig/:bounds", state.token);

    let mut router = Router::new()
        .route(&root, get(index))
        .route(&chromosome_ui, get(chromosome_index))
        .route(&locus_ui, get(locus_index))
        .route(&assets, get(asset))
        .route(&manifest, get(manifest_response))
        .route(&raster, get(raster_response));
    if let Some(dataset) = &state.dataset {
        let table = format!("/{}/api/datasets/{}/table", state.token, dataset.dataset_id);
        let region = format!(
            "/{}/api/datasets/{}/region",
            state.token, dataset.dataset_id
        );
        let chromosome_manifest = format!(
            "/{}/api/datasets/{}/chromosomes/:contig/manifest",
            state.token, dataset.dataset_id
        );
        let chromosome_raster = format!(
            "/{}/api/datasets/{}/chromosomes/:contig/raster",
            state.token, dataset.dataset_id
        );
        let region_manifest = format!(
            "/{}/api/datasets/{}/region/manifest",
            state.token, dataset.dataset_id
        );
        let region_raster = format!(
            "/{}/api/datasets/{}/region/renders/:render_id/raster",
            state.token, dataset.dataset_id
        );
        let region_overlay = format!(
            "/{}/api/datasets/{}/region/renders/:render_id/overlays/:overlay_id",
            state.token, dataset.dataset_id
        );
        router = router
            .route(&table, get(table_response))
            .route(&region, get(region_response))
            .route(&chromosome_manifest, get(chromosome_manifest_response))
            .route(&chromosome_raster, get(chromosome_raster_response))
            .route(&region_manifest, get(region_manifest_response))
            .route(&region_raster, get(region_raster_response))
            .route(&region_overlay, get(region_overlay_response));
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

async fn chromosome_index(
    State(state): State<ServerState>,
    Path(contig): Path<String>,
) -> Response {
    let Some(dataset) = state.dataset.as_ref() else {
        return not_found().await;
    };
    if dataset
        .detail
        .as_ref()
        .and_then(|detail| detail.canonical_contig_length(&contig))
        .is_none()
    {
        return not_found().await;
    }
    index_for_token(&state.token).await
}

async fn locus_index(
    State(state): State<ServerState>,
    Path((contig, bounds)): Path<(String, String)>,
) -> Response {
    let Some(dataset) = state.dataset.as_ref() else {
        return not_found().await;
    };
    let Some((start, end)) = parse_canonical_bounds(&bounds) else {
        return not_found().await;
    };
    if !dataset
        .detail
        .as_ref()
        .is_some_and(|detail| detail.validates_region(&contig, start, end))
    {
        return not_found().await;
    }
    index_for_token(&state.token).await
}

async fn index_for_token(token: &str) -> Response {
    match viewer_index() {
        Some(mut asset) => {
            let Ok(html) = String::from_utf8(asset.bytes) else {
                return internal_error();
            };
            let base = format!("<base href=\"/{}/\">", html_escape(token));
            let Some(head) = html.find("<head>").map(|offset| offset + "<head>".len()) else {
                return internal_error();
            };
            let mut injected = String::with_capacity(html.len() + base.len());
            injected.push_str(&html[..head]);
            injected.push_str(&base);
            injected.push_str(&html[head..]);
            asset.bytes = injected.into_bytes();
            response(
                StatusCode::OK,
                "text/html; charset=utf-8",
                NO_STORE,
                asset.bytes,
            )
        }
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegionManifestQueryV1 {
    contig: String,
    start: i64,
    end: i64,
    width: u32,
    height: u32,
    dpr: u8,
    #[serde(default)]
    threshold: Option<f64>,
    #[serde(default)]
    selected: Option<u64>,
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

async fn chromosome_manifest_response(
    State(state): State<ServerState>,
    Path(contig): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    if uri.query().is_some() {
        return bad_request();
    }
    let Some(detail) = state.dataset.and_then(|dataset| dataset.detail) else {
        return not_found().await;
    };
    match detail.chromosome(&contig).await {
        Ok(product) => json_response(&product.manifest),
        Err(error) => detail_error_response(error).await,
    }
}

async fn chromosome_raster_response(
    State(state): State<ServerState>,
    Path(contig): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    if uri.query().is_some() {
        return bad_request();
    }
    let Some(detail) = state.dataset.and_then(|dataset| dataset.detail) else {
        return not_found().await;
    };
    match detail.chromosome(&contig).await {
        Ok(product) => response(
            StatusCode::OK,
            "image/png",
            NO_STORE,
            product.raster.clone(),
        ),
        Err(error) => detail_error_response(error).await,
    }
}

async fn region_manifest_response(
    State(state): State<ServerState>,
    query: Result<Query<RegionManifestQueryV1>, QueryRejection>,
) -> Response {
    let Some(detail) = state.dataset.and_then(|dataset| dataset.detail) else {
        return not_found().await;
    };
    let Ok(Query(query)) = query else {
        return bad_request();
    };
    let request = RegionRequest {
        contig: query.contig,
        start: query.start,
        end: query.end,
        width: query.width,
        height: query.height,
        dpr: query.dpr,
        threshold: query.threshold,
        selected_ordinal: query.selected,
    };
    match detail.region(request).await {
        Ok(product) => json_response(&product.manifest),
        Err(error) => detail_error_response(error).await,
    }
}

async fn region_raster_response(
    State(state): State<ServerState>,
    Path(render_id): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    if uri.query().is_some() {
        return not_found().await;
    }
    let Some(detail) = state.dataset.and_then(|dataset| dataset.detail) else {
        return not_found().await;
    };
    if !valid_resource_id(&render_id) {
        return not_found().await;
    }
    match detail.cache.region_raster(&render_id).await {
        Some(product) => response(StatusCode::OK, "image/png", NO_STORE, product.bytes.clone()),
        None => not_found().await,
    }
}

async fn region_overlay_response(
    State(state): State<ServerState>,
    Path((render_id, overlay_id)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    if uri.query().is_some() {
        return not_found().await;
    }
    let Some(detail) = state.dataset.and_then(|dataset| dataset.detail) else {
        return not_found().await;
    };
    if !valid_resource_id(&render_id) || !valid_resource_id(&overlay_id) {
        return not_found().await;
    }
    let Some(overlay) = detail.cache.region_overlay(&overlay_id).await else {
        return not_found().await;
    };
    let expected_render = overlay
        .overlay
        .raster_hash
        .strip_prefix("sha256:")
        .unwrap_or_default();
    if expected_render != render_id {
        return not_found().await;
    }
    response(
        StatusCode::OK,
        "application/json",
        NO_STORE,
        overlay.bytes.clone(),
    )
}

fn json_response<T: serde::Serialize>(value: &T) -> Response {
    match serde_json::to_vec(value) {
        Ok(bytes) => response(StatusCode::OK, "application/json", NO_STORE, bytes),
        Err(_) => internal_error(),
    }
}

async fn detail_error_response(error: DetailError) -> Response {
    match error {
        DetailError::NotFound => not_found().await,
        DetailError::BadRequest => bad_request(),
        DetailError::TooDense => response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "text/plain; charset=utf-8",
            NO_STORE,
            b"region is too dense; narrow the interval\n".to_vec(),
        ),
        DetailError::Internal => internal_error(),
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

fn parse_canonical_bounds(value: &str) -> Option<(i64, i64)> {
    let (start, end) = value.split_once('-')?;
    let start = start.parse::<i64>().ok()?;
    let end = end.parse::<i64>().ok()?;
    if value != format!("{start}-{end}") {
        return None;
    }
    Some((start, end))
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
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
