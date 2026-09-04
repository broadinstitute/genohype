use axum::body::Bytes;
use genohype_association::{
    canonical_contig, AssociationRasterizer, AssociationRegionCountsV1, AssociationRegionOverlayV1,
    AssociationRegionRenderManifestV1, AssociationRegionResourcesV1,
    AssociationRegionScanRequestV1, AssociationStore, AssociationStoreError,
    AssociationVisualizationManifestV1, AssociationVisualizationResourcesV1, Background,
    DatasetIdV1, GeneAnnotationStateV1, GeneAnnotationsV1, GeneReferenceProvider,
    GeneRegionRequestV1, GenomeLayoutV1, LocusRasterizer, PlotKindV1, RegionContractError,
    RegionLayoutV1, RegionOverlayReducerV1, RegionRenderIdentityV1, ResourceDescriptorV1,
    SqliteAssociationStore, TopK, COORDINATE_CONVENTION_V1, JSON_SAFE_INTEGER_MAX,
    SCHEMA_VERSION_V1,
};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

const DEFAULT_CACHE_ENTRIES: usize = 32;
const DEFAULT_CACHE_BYTES: usize = 256 * 1024 * 1024;
const BACKGROUND_ID: &str = "white";
const POINT_RADIUS: f32 = 2.5;
const POINT_ALPHA: u8 = 178;
const THRESHOLD_COLOR: &str = "#B71C1C";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RegionWorkKey {
    render_hash: String,
    selected_ordinal: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CacheKey {
    RegionRaster(String),
    RegionOverlay(String),
    Chromosome(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum FlightKey {
    RegionRaster(String),
    RegionWork(RegionWorkKey),
    Chromosome(String),
}

#[derive(Clone)]
pub(crate) struct RegionRasterProduct {
    pub(crate) bytes: Bytes,
    pub(crate) checksum: String,
}

#[derive(Clone)]
pub(crate) struct RegionOverlayProduct {
    pub(crate) overlay: AssociationRegionOverlayV1,
    pub(crate) bytes: Bytes,
    pub(crate) resource_id: String,
    work_key: RegionWorkKey,
}

#[derive(Clone)]
pub(crate) struct ChromosomeProduct {
    pub(crate) raster: Bytes,
    pub(crate) manifest: AssociationVisualizationManifestV1,
}

#[derive(Clone)]
enum CachedValue {
    RegionRaster(Arc<RegionRasterProduct>),
    RegionOverlay(Arc<RegionOverlayProduct>),
    Chromosome(Arc<ChromosomeProduct>),
}

impl CachedValue {
    fn byte_weight(&self) -> usize {
        match self {
            Self::RegionRaster(value) => value.bytes.len(),
            Self::RegionOverlay(value) => value.bytes.len(),
            Self::Chromosome(value) => value.raster.len(),
        }
    }
}

struct CacheEntry {
    value: CachedValue,
    last_used: u64,
}

struct CacheState {
    entries: HashMap<CacheKey, CacheEntry>,
    flights: HashMap<FlightKey, Arc<Notify>>,
    total_bytes: usize,
    clock: u64,
}

pub(crate) struct DetailCache {
    state: Mutex<CacheState>,
    max_entries: usize,
    max_bytes: usize,
}

impl DetailCache {
    pub(crate) fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            state: Mutex::new(CacheState {
                entries: HashMap::new(),
                flights: HashMap::new(),
                total_bytes: 0,
                clock: 0,
            }),
            max_entries,
            max_bytes,
        }
    }

    pub(crate) fn default_bounded() -> Self {
        Self::new(DEFAULT_CACHE_ENTRIES, DEFAULT_CACHE_BYTES)
    }

    async fn get(&self, key: &CacheKey) -> Option<CachedValue> {
        let mut state = self.state.lock().await;
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        let entry = state.entries.get_mut(key)?;
        entry.last_used = clock;
        Some(entry.value.clone())
    }

    async fn matching_overlay(
        &self,
        work_key: &RegionWorkKey,
    ) -> Option<Arc<RegionOverlayProduct>> {
        let mut state = self.state.lock().await;
        let found = state
            .entries
            .iter()
            .find_map(|(key, entry)| match &entry.value {
                CachedValue::RegionOverlay(value) if &value.work_key == work_key => {
                    Some((key.clone(), value.clone()))
                }
                _ => None,
            });
        let (key, value) = found?;
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        if let Some(entry) = state.entries.get_mut(&key) {
            entry.last_used = clock;
        }
        Some(value)
    }

    async fn begin_flight(&self, key: FlightKey) -> Result<(), Arc<Notify>> {
        let mut state = self.state.lock().await;
        if let Some(notify) = state.flights.get(&key) {
            return Err(notify.clone());
        }
        state.flights.insert(key, Arc::new(Notify::new()));
        Ok(())
    }

    async fn finish_flight(&self, key: &FlightKey) {
        let notify = self.state.lock().await.flights.remove(key);
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }

    async fn insert(&self, key: CacheKey, value: CachedValue) {
        let weight = value.byte_weight();
        let mut state = self.state.lock().await;
        if let Some(previous) = state.entries.remove(&key) {
            state.total_bytes = state
                .total_bytes
                .saturating_sub(previous.value.byte_weight());
        }
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        state.entries.insert(
            key,
            CacheEntry {
                value,
                last_used: clock,
            },
        );
        state.total_bytes = state.total_bytes.saturating_add(weight);
        while state.entries.len() > self.max_entries || state.total_bytes > self.max_bytes {
            let Some(oldest) = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            if let Some(removed) = state.entries.remove(&oldest) {
                state.total_bytes = state
                    .total_bytes
                    .saturating_sub(removed.value.byte_weight());
            }
        }
    }

    pub(crate) async fn region_raster(&self, id: &str) -> Option<Arc<RegionRasterProduct>> {
        match self.get(&CacheKey::RegionRaster(id.to_string())).await? {
            CachedValue::RegionRaster(value) => Some(value),
            _ => None,
        }
    }

    pub(crate) async fn region_overlay(&self, id: &str) -> Option<Arc<RegionOverlayProduct>> {
        match self.get(&CacheKey::RegionOverlay(id.to_string())).await? {
            CachedValue::RegionOverlay(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DetailError {
    NotFound,
    BadRequest,
    TooDense,
    Internal,
}

pub(crate) struct DetailService {
    pub(crate) dataset_id: DatasetIdV1,
    database_path: Arc<PathBuf>,
    root: Arc<AssociationVisualizationManifestV1>,
    gene_provider: Option<Arc<dyn GeneReferenceProvider>>,
    pub(crate) cache: Arc<DetailCache>,
}

impl Clone for DetailService {
    fn clone(&self) -> Self {
        Self {
            dataset_id: self.dataset_id.clone(),
            database_path: self.database_path.clone(),
            root: self.root.clone(),
            gene_provider: self.gene_provider.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl DetailService {
    pub(crate) fn new(
        dataset_id: DatasetIdV1,
        database_path: PathBuf,
        root: AssociationVisualizationManifestV1,
    ) -> Self {
        Self {
            dataset_id,
            database_path: Arc::new(database_path),
            root: Arc::new(root),
            gene_provider: None,
            cache: Arc::new(DetailCache::default_bounded()),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_gene_provider(mut self, provider: Arc<dyn GeneReferenceProvider>) -> Self {
        self.gene_provider = Some(provider);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_cache_limits(mut self, entries: usize, bytes: usize) -> Self {
        self.cache = Arc::new(DetailCache::new(entries, bytes));
        self
    }

    pub(crate) fn canonical_contig_length(&self, input: &str) -> Option<i64> {
        let canonical = canonical_contig(input)?;
        if canonical != input {
            return None;
        }
        let length = self
            .root
            .layout
            .contigs
            .iter()
            .find(|contig| contig.name == canonical)?
            .length;
        i64::try_from(length).ok()
    }

    pub(crate) fn validates_region(&self, contig: &str, start: i64, end: i64) -> bool {
        self.canonical_contig_length(contig)
            .is_some_and(|length| start >= 1 && end >= start && end <= length)
    }

    fn threshold(&self) -> f64 {
        self.root
            .thresholds
            .iter()
            .find(|threshold| threshold.name == "genome_wide_significance")
            .or_else(|| self.root.thresholds.first())
            .map_or(5e-8, |threshold| threshold.pvalue)
    }

    pub(crate) async fn chromosome(
        &self,
        contig: &str,
    ) -> Result<Arc<ChromosomeProduct>, DetailError> {
        let contig_length = self
            .canonical_contig_length(contig)
            .ok_or(DetailError::NotFound)?;
        let cache_id = chromosome_cache_id(&self.root, contig);
        let cache_key = CacheKey::Chromosome(cache_id.clone());
        loop {
            if let Some(CachedValue::Chromosome(value)) = self.cache.get(&cache_key).await {
                return Ok(value);
            }
            let flight = FlightKey::Chromosome(cache_id.clone());
            match self.cache.begin_flight(flight.clone()).await {
                Ok(()) => {
                    let result = self
                        .build_chromosome(contig.to_string(), contig_length)
                        .await;
                    if let Ok(value) = &result {
                        self.cache
                            .insert(cache_key.clone(), CachedValue::Chromosome(value.clone()))
                            .await;
                    }
                    self.cache.finish_flight(&flight).await;
                    return result;
                }
                Err(notify) => notify.notified().await,
            }
        }
    }

    async fn build_chromosome(
        &self,
        contig: String,
        contig_length: i64,
    ) -> Result<Arc<ChromosomeProduct>, DetailError> {
        let path = self.database_path.as_ref().clone();
        let dataset_id = self.dataset_id.clone();
        let root = self.root.clone();
        let threshold = self.threshold();
        run_blocking(move || {
            let layout = GenomeLayoutV1::new(
                [(contig.clone(), contig_length as u64)],
                root.layout.width,
                root.layout.height,
                0,
            )
            .map_err(|_| DetailError::Internal)?;
            let mut raster = AssociationRasterizer::from_layout(&layout, Background::White)
                .map_err(|_| DetailError::Internal)?;
            for line in &root.thresholds {
                raster
                    .render_threshold(&layout, line.pvalue, THRESHOLD_COLOR)
                    .map_err(|_| DetailError::Internal)?;
            }
            // The completed root contains exactly min(configured K, dataset rows)
            // hits, which is also a sufficient bound for every chromosome.
            let mut top_hits = TopK::new(root.top_hits.len()).map_err(|_| DetailError::Internal)?;
            let mut significant = 0_u64;
            let request = AssociationRegionScanRequestV1 {
                dataset_id: dataset_id.clone(),
                contig: contig.clone(),
                start: 1,
                end: contig_length,
            };
            let store = SqliteAssociationStore::open_read_only(path).map_err(store_detail_error)?;
            let summary = store
                .scan_region(&request, &mut |point| {
                    raster
                        .render_association_point(&layout, point, POINT_RADIUS, POINT_ALPHA)
                        .map_err(|_| AssociationStoreError::Sink("render failed".into()))?;
                    top_hits.insert_normalized(point).map_err(|_| {
                        AssociationStoreError::Sink("top hit reduction failed".into())
                    })?;
                    significant += u64::from(point.pvalue == 0.0 || point.pvalue < threshold);
                    Ok(())
                })
                .map_err(store_detail_error)?;
            let png = raster.png_bytes().map_err(|_| DetailError::Internal)?;
            let checksum = checksum(&png);
            let manifest = AssociationVisualizationManifestV1 {
                schema_version: SCHEMA_VERSION_V1.into(),
                plot_id: root.plot_id.clone(),
                kind: PlotKindV1::Manhattan,
                title: root.title.clone(),
                assembly: root.assembly.clone(),
                coordinate_convention: COORDINATE_CONVENTION_V1.into(),
                source_fingerprint: root.source_fingerprint.clone(),
                genohype_version: root.genohype_version.clone(),
                created_at: root.created_at.clone(),
                layout_hash: layout.layout_hash(&root.assembly),
                layout,
                thresholds: root.thresholds.clone(),
                row_count: summary.point_count,
                valid_row_count: summary.point_count,
                skipped_row_count: 0,
                rendered_point_count: summary.point_count,
                significant_hit_count: significant,
                top_hits: top_hits.into_sorted_vec(),
                warnings: vec![],
                resources: AssociationVisualizationResourcesV1 {
                    raster: ResourceDescriptorV1 {
                        url: "raster".into(),
                        media_type: "image/png".into(),
                        byte_size: Some(png.len() as u64),
                        checksum: Some(checksum),
                        expires_at: None,
                    },
                    region_api: None,
                    table_api: None,
                },
            };
            manifest.validate().map_err(|_| DetailError::Internal)?;
            Ok(Arc::new(ChromosomeProduct {
                raster: Bytes::from(png),
                manifest,
            }))
        })
        .await
    }

    pub(crate) async fn region(
        &self,
        request: RegionRequest,
    ) -> Result<RegionResponse, DetailError> {
        let contig_length = self
            .canonical_contig_length(&request.contig)
            .ok_or(DetailError::NotFound)?;
        if request
            .selected_ordinal
            .is_some_and(|value| value > JSON_SAFE_INTEGER_MAX)
        {
            return Err(DetailError::BadRequest);
        }
        let threshold = request.threshold.unwrap_or_else(|| self.threshold());
        let layout = RegionLayoutV1::new(
            request.contig,
            contig_length,
            request.start,
            request.end,
            request.width,
            request.height,
            request.dpr,
        )
        .map_err(|_| DetailError::BadRequest)?;
        let identity = RegionRenderIdentityV1::new(
            self.dataset_id.clone(),
            self.root.source_fingerprint.clone(),
            self.root.assembly.clone(),
            BACKGROUND_ID,
            layout,
            threshold,
        )
        .map_err(|_| DetailError::BadRequest)?;
        let raster_id = digest_id(identity.raster_hash()).ok_or(DetailError::Internal)?;
        let work_key = RegionWorkKey {
            render_hash: identity.render_hash().to_string(),
            selected_ordinal: request.selected_ordinal,
        };

        loop {
            let raster = match self
                .cache
                .get(&CacheKey::RegionRaster(raster_id.clone()))
                .await
            {
                Some(CachedValue::RegionRaster(value)) => Some(value),
                _ => None,
            };
            let overlay = self.cache.matching_overlay(&work_key).await;
            if let (Some(raster), Some(overlay)) = (raster, overlay) {
                return self.region_response(&identity, raster_id, raster, overlay);
            }
            let work_flight = FlightKey::RegionWork(work_key.clone());
            match self.cache.begin_flight(work_flight.clone()).await {
                Ok(()) => {
                    let result = self
                        .build_region_work(identity.clone(), raster_id.clone(), work_key.clone())
                        .await;
                    self.cache.finish_flight(&work_flight).await;
                    return result;
                }
                Err(notify) => notify.notified().await,
            }
        }
    }

    async fn build_region_work(
        &self,
        identity: RegionRenderIdentityV1,
        raster_id: String,
        work_key: RegionWorkKey,
    ) -> Result<RegionResponse, DetailError> {
        let existing_raster = match self
            .cache
            .get(&CacheKey::RegionRaster(raster_id.clone()))
            .await
        {
            Some(CachedValue::RegionRaster(value)) => Some(value),
            _ => None,
        };
        let existing_overlay = self.cache.matching_overlay(&work_key).await;

        let mut owns_raster_flight = false;
        let raster = if let Some(raster) = existing_raster {
            Some(raster)
        } else {
            loop {
                let flight = FlightKey::RegionRaster(raster_id.clone());
                match self.cache.begin_flight(flight).await {
                    Ok(()) => {
                        owns_raster_flight = true;
                        break None;
                    }
                    Err(notify) => {
                        notify.notified().await;
                        if let Some(CachedValue::RegionRaster(value)) = self
                            .cache
                            .get(&CacheKey::RegionRaster(raster_id.clone()))
                            .await
                        {
                            break Some(value);
                        }
                    }
                }
            }
        };

        let build_raster = raster.is_none();
        let build_overlay = existing_overlay.is_none();
        let built = if build_raster || build_overlay {
            self.build_region(
                identity.clone(),
                work_key.clone(),
                build_raster,
                build_overlay,
            )
            .await
        } else {
            unreachable!("region work was requested with both products present")
        };

        // Publish a successful raster before waking requests for a different
        // exact overlay that share this effective raster identity.
        if owns_raster_flight {
            if let Ok(value) = &built {
                if let Some(raster) = &value.raster {
                    self.cache
                        .insert(
                            CacheKey::RegionRaster(raster_id.clone()),
                            CachedValue::RegionRaster(raster.clone()),
                        )
                        .await;
                }
            }
            self.cache
                .finish_flight(&FlightKey::RegionRaster(raster_id.clone()))
                .await;
        }
        let built = built?;
        let raster = if let Some(value) = raster {
            value
        } else {
            built.raster.ok_or(DetailError::Internal)?
        };
        let overlay = if let Some(value) = existing_overlay {
            value
        } else {
            let value = built.overlay.ok_or(DetailError::Internal)?;
            self.cache
                .insert(
                    CacheKey::RegionOverlay(value.resource_id.clone()),
                    CachedValue::RegionOverlay(value.clone()),
                )
                .await;
            value
        };
        self.region_response(&identity, raster_id, raster, overlay)
    }

    async fn build_region(
        &self,
        identity: RegionRenderIdentityV1,
        work_key: RegionWorkKey,
        build_raster: bool,
        build_overlay: bool,
    ) -> Result<BuiltRegion, DetailError> {
        let path = self.database_path.as_ref().clone();
        let dataset_id = self.dataset_id.clone();
        let provider = self.gene_provider.clone();
        let assembly = self.root.assembly.clone();
        run_blocking(move || {
            let selected = work_key
                .selected_ordinal
                .into_iter()
                .collect::<HashSet<_>>();
            let gene_annotations = if build_overlay {
                load_genes(provider.as_deref(), &assembly, identity.layout())
            } else {
                unavailable_genes()
            };
            let mut reducer = if build_overlay {
                Some(
                    RegionOverlayReducerV1::new(
                        identity.layout(),
                        identity.significance_threshold(),
                        &selected,
                    )
                    .map_err(contract_detail_error)?,
                )
            } else {
                None
            };
            let mut raster = if build_raster {
                Some(
                    LocusRasterizer::from_layout(identity.layout(), Background::White)
                        .map_err(|_| DetailError::Internal)?,
                )
            } else {
                None
            };
            let scan_request = AssociationRegionScanRequestV1 {
                dataset_id,
                contig: identity.layout().contig.clone(),
                start: identity.layout().effective_start,
                end: identity.layout().effective_end,
            };
            let store = SqliteAssociationStore::open_read_only(path).map_err(store_detail_error)?;
            let mut contract_error = None;
            let scan = store.scan_region(&scan_request, &mut |point| {
                if let Some(raster) = raster.as_mut() {
                    raster
                        .render_point(identity.layout(), point)
                        .map_err(|_| AssociationStoreError::Sink("render failed".into()))?;
                }
                if let Some(reducer) = reducer.as_mut() {
                    if let Err(error) = reducer.push(point) {
                        contract_error = Some(error);
                        return Err(AssociationStoreError::Sink(
                            "overlay reduction failed".into(),
                        ));
                    }
                }
                Ok(())
            });
            if let Some(error) = contract_error {
                return Err(contract_detail_error(error));
            }
            scan.map_err(store_detail_error)?;

            let raster = if let Some(mut raster) = raster {
                raster
                    .render_threshold(identity.layout(), identity.significance_threshold())
                    .map_err(|_| DetailError::Internal)?;
                let bytes = raster.png_bytes().map_err(|_| DetailError::Internal)?;
                Some(Arc::new(RegionRasterProduct {
                    checksum: checksum(&bytes),
                    bytes: Bytes::from(bytes),
                }))
            } else {
                None
            };
            let overlay = if let Some(reducer) = reducer {
                let overlay = reducer
                    .finish(&identity, gene_annotations)
                    .map_err(contract_detail_error)?;
                overlay.validate().map_err(contract_detail_error)?;
                let bytes = serde_json::to_vec(&overlay).map_err(|_| DetailError::Internal)?;
                let resource_id =
                    overlay_resource_id(identity.render_hash(), &overlay.overlay_hash);
                Some(Arc::new(RegionOverlayProduct {
                    overlay,
                    bytes: Bytes::from(bytes),
                    resource_id,
                    work_key,
                }))
            } else {
                None
            };
            Ok(BuiltRegion { raster, overlay })
        })
        .await
    }

    fn region_response(
        &self,
        identity: &RegionRenderIdentityV1,
        raster_id: String,
        raster: Arc<RegionRasterProduct>,
        overlay: Arc<RegionOverlayProduct>,
    ) -> Result<RegionResponse, DetailError> {
        let counts = AssociationRegionCountsV1 {
            source_point_count: overlay.overlay.aggregation.input_point_count,
            rasterized_point_count: overlay.overlay.aggregation.rasterized_point_count,
            significant_point_count: overlay.overlay.aggregation.protected_significant_count,
            coding_point_count: overlay.overlay.aggregation.protected_coding_count,
            selected_point_count: overlay.overlay.aggregation.protected_selected_count,
            overlay_point_count: overlay.overlay.aggregation.overlay_point_count,
        };
        let manifest = AssociationRegionRenderManifestV1 {
            schema_version: genohype_association::ASSOCIATION_REGION_SCHEMA_VERSION_V1.into(),
            dataset_id: self.dataset_id.clone(),
            title: self.root.title.clone(),
            assembly: self.root.assembly.clone(),
            coordinate_convention: COORDINATE_CONVENTION_V1.into(),
            source_fingerprint: self.root.source_fingerprint.clone(),
            layout: identity.layout().clone(),
            layout_hash: identity.layout_hash().into(),
            background_id: BACKGROUND_ID.into(),
            raster_hash: identity.raster_hash().into(),
            render_hash: identity.render_hash().into(),
            overlay_hash: overlay.overlay.overlay_hash.clone(),
            significance_threshold: identity.significance_threshold(),
            counts,
            aggregation: overlay.overlay.aggregation.clone(),
            resources: AssociationRegionResourcesV1 {
                raster: ResourceDescriptorV1 {
                    url: format!("renders/{raster_id}/raster"),
                    media_type: "image/png".into(),
                    byte_size: Some(raster.bytes.len() as u64),
                    checksum: Some(raster.checksum.clone()),
                    expires_at: None,
                },
                overlay: ResourceDescriptorV1 {
                    url: format!("renders/{raster_id}/overlays/{}", overlay.resource_id),
                    media_type: "application/json".into(),
                    byte_size: Some(overlay.bytes.len() as u64),
                    checksum: Some(checksum(&overlay.bytes)),
                    expires_at: None,
                },
            },
        };
        manifest.validate().map_err(contract_detail_error)?;
        overlay
            .overlay
            .validate_against(&manifest)
            .map_err(contract_detail_error)?;
        Ok(RegionResponse {
            manifest,
            _raster: raster,
            _overlay: overlay,
        })
    }
}

struct BuiltRegion {
    raster: Option<Arc<RegionRasterProduct>>,
    overlay: Option<Arc<RegionOverlayProduct>>,
}

pub(crate) struct RegionResponse {
    pub(crate) manifest: AssociationRegionRenderManifestV1,
    // Retain both cache products through manifest serialization even if a
    // concurrent request causes immediate LRU pressure.
    pub(crate) _raster: Arc<RegionRasterProduct>,
    pub(crate) _overlay: Arc<RegionOverlayProduct>,
}

pub(crate) struct RegionRequest {
    pub(crate) contig: String,
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) dpr: u8,
    pub(crate) threshold: Option<f64>,
    pub(crate) selected_ordinal: Option<u64>,
}

fn unavailable_genes() -> GeneAnnotationsV1 {
    GeneAnnotationsV1 {
        state: GeneAnnotationStateV1::Unavailable,
        features: vec![],
        message: Some("gene annotations are unavailable".into()),
    }
}

fn load_genes(
    provider: Option<&dyn GeneReferenceProvider>,
    assembly: &str,
    layout: &RegionLayoutV1,
) -> GeneAnnotationsV1 {
    let Some(provider) = provider else {
        return unavailable_genes();
    };
    let request = GeneRegionRequestV1 {
        assembly: assembly.into(),
        contig: layout.contig.clone(),
        start: layout.effective_start,
        end: layout.effective_end,
    };
    match provider.genes_overlapping(&request) {
        Ok(features) => match genohype_association::normalize_gene_features(&request, features) {
            Ok(features) => GeneAnnotationsV1 {
                state: GeneAnnotationStateV1::Available,
                features,
                message: None,
            },
            Err(_) => gene_error(),
        },
        Err(_) => gene_error(),
    }
}

fn gene_error() -> GeneAnnotationsV1 {
    GeneAnnotationsV1 {
        state: GeneAnnotationStateV1::Error,
        features: vec![],
        message: Some("gene annotations could not be loaded".into()),
    }
}

fn chromosome_cache_id(root: &AssociationVisualizationManifestV1, contig: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"genohype.association.chromosome-cache.v1\0");
    digest.update(root.plot_id.as_bytes());
    digest.update(root.source_fingerprint.as_bytes());
    digest.update(root.assembly.as_bytes());
    digest.update(contig.as_bytes());
    digest.update(root.layout.width.to_be_bytes());
    digest.update(root.layout.height.to_be_bytes());
    format!("{:x}", digest.finalize())
}

fn overlay_resource_id(render_hash: &str, overlay_hash: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"genohype.association.overlay-resource.v1\0");
    digest.update(render_hash.as_bytes());
    digest.update(overlay_hash.as_bytes());
    format!("{:x}", digest.finalize())
}

fn digest_id(hash: &str) -> Option<String> {
    let digest = hash.strip_prefix("sha256:")?;
    if digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Some(digest.into())
    } else {
        None
    }
}

pub(crate) fn valid_resource_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn checksum(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn store_detail_error(error: AssociationStoreError) -> DetailError {
    match error {
        AssociationStoreError::DatasetNotFound => DetailError::NotFound,
        AssociationStoreError::InvalidRequest(_) => DetailError::BadRequest,
        _ => DetailError::Internal,
    }
}

fn contract_detail_error(error: RegionContractError) -> DetailError {
    match error {
        RegionContractError::ProtectedOverlayLimit { .. }
        | RegionContractError::OverlayTextLimit => DetailError::TooDense,
        _ => DetailError::Internal,
    }
}

async fn run_blocking<T, F>(operation: F) -> Result<T, DetailError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, DetailError> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|_| DetailError::Internal)?
}
