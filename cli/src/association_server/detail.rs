use axum::body::Bytes;
use genohype_association::{
    canonical_contig, AssociationRasterizer, AssociationRegionCountsV1,
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
use std::mem::size_of;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{watch, Mutex, OwnedSemaphorePermit, Semaphore};

use crate::association_session::CompletedAssociationSession;

const DEFAULT_CACHE_ENTRIES: usize = 32;
const DEFAULT_CACHE_BYTES: usize = 256 * 1024 * 1024;
// Two maximum-size locus pixmaps are at most about 128 MiB before encoding.
const DEFAULT_MAX_FLIGHTS: usize = 2;
const DEFAULT_FETCH_LEASE: Duration = Duration::from_secs(15);
const CACHE_MAP_OVERHEAD: usize = 256;
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
    Region(String),
    Chromosome(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum FlightKey {
    Region(RegionWorkKey),
    Chromosome(String),
}

#[derive(Debug, Clone)]
pub(crate) struct RegionBundle {
    pub(crate) manifest: Bytes,
    pub(crate) raster: Bytes,
    pub(crate) overlay: Bytes,
    pub(crate) raster_id: String,
    pub(crate) overlay_id: String,
    work_key: RegionWorkKey,
}

#[derive(Debug, Clone)]
pub(crate) struct ChromosomeProduct {
    pub(crate) raster: Bytes,
    pub(crate) manifest: Bytes,
}

#[derive(Clone)]
enum CachedValue {
    Region(Arc<RegionBundle>),
    Chromosome(Arc<ChromosomeProduct>),
}

#[derive(Clone)]
enum FlightOutcome {
    Region(Result<Arc<RegionBundle>, DetailError>),
    Chromosome(Result<Arc<ChromosomeProduct>, DetailError>),
}

#[derive(Clone)]
enum FlightState {
    Pending,
    Complete(FlightOutcome),
}

struct FetchLease {
    expires_at: Instant,
    raster_pending: bool,
    overlay_pending: bool,
}

struct CacheEntry {
    value: CachedValue,
    last_used: u64,
    byte_weight: usize,
    lease: Option<FetchLease>,
}

struct CacheState {
    entries: HashMap<CacheKey, CacheEntry>,
    flights: HashMap<FlightKey, watch::Receiver<FlightState>>,
    total_bytes: usize,
    clock: u64,
}

type Now = Arc<dyn Fn() -> Instant + Send + Sync>;

pub(crate) struct DetailCache {
    state: Mutex<CacheState>,
    admission: Arc<Semaphore>,
    max_entries: usize,
    max_bytes: usize,
    fetch_lease: Duration,
    now: Now,
}

enum FlightAdmission {
    Ready(FlightOutcome),
    Joined(watch::Receiver<FlightState>),
    Started {
        receiver: watch::Receiver<FlightState>,
        sender: watch::Sender<FlightState>,
        permit: OwnedSemaphorePermit,
    },
}

impl DetailCache {
    pub(crate) fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self::configured(
            max_entries,
            max_bytes,
            DEFAULT_MAX_FLIGHTS,
            DEFAULT_FETCH_LEASE,
            Arc::new(Instant::now),
        )
    }

    fn configured(
        max_entries: usize,
        max_bytes: usize,
        max_flights: usize,
        fetch_lease: Duration,
        now: Now,
    ) -> Self {
        Self {
            state: Mutex::new(CacheState {
                entries: HashMap::new(),
                flights: HashMap::new(),
                total_bytes: 0,
                clock: 0,
            }),
            admission: Arc::new(Semaphore::new(max_flights)),
            max_entries,
            max_bytes,
            fetch_lease,
            now,
        }
    }

    pub(crate) fn default_bounded() -> Self {
        Self::new(DEFAULT_CACHE_ENTRIES, DEFAULT_CACHE_BYTES)
    }

    async fn chromosome(&self, key: &CacheKey) -> Option<Arc<ChromosomeProduct>> {
        let mut state = self.state.lock().await;
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        let entry = state.entries.get_mut(key)?;
        entry.last_used = clock;
        match &entry.value {
            CachedValue::Chromosome(value) => Some(value.clone()),
            _ => None,
        }
    }

    async fn region_for_manifest(&self, work_key: &RegionWorkKey) -> Option<Arc<RegionBundle>> {
        let mut state = self.state.lock().await;
        let found = state
            .entries
            .iter()
            .find_map(|(key, entry)| match &entry.value {
                CachedValue::Region(value) if &value.work_key == work_key => {
                    Some((key.clone(), value.clone()))
                }
                _ => None,
            });
        let (key, value) = found?;
        Self::advertise_locked(&mut state, &key, (self.now)(), self.fetch_lease);
        Some(value)
    }

    async fn begin_flight(&self, key: FlightKey) -> Result<FlightAdmission, DetailError> {
        let mut state = self.state.lock().await;
        // Repeat the cache lookup while establishing/joining a flight under the
        // same lock. This closes the lookup-to-subscribe race without forcing
        // callers to hold a mutex while they wait.
        let ready = match &key {
            FlightKey::Chromosome(id) => {
                let cache_key = CacheKey::Chromosome(id.clone());
                let value = state
                    .entries
                    .get(&cache_key)
                    .and_then(|entry| match &entry.value {
                        CachedValue::Chromosome(value) => Some(value.clone()),
                        _ => None,
                    });
                if value.is_some() {
                    state.clock = state.clock.wrapping_add(1);
                    let clock = state.clock;
                    if let Some(entry) = state.entries.get_mut(&cache_key) {
                        entry.last_used = clock;
                    }
                }
                value.map(|value| FlightOutcome::Chromosome(Ok(value)))
            }
            FlightKey::Region(work_key) => {
                let found =
                    state
                        .entries
                        .iter()
                        .find_map(|(cache_key, entry)| match &entry.value {
                            CachedValue::Region(value) if &value.work_key == work_key => {
                                Some((cache_key.clone(), value.clone()))
                            }
                            _ => None,
                        });
                found.map(|(cache_key, value)| {
                    Self::advertise_locked(&mut state, &cache_key, (self.now)(), self.fetch_lease);
                    FlightOutcome::Region(Ok(value))
                })
            }
        };
        if let Some(ready) = ready {
            return Ok(FlightAdmission::Ready(ready));
        }
        if let Some(receiver) = state.flights.get(&key) {
            return Ok(FlightAdmission::Joined(receiver.clone()));
        }
        Self::expire_leases(&mut state, (self.now)());
        let only_leased_entries =
            !state.entries.is_empty() && state.entries.values().all(|entry| entry.lease.is_some());
        if only_leased_entries
            && (state.entries.len() >= self.max_entries || state.total_bytes >= self.max_bytes)
        {
            // Do not start work that cannot occupy the cache without breaking
            // resources already advertised by successful manifests.
            return Err(DetailError::Busy);
        }
        // Admission is intentionally non-queuing: this also bounds the flight map.
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| DetailError::Busy)?;
        let (sender, receiver) = watch::channel(FlightState::Pending);
        state.flights.insert(key, receiver.clone());
        Ok(FlightAdmission::Started {
            receiver,
            sender,
            permit,
        })
    }

    async fn finish_flight(
        &self,
        key: &FlightKey,
        sender: watch::Sender<FlightState>,
        outcome: FlightOutcome,
        permit: OwnedSemaphorePermit,
    ) {
        let mut state = self.state.lock().await;
        // watch::Sender retains the latest value, so a waiter cannot miss this
        // transition even when completion occurs before its first poll.
        sender.send_replace(FlightState::Complete(outcome));
        state.flights.remove(key);
        // Release admission while still holding the state lock. No caller can
        // observe a removed flight before its capacity is reusable, and no new
        // record can transiently push the map above the permit bound.
        drop(permit);
    }

    async fn publish_chromosome(
        &self,
        key: CacheKey,
        result: Result<Arc<ChromosomeProduct>, DetailError>,
    ) -> Result<Arc<ChromosomeProduct>, DetailError> {
        let value = result?;
        let weight = chromosome_weight(&key, &value)?;
        self.insert(key, CachedValue::Chromosome(value.clone()), weight, false)
            .await?;
        Ok(value)
    }

    async fn publish_region(
        &self,
        value: Result<Arc<RegionBundle>, DetailError>,
    ) -> Result<Arc<RegionBundle>, DetailError> {
        let value = value?;
        let key = CacheKey::Region(value.overlay_id.clone());
        let weight = region_weight(&key, &value)?;
        self.insert(key, CachedValue::Region(value.clone()), weight, true)
            .await?;
        Ok(value)
    }

    async fn advertise_region(
        &self,
        value: Arc<RegionBundle>,
    ) -> Result<Arc<RegionBundle>, DetailError> {
        let key = CacheKey::Region(value.overlay_id.clone());
        let mut state = self.state.lock().await;
        let now = (self.now)();
        if state.entries.contains_key(&key) {
            Self::advertise_locked(&mut state, &key, now, self.fetch_lease);
            return Ok(value);
        }
        drop(state);
        let weight = region_weight(&key, &value)?;
        self.insert(
            key.clone(),
            CachedValue::Region(value.clone()),
            weight,
            true,
        )
        .await?;
        Ok(value)
    }

    fn advertise_locked(
        state: &mut CacheState,
        key: &CacheKey,
        now: Instant,
        lease_duration: Duration,
    ) {
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        if let Some(entry) = state.entries.get_mut(key) {
            entry.last_used = clock;
            if matches!(entry.value, CachedValue::Region(_)) {
                entry.lease = Some(FetchLease {
                    expires_at: now + lease_duration,
                    raster_pending: true,
                    overlay_pending: true,
                });
            }
        }
    }

    async fn insert(
        &self,
        key: CacheKey,
        value: CachedValue,
        weight: usize,
        lease: bool,
    ) -> Result<(), DetailError> {
        if self.max_entries == 0 || weight > self.max_bytes {
            return Err(DetailError::TooLarge);
        }
        let mut state = self.state.lock().await;
        let now = (self.now)();
        Self::expire_leases(&mut state, now);

        let old_weight = state.entries.get(&key).map_or(0, |entry| entry.byte_weight);
        let base_entries = state.entries.len() - usize::from(old_weight != 0);
        let base_bytes = state
            .total_bytes
            .checked_sub(old_weight)
            .ok_or(DetailError::Internal)?;
        let mut projected_entries = base_entries.checked_add(1).ok_or(DetailError::TooLarge)?;
        let mut projected_bytes = base_bytes
            .checked_add(weight)
            .ok_or(DetailError::TooLarge)?;

        let mut candidates: Vec<_> = state
            .entries
            .iter()
            .filter(|(candidate, entry)| **candidate != key && entry.lease.is_none())
            .map(|(candidate, entry)| (candidate.clone(), entry.last_used, entry.byte_weight))
            .collect();
        candidates.sort_by_key(|(_, last_used, _)| *last_used);
        let mut victims = Vec::new();
        for (candidate, _, candidate_weight) in candidates {
            if projected_entries <= self.max_entries && projected_bytes <= self.max_bytes {
                break;
            }
            projected_entries -= 1;
            projected_bytes = projected_bytes
                .checked_sub(candidate_weight)
                .ok_or(DetailError::Internal)?;
            victims.push(candidate);
        }
        if projected_entries > self.max_entries || projected_bytes > self.max_bytes {
            return Err(DetailError::Busy);
        }

        for victim in victims {
            if let Some(removed) = state.entries.remove(&victim) {
                state.total_bytes = state
                    .total_bytes
                    .checked_sub(removed.byte_weight)
                    .ok_or(DetailError::Internal)?;
            }
        }
        if let Some(previous) = state.entries.remove(&key) {
            state.total_bytes = state
                .total_bytes
                .checked_sub(previous.byte_weight)
                .ok_or(DetailError::Internal)?;
        }
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        state.total_bytes = state
            .total_bytes
            .checked_add(weight)
            .ok_or(DetailError::TooLarge)?;
        state.entries.insert(
            key,
            CacheEntry {
                value,
                last_used: clock,
                byte_weight: weight,
                lease: lease.then(|| FetchLease {
                    expires_at: now + self.fetch_lease,
                    raster_pending: true,
                    overlay_pending: true,
                }),
            },
        );
        Ok(())
    }

    fn expire_leases(state: &mut CacheState, now: Instant) {
        for entry in state.entries.values_mut() {
            if entry
                .lease
                .as_ref()
                .is_some_and(|lease| lease.expires_at <= now)
            {
                entry.lease = None;
            }
        }
    }

    pub(crate) async fn region_raster(&self, id: &str) -> Option<Bytes> {
        let mut state = self.state.lock().await;
        Self::expire_leases(&mut state, (self.now)());
        let keys: Vec<_> = state
            .entries
            .iter()
            .filter_map(|(key, entry)| match &entry.value {
                CachedValue::Region(value) if value.raster_id == id => Some(key.clone()),
                _ => None,
            })
            .collect();
        let first = keys.first()?.clone();
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        let bytes = match &state.entries.get(&first)?.value {
            CachedValue::Region(value) => value.raster.clone(),
            _ => return None,
        };
        for key in keys {
            if let Some(entry) = state.entries.get_mut(&key) {
                entry.last_used = clock;
                if let Some(lease) = entry.lease.as_mut() {
                    lease.raster_pending = false;
                    if !lease.overlay_pending {
                        entry.lease = None;
                    }
                }
            }
        }
        Some(bytes)
    }

    pub(crate) async fn region_overlay(&self, render_id: &str, overlay_id: &str) -> Option<Bytes> {
        let key = CacheKey::Region(overlay_id.to_string());
        let mut state = self.state.lock().await;
        Self::expire_leases(&mut state, (self.now)());
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        let entry = state.entries.get_mut(&key)?;
        let bytes = match &entry.value {
            CachedValue::Region(value) if value.raster_id == render_id => value.overlay.clone(),
            _ => return None,
        };
        entry.last_used = clock;
        if let Some(lease) = entry.lease.as_mut() {
            lease.overlay_pending = false;
            if !lease.raster_pending {
                entry.lease = None;
            }
        }
        Some(bytes)
    }

    #[cfg(test)]
    pub(crate) async fn stats(&self) -> (usize, usize, usize) {
        let state = self.state.lock().await;
        (state.entries.len(), state.total_bytes, state.flights.len())
    }
}

fn checked_weight(parts: impl IntoIterator<Item = usize>) -> Result<usize, DetailError> {
    parts
        .into_iter()
        .try_fold(CACHE_MAP_OVERHEAD, |total, part| total.checked_add(part))
        .ok_or(DetailError::TooLarge)
}

fn chromosome_weight(key: &CacheKey, value: &ChromosomeProduct) -> Result<usize, DetailError> {
    let key_bytes = match key {
        CacheKey::Chromosome(id) => id.capacity(),
        CacheKey::Region(_) => return Err(DetailError::Internal),
    };
    checked_weight([
        size_of::<CacheEntry>(),
        size_of::<CacheKey>(),
        size_of::<ChromosomeProduct>(),
        key_bytes,
        value.manifest.len(),
        value.raster.len(),
    ])
}

fn region_weight(key: &CacheKey, value: &RegionBundle) -> Result<usize, DetailError> {
    let map_key_bytes = match key {
        CacheKey::Region(id) => id.capacity(),
        CacheKey::Chromosome(_) => return Err(DetailError::Internal),
    };
    checked_weight([
        size_of::<CacheEntry>(),
        size_of::<CacheKey>(),
        size_of::<RegionBundle>(),
        map_key_bytes,
        value.raster_id.capacity(),
        value.overlay_id.capacity(),
        value.work_key.render_hash.capacity(),
        value.manifest.len(),
        value.raster.len(),
        value.overlay.len(),
    ])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DetailError {
    NotFound,
    BadRequest,
    TooDense,
    TooLarge,
    Busy,
    Internal,
}

#[cfg(test)]
pub(crate) struct BuildProbe {
    pub(crate) starts: AtomicUsize,
    pub(crate) active: AtomicUsize,
    pub(crate) peak: AtomicUsize,
    delay: Duration,
    failures: AtomicUsize,
    panics: AtomicUsize,
}

#[cfg(test)]
impl BuildProbe {
    pub(crate) fn new(delay: Duration, failures: usize, panics: usize) -> Arc<Self> {
        Arc::new(Self {
            starts: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            delay,
            failures: AtomicUsize::new(failures),
            panics: AtomicUsize::new(panics),
        })
    }

    fn enter(self: &Arc<Self>) -> Result<BuildProbeGuard, DetailError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let guard = BuildProbeGuard(self.clone());
        std::thread::sleep(self.delay);
        if consume_one(&self.panics) {
            panic!("injected detached detail build panic");
        }
        if consume_one(&self.failures) {
            return Err(DetailError::Internal);
        }
        Ok(guard)
    }
}

#[cfg(test)]
fn consume_one(value: &AtomicUsize) -> bool {
    value
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            current.checked_sub(1)
        })
        .is_ok()
}

#[cfg(test)]
struct BuildProbeGuard(Arc<BuildProbe>);

#[cfg(test)]
impl Drop for BuildProbeGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

pub(crate) struct DetailService {
    pub(crate) dataset_id: DatasetIdV1,
    database_path: Arc<PathBuf>,
    root: Arc<AssociationVisualizationManifestV1>,
    gene_provider: Option<Arc<dyn GeneReferenceProvider>>,
    pub(crate) cache: Arc<DetailCache>,
    // Detached producers must retain the completed generation and its locks.
    session: Option<CompletedAssociationSession>,
    #[cfg(test)]
    build_probe: Option<Arc<BuildProbe>>,
}

impl Clone for DetailService {
    fn clone(&self) -> Self {
        Self {
            dataset_id: self.dataset_id.clone(),
            database_path: self.database_path.clone(),
            root: self.root.clone(),
            gene_provider: self.gene_provider.clone(),
            cache: self.cache.clone(),
            session: self.session.clone(),
            #[cfg(test)]
            build_probe: self.build_probe.clone(),
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
            session: None,
            #[cfg(test)]
            build_probe: None,
        }
    }

    pub(crate) fn with_session(mut self, session: CompletedAssociationSession) -> Self {
        self.session = Some(session);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_gene_provider(mut self, provider: Arc<dyn GeneReferenceProvider>) -> Self {
        self.gene_provider = Some(provider);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_build_probe(mut self, probe: Arc<BuildProbe>) -> Self {
        self.build_probe = Some(probe);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_cache_limits(mut self, entries: usize, bytes: usize) -> Self {
        self.cache = Arc::new(DetailCache::new(entries, bytes));
        self
    }

    #[cfg(test)]
    pub(crate) fn with_cache_config(
        mut self,
        entries: usize,
        bytes: usize,
        flights: usize,
        lease: Duration,
        now: Now,
    ) -> Self {
        self.cache = Arc::new(DetailCache::configured(entries, bytes, flights, lease, now));
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
        if let Some(value) = self.cache.chromosome(&cache_key).await {
            return Ok(value);
        }
        let flight_key = FlightKey::Chromosome(cache_id);
        let owned_contig = contig.to_string();
        let receiver = match self.cache.begin_flight(flight_key.clone()).await? {
            FlightAdmission::Ready(FlightOutcome::Chromosome(result)) => return result,
            FlightAdmission::Ready(_) => return Err(DetailError::Internal),
            FlightAdmission::Joined(receiver) => receiver,
            FlightAdmission::Started {
                receiver,
                sender,
                permit,
            } => {
                let service = self.clone();
                let cache_key = cache_key.clone();
                tokio::spawn(async move {
                    let result = service.build_chromosome(owned_contig, contig_length).await;
                    let result = service.cache.publish_chromosome(cache_key, result).await;
                    service
                        .cache
                        .finish_flight(
                            &flight_key,
                            sender,
                            FlightOutcome::Chromosome(result),
                            permit,
                        )
                        .await;
                });
                receiver
            }
        };
        match wait_for_flight(receiver).await? {
            FlightOutcome::Chromosome(result) => result,
            _ => Err(DetailError::Internal),
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
        #[cfg(test)]
        let probe = self.build_probe.clone();
        run_blocking(move || {
            #[cfg(test)]
            let _probe_guard = match probe {
                Some(probe) => Some(probe.enter()?),
                None => None,
            };
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
            let manifest = serde_json::to_vec(&manifest).map_err(|_| DetailError::Internal)?;
            Ok(Arc::new(ChromosomeProduct {
                raster: exact_bytes(png),
                manifest: exact_bytes(manifest),
            }))
        })
        .await
    }

    pub(crate) async fn region(
        &self,
        request: RegionRequest,
    ) -> Result<Arc<RegionBundle>, DetailError> {
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

        if let Some(value) = self.cache.region_for_manifest(&work_key).await {
            return Ok(value);
        }
        let flight_key = FlightKey::Region(work_key.clone());
        let receiver = match self.cache.begin_flight(flight_key.clone()).await? {
            FlightAdmission::Ready(FlightOutcome::Region(result)) => {
                return self.cache.advertise_region(result?).await;
            }
            FlightAdmission::Ready(_) => return Err(DetailError::Internal),
            FlightAdmission::Joined(receiver) => receiver,
            FlightAdmission::Started {
                receiver,
                sender,
                permit,
            } => {
                let service = self.clone();
                tokio::spawn(async move {
                    let result = service.build_region(identity, raster_id, work_key).await;
                    let result = service.cache.publish_region(result).await;
                    service
                        .cache
                        .finish_flight(&flight_key, sender, FlightOutcome::Region(result), permit)
                        .await;
                });
                receiver
            }
        };
        let value = match wait_for_flight(receiver).await? {
            FlightOutcome::Region(result) => result?,
            _ => return Err(DetailError::Internal),
        };
        // Every successful manifest response refreshes or restores its atomic
        // bundle before advertising query-free resource URLs.
        self.cache.advertise_region(value).await
    }

    async fn build_region(
        &self,
        identity: RegionRenderIdentityV1,
        raster_id: String,
        work_key: RegionWorkKey,
    ) -> Result<Arc<RegionBundle>, DetailError> {
        let path = self.database_path.as_ref().clone();
        let dataset_id = self.dataset_id.clone();
        let provider = self.gene_provider.clone();
        let assembly = self.root.assembly.clone();
        let title = self.root.title.clone();
        let source_fingerprint = self.root.source_fingerprint.clone();
        #[cfg(test)]
        let probe = self.build_probe.clone();
        run_blocking(move || {
            #[cfg(test)]
            let _probe_guard = match probe {
                Some(probe) => Some(probe.enter()?),
                None => None,
            };
            let selected = work_key
                .selected_ordinal
                .into_iter()
                .collect::<HashSet<_>>();
            let gene_annotations = load_genes(provider.as_deref(), &assembly, identity.layout());
            let mut reducer = RegionOverlayReducerV1::new(
                identity.layout(),
                identity.significance_threshold(),
                &selected,
            )
            .map_err(contract_detail_error)?;
            let mut raster = LocusRasterizer::from_layout(identity.layout(), Background::White)
                .map_err(|_| DetailError::Internal)?;
            let scan_request = AssociationRegionScanRequestV1 {
                dataset_id: dataset_id.clone(),
                contig: identity.layout().contig.clone(),
                start: identity.layout().effective_start,
                end: identity.layout().effective_end,
            };
            let store = SqliteAssociationStore::open_read_only(path).map_err(store_detail_error)?;
            let mut contract_error = None;
            let scan = store.scan_region(&scan_request, &mut |point| {
                raster
                    .render_point(identity.layout(), point)
                    .map_err(|_| AssociationStoreError::Sink("render failed".into()))?;
                if let Err(error) = reducer.push(point) {
                    contract_error = Some(error);
                    return Err(AssociationStoreError::Sink(
                        "overlay reduction failed".into(),
                    ));
                }
                Ok(())
            });
            if let Some(error) = contract_error {
                return Err(contract_detail_error(error));
            }
            scan.map_err(store_detail_error)?;

            raster
                .render_threshold(identity.layout(), identity.significance_threshold())
                .map_err(|_| DetailError::Internal)?;
            let raster_bytes = raster.png_bytes().map_err(|_| DetailError::Internal)?;
            let raster_checksum = checksum(&raster_bytes);
            let overlay = reducer
                .finish(&identity, gene_annotations)
                .map_err(contract_detail_error)?;
            overlay.validate().map_err(contract_detail_error)?;
            let overlay_bytes = serde_json::to_vec(&overlay).map_err(|_| DetailError::Internal)?;
            let overlay_id = overlay_resource_id(identity.render_hash(), &overlay.overlay_hash);
            let counts = AssociationRegionCountsV1 {
                source_point_count: overlay.aggregation.input_point_count,
                rasterized_point_count: overlay.aggregation.rasterized_point_count,
                significant_point_count: overlay.aggregation.protected_significant_count,
                coding_point_count: overlay.aggregation.protected_coding_count,
                selected_point_count: overlay.aggregation.protected_selected_count,
                overlay_point_count: overlay.aggregation.overlay_point_count,
            };
            let manifest = AssociationRegionRenderManifestV1 {
                schema_version: genohype_association::ASSOCIATION_REGION_SCHEMA_VERSION_V1.into(),
                dataset_id,
                title,
                assembly,
                coordinate_convention: COORDINATE_CONVENTION_V1.into(),
                source_fingerprint,
                layout: identity.layout().clone(),
                layout_hash: identity.layout_hash().into(),
                background_id: BACKGROUND_ID.into(),
                raster_hash: identity.raster_hash().into(),
                render_hash: identity.render_hash().into(),
                overlay_hash: overlay.overlay_hash.clone(),
                significance_threshold: identity.significance_threshold(),
                counts,
                aggregation: overlay.aggregation.clone(),
                resources: AssociationRegionResourcesV1 {
                    raster: ResourceDescriptorV1 {
                        url: format!("renders/{raster_id}/raster"),
                        media_type: "image/png".into(),
                        byte_size: Some(raster_bytes.len() as u64),
                        checksum: Some(raster_checksum),
                        expires_at: None,
                    },
                    overlay: ResourceDescriptorV1 {
                        url: format!("renders/{raster_id}/overlays/{overlay_id}"),
                        media_type: "application/json".into(),
                        byte_size: Some(overlay_bytes.len() as u64),
                        checksum: Some(checksum(&overlay_bytes)),
                        expires_at: None,
                    },
                },
            };
            manifest.validate().map_err(contract_detail_error)?;
            overlay
                .validate_against(&manifest)
                .map_err(contract_detail_error)?;
            let manifest = serde_json::to_vec(&manifest).map_err(|_| DetailError::Internal)?;
            Ok(Arc::new(RegionBundle {
                manifest: exact_bytes(manifest),
                raster: exact_bytes(raster_bytes),
                overlay: exact_bytes(overlay_bytes),
                raster_id,
                overlay_id,
                work_key,
            }))
        })
        .await
    }
}

async fn wait_for_flight(
    mut receiver: watch::Receiver<FlightState>,
) -> Result<FlightOutcome, DetailError> {
    loop {
        let state = receiver.borrow().clone();
        if let FlightState::Complete(outcome) = state {
            return Ok(outcome);
        }
        receiver
            .changed()
            .await
            .map_err(|_| DetailError::Internal)?;
    }
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

fn exact_bytes(bytes: Vec<u8>) -> Bytes {
    Bytes::from(bytes.into_boxed_slice())
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

#[cfg(test)]
mod detail_tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    fn bundle(id: char, bytes: usize) -> Arc<RegionBundle> {
        let id = id.to_string().repeat(64);
        Arc::new(RegionBundle {
            manifest: Bytes::from(vec![1; bytes]),
            raster: Bytes::from(vec![2; bytes]),
            overlay: Bytes::from(vec![3; bytes]),
            raster_id: id.clone(),
            overlay_id: id.clone(),
            work_key: RegionWorkKey {
                render_hash: id,
                selected_ordinal: None,
            },
        })
    }

    #[tokio::test]
    async fn durable_completion_precedes_wait_registration() {
        let cache = DetailCache::new(1, 1024 * 1024);
        let key = FlightKey::Chromosome("x".into());
        let FlightAdmission::Started {
            receiver,
            sender,
            permit,
        } = cache.begin_flight(key.clone()).await.unwrap()
        else {
            panic!("expected owner")
        };
        let product = Arc::new(ChromosomeProduct {
            raster: Bytes::from_static(b"r"),
            manifest: Bytes::from_static(b"m"),
        });
        cache
            .finish_flight(
                &key,
                sender,
                FlightOutcome::Chromosome(Ok(product.clone())),
                permit,
            )
            .await;
        let outcome = tokio::time::timeout(Duration::from_millis(50), wait_for_flight(receiver))
            .await
            .expect("durable completion was missed")
            .unwrap();
        assert!(matches!(outcome, FlightOutcome::Chromosome(Ok(_))));
        assert_eq!(cache.stats().await.2, 0);
    }

    #[tokio::test]
    async fn atomic_bundle_lease_releases_after_both_fetches_and_expires() {
        let origin = Instant::now();
        let now = Arc::new(StdMutex::new(origin));
        let clock: Now = {
            let now = now.clone();
            Arc::new(move || *now.lock().unwrap())
        };
        let cache = DetailCache::configured(1, 1024 * 1024, 1, Duration::from_secs(5), clock);
        let first = bundle('a', 16);
        cache.publish_region(Ok(first.clone())).await.unwrap();
        assert!(cache
            .region_overlay(&"f".repeat(64), &first.overlay_id)
            .await
            .is_none());
        assert!(matches!(
            cache.publish_region(Ok(bundle('b', 16))).await,
            Err(DetailError::Busy)
        ));
        assert!(cache
            .region_overlay(&first.raster_id, &first.overlay_id)
            .await
            .is_some());
        assert!(matches!(
            cache.publish_region(Ok(bundle('b', 16))).await,
            Err(DetailError::Busy)
        ));
        assert!(cache.region_raster(&first.raster_id).await.is_some());
        cache.publish_region(Ok(bundle('b', 16))).await.unwrap();

        let second = bundle('c', 16);
        assert!(matches!(
            cache.publish_region(Ok(second)).await,
            Err(DetailError::Busy)
        ));
        *now.lock().unwrap() = origin + Duration::from_secs(6);
        cache.publish_region(Ok(bundle('c', 16))).await.unwrap();
    }

    #[tokio::test]
    async fn weights_are_conservative_and_oversized_products_are_rejected() {
        let value = bundle('d', 100);
        let key = CacheKey::Region(value.overlay_id.clone());
        let weight = region_weight(&key, &value).unwrap();
        assert!(weight > value.manifest.len() + value.raster.len() + value.overlay.len());
        let cache = DetailCache::configured(
            1,
            weight - 1,
            1,
            Duration::from_secs(1),
            Arc::new(Instant::now),
        );
        assert!(matches!(
            cache.publish_region(Ok(value)).await,
            Err(DetailError::TooLarge)
        ));
        assert_eq!(cache.stats().await, (0, 0, 0));
    }
}
