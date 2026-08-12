//! Cloud storage adapter that bridges async object_store with sync std::io::Read
//!
//! This module provides a `CloudReader` that fetches data in large chunks from
//! cloud storage and serves it synchronously through the `std::io::Read` trait.
//!
//! ## High-Performance Streaming Architecture
//!
//! The `PrefetchingCloudReader` implements a latency-hiding prefetch strategy:
//! - A background task on the IO runtime fetches chunks ahead of the current read position
//! - Chunks are pushed into a bounded channel, providing backpressure
//! - The CPU never waits for network I/O as data is always ready in the channel
//!
//! This approach enables streaming from cloud storage (GCS, S3) at near-network speeds
//! without requiring local disk staging.

use crate::{HailError, Result};
use futures::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{GetOptions, GetRange, ObjectMeta, ObjectStore};
use once_cell::sync::Lazy;
use sha2::Digest;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;
use tracing::{debug, trace};

use super::{ResolvedObject, SourceObject, SourceSnapshot};

/// Default chunk size for cloud reads (8MB) - used by non-prefetching reader
const DEFAULT_CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// Prefetch chunk size (32MB) - balances per-request overhead vs memory usage
const PREFETCH_CHUNK_SIZE: usize = 32 * 1024 * 1024;

/// Default concurrent in-flight fetches per reader. Adaptive — adjusted by worker
/// telemetry based on memory pressure (AIMD: halve on pressure, +1 when stable).
const DEFAULT_PREFETCH_DEPTH: usize = 4;
const MIN_PREFETCH_DEPTH: usize = 2;
const MAX_PREFETCH_DEPTH: usize = 16;

/// Global adaptive prefetch depth, shared by all readers on this worker
static PREFETCH_DEPTH: AtomicUsize = AtomicUsize::new(DEFAULT_PREFETCH_DEPTH);

/// Get the current adaptive prefetch depth
pub fn get_prefetch_depth() -> usize {
    PREFETCH_DEPTH.load(Ordering::Relaxed)
}

/// Set the adaptive prefetch depth (clamped to [MIN, MAX])
pub fn set_prefetch_depth(depth: usize) {
    PREFETCH_DEPTH.store(
        depth.clamp(MIN_PREFETCH_DEPTH, MAX_PREFETCH_DEPTH),
        Ordering::Relaxed,
    );
}

/// Global byte-weighted chunk cache for cloud IO.
///
/// Both foreground (projected) and background (unprojected) queries hitting the same
/// genomic intervals will request identical chunk ranges. This cache eliminates the
/// redundant GCS download on the second pass.
static CHUNK_CACHE: Lazy<moka::future::Cache<String, bytes::Bytes>> = Lazy::new(|| {
    moka::future::Cache::builder()
        .weigher(|_k: &String, v: &bytes::Bytes| v.len() as u32)
        .max_capacity(512 * 1024 * 1024) // 512 MB ceiling
        .build()
});

/// Shared Tokio runtime for IO operations
///
/// This runtime is used for all async cloud storage operations,
/// including prefetching reads and uploading writes.
pub(crate) static IO_RUNTIME: Lazy<Runtime> =
    Lazy::new(|| Runtime::new().expect("Failed to create IO runtime"));

/// Global cache of GCS clients by bucket name.
///
/// Clients are thread-safe and can be shared across all readers/writers.
/// This avoids repeated metadata token fetches on high-core VMs which can
/// cause rate limiting errors from the GCP metadata service.
#[cfg(feature = "gcp")]
static GCS_CLIENT_CACHE: Lazy<Mutex<HashMap<String, Arc<dyn ObjectStore>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
struct CloudObjectContext {
    canonical_store_id: String,
    canonical_uri: String,
    expected: Option<SourceObject>,
    /// Identity observed by the opening HEAD, used only to isolate legacy cache entries.
    observed_version: Option<String>,
    observed_etag: Option<String>,
}

impl CloudObjectContext {
    fn from_resolved(resolved: &ResolvedObject, meta: &ObjectMeta) -> Self {
        Self {
            canonical_store_id: resolved.canonical_store_id.clone(),
            canonical_uri: resolved.canonical_uri.clone(),
            expected: resolved.expected.clone(),
            observed_version: meta.version.clone(),
            observed_etag: meta.e_tag.clone(),
        }
    }

    fn cache_key(&self, offset: u64, len: usize) -> String {
        let version = self
            .expected
            .as_ref()
            .map(|identity| identity.version.as_str())
            .or(self.observed_version.as_deref())
            .unwrap_or("<absent>");
        let etag = self
            .expected
            .as_ref()
            .map(|identity| identity.etag.as_str())
            .or(self.observed_etag.as_deref())
            .unwrap_or("<absent>");
        format!(
            "{}|{}|{}|{}|{}|{}",
            self.canonical_store_id, self.canonical_uri, version, etag, offset, len
        )
    }
}

fn pinned_options(
    expected: Option<&SourceObject>,
    range: Option<std::ops::Range<usize>>,
    head: bool,
) -> GetOptions {
    GetOptions {
        version: expected.map(|identity| identity.version.clone()),
        if_match: expected.map(|identity| identity.etag.clone()),
        range: range.map(GetRange::Bounded),
        head,
        ..Default::default()
    }
}

fn verify_object_meta(uri: &str, expected: &SourceObject, meta: &ObjectMeta) -> Result<()> {
    if meta.version.as_deref() != Some(expected.version.as_str()) {
        return Err(HailError::InvalidFormat(format!(
            "source object version mismatch for {uri}: expected {}, got {:?}",
            expected.version, meta.version
        )));
    }
    if meta.e_tag.as_deref() != Some(expected.etag.as_str()) {
        return Err(HailError::InvalidFormat(format!(
            "source object ETag mismatch for {uri}: expected {}, got {:?}",
            expected.etag, meta.e_tag
        )));
    }
    if meta.size as u64 != expected.size {
        return Err(HailError::InvalidFormat(format!(
            "source object size mismatch for {uri}: expected {}, got {}",
            expected.size, meta.size
        )));
    }
    Ok(())
}

async fn head_resolved(resolved: &ResolvedObject) -> Result<ObjectMeta> {
    let result = resolved
        .store
        .get_opts(
            &resolved.path,
            pinned_options(resolved.expected.as_ref(), None, true),
        )
        .await?;
    if let Some(expected) = &resolved.expected {
        verify_object_meta(&resolved.canonical_uri, expected, &result.meta)?;
    }
    Ok(result.meta)
}

/// Fetch a byte range with app-level retries for transient mid-stream failures.
/// Every snapshot request carries both version and If-Match, then verifies the
/// identity returned with the body before exposing bytes to a decoder.
async fn get_range_retrying(
    store: &Arc<dyn ObjectStore>,
    path: &ObjPath,
    context: &CloudObjectContext,
    range: std::ops::Range<usize>,
) -> Result<bytes::Bytes> {
    const MAX_ATTEMPTS: u32 = 6;
    let mut backoff = std::time::Duration::from_millis(500);
    let mut attempt = 0;
    loop {
        let request = store
            .get_opts(
                path,
                pinned_options(context.expected.as_ref(), Some(range.clone()), false),
            )
            .await;
        match request {
            Ok(result) => {
                if let Some(expected) = &context.expected {
                    verify_object_meta(&context.canonical_uri, expected, &result.meta)?;
                }
                if result.range != range {
                    return Err(HailError::InvalidFormat(format!(
                        "source object range mismatch for {}: expected {:?}, got {:?}",
                        context.canonical_uri, range, result.range
                    )));
                }
                let bytes = result.bytes().await?;
                if bytes.len() != range.len() {
                    return Err(HailError::InvalidFormat(format!(
                        "source object short range for {}: expected {} bytes, got {}",
                        context.canonical_uri,
                        range.len(),
                        bytes.len()
                    )));
                }
                return Ok(bytes);
            }
            Err(error) => {
                attempt += 1;
                let message = error.to_string();
                let transient = message.contains("error decoding response body")
                    || message.contains("decoding response")
                    || message.contains("connection")
                    || message.contains("timed out")
                    || message.contains("broken pipe")
                    || message.contains("reset");
                if attempt >= MAX_ATTEMPTS || !transient {
                    return Err(error.into());
                }
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, std::time::Duration::from_secs(30));
            }
        }
    }
}

/// Get or create a GCS client for the given bucket.
///
/// Clients are cached and reused to avoid repeated token fetches from
/// the metadata server. The `object_store` crate handles token refresh
/// internally per client, so sharing clients also reduces token storms.
///
/// Supports `GOOGLE_APPLICATION_CREDENTIALS` environment variable for
/// service account key files, which bypasses the metadata service entirely.
#[cfg(feature = "gcp")]
pub fn get_gcs_client(bucket: &str) -> Result<Arc<dyn ObjectStore>> {
    let mut cache = GCS_CLIENT_CACHE.lock().unwrap();

    if let Some(client) = cache.get(bucket) {
        debug!("GCS client cache hit for bucket {}", bucket);
        return Ok(client.clone());
    }

    debug!(
        "GCS client cache miss for bucket {}, creating new client",
        bucket
    );
    let start = std::time::Instant::now();

    // Build the GCS client, optionally using service account key file
    let mut builder = object_store::gcp::GoogleCloudStorageBuilder::new().with_bucket_name(bucket);

    // Use key file if available, otherwise fall back to metadata service
    if let Ok(key_path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        builder = builder.with_service_account_path(key_path);
    }

    // Configure retry with exponential backoff for 429/5xx errors
    let retry_config = object_store::RetryConfig {
        max_retries: 10,
        retry_timeout: std::time::Duration::from_secs(180),
        backoff: object_store::BackoffConfig {
            init_backoff: std::time::Duration::from_millis(500),
            max_backoff: std::time::Duration::from_secs(30),
            base: 2.0,
        },
    };
    builder = builder.with_retry(retry_config);

    // Large connection pool for high-concurrency prefetch (48 cores × 16 in-flight = 768 streams)
    let client_options = object_store::ClientOptions::new()
        .with_pool_max_idle_per_host(1024)
        .with_http2_keep_alive_interval(std::time::Duration::from_secs(30))
        .with_http2_keep_alive_while_idle();
    builder = builder.with_client_options(client_options);

    let client: Arc<dyn ObjectStore> =
        Arc::new(builder.build().map_err(|e| {
            HailError::InvalidFormat(format!("Failed to create GCS client: {}", e))
        })?);

    debug!(
        "GCS client created for bucket {} in {:?}",
        bucket,
        start.elapsed()
    );
    cache.insert(bucket.to_string(), client.clone());
    Ok(client)
}

/// A reader that fetches data from cloud storage in chunks
///
/// This struct implements `std::io::Read` and `std::io::Seek`, allowing it to be
/// used with the synchronous buffer stack. Data is fetched from cloud storage
/// in large chunks to minimize HTTP request overhead.
pub struct CloudReader {
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    position: u64,
    buffer: Vec<u8>,
    buffer_start: u64,
    file_size: u64,
    chunk_size: usize,
    context: CloudObjectContext,
}

impl CloudReader {
    /// Create a new CloudReader for the given store and path
    pub fn new(store: Arc<dyn ObjectStore>, path: ObjPath, file_size: u64) -> Self {
        let canonical_uri = path.to_string();
        CloudReader {
            store,
            path,
            position: 0,
            buffer: Vec::new(),
            buffer_start: 0,
            file_size,
            chunk_size: DEFAULT_CHUNK_SIZE,
            context: CloudObjectContext {
                canonical_store_id: "<direct-store>".to_string(),
                canonical_uri,
                expected: None,
                observed_version: None,
                observed_etag: None,
            },
        }
    }

    fn new_resolved(
        store: Arc<dyn ObjectStore>,
        path: ObjPath,
        file_size: u64,
        context: CloudObjectContext,
    ) -> Self {
        let mut reader = Self::new(store, path, file_size);
        reader.context = context;
        reader
    }

    /// Set the chunk size for reads
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    /// Fill the internal buffer starting from the current position
    fn fill_buffer(&mut self) -> std::io::Result<()> {
        // Calculate the range to fetch
        let start = self.position;
        let end = std::cmp::min(self.position + self.chunk_size as u64, self.file_size);

        if start >= self.file_size {
            self.buffer.clear();
            self.buffer_start = start;
            return Ok(());
        }

        trace!("CloudReader: fetching range {}..{}", start, end);

        // Fetch the range from cloud storage
        let range = start as usize..end as usize;
        let store = self.store.clone();
        let path = self.path.clone();
        let context = self.context.clone();

        let start_time = std::time::Instant::now();
        let bytes = IO_RUNTIME
            .block_on(async { get_range_retrying(&store, &path, &context, range).await })
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        trace!("CloudReader: fetch completed in {:?}", start_time.elapsed());

        self.buffer = bytes.to_vec();
        self.buffer_start = start;
        Ok(())
    }

    /// Check if the current position is within the buffered range
    fn position_in_buffer(&self) -> bool {
        if self.buffer.is_empty() {
            return false;
        }
        let buffer_end = self.buffer_start + self.buffer.len() as u64;
        self.position >= self.buffer_start && self.position < buffer_end
    }

    /// Get the offset within the buffer for the current position
    fn buffer_offset(&self) -> usize {
        (self.position - self.buffer_start) as usize
    }
}

impl Read for CloudReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.position >= self.file_size {
            return Ok(0); // EOF
        }

        // Refill buffer if needed
        if !self.position_in_buffer() {
            self.fill_buffer()?;
        }

        // Copy data from buffer to output
        let offset = self.buffer_offset();
        let available = self.buffer.len() - offset;
        let to_copy = std::cmp::min(available, buf.len());

        if to_copy == 0 {
            return Ok(0); // EOF
        }

        buf[..to_copy].copy_from_slice(&self.buffer[offset..offset + to_copy]);
        self.position += to_copy as u64;
        Ok(to_copy)
    }
}

impl Seek for CloudReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_position = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => {
                if offset >= 0 {
                    self.file_size.saturating_add(offset as u64)
                } else {
                    self.file_size.saturating_sub((-offset) as u64)
                }
            }
            SeekFrom::Current(offset) => {
                if offset >= 0 {
                    self.position.saturating_add(offset as u64)
                } else {
                    self.position.saturating_sub((-offset) as u64)
                }
            }
        };

        self.position = new_position;
        Ok(self.position)
    }
}

/// A high-performance cloud reader that prefetches data in background
///
/// This reader spawns a background task that aggressively fetches chunks ahead
/// of the read position, hiding network latency from the CPU. Data flows through
/// a bounded channel providing natural backpressure.
///
/// # Architecture
///
/// ```text
/// GCS/S3 -> [Background Task] -> [Channel Buffer] -> [Read calls]
///              (async)             (PREFETCH_DEPTH    (sync)
///                                   chunks)
/// ```
///
/// The CPU processes data from the channel while the background task fetches
/// the next chunks, ensuring the CPU never waits for network I/O.
pub struct PrefetchingCloudReader {
    /// Channel receiver for prefetched chunks
    rx: tokio::sync::mpsc::Receiver<std::io::Result<bytes::Bytes>>,
    /// Current chunk being consumed
    current_chunk: bytes::Bytes,
    /// Position within current chunk
    chunk_pos: usize,
    /// Logical file position
    position: u64,
    /// Total file size
    file_size: u64,
    /// Store and path for seek operations
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    context: CloudObjectContext,
    /// Dropped to signal prefetch task to stop via channel closure
    _cancel_tx: Option<tokio::sync::mpsc::Sender<()>>,
}

impl PrefetchingCloudReader {
    /// Create a new prefetching cloud reader
    ///
    /// Immediately starts a background task to fetch data from the given offset.
    pub fn new(store: Arc<dyn ObjectStore>, path: ObjPath, file_size: u64) -> Self {
        let canonical_uri = path.to_string();
        Self::new_resolved(
            store,
            path,
            file_size,
            CloudObjectContext {
                canonical_store_id: "<direct-store>".to_string(),
                canonical_uri,
                expected: None,
                observed_version: None,
                observed_etag: None,
            },
        )
    }

    fn new_resolved(
        store: Arc<dyn ObjectStore>,
        path: ObjPath,
        file_size: u64,
        context: CloudObjectContext,
    ) -> Self {
        let (rx, cancel_tx) =
            Self::start_prefetch(store.clone(), path.clone(), context.clone(), 0, file_size);

        PrefetchingCloudReader {
            rx,
            current_chunk: bytes::Bytes::new(),
            chunk_pos: 0,
            position: 0,
            file_size,
            store,
            path,
            context,
            _cancel_tx: Some(cancel_tx),
        }
    }

    /// Start or restart the prefetch background task
    ///
    /// Launches PREFETCH_DEPTH concurrent range fetches using `futures::stream::buffered`,
    /// so multiple HTTP requests are in-flight simultaneously. This saturates the network
    /// link instead of waiting sequentially for each chunk.
    fn start_prefetch(
        store: Arc<dyn ObjectStore>,
        path: ObjPath,
        context: CloudObjectContext,
        start_offset: u64,
        file_size: u64,
    ) -> (
        tokio::sync::mpsc::Receiver<std::io::Result<bytes::Bytes>>,
        tokio::sync::mpsc::Sender<()>,
    ) {
        let depth = get_prefetch_depth();
        let (tx, rx) = tokio::sync::mpsc::channel(depth);
        let (cancel_tx, mut cancel_rx) = tokio::sync::mpsc::channel::<()>(1);

        IO_RUNTIME.spawn(async move {
            // Build a stream of chunk offsets, then fetch them concurrently
            let chunk_count = ((file_size - start_offset) as usize + PREFETCH_CHUNK_SIZE - 1)
                / PREFETCH_CHUNK_SIZE;

            let store_ref = &store;
            let path_ref = &path;
            let context_ref = &context;

            let mut fetch_stream = futures::stream::iter(0..chunk_count)
                .map(|i| {
                    let offset = start_offset + (i as u64 * PREFETCH_CHUNK_SIZE as u64);
                    let len =
                        std::cmp::min(PREFETCH_CHUNK_SIZE as u64, file_size - offset) as usize;
                    let range = offset as usize..(offset as usize + len);
                    let store = store_ref.clone();
                    let path = path_ref.clone();
                    let context = context_ref.clone();
                    async move {
                        let cache_key = context.cache_key(offset, len);
                        // Check chunk cache first
                        if let Some(cached) = CHUNK_CACHE.get(&cache_key).await {
                            trace!(
                                "PrefetchingCloudReader: cache hit for range {}..{}",
                                offset,
                                offset + len as u64
                            );
                            return Ok(cached);
                        }
                        trace!(
                            "PrefetchingCloudReader: fetching range {}..{}",
                            offset,
                            offset + len as u64
                        );
                        let start_time = std::time::Instant::now();
                        let result = get_range_retrying(&store, &path, &context, range).await;
                        trace!(
                            "PrefetchingCloudReader: fetch completed in {:?}",
                            start_time.elapsed()
                        );
                        match result {
                            Ok(bytes) => {
                                CHUNK_CACHE.insert(cache_key, bytes.clone()).await;
                                Ok(bytes)
                            }
                            Err(e) => Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e.to_string(),
                            )),
                        }
                    }
                })
                .buffered(depth);

            loop {
                tokio::select! {
                    _ = cancel_rx.recv() => break,
                    chunk = fetch_stream.next() => {
                        match chunk {
                            Some(result) => {
                                if tx.send(result).await.is_err() {
                                    break; // Receiver dropped
                                }
                            }
                            None => break, // Stream exhausted
                        }
                    }
                }
            }
        });

        (rx, cancel_tx)
    }

    /// Ensure we have data in current_chunk to read from
    fn ensure_chunk(&mut self) -> std::io::Result<bool> {
        // If we have data remaining in current chunk, we're good
        if self.chunk_pos < self.current_chunk.len() {
            return Ok(true);
        }

        // Try to get next chunk from channel (blocking — called from rayon threads)
        match self.rx.blocking_recv() {
            Some(result) => {
                self.current_chunk = result?;
                self.chunk_pos = 0;
                Ok(!self.current_chunk.is_empty())
            }
            None => {
                // Channel closed, no more data
                Ok(false)
            }
        }
    }
}

impl Read for PrefetchingCloudReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.position >= self.file_size {
            return Ok(0);
        }

        if !self.ensure_chunk()? {
            return Ok(0);
        }

        let available = self.current_chunk.len() - self.chunk_pos;
        let to_copy = std::cmp::min(available, buf.len());

        buf[..to_copy]
            .copy_from_slice(&self.current_chunk[self.chunk_pos..self.chunk_pos + to_copy]);
        self.chunk_pos += to_copy;
        self.position += to_copy as u64;

        Ok(to_copy)
    }
}

impl Seek for PrefetchingCloudReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_position = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => {
                if offset >= 0 {
                    self.file_size.saturating_add(offset as u64)
                } else {
                    self.file_size.saturating_sub((-offset) as u64)
                }
            }
            SeekFrom::Current(offset) => {
                if offset >= 0 {
                    self.position.saturating_add(offset as u64)
                } else {
                    self.position.saturating_sub((-offset) as u64)
                }
            }
        };

        if new_position != self.position {
            // Drop old cancel sender to signal prefetch task to stop
            self._cancel_tx = None;

            // Start new prefetch from the new position
            let (rx, cancel_tx) = Self::start_prefetch(
                self.store.clone(),
                self.path.clone(),
                self.context.clone(),
                new_position,
                self.file_size,
            );

            self.rx = rx;
            self._cancel_tx = Some(cancel_tx);
            self.current_chunk = bytes::Bytes::new();
            self.chunk_pos = 0;
            self.position = new_position;
        }

        Ok(self.position)
    }
}

/// A memory-mapped reader for local files
///
/// This provides zero-copy access to local files by mapping them directly
/// into the process's virtual address space. The OS handles paging data
/// in from disk as needed.
///
/// This is optimal for local NVMe storage where random access is fast.
pub struct MmapReader {
    /// The memory-mapped data
    mmap: memmap2::Mmap,
    /// Current read position
    position: usize,
}

impl MmapReader {
    /// Create a new memory-mapped reader for a file
    pub fn new(file: &File) -> std::io::Result<Self> {
        // SAFETY: We treat the file as read-only and don't modify it
        let mmap = unsafe { memmap2::Mmap::map(file)? };
        Ok(MmapReader { mmap, position: 0 })
    }
}

impl Read for MmapReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.mmap.len().saturating_sub(self.position);
        let to_copy = std::cmp::min(remaining, buf.len());

        if to_copy == 0 {
            return Ok(0);
        }

        buf[..to_copy].copy_from_slice(&self.mmap[self.position..self.position + to_copy]);
        self.position += to_copy;
        Ok(to_copy)
    }
}

impl Seek for MmapReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let len = self.mmap.len() as u64;
        let new_position = match pos {
            SeekFrom::Start(offset) => offset as usize,
            SeekFrom::End(offset) => {
                if offset >= 0 {
                    len.saturating_add(offset as u64) as usize
                } else {
                    len.saturating_sub((-offset) as u64) as usize
                }
            }
            SeekFrom::Current(offset) => {
                if offset >= 0 {
                    (self.position as u64).saturating_add(offset as u64) as usize
                } else {
                    (self.position as u64).saturating_sub((-offset) as u64) as usize
                }
            }
        };

        self.position = new_position;
        Ok(self.position as u64)
    }
}

/// Trait combining Read and Seek for seekable readers
pub trait ReadSeek: Read + Seek {}

/// Blanket implementation for any type that implements both Read and Seek
impl<T: Read + Seek> ReadSeek for T {}

/// A boxed reader that can be either a local file or a cloud reader
/// Includes Seek to support indexed reading (BGZF/Tabix)
pub type BoxedReader = Box<dyn ReadSeek + Send + Sync>;

/// Create a reader for a path, detecting whether it's local or cloud
///
/// Supported URL schemes:
/// - `gs://bucket/path` - Google Cloud Storage
/// - `s3://bucket/path` - Amazon S3
/// - `http://` or `https://` - HTTP(S) URLs
/// - Local file path - Regular file system access
///
/// For cloud paths, uses `PrefetchingCloudReader` for latency-hiding streaming.
/// For local paths, attempts to use memory-mapping for optimal NVMe performance,
/// falling back to standard file I/O if mmap fails.
#[tracing::instrument(skip_all, fields(path))]
pub fn get_reader(path: &str) -> Result<BoxedReader> {
    get_reader_with_snapshot(path, None)
}

/// Create a reader bound to an explicit immutable source snapshot.
/// Every path opened in snapshot mode must have exactly one manifest entry.
pub fn get_reader_with_snapshot(
    path: &str,
    snapshot: Option<&SourceSnapshot>,
) -> Result<BoxedReader> {
    if path.starts_with("gs://")
        || path.starts_with("s3://")
        || path.starts_with("http://")
        || path.starts_with("https://")
    {
        create_cloud_reader(path, snapshot)
    } else {
        if let Some(snapshot) = snapshot {
            verify_local_object(path, snapshot.object(path)?)?;
        }
        // Local file - try mmap first for optimal NVMe performance
        let file = File::open(path).map_err(HailError::Io)?;

        // Try memory mapping for zero-copy access
        match MmapReader::new(&file) {
            Ok(mmap_reader) => {
                trace!("Using memory-mapped reader for {}", path);
                Ok(Box::new(mmap_reader))
            }
            Err(e) => {
                // Fall back to standard file reading if mmap fails
                trace!(
                    "Mmap failed ({}), falling back to standard file reader for {}",
                    e,
                    path
                );
                // Re-open the file since we may have consumed it
                let file = File::open(path).map_err(|e| HailError::Io(e))?;
                Ok(Box::new(file))
            }
        }
    }
}

/// Create a cloud reader for the given URL
///
/// Uses `PrefetchingCloudReader` which spawns a background task to fetch
/// data ahead of the read position, hiding network latency.
#[tracing::instrument(skip_all, fields(url = url_str))]
fn create_cloud_reader(url_str: &str, snapshot: Option<&SourceSnapshot>) -> Result<BoxedReader> {
    let resolved = crate::io::resolve_object(url_str, snapshot)?;
    // A snapshot HEAD is itself version- and ETag-conditioned. Legacy HEADs
    // provide the identity component used to isolate shared cache entries.
    let meta = IO_RUNTIME.block_on(head_resolved(&resolved))?;
    let file_size = resolved
        .expected
        .as_ref()
        .map(|identity| identity.size)
        .unwrap_or(meta.size as u64);
    let context = CloudObjectContext::from_resolved(&resolved, &meta);

    trace!(
        "Creating PrefetchingCloudReader for {} ({} bytes, pinned={})",
        url_str,
        file_size,
        resolved.expected.is_some()
    );
    Ok(Box::new(PrefetchingCloudReader::new_resolved(
        resolved.store,
        resolved.path,
        file_size,
        context,
    )))
}

fn verify_local_object(path: &str, expected: &SourceObject) -> Result<()> {
    let metadata = std::fs::metadata(path)?;
    if metadata.len() != expected.size {
        return Err(HailError::InvalidFormat(format!(
            "source object size mismatch for {path}: expected {}, got {}",
            expected.size,
            metadata.len()
        )));
    }
    let expected_sha256 = expected.sha256.as_ref().ok_or_else(|| {
        HailError::InvalidFormat(format!(
            "local snapshot object requires SHA-256 for byte verification: {path}"
        ))
    })?;
    let mut file = File::open(path)?;
    let mut hasher = sha2::Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let actual = {
        let digest = hasher.finalize();
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    if &actual != expected_sha256 {
        return Err(HailError::InvalidFormat(format!(
            "source object SHA-256 mismatch for {path}: expected {expected_sha256}, got {actual}"
        )));
    }
    Ok(())
}

/// Join a base path with a child path, handling both local and cloud paths correctly
///
/// For cloud URLs, this handles path joining with forward slashes.
/// For local paths, this uses the OS-appropriate separator.
pub fn join_path(base: &str, child: &str) -> String {
    // Normalize child path (remove leading separators)
    let child = child.trim_start_matches('/').trim_start_matches('\\');

    if base.starts_with("gs://")
        || base.starts_with("s3://")
        || base.starts_with("http://")
        || base.starts_with("https://")
    {
        // Cloud URL - always use forward slashes
        let base = base.trim_end_matches('/');
        format!("{}/{}", base, child)
    } else {
        // Local path - use std::path for OS-appropriate handling
        let path = std::path::Path::new(base).join(child);
        path.to_string_lossy().to_string()
    }
}

/// Check if a path is a cloud URL
pub fn is_cloud_path(path: &str) -> bool {
    path.starts_with("gs://")
        || path.starts_with("s3://")
        || path.starts_with("http://")
        || path.starts_with("https://")
}

/// Read a specific byte range from a file (local or cloud)
///
/// This is the key function for efficient random access. For cloud storage,
/// it uses HTTP Range requests to fetch only the needed bytes.
///
/// # Arguments
/// * `path` - Path to the file (local or cloud URL)
/// * `offset` - Start offset in bytes
/// * `length` - Number of bytes to read
///
/// # Returns
/// A vector containing the requested bytes
pub fn range_read(path: &str, offset: u64, length: usize) -> Result<Vec<u8>> {
    range_read_with_snapshot(path, offset, length, None)
}

pub fn range_read_with_snapshot(
    path: &str,
    offset: u64,
    length: usize,
    snapshot: Option<&SourceSnapshot>,
) -> Result<Vec<u8>> {
    if is_cloud_path(path) {
        range_read_cloud(path, offset, length, snapshot)
    } else {
        if let Some(snapshot) = snapshot {
            verify_local_object(path, snapshot.object(path)?)?;
        }
        range_read_local(path, offset, length)
    }
}

/// Read a byte range from a local file
fn range_read_local(path: &str, offset: u64, length: usize) -> Result<Vec<u8>> {
    use std::fs::File;

    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;

    let mut buf = vec![0u8; length];
    let bytes_read = file.read(&mut buf)?;
    buf.truncate(bytes_read);

    Ok(buf)
}

/// Read a byte range from a cloud storage URL
fn range_read_cloud(
    url_str: &str,
    offset: u64,
    length: usize,
    snapshot: Option<&SourceSnapshot>,
) -> Result<Vec<u8>> {
    let resolved = crate::io::resolve_object(url_str, snapshot)?;
    let meta = IO_RUNTIME.block_on(head_resolved(&resolved))?;
    let context = CloudObjectContext::from_resolved(&resolved, &meta);
    let range = offset as usize..(offset as usize + length);
    let bytes = IO_RUNTIME.block_on(async {
        get_range_retrying(&resolved.store, &resolved.path, &context, range).await
    })?;
    Ok(bytes.to_vec())
}

/// Read a single compressed block from a partition file
///
/// This reads the block at the given file offset, including the 4-byte
/// length header and the block data.
///
/// # Arguments
/// * `path` - Path to the partition file (local or cloud)
/// * `file_offset` - Offset into the file where the block starts
///
/// # Returns
/// A tuple of (block_data, compressed_length) where block_data includes
/// the decompressed size prefix (first 4 bytes)
pub fn read_single_block(path: &str, file_offset: u64) -> Result<Vec<u8>> {
    // Read the 4-byte block length header first
    let header = range_read(path, file_offset, 4)?;
    if header.len() < 4 {
        return Err(HailError::UnexpectedEof);
    }
    let block_len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;

    // Now read the actual block data
    let block_data = range_read(path, file_offset + 4, block_len)?;

    Ok(block_data)
}

/// Get the size of a file (local or cloud)
///
/// # Arguments
/// * `path` - Path to the file (local or cloud URL)
///
/// # Returns
/// The size of the file in bytes
pub fn get_file_size(path: &str) -> Result<u64> {
    get_file_size_with_snapshot(path, None)
}

pub fn get_file_size_with_snapshot(path: &str, snapshot: Option<&SourceSnapshot>) -> Result<u64> {
    if is_cloud_path(path) {
        let resolved = crate::io::resolve_object(path, snapshot)?;
        let meta = IO_RUNTIME.block_on(head_resolved(&resolved))?;
        Ok(resolved
            .expected
            .as_ref()
            .map(|identity| identity.size)
            .unwrap_or(meta.size as u64))
    } else {
        if let Some(snapshot) = snapshot {
            let expected = snapshot.object(path)?;
            verify_local_object(path, expected)?;
            Ok(expected.size)
        } else {
            Ok(std::fs::metadata(path)?.len())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        ListResult, MultipartUpload, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    };

    #[derive(Debug)]
    struct VersionedStore {
        inner: InMemory,
        version: String,
    }

    impl std::fmt::Display for VersionedStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "versioned-memory({})", self.version)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for VersionedStore {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjPath,
            mut options: GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if let Some(requested) = options.version.take() {
                if requested != self.version {
                    return Err(object_store::Error::Precondition {
                        path: location.to_string(),
                        source: format!("version {} does not match {}", requested, self.version)
                            .into(),
                    });
                }
            }
            let mut result = self.inner.get_opts(location, options).await?;
            result.meta.version = Some(self.version.clone());
            Ok(result)
        }

        async fn delete(&self, location: &ObjPath) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &ObjPath, to: &ObjPath) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &ObjPath,
            to: &ObjPath,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    fn versioned_object(
        uri: &str,
        store_id: &str,
        version: &str,
        data: &[u8],
    ) -> (Arc<dyn ObjectStore>, ObjPath, CloudObjectContext) {
        let store = Arc::new(VersionedStore {
            inner: InMemory::new(),
            version: version.to_string(),
        });
        let path = ObjPath::from("same/path");
        let put = IO_RUNTIME
            .block_on(store.put(&path, bytes::Bytes::copy_from_slice(data).into()))
            .unwrap();
        let expected = SourceObject {
            uri: uri.to_string(),
            version: version.to_string(),
            etag: put.e_tag.unwrap(),
            size: data.len() as u64,
            crc32c: Some("test-crc32c".to_string()),
            sha256: None,
        };
        let context = CloudObjectContext {
            canonical_store_id: store_id.to_string(),
            canonical_uri: uri.to_string(),
            expected: Some(expected),
            observed_version: None,
            observed_etag: None,
        };
        (store, path, context)
    }

    #[test]
    fn pinned_prefetch_range_seek_and_cache_isolation() {
        let unique = uuid::Uuid::new_v4();
        let uri_a = format!("gs://bucket-a/{unique}");
        let uri_b = format!("gs://bucket-b/{unique}");
        let (store_a, path_a, context_a) =
            versioned_object(&uri_a, "gs://bucket-a", "generation-1", b"abcdefghij");
        let (store_b, path_b, context_b) =
            versioned_object(&uri_b, "gs://bucket-b", "generation-1", b"0123456789");

        let mut a = PrefetchingCloudReader::new_resolved(store_a, path_a, 10, context_a.clone());
        let mut first = [0; 4];
        a.read_exact(&mut first).unwrap();
        assert_eq!(&first, b"abcd");
        a.seek(SeekFrom::Start(6)).unwrap();
        let mut tail = Vec::new();
        a.read_to_end(&mut tail).unwrap();
        assert_eq!(&tail, b"ghij");

        // Same object path/version/ETag in another store must not reuse A's bytes.
        let mut b = PrefetchingCloudReader::new_resolved(store_b, path_b, 10, context_b);
        let mut all = Vec::new();
        b.read_to_end(&mut all).unwrap();
        assert_eq!(&all, b"0123456789");

        assert_ne!(
            context_a.cache_key(0, 10),
            CloudObjectContext {
                canonical_store_id: "gs://bucket-b".to_string(),
                canonical_uri: uri_a,
                expected: context_a.expected.clone(),
                observed_version: None,
                observed_etag: None,
            }
            .cache_key(0, 10)
        );
    }

    #[test]
    fn pinned_reads_fail_on_wrong_or_absent_identity_and_missing_object() {
        let uri = format!("gs://bucket/{}", uuid::Uuid::new_v4());
        let (store, path, context) =
            versioned_object(&uri, "gs://bucket", "generation-7", b"payload");

        let mut wrong_version = context.clone();
        wrong_version.expected.as_mut().unwrap().version = "generation-8".to_string();
        let error = IO_RUNTIME
            .block_on(get_range_retrying(&store, &path, &wrong_version, 0..7))
            .unwrap_err()
            .to_string();
        assert!(error.contains("version"), "{error}");

        let mut wrong_etag = context.clone();
        wrong_etag.expected.as_mut().unwrap().etag = "wrong-etag".to_string();
        let error = IO_RUNTIME
            .block_on(get_range_retrying(&store, &path, &wrong_etag, 0..7))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("precondition") || error.contains("ETag"),
            "{error}"
        );

        let meta = IO_RUNTIME.block_on(store.head(&path)).unwrap();
        let mut wrong_size = context.expected.clone().unwrap();
        wrong_size.size += 1;
        assert!(verify_object_meta(&uri, &wrong_size, &meta)
            .unwrap_err()
            .to_string()
            .contains("size mismatch"));

        let mut absent_version = meta.clone();
        absent_version.version = None;
        assert!(
            verify_object_meta(&uri, context.expected.as_ref().unwrap(), &absent_version)
                .unwrap_err()
                .to_string()
                .contains("version mismatch")
        );

        let missing = ObjPath::from("missing");
        assert!(IO_RUNTIME
            .block_on(get_range_retrying(&store, &missing, &context, 0..1))
            .is_err());
    }

    #[test]
    fn test_join_path_local() {
        let base = "/path/to/table.ht";
        let child = "rows/metadata.json.gz";
        let result = join_path(base, child);
        assert!(result.contains("rows"));
        assert!(result.contains("metadata.json.gz"));
    }

    #[test]
    fn test_join_path_gcs() {
        let base = "gs://my-bucket/data/table.ht";
        let child = "rows/metadata.json.gz";
        let result = join_path(base, child);
        assert_eq!(result, "gs://my-bucket/data/table.ht/rows/metadata.json.gz");
    }

    #[test]
    fn test_join_path_s3() {
        let base = "s3://my-bucket/data/table.ht/";
        let child = "/rows/metadata.json.gz";
        let result = join_path(base, child);
        assert_eq!(result, "s3://my-bucket/data/table.ht/rows/metadata.json.gz");
    }

    #[test]
    fn test_is_cloud_path() {
        assert!(is_cloud_path("gs://bucket/path"));
        assert!(is_cloud_path("s3://bucket/path"));
        assert!(is_cloud_path("https://example.com/path"));
        assert!(!is_cloud_path("/local/path"));
        assert!(!is_cloud_path("./relative/path"));
    }

    #[test]
    fn test_get_reader_local_file() {
        // This test uses a file that exists in the repo
        let result = get_reader("Cargo.toml");
        assert!(result.is_ok());
    }
}
