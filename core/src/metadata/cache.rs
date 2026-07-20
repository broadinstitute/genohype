//! Local filesystem cache for remote metadata files.
//!
//! Caches metadata files (metadata.json.gz, rows/metadata.json.gz) on local disk
//! to avoid re-downloading them on every CLI invocation. Hail tables are immutable,
//! so caching is safe — a 24h TTL provides insurance against path reuse.

use crate::Result;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tracing::{debug, warn};

/// Default TTL: 24 hours
const DEFAULT_TTL_SECS: u64 = 24 * 60 * 60;

/// Options controlling metadata caching behavior.
#[derive(Debug, Clone)]
pub struct CacheOptions {
    /// Whether caching is enabled
    pub enabled: bool,
    /// Time-to-live in seconds (files older than this are re-fetched)
    pub ttl_secs: u64,
    /// If true, skip cache reads but still write (force refresh)
    pub force_refresh: bool,
}

impl Default for CacheOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_secs: DEFAULT_TTL_SECS,
            force_refresh: false,
        }
    }
}

impl CacheOptions {
    /// Create disabled cache options (no caching at all)
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }
}

/// Metadata cache backed by the local filesystem.
///
/// Cache layout mirrors the URL path:
/// ```text
/// <cache_dir>/genohype/gs/bucket-name/path/to/table.ht/
///   rows_metadata.json.gz
///   table_metadata.json.gz
/// ```
pub struct MetadataCache {
    cache_dir: PathBuf,
}

impl MetadataCache {
    /// Create a new MetadataCache using the platform cache directory.
    ///
    /// Uses `dirs::cache_dir()`:
    /// - macOS: ~/Library/Caches/genohype
    /// - Linux: ~/.cache/genohype
    pub fn new() -> Option<Self> {
        let base = dirs::cache_dir()?;
        Some(Self {
            cache_dir: base.join("genohype"),
        })
    }

    /// Get the root cache directory path.
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    /// Map a URL to a local cache directory path.
    ///
    /// `gs://bucket/path/to/table.ht` → `<cache_dir>/gs/bucket/path/to/table.ht/`
    fn url_to_cache_dir(&self, url: &str) -> PathBuf {
        // Replace :// with / and strip trailing slashes
        let sanitized = url.replace("://", "/").trim_end_matches('/').to_string();

        // Prevent directory traversal
        let sanitized = sanitized.replace("..", "_");

        self.cache_dir.join(sanitized)
    }

    /// Try to read a cached file, returning the bytes if the cache is fresh.
    ///
    /// Returns `None` on cache miss (file doesn't exist, expired, or corrupt).
    fn read_cached(&self, cache_path: &Path, opts: &CacheOptions) -> Option<Vec<u8>> {
        if opts.force_refresh {
            debug!("Cache force-refresh, skipping read for {:?}", cache_path);
            return None;
        }

        let metadata = std::fs::metadata(cache_path).ok()?;
        let modified = metadata.modified().ok()?;
        let age = SystemTime::now().duration_since(modified).ok()?;

        if age > Duration::from_secs(opts.ttl_secs) {
            debug!(
                "Cache expired for {:?} (age={:.1}h, ttl={:.1}h)",
                cache_path,
                age.as_secs_f64() / 3600.0,
                opts.ttl_secs as f64 / 3600.0,
            );
            return None;
        }

        match std::fs::read(cache_path) {
            Ok(data) => {
                debug!(
                    "Cache hit: {:?} ({} bytes, age={:.1}h)",
                    cache_path,
                    data.len(),
                    age.as_secs_f64() / 3600.0,
                );
                Some(data)
            }
            Err(e) => {
                warn!("Cache read error for {:?}: {}", cache_path, e);
                None
            }
        }
    }

    /// Write data to the cache atomically (write tmp + rename).
    fn write_cached(&self, cache_path: &Path, data: &[u8]) {
        if let Some(parent) = cache_path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                warn!("Failed to create cache dir {:?}: {}", parent, e);
                return;
            }
        }

        let tmp_path = cache_path.with_extension(format!("tmp.{}", std::process::id()));

        if let Err(e) = std::fs::write(&tmp_path, data) {
            warn!("Failed to write cache tmp {:?}: {}", tmp_path, e);
            let _ = std::fs::remove_file(&tmp_path);
            return;
        }

        if let Err(e) = std::fs::rename(&tmp_path, cache_path) {
            warn!("Failed to rename cache file {:?}: {}", cache_path, e);
            let _ = std::fs::remove_file(&tmp_path);
            return;
        }

        debug!("Cached {} bytes to {:?}", data.len(), cache_path);
    }

    /// Get or fetch metadata bytes for a given URL and filename.
    ///
    /// On cache hit: returns local data.
    /// On cache miss: calls `fetch_fn` to download, caches result, returns data.
    pub fn get_or_fetch<F>(
        &self,
        url: &str,
        filename: &str,
        opts: &CacheOptions,
        fetch_fn: F,
    ) -> Result<Vec<u8>>
    where
        F: FnOnce() -> Result<Vec<u8>>,
    {
        if !opts.enabled {
            return fetch_fn();
        }

        let dir = self.url_to_cache_dir(url);
        let cache_path = dir.join(filename);

        // Try cache read
        if let Some(data) = self.read_cached(&cache_path, opts) {
            return Ok(data);
        }

        // Cache miss: fetch from source
        let data = fetch_fn()?;

        // Write through to cache
        self.write_cached(&cache_path, &data);

        Ok(data)
    }

    /// Check if a cached file exists and is fresh, returning its path.
    ///
    /// On cache miss: calls `fetch_fn` to download, writes the bytes to the
    /// cache directory, and returns the local `PathBuf`. Callers can then
    /// open the file with `get_reader()` which will use `MmapReader` for
    /// zero-copy local access.
    pub fn get_or_fetch_file<F>(
        &self,
        url: &str,
        filename: &str,
        opts: &CacheOptions,
        fetch_fn: F,
    ) -> Result<PathBuf>
    where
        F: FnOnce() -> Result<Vec<u8>>,
    {
        let dir = self.url_to_cache_dir(url);
        let cache_path = dir.join(filename);

        if opts.enabled && !opts.force_refresh {
            // Check if cached file exists and is fresh (by TTL)
            if let Ok(metadata) = std::fs::metadata(&cache_path) {
                if let Ok(modified) = metadata.modified() {
                    if let Ok(age) = SystemTime::now().duration_since(modified) {
                        if age <= Duration::from_secs(opts.ttl_secs) {
                            debug!(
                                "Cache file hit: {:?} (age={:.1}h)",
                                cache_path,
                                age.as_secs_f64() / 3600.0,
                            );
                            return Ok(cache_path);
                        }
                    }
                }
            }
        }

        if !opts.enabled {
            // No caching: fetch and write to a temp-like path but still return it
            // so callers have a local file. We still use the cache dir for simplicity.
        }

        // Cache miss: fetch and write
        let data = fetch_fn()?;
        self.write_cached(&cache_path, &data);
        Ok(cache_path)
    }

    /// Remove all cached data.
    pub fn clear(&self) -> std::io::Result<()> {
        if self.cache_dir.exists() {
            std::fs::remove_dir_all(&self.cache_dir)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_cache() -> (tempfile::TempDir, MetadataCache) {
        let tmp = tempfile::tempdir().unwrap();
        let cache = MetadataCache {
            cache_dir: tmp.path().join("genohype"),
        };
        (tmp, cache)
    }

    #[test]
    fn test_url_to_cache_dir() {
        let (_tmp, cache) = temp_cache();
        let dir = cache.url_to_cache_dir("gs://my-bucket/path/to/table.ht");
        assert!(dir.ends_with("gs/my-bucket/path/to/table.ht"));
    }

    #[test]
    fn test_url_to_cache_dir_strips_trailing_slash() {
        let (_tmp, cache) = temp_cache();
        let d1 = cache.url_to_cache_dir("gs://bucket/table.ht/");
        let d2 = cache.url_to_cache_dir("gs://bucket/table.ht");
        assert_eq!(d1, d2);
    }

    #[test]
    fn test_url_to_cache_dir_prevents_traversal() {
        let (_tmp, cache) = temp_cache();
        let dir = cache.url_to_cache_dir("gs://bucket/../../../etc/passwd");
        let dir_str = dir.to_string_lossy();
        assert!(!dir_str.contains(".."));
    }

    #[test]
    fn test_cache_miss_then_hit() {
        let (_tmp, cache) = temp_cache();
        let opts = CacheOptions::default();
        let call_count = std::sync::atomic::AtomicUsize::new(0);

        // First call: cache miss, fetches
        let data = cache
            .get_or_fetch("gs://bucket/table.ht", "metadata.json.gz", &opts, || {
                call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(b"test-metadata".to_vec())
            })
            .unwrap();
        assert_eq!(data, b"test-metadata");
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);

        // Second call: cache hit, no fetch
        let data = cache
            .get_or_fetch("gs://bucket/table.ht", "metadata.json.gz", &opts, || {
                call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(b"should-not-be-called".to_vec())
            })
            .unwrap();
        assert_eq!(data, b"test-metadata");
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn test_force_refresh_skips_read() {
        let (_tmp, cache) = temp_cache();
        let opts_normal = CacheOptions::default();
        let opts_refresh = CacheOptions {
            force_refresh: true,
            ..Default::default()
        };

        // Populate cache
        cache
            .get_or_fetch("gs://bucket/t.ht", "m.json.gz", &opts_normal, || {
                Ok(b"old-data".to_vec())
            })
            .unwrap();

        // Force refresh: should call fetch again
        let data = cache
            .get_or_fetch("gs://bucket/t.ht", "m.json.gz", &opts_refresh, || {
                Ok(b"new-data".to_vec())
            })
            .unwrap();
        assert_eq!(data, b"new-data");
    }

    #[test]
    fn test_disabled_cache() {
        let (_tmp, cache) = temp_cache();
        let opts = CacheOptions::disabled();

        let data = cache
            .get_or_fetch("gs://bucket/t.ht", "m.json.gz", &opts, || {
                Ok(b"fetched".to_vec())
            })
            .unwrap();
        assert_eq!(data, b"fetched");

        // Should not have written to cache
        let dir = cache.url_to_cache_dir("gs://bucket/t.ht");
        assert!(!dir.join("m.json.gz").exists());
    }

    #[test]
    fn test_expired_ttl() {
        let (_tmp, cache) = temp_cache();

        // Write a file and backdate its mtime
        let dir = cache.url_to_cache_dir("gs://bucket/t.ht");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("m.json.gz");
        std::fs::write(&path, b"stale").unwrap();

        // Set mtime to 25 hours ago
        let old_time = SystemTime::now() - Duration::from_secs(25 * 3600);
        filetime::set_file_mtime(&path, filetime::FileTime::from_system_time(old_time)).unwrap();

        let opts = CacheOptions::default(); // 24h TTL
        let data = cache
            .get_or_fetch("gs://bucket/t.ht", "m.json.gz", &opts, || {
                Ok(b"fresh".to_vec())
            })
            .unwrap();
        assert_eq!(data, b"fresh");
    }

    #[test]
    fn test_get_or_fetch_file_returns_path() {
        let (_tmp, cache) = temp_cache();
        let opts = CacheOptions::default();
        let call_count = std::sync::atomic::AtomicUsize::new(0);

        // First call: cache miss, fetches and writes file
        let path = cache
            .get_or_fetch_file(
                "gs://bucket/table.ht/index/part-0.idx",
                "index",
                &opts,
                || {
                    call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(b"index-data".to_vec())
                },
            )
            .unwrap();
        assert!(path.exists());
        assert_eq!(std::fs::read(&path).unwrap(), b"index-data");
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);

        // Second call: cache hit, returns same path without fetching
        let path2 = cache
            .get_or_fetch_file(
                "gs://bucket/table.ht/index/part-0.idx",
                "index",
                &opts,
                || {
                    call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(b"should-not-be-called".to_vec())
                },
            )
            .unwrap();
        assert_eq!(path, path2);
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn test_clear() {
        let (_tmp, cache) = temp_cache();
        let opts = CacheOptions::default();

        cache
            .get_or_fetch("gs://bucket/t.ht", "m.json.gz", &opts, || {
                Ok(b"data".to_vec())
            })
            .unwrap();

        assert!(cache.cache_dir.exists());
        cache.clear().unwrap();
        assert!(!cache.cache_dir.exists());
    }
}
