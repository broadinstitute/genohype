//! Private, generation-based lifecycle for indexed association dataset sessions.
//!
//! This module intentionally exposes no constructor for caller-selected cache roots in
//! production. Source paths are used only to make an in-memory observation and are never
//! written to the registry, completion marker, manifest, or browser metadata.

use fs2::FileExt;
use genohype_association::{
    validate_raster_dimensions, AssociationBuildCancellation, AssociationSessionCompletionV1,
    AssociationSortDirectionV1, AssociationStore, AssociationTablePageRequestV1,
    AssociationTableSortV1, AssociationVisualizationManifestV1, CompletedArtifactV1, DatasetIdV1,
    NormalizedFieldMappingV1, SessionBackgroundV1, SessionKeyInputsV1, SqliteAssociationStore,
    MAX_ENCODED_RASTER_BYTES, SCHEMA_VERSION_V1, STORE_SCHEMA_VERSION_V1,
};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const REGISTRY_SCHEMA_VERSION: u32 = 1;
const COMPLETE_MAX_BYTES: u64 = 64 * 1024;
const MANIFEST_MAX_BYTES: u64 = 16 * 1024 * 1024;
const DATABASE_NAME: &str = "associations.sqlite3";
const RASTER_NAME: &str = "raster.png";
const MANIFEST_NAME: &str = "manifest.json";
const COMPLETE_NAME: &str = "complete.json";
const ACTIVE_LOCK_NAME: &str = ".active.lock";
const CACHE_CLEAR_LOCK_NAME: &str = ".genohype-cache.lock";
const STAGING_PROTOCOL_LOCK_NAME: &str = ".staging-protocol.lock";
const STREAM_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct NormalizedSessionRequestV1 {
    pub association_schema_version: String,
    pub store_schema_version: u32,
    pub mapping: NormalizedFieldMappingV1,
    pub assembly: String,
    pub thresholds: Vec<genohype_association::AssociationThresholdV1>,
    pub significance_threshold: f64,
    pub top_hits: usize,
    pub width: u32,
    pub height: u32,
    pub layout_hash: String,
    pub point_radius: f32,
    pub point_alpha: u8,
    pub background: SessionBackgroundV1,
    pub renderer_version: String,
}
impl NormalizedSessionRequestV1 {
    fn with_source(&self, source_fingerprint: String) -> SessionKeyInputsV1 {
        SessionKeyInputsV1 {
            association_schema_version: self.association_schema_version.clone(),
            store_schema_version: self.store_schema_version,
            source_fingerprint,
            mapping: self.mapping.clone(),
            assembly: self.assembly.clone(),
            thresholds: self.thresholds.clone(),
            significance_threshold: self.significance_threshold,
            top_hits: self.top_hits,
            width: self.width,
            height: self.height,
            layout_hash: self.layout_hash.clone(),
            point_radius: self.point_radius,
            point_alpha: self.point_alpha,
            background: self.background.clone(),
            renderer_version: self.renderer_version.clone(),
        }
    }
    fn validate(&self) -> Result<(), AssociationSessionError> {
        self.with_source(format!("sha256:{}", "0".repeat(64)))
            .validate()
            .map_err(|_| AssociationSessionError::InvalidRequest)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SessionLookup {
    observation: SourceObservation,
    request_key: String,
    request: NormalizedSessionRequestV1,
    local_path: Option<PathBuf>,
}
impl SessionLookup {
    pub(crate) fn observe_local(
        path: &Path,
        request: NormalizedSessionRequestV1,
    ) -> Result<Self, AssociationSessionError> {
        request.validate()?;
        let observation = SourceObservation::local(path)?;
        let request_key = request_key(&observation.key, &request)?;
        Ok(Self {
            observation,
            request_key,
            request,
            local_path: Some(path.to_path_buf()),
        })
    }

    /// Stable non-path observations (for example, a pinned inventory digest).
    pub(crate) fn observe_stable(
        kind: &str,
        stable_observation: &str,
        request: NormalizedSessionRequestV1,
    ) -> Result<Self, AssociationSessionError> {
        request.validate()?;
        if kind.is_empty() || kind.len() > 64 || !valid_sha256(stable_observation) {
            return Err(AssociationSessionError::InvalidObservation);
        }
        let mut hash = Sha256::new();
        hash.update(b"genohype.association.source-observation.stable.v1\0");
        hash_string(&mut hash, kind);
        hash_string(&mut hash, stable_observation);
        let observation = SourceObservation {
            key: digest(hash),
            local_identity: None,
        };
        let request_key = request_key(&observation.key, &request)?;
        Ok(Self {
            observation,
            request_key,
            request,
            local_path: None,
        })
    }

    pub(crate) fn request_key(&self) -> &str {
        &self.request_key
    }

    fn source_unchanged(&self) -> Result<bool, AssociationSessionError> {
        match &self.local_path {
            Some(path) => Ok(SourceObservation::local(path)? == self.observation),
            None => Ok(true),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceObservation {
    key: String,
    local_identity: Option<LocalIdentity>,
}
impl SourceObservation {
    fn local(path: &Path) -> Result<Self, AssociationSessionError> {
        let canonical = fs::canonicalize(path).map_err(private_io)?;
        let metadata = fs::metadata(&canonical).map_err(private_io)?;
        if !metadata.is_file() {
            return Err(AssociationSessionError::InvalidObservation);
        }
        let identity = LocalIdentity::from_metadata(&metadata);
        let mut hash = Sha256::new();
        hash.update(b"genohype.association.source-observation.local.v1\0");
        hash_bytes(&mut hash, canonical.as_os_str().as_encoded_bytes());
        identity.hash_into(&mut hash);
        Ok(Self {
            key: digest(hash),
            local_identity: Some(identity),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalIdentity {
    len: u64,
    modified_ns: u128,
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
    #[cfg(unix)]
    ctime: i64,
    #[cfg(unix)]
    ctime_nsec: i64,
}
impl LocalIdentity {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        let modified_ns = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map_or(0, |duration| duration.as_nanos());
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Self {
                len: metadata.len(),
                modified_ns,
                dev: metadata.dev(),
                ino: metadata.ino(),
                ctime: metadata.ctime(),
                ctime_nsec: metadata.ctime_nsec(),
            }
        }
        #[cfg(not(unix))]
        Self {
            len: metadata.len(),
            modified_ns,
        }
    }
    fn hash_into(&self, hash: &mut Sha256) {
        hash.update(self.len.to_be_bytes());
        hash.update(self.modified_ns.to_be_bytes());
        #[cfg(unix)]
        {
            hash.update(self.dev.to_be_bytes());
            hash.update(self.ino.to_be_bytes());
            hash.update(self.ctime.to_be_bytes());
            hash.update(self.ctime_nsec.to_be_bytes());
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SessionPolicy {
    pub rebuild: bool,
    pub ephemeral: bool,
    pub retention: Duration,
}
impl Default for SessionPolicy {
    fn default() -> Self {
        Self {
            rebuild: false,
            ephemeral: false,
            retention: Duration::from_secs(7 * 24 * 60 * 60),
        }
    }
}
impl SessionPolicy {
    pub(crate) fn expires_at(
        &self,
        created_at: SystemTime,
    ) -> Result<Option<SystemTime>, AssociationSessionError> {
        if self.ephemeral {
            Ok(None)
        } else {
            created_at
                .checked_add(self.retention)
                .map(Some)
                .ok_or(AssociationSessionError::InvalidTime)
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CleanupPolicy {
    pub now: SystemTime,
    pub remove_expired: bool,
    pub remove_orphans: bool,
    pub remove_staging: bool,
}
impl Default for CleanupPolicy {
    fn default() -> Self {
        Self {
            now: SystemTime::now(),
            remove_expired: true,
            remove_orphans: true,
            remove_staging: true,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CleanupSummary {
    pub staging_removed: u64,
    pub sessions_removed: u64,
    pub retained_locked: u64,
    pub refused_entries: u64,
}

#[derive(Clone)]
pub(crate) struct AssociationSessionRegistry {
    root: Arc<PathBuf>,
    /// Persistent and deliberately outside `<cache>/genohype`, which broad
    /// cache clear removes. Recreating the cache tree can never replace the
    /// inode on which active sessions hold their shared lock.
    cache_clear_lock: Arc<PathBuf>,
    #[cfg(test)]
    after_rename_hook: Arc<std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>>,
    #[cfg(test)]
    before_active_transition_hook: Arc<std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>>,
}
impl AssociationSessionRegistry {
    pub(crate) fn open_default() -> Result<Self, AssociationSessionError> {
        let cache = dirs::cache_dir().ok_or(AssociationSessionError::CacheUnavailable)?;
        let genohype = cache.join("genohype");
        ensure_private_directory(&genohype)?;
        let association = genohype.join("association");
        ensure_private_directory(&association)?;
        Self::open_root(association.join("v1"), cache.join(CACHE_CLEAR_LOCK_NAME))
    }

    #[cfg(test)]
    fn open_for_test(root: PathBuf) -> Result<Self, AssociationSessionError> {
        let parent = root
            .parent()
            .ok_or(AssociationSessionError::CacheUnavailable)?;
        let unique = root
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(AssociationSessionError::CacheUnavailable)?;
        let clear_lock = parent.join(format!(".{unique}-genohype-cache.lock"));
        Self::open_root(root, clear_lock)
    }

    fn open_root(
        root: PathBuf,
        cache_clear_lock: PathBuf,
    ) -> Result<Self, AssociationSessionError> {
        ensure_private_directory(&root)?;
        for child in ["claims", "staging", "sessions"] {
            ensure_private_directory(&root.join(child))?;
        }
        let protocol = open_private_lock(&root.join(STAGING_PROTOCOL_LOCK_NAME))?;
        drop(protocol);
        let clear = open_private_lock(&cache_clear_lock)?;
        drop(clear);
        let registry = Self {
            root: Arc::new(root),
            cache_clear_lock: Arc::new(cache_clear_lock),
            #[cfg(test)]
            after_rename_hook: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            before_active_transition_hook: Arc::new(std::sync::Mutex::new(None)),
        };
        registry.initialize_registry()?;
        Ok(registry)
    }

    #[cfg(test)]
    fn set_after_rename_hook(&self, hook: impl FnOnce() + Send + 'static) {
        *self.after_rename_hook.lock().unwrap() = Some(Box::new(hook));
    }

    #[cfg(test)]
    fn run_after_rename_hook(&self) {
        let hook = self.after_rename_hook.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    fn set_before_active_transition_hook(&self, hook: impl FnOnce() + Send + 'static) {
        *self.before_active_transition_hook.lock().unwrap() = Some(Box::new(hook));
    }

    #[cfg(test)]
    fn run_before_active_transition_hook(&self) {
        let hook = self.before_active_transition_hook.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
    }

    pub(crate) fn lookup_valid(
        &self,
        lookup: &SessionLookup,
        now: SystemTime,
    ) -> Result<Option<CompletedAssociationSession>, AssociationSessionError> {
        let now = system_seconds(now)?;
        let Some(row) = self.registry_row(&lookup.request_key)? else {
            return Ok(None);
        };
        if row.observation_key != lookup.observation.key
            || row.expires_at.is_some_and(|expiry| now >= expiry)
        {
            return Ok(None);
        }
        match self.validate_generation(
            &row.dataset_id,
            Some(&row.session_key),
            Some(&row.marker_checksum),
            Some(&lookup.request),
            false,
            None,
        ) {
            Ok(session) => {
                let expected_session_key = lookup
                    .request
                    .with_source(session.completion().source_fingerprint.clone())
                    .session_key()
                    .map_err(|_| AssociationSessionError::CorruptSession)?;
                let marker_expiry = session
                    .completion()
                    .expires_at
                    .as_deref()
                    .map(parse_time)
                    .transpose()
                    .map_err(|_| AssociationSessionError::CorruptSession)?;
                if expected_session_key != row.session_key || marker_expiry != row.expires_at {
                    Ok(None)
                } else {
                    Ok(Some(session))
                }
            }
            Err(_) => Ok(None),
        }
    }

    pub(crate) fn claim(
        &self,
        lookup: SessionLookup,
    ) -> Result<BuildClaim, AssociationSessionError> {
        let global_lock = open_private_lock(&self.cache_clear_lock)?;
        FileExt::lock_shared(&global_lock).map_err(private_io)?;
        let claim_path = self
            .root
            .join("claims")
            .join(format!("{}.lock", lookup.request_key));
        let request_lock = open_private_lock(&claim_path)?;
        FileExt::lock_exclusive(&request_lock).map_err(private_io)?;

        let dataset_id = DatasetIdV1::new(Uuid::new_v4().to_string())
            .map_err(|_| AssociationSessionError::InvalidDataset)?;
        // Exclude staging scavengers only for the tiny directory/lock creation
        // window. A crash before `.active.lock` is then safely distinguishable
        // from a live creator by cleanup holding this same protocol lock.
        let protocol = open_private_lock(&self.root.join(STAGING_PROTOCOL_LOCK_NAME))?;
        FileExt::lock_exclusive(&protocol).map_err(private_io)?;
        let staging_name = format!(".{}.building", Uuid::new_v4());
        let staging_path = self.root.join("staging").join(staging_name);
        create_private_directory(&staging_path)?;
        let active_path = staging_path.join(ACTIVE_LOCK_NAME);
        let active_lock = open_private_lock(&active_path)?;
        FileExt::lock_exclusive(&active_lock).map_err(private_io)?;
        drop(protocol);
        let database_path = staging_path.join(DATABASE_NAME);
        let database_file = open_private_lock(&database_path)?;
        drop(database_file);

        Ok(BuildClaim {
            registry: self.clone(),
            lookup,
            dataset_id,
            database_path,
            staging_path: Some(staging_path),
            generation_path: None,
            request_lock: Some(request_lock),
            active_lock: Some(active_lock),
            global_lock: Some(global_lock),
        })
    }

    pub(crate) fn cleanup(
        &self,
        policy: CleanupPolicy,
    ) -> Result<CleanupSummary, AssociationSessionError> {
        let now = system_seconds(policy.now)?;
        let global = open_private_lock(&self.cache_clear_lock)?;
        FileExt::lock_shared(&global).map_err(private_io)?;
        let mut summary = CleanupSummary::default();
        if policy.remove_staging {
            let protocol = open_private_lock(&self.root.join(STAGING_PROTOCOL_LOCK_NAME))?;
            FileExt::lock_exclusive(&protocol).map_err(private_io)?;
            for entry in fs::read_dir(self.root.join("staging")).map_err(private_io)? {
                let entry = entry.map_err(private_io)?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if !valid_staging_name(&name) || !entry.file_type().map_err(private_io)?.is_dir() {
                    summary.refused_entries += 1;
                    continue;
                }
                match try_lock_active(&entry.path(), true)? {
                    Some(lock) => {
                        if remove_generation_directory(&entry.path()).is_ok() {
                            summary.staging_removed += 1;
                        } else {
                            summary.refused_entries += 1;
                        }
                        drop(lock);
                    }
                    None => summary.retained_locked += 1,
                }
            }
            drop(protocol);
        }

        let registered = self.registered_sessions()?;
        for entry in fs::read_dir(self.root.join("sessions")).map_err(private_io)? {
            let entry = entry.map_err(private_io)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if DatasetIdV1::new(name.clone()).is_err()
                || !entry.file_type().map_err(private_io)?.is_dir()
            {
                summary.refused_entries += 1;
                continue;
            }
            let registered_expiry = registered.get(&name).copied().flatten();
            let expired = registered_expiry.is_some_and(|expiry| now >= expiry);
            let orphan = !registered.contains_key(&name);
            if !(policy.remove_expired && expired || policy.remove_orphans && orphan) {
                continue;
            }
            match try_lock_active(&entry.path(), false)? {
                Some(lock) => {
                    // Recheck under the active lock so a concurrent publication cannot be removed.
                    let current = self.registered_sessions()?;
                    let current_expiry = current.get(&name).copied().flatten();
                    let still_expired = current_expiry.is_some_and(|expiry| now >= expiry);
                    let still_orphan = !current.contains_key(&name);
                    if policy.remove_expired && still_expired
                        || policy.remove_orphans && still_orphan
                    {
                        if remove_generation_directory(&entry.path()).is_ok() {
                            summary.sessions_removed += 1;
                            self.delete_registry_dataset(&name)?;
                        } else {
                            summary.refused_entries += 1;
                        }
                    }
                    drop(lock);
                }
                None => summary.retained_locked += 1,
            }
        }
        Ok(summary)
    }

    /// Acquire the exclusive global lock required before broad Genohype cache removal.
    pub(crate) fn try_claim_broad_cache_clear(
        &self,
    ) -> Result<Option<BroadCacheClearGuard>, AssociationSessionError> {
        let file = open_private_lock(&self.cache_clear_lock)?;
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => Ok(Some(BroadCacheClearGuard { _file: file })),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(private_io(error)),
        }
    }

    fn initialize_registry(&self) -> Result<(), AssociationSessionError> {
        let path = self.root.join("registry.sqlite3");
        reject_symlink_if_present(&path)?;
        let connection = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )
        .map_err(private_db)?;
        set_private_file(&path)?;
        connection
            .pragma_update(None, "journal_mode", "DELETE")
            .map_err(private_db)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(private_db)?;
        let version: u32 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(private_db)?;
        if version == 0 {
            connection
                .execute_batch(
                    "BEGIN IMMEDIATE;
                     CREATE TABLE association_sessions(
                       request_key TEXT PRIMARY KEY,
                       observation_key TEXT NOT NULL,
                       dataset_id TEXT NOT NULL UNIQUE,
                       session_key TEXT NOT NULL,
                       expires_at INTEGER,
                       marker_checksum TEXT NOT NULL
                     ) STRICT;
                     PRAGMA user_version=1;
                     COMMIT;",
                )
                .map_err(private_db)?;
        } else if version != REGISTRY_SCHEMA_VERSION {
            return Err(AssociationSessionError::CorruptRegistry);
        }
        let columns = connection
            .prepare("PRAGMA table_info(association_sessions)")
            .and_then(|mut statement| {
                statement
                    .query_map([], |row| row.get::<_, String>(1))?
                    .collect::<Result<Vec<_>, _>>()
            })
            .map_err(private_db)?;
        if columns
            != [
                "request_key",
                "observation_key",
                "dataset_id",
                "session_key",
                "expires_at",
                "marker_checksum",
            ]
        {
            return Err(AssociationSessionError::CorruptRegistry);
        }
        drop(connection);
        sync_file(&path)?;
        sync_directory(&self.root)?;
        Ok(())
    }

    fn connection(&self) -> Result<Connection, AssociationSessionError> {
        reject_regular_file(&self.root.join("registry.sqlite3"))?;
        let connection = Connection::open_with_flags(
            self.root.join("registry.sqlite3"),
            OpenFlags::SQLITE_OPEN_READ_WRITE,
        )
        .map_err(private_db)?;
        connection
            .pragma_update(None, "journal_mode", "DELETE")
            .map_err(private_db)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(private_db)?;
        Ok(connection)
    }

    fn registry_row(
        &self,
        request_key: &str,
    ) -> Result<Option<RegistryRow>, AssociationSessionError> {
        self.connection()?
            .query_row(
                "SELECT observation_key,dataset_id,session_key,expires_at,marker_checksum FROM association_sessions WHERE request_key=?1",
                [request_key],
                |row| {
                    Ok(RegistryRow {
                        observation_key: row.get(0)?,
                        dataset_id: row.get(1)?,
                        session_key: row.get(2)?,
                        expires_at: row.get(3)?,
                        marker_checksum: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(private_db)
    }

    fn publish_registry(
        &self,
        lookup: &SessionLookup,
        marker: &AssociationSessionCompletionV1,
        marker_checksum: &str,
    ) -> Result<(), AssociationSessionError> {
        let expires_at = marker.expires_at.as_deref().map(parse_time).transpose()?;
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(private_db)?;
        transaction
            .execute(
                "INSERT INTO association_sessions(request_key,observation_key,dataset_id,session_key,expires_at,marker_checksum)
                 VALUES(?1,?2,?3,?4,?5,?6)
                 ON CONFLICT(request_key) DO UPDATE SET
                   observation_key=excluded.observation_key,dataset_id=excluded.dataset_id,
                   session_key=excluded.session_key,expires_at=excluded.expires_at,
                   marker_checksum=excluded.marker_checksum",
                params![
                    lookup.request_key,
                    lookup.observation.key,
                    marker.dataset_id.as_str(),
                    marker.session_key,
                    expires_at,
                    marker_checksum,
                ],
            )
            .map_err(private_db)?;

        // SQLite FULL commit is the final fallible pointer-publication boundary.
        // There are intentionally no file/directory fsync calls afterward: those
        // could report failure after the durable row already replaced the old one.
        if transaction.commit().is_ok() {
            return Ok(());
        }
        drop(connection);

        // A commit error can be outcome-ambiguous. Recover success iff a fresh
        // connection proves the exact intended row won; otherwise report failure
        // and leave the previous pointer authoritative.
        let committed = self.registry_row(&lookup.request_key)?.is_some_and(|row| {
            row.observation_key == lookup.observation.key
                && row.dataset_id == marker.dataset_id.as_str()
                && row.session_key == marker.session_key
                && row.expires_at == expires_at
                && row.marker_checksum == marker_checksum
        });
        if committed {
            Ok(())
        } else {
            Err(AssociationSessionError::PrivateDatabase)
        }
    }

    fn validate_generation(
        &self,
        dataset_id: &str,
        expected_session_key: Option<&str>,
        expected_marker_checksum: Option<&str>,
        expected_request: Option<&NormalizedSessionRequestV1>,
        ephemeral: bool,
        held_active_lock: Option<File>,
    ) -> Result<CompletedAssociationSession, AssociationSessionError> {
        let id = DatasetIdV1::new(dataset_id.to_string())
            .map_err(|_| AssociationSessionError::CorruptSession)?;
        let directory = self.root.join("sessions").join(dataset_id);
        reject_directory(&directory)?;
        validate_generation_entries(&directory)?;

        let marker_bytes = read_bounded(&directory.join(COMPLETE_NAME), COMPLETE_MAX_BYTES)?;
        let marker_checksum = checksum(&marker_bytes);
        if expected_marker_checksum.is_some_and(|expected| expected != marker_checksum) {
            return Err(AssociationSessionError::CorruptSession);
        }
        let marker: AssociationSessionCompletionV1 = serde_json::from_slice(&marker_bytes)
            .map_err(|_| AssociationSessionError::CorruptSession)?;
        marker
            .validate()
            .map_err(|_| AssociationSessionError::CorruptSession)?;
        if marker.dataset_id != id
            || expected_session_key.is_some_and(|expected| expected != marker.session_key)
        {
            return Err(AssociationSessionError::CorruptSession);
        }
        validate_artifact(&directory.join(DATABASE_NAME), &marker.database)?;
        let raster = read_and_validate_artifact(
            &directory.join(RASTER_NAME),
            &marker.raster,
            MAX_ENCODED_RASTER_BYTES,
        )?;
        let manifest_bytes = read_and_validate_artifact(
            &directory.join(MANIFEST_NAME),
            &marker.manifest,
            MANIFEST_MAX_BYTES,
        )?;
        validate_png_dimensions(&raster, marker.raster_width, marker.raster_height)?;
        let manifest: AssociationVisualizationManifestV1 = serde_json::from_slice(&manifest_bytes)
            .map_err(|_| AssociationSessionError::CorruptSession)?;
        manifest
            .validate()
            .map_err(|_| AssociationSessionError::CorruptSession)?;
        let table_url = format!("datasets/{id}/table");
        let region_url = format!("datasets/{id}/region");
        if expected_request.is_some_and(|request| !manifest_matches_request(&manifest, request))
            || manifest.plot_id != id.as_str()
            || manifest.source_fingerprint != marker.source_fingerprint
            || manifest.created_at != marker.created_at
            || manifest.layout.width != marker.raster_width
            || manifest.layout.height != marker.raster_height
            || manifest.resources.raster.url != RASTER_NAME
            || manifest.resources.raster.byte_size != Some(marker.raster.byte_size)
            || manifest.resources.raster.checksum.as_deref() != Some(&marker.raster.checksum)
            || manifest
                .resources
                .table_api
                .as_ref()
                .map(|resource| resource.url.as_str())
                != Some(table_url.as_str())
            || manifest
                .resources
                .region_api
                .as_ref()
                .map(|resource| resource.url.as_str())
                != Some(region_url.as_str())
        {
            return Err(AssociationSessionError::CorruptSession);
        }
        validate_database(
            &directory.join(DATABASE_NAME),
            &marker,
            &manifest,
            expected_request,
        )?;

        let active_lock = match held_active_lock {
            Some(lock) => lock,
            None => {
                let lock = open_existing_private_file(&directory.join(ACTIVE_LOCK_NAME))?;
                FileExt::lock_shared(&lock).map_err(private_io)?;
                lock
            }
        };
        let global_lock = open_private_lock(&self.cache_clear_lock)?;
        FileExt::lock_shared(&global_lock).map_err(private_io)?;
        Ok(CompletedAssociationSession {
            inner: Arc::new(CompletedInner {
                dataset_id: id,
                directory,
                marker,
                active_lock: Some(active_lock),
                global_lock: Some(global_lock),
                ephemeral,
            }),
        })
    }

    fn registered_sessions(
        &self,
    ) -> Result<std::collections::HashMap<String, Option<u64>>, AssociationSessionError> {
        let connection = self.connection()?;
        let mut statement = connection
            .prepare("SELECT dataset_id,expires_at FROM association_sessions")
            .map_err(private_db)?;
        let rows = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(private_db)?
            .collect::<Result<_, _>>()
            .map_err(private_db)?;
        Ok(rows)
    }

    fn delete_registry_dataset(&self, dataset_id: &str) -> Result<(), AssociationSessionError> {
        self.connection()?
            .execute(
                "DELETE FROM association_sessions WHERE dataset_id=?1",
                [dataset_id],
            )
            .map_err(private_db)?;
        Ok(())
    }
}

struct RegistryRow {
    observation_key: String,
    dataset_id: String,
    session_key: String,
    expires_at: Option<u64>,
    marker_checksum: String,
}

pub(crate) struct BuildClaim {
    registry: AssociationSessionRegistry,
    lookup: SessionLookup,
    dataset_id: DatasetIdV1,
    database_path: PathBuf,
    staging_path: Option<PathBuf>,
    generation_path: Option<PathBuf>,
    request_lock: Option<File>,
    active_lock: Option<File>,
    global_lock: Option<File>,
}
impl BuildClaim {
    pub(crate) fn dataset_id(&self) -> &DatasetIdV1 {
        &self.dataset_id
    }
    pub(crate) fn database_path(&self) -> &Path {
        &self.database_path
    }
    pub(crate) fn recheck_valid(
        &self,
        now: SystemTime,
    ) -> Result<Option<CompletedAssociationSession>, AssociationSessionError> {
        self.registry.lookup_valid(&self.lookup, now)
    }
    pub(crate) fn write_raster(
        &self,
        bytes: &[u8],
        cancellation: &dyn AssociationBuildCancellation,
    ) -> Result<(), AssociationSessionError> {
        if bytes.len() as u64 > MAX_ENCODED_RASTER_BYTES {
            return Err(AssociationSessionError::InvalidCompletion);
        }
        self.write_artifact(RASTER_NAME, bytes, cancellation)
    }
    pub(crate) fn write_manifest(
        &self,
        manifest: &AssociationVisualizationManifestV1,
        cancellation: &dyn AssociationBuildCancellation,
    ) -> Result<(), AssociationSessionError> {
        manifest
            .validate()
            .map_err(|_| AssociationSessionError::InvalidCompletion)?;
        let mut bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|_| AssociationSessionError::InvalidCompletion)?;
        bytes.push(b'\n');
        self.write_artifact(MANIFEST_NAME, &bytes, cancellation)
    }
    fn write_artifact(
        &self,
        name: &str,
        bytes: &[u8],
        cancellation: &dyn AssociationBuildCancellation,
    ) -> Result<(), AssociationSessionError> {
        check_cancel(cancellation)?;
        let path = self
            .staging_path
            .as_ref()
            .ok_or(AssociationSessionError::ClaimConsumed)?
            .join(name);
        write_new_private(&path, bytes)?;
        check_cancel(cancellation)
    }

    pub(crate) fn publish(
        mut self,
        completion: SessionCompletion,
        cancellation: &dyn AssociationBuildCancellation,
    ) -> Result<CompletedAssociationSession, AssociationSessionError> {
        check_cancel(cancellation)?;
        if !self.lookup.source_unchanged()? {
            return Err(AssociationSessionError::SourceChanged);
        }
        let staging = self
            .staging_path
            .as_ref()
            .ok_or(AssociationSessionError::ClaimConsumed)?;
        let database_path = staging.join(DATABASE_NAME);
        set_private_file(&database_path)?;
        validate_self_contained_database(&database_path)?;
        sync_file(&database_path)?;
        check_cancel(cancellation)?;

        if completion
            .expires_at
            .is_some_and(|expires| expires <= completion.created_at)
        {
            return Err(AssociationSessionError::InvalidTime);
        }
        let source_fingerprint = completion.source_fingerprint;
        let session_key = self
            .lookup
            .request
            .with_source(source_fingerprint.clone())
            .session_key()
            .map_err(|_| AssociationSessionError::InvalidCompletion)?;
        let database = artifact(&database_path, cancellation)?;
        let raster = artifact(&staging.join(RASTER_NAME), cancellation)?;
        if raster.byte_size > MAX_ENCODED_RASTER_BYTES {
            return Err(AssociationSessionError::InvalidCompletion);
        }
        let manifest_path = staging.join(MANIFEST_NAME);
        let manifest = artifact(&manifest_path, cancellation)?;
        let completed_manifest = serde_json::from_slice::<AssociationVisualizationManifestV1>(
            &read_bounded(&manifest_path, MANIFEST_MAX_BYTES)?,
        )
        .map_err(|_| AssociationSessionError::InvalidCompletion)?;
        completed_manifest
            .validate()
            .map_err(|_| AssociationSessionError::InvalidCompletion)?;
        if completed_manifest.source_fingerprint != source_fingerprint
            || !manifest_matches_request(&completed_manifest, &self.lookup.request)
        {
            return Err(AssociationSessionError::InvalidCompletion);
        }
        let manifest_created_at = completed_manifest.created_at.clone();
        let marker = AssociationSessionCompletionV1 {
            schema_version: SCHEMA_VERSION_V1.into(),
            dataset_id: self.dataset_id.clone(),
            session_key,
            source_fingerprint,
            store_schema_version: STORE_SCHEMA_VERSION_V1,
            database,
            raster,
            manifest,
            raster_width: self.lookup.request.width,
            raster_height: self.lookup.request.height,
            created_at: manifest_created_at,
            expires_at: completion.expires_at.map(format_time).transpose()?,
        };
        marker
            .validate()
            .map_err(|_| AssociationSessionError::InvalidCompletion)?;
        let mut marker_bytes = serde_json::to_vec_pretty(&marker)
            .map_err(|_| AssociationSessionError::InvalidCompletion)?;
        marker_bytes.push(b'\n');
        check_cancel(cancellation)?;
        write_new_private(&staging.join(COMPLETE_NAME), &marker_bytes)?;
        sync_directory(staging)?;
        check_cancel(cancellation)?;

        // Keep the original exclusive active lock continuously held across
        // rename, fsync, validation, and registry publication. A clone is moved
        // into the completed handle only after content validation.
        let staging = self
            .staging_path
            .take()
            .ok_or(AssociationSessionError::ClaimConsumed)?;
        let destination = self
            .registry
            .root
            .join("sessions")
            .join(self.dataset_id.as_str());
        fs::rename(&staging, &destination).map_err(private_io)?;
        self.generation_path = Some(destination);
        sync_directory(&self.registry.root.join("sessions"))?;
        #[cfg(test)]
        self.registry.run_after_rename_hook();
        check_cancel(cancellation)?;

        let marker_checksum = checksum(&marker_bytes);
        let active_for_session = self
            .active_lock
            .as_ref()
            .ok_or(AssociationSessionError::ClaimConsumed)?
            .try_clone()
            .map_err(private_io)?;
        let session = self.registry.validate_generation(
            self.dataset_id.as_str(),
            Some(&marker.session_key),
            Some(&marker_checksum),
            Some(&self.lookup.request),
            completion.ephemeral,
            Some(active_for_session),
        )?;
        check_cancel(cancellation)?;

        if !completion.ephemeral {
            // Publish while the original exclusive active lock is still held.
            self.registry
                .publish_registry(&self.lookup, &marker, &marker_checksum)?;
            #[cfg(test)]
            self.registry.run_before_active_transition_hook();
        } else {
            // Ephemeral generations are intentionally never registered.
            #[cfg(test)]
            self.registry.run_before_active_transition_hook();
        }
        // Retain exclusive ownership in the returned handle. `flock` cannot
        // atomically convert an exclusive lock to a shared lock, and even a
        // registered generation can become cleanup-eligible before that conversion
        // completes (for example when a long build outlives its retention). Keeping
        // the existing lock mode closes that deletion window for both retained and
        // ephemeral sessions. A later opener may acquire a shared lock after this
        // active handle is released.
        // Irreversible success point: no cancellation probe or fallible operation
        // follows registry publication (or completed ephemeral validation).
        self.generation_path.take();
        self.active_lock.take();
        self.release_locks();
        Ok(session)
    }

    pub(crate) fn abandon(mut self) -> Result<(), AssociationSessionError> {
        let result = self.cleanup_staging();
        self.release_locks();
        result
    }

    fn cleanup_staging(&mut self) -> Result<(), AssociationSessionError> {
        if let Some(active) = self.active_lock.as_ref() {
            // Upgrade/retain exclusive ownership before deleting either a staging
            // directory or a renamed-but-unpublished generation.
            FileExt::lock_exclusive(active).map_err(private_io)?;
        }
        let path = self
            .generation_path
            .take()
            .or_else(|| self.staging_path.take());
        if let Some(path) = path {
            remove_generation_directory(&path)?;
        }
        self.active_lock.take();
        Ok(())
    }
    fn release_locks(&mut self) {
        if let Some(file) = self.request_lock.take() {
            let _ = FileExt::unlock(&file);
        }
        if let Some(file) = self.global_lock.take() {
            let _ = FileExt::unlock(&file);
        }
    }
}
impl Drop for BuildClaim {
    fn drop(&mut self) {
        let _ = self.cleanup_staging();
        self.release_locks();
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SessionCompletion {
    pub source_fingerprint: String,
    pub created_at: SystemTime,
    pub expires_at: Option<SystemTime>,
    pub ephemeral: bool,
}

#[derive(Clone)]
pub(crate) struct CompletedAssociationSession {
    inner: Arc<CompletedInner>,
}
impl CompletedAssociationSession {
    pub(crate) fn dataset_id(&self) -> &DatasetIdV1 {
        &self.inner.dataset_id
    }
    pub(crate) fn database_path(&self) -> PathBuf {
        self.inner.directory.join(DATABASE_NAME)
    }
    pub(crate) fn raster_path(&self) -> PathBuf {
        self.inner.directory.join(RASTER_NAME)
    }
    pub(crate) fn manifest_path(&self) -> PathBuf {
        self.inner.directory.join(MANIFEST_NAME)
    }
    pub(crate) fn completion(&self) -> &AssociationSessionCompletionV1 {
        &self.inner.marker
    }
}
struct CompletedInner {
    dataset_id: DatasetIdV1,
    directory: PathBuf,
    marker: AssociationSessionCompletionV1,
    active_lock: Option<File>,
    global_lock: Option<File>,
    ephemeral: bool,
}
impl Drop for CompletedInner {
    fn drop(&mut self) {
        if !self.ephemeral {
            return;
        }
        // Arc uniqueness means this is the final viewer handle. Upgrade the
        // continuously-held active lock before deletion while retaining the
        // shared broad-cache lock, so neither cleanup nor cache clear can race.
        let Some(active) = self.active_lock.as_ref() else {
            return;
        };
        if FileExt::lock_exclusive(active).is_err() {
            return;
        }
        let _ = remove_generation_directory(&self.directory);
        self.active_lock.take();
        self.global_lock.take();
    }
}

pub(crate) struct BroadCacheClearGuard {
    _file: File,
}

fn manifest_matches_request(
    manifest: &AssociationVisualizationManifestV1,
    request: &NormalizedSessionRequestV1,
) -> bool {
    manifest.schema_version == request.association_schema_version
        && manifest.assembly == request.assembly
        && manifest.thresholds == request.thresholds
        && manifest
            .thresholds
            .iter()
            .filter(|threshold| {
                threshold.pvalue.to_bits() == request.significance_threshold.to_bits()
            })
            .count()
            == 1
        && manifest.layout.width == request.width
        && manifest.layout.height == request.height
        && manifest.layout_hash == request.layout_hash
        && manifest.genohype_version == request.renderer_version
        && manifest.top_hits.len() as u64 == (request.top_hits as u64).min(manifest.valid_row_count)
}

fn validate_database(
    path: &Path,
    marker: &AssociationSessionCompletionV1,
    manifest: &AssociationVisualizationManifestV1,
    expected_request: Option<&NormalizedSessionRequestV1>,
) -> Result<(), AssociationSessionError> {
    validate_self_contained_database(path)?;
    let store = SqliteAssociationStore::open_read_only(path)
        .map_err(|_| AssociationSessionError::CorruptSession)?;
    if !store
        .is_complete(&marker.dataset_id)
        .map_err(|_| AssociationSessionError::CorruptSession)?
    {
        return Err(AssociationSessionError::CorruptSession);
    }
    let connection =
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(private_db)?;
    let (fingerprint, created_at, status, count): (String, String, String, u64) = connection
        .query_row(
            "SELECT d.source_fingerprint,d.created_at,d.status,COUNT(p.source_ordinal)
             FROM association_datasets d LEFT JOIN association_points p USING(dataset_id)
             WHERE d.dataset_id=?1 GROUP BY d.dataset_id",
            [marker.dataset_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(|_| AssociationSessionError::CorruptSession)?;
    let dataset_count: u64 = connection
        .query_row("SELECT COUNT(*) FROM association_datasets", [], |row| {
            row.get(0)
        })
        .map_err(private_db)?;
    let top_hit_count: u64 = connection
        .query_row(
            "SELECT COUNT(*) FROM top_hits WHERE dataset_id=?1",
            [marker.dataset_id.as_str()],
            |row| row.get(0),
        )
        .map_err(private_db)?;
    let significant_hit_count = expected_request
        .map(|request| {
            connection
                .query_row(
                    "SELECT COUNT(*) FROM association_points WHERE dataset_id=?1 AND pvalue < ?2",
                    params![marker.dataset_id.as_str(), request.significance_threshold],
                    |row| row.get::<_, u64>(0),
                )
                .map_err(private_db)
        })
        .transpose()?;
    if fingerprint != marker.source_fingerprint
        || created_at != marker.created_at
        || status != "complete"
        || dataset_count != 1
        || count != manifest.valid_row_count
        || top_hit_count != manifest.top_hits.len() as u64
        || significant_hit_count.is_some_and(|count| count != manifest.significant_hit_count)
    {
        return Err(AssociationSessionError::CorruptSession);
    }
    if !manifest.top_hits.is_empty() {
        let stored = store
            .top_hits(&marker.dataset_id, manifest.top_hits.len() as u32)
            .map_err(|_| AssociationSessionError::CorruptSession)?;
        if stored != manifest.top_hits {
            return Err(AssociationSessionError::CorruptSession);
        }
    } else {
        let count: u64 = connection
            .query_row(
                "SELECT COUNT(*) FROM top_hits WHERE dataset_id=?1",
                [marker.dataset_id.as_str()],
                |row| row.get(0),
            )
            .map_err(private_db)?;
        if count != 0 {
            return Err(AssociationSessionError::CorruptSession);
        }
    }
    // Exercise the typed page contract as a final store-state check when rows exist.
    if manifest.valid_row_count > 0 {
        let page = store
            .table_page(&AssociationTablePageRequestV1 {
                dataset_id: marker.dataset_id.clone(),
                offset: 0,
                limit: 1,
                sort: AssociationTableSortV1::Pvalue,
                direction: AssociationSortDirectionV1::Ascending,
                search: None,
            })
            .map_err(|_| AssociationSessionError::CorruptSession)?;
        if page.total != manifest.valid_row_count {
            return Err(AssociationSessionError::CorruptSession);
        }
    }
    Ok(())
}

fn validate_self_contained_database(path: &Path) -> Result<(), AssociationSessionError> {
    reject_regular_file(path)?;
    for suffix in ["-wal", "-shm"] {
        if path
            .with_file_name(format!("{DATABASE_NAME}{suffix}"))
            .exists()
        {
            return Err(AssociationSessionError::CorruptSession);
        }
    }
    let connection =
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(private_db)?;
    let journal: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .map_err(private_db)?;
    let version: u32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(private_db)?;
    if journal.eq_ignore_ascii_case("wal") || version != STORE_SCHEMA_VERSION_V1 {
        return Err(AssociationSessionError::CorruptSession);
    }
    let check: String = connection
        .query_row("PRAGMA quick_check(1)", [], |row| row.get(0))
        .map_err(private_db)?;
    if check == "ok" {
        Ok(())
    } else {
        Err(AssociationSessionError::CorruptSession)
    }
}

fn validate_generation_entries(directory: &Path) -> Result<(), AssociationSessionError> {
    let expected: HashSet<&str> = [
        DATABASE_NAME,
        RASTER_NAME,
        MANIFEST_NAME,
        COMPLETE_NAME,
        ACTIVE_LOCK_NAME,
    ]
    .into_iter()
    .collect();
    let mut actual = HashSet::new();
    for entry in fs::read_dir(directory).map_err(private_io)? {
        let entry = entry.map_err(private_io)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(AssociationSessionError::CorruptSession);
        };
        if !expected.contains(name) || !entry.file_type().map_err(private_io)?.is_file() {
            return Err(AssociationSessionError::CorruptSession);
        }
        actual.insert(name.to_string());
    }
    if actual.len() != expected.len() {
        return Err(AssociationSessionError::CorruptSession);
    }
    Ok(())
}

fn remove_generation_directory(path: &Path) -> Result<(), AssociationSessionError> {
    reject_directory(path)?;
    for entry in fs::read_dir(path).map_err(private_io)? {
        let entry = entry.map_err(private_io)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(AssociationSessionError::UnsafeEntry);
        };
        if !matches!(
            name,
            DATABASE_NAME | RASTER_NAME | MANIFEST_NAME | COMPLETE_NAME | ACTIVE_LOCK_NAME
        ) || !entry.file_type().map_err(private_io)?.is_file()
        {
            return Err(AssociationSessionError::UnsafeEntry);
        }
    }
    for name in [
        COMPLETE_NAME,
        MANIFEST_NAME,
        RASTER_NAME,
        DATABASE_NAME,
        ACTIVE_LOCK_NAME,
    ] {
        let child = path.join(name);
        match fs::symlink_metadata(&child) {
            Ok(metadata) if metadata.file_type().is_file() => {
                fs::remove_file(&child).map_err(private_io)?
            }
            Ok(_) => return Err(AssociationSessionError::UnsafeEntry),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(private_io(error)),
        }
    }
    fs::remove_dir(path).map_err(private_io)
}

fn try_lock_active(
    directory: &Path,
    create_if_missing: bool,
) -> Result<Option<File>, AssociationSessionError> {
    let path = directory.join(ACTIVE_LOCK_NAME);
    let file = if create_if_missing {
        // Caller holds the exclusive staging-protocol lock, proving that a
        // missing active lock is crash residue rather than live creation.
        open_private_lock(&path)?
    } else {
        open_existing_private_file(&path)?
    };
    match FileExt::try_lock_exclusive(&file) {
        Ok(()) => Ok(Some(file)),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(error) => Err(private_io(error)),
    }
}

fn artifact(
    path: &Path,
    cancellation: &dyn AssociationBuildCancellation,
) -> Result<CompletedArtifactV1, AssociationSessionError> {
    let (byte_size, checksum) = stream_artifact(path, Some(cancellation))?;
    Ok(CompletedArtifactV1 {
        byte_size,
        checksum,
    })
}
fn validate_artifact(
    path: &Path,
    expected: &CompletedArtifactV1,
) -> Result<(), AssociationSessionError> {
    let (byte_size, checksum) = stream_artifact(path, None)?;
    if byte_size != expected.byte_size || checksum != expected.checksum {
        return Err(AssociationSessionError::CorruptSession);
    }
    Ok(())
}

/// Hash through one fixed stack buffer. This is used for multi-gigabyte SQLite
/// databases on both publication and warm validation paths.
fn stream_artifact(
    path: &Path,
    cancellation: Option<&dyn AssociationBuildCancellation>,
) -> Result<(u64, String), AssociationSessionError> {
    reject_regular_file(path)?;
    let expected_size = fs::metadata(path).map_err(private_io)?.len();
    let mut file = File::open(path).map_err(private_io)?;
    let mut buffer = [0_u8; STREAM_BUFFER_BYTES];
    let mut size = 0_u64;
    let mut hash = Sha256::new();
    loop {
        if let Some(cancellation) = cancellation {
            check_cancel(cancellation)?;
        }
        let read = file.read(&mut buffer).map_err(private_io)?;
        if read == 0 {
            break;
        }
        size = size
            .checked_add(read as u64)
            .ok_or(AssociationSessionError::CorruptSession)?;
        hash.update(&buffer[..read]);
    }
    if size != expected_size {
        return Err(AssociationSessionError::CorruptSession);
    }
    Ok((size, format!("sha256:{:x}", hash.finalize())))
}
fn read_and_validate_artifact(
    path: &Path,
    expected: &CompletedArtifactV1,
    maximum: u64,
) -> Result<Vec<u8>, AssociationSessionError> {
    if expected.byte_size > maximum {
        return Err(AssociationSessionError::CorruptSession);
    }
    let bytes = read_bounded(path, maximum)?;
    if bytes.len() as u64 != expected.byte_size || checksum(&bytes) != expected.checksum {
        return Err(AssociationSessionError::CorruptSession);
    }
    Ok(bytes)
}
fn read_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>, AssociationSessionError> {
    reject_regular_file(path)?;
    let metadata = fs::metadata(path).map_err(private_io)?;
    if metadata.len() > maximum {
        return Err(AssociationSessionError::CorruptSession);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)
        .map_err(private_io)?
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(private_io)?;
    if bytes.len() as u64 > maximum {
        return Err(AssociationSessionError::CorruptSession);
    }
    Ok(bytes)
}

fn validate_png_dimensions(
    bytes: &[u8],
    width: u32,
    height: u32,
) -> Result<(), AssociationSessionError> {
    if bytes.len() as u64 > MAX_ENCODED_RASTER_BYTES
        || validate_raster_dimensions(width, height).is_err()
    {
        return Err(AssociationSessionError::CorruptSession);
    }
    // Inspect only the fixed-size signature/IHDR prefix before invoking a decoder
    // that allocates from encoded dimensions.
    let (encoded_width, encoded_height) =
        png_ihdr_dimensions(bytes).ok_or(AssociationSessionError::CorruptSession)?;
    if validate_raster_dimensions(encoded_width, encoded_height).is_err()
        || encoded_width != width
        || encoded_height != height
        || !valid_png_chunk_crcs(bytes)
    {
        return Err(AssociationSessionError::CorruptSession);
    }
    // The bounded IHDR now makes decoder allocation safe. Full decode validates
    // image structure, truncation, and DEFLATE data; the explicit chunk walk
    // above enforces every PNG CRC.
    let pixmap = tiny_skia::Pixmap::decode_png(bytes)
        .map_err(|_| AssociationSessionError::CorruptSession)?;
    if pixmap.width() != width || pixmap.height() != height {
        return Err(AssociationSessionError::CorruptSession);
    }
    Ok(())
}

fn png_ihdr_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    let prefix = bytes.get(..33)?;
    if &prefix[..8] != b"\x89PNG\r\n\x1a\n"
        || u32::from_be_bytes(prefix[8..12].try_into().ok()?) != 13
        || &prefix[12..16] != b"IHDR"
    {
        return None;
    }
    Some((
        u32::from_be_bytes(prefix[16..20].try_into().ok()?),
        u32::from_be_bytes(prefix[20..24].try_into().ok()?),
    ))
}

fn valid_png_chunk_crcs(bytes: &[u8]) -> bool {
    if !bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return false;
    }
    let mut offset = 8_usize;
    let mut saw_iend = false;
    while offset < bytes.len() {
        let Some(header) = bytes.get(offset..offset.saturating_add(8)) else {
            return false;
        };
        let length = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
        let chunk_end = match offset
            .checked_add(12)
            .and_then(|base| base.checked_add(length))
        {
            Some(end) if end <= bytes.len() => end,
            _ => return false,
        };
        let kind_and_data = &bytes[offset + 4..offset + 8 + length];
        let expected =
            u32::from_be_bytes(bytes[offset + 8 + length..chunk_end].try_into().unwrap());
        if png_crc32(kind_and_data) != expected {
            return false;
        }
        if &header[4..8] == b"IEND" {
            saw_iend = length == 0 && chunk_end == bytes.len();
            break;
        }
        offset = chunk_end;
    }
    saw_iend
}

fn png_crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

fn ensure_private_directory(path: &Path) -> Result<(), AssociationSessionError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => set_private_directory(path),
        Ok(_) => Err(AssociationSessionError::UnsafeEntry),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            create_private_directory(path)
        }
        Err(error) => Err(private_io(error)),
    }
}
fn create_private_directory(path: &Path) -> Result<(), AssociationSessionError> {
    fs::create_dir(path).map_err(private_io)?;
    set_private_directory(path)
}
fn reject_directory(path: &Path) -> Result<(), AssociationSessionError> {
    let metadata = fs::symlink_metadata(path).map_err(private_io)?;
    if metadata.file_type().is_dir() {
        Ok(())
    } else {
        Err(AssociationSessionError::UnsafeEntry)
    }
}
fn reject_regular_file(path: &Path) -> Result<(), AssociationSessionError> {
    let metadata = fs::symlink_metadata(path).map_err(private_io)?;
    if metadata.file_type().is_file() {
        Ok(())
    } else {
        Err(AssociationSessionError::UnsafeEntry)
    }
}
fn reject_symlink_if_present(path: &Path) -> Result<(), AssociationSessionError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(AssociationSessionError::UnsafeEntry),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(private_io(error)),
    }
}

fn open_private_lock(path: &Path) -> Result<File, AssociationSessionError> {
    reject_symlink_if_present(path)?;
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path).map_err(private_io)?;
    set_private_file(path)?;
    Ok(file)
}
fn open_existing_private_file(path: &Path) -> Result<File, AssociationSessionError> {
    reject_regular_file(path)?;
    set_private_file(path)?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(private_io)
}
fn write_new_private(path: &Path, bytes: &[u8]) -> Result<(), AssociationSessionError> {
    reject_symlink_if_present(path)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(private_io)?;
    file.write_all(bytes).map_err(private_io)?;
    file.sync_all().map_err(private_io)?;
    set_private_file(path)
}
fn sync_file(path: &Path) -> Result<(), AssociationSessionError> {
    reject_regular_file(path)?;
    File::open(path)
        .map_err(private_io)?
        .sync_all()
        .map_err(private_io)
}
fn sync_directory(path: &Path) -> Result<(), AssociationSessionError> {
    reject_directory(path)?;
    File::open(path)
        .map_err(private_io)?
        .sync_all()
        .map_err(private_io)
}

#[cfg(unix)]
fn set_private_directory(path: &Path) -> Result<(), AssociationSessionError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(private_io)
}
#[cfg(not(unix))]
fn set_private_directory(_path: &Path) -> Result<(), AssociationSessionError> {
    Ok(())
}
#[cfg(unix)]
fn set_private_file(path: &Path) -> Result<(), AssociationSessionError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(private_io)
}
#[cfg(not(unix))]
fn set_private_file(_path: &Path) -> Result<(), AssociationSessionError> {
    Ok(())
}

fn request_key(
    observation_key: &str,
    request: &NormalizedSessionRequestV1,
) -> Result<String, AssociationSessionError> {
    let bytes = serde_json::to_vec(request).map_err(|_| AssociationSessionError::InvalidRequest)?;
    let mut hash = Sha256::new();
    hash.update(b"genohype.association.request-key.v1\0");
    hash_string(&mut hash, observation_key);
    hash_bytes(&mut hash, &bytes);
    Ok(digest(hash))
}
fn hash_string(hash: &mut Sha256, value: &str) {
    hash_bytes(hash, value.as_bytes());
}
fn hash_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}
fn digest(hash: Sha256) -> String {
    format!("sha256:{:x}", hash.finalize())
}
fn checksum(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}
fn valid_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    })
}
fn system_seconds(time: SystemTime) -> Result<u64, AssociationSessionError> {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| AssociationSessionError::InvalidTime)
}
fn format_time(time: SystemTime) -> Result<String, AssociationSessionError> {
    Ok(format!("unix:{}", system_seconds(time)?))
}
fn parse_time(value: &str) -> Result<u64, AssociationSessionError> {
    value
        .strip_prefix("unix:")
        .and_then(|value| value.parse().ok())
        .ok_or(AssociationSessionError::InvalidTime)
}
fn valid_staging_name(name: &str) -> bool {
    name.strip_prefix('.')
        .and_then(|name| name.strip_suffix(".building"))
        .and_then(|uuid| Uuid::parse_str(uuid).ok())
        .is_some_and(|uuid| uuid.get_version_num() == 4)
}
fn check_cancel(
    cancellation: &dyn AssociationBuildCancellation,
) -> Result<(), AssociationSessionError> {
    if cancellation.is_cancelled() {
        Err(AssociationSessionError::Cancelled)
    } else {
        Ok(())
    }
}
fn private_io(_: std::io::Error) -> AssociationSessionError {
    AssociationSessionError::PrivateIo
}
fn private_db(_: rusqlite::Error) -> AssociationSessionError {
    AssociationSessionError::PrivateDatabase
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AssociationSessionError {
    CacheUnavailable,
    InvalidRequest,
    InvalidObservation,
    InvalidDataset,
    InvalidCompletion,
    InvalidTime,
    SourceChanged,
    CorruptRegistry,
    CorruptSession,
    UnsafeEntry,
    ClaimConsumed,
    Cancelled,
    PrivateIo,
    PrivateDatabase,
}
impl std::fmt::Display for AssociationSessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::CacheUnavailable => "association cache directory is unavailable",
            Self::InvalidRequest => "association session request is invalid",
            Self::InvalidObservation => "association source cannot be observed safely",
            Self::InvalidDataset => "association dataset identity is invalid",
            Self::InvalidCompletion => "association session completion is invalid",
            Self::InvalidTime => "association session time is invalid",
            Self::SourceChanged => "association source changed during the build",
            Self::CorruptRegistry => "association session registry is corrupt",
            Self::CorruptSession => "association session failed validation",
            Self::UnsafeEntry => "association cache contains an unsafe entry",
            Self::ClaimConsumed => "association build claim was already consumed",
            Self::Cancelled => "association session build was cancelled",
            Self::PrivateIo => "association private cache operation failed",
            Self::PrivateDatabase => "association private registry operation failed",
        };
        f.write_str(message)
    }
}
impl std::error::Error for AssociationSessionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use genohype_association::{
        AssociationBuildV1, AssociationPointV1, AssociationThresholdV1,
        AssociationVisualizationResourcesV1, Background, GenomeLayoutV1, NeverCancel, PlotKindV1,
        ResourceDescriptorV1, COORDINATE_CONVENTION_V1,
    };
    use tempfile::TempDir;

    fn request(layout: &GenomeLayoutV1) -> NormalizedSessionRequestV1 {
        NormalizedSessionRequestV1 {
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
        }
    }

    fn point() -> AssociationPointV1 {
        AssociationPointV1 {
            contig: "1".into(),
            position: 1,
            ref_allele: None,
            alt_allele: None,
            pvalue: 1e-9,
            neg_log10_p: 9.0,
            beta: None,
            standard_error: None,
            allele_frequency: None,
            allele_count: None,
            gene_symbol: None,
            consequence: None,
            series: None,
            source_ordinal: 0,
        }
    }

    fn prepare_claim(
        root: &Path,
    ) -> (
        AssociationSessionRegistry,
        SessionLookup,
        BuildClaim,
        AssociationVisualizationManifestV1,
    ) {
        let registry = AssociationSessionRegistry::open_for_test(root.to_path_buf()).unwrap();
        let source = root.join("source.tsv");
        fs::write(&source, b"CHR\tPOS\tP\n1\t1\t1e-9\n").unwrap();
        let layout = GenomeLayoutV1::new([("1", 100)], 64, 32, 0).unwrap();
        let lookup = SessionLookup::observe_local(&source, request(&layout)).unwrap();
        let claim = registry.claim(lookup.clone()).unwrap();
        let fingerprint = format!("sha256:{}", "a".repeat(64));
        let mut store = SqliteAssociationStore::create(claim.database_path()).unwrap();
        store
            .begin_build(&AssociationBuildV1 {
                dataset_id: claim.dataset_id().clone(),
                created_at: "unix:1".into(),
            })
            .unwrap();
        store.insert_points(claim.dataset_id(), &[point()]).unwrap();
        store
            .replace_top_hits(claim.dataset_id(), &[point()])
            .unwrap();
        store
            .complete_build(claim.dataset_id(), &fingerprint)
            .unwrap();
        drop(store);
        let mut raster =
            genohype_association::AssociationRasterizer::from_layout(&layout, Background::White)
                .unwrap();
        raster
            .render_association_point(&layout, &point(), 2.5, 178)
            .unwrap();
        let png = raster.png_bytes().unwrap();
        let manifest = AssociationVisualizationManifestV1 {
            schema_version: SCHEMA_VERSION_V1.into(),
            plot_id: claim.dataset_id().to_string(),
            kind: PlotKindV1::Manhattan,
            title: "Association dataset".into(),
            assembly: "GRCh38".into(),
            coordinate_convention: COORDINATE_CONVENTION_V1.into(),
            source_fingerprint: fingerprint,
            genohype_version: "test".into(),
            created_at: "unix:1".into(),
            layout: layout.clone(),
            layout_hash: layout.layout_hash("GRCh38"),
            thresholds: request(&layout).thresholds,
            row_count: 1,
            valid_row_count: 1,
            skipped_row_count: 0,
            rendered_point_count: 1,
            significant_hit_count: 1,
            top_hits: vec![point()],
            warnings: vec![],
            resources: AssociationVisualizationResourcesV1 {
                raster: ResourceDescriptorV1 {
                    url: RASTER_NAME.into(),
                    media_type: "image/png".into(),
                    byte_size: Some(png.len() as u64),
                    checksum: Some(checksum(&png)),
                    expires_at: None,
                },
                region_api: Some(ResourceDescriptorV1 {
                    url: format!("datasets/{}/region", claim.dataset_id()),
                    media_type: "application/json".into(),
                    byte_size: None,
                    checksum: None,
                    expires_at: None,
                }),
                table_api: Some(ResourceDescriptorV1 {
                    url: format!("datasets/{}/table", claim.dataset_id()),
                    media_type: "application/json".into(),
                    byte_size: None,
                    checksum: None,
                    expires_at: None,
                }),
            },
        };
        claim.write_raster(&png, &NeverCancel).unwrap();
        claim.write_manifest(&manifest, &NeverCancel).unwrap();
        (registry, lookup, claim, manifest)
    }

    fn prepare_rebuild(
        registry: &AssociationSessionRegistry,
        lookup: &SessionLookup,
        mut manifest: AssociationVisualizationManifestV1,
    ) -> (BuildClaim, AssociationVisualizationManifestV1) {
        let claim = registry.claim(lookup.clone()).unwrap();
        let fingerprint = manifest.source_fingerprint.clone();
        let mut store = SqliteAssociationStore::create(claim.database_path()).unwrap();
        store
            .begin_build(&AssociationBuildV1 {
                dataset_id: claim.dataset_id().clone(),
                created_at: manifest.created_at.clone(),
            })
            .unwrap();
        store.insert_points(claim.dataset_id(), &[point()]).unwrap();
        store
            .replace_top_hits(claim.dataset_id(), &[point()])
            .unwrap();
        store
            .complete_build(claim.dataset_id(), &fingerprint)
            .unwrap();
        drop(store);

        let mut raster = genohype_association::AssociationRasterizer::from_layout(
            &manifest.layout,
            Background::White,
        )
        .unwrap();
        raster
            .render_association_point(&manifest.layout, &point(), 2.5, 178)
            .unwrap();
        let png = raster.png_bytes().unwrap();
        manifest.plot_id = claim.dataset_id().to_string();
        manifest.resources.raster.byte_size = Some(png.len() as u64);
        manifest.resources.raster.checksum = Some(checksum(&png));
        manifest.resources.table_api.as_mut().unwrap().url =
            format!("datasets/{}/table", claim.dataset_id());
        manifest.resources.region_api.as_mut().unwrap().url =
            format!("datasets/{}/region", claim.dataset_id());
        claim.write_raster(&png, &NeverCancel).unwrap();
        claim.write_manifest(&manifest, &NeverCancel).unwrap();
        (claim, manifest)
    }

    #[test]
    fn identities_are_distinct_and_warm_lookup_does_not_touch_source_contents() {
        let temp = TempDir::new().unwrap();
        let (registry, lookup, claim, manifest) = prepare_claim(temp.path());
        assert_ne!(lookup.request_key(), claim.dataset_id().as_str());
        let session = claim
            .publish(
                SessionCompletion {
                    source_fingerprint: manifest.source_fingerprint.clone(),
                    created_at: UNIX_EPOCH + Duration::from_secs(1),
                    expires_at: Some(SystemTime::now() + Duration::from_secs(60)),
                    ephemeral: false,
                },
                &NeverCancel,
            )
            .unwrap();
        let first = session.dataset_id().clone();
        drop(session);
        let warm = registry
            .lookup_valid(&lookup, SystemTime::now())
            .unwrap()
            .unwrap();
        assert_eq!(warm.dataset_id(), &first);
        assert_ne!(
            warm.completion().session_key,
            warm.completion().source_fingerprint
        );
        let private_registry = fs::read(temp.path().join("registry.sqlite3")).unwrap();
        let raw_path = temp
            .path()
            .join("source.tsv")
            .to_string_lossy()
            .into_owned();
        assert!(!private_registry
            .windows(raw_path.len())
            .any(|window| window == raw_path.as_bytes()));
    }

    #[test]
    fn concurrent_same_request_rechecks_the_published_winner() {
        use std::sync::mpsc;
        let temp = TempDir::new().unwrap();
        let (registry, lookup, claim, manifest) = prepare_claim(temp.path());
        let first_id = claim.dataset_id().clone();
        let competing_registry = registry.clone();
        let competing_lookup = lookup.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let competing = competing_registry.claim(competing_lookup).unwrap();
            let competing_id = competing.dataset_id().clone();
            let winner = competing
                .recheck_valid(SystemTime::now())
                .unwrap()
                .unwrap()
                .dataset_id()
                .clone();
            competing.abandon().unwrap();
            done_tx.send((competing_id, winner)).unwrap();
        });
        started_rx.recv().unwrap();
        assert!(done_rx.recv_timeout(Duration::from_millis(50)).is_err());
        let session = claim
            .publish(
                SessionCompletion {
                    source_fingerprint: manifest.source_fingerprint,
                    created_at: UNIX_EPOCH + Duration::from_secs(1),
                    expires_at: Some(SystemTime::now() + Duration::from_secs(60)),
                    ephemeral: false,
                },
                &NeverCancel,
            )
            .unwrap();
        let (competing_id, winner) = done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        worker.join().unwrap();
        assert_eq!(winner, first_id);
        assert_ne!(competing_id, first_id);
        assert_eq!(session.dataset_id(), &first_id);
    }

    #[test]
    fn failed_rebuild_preserves_old_generation_and_corruption_is_rejected() {
        let temp = TempDir::new().unwrap();
        let (registry, lookup, claim, manifest) = prepare_claim(temp.path());
        let old = claim
            .publish(
                SessionCompletion {
                    source_fingerprint: manifest.source_fingerprint,
                    created_at: UNIX_EPOCH + Duration::from_secs(1),
                    expires_at: Some(SystemTime::now() + Duration::from_secs(60)),
                    ephemeral: false,
                },
                &NeverCancel,
            )
            .unwrap();
        let old_id = old.dataset_id().clone();
        drop(old);

        let failed = registry.claim(lookup.clone()).unwrap();
        fs::write(failed.database_path(), b"corrupt").unwrap();
        assert!(failed
            .publish(
                SessionCompletion {
                    source_fingerprint: format!("sha256:{}", "b".repeat(64)),
                    created_at: SystemTime::now(),
                    expires_at: None,
                    ephemeral: false,
                },
                &NeverCancel,
            )
            .is_err());
        assert_eq!(
            registry
                .lookup_valid(&lookup, SystemTime::now())
                .unwrap()
                .unwrap()
                .dataset_id(),
            &old_id
        );
        let raster = temp
            .path()
            .join("sessions")
            .join(old_id.as_str())
            .join(RASTER_NAME);
        fs::write(raster, b"tampered").unwrap();
        assert!(registry
            .lookup_valid(&lookup, SystemTime::now())
            .unwrap()
            .is_none());
    }

    #[test]
    fn post_rename_failure_and_cancellation_preserve_old_generation() {
        let temp = TempDir::new().unwrap();
        let (registry, lookup, claim, manifest) = prepare_claim(temp.path());
        let old = claim
            .publish(
                SessionCompletion {
                    source_fingerprint: manifest.source_fingerprint.clone(),
                    created_at: UNIX_EPOCH + Duration::from_secs(1),
                    expires_at: Some(SystemTime::now() + Duration::from_secs(60)),
                    ephemeral: false,
                },
                &NeverCancel,
            )
            .unwrap();
        let old_id = old.dataset_id().clone();
        drop(old);

        let (claim, rebuilt_manifest) = prepare_rebuild(&registry, &lookup, manifest.clone());
        let new_id = claim.dataset_id().clone();
        let corrupt_path = temp
            .path()
            .join("sessions")
            .join(new_id.as_str())
            .join(MANIFEST_NAME);
        registry.set_after_rename_hook(move || fs::remove_file(corrupt_path).unwrap());
        assert!(claim
            .publish(
                SessionCompletion {
                    source_fingerprint: rebuilt_manifest.source_fingerprint,
                    created_at: UNIX_EPOCH + Duration::from_secs(1),
                    expires_at: None,
                    ephemeral: false,
                },
                &NeverCancel,
            )
            .is_err());
        assert!(!temp.path().join("sessions").join(new_id.as_str()).exists());
        assert_eq!(
            registry
                .lookup_valid(&lookup, SystemTime::now())
                .unwrap()
                .unwrap()
                .dataset_id(),
            &old_id
        );

        let (claim, rebuilt_manifest) = prepare_rebuild(&registry, &lookup, manifest);
        let cancelled_id = claim.dataset_id().clone();
        let cancellation = genohype_association::BuildCancellation::default();
        let trigger = cancellation.clone();
        registry.set_after_rename_hook(move || trigger.cancel());
        assert!(matches!(
            claim.publish(
                SessionCompletion {
                    source_fingerprint: rebuilt_manifest.source_fingerprint,
                    created_at: UNIX_EPOCH + Duration::from_secs(1),
                    expires_at: None,
                    ephemeral: false,
                },
                &cancellation,
            ),
            Err(AssociationSessionError::Cancelled)
        ));
        assert!(!temp
            .path()
            .join("sessions")
            .join(cancelled_id.as_str())
            .exists());
        assert_eq!(
            registry
                .lookup_valid(&lookup, SystemTime::now())
                .unwrap()
                .unwrap()
                .dataset_id(),
            &old_id
        );
    }

    #[test]
    fn deep_validation_rejects_dimension_and_store_state_corruption() {
        let temp = TempDir::new().unwrap();
        let (registry, _lookup, claim, manifest) = prepare_claim(temp.path());
        let session = claim
            .publish(
                SessionCompletion {
                    source_fingerprint: manifest.source_fingerprint,
                    created_at: UNIX_EPOCH + Duration::from_secs(1),
                    expires_at: None,
                    ephemeral: false,
                },
                &NeverCancel,
            )
            .unwrap();
        let id = session.dataset_id().clone();
        drop(session);
        let directory = temp.path().join("sessions").join(id.as_str());
        let marker_path = directory.join(COMPLETE_NAME);
        let mut marker: AssociationSessionCompletionV1 =
            serde_json::from_slice(&fs::read(&marker_path).unwrap()).unwrap();
        marker.raster_width += 1;
        fs::write(&marker_path, serde_json::to_vec(&marker).unwrap()).unwrap();
        assert!(registry
            .validate_generation(id.as_str(), None, None, None, false, None)
            .is_err());

        let temp = TempDir::new().unwrap();
        let (registry, _lookup, claim, manifest) = prepare_claim(temp.path());
        let session = claim
            .publish(
                SessionCompletion {
                    source_fingerprint: manifest.source_fingerprint,
                    created_at: UNIX_EPOCH + Duration::from_secs(1),
                    expires_at: None,
                    ephemeral: false,
                },
                &NeverCancel,
            )
            .unwrap();
        let id = session.dataset_id().clone();
        drop(session);
        let directory = temp.path().join("sessions").join(id.as_str());
        let database_path = directory.join(DATABASE_NAME);
        Connection::open(&database_path)
            .unwrap()
            .execute_batch("PRAGMA user_version=2")
            .unwrap();
        let marker_path = directory.join(COMPLETE_NAME);
        let mut marker: AssociationSessionCompletionV1 =
            serde_json::from_slice(&fs::read(&marker_path).unwrap()).unwrap();
        marker.database = artifact(&database_path, &NeverCancel).unwrap();
        fs::write(&marker_path, serde_json::to_vec(&marker).unwrap()).unwrap();
        assert!(registry
            .validate_generation(id.as_str(), None, None, None, false, None)
            .is_err());
    }

    #[test]
    fn significance_threshold_is_keyed_and_must_be_advertised() {
        let layout = GenomeLayoutV1::new([("1", 100)], 64, 32, 0).unwrap();
        let baseline = request(&layout);
        let observation = format!("sha256:{}", "a".repeat(64));
        let baseline_key = baseline
            .with_source(observation.clone())
            .session_key()
            .unwrap();

        let mut changed = baseline.clone();
        changed.significance_threshold = 1e-6;
        assert!(changed.validate().is_err());
        changed.thresholds[0].pvalue = 1e-6;
        assert_ne!(
            baseline_key,
            changed.with_source(observation).session_key().unwrap()
        );

        let temp = TempDir::new().unwrap();
        let (_registry, lookup, claim, mut manifest) = prepare_claim(temp.path());
        let mut request = lookup.request.clone();
        let suggestive = AssociationThresholdV1 {
            name: "suggestive".into(),
            pvalue: 1e-5,
        };
        request.thresholds.push(suggestive.clone());
        manifest.thresholds.push(suggestive);
        assert!(manifest_matches_request(&manifest, &request));
        manifest.thresholds.swap(0, 1);
        assert!(!manifest_matches_request(&manifest, &request));
        claim.abandon().unwrap();
    }

    #[test]
    fn publication_and_warm_reuse_bind_request_to_manifest_thresholds() {
        let temp = TempDir::new().unwrap();
        let (registry, lookup, claim, mut manifest) = prepare_claim(temp.path());
        manifest.thresholds[0].pvalue = 1e-6;
        fs::write(
            claim.staging_path.as_ref().unwrap().join(MANIFEST_NAME),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            claim.publish(
                SessionCompletion {
                    source_fingerprint: manifest.source_fingerprint,
                    created_at: UNIX_EPOCH + Duration::from_secs(1),
                    expires_at: None,
                    ephemeral: false,
                },
                &NeverCancel,
            ),
            Err(AssociationSessionError::InvalidCompletion)
        ));
        assert!(registry
            .registry_row(lookup.request_key())
            .unwrap()
            .is_none());
        assert_eq!(
            fs::read_dir(temp.path().join("sessions")).unwrap().count(),
            0
        );

        let temp = TempDir::new().unwrap();
        let (registry, lookup, claim, manifest) = prepare_claim(temp.path());
        let session = claim
            .publish(
                SessionCompletion {
                    source_fingerprint: manifest.source_fingerprint,
                    created_at: UNIX_EPOCH + Duration::from_secs(1),
                    expires_at: None,
                    ephemeral: false,
                },
                &NeverCancel,
            )
            .unwrap();
        let directory = session.inner.directory.clone();
        drop(session);
        let manifest_path = directory.join(MANIFEST_NAME);
        let mut mismatched: AssociationVisualizationManifestV1 =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        mismatched.thresholds[0].pvalue = 1e-6;
        let manifest_bytes = serde_json::to_vec_pretty(&mismatched).unwrap();
        fs::write(&manifest_path, &manifest_bytes).unwrap();
        let marker_path = directory.join(COMPLETE_NAME);
        let mut marker: AssociationSessionCompletionV1 =
            serde_json::from_slice(&fs::read(&marker_path).unwrap()).unwrap();
        marker.manifest = CompletedArtifactV1 {
            byte_size: manifest_bytes.len() as u64,
            checksum: checksum(&manifest_bytes),
        };
        let marker_bytes = serde_json::to_vec_pretty(&marker).unwrap();
        fs::write(&marker_path, &marker_bytes).unwrap();
        registry
            .connection()
            .unwrap()
            .execute(
                "UPDATE association_sessions SET marker_checksum=?1 WHERE request_key=?2",
                params![checksum(&marker_bytes), lookup.request_key()],
            )
            .unwrap();
        assert!(registry
            .lookup_valid(&lookup, SystemTime::now())
            .unwrap()
            .is_none());
    }

    #[test]
    fn strict_significance_count_is_reconciled_from_the_database() {
        let temp = TempDir::new().unwrap();
        let (_registry, _lookup, claim, mut manifest) = prepare_claim(temp.path());
        let threshold = 5e-8_f64;
        let neg_log = -threshold.log10();
        Connection::open(claim.database_path())
            .unwrap()
            .execute(
                "UPDATE association_points SET pvalue=?1,neg_log10_p=?2",
                params![threshold, neg_log],
            )
            .unwrap();
        manifest.top_hits[0].pvalue = threshold;
        manifest.top_hits[0].neg_log10_p = neg_log;
        // Equality is not significant: the contract is strictly pvalue < threshold.
        manifest.significant_hit_count = 1;
        fs::write(
            claim.staging_path.as_ref().unwrap().join(MANIFEST_NAME),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        assert!(claim
            .publish(
                SessionCompletion {
                    source_fingerprint: manifest.source_fingerprint,
                    created_at: UNIX_EPOCH + Duration::from_secs(1),
                    expires_at: None,
                    ephemeral: false,
                },
                &NeverCancel,
            )
            .is_err());
        assert_eq!(
            fs::read_dir(temp.path().join("sessions")).unwrap().count(),
            0
        );
    }

    #[test]
    fn png_validation_bounds_encoded_ihdr_before_full_decode() {
        let pixmap = tiny_skia::Pixmap::new(8, 8).unwrap();
        let valid = pixmap.encode_png().unwrap();
        assert!(validate_png_dimensions(&valid, 8, 8).is_ok());

        let mut oversized_ihdr = valid.clone();
        oversized_ihdr[16..20]
            .copy_from_slice(&(genohype_association::MAX_RASTER_DIMENSION + 1).to_be_bytes());
        let ihdr_crc = png_crc32(&oversized_ihdr[12..29]);
        oversized_ihdr[29..33].copy_from_slice(&ihdr_crc.to_be_bytes());
        assert!(oversized_ihdr.len() < 1_024);
        assert_eq!(
            validate_png_dimensions(&oversized_ihdr, 8, 8),
            Err(AssociationSessionError::CorruptSession)
        );

        let mut bad_crc = valid;
        let last = bad_crc.len() - 1;
        bad_crc[last] ^= 0xff;
        assert_eq!(
            validate_png_dimensions(&bad_crc, 8, 8),
            Err(AssociationSessionError::CorruptSession)
        );
        assert_eq!(
            validate_png_dimensions(&[], 4097, 4096),
            Err(AssociationSessionError::CorruptSession)
        );
    }

    #[test]
    fn sparse_artifacts_are_streamed_through_the_fixed_buffer() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("large-sparse.sqlite3");
        let file = File::create(&path).unwrap();
        let size = 32 * 1024 * 1024 + 17;
        file.set_len(size).unwrap();
        drop(file);
        let (observed, digest) = stream_artifact(&path, None).unwrap();
        assert_eq!(observed, size);
        assert_eq!(STREAM_BUFFER_BYTES, 64 * 1024);
        assert!(valid_sha256(&digest));
    }

    #[test]
    fn missing_active_lock_crash_staging_is_safely_scavenged() {
        let temp = TempDir::new().unwrap();
        let registry =
            AssociationSessionRegistry::open_for_test(temp.path().to_path_buf()).unwrap();
        let crashed = temp
            .path()
            .join("staging")
            .join(format!(".{}.building", Uuid::new_v4()));
        create_private_directory(&crashed).unwrap();
        fs::write(crashed.join(DATABASE_NAME), b"partial").unwrap();
        assert!(!crashed.join(ACTIVE_LOCK_NAME).exists());
        let summary = registry.cleanup(CleanupPolicy::default()).unwrap();
        assert_eq!(summary.staging_removed, 1);
        assert!(!crashed.exists());
    }

    #[test]
    fn cleanup_cannot_take_renamed_generation_during_publication_transition() {
        use std::sync::mpsc;

        let temp = TempDir::new().unwrap();
        let (registry, _lookup, claim, manifest) = prepare_claim(temp.path());
        let (cleanup_thread_tx, cleanup_thread_rx) = mpsc::channel();
        let cleanup_registry = registry.clone();
        registry.set_before_active_transition_hook(move || {
            let thread = std::thread::spawn(move || {
                cleanup_registry
                    .cleanup(CleanupPolicy {
                        now: UNIX_EPOCH + Duration::from_secs(3),
                        remove_expired: true,
                        remove_orphans: true,
                        remove_staging: false,
                    })
                    .unwrap()
            });
            cleanup_thread_tx.send(thread).unwrap();
            std::thread::yield_now();
        });
        let session = claim
            .publish(
                SessionCompletion {
                    source_fingerprint: manifest.source_fingerprint,
                    created_at: UNIX_EPOCH + Duration::from_secs(1),
                    expires_at: Some(UNIX_EPOCH + Duration::from_secs(2)),
                    ephemeral: false,
                },
                &NeverCancel,
            )
            .unwrap();
        let concurrent_cleanup = cleanup_thread_rx.recv().unwrap().join().unwrap();
        assert_eq!(concurrent_cleanup.retained_locked, 1);
        assert_eq!(
            registry
                .cleanup(CleanupPolicy {
                    now: UNIX_EPOCH + Duration::from_secs(3),
                    remove_expired: true,
                    remove_orphans: true,
                    remove_staging: false,
                })
                .unwrap()
                .retained_locked,
            1
        );
        assert!(session.database_path().exists());

        let temp = TempDir::new().unwrap();
        let (registry, _lookup, claim, manifest) = prepare_claim(temp.path());
        let (cleanup_tx, cleanup_rx) = mpsc::channel();
        let cleanup_registry = registry.clone();
        registry.set_before_active_transition_hook(move || {
            cleanup_tx
                .send(cleanup_registry.cleanup(CleanupPolicy::default()).unwrap())
                .unwrap();
        });
        let session = claim
            .publish(
                SessionCompletion {
                    source_fingerprint: manifest.source_fingerprint,
                    created_at: UNIX_EPOCH + Duration::from_secs(1),
                    expires_at: None,
                    ephemeral: true,
                },
                &NeverCancel,
            )
            .unwrap();
        assert_eq!(cleanup_rx.recv().unwrap().retained_locked, 1);
        assert_eq!(
            registry
                .cleanup(CleanupPolicy::default())
                .unwrap()
                .retained_locked,
            1
        );
        let directory = session.inner.directory.clone();
        drop(session);
        assert!(!directory.exists());
    }

    #[test]
    fn policy_retention_is_bounded_from_creation_time() {
        let policy = SessionPolicy::default();
        assert!(!policy.rebuild && !policy.ephemeral);
        assert_eq!(
            policy.expires_at(UNIX_EPOCH).unwrap(),
            Some(UNIX_EPOCH + Duration::from_secs(7 * 24 * 60 * 60))
        );
        let ephemeral = SessionPolicy {
            ephemeral: true,
            ..policy
        };
        assert_eq!(ephemeral.expires_at(UNIX_EPOCH).unwrap(), None);
    }

    #[test]
    fn cancellation_abandons_staging_and_ephemeral_drops_generation() {
        let temp = TempDir::new().unwrap();
        let (_registry, _lookup, claim, manifest) = prepare_claim(temp.path());
        let cancellation = genohype_association::BuildCancellation::default();
        cancellation.cancel();
        assert!(matches!(
            claim.publish(
                SessionCompletion {
                    source_fingerprint: manifest.source_fingerprint,
                    created_at: SystemTime::now(),
                    expires_at: None,
                    ephemeral: true,
                },
                &cancellation,
            ),
            Err(AssociationSessionError::Cancelled)
        ));
        assert_eq!(
            fs::read_dir(temp.path().join("staging")).unwrap().count(),
            0
        );

        let (_registry, _lookup, claim, manifest) = prepare_claim(temp.path());
        let id = claim.dataset_id().clone();
        let session = claim
            .publish(
                SessionCompletion {
                    source_fingerprint: manifest.source_fingerprint,
                    created_at: UNIX_EPOCH + Duration::from_secs(1),
                    expires_at: None,
                    ephemeral: true,
                },
                &NeverCancel,
            )
            .unwrap();
        assert!(session.database_path().exists());
        drop(session);
        assert!(!temp.path().join("sessions").join(id.as_str()).exists());
    }

    #[cfg(unix)]
    #[test]
    fn permissions_active_locks_cleanup_and_symlinks_fail_closed() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let temp = TempDir::new().unwrap();
        let (registry, _lookup, claim, manifest) = prepare_claim(temp.path());
        assert_eq!(
            fs::metadata(temp.path()).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(claim.database_path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let id = claim.dataset_id().clone();
        let session = claim
            .publish(
                SessionCompletion {
                    source_fingerprint: manifest.source_fingerprint,
                    created_at: UNIX_EPOCH + Duration::from_secs(1),
                    expires_at: Some(UNIX_EPOCH + Duration::from_secs(2)),
                    ephemeral: false,
                },
                &NeverCancel,
            )
            .unwrap();
        let cleanup = registry.cleanup(CleanupPolicy::default()).unwrap();
        assert_eq!(cleanup.retained_locked, 1);
        assert!(session.database_path().exists());
        drop(session);
        let cleanup = registry.cleanup(CleanupPolicy::default()).unwrap();
        assert_eq!(cleanup.sessions_removed, 1);
        assert!(!temp.path().join("sessions").join(id.as_str()).exists());

        let outside = temp.path().join("outside");
        fs::write(&outside, b"safe").unwrap();
        let unsafe_dir = temp
            .path()
            .join("staging")
            .join(format!(".{}.building", Uuid::new_v4()));
        fs::create_dir(&unsafe_dir).unwrap();
        symlink(&outside, unsafe_dir.join(ACTIVE_LOCK_NAME)).unwrap();
        assert!(registry.cleanup(CleanupPolicy::default()).is_err());
        assert_eq!(fs::read(&outside).unwrap(), b"safe");
    }

    #[test]
    fn source_mutation_and_global_lock_prevent_publication_or_broad_clear() {
        let temp = TempDir::new().unwrap();
        let (registry, lookup, claim, manifest) = prepare_claim(temp.path());
        assert!(registry.try_claim_broad_cache_clear().unwrap().is_none());
        fs::write(lookup.local_path.as_ref().unwrap(), b"changed").unwrap();
        assert!(matches!(
            claim.publish(
                SessionCompletion {
                    source_fingerprint: manifest.source_fingerprint,
                    created_at: SystemTime::now(),
                    expires_at: None,
                    ephemeral: false,
                },
                &NeverCancel,
            ),
            Err(AssociationSessionError::SourceChanged)
        ));
        assert!(registry.try_claim_broad_cache_clear().unwrap().is_some());
    }
}
