//! Bounded, read-only inventory of the exact objects needed to read a Hail table.
//!
//! The inventory engine is provider-neutral. This first implementation includes a
//! content-addressed local backend; cloud roots fail explicitly rather than using
//! an incomplete provider identity.

use crate::index::IndexMetadata;
use crate::metadata::{parse_metadata_at, RVDComponentSpec, TableMetadata};
use crate::{HailError, Result};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Component, Path, PathBuf};

use super::{SourceManifest, SourceObject};

const TOP_METADATA: &str = "metadata.json.gz";
const SUCCESS: &str = "_SUCCESS";
pub const DEFAULT_MAX_OBJECTS: usize = 1_000_000;
pub const MAX_METADATA_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InventoryAccess {
    Sequential,
    Indexed,
}

impl InventoryAccess {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sequential => "sequential",
            Self::Indexed => "indexed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct InventoryOptions {
    pub access: InventoryAccess,
    pub max_objects: usize,
    pub max_metadata_bytes: u64,
}

impl Default for InventoryOptions {
    fn default() -> Self {
        Self {
            access: InventoryAccess::Sequential,
            max_objects: DEFAULT_MAX_OBJECTS,
            max_metadata_bytes: MAX_METADATA_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelativeObjectPath(String);

impl RelativeObjectPath {
    pub fn new(value: &str) -> Result<Self> {
        normalize_relative("", value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedObject {
    pub size: u64,
    /// Backend listing identity. For local files this includes size and mtime;
    /// cloud implementations must include generation, size, CRC32C, and listing ETag.
    pub listing_identity: String,
    pub directory_marker: bool,
    /// Local backends expose links without following them so unrelated links can
    /// remain unrelated while any selected/relevant link fails closed.
    pub symlink: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObservedListing {
    pub objects: BTreeMap<RelativeObjectPath, ObservedObject>,
}

impl ObservedListing {
    pub fn insert(&mut self, path: RelativeObjectPath, object: ObservedObject) -> Result<()> {
        if self.objects.insert(path.clone(), object).is_some() {
            return invalid(format!(
                "duplicate object in source listing: {}",
                path.as_str()
            ));
        }
        Ok(())
    }
}

pub trait SourceInventoryBackend {
    fn canonical_root(&self) -> &str;
    fn list(&mut self, max_objects: usize) -> Result<ObservedListing>;
    fn stat(&mut self, relative: &RelativeObjectPath) -> Result<SourceObject>;
    fn read_pinned(
        &mut self,
        relative: &RelativeObjectPath,
        identity: &SourceObject,
        max_bytes: u64,
    ) -> Result<Vec<u8>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryRole {
    pub path: RelativeObjectPath,
    pub role: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IgnoredCounts {
    pub directory_markers: usize,
    pub hadoop_checksums: usize,
    pub unrelated: usize,
    pub sequential_index_objects: usize,
}

#[derive(Debug, Clone)]
pub struct InventoryResult {
    pub root: String,
    pub access: InventoryAccess,
    pub manifest: SourceManifest,
    pub digest_sha256: String,
    pub roles: Vec<InventoryRole>,
    pub ignored: IgnoredCounts,
}

#[derive(Debug, Clone)]
struct ComponentPlan {
    label: &'static str,
    root: RelativeObjectPath,
    metadata: RelativeObjectPath,
    spec: RVDComponentSpec,
    index_root: Option<RelativeObjectPath>,
}

#[derive(Debug, Clone, Default)]
struct Closure {
    required: BTreeMap<RelativeObjectPath, String>,
    component_roots: Vec<RelativeObjectPath>,
    reference_root: Option<RelativeObjectPath>,
    index_roots: Vec<RelativeObjectPath>,
    expected_index_dirs: Vec<RelativeObjectPath>,
}

impl Closure {
    fn require(&mut self, path: RelativeObjectPath, role: impl Into<String>) -> Result<()> {
        let role = role.into();
        if let Some(existing) = self.required.insert(path.clone(), role.clone()) {
            return invalid(format!(
                "semantic object collision at {}: {existing} and {role}",
                path.as_str()
            ));
        }
        Ok(())
    }
}

/// Inventory a table through a mockable provider backend.
pub fn inventory_hail_table<B: SourceInventoryBackend>(
    backend: &mut B,
    options: &InventoryOptions,
) -> Result<InventoryResult> {
    if options.max_objects == 0 {
        return invalid("max_objects must be at least 1");
    }
    if options.max_metadata_bytes == 0 {
        return invalid("max_metadata_bytes must be at least 1");
    }

    let listing_a = backend.list(options.max_objects)?;
    let mut parsed_identities = BTreeMap::new();

    let top_path = RelativeObjectPath::new(TOP_METADATA)?;
    let top_identity = metadata_identity(backend, &listing_a, &top_path)?;
    let top_bytes = backend.read_pinned(&top_path, &top_identity, options.max_metadata_bytes)?;
    let top: TableMetadata = parse_metadata_at(&top_bytes, top_path.as_str())?;
    parsed_identities.insert(top_path.clone(), top_identity);

    let rows_root = component_root(&top, "rows")?;
    let globals_root = component_root(&top, "globals")?;
    let references_root = RelativeObjectPath::new(&top.references_rel_path)?;
    validate_disjoint_roots(&[&rows_root, &globals_root, &references_root])?;

    let rows = read_component(
        backend,
        &listing_a,
        "rows",
        rows_root,
        options,
        &mut parsed_identities,
    )?;
    let globals = read_component(
        backend,
        &listing_a,
        "globals",
        globals_root,
        options,
        &mut parsed_identities,
    )?;
    validate_index_roots(&rows, &globals, &references_root)?;
    if let Some(counts) = top.partition_counts() {
        if counts.len() != rows.spec.part_files.len() {
            return invalid(format!(
                "top partition count length {} does not match row part count {}",
                counts.len(),
                rows.spec.part_files.len()
            ));
        }
    }

    let mut closure = Closure {
        component_roots: vec![rows.root.clone(), globals.root.clone()],
        reference_root: Some(references_root.clone()),
        ..Closure::default()
    };
    closure.require(top_path, "top metadata")?;
    closure.require(RelativeObjectPath::new(SUCCESS)?, "success marker")?;
    add_component(&mut closure, &rows)?;
    add_component(&mut closure, &globals)?;

    for (path, observed) in &listing_a.objects {
        if is_descendant_or_equal(path, &references_root) && path != &references_root {
            if observed.directory_marker || is_hadoop_checksum(path) {
                continue;
            }
            closure.require(path.clone(), "reference object")?;
        }
    }

    if options.access == InventoryAccess::Indexed {
        for component in [&rows, &globals] {
            add_indexes(
                backend,
                &listing_a,
                component,
                options,
                &mut closure,
                &mut parsed_identities,
            )?;
        }
    } else {
        for component in [&rows, &globals] {
            if let Some(root) = &component.index_root {
                closure.index_roots.push(root.clone());
            }
        }
    }

    let ignored_a = reconcile(&listing_a, &closure, options.access)?;
    let listing_b = backend.list(options.max_objects)?;
    let ignored_b = reconcile(&listing_b, &closure, options.access)?;
    require_stable_observations(&listing_a, &listing_b, &closure)?;
    if ignored_a != ignored_b {
        return invalid("ignored source objects changed between inventory observations");
    }

    let mut objects = Vec::with_capacity(closure.required.len());
    for path in closure.required.keys() {
        let identity = backend.stat(path)?;
        if let Some(parsed) = parsed_identities.get(path) {
            if &identity != parsed {
                return invalid(format!(
                    "metadata object changed while inventory was planned: {}",
                    path.as_str()
                ));
            }
        }
        objects.push(identity);
    }

    let manifest = SourceManifest::new(objects)?;
    let digest_sha256 = manifest.digest_sha256()?;
    let roles = closure
        .required
        .into_iter()
        .map(|(path, role)| InventoryRole { path, role })
        .collect();
    Ok(InventoryResult {
        root: backend.canonical_root().to_string(),
        access: options.access,
        manifest,
        digest_sha256,
        roles,
        ignored: ignored_a,
    })
}

/// Dispatch inventory by root scheme. Cloud inventory is deliberately unsupported
/// until its listing generation/CRC32C can be joined safely to generation-selected HEAD.
pub fn inventory_hail_table_path(
    root: &str,
    options: &InventoryOptions,
) -> Result<InventoryResult> {
    if root.starts_with("gs://") {
        #[cfg(feature = "gcp")]
        return invalid("GCS source-manifest inventory is not yet implemented safely");
        #[cfg(not(feature = "gcp"))]
        return invalid("GCS source-manifest inventory requires the 'gcp' feature");
    }
    if root.contains("://") {
        return invalid("unsupported by source-manifest inventory v1");
    }
    let mut backend = LocalInventoryBackend::new(root)?;
    inventory_hail_table(&mut backend, options)
}

fn component_root(top: &TableMetadata, name: &str) -> Result<RelativeObjectPath> {
    let value = top
        .extra
        .get("components")
        .and_then(|v| v.get(name))
        .and_then(|v| v.get("rel_path"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            HailError::InvalidFormat(format!(
                "top metadata must contain string components.{name}.rel_path"
            ))
        })?;
    RelativeObjectPath::new(value)
}

fn read_component<B: SourceInventoryBackend>(
    backend: &mut B,
    listing: &ObservedListing,
    label: &'static str,
    root: RelativeObjectPath,
    options: &InventoryOptions,
    parsed: &mut BTreeMap<RelativeObjectPath, SourceObject>,
) -> Result<ComponentPlan> {
    let metadata = normalize_relative(root.as_str(), "metadata.json.gz")?;
    let identity = metadata_identity(backend, listing, &metadata)?;
    let bytes = backend.read_pinned(&metadata, &identity, options.max_metadata_bytes)?;
    let spec: RVDComponentSpec = parse_metadata_at(&bytes, metadata.as_str())?;
    parsed.insert(metadata.clone(), identity);
    validate_component(label, &spec)?;
    let index_root = spec
        .index_spec
        .as_ref()
        .map(|index| normalize_relative(root.as_str(), &index.rel_path))
        .transpose()?;
    Ok(ComponentPlan {
        label,
        root,
        metadata,
        spec,
        index_root,
    })
}

fn validate_component(label: &str, spec: &RVDComponentSpec) -> Result<()> {
    if spec.part_files.is_empty() {
        return invalid(format!("{label} component declares no part files"));
    }
    let mut unique = BTreeSet::new();
    for part in &spec.part_files {
        validate_part_name(part)?;
        if !unique.insert(part) {
            return invalid(format!("{label} component declares duplicate part {part}"));
        }
    }
    if spec.range_bounds.len() != spec.part_files.len() {
        return invalid(format!(
            "{label} range-bound count {} does not match part count {}",
            spec.range_bounds.len(),
            spec.part_files.len()
        ));
    }
    Ok(())
}

fn add_component(closure: &mut Closure, component: &ComponentPlan) -> Result<()> {
    closure.require(
        component.metadata.clone(),
        format!("{} metadata", component.label),
    )?;
    for part in &component.spec.part_files {
        closure.require(
            normalize_relative(component.root.as_str(), &format!("parts/{part}"))?,
            format!("{} part {part}", component.label),
        )?;
    }
    Ok(())
}

fn add_indexes<B: SourceInventoryBackend>(
    backend: &mut B,
    listing: &ObservedListing,
    component: &ComponentPlan,
    options: &InventoryOptions,
    closure: &mut Closure,
    parsed: &mut BTreeMap<RelativeObjectPath, SourceObject>,
) -> Result<()> {
    let Some(index_root) = &component.index_root else {
        return Ok(());
    };
    if closure.index_roots.iter().any(|root| root == index_root) {
        return invalid(format!("duplicate index root: {}", index_root.as_str()));
    }
    closure.index_roots.push(index_root.clone());
    for part in &component.spec.part_files {
        let index_dir = normalize_relative(index_root.as_str(), &format!("{part}.idx"))?;
        if closure
            .expected_index_dirs
            .iter()
            .any(|dir| dir == &index_dir)
        {
            return invalid(format!("duplicate index directory: {}", index_dir.as_str()));
        }
        closure.expected_index_dirs.push(index_dir.clone());
        let metadata = normalize_relative(index_dir.as_str(), "metadata.json.gz")?;
        let identity = metadata_identity(backend, listing, &metadata)?;
        let bytes = backend.read_pinned(&metadata, &identity, options.max_metadata_bytes)?;
        let index: IndexMetadata = parse_metadata_at(&bytes, metadata.as_str())?;
        parsed.insert(metadata.clone(), identity);
        closure.require(
            metadata,
            format!("{} index metadata {part}", component.label),
        )?;
        let data = normalize_relative(index_dir.as_str(), &index.index_path)?;
        if data == index_dir || !is_descendant_or_equal(&data, &index_dir) {
            return invalid(format!(
                "indexPath escapes index directory for {part}: {}",
                index.index_path
            ));
        }
        closure.require(data, format!("{} index data {part}", component.label))?;
    }
    Ok(())
}

fn metadata_identity<B: SourceInventoryBackend>(
    backend: &mut B,
    listing: &ObservedListing,
    path: &RelativeObjectPath,
) -> Result<SourceObject> {
    if !listing.objects.contains_key(path) {
        return invalid(format!(
            "missing required metadata object: {}",
            path.as_str()
        ));
    }
    backend.stat(path)
}

fn reconcile(
    listing: &ObservedListing,
    closure: &Closure,
    access: InventoryAccess,
) -> Result<IgnoredCounts> {
    let missing: Vec<_> = closure
        .required
        .keys()
        .filter(|path| {
            listing
                .objects
                .get(*path)
                .is_none_or(|object| object.directory_marker)
        })
        .map(|path| path.as_str().to_string())
        .collect();
    let mut unexpected = Vec::new();
    let mut ignored = IgnoredCounts::default();

    for (path, observed) in &listing.objects {
        if observed.directory_marker {
            ignored.directory_markers += 1;
            continue;
        }
        if closure.required.contains_key(path) {
            if observed.symlink {
                return invalid(format!(
                    "symlink is forbidden for required source object: {}",
                    path.as_str()
                ));
            }
            continue;
        }
        if is_hadoop_checksum(path) {
            ignored.hadoop_checksums += 1;
            continue;
        }
        if access == InventoryAccess::Sequential
            && closure
                .index_roots
                .iter()
                .any(|root| is_descendant_or_equal(path, root))
        {
            ignored.sequential_index_objects += 1;
            continue;
        }
        let in_component = closure
            .component_roots
            .iter()
            .any(|root| is_descendant_or_equal(path, root));
        let in_reference = closure
            .reference_root
            .as_ref()
            .is_some_and(|root| is_descendant_or_equal(path, root));
        let in_index = access == InventoryAccess::Indexed
            && closure
                .index_roots
                .iter()
                .any(|root| is_descendant_or_equal(path, root));
        if in_component || in_reference || in_index {
            if observed.symlink {
                unexpected.push(format!("{} (symlink)", path.as_str()));
            } else {
                unexpected.push(path.as_str().to_string());
            }
        } else {
            ignored.unrelated += 1;
        }
    }

    if !missing.is_empty() || !unexpected.is_empty() {
        return invalid(format_path_mismatch(&missing, &unexpected));
    }
    Ok(ignored)
}

fn require_stable_observations(
    a: &ObservedListing,
    b: &ObservedListing,
    closure: &Closure,
) -> Result<()> {
    for path in closure.required.keys() {
        let left = a.objects.get(path);
        let right = b.objects.get(path);
        if left != right {
            return invalid(format!(
                "required object changed between inventory observations: {}",
                path.as_str()
            ));
        }
    }
    Ok(())
}

fn format_path_mismatch(missing: &[String], unexpected: &[String]) -> String {
    fn sample(paths: &[String]) -> String {
        paths
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ")
    }
    let mut parts = Vec::new();
    if !missing.is_empty() {
        parts.push(format!("missing {} [{}]", missing.len(), sample(missing)));
    }
    if !unexpected.is_empty() {
        parts.push(format!(
            "unexpected relevant {} [{}]",
            unexpected.len(),
            sample(unexpected)
        ));
    }
    format!(
        "source inventory reconciliation failed: {}",
        parts.join("; ")
    )
}

fn validate_index_roots(
    rows: &ComponentPlan,
    globals: &ComponentPlan,
    references: &RelativeObjectPath,
) -> Result<()> {
    let data_roots = [&rows.root, &globals.root, references];
    let index_roots: Vec<_> = [rows.index_root.as_ref(), globals.index_root.as_ref()]
        .into_iter()
        .flatten()
        .collect();
    for (position, index_root) in index_roots.iter().enumerate() {
        for data_root in data_roots {
            if is_descendant_or_equal(index_root, data_root)
                || is_descendant_or_equal(data_root, index_root)
            {
                return invalid(format!(
                    "index root overlaps table data root: {} and {}",
                    index_root.as_str(),
                    data_root.as_str()
                ));
            }
        }
        for other in &index_roots[position + 1..] {
            if is_descendant_or_equal(index_root, other)
                || is_descendant_or_equal(other, index_root)
            {
                return invalid(format!(
                    "index roots overlap: {} and {}",
                    index_root.as_str(),
                    other.as_str()
                ));
            }
        }
    }
    Ok(())
}

fn validate_disjoint_roots(roots: &[&RelativeObjectPath]) -> Result<()> {
    for (index, left) in roots.iter().enumerate() {
        for right in &roots[index + 1..] {
            if is_descendant_or_equal(left, right) || is_descendant_or_equal(right, left) {
                return invalid(format!(
                    "rows, globals, and references roots must be distinct and nonoverlapping: {} and {}",
                    left.as_str(),
                    right.as_str()
                ));
            }
        }
    }
    Ok(())
}

fn validate_part_name(part: &str) -> Result<()> {
    if part.is_empty()
        || part == "."
        || part == ".."
        || part.contains('/')
        || part.contains('\\')
        || part.contains('?')
        || part.contains('#')
    {
        return invalid(format!("unsafe part name: {part:?}"));
    }
    Ok(())
}

fn normalize_relative(base: &str, value: &str) -> Result<RelativeObjectPath> {
    if value.is_empty()
        || value.contains('\\')
        || value.contains('?')
        || value.contains('#')
        || value.contains("://")
        || Path::new(value).is_absolute()
    {
        return invalid(format!("unsafe relative object path: {value:?}"));
    }
    let mut pieces: Vec<&str> = if base.is_empty() {
        Vec::new()
    } else {
        base.split('/').collect()
    };
    for component in Path::new(value).components() {
        match component {
            Component::Normal(piece) => {
                let piece = piece.to_str().ok_or_else(|| {
                    HailError::InvalidFormat("object path is not valid UTF-8".to_string())
                })?;
                if piece.is_empty() {
                    return invalid(format!("unsafe relative object path: {value:?}"));
                }
                pieces.push(piece);
            }
            Component::ParentDir => {
                if pieces.pop().is_none() {
                    return invalid(format!(
                        "relative object path escapes table root: {value:?}"
                    ));
                }
            }
            Component::CurDir => {}
            Component::RootDir | Component::Prefix(_) => {
                return invalid(format!("unsafe relative object path: {value:?}"));
            }
        }
    }
    if pieces.is_empty() {
        return invalid(format!(
            "relative object path resolves to table root: {value:?}"
        ));
    }
    Ok(RelativeObjectPath(pieces.join("/")))
}

fn is_descendant_or_equal(path: &RelativeObjectPath, root: &RelativeObjectPath) -> bool {
    path == root
        || path
            .as_str()
            .strip_prefix(root.as_str())
            .is_some_and(|rest| rest.starts_with('/'))
}

fn is_hadoop_checksum(path: &RelativeObjectPath) -> bool {
    path.as_str()
        .rsplit('/')
        .next()
        .is_some_and(|name| name.starts_with('.') && name.ends_with(".crc") && name.len() > 5)
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(HailError::InvalidFormat(message.into()))
}

/// Local content-addressed inventory backend.
pub struct LocalInventoryBackend {
    root: PathBuf,
    canonical_root: String,
}

impl LocalInventoryBackend {
    pub fn new(root: &str) -> Result<Self> {
        if root.contains("://") {
            return invalid("local inventory root must be a filesystem path");
        }
        let supplied = Path::new(root);
        if !supplied.is_absolute() {
            return invalid("local inventory root must be absolute");
        }
        let canonical = fs::canonicalize(supplied)?;
        let metadata = fs::symlink_metadata(&canonical)?;
        if !metadata.is_dir() {
            return invalid(format!(
                "inventory root is not a directory: {}",
                canonical.display()
            ));
        }
        let canonical_root = canonical
            .to_str()
            .ok_or_else(|| {
                HailError::InvalidFormat("local inventory root is not valid UTF-8".to_string())
            })?
            .to_string();
        Ok(Self {
            root: canonical,
            canonical_root,
        })
    }

    fn full_path(&self, relative: &RelativeObjectPath) -> PathBuf {
        let mut path = self.root.clone();
        for piece in relative.as_str().split('/') {
            path.push(piece);
        }
        path
    }

    fn source_object(&self, relative: &RelativeObjectPath) -> Result<SourceObject> {
        let path = self.full_path(relative);
        let path_metadata = fs::symlink_metadata(&path).map_err(|error| {
            HailError::InvalidFormat(format!(
                "cannot stat required object {}: {error}",
                relative.as_str()
            ))
        })?;
        if path_metadata.file_type().is_symlink() {
            return invalid(format!(
                "symlink is forbidden in source inventory: {}",
                relative.as_str()
            ));
        }
        if !path_metadata.is_file() {
            return invalid(format!(
                "required object is not a regular file: {}",
                relative.as_str()
            ));
        }
        let file = File::open(&path)?;
        let before = file.metadata()?;
        let mut reader = BufReader::new(file);
        let mut hasher = Sha256::new();
        let mut size = 0u64;
        let mut buffer = [0u8; 1024 * 1024];
        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            size = size.checked_add(count as u64).ok_or_else(|| {
                HailError::InvalidFormat(format!("object size overflow: {}", relative.as_str()))
            })?;
            hasher.update(&buffer[..count]);
        }
        let after_open = reader.get_ref().metadata()?;
        let after_path = fs::symlink_metadata(&path)?;
        if after_path.file_type().is_symlink()
            || !after_path.is_file()
            || before.len() != size
            || after_open.len() != size
            || after_path.len() != size
            || before.modified().ok() != after_open.modified().ok()
            || before.modified().ok() != after_path.modified().ok()
        {
            return invalid(format!(
                "local object changed while hashing: {}",
                relative.as_str()
            ));
        }
        let digest = format!("{:x}", hasher.finalize());
        let synthetic = format!("local-sha256:{digest}");
        let uri = path
            .to_str()
            .ok_or_else(|| {
                HailError::InvalidFormat(format!(
                    "local object path is not UTF-8: {}",
                    path.display()
                ))
            })?
            .to_string();
        Ok(SourceObject {
            uri,
            version: synthetic.clone(),
            etag: synthetic,
            size,
            crc32c: None,
            sha256: Some(digest),
        })
    }
}

impl SourceInventoryBackend for LocalInventoryBackend {
    fn canonical_root(&self) -> &str {
        &self.canonical_root
    }

    fn list(&mut self, max_objects: usize) -> Result<ObservedListing> {
        let mut listing = ObservedListing::default();
        let mut pending = vec![(self.root.clone(), String::new())];
        while let Some((directory, prefix)) = pending.pop() {
            let mut entries: Vec<_> = fs::read_dir(&directory)?.collect::<std::io::Result<_>>()?;
            entries.sort_by_key(|entry| entry.file_name());
            for entry in entries {
                let name = entry.file_name().into_string().map_err(|_| {
                    HailError::InvalidFormat("source path is not valid UTF-8".to_string())
                })?;
                let relative_text = if prefix.is_empty() {
                    name
                } else {
                    format!("{prefix}/{name}")
                };
                let relative = RelativeObjectPath::new(&relative_text)?;
                let metadata = fs::symlink_metadata(entry.path())?;
                if metadata.file_type().is_symlink() {
                    if listing.objects.len() >= max_objects {
                        return invalid(format!(
                            "source listing exceeds max_objects {max_objects}"
                        ));
                    }
                    listing.insert(
                        relative,
                        ObservedObject {
                            size: metadata.len(),
                            listing_identity: format!("local-symlink:{}", metadata.len()),
                            directory_marker: false,
                            symlink: true,
                        },
                    )?;
                } else if metadata.is_dir() {
                    pending.push((entry.path(), relative_text));
                } else if metadata.is_file() {
                    if listing.objects.len() >= max_objects {
                        return invalid(format!(
                            "source listing exceeds max_objects {max_objects}"
                        ));
                    }
                    let modified = metadata
                        .modified()
                        .ok()
                        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|duration| duration.as_nanos().to_string())
                        .unwrap_or_else(|| "unknown".to_string());
                    listing.insert(
                        relative,
                        ObservedObject {
                            size: metadata.len(),
                            listing_identity: format!("local:{}:{modified}", metadata.len()),
                            directory_marker: false,
                            symlink: false,
                        },
                    )?;
                }
            }
        }
        Ok(listing)
    }

    fn stat(&mut self, relative: &RelativeObjectPath) -> Result<SourceObject> {
        self.source_object(relative)
    }

    fn read_pinned(
        &mut self,
        relative: &RelativeObjectPath,
        identity: &SourceObject,
        max_bytes: u64,
    ) -> Result<Vec<u8>> {
        if identity.size > max_bytes {
            return invalid(format!(
                "metadata object {} exceeds byte limit {}",
                relative.as_str(),
                max_bytes
            ));
        }
        let current = self.source_object(relative)?;
        if &current != identity {
            return invalid(format!(
                "metadata object changed before read: {}",
                relative.as_str()
            ));
        }
        let path = self.full_path(relative);
        let file = File::open(path)?;
        let capacity = usize::try_from(identity.size).map_err(|_| {
            HailError::InvalidFormat(format!(
                "metadata object is too large: {}",
                relative.as_str()
            ))
        })?;
        let mut bytes = Vec::with_capacity(capacity);
        file.take(max_bytes + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > max_bytes {
            return invalid(format!(
                "metadata object {} exceeds byte limit {}",
                relative.as_str(),
                max_bytes
            ));
        }
        let after = self.source_object(relative)?;
        if after != *identity {
            return invalid(format!(
                "metadata object changed during read: {}",
                relative.as_str()
            ));
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn gzip_json(value: serde_json::Value) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder
            .write_all(&serde_json::to_vec(&value).unwrap())
            .unwrap();
        encoder.finish().unwrap()
    }

    fn component(parts: &[&str], index: bool) -> serde_json::Value {
        let bounds: Vec<_> = parts
            .iter()
            .map(|_| {
                serde_json::json!({
                    "start": null, "end": null, "includeStart": true, "includeEnd": false
                })
            })
            .collect();
        serde_json::json!({
            "name":"RVDComponentSpec", "_key":[],
            "_codecSpec":{"name":"TypedCodecSpec","_eType":"EBaseStruct{}","_vType":"Struct{}","_bufferSpec":{"name":"StreamBlockBufferSpec"}},
            "_indexSpec": if index { serde_json::json!({"name":"IndexSpec","_relPath":"../index","_leafCodec":{"name":"TypedCodecSpec","_eType":"EBaseStruct{}","_vType":"Struct{}","_bufferSpec":{"name":"StreamBlockBufferSpec"}},"_internalNodeCodec":{"name":"TypedCodecSpec","_eType":"EBaseStruct{}","_vType":"Struct{}","_bufferSpec":{"name":"StreamBlockBufferSpec"}},"_keyType":"Struct{}","_annotationType":"Struct{}"}) } else { serde_json::Value::Null },
            "_partFiles":parts, "_jRangeBounds":bounds, "_attrs":{}
        })
    }

    fn index_metadata(path: &str) -> serde_json::Value {
        serde_json::json!({"fileVersion":66048,"branchingFactor":4096,"height":1,"keyType":"Struct{}","annotationType":"Struct{}","nKeys":0,"indexPath":path,"rootOffset":0,"attributes":{}})
    }

    fn make_table(index: bool) -> tempfile::TempDir {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        for dir in ["rows/parts", "globals/parts", "references"] {
            fs::create_dir_all(root.join(dir)).unwrap();
        }
        fs::write(root.join("_SUCCESS"), b"").unwrap();
        fs::write(root.join("metadata.json.gz"), gzip_json(serde_json::json!({
            "file_version":67328,"hail_version":"test","references_rel_path":"references",
            "components":{"rows":{"rel_path":"rows"},"globals":{"rel_path":"globals"},"partition_counts":{"counts":[1]}}
        }))).unwrap();
        fs::write(
            root.join("rows/metadata.json.gz"),
            gzip_json(component(&["part-a"], index)),
        )
        .unwrap();
        fs::write(
            root.join("globals/metadata.json.gz"),
            gzip_json(component(&["part-g"], false)),
        )
        .unwrap();
        fs::write(root.join("rows/parts/part-a"), b"row").unwrap();
        fs::write(root.join("globals/parts/part-g"), b"global").unwrap();
        if index {
            fs::create_dir_all(root.join("index/part-a.idx")).unwrap();
            fs::write(
                root.join("index/part-a.idx/metadata.json.gz"),
                gzip_json(index_metadata("custom-index")),
            )
            .unwrap();
            fs::write(root.join("index/part-a.idx/custom-index"), b"index").unwrap();
        }
        temp
    }

    #[test]
    fn relative_paths_reject_escape_and_nested_parts() {
        assert!(RelativeObjectPath::new("../escape").is_err());
        assert!(RelativeObjectPath::new("/absolute").is_err());
        assert!(validate_part_name("nested/part").is_err());
        assert_eq!(
            normalize_relative("rows", "../index").unwrap().as_str(),
            "index"
        );
    }

    #[test]
    fn local_sequential_and_indexed_closures_are_exact_and_stable() {
        let temp = make_table(true);
        fs::write(temp.path().join("README.txt"), b"ignored").unwrap();
        fs::write(temp.path().join("rows/parts/.part-a.crc"), b"ignored").unwrap();
        let root = temp.path().to_str().unwrap();
        let sequential = inventory_hail_table_path(root, &InventoryOptions::default()).unwrap();
        assert_eq!(sequential.manifest.objects.len(), 6);
        assert_eq!(sequential.ignored.sequential_index_objects, 2);
        assert_eq!(sequential.ignored.hadoop_checksums, 1);
        assert_eq!(sequential.ignored.unrelated, 1);
        let again = inventory_hail_table_path(root, &InventoryOptions::default()).unwrap();
        assert_eq!(
            sequential.manifest.canonical_bytes().unwrap(),
            again.manifest.canonical_bytes().unwrap()
        );
        assert_eq!(sequential.digest_sha256, again.digest_sha256);
        for object in &sequential.manifest.objects {
            assert!(object.uri.starts_with('/'));
            assert!(object.sha256.is_some());
            assert!(object.version.starts_with("local-sha256:"));
            assert_eq!(object.version, object.etag);
            assert!(object.crc32c.is_none());
        }

        let indexed = inventory_hail_table_path(
            root,
            &InventoryOptions {
                access: InventoryAccess::Indexed,
                ..InventoryOptions::default()
            },
        )
        .unwrap();
        assert_eq!(indexed.manifest.objects.len(), 8);
        assert!(indexed
            .roles
            .iter()
            .any(|role| role.path.as_str().ends_with("custom-index")));
    }

    #[test]
    fn missing_success_and_unexpected_component_object_fail() {
        let temp = make_table(false);
        fs::remove_file(temp.path().join("_SUCCESS")).unwrap();
        let error =
            inventory_hail_table_path(temp.path().to_str().unwrap(), &InventoryOptions::default())
                .unwrap_err()
                .to_string();
        assert!(error.contains("_SUCCESS"), "{error}");

        fs::write(temp.path().join("_SUCCESS"), b"").unwrap();
        fs::write(temp.path().join("rows/parts/stale"), b"stale").unwrap();
        let error =
            inventory_hail_table_path(temp.path().to_str().unwrap(), &InventoryOptions::default())
                .unwrap_err()
                .to_string();
        assert!(error.contains("unexpected relevant"), "{error}");
    }

    #[test]
    fn references_are_recursive_and_tiny_fixture_without_success_is_negative() {
        let temp = make_table(false);
        fs::create_dir_all(temp.path().join("references/grch38")).unwrap();
        fs::write(temp.path().join("references/grch38/reference.json"), b"{}").unwrap();
        let result =
            inventory_hail_table_path(temp.path().to_str().unwrap(), &InventoryOptions::default())
                .unwrap();
        assert_eq!(result.manifest.objects.len(), 7);

        let fixture = fs::canonicalize("tests/fixtures/tiny-keyed.ht").unwrap();
        let error =
            inventory_hail_table_path(fixture.to_str().unwrap(), &InventoryOptions::default())
                .unwrap_err()
                .to_string();
        assert!(error.contains("_SUCCESS"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn unrelated_symlink_is_ignored_but_required_symlink_fails() {
        use std::os::unix::fs::symlink;

        let temp = make_table(false);
        symlink("README-target", temp.path().join("operator-link")).unwrap();
        let result =
            inventory_hail_table_path(temp.path().to_str().unwrap(), &InventoryOptions::default())
                .unwrap();
        assert_eq!(result.ignored.unrelated, 1);

        fs::remove_file(temp.path().join("rows/parts/part-a")).unwrap();
        symlink(
            "../../globals/parts/part-g",
            temp.path().join("rows/parts/part-a"),
        )
        .unwrap();
        let error =
            inventory_hail_table_path(temp.path().to_str().unwrap(), &InventoryOptions::default())
                .unwrap_err()
                .to_string();
        assert!(error.contains("symlink is forbidden"), "{error}");
    }

    #[test]
    fn indexed_profile_rejects_extra_index_artifacts() {
        let temp = make_table(true);
        fs::write(temp.path().join("index/part-a.idx/stale"), b"stale").unwrap();
        let error = inventory_hail_table_path(
            temp.path().to_str().unwrap(),
            &InventoryOptions {
                access: InventoryAccess::Indexed,
                ..InventoryOptions::default()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("unexpected relevant"), "{error}");
    }

    #[test]
    fn full_local_fixture_has_expected_profile_counts() {
        let fixture = fs::canonicalize("../test_hail_data/01_single_int.ht").unwrap();
        let sequential =
            inventory_hail_table_path(fixture.to_str().unwrap(), &InventoryOptions::default())
                .unwrap();
        assert_eq!(sequential.manifest.objects.len(), 17);
        let indexed = inventory_hail_table_path(
            fixture.to_str().unwrap(),
            &InventoryOptions {
                access: InventoryAccess::Indexed,
                ..InventoryOptions::default()
            },
        )
        .unwrap();
        assert_eq!(indexed.manifest.objects.len(), 41);
    }
}
