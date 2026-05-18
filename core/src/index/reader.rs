//! Index reader for B-tree index files

use crate::buffer::{InputBuffer, LEB128Buffer};
use crate::codec::{EncodedType, ETypeParser};
use crate::index::{IndexMetadata, IndexNode, InternalNode, LeafNode};
use crate::io::join_path;
use crate::metadata::IndexSpec;
use crate::HailError;
use crate::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// Index reader for Hail B-tree indexes
pub struct IndexReader {
    _index_dir: String,
    metadata: IndexMetadata,
    _leaf_type: EncodedType,
    _internal_type: EncodedType,
    node_cache: HashMap<u64, IndexNode>,
}

impl IndexReader {
    /// Create a new index reader from a local index directory and spec
    ///
    /// # Arguments
    /// * `index_dir` - Path to the index directory (e.g., `.../index/part-0-xxx.idx`)
    /// * `spec` - Index specification from the table metadata
    ///
    /// This loads all index nodes into memory for fast lookups.
    pub fn new<P: AsRef<Path>>(index_dir: P, spec: &IndexSpec) -> Result<Self> {
        let index_dir_str = index_dir.as_ref().to_string_lossy().to_string();
        Self::new_from_path(&index_dir_str, spec)
    }

    /// Create a new index reader from a path string (local or cloud URL)
    ///
    /// # Arguments
    /// * `index_dir` - Path to the index directory (e.g., `gs://bucket/table.ht/index/part-0-xxx.idx`)
    /// * `spec` - Index specification from the table metadata
    ///
    /// # Supported URL schemes
    /// - `gs://bucket/path` - Google Cloud Storage
    /// - `s3://bucket/path` - Amazon S3
    /// - `http://` or `https://` - HTTP(S) URLs
    /// - Local file path - Regular file system access
    ///
    /// This loads all index nodes into memory for fast lookups.
    pub fn new_from_path(index_dir: &str, spec: &IndexSpec) -> Result<Self> {
        // Read index metadata
        let metadata_path = join_path(index_dir, "metadata.json.gz");
        let metadata = IndexMetadata::from_path(&metadata_path)?;

        // Parse the EType for leaf nodes
        let leaf_type = ETypeParser::parse(&spec.leaf_codec.e_type)?;

        // Parse the EType for internal nodes
        let internal_type = ETypeParser::parse(&spec.internal_node_codec.e_type)?;

        // Load all nodes from the index file into cache
        let node_cache = Self::load_all_nodes_from_path(
            index_dir,
            &metadata,
            &leaf_type,
            &internal_type,
        )?;

        Ok(IndexReader {
            _index_dir: index_dir.to_string(),
            metadata,
            _leaf_type: leaf_type,
            _internal_type: internal_type,
            node_cache,
        })
    }

    /// Load all nodes from the index file into a cache (local path version)
    ///
    /// Index files are typically small, so we load them entirely into memory.
    ///
    /// # Virtual Offsets
    /// Hail uses "virtual offsets" for index nodes when using BlockingBuffer (default).
    /// A virtual offset is a 64-bit integer:
    /// - High 48 bits: Physical file offset of the start of the block
    /// - Low 16 bits: Offset within the decompressed block
    ///
    /// We must read the file block-by-block, track physical offsets, and map them
    /// to memory offsets in our decompressed buffer.
    #[allow(dead_code)]
    fn load_all_nodes(
        index_dir: &Path,
        metadata: &IndexMetadata,
        leaf_type: &EncodedType,
        internal_type: &EncodedType,
    ) -> Result<HashMap<u64, IndexNode>> {
        let index_file_path = index_dir.join(&metadata.index_path);
        let mut file = File::open(&index_file_path)?;
        Self::load_all_nodes_from_reader(&mut file, metadata, leaf_type, internal_type)
    }

    /// Load all nodes from the index file into a cache (cloud path version)
    fn load_all_nodes_from_path(
        index_dir: &str,
        metadata: &IndexMetadata,
        leaf_type: &EncodedType,
        internal_type: &EncodedType,
    ) -> Result<HashMap<u64, IndexNode>> {
        let index_file_path = join_path(index_dir, &metadata.index_path);
        let mut reader = crate::io::get_reader(&index_file_path)?;

        // Read the entire file into memory first (index files are small)
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        let mut cursor = std::io::Cursor::new(data);

        Self::load_all_nodes_from_reader(&mut cursor, metadata, leaf_type, internal_type)
    }

    /// Load all nodes from any reader that implements Read + Seek
    fn load_all_nodes_from_reader<R: Read + Seek>(
        reader: &mut R,
        metadata: &IndexMetadata,
        leaf_type: &EncodedType,
        internal_type: &EncodedType,
    ) -> Result<HashMap<u64, IndexNode>> {

        // Map physical file offset -> offset in decompressed_data
        let mut block_map: HashMap<u64, usize> = HashMap::new();
        let mut decompressed_data: Vec<u8> = Vec::new();

        // Read blocks until EOF
        loop {
            let phys_offset = reader.seek(SeekFrom::Current(0))?;

            // Read 4-byte block length
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break, // EOF
                Err(e) => return Err(e.into()),
            }
            let block_len = u32::from_le_bytes(len_buf) as usize;

            // Read compressed block data
            let mut compressed = vec![0u8; block_len];
            reader.read_exact(&mut compressed)?;

            // Decompress (format: [4-byte decompressed len][zstd data])
            if block_len < 4 {
                return Err(HailError::InvalidFormat("Block too small".to_string()));
            }

            let expected_len = i32::from_le_bytes([
                compressed[0],
                compressed[1],
                compressed[2],
                compressed[3],
            ]) as usize;

            let zstd_data = &compressed[4..];
            let decompressed_block =
                zstd::decode_all(zstd_data).map_err(|_| HailError::Zstd)?;

            if decompressed_block.len() != expected_len {
                return Err(HailError::InvalidFormat(format!(
                    "Decompressed size mismatch: expected {}, got {}",
                    expected_len,
                    decompressed_block.len()
                )));
            }

            // Record mapping and append data
            block_map.insert(phys_offset, decompressed_data.len());
            decompressed_data.extend_from_slice(&decompressed_block);
        }

        // Helper to resolve offsets
        // Hail index offsets can be either:
        // 1. Direct physical file offsets (for root_offset) - maps to start of a block
        // 2. Virtual offsets (for child index_file_offset) - (phys_offset << 16) | local_offset
        //
        // We detect which by checking if the offset matches a block boundary directly.
        // If it does, we use the block's start. Otherwise we decode as virtual offset.
        let resolve_offset = |v_off: u64| -> Result<usize> {
            // First check if this is a direct physical block offset
            if let Some(&mem_base) = block_map.get(&v_off) {
                return Ok(mem_base);
            }

            // Otherwise decode as virtual offset
            let phys = v_off >> 16;
            let local = (v_off & 0xFFFF) as usize;

            let mem_base = block_map.get(&phys).ok_or_else(|| {
                HailError::Index(format!(
                    "Invalid physical offset in virtual offset: {} (from v_off={})",
                    phys, v_off
                ))
            })?;

            let mem_offset = mem_base + local;
            if mem_offset >= decompressed_data.len() {
                return Err(HailError::Index(format!(
                    "Virtual offset out of bounds: {} -> {}",
                    v_off, mem_offset
                )));
            }
            Ok(mem_offset)
        };

        // Parse nodes starting from root
        let mut cache = HashMap::new();
        let mut parsed_offsets = std::collections::HashSet::new();
        let mut queue: Vec<u64> = vec![metadata.root_offset];

        while let Some(v_offset) = queue.pop() {
            if parsed_offsets.contains(&v_offset) {
                continue;
            }

            let mem_offset = resolve_offset(v_offset)?;

            // Create a buffer slice for this node
            let slice = &decompressed_data[mem_offset..];
            let slice_buffer = SliceBuffer {
                data: slice,
                position: 0,
            };
            let mut leb_buffer = LEB128Buffer::new(slice_buffer);

            // Read node type
            let node_type = match leb_buffer.read_u8() {
                Ok(b) => b,
                Err(_) => continue,
            };

            let node = match node_type {
                0 => {
                    // Leaf
                    let value = leaf_type.read_present_value(&mut leb_buffer)?;
                    IndexNode::Leaf(LeafNode::from_encoded(value)?)
                }
                1 => {
                    // Internal
                    let value = internal_type.read_present_value(&mut leb_buffer)?;
                    let internal = InternalNode::from_encoded(value)?;

                    // Enqueue children
                    for child in &internal.children {
                        queue.push(child.index_file_offset as u64);
                    }
                    IndexNode::Internal(internal)
                }
                _ => {
                    return Err(HailError::InvalidFormat(format!(
                        "Unknown node type: {}",
                        node_type
                    )));
                }
            };

            cache.insert(v_offset, node);
            parsed_offsets.insert(v_offset);
        }

        Ok(cache)
    }

    /// Get the index metadata
    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    /// Read a node at the given offset in the index file
    ///
    /// Nodes are read from an in-memory cache that was loaded during initialization.
    pub fn read_node(&self, offset: u64) -> Result<&IndexNode> {
        self.node_cache
            .get(&offset)
            .ok_or_else(|| HailError::Index(format!("No node found at offset {}", offset)))
    }

    /// Perform a point lookup in the index
    ///
    /// Returns the byte offset in the partition data file where the row
    /// with the given key can be found, or None if the key doesn't exist.
    pub fn lookup(&self, key: &crate::codec::EncodedValue) -> Result<Option<i64>> {
        self.lower_bound(key, self.metadata.height - 1, self.metadata.root_offset)
    }

    /// Seek to the lower bound of a key in the index
    ///
    /// Returns the byte offset (decompressed) of the first indexed entry
    /// where `search_key <= entry.key`. This enables range queries by seeking
    /// to the start of the interval within a partition.
    ///
    /// Returns `None` if all entries in the leaf are strictly less than `search_key`,
    /// meaning the interval starts after all indexed data in this partition path.
    pub fn seek_lower_bound(&self, key: &crate::codec::EncodedValue) -> Result<Option<i64>> {
        let result = self.lower_bound_seek(key, self.metadata.height - 1, self.metadata.root_offset);
        tracing::debug!("seek_lower_bound: height={}, result={:?}", self.metadata.height, result);
        result
    }

    /// B-tree traversal for lower bound seek (recursive)
    ///
    /// Internal nodes use the same traversal as `lower_bound` (find last child
    /// where `first_key <= search_key`). Leaf nodes find the first entry where
    /// `search_key <= entry.key` instead of requiring exact match.
    fn lower_bound_seek(&self, key: &crate::codec::EncodedValue, level: u32, offset: u64) -> Result<Option<i64>> {
        let node = self.read_node(offset)?;

        if level == 0 {
            match node {
                IndexNode::Leaf(leaf) => {
                    for entry in &leaf.keys {
                        if Self::key_less_or_equal(key, &entry.key) {
                            return Ok(Some(entry.offset));
                        }
                    }
                    Ok(None)
                }
                _ => Err(HailError::Codec(
                    "Expected leaf node at level 0".to_string(),
                )),
            }
        } else {
            match node {
                IndexNode::Internal(internal) => {
                    let mut child_idx = 0;
                    for (i, entry) in internal.children.iter().enumerate() {
                        if Self::key_less_or_equal(&entry.first_key, key) {
                            child_idx = i;
                        } else {
                            break;
                        }
                    }
                    let child_offset = internal.children[child_idx].index_file_offset as u64;
                    match self.lower_bound_seek(key, level - 1, child_offset) {
                        Ok(Some(offset)) => Ok(Some(offset)),
                        Ok(None) => {
                            // All keys in the subtree were < search_key (can happen with
                            // prefix/partial keys). The true lower bound is the first
                            // record of the next child.
                            if child_idx + 1 < internal.children.len() {
                                Ok(Some(internal.children[child_idx + 1].first_record_offset))
                            } else {
                                Ok(None)
                            }
                        }
                        Err(e) => Err(e),
                    }
                }
                _ => Err(HailError::Codec(
                    "Expected internal node at non-zero level".to_string(),
                )),
            }
        }
    }

    /// Binary search for a key in the B-tree (recursive)
    ///
    /// This implements the lower_bound algorithm from Hail's IndexReader.scala
    fn lower_bound(&self, key: &crate::codec::EncodedValue, level: u32, offset: u64) -> Result<Option<i64>> {
        let node = self.read_node(offset)?;

        if level == 0 {
            // We're at a leaf node
            match node {
                IndexNode::Leaf(leaf) => {
                    // Binary search for the key in the leaf
                    for entry in &leaf.keys {
                        if Self::keys_equal(&entry.key, key) {
                            return Ok(Some(entry.offset));
                        }
                    }
                    Ok(None)
                }
                _ => Err(HailError::Codec(
                    "Expected leaf node at level 0".to_string(),
                )),
            }
        } else {
            // We're at an internal node
            match node {
                IndexNode::Internal(internal) => {
                    // Find the correct child to descend into
                    // We want the last child whose first_key <= query_key
                    let mut child_idx = 0;
                    for (i, entry) in internal.children.iter().enumerate() {
                        if Self::key_less_or_equal(&entry.first_key, key) {
                            child_idx = i;
                        } else {
                            break;
                        }
                    }

                    // Recurse into the child
                    let child_offset = internal.children[child_idx].index_file_offset as u64;
                    self.lower_bound(key, level - 1, child_offset)
                }
                _ => Err(HailError::Codec(
                    "Expected internal node at non-zero level".to_string(),
                )),
            }
        }
    }

    /// Compare two keys for equality
    fn keys_equal(a: &crate::codec::EncodedValue, b: &crate::codec::EncodedValue) -> bool {
        matches!(Self::compare_encoded_values(a, b), Some(std::cmp::Ordering::Equal))
    }

    /// Check if key a <= key b
    fn key_less_or_equal(a: &crate::codec::EncodedValue, b: &crate::codec::EncodedValue) -> bool {
        matches!(Self::compare_encoded_values(a, b), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal))
    }

    /// Recursively compare two EncodedValues with genomic-aware ordering.
    ///
    /// Handles nested Struct and Array types. For Structs with different lengths
    /// (e.g., partial seek key vs full index key), the shorter one compares as Less
    /// when all overlapping fields are equal.
    /// Binary values that look like chromosome names use genomic contig ordering.
    fn compare_encoded_values(
        a: &crate::codec::EncodedValue,
        b: &crate::codec::EncodedValue,
    ) -> Option<std::cmp::Ordering> {
        use crate::codec::EncodedValue;
        match (a, b) {
            (EncodedValue::Struct(a_fields), EncodedValue::Struct(b_fields)) => {
                for ((_, a_val), (_, b_val)) in a_fields.iter().zip(b_fields.iter()) {
                    match Self::compare_encoded_values(a_val, b_val) {
                        Some(std::cmp::Ordering::Equal) => continue,
                        other => return other,
                    }
                }
                // Shorter struct is Less when all overlapping fields match
                Some(a_fields.len().cmp(&b_fields.len()))
            }
            (EncodedValue::Array(a_elems), EncodedValue::Array(b_elems)) => {
                for (a_val, b_val) in a_elems.iter().zip(b_elems.iter()) {
                    match Self::compare_encoded_values(a_val, b_val) {
                        Some(std::cmp::Ordering::Equal) => continue,
                        other => return other,
                    }
                }
                Some(a_elems.len().cmp(&b_elems.len()))
            }
            (EncodedValue::Binary(a), EncodedValue::Binary(b)) => {
                let a_str = String::from_utf8_lossy(a);
                let b_str = String::from_utf8_lossy(b);
                // Use genomic contig ordering if both look like chromosome names
                match (contig_sort_index(&a_str), contig_sort_index(&b_str)) {
                    (Some(ai), Some(bi)) => Some(ai.cmp(&bi)),
                    _ => Some(a_str.cmp(&b_str)),
                }
            }
            (EncodedValue::Int32(a), EncodedValue::Int32(b)) => Some(a.cmp(b)),
            (EncodedValue::Int64(a), EncodedValue::Int64(b)) => Some(a.cmp(b)),
            (EncodedValue::Float32(a), EncodedValue::Float32(b)) => a.partial_cmp(b),
            (EncodedValue::Float64(a), EncodedValue::Float64(b)) => a.partial_cmp(b),
            (EncodedValue::Boolean(a), EncodedValue::Boolean(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

/// Map a contig name to its sort index in standard reference genomes.
/// Returns None if the name doesn't match a known contig, falling back to lexicographic.
fn contig_sort_index(name: &str) -> Option<usize> {
    // GRCh38 ordering (chr-prefixed)
    const GRCH38: &[&str] = &[
        "chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10",
        "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19", "chr20",
        "chr21", "chr22", "chrX", "chrY", "chrM",
    ];
    // GRCh37 ordering (no prefix)
    const GRCH37: &[&str] = &[
        "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
        "21", "22", "X", "Y", "MT",
    ];
    GRCH38.iter().position(|&c| c == name)
        .or_else(|| GRCH37.iter().position(|&c| c == name))
}

// Simple buffer wrapper for in-memory slices
struct SliceBuffer<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> InputBuffer for SliceBuffer<'a> {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        if self.position + buf.len() > self.data.len() {
            return Err(HailError::UnexpectedEof);
        }
        buf.copy_from_slice(&self.data[self.position..self.position + buf.len()]);
        self.position += buf.len();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::EncodedValue;

    #[test]
    fn test_compare_encoded_values_contig_ordering() {
        // Genomic ordering: chr2 < chr17 (not lexicographic where "chr17" < "chr2")
        let chr2 = EncodedValue::Binary(b"chr2".to_vec());
        let chr17 = EncodedValue::Binary(b"chr17".to_vec());
        assert_eq!(
            IndexReader::compare_encoded_values(&chr2, &chr17),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            IndexReader::compare_encoded_values(&chr17, &chr2),
            Some(std::cmp::Ordering::Greater)
        );
    }

    #[test]
    fn test_compare_encoded_values_struct_prefix() {
        // A shorter struct (prefix key) should be Less than a longer one when overlapping fields match
        let seek_key = EncodedValue::Struct(vec![
            ("locus".to_string(), EncodedValue::Struct(vec![
                ("contig".to_string(), EncodedValue::Binary(b"chr17".to_vec())),
                ("position".to_string(), EncodedValue::Int32(43044295)),
            ])),
        ]);
        let index_key = EncodedValue::Struct(vec![
            ("locus".to_string(), EncodedValue::Struct(vec![
                ("contig".to_string(), EncodedValue::Binary(b"chr17".to_vec())),
                ("position".to_string(), EncodedValue::Int32(43044295)),
            ])),
            ("alleles".to_string(), EncodedValue::Array(vec![
                EncodedValue::Binary(b"A".to_vec()),
                EncodedValue::Binary(b"G".to_vec()),
            ])),
        ]);
        // Seek key (1 field) < index key (2 fields) when locus matches
        assert_eq!(
            IndexReader::compare_encoded_values(&seek_key, &index_key),
            Some(std::cmp::Ordering::Less)
        );
        assert!(IndexReader::key_less_or_equal(&seek_key, &index_key));
    }

    #[test]
    fn test_compare_encoded_values_nested_struct() {
        // Struct comparison recurses into nested structs
        let a = EncodedValue::Struct(vec![
            ("locus".to_string(), EncodedValue::Struct(vec![
                ("contig".to_string(), EncodedValue::Binary(b"chr2".to_vec())),
                ("position".to_string(), EncodedValue::Int32(100)),
            ])),
        ]);
        let b = EncodedValue::Struct(vec![
            ("locus".to_string(), EncodedValue::Struct(vec![
                ("contig".to_string(), EncodedValue::Binary(b"chr17".to_vec())),
                ("position".to_string(), EncodedValue::Int32(100)),
            ])),
        ]);
        // chr2 < chr17 in genomic ordering
        assert!(IndexReader::key_less_or_equal(&a, &b));
        assert!(!IndexReader::key_less_or_equal(&b, &a));
    }

    #[test]
    fn test_seek_lower_bound_local_index() {
        use crate::metadata::IndexSpec;
        let idx_dir = "/tmp/gnomad-idx-4765";
        if !std::path::Path::new(idx_dir).exists() {
            eprintln!("Skipping test: {} not found (download with gsutil)", idx_dir);
            return;
        }

        let buf_spec = crate::metadata::BufferSpec::LEB128 {
            child: Box::new(crate::metadata::BufferSpec::StreamBlock),
        };
        let spec = IndexSpec {
            name: "index".to_string(),
            rel_path: "index".to_string(),
            leaf_codec: crate::metadata::CodecSpec {
                name: "leaf".to_string(),
                e_type: "EBaseStruct{first_idx:+EInt64,keys:+EArray[+EBaseStruct{key:+EBaseStruct{locus:EBaseStruct{contig:+EBinary,position:+EInt32},alleles:EArray[EBinary]},offset:+EInt64,annotation:+EBaseStruct{}}]}".to_string(),
                v_type: String::new(),
                buffer_spec: buf_spec.clone(),
            },
            internal_node_codec: crate::metadata::CodecSpec {
                name: "internal".to_string(),
                e_type: "EBaseStruct{children:+EArray[+EBaseStruct{index_file_offset:+EInt64,first_idx:+EInt64,first_key:+EBaseStruct{locus:EBaseStruct{contig:+EBinary,position:+EInt32},alleles:EArray[EBinary]},first_record_offset:+EInt64,first_annotation:+EBaseStruct{}}]}".to_string(),
                v_type: String::new(),
                buffer_spec: buf_spec,
            },
            key_type: "Struct{locus:Locus(GRCh38),alleles:Array[String]}".to_string(),
            annotation_type: "Struct{}".to_string(),
        };

        let reader = IndexReader::new(idx_dir, &spec).expect("Failed to load index");
        let meta = reader.metadata();
        eprintln!("Index: height={}, n_keys={}, branching_factor={}", meta.height, meta.n_keys, meta.branching_factor);

        // Inspect root node
        let root = reader.read_node(meta.root_offset).expect("Failed to read root");
        match root {
            IndexNode::Internal(internal) => {
                eprintln!("Root is internal with {} children", internal.children.len());
                for (i, child) in internal.children.iter().enumerate() {
                    // Pretty-print contig name from first_key
                    let contig = if let EncodedValue::Struct(fields) = &child.first_key {
                        if let Some((_, EncodedValue::Struct(locus_fields))) = fields.first() {
                            if let Some((_, EncodedValue::Binary(b))) = locus_fields.first() {
                                String::from_utf8_lossy(b).to_string()
                            } else { "?".to_string() }
                        } else { "?".to_string() }
                    } else { "?".to_string() };
                    let pos = if let EncodedValue::Struct(fields) = &child.first_key {
                        if let Some((_, EncodedValue::Struct(locus_fields))) = fields.first() {
                            if let Some((_, EncodedValue::Int32(p))) = locus_fields.get(1) {
                                *p
                            } else { 0 }
                        } else { 0 }
                    } else { 0 };
                    eprintln!("  child[{}]: {}:{}, first_record_offset={}", i, contig, pos, child.first_record_offset);
                }
            }
            IndexNode::Leaf(leaf) => {
                eprintln!("Root is leaf with {} keys", leaf.keys.len());
                for (i, entry) in leaf.keys.iter().take(3).enumerate() {
                    eprintln!("  key[{}]: {:?}, offset={}", i, entry.key, entry.offset);
                }
            }
        }

        // Test seek for chr17:43044295
        let seek_key = EncodedValue::Struct(vec![
            ("locus".to_string(), EncodedValue::Struct(vec![
                ("contig".to_string(), EncodedValue::Binary(b"chr17".to_vec())),
                ("position".to_string(), EncodedValue::Int32(43044295)),
            ])),
        ]);

        let result = reader.seek_lower_bound(&seek_key).expect("seek_lower_bound failed");
        eprintln!("seek_lower_bound(chr17:43044295) = {:?}", result);

        // Test with chr10 key (which IS in this partition)
        let seek_chr10 = EncodedValue::Struct(vec![
            ("locus".to_string(), EncodedValue::Struct(vec![
                ("contig".to_string(), EncodedValue::Binary(b"chr10".to_vec())),
                ("position".to_string(), EncodedValue::Int32(80000)),
            ])),
        ]);
        let result_chr10 = reader.seek_lower_bound(&seek_chr10).expect("seek failed");
        eprintln!("seek_lower_bound(chr10:80000) = {:?}", result_chr10);
        assert!(result_chr10.is_some(), "chr10:80000 should be found in this partition (has chr10 data)");
        let offset = result_chr10.unwrap();
        eprintln!("chr10:80000 offset = {} (non-zero: {})", offset, offset != 0);

        // Test with chr9 at the very start
        let seek_chr9_start = EncodedValue::Struct(vec![
            ("locus".to_string(), EncodedValue::Struct(vec![
                ("contig".to_string(), EncodedValue::Binary(b"chr9".to_vec())),
                ("position".to_string(), EncodedValue::Int32(138175445)),
            ])),
        ]);
        let result_chr9 = reader.seek_lower_bound(&seek_chr9_start).expect("seek failed");
        eprintln!("seek_lower_bound(chr9:138175445) = {:?}", result_chr9);
        assert!(result_chr9.is_some(), "chr9:138175445 should be found (first key)");
    }
}
