//! Block offset mapping for efficient random access to compressed partition files
//!
//! Hail partition files consist of zstd-compressed blocks. The index stores
//! decompressed byte offsets, but to read a specific row we need to know
//! which compressed block contains that offset.
//!
//! The BlockMap scans block headers (without decompressing) to build a mapping
//! from decompressed offsets to compressed file positions. This enables
//! seeking directly to the relevant block instead of reading from the beginning.

use crate::Result;
use std::io::{Read, Seek, SeekFrom};

/// Entry in the block map representing a single compressed block
#[derive(Debug, Clone, Copy)]
pub struct BlockEntry {
    /// Offset into the compressed file where this block starts
    pub file_offset: u64,
    /// Decompressed stream offset where this block's data starts
    pub decompressed_offset: u64,
    /// Size of the decompressed block data
    pub decompressed_size: u32,
}

/// Maps compressed block positions to decompressed byte offsets
///
/// This allows efficient random access to zstd-compressed partition files
/// by finding which block contains a given decompressed offset.
#[derive(Debug)]
pub struct BlockMap {
    /// Sorted list of block entries (sorted by decompressed_offset)
    blocks: Vec<BlockEntry>,
}

impl BlockMap {
    /// Build a block map by scanning block headers from a local file
    ///
    /// This reads only the block headers (8 bytes per block: 4-byte length + 4-byte decompressed size)
    /// without decompressing any data. For a file with N blocks, this reads only 8*N bytes.
    ///
    /// # Arguments
    /// * `path` - Path to the partition file (local only - use `build_from_cloud_path` for cloud)
    pub fn build_from_local_path(path: &str) -> Result<Self> {
        let mut reader = std::fs::File::open(path)?;
        let mut blocks = Vec::new();
        let mut decompressed_offset = 0u64;

        loop {
            let file_offset = reader.seek(SeekFrom::Current(0))?;

            // Read block length header (4 bytes)
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let block_len = u32::from_le_bytes(len_buf) as usize;

            if block_len < 4 {
                // Block too small to have decompressed size
                break;
            }

            // Read decompressed size (first 4 bytes of block data)
            let mut size_buf = [0u8; 4];
            reader.read_exact(&mut size_buf)?;
            let decompressed_size = i32::from_le_bytes(size_buf) as u32;

            // Skip rest of compressed data
            reader.seek(SeekFrom::Current((block_len - 4) as i64))?;

            blocks.push(BlockEntry {
                file_offset,
                decompressed_offset,
                decompressed_size,
            });
            decompressed_offset += decompressed_size as u64;
        }

        Ok(BlockMap { blocks })
    }

    /// Build a block map from either a local or cloud path
    ///
    /// Uses `crate::io::get_reader()` which returns a `PrefetchingCloudReader` for
    /// cloud paths (streaming via 32MB prefetch chunks) or a local file reader.
    /// This replaces the old `build_from_cloud_path` which made N sequential HTTP
    /// range requests (one per block header), causing massive latency on remote files.
    pub fn build_from_path(path: &str) -> Result<Self> {
        let reader = crate::io::get_reader(path)?;
        Self::build_from_reader(reader)
    }

    /// Build a block map from any reader that implements Read + Seek
    pub fn build_from_reader<R: Read + Seek>(mut reader: R) -> Result<Self> {
        let mut blocks = Vec::new();
        let mut decompressed_offset = 0u64;

        loop {
            let file_offset = reader.seek(SeekFrom::Current(0))?;

            // Read block length header (4 bytes)
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let block_len = u32::from_le_bytes(len_buf) as usize;

            if block_len < 4 {
                break;
            }

            // Read decompressed size (first 4 bytes of block data)
            let mut size_buf = [0u8; 4];
            reader.read_exact(&mut size_buf)?;
            let decompressed_size = i32::from_le_bytes(size_buf) as u32;

            // Skip rest of compressed data
            reader.seek(SeekFrom::Current((block_len - 4) as i64))?;

            blocks.push(BlockEntry {
                file_offset,
                decompressed_offset,
                decompressed_size,
            });
            decompressed_offset += decompressed_size as u64;
        }

        Ok(BlockMap { blocks })
    }

    /// Find the block containing the given decompressed offset
    ///
    /// Returns the block entry if found, or None if the offset is out of bounds.
    ///
    /// # Arguments
    /// * `decompressed_offset` - Byte offset into the decompressed stream
    pub fn find_block(&self, decompressed_offset: u64) -> Option<&BlockEntry> {
        if self.blocks.is_empty() {
            return None;
        }

        // Binary search for the block containing this offset
        // We want the largest block where decompressed_offset <= target
        let idx = self.blocks.partition_point(|entry| entry.decompressed_offset <= decompressed_offset);

        if idx == 0 {
            // Offset is before the first block
            return None;
        }

        let block = &self.blocks[idx - 1];

        // Verify the offset is within this block
        if decompressed_offset < block.decompressed_offset + block.decompressed_size as u64 {
            Some(block)
        } else {
            // Offset is past the end of the last block
            None
        }
    }

    /// Get the total number of blocks
    pub fn num_blocks(&self) -> usize {
        self.blocks.len()
    }

    /// Get the total decompressed size (sum of all block sizes)
    pub fn total_decompressed_size(&self) -> u64 {
        self.blocks
            .last()
            .map(|b| b.decompressed_offset + b.decompressed_size as u64)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_block_map() -> BlockMap {
        // Simulate a file with 3 blocks:
        // Block 0: file_offset=0, decompressed_offset=0, size=1000
        // Block 1: file_offset=100, decompressed_offset=1000, size=2000
        // Block 2: file_offset=250, decompressed_offset=3000, size=1500
        BlockMap {
            blocks: vec![
                BlockEntry {
                    file_offset: 0,
                    decompressed_offset: 0,
                    decompressed_size: 1000,
                },
                BlockEntry {
                    file_offset: 100,
                    decompressed_offset: 1000,
                    decompressed_size: 2000,
                },
                BlockEntry {
                    file_offset: 250,
                    decompressed_offset: 3000,
                    decompressed_size: 1500,
                },
            ],
        }
    }

    #[test]
    fn test_find_block_first() {
        let map = create_test_block_map();
        let block = map.find_block(500).unwrap();
        assert_eq!(block.file_offset, 0);
        assert_eq!(block.decompressed_offset, 0);
    }

    #[test]
    fn test_find_block_middle() {
        let map = create_test_block_map();
        let block = map.find_block(1500).unwrap();
        assert_eq!(block.file_offset, 100);
        assert_eq!(block.decompressed_offset, 1000);
    }

    #[test]
    fn test_find_block_last() {
        let map = create_test_block_map();
        let block = map.find_block(4000).unwrap();
        assert_eq!(block.file_offset, 250);
        assert_eq!(block.decompressed_offset, 3000);
    }

    #[test]
    fn test_find_block_boundary() {
        let map = create_test_block_map();
        // Exactly at the start of block 1
        let block = map.find_block(1000).unwrap();
        assert_eq!(block.file_offset, 100);
        assert_eq!(block.decompressed_offset, 1000);
    }

    #[test]
    fn test_find_block_out_of_bounds() {
        let map = create_test_block_map();
        // Past the end of all blocks
        let block = map.find_block(10000);
        assert!(block.is_none());
    }

    #[test]
    fn test_total_decompressed_size() {
        let map = create_test_block_map();
        assert_eq!(map.total_decompressed_size(), 4500); // 1000 + 2000 + 1500
    }

    #[test]
    fn test_num_blocks() {
        let map = create_test_block_map();
        assert_eq!(map.num_blocks(), 3);
    }
}
