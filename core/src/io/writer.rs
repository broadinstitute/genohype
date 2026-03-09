//! Cloud storage writer for streaming output to object stores
//!
//! This module provides cloud storage writers implementing `std::io::Write`:
//!
//! - `CloudWriter` - Buffers entire file in memory, uploads on finish (simple, for <1GB files)
//! - `StreamingCloudWriter` - Streams parts in background as data is written (for large files)
//!
//! ## Architecture
//!
//! ### CloudWriter (Simple)
//! ```text
//! write() → [Memory Buffer] → finish() → [Single Upload]
//! ```
//! Good for files under ~1GB. Simple and reliable.
//!
//! ### StreamingCloudWriter (Background Pipeline)
//! ```text
//! write() → [8MB Buffer] → [Channel] → [Background Task] → [Multipart Upload]
//!              sync          async        parallel upload
//! ```
//! Good for large files. Uploads happen in parallel with continued writing.

use crate::io::adapter::IO_RUNTIME;
use crate::{HailError, Result};
use bytes::Bytes;
use crossbeam_channel::{bounded, Sender};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, WriteMultipart};
use std::io::Write;
use std::sync::Arc;
use std::thread::JoinHandle;

/// Minimum part size for multipart uploads (5MB for S3/GCS)
const MIN_PART_SIZE: usize = 5 * 1024 * 1024;

/// Default part size for streaming uploads (8MB)
const DEFAULT_PART_SIZE: usize = 8 * 1024 * 1024;

/// Number of parts to buffer in the channel before backpressure kicks in
const UPLOAD_CHANNEL_CAPACITY: usize = 4;

/// A writer that buffers data and uploads to cloud storage on finish
///
/// This writer implements `std::io::Write`, allowing it to be used with
/// `ParquetWriter` and other components expecting synchronous writers.
///
/// # Usage
///
/// ```no_run
/// use genohype_core::io::CloudWriter;
///
/// let writer = CloudWriter::new("gs://bucket/path/output.parquet")?;
/// // Write data...
/// writer.finish()?; // Uploads to GCS
/// # Ok::<(), hail_decoder::HailError>(())
/// ```
pub struct CloudWriter {
    /// The object store to write to
    store: Arc<dyn ObjectStore>,
    /// The path within the store
    path: ObjPath,
    /// Buffer accumulating written data
    buffer: Vec<u8>,
}

impl CloudWriter {
    /// Create a new CloudWriter for the given cloud URL
    ///
    /// # Arguments
    /// * `url_str` - Cloud storage URL (gs://, s3://)
    ///
    /// # Example
    /// ```no_run
    /// let writer = CloudWriter::new("gs://my-bucket/output.parquet")?;
    /// # Ok::<(), hail_decoder::HailError>(())
    /// ```
    pub fn new(url_str: &str) -> Result<Self> {
        let (store, path) = crate::io::resolve_url_for_write(url_str)?;

        Ok(CloudWriter {
            store,
            path,
            buffer: Vec::with_capacity(64 * 1024 * 1024), // Pre-allocate 64MB
        })
    }

    /// Create a CloudWriter from an existing ObjectStore and path
    ///
    /// This is useful when you already have a configured ObjectStore instance.
    pub fn from_store(store: Arc<dyn ObjectStore>, path: ObjPath) -> Self {
        CloudWriter {
            store,
            path,
            buffer: Vec::with_capacity(64 * 1024 * 1024),
        }
    }

    /// Finish writing and upload the data to cloud storage
    ///
    /// This must be called after all data has been written. It blocks until
    /// the upload is complete.
    ///
    /// # Returns
    /// The number of bytes uploaded.
    pub fn finish(self) -> Result<usize> {
        let size = self.buffer.len();
        let store = self.store;
        let path = self.path;
        let data = self.buffer;

        tracing::trace!("CloudWriter: uploading {} bytes to {:?}", size, path);
        let start_time = std::time::Instant::now();

        IO_RUNTIME.block_on(async move {
            store.put(&path, data.into()).await
        }).map_err(|e| HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to upload to cloud storage: {}", e),
        )))?;

        tracing::trace!("CloudWriter: upload completed in {:?}", start_time.elapsed());
        Ok(size)
    }

    /// Get the current buffer size (bytes written so far)
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if no data has been written
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

impl Write for CloudWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // No-op: we flush everything on finish()
        Ok(())
    }
}

/// A streaming cloud writer that uploads parts in the background
///
/// This writer uses multipart upload to stream data to cloud storage as it's
/// written, rather than buffering the entire file in memory. A background task
/// handles the actual uploads, allowing writes to continue in parallel.
///
/// ## Architecture
///
/// ```text
/// ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
/// │   write(data)   │      │  Bounded Channel │      │ Background Task │
/// │       │         │      │   (4 parts max)  │      │                 │
/// │       ▼         │      │                  │      │  for part in rx │
/// │  [8MB buffer]   │─────►│  [part queue]    │─────►│    upload(part) │
/// │       │         │      │                  │      │  end            │
/// │  send(buffer)   │      │                  │      │  complete()     │
/// └─────────────────┘      └─────────────────┘      └─────────────────┘
/// ```
///
/// ## Usage
///
/// ```no_run
/// use genohype_core::io::StreamingCloudWriter;
///
/// let mut writer = StreamingCloudWriter::new("gs://bucket/large-file.parquet")?;
/// // Write data in chunks - uploads happen in background
/// writer.write_all(&data)?;
/// // Finish uploads and complete the multipart upload
/// writer.finish()?;
/// # Ok::<(), hail_decoder::HailError>(())
/// ```
pub struct StreamingCloudWriter {
    /// Channel to send completed parts to the upload task
    part_tx: Option<Sender<Bytes>>,
    /// Current buffer accumulating writes until it reaches part size
    buffer: Vec<u8>,
    /// Size threshold for sending a part
    part_size: usize,
    /// Handle to the background upload task
    upload_handle: Option<JoinHandle<std::result::Result<(), String>>>,
    /// Total bytes written (for reporting)
    total_bytes: usize,
}

impl StreamingCloudWriter {
    /// Create a new streaming cloud writer for the given URL
    ///
    /// Immediately starts a background task that will handle multipart uploads.
    pub fn new(url_str: &str) -> Result<Self> {
        Self::with_part_size(url_str, DEFAULT_PART_SIZE)
    }

    /// Create a streaming writer with a custom part size
    ///
    /// Part size must be at least 5MB (cloud provider minimum).
    pub fn with_part_size(url_str: &str, part_size: usize) -> Result<Self> {
        let part_size = part_size.max(MIN_PART_SIZE);
        let (store, path) = crate::io::resolve_url_for_write(url_str)?;
        Self::from_store(store, path, part_size)
    }

    /// Create a streaming writer from an existing ObjectStore
    pub fn from_store(store: Arc<dyn ObjectStore>, path: ObjPath, part_size: usize) -> Result<Self> {
        let (part_tx, part_rx) = bounded::<Bytes>(UPLOAD_CHANNEL_CAPACITY);

        // Start the multipart upload and spawn the background task
        let path_clone = path.clone();
        let upload_handle = std::thread::spawn(move || {
            IO_RUNTIME.block_on(async move {
                // Initialize multipart upload
                let upload = store.put_multipart(&path_clone).await
                    .map_err(|e| format!("Failed to initiate multipart upload: {}", e))?;

                let mut writer = WriteMultipart::new(upload);

                // Receive and upload parts
                while let Ok(part_data) = part_rx.recv() {
                    writer.write(&part_data);
                }

                // Complete the multipart upload
                writer.finish().await
                    .map_err(|e| format!("Failed to complete multipart upload: {}", e))?;

                Ok(())
            })
        });

        Ok(StreamingCloudWriter {
            part_tx: Some(part_tx),
            buffer: Vec::with_capacity(part_size),
            part_size,
            upload_handle: Some(upload_handle),
            total_bytes: 0,
        })
    }

    /// Send the current buffer as a part if it's large enough
    fn maybe_send_part(&mut self) -> std::io::Result<()> {
        if self.buffer.len() >= self.part_size {
            self.send_current_buffer()?;
        }
        Ok(())
    }

    /// Send the current buffer as a part (regardless of size)
    fn send_current_buffer(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let part_data = std::mem::replace(&mut self.buffer, Vec::with_capacity(self.part_size));
        let part_size = part_data.len();

        if let Some(ref tx) = self.part_tx {
            tx.send(Bytes::from(part_data)).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    format!("Upload task disconnected: {}", e),
                )
            })?;
            tracing::trace!("StreamingCloudWriter: sent part of {} bytes", part_size);
        }

        Ok(())
    }

    /// Finish writing and complete the multipart upload
    ///
    /// This sends any remaining buffered data and waits for the background
    /// upload task to complete.
    ///
    /// # Returns
    /// The total number of bytes uploaded.
    pub fn finish(mut self) -> Result<usize> {
        // Send any remaining data
        self.send_current_buffer().map_err(HailError::Io)?;

        // Drop the sender to signal completion to the upload task
        self.part_tx = None;

        // Wait for the upload task to finish
        if let Some(handle) = self.upload_handle.take() {
            match handle.join() {
                Ok(Ok(())) => {
                    tracing::trace!("StreamingCloudWriter: upload complete, {} bytes total", self.total_bytes);
                }
                Ok(Err(e)) => {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Upload failed: {}", e),
                    )));
                }
                Err(_) => {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Upload task panicked",
                    )));
                }
            }
        }

        Ok(self.total_bytes)
    }

    /// Get the total bytes written so far
    pub fn bytes_written(&self) -> usize {
        self.total_bytes
    }
}

impl Write for StreamingCloudWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        self.total_bytes += buf.len();
        self.maybe_send_part()?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // Don't send partial parts on flush - only on finish or when buffer is full
        Ok(())
    }
}

impl Drop for StreamingCloudWriter {
    fn drop(&mut self) {
        // If we're being dropped without finish() being called, try to clean up gracefully
        if self.part_tx.is_some() {
            // Drop the sender to signal the upload task to stop
            self.part_tx = None;
            // Wait briefly for the task to finish
            if let Some(handle) = self.upload_handle.take() {
                let _ = handle.join();
            }
        }
    }
}

/// Enum wrapping either a local file or cloud writer for unified handling
pub enum OutputWriter {
    /// Local file system writer
    File(std::fs::File),
    /// Cloud storage writer
    Cloud(CloudWriter),
}

impl OutputWriter {
    /// Create an OutputWriter for the given path
    ///
    /// Automatically detects whether the path is local or cloud and creates
    /// the appropriate writer.
    pub fn new(path: &str) -> Result<Self> {
        if crate::io::is_cloud_path(path) {
            Ok(OutputWriter::Cloud(CloudWriter::new(path)?))
        } else {
            let file = std::fs::File::create(path)?;
            Ok(OutputWriter::File(file))
        }
    }

    /// Finish writing
    ///
    /// For cloud writers, this uploads the buffered data.
    /// For local files, this syncs and closes the file.
    pub fn finish(self) -> Result<usize> {
        match self {
            OutputWriter::File(file) => {
                file.sync_all()?;
                Ok(0) // File doesn't track bytes written in this simple implementation
            }
            OutputWriter::Cloud(writer) => writer.finish(),
        }
    }
}

impl Write for OutputWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            OutputWriter::File(file) => file.write(buf),
            OutputWriter::Cloud(writer) => writer.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            OutputWriter::File(file) => file.flush(),
            OutputWriter::Cloud(writer) => writer.flush(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_output_writer_local() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut writer = OutputWriter::new(path).unwrap();
        writer.write_all(b"Hello, world!").unwrap();
        writer.finish().unwrap();

        let content = std::fs::read_to_string(path).unwrap();
        assert_eq!(content, "Hello, world!");
    }
}
