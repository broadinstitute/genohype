//! Tabix index generation for BGZF-compressed VCF files.
//!
//! Builds `.tbi` indexes that enable efficient region queries on VCFs.
//! Works with both local files and cloud paths (GCS, S3) via `get_reader()`.

use crate::{HailError, Result};
use noodles::bgzf;
use noodles::csi::binning_index::index::reference_sequence::bin::Chunk;
use noodles::tabix;
use noodles::vcf::{self, variant::Record as _};
use std::io::Write;
use tracing::debug;

/// Build a tabix index for a BGZF-compressed VCF.
///
/// Works with local files and cloud paths (GCS, S3) via `get_reader()`.
/// The VCF must be BGZF-compressed (.vcf.gz or .vcf.bgz).
///
/// Returns the index in memory — caller decides where to write it.
///
/// The optional `on_record` callback is invoked with the record count after each record,
/// allowing callers to report progress.
pub fn build_tabix_index(vcf_path: &str, on_record: Option<&dyn Fn(u64)>) -> Result<tabix::Index> {
    let reader = crate::io::get_reader(vcf_path)?;
    let bgzf_reader = bgzf::Reader::new(reader);
    let mut vcf_reader = vcf::io::Reader::new(bgzf_reader);

    let header = vcf_reader.read_header().map_err(HailError::Io)?;

    let mut indexer = tabix::index::Indexer::default();
    indexer.set_header(noodles::csi::binning_index::index::header::Builder::vcf().build());

    let mut record = vcf::Record::default();
    let mut start_position = vcf_reader.get_ref().virtual_position();
    let mut count: u64 = 0;

    while vcf_reader.read_record(&mut record).map_err(HailError::Io)? != 0 {
        let end_position = vcf_reader.get_ref().virtual_position();
        let chunk = Chunk::new(start_position, end_position);

        let ref_name = record.reference_sequence_name();
        let start = record
            .variant_start()
            .transpose()
            .map_err(HailError::Io)?
            .ok_or_else(|| HailError::InvalidFormat("missing variant position".into()))?;
        let end = record.variant_end(&header).unwrap_or(start);

        indexer
            .add_record(ref_name, start, end, chunk)
            .map_err(HailError::Io)?;

        start_position = end_position;
        count += 1;

        if let Some(cb) = on_record {
            cb(count);
        }
    }

    debug!("Indexed {} records from {}", count, vcf_path);
    Ok(indexer.build())
}

/// Write a tabix index to a local or cloud path.
pub fn write_tabix_index(index: &tabix::Index, path: &str) -> Result<()> {
    let mut buf = Vec::new();
    {
        let mut writer = tabix::io::Writer::new(&mut buf);
        writer.write_index(index).map_err(HailError::Io)?;
    }
    let mut output = crate::io::OutputWriter::new(path)?;
    output.write_all(&buf)?;
    output.finish()?;
    Ok(())
}
