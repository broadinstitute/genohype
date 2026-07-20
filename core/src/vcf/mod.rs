//! VCF support for Hail Decoder
//!
//! This module provides support for reading and writing VCF (Variant Call Format) files
//! and converting them to/from the same `EncodedType`/`EncodedValue` format used
//! by Hail Tables.
//!
//! ## Modules
//!
//! - `schema`: Converts VCF headers to `EncodedType` schemas
//! - `codec`: Converts VCF records to `EncodedValue` rows
//! - `reader`: `DataSource` implementation for VCF files
//! - `writer`: Converts `EncodedValue` rows back to VCF records

pub mod codec;
pub mod index;
pub mod reader;
pub mod schema;
pub mod writer;

pub use reader::VcfDataSource;
pub use writer::{BgzfVcfWriter, VcfFileWriter, VcfWriter};
