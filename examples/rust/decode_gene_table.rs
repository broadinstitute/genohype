///! Example: Decode and display gene table data
///!
///! This example demonstrates successful decoding of a real-world Hail gene table
///! with complex nested structures, nullable fields, and arrays.

use genohype_core::buffer::{BufferBuilder, InputBuffer};
use genohype_core::codec::{EncodedValue, ETypeParser};
use std::fs::File;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Hail Gene Table Decoder Demo ===\n");

    // Load metadata
    let metadata_path = "data/gene_models_hds/ht/prep_table.ht/rows/metadata.json.gz";
    let metadata_file = File::open(metadata_path)?;
    let decoder = flate2::read::GzDecoder::new(metadata_file);
    let metadata: serde_json::Value = serde_json::from_reader(decoder)?;

    let etype_str = metadata["_codecSpec"]["_eType"]
        .as_str()
        .ok_or("Failed to get EType string")?;

    println!("Parsing EType schema...");
    let row_type = ETypeParser::parse(etype_str)?;
    println!("Schema parsed successfully\n");

    // Open data file
    let parts_dir = "data/gene_models_hds/ht/prep_table.ht/rows/parts";
    let entries: Vec<_> = std::fs::read_dir(parts_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name_str = name.to_string_lossy();
            name_str.starts_with("part-0") && !name_str.ends_with(".crc")
        })
        .collect();

    let part_file = &entries[0];
    println!("Reading from: {}\n", part_file.path().display());

    let mut buffer = BufferBuilder::from_file(part_file.path())?
        .with_leb128()
        .build();

    // Decode row
    let row_present = buffer.read_bool()?;
    if !row_present {
        return Err("Row is null".into());
    }

    let result = row_type.read_present_value(&mut buffer)?;

    // Extract and display gene data
    if let EncodedValue::Struct(fields) = result {
        println!("Decoded Gene Information:\n");
        println!("================================================");

        // Gene ID
        if let Some(gene_id) = get_string_field(&fields, "gene_id") {
            println!("Gene ID:          {}", gene_id);
        }

        // Symbol
        if let Some(symbol) = get_string_field(&fields, "symbol") {
            println!("Symbol:           {}", symbol);
        }

        // Full name
        if let Some(name) = get_string_field(&fields, "name") {
            println!("Name:             {}", name);
        }

        // Location
        if let Some(chrom) = get_string_field(&fields, "chrom") {
            println!("Chromosome:       {}", chrom);
        }

        if let Some(start) = get_int_field(&fields, "start") {
            println!("Start position:   {}", start);
        }

        if let Some(stop) = get_int_field(&fields, "stop") {
            println!("Stop position:    {}", stop);
        }

        if let Some(strand) = get_string_field(&fields, "strand") {
            println!("Strand:           {}", strand);
        }

        println!("================================================\n");

        // Exons
        if let Some(EncodedValue::Array(exons)) = fields.iter()
            .find(|(name, _)| name == "exons")
            .map(|(_, val)| val)
        {
            println!("Exons: {} total", exons.len());
            println!("\nFirst 5 exons:");
            for (i, exon) in exons.iter().take(5).enumerate() {
                if let EncodedValue::Struct(exon_fields) = exon {
                    let feature_type = get_string_field(exon_fields, "feature_type")
                        .unwrap_or_else(|| "?".to_string());
                    let start = get_int_field(exon_fields, "start")
                        .unwrap_or(0);
                    let stop = get_int_field(exon_fields, "stop")
                        .unwrap_or(0);

                    println!("  {}. {} | {}..{}", i + 1, feature_type, start, stop);
                }
            }
            if exons.len() > 5 {
                println!("  ... and {} more exons", exons.len() - 5);
            }
            println!();
        }

        // Transcripts
        if let Some(EncodedValue::Array(transcripts)) = fields.iter()
            .find(|(name, _)| name == "transcripts")
            .map(|(_, val)| val)
        {
            println!("Transcripts: {} total\n", transcripts.len());
        }

        // Identifiers
        println!("External Identifiers:");
        if let Some(hgnc_id) = get_string_field(&fields, "hgnc_id") {
            println!("  HGNC ID:  {}", hgnc_id);
        }
        if let Some(omim_id) = get_string_field(&fields, "omim_id") {
            println!("  OMIM ID:  {}", omim_id);
        }
        if let Some(ncbi_id) = get_string_field(&fields, "ncbi_id") {
            println!("  NCBI ID:  {}", ncbi_id);
        }

        println!("\nSuccessfully decoded complete gene table row!");
        println!("Total fields decoded: {}", fields.len());
    }

    Ok(())
}

fn get_string_field(fields: &[(String, EncodedValue)], name: &str) -> Option<String> {
    fields.iter()
        .find(|(n, _)| n == name)
        .and_then(|(_, val)| match val {
            EncodedValue::Binary(bytes) => Some(String::from_utf8_lossy(bytes).to_string()),
            _ => None,
        })
}

fn get_int_field(fields: &[(String, EncodedValue)], name: &str) -> Option<i32> {
    fields.iter()
        .find(|(n, _)| n == name)
        .and_then(|(_, val)| match val {
            EncodedValue::Int32(v) => Some(*v),
            _ => None,
        })
}
