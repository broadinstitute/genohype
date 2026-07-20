///! Test EType parser with real metadata from the gene models table
use genohype_core::codec::ETypeParser;

#[test]
fn test_parse_row_etype_from_metadata() {
    // This is the actual _eType from data/gene_models_hds/ht/prep_table.ht/rows/metadata.json.gz
    let etype_str = "+EBaseStruct{interval:EBaseStruct{start:EBaseStruct{contig:+EBinary,position:+EInt32},end:EBaseStruct{contig:+EBinary,position:+EInt32},includesStart:+EBoolean,includesEnd:+EBoolean},gene_id:EBinary,gene_version:EBinary,gencode_symbol:EBinary,chrom:EBinary,strand:EBinary,start:EInt32,stop:EInt32,xstart:EInt64,xstop:EInt64,exons:EArray[EBaseStruct{feature_type:EBinary,start:EInt32,stop:EInt32,xstart:EInt64,xstop:EInt64}]}";

    let result = ETypeParser::parse(etype_str);

    match result {
        Ok(etype) => {
            println!("✓ Successfully parsed EType from metadata");
            println!("  Type: {:?}", etype);

            // Verify it's a required struct
            assert!(etype.is_required(), "Row struct should be required");
        }
        Err(e) => {
            panic!("Failed to parse EType: {:?}", e);
        }
    }
}

#[test]
fn test_parse_exons_array() {
    // Just the exons field
    let etype_str = "EArray[EBaseStruct{feature_type:EBinary,start:EInt32,stop:EInt32,xstart:EInt64,xstop:EInt64}]";

    let result = ETypeParser::parse(etype_str);

    match result {
        Ok(etype) => {
            println!("✓ Successfully parsed exons array");

            // Should be nullable array
            assert!(!etype.is_required());
        }
        Err(e) => {
            panic!("Failed to parse exons array: {:?}", e);
        }
    }
}

#[test]
fn test_parse_interval_struct() {
    let etype_str = "EBaseStruct{start:EBaseStruct{contig:+EBinary,position:+EInt32},end:EBaseStruct{contig:+EBinary,position:+EInt32},includesStart:+EBoolean,includesEnd:+EBoolean}";

    let result = ETypeParser::parse(etype_str);

    match result {
        Ok(etype) => {
            println!("✓ Successfully parsed interval struct");
            assert!(!etype.is_required()); // Interval itself is nullable
        }
        Err(e) => {
            panic!("Failed to parse interval: {:?}", e);
        }
    }
}
