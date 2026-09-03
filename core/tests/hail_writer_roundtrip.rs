use genohype_core::codec::{EncodedField, EncodedType, EncodedValue};
use genohype_core::export::hail::HailTableWriter;
use genohype_core::query::QueryEngine;
use tempfile::tempdir;

const ROW_COUNT: usize = 1_500;

fn field(name: &str, encoded_type: EncodedType, index: usize) -> EncodedField {
    EncodedField {
        name: name.to_string(),
        encoded_type,
        index,
    }
}

fn association_row_type() -> EncodedType {
    let locus = EncodedType::EBaseStruct {
        required: true,
        fields: vec![
            field("contig", EncodedType::EBinary { required: true }, 0),
            field("position", EncodedType::EInt32 { required: true }, 1),
        ],
    };

    EncodedType::EBaseStruct {
        required: true,
        fields: vec![
            field("locus", locus, 0),
            field(
                "alleles",
                EncodedType::EArray {
                    required: true,
                    element: Box::new(EncodedType::EBinary { required: true }),
                },
                1,
            ),
            field("Pvalue", EncodedType::EFloat64 { required: true }, 2),
            field("Pvalue_log10", EncodedType::EFloat64 { required: false }, 3),
            field("BETA", EncodedType::EFloat64 { required: false }, 4),
            field("SE", EncodedType::EFloat64 { required: false }, 5),
            field("AF_Allele2", EncodedType::EFloat64 { required: false }, 6),
            field("AC_Allele2", EncodedType::EFloat64 { required: false }, 7),
            // Keep rows large enough that this test necessarily crosses several
            // 64 KiB codec blocks and catches accidental whole-partition frames.
            field("gene_symbol", EncodedType::EBinary { required: true }, 8),
        ],
    }
}

fn association_row(ordinal: usize) -> EncodedValue {
    let contig = if ordinal % 2 == 0 { "chr1" } else { "chr4" };
    let position = 68_000_000 + ordinal as i32;
    let pvalue = if ordinal == 0 {
        0.0
    } else {
        (ordinal as f64 + 1.0) / 10_000.0
    };

    EncodedValue::Struct(vec![
        (
            "locus".into(),
            EncodedValue::Struct(vec![
                (
                    "contig".into(),
                    EncodedValue::Binary(contig.as_bytes().to_vec()),
                ),
                ("position".into(), EncodedValue::Int32(position)),
            ]),
        ),
        (
            "alleles".into(),
            EncodedValue::Array(vec![
                EncodedValue::Binary(b"A".to_vec()),
                EncodedValue::Binary(b"C".to_vec()),
            ]),
        ),
        ("Pvalue".into(), EncodedValue::Float64(pvalue)),
        (
            "Pvalue_log10".into(),
            if ordinal == 0 {
                EncodedValue::Float64(350.0)
            } else {
                EncodedValue::Null
            },
        ),
        (
            "BETA".into(),
            if ordinal % 11 == 0 {
                EncodedValue::Null
            } else {
                EncodedValue::Float64(ordinal as f64 / 100.0)
            },
        ),
        ("SE".into(), EncodedValue::Float64(0.01)),
        ("AF_Allele2".into(), EncodedValue::Float64(0.125)),
        (
            "AC_Allele2".into(),
            EncodedValue::Float64((ordinal * 2) as f64),
        ),
        (
            "gene_symbol".into(),
            EncodedValue::Binary(format!("GENE_{ordinal:04}_{}", "X".repeat(96)).into_bytes()),
        ),
    ])
}

fn get_field<'a>(row: &'a EncodedValue, name: &str) -> &'a EncodedValue {
    let EncodedValue::Struct(fields) = row else {
        panic!("row is not a struct");
    };
    fields
        .iter()
        .find_map(|(field_name, value)| (field_name == name).then_some(value))
        .unwrap_or_else(|| panic!("missing field {name}"))
}

#[test]
fn association_rows_round_trip_across_multiple_zstd_blocks() {
    let temp = tempdir().expect("create temp directory");
    let table = temp.path().join("association.ht");
    let row_type = association_row_type();
    let key_fields = vec!["locus".to_string(), "alleles".to_string()];

    let mut writer = HailTableWriter::new(
        table.to_str().expect("UTF-8 temp path"),
        &row_type,
        &key_fields,
        None,
    )
    .expect("create Hail writer");
    writer
        .write_partition(0, (0..ROW_COUNT).map(association_row))
        .expect("write association rows");
    writer.finish().expect("finish Hail table");

    let engine = QueryEngine::open_path(table.to_str().expect("UTF-8 temp path"))
        .expect("reopen exported table");
    assert_eq!(engine.key_fields(), key_fields);

    let rows = engine
        .query_iter(&[])
        .expect("start full scan")
        .collect::<genohype_core::Result<Vec<_>>>()
        .expect("scan every exported row");
    assert_eq!(rows.len(), ROW_COUNT);

    for ordinal in [0, 777, ROW_COUNT - 1] {
        let row = &rows[ordinal];
        let EncodedValue::Struct(locus) = get_field(row, "locus") else {
            panic!("locus is not a struct");
        };
        let position = locus
            .iter()
            .find_map(|(name, value)| (name == "position").then_some(value));
        assert_eq!(
            position,
            Some(&EncodedValue::Int32(68_000_000 + ordinal as i32))
        );
        assert_eq!(
            get_field(row, "AC_Allele2"),
            &EncodedValue::Float64((ordinal * 2) as f64)
        );
    }

    assert_eq!(get_field(&rows[0], "Pvalue"), &EncodedValue::Float64(0.0));
    assert_eq!(
        get_field(&rows[0], "Pvalue_log10"),
        &EncodedValue::Float64(350.0)
    );
    assert_eq!(get_field(&rows[0], "BETA"), &EncodedValue::Null);
    assert_eq!(get_field(&rows[777], "Pvalue_log10"), &EncodedValue::Null);
}
