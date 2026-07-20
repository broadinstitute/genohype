///! Focused test for nested struct decoding
///!
///! This test isolates the `interval` field decoding to verify that nested structs
///! correctly read their own bitmaps.
use genohype_core::buffer::InputBuffer;
use genohype_core::codec::{EncodedField, EncodedType, EncodedValue};
use genohype_core::Result;
use std::io::{Cursor, Read};

/// Simple buffer wrapper for testing that implements InputBuffer
struct TestBuffer {
    cursor: Cursor<Vec<u8>>,
}

impl TestBuffer {
    fn new(data: Vec<u8>) -> Self {
        TestBuffer {
            cursor: Cursor::new(data),
        }
    }

    fn position(&self) -> u64 {
        self.cursor.position()
    }

    fn get_ref(&self) -> &Vec<u8> {
        self.cursor.get_ref()
    }
}

impl InputBuffer for TestBuffer {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.cursor
            .read_exact(buf)
            .map_err(|e| genohype_core::HailError::Io(e))
    }
}

#[test]
fn test_decode_interval_struct() {
    // The interval struct has this structure:
    // EBaseStruct{
    //   start: EBaseStruct{contig:+EBinary, position:+EInt32},  // nullable
    //   end: EBaseStruct{contig:+EBinary, position:+EInt32},    // nullable
    //   includesStart: +EBoolean,                                // required
    //   includesEnd: +EBoolean                                   // required
    // }

    // The interval has 2 nullable fields (start, end), so its bitmap is 1 byte
    // Both sub-structs (start, end) have 0 nullable fields (all fields required), so no bitmap

    // Build the interval type programmatically
    let start_struct = EncodedType::EBaseStruct {
        required: false,
        fields: vec![
            EncodedField {
                name: "contig".to_string(),
                encoded_type: EncodedType::EBinary { required: true },
                index: 0,
            },
            EncodedField {
                name: "position".to_string(),
                encoded_type: EncodedType::EInt32 { required: true },
                index: 1,
            },
        ],
    };

    let end_struct = EncodedType::EBaseStruct {
        required: false,
        fields: vec![
            EncodedField {
                name: "contig".to_string(),
                encoded_type: EncodedType::EBinary { required: true },
                index: 0,
            },
            EncodedField {
                name: "position".to_string(),
                encoded_type: EncodedType::EInt32 { required: true },
                index: 1,
            },
        ],
    };

    let interval_struct = EncodedType::EBaseStruct {
        required: true, // Note: in the actual data, interval might be nullable
        fields: vec![
            EncodedField {
                name: "start".to_string(),
                encoded_type: start_struct,
                index: 0,
            },
            EncodedField {
                name: "end".to_string(),
                encoded_type: end_struct,
                index: 1,
            },
            EncodedField {
                name: "includesStart".to_string(),
                encoded_type: EncodedType::EBoolean { required: true },
                index: 2,
            },
            EncodedField {
                name: "includesEnd".to_string(),
                encoded_type: EncodedType::EBoolean { required: true },
                index: 3,
            },
        ],
    };

    // Create test data for an interval
    // Expected data format:
    // [1 byte bitmap for interval: 0x00 = both start and end present]
    // [start struct data - NO bitmap since all fields required]
    //   - contig length (i32) = 5
    //   - contig bytes = "chr10"
    //   - position (i32) = 972470716
    // [end struct data - NO bitmap since all fields required]
    //   - contig length (i32) = 5
    //   - contig bytes = "chr10"
    //   - position (i32) = 972940282
    // [includesStart boolean = true]
    // [includesEnd boolean = true]

    let mut test_data = Vec::new();

    // Interval bitmap: 0x00 (both start and end present)
    test_data.push(0x00);

    // Start struct (no bitmap - all required fields)
    // contig length as i32 (5) in little-endian
    test_data.extend_from_slice(&5i32.to_le_bytes());
    // contig bytes "chr10"
    test_data.extend_from_slice(b"chr10");
    // position as i32 (972470716) in little-endian
    test_data.extend_from_slice(&972470716i32.to_le_bytes());

    // End struct (no bitmap - all required fields)
    // contig length as i32 (5) in little-endian
    test_data.extend_from_slice(&5i32.to_le_bytes());
    // contig bytes "chr10"
    test_data.extend_from_slice(b"chr10");
    // position as i32 (972940282) in little-endian
    test_data.extend_from_slice(&972940282i32.to_le_bytes());

    // includesStart boolean (true = 1)
    test_data.push(0x01);
    // includesEnd boolean (true = 1)
    test_data.push(0x01);

    println!(
        "\nTest data ({} bytes): {:02x?}",
        test_data.len(),
        test_data
    );

    // Create a buffer with the test data
    let mut buffer = TestBuffer::new(test_data);

    // Decode using read_present_value (simulating that the parent already checked presence)
    let result = interval_struct
        .read_present_value(&mut buffer)
        .expect("Failed to decode interval struct");

    // Validate the result
    if let EncodedValue::Struct(fields) = result {
        println!(
            "\n✓ Successfully decoded interval with {} fields",
            fields.len()
        );
        assert_eq!(fields.len(), 4);

        // Check start struct
        let start = fields
            .iter()
            .find(|(name, _)| name == "start")
            .expect("Missing start field");
        if let (_, EncodedValue::Struct(start_fields)) = start {
            let contig = start_fields
                .iter()
                .find(|(name, _)| name == "contig")
                .and_then(|(_, val)| val.as_string())
                .expect("Missing contig in start");
            let position = start_fields
                .iter()
                .find(|(name, _)| name == "position")
                .and_then(|(_, val)| val.as_i32())
                .expect("Missing position in start");

            println!(
                "  start: {{ contig: \"{}\", position: {} }}",
                contig, position
            );
            assert_eq!(contig, "chr10");
            assert_eq!(position, 972470716);
        } else {
            panic!("start is not a struct");
        }

        // Check end struct
        let end = fields
            .iter()
            .find(|(name, _)| name == "end")
            .expect("Missing end field");
        if let (_, EncodedValue::Struct(end_fields)) = end {
            let contig = end_fields
                .iter()
                .find(|(name, _)| name == "contig")
                .and_then(|(_, val)| val.as_string())
                .expect("Missing contig in end");
            let position = end_fields
                .iter()
                .find(|(name, _)| name == "position")
                .and_then(|(_, val)| val.as_i32())
                .expect("Missing position in end");

            println!(
                "  end: {{ contig: \"{}\", position: {} }}",
                contig, position
            );
            assert_eq!(contig, "chr10");
            assert_eq!(position, 972940282);
        } else {
            panic!("end is not a struct");
        }

        // Check booleans
        let includes_start = fields
            .iter()
            .find(|(name, _)| name == "includesStart")
            .map(|(_, val)| val);
        let includes_end = fields
            .iter()
            .find(|(name, _)| name == "includesEnd")
            .map(|(_, val)| val);

        if let Some(EncodedValue::Boolean(true)) = includes_start {
            println!("  includesStart: true");
        } else {
            panic!("includesStart is not true");
        }

        if let Some(EncodedValue::Boolean(true)) = includes_end {
            println!("  includesEnd: true");
        } else {
            panic!("includesEnd is not true");
        }

        println!("\n✓ All validations passed!");
    } else {
        panic!("Result is not a struct");
    }

    // Verify we consumed all bytes
    let remaining = buffer.get_ref().len() - buffer.position() as usize;
    assert_eq!(
        remaining, 0,
        "Should have consumed all bytes, {} remaining",
        remaining
    );
}
