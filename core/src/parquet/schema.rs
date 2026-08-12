//! Arrow schema construction from a retained recursive schema plan.

use crate::error::Result;
use crate::schema::{ArrowSchemaPlan, PairedSchema};
use arrow::datatypes::Schema;

/// Create an Arrow schema from an already validated physical/logical pair.
pub fn create_schema(paired: &PairedSchema) -> Result<Schema> {
    Ok(paired.arrow_plan()?.schema().as_ref().clone())
}

/// Return the complete retained plan used by builders and writers.
pub fn create_plan(paired: &PairedSchema) -> Result<ArrowSchemaPlan> {
    paired.arrow_plan()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::ETypeParser;
    use arrow::datatypes::DataType;

    #[test]
    fn binary_meaning_comes_from_vtype() {
        let paired = PairedSchema::from_raw_vtype(
            ETypeParser::parse("+EBaseStruct{text:+EBinary,bytes:EBinary}").unwrap(),
            "Struct{text:String,bytes:Binary}",
        )
        .unwrap();
        let schema = create_schema(&paired).unwrap();
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Binary);
        assert!(!schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
    }
}
