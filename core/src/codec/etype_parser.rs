//! Parser for Hail's EType string format
//!
//! Format examples:
//! - `+EBinary` - required binary
//! - `EBinary` - nullable binary
//! - `+EInt32` - required int32
//! - `EArray[+EBinary]` - array of required binary
//! - `+EBaseStruct{field1:+EBinary,field2:EInt32}` - struct with fields

use crate::codec::encoded_type::{EncodedField, EncodedType};
use crate::error::{HailError, Result};

pub struct ETypeParser {
    input: String,
    pos: usize,
}

impl ETypeParser {
    pub fn new(input: String) -> Self {
        Self { input, pos: 0 }
    }

    pub fn parse(input: &str) -> Result<EncodedType> {
        let mut parser = Self::new(input.to_string());
        parser.parse_type()
    }

    fn current_char(&self) -> Option<char> {
        self.input.chars().nth(self.pos)
    }

    fn _peek_char(&self, offset: usize) -> Option<char> {
        self.input.chars().nth(self.pos + offset)
    }

    fn advance(&mut self) {
        self.pos += 1;
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.current_char() {
            if ch.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn expect_char(&mut self, expected: char) -> Result<()> {
        self.skip_whitespace();
        if let Some(ch) = self.current_char() {
            if ch == expected {
                self.advance();
                Ok(())
            } else {
                Err(HailError::ParseError(format!(
                    "Expected '{}' but found '{}' at position {}",
                    expected, ch, self.pos
                )))
            }
        } else {
            Err(HailError::ParseError(format!(
                "Expected '{}' but reached end of input",
                expected
            )))
        }
    }

    fn read_identifier(&mut self) -> Result<String> {
        self.skip_whitespace();

        // Handle backtick-quoted identifiers (e.g., `p.value.NA`, `Is.SPA`)
        // Hail uses backticks to quote field names with special characters
        if self.current_char() == Some('`') {
            self.advance(); // skip opening backtick
            let start = self.pos;

            while let Some(ch) = self.current_char() {
                if ch == '`' {
                    let name = self.input[start..self.pos].to_string();
                    self.advance(); // skip closing backtick
                    return Ok(name);
                }
                self.advance();
            }

            return Err(HailError::ParseError(
                "Unclosed backtick-quoted identifier".to_string(),
            ));
        }

        // Standard identifier: alphanumeric and underscores
        let start = self.pos;

        while let Some(ch) = self.current_char() {
            if ch.is_alphanumeric() || ch == '_' {
                self.advance();
            } else {
                break;
            }
        }

        if self.pos == start {
            return Err(HailError::ParseError(format!(
                "Expected identifier at position {}",
                self.pos
            )));
        }

        Ok(self.input[start..self.pos].to_string())
    }

    fn parse_type(&mut self) -> Result<EncodedType> {
        self.skip_whitespace();

        // Check for required marker '+'
        let required = if self.current_char() == Some('+') {
            self.advance();
            true
        } else {
            false
        };

        self.skip_whitespace();
        let type_name = self.read_identifier()?;

        match type_name.as_str() {
            "EBinary" => Ok(EncodedType::EBinary { required }),
            "EInt32" => Ok(EncodedType::EInt32 { required }),
            "EInt64" => Ok(EncodedType::EInt64 { required }),
            "EFloat32" => Ok(EncodedType::EFloat32 { required }),
            "EFloat64" => Ok(EncodedType::EFloat64 { required }),
            "EBoolean" => Ok(EncodedType::EBoolean { required }),
            "EArray" => {
                // Parse: EArray[elementType]
                self.expect_char('[')?;
                let element = Box::new(self.parse_type()?);
                self.expect_char(']')?;
                Ok(EncodedType::EArray { required, element })
            }
            "EBaseStruct" => {
                // Parse: EBaseStruct{field1:type1,field2:type2,...}
                self.expect_char('{')?;
                let fields = self.parse_struct_fields()?;
                self.expect_char('}')?;
                Ok(EncodedType::EBaseStruct { required, fields })
            }
            _ => Err(HailError::ParseError(format!(
                "Unknown type: {}",
                type_name
            ))),
        }
    }

    fn parse_struct_fields(&mut self) -> Result<Vec<EncodedField>> {
        let mut fields = Vec::new();
        let mut index = 0;

        self.skip_whitespace();

        // Empty struct
        if self.current_char() == Some('}') {
            return Ok(fields);
        }

        loop {
            self.skip_whitespace();

            // Parse field name
            let field_name = self.read_identifier()?;

            // Expect colon
            self.expect_char(':')?;

            // Parse field type
            let field_type = self.parse_type()?;

            fields.push(EncodedField {
                name: field_name,
                encoded_type: field_type,
                index,
            });

            index += 1;

            self.skip_whitespace();

            // Check for comma (more fields) or closing brace (done)
            match self.current_char() {
                Some(',') => {
                    self.advance();
                    continue;
                }
                Some('}') => {
                    break;
                }
                Some(ch) => {
                    return Err(HailError::ParseError(format!(
                        "Expected ',' or '}}' but found '{}' at position {}",
                        ch, self.pos
                    )));
                }
                None => {
                    return Err(HailError::ParseError(
                        "Unexpected end of input while parsing struct fields".to_string(),
                    ));
                }
            }
        }

        Ok(fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_types() {
        assert!(matches!(
            ETypeParser::parse("+EBinary").unwrap(),
            EncodedType::EBinary { required: true }
        ));

        assert!(matches!(
            ETypeParser::parse("EBinary").unwrap(),
            EncodedType::EBinary { required: false }
        ));

        assert!(matches!(
            ETypeParser::parse("+EInt32").unwrap(),
            EncodedType::EInt32 { required: true }
        ));

        assert!(matches!(
            ETypeParser::parse("EBoolean").unwrap(),
            EncodedType::EBoolean { required: false }
        ));
    }

    #[test]
    fn test_parse_array() {
        let result = ETypeParser::parse("EArray[+EBinary]").unwrap();

        if let EncodedType::EArray { required, element } = result {
            assert!(!required);
            assert!(matches!(*element, EncodedType::EBinary { required: true }));
        } else {
            panic!("Expected EArray");
        }
    }

    #[test]
    fn test_parse_simple_struct() {
        let result = ETypeParser::parse("+EBaseStruct{field1:+EBinary,field2:EInt32}").unwrap();

        if let EncodedType::EBaseStruct { required, fields } = result {
            assert!(required);
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name, "field1");
            assert_eq!(fields[1].name, "field2");
            assert!(matches!(
                fields[0].encoded_type,
                EncodedType::EBinary { required: true }
            ));
            assert!(matches!(
                fields[1].encoded_type,
                EncodedType::EInt32 { required: false }
            ));
        } else {
            panic!("Expected EBaseStruct");
        }
    }

    #[test]
    fn test_parse_nested_struct() {
        let result =
            ETypeParser::parse("EBaseStruct{locus:+EBaseStruct{contig:+EBinary,position:+EInt32}}")
                .unwrap();

        if let EncodedType::EBaseStruct { required, fields } = result {
            assert!(!required);
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].name, "locus");

            if let EncodedType::EBaseStruct {
                required: inner_required,
                fields: inner_fields,
            } = &fields[0].encoded_type
            {
                assert!(inner_required);
                assert_eq!(inner_fields.len(), 2);
                assert_eq!(inner_fields[0].name, "contig");
                assert_eq!(inner_fields[1].name, "position");
            } else {
                panic!("Expected nested EBaseStruct");
            }
        } else {
            panic!("Expected EBaseStruct");
        }
    }

    #[test]
    fn test_parse_backtick_quoted_field_names() {
        // Hail uses backticks for field names with special characters like dots
        let result = ETypeParser::parse(
            "EBaseStruct{normal_field:EInt32,`p.value.NA`:EFloat64,`Is.SPA`:EBoolean}",
        )
        .unwrap();

        if let EncodedType::EBaseStruct {
            required: _,
            fields,
        } = result
        {
            assert_eq!(fields.len(), 3);
            assert_eq!(fields[0].name, "normal_field");
            assert_eq!(fields[1].name, "p.value.NA");
            assert_eq!(fields[2].name, "Is.SPA");
            assert!(matches!(
                fields[0].encoded_type,
                EncodedType::EInt32 { required: false }
            ));
            assert!(matches!(
                fields[1].encoded_type,
                EncodedType::EFloat64 { required: false }
            ));
            assert!(matches!(
                fields[2].encoded_type,
                EncodedType::EBoolean { required: false }
            ));
        } else {
            panic!("Expected EBaseStruct");
        }
    }

    #[test]
    fn test_parse_per_ancestry_variant_schema() {
        // Real schema fragment from per-ancestry variant results tables
        let result = ETypeParser::parse(
            "EBaseStruct{Pvalue_log10:EFloat64,`p.value.NA`:EFloat64,`Is.SPA`:EBoolean,AF_case:EFloat64,AF_ctrl:EFloat64}"
        ).unwrap();

        if let EncodedType::EBaseStruct {
            required: _,
            fields,
        } = result
        {
            assert_eq!(fields.len(), 5);
            assert_eq!(fields[0].name, "Pvalue_log10");
            assert_eq!(fields[1].name, "p.value.NA");
            assert_eq!(fields[2].name, "Is.SPA");
            assert_eq!(fields[3].name, "AF_case");
            assert_eq!(fields[4].name, "AF_ctrl");
        } else {
            panic!("Expected EBaseStruct");
        }
    }
}
