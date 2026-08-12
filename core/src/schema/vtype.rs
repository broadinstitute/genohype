//! Strict parser and lossless model for Hail virtual types (VTypes).

use crate::{HailError, Result};
use serde::{Deserialize, Serialize};

/// A Hail logical type. Unlike Arrow or EType, this preserves every VType
/// distinction needed to interpret the physical representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LogicalType {
    Int32,
    Int64,
    Float32,
    Float64,
    Boolean,
    String,
    Binary,
    Call,
    Array {
        element: Box<LogicalType>,
    },
    Set {
        element: Box<LogicalType>,
    },
    Dict {
        key: Box<LogicalType>,
        value: Box<LogicalType>,
    },
    Tuple {
        elements: Vec<LogicalType>,
    },
    Struct {
        fields: Vec<LogicalField>,
    },
    Locus {
        reference_genome: String,
    },
    Interval {
        point: Box<LogicalType>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalField {
    pub name: String,
    pub logical_type: LogicalType,
}

impl LogicalType {
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Int32 => "Int32",
            Self::Int64 => "Int64",
            Self::Float32 => "Float32",
            Self::Float64 => "Float64",
            Self::Boolean => "Boolean",
            Self::String => "String",
            Self::Binary => "Binary",
            Self::Call => "Call",
            Self::Array { .. } => "Array",
            Self::Set { .. } => "Set",
            Self::Dict { .. } => "Dict",
            Self::Tuple { .. } => "Tuple",
            Self::Struct { .. } => "Struct",
            Self::Locus { .. } => "Locus",
            Self::Interval { .. } => "Interval",
        }
    }
}

pub struct VTypeParser;

impl VTypeParser {
    /// Parse exactly one VType. Unknown names, malformed delimiters, and
    /// trailing input are errors rather than an `Any` escape hatch.
    pub fn parse(raw: &str) -> Result<LogicalType> {
        let mut parser = Parser { raw, pos: 0 };
        let value = parser.parse_type()?;
        parser.skip_ws();
        if !parser.eof() {
            return parser.error("trailing VType syntax");
        }
        Ok(value)
    }
}

struct Parser<'a> {
    raw: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn parse_type(&mut self) -> Result<LogicalType> {
        self.skip_ws();
        // Some retained metadata contains EType-style required markers even
        // though requiredness is physical. Accept and ignore repeated markers.
        while self.consume('+') {
            self.skip_ws();
        }
        let name = self.identifier("type name")?;
        match name.as_str() {
            "Int32" | "int32" => Ok(LogicalType::Int32),
            "Int64" | "int64" => Ok(LogicalType::Int64),
            "Float32" | "float32" => Ok(LogicalType::Float32),
            "Float64" | "float64" => Ok(LogicalType::Float64),
            "Boolean" | "bool" | "boolean" => Ok(LogicalType::Boolean),
            "String" | "str" | "string" => Ok(LogicalType::String),
            "Binary" | "binary" => Ok(LogicalType::Binary),
            "Call" | "call" => Ok(LogicalType::Call),
            "Array" | "array" => Ok(LogicalType::Array {
                element: Box::new(self.one_bracket_arg("Array")?),
            }),
            "Set" | "set" => Ok(LogicalType::Set {
                element: Box::new(self.one_bracket_arg("Set")?),
            }),
            "Interval" | "interval" => Ok(LogicalType::Interval {
                point: Box::new(self.one_bracket_arg("Interval")?),
            }),
            "Dict" | "dict" => {
                self.expect('[')?;
                let key = self.parse_type()?;
                self.expect(',')?;
                let value = self.parse_type()?;
                self.expect(']')?;
                Ok(LogicalType::Dict {
                    key: Box::new(key),
                    value: Box::new(value),
                })
            }
            "Tuple" | "tuple" => {
                self.expect('[')?;
                let mut elements = Vec::new();
                self.skip_ws();
                if !self.consume(']') {
                    loop {
                        elements.push(self.parse_type()?);
                        self.skip_ws();
                        if self.consume(']') {
                            break;
                        }
                        self.expect(',')?;
                    }
                }
                Ok(LogicalType::Tuple { elements })
            }
            "Struct" | "struct" => self.parse_struct(),
            "Locus" | "locus" => {
                self.expect('(')?;
                self.skip_ws();
                let reference_genome = if self.peek() == Some('"') {
                    self.json_string()?
                } else if self.peek() == Some('`') {
                    self.backtick_string()?
                } else {
                    let start = self.pos;
                    while let Some(ch) = self.peek() {
                        if ch == ')' || ch.is_whitespace() {
                            break;
                        }
                        self.bump();
                    }
                    self.raw[start..self.pos].to_string()
                };
                if reference_genome.is_empty() {
                    return self.error("Locus reference genome must not be empty");
                }
                self.expect(')')?;
                Ok(LogicalType::Locus { reference_genome })
            }
            _ => self.error(&format!("unknown VType {name:?}")),
        }
    }

    fn one_bracket_arg(&mut self, owner: &str) -> Result<LogicalType> {
        self.expect('[')?;
        self.skip_ws();
        if self.consume(']') {
            return self.error(&format!("{owner} requires one type argument"));
        }
        let value = self.parse_type()?;
        self.expect(']')?;
        Ok(value)
    }

    fn parse_struct(&mut self) -> Result<LogicalType> {
        self.expect('{')?;
        let mut fields = Vec::new();
        self.skip_ws();
        if self.consume('}') {
            return Ok(LogicalType::Struct { fields });
        }
        loop {
            let name = self.field_name()?;
            if name.is_empty() {
                return self.error("struct field name must not be empty");
            }
            self.expect(':')?;
            let logical_type = self.parse_type()?;
            fields.push(LogicalField { name, logical_type });
            self.skip_ws();
            if self.consume('}') {
                break;
            }
            self.expect(',')?;
        }
        Ok(LogicalType::Struct { fields })
    }

    fn field_name(&mut self) -> Result<String> {
        self.skip_ws();
        match self.peek() {
            Some('"') => self.json_string(),
            Some('`') => self.backtick_string(),
            _ => {
                let start = self.pos;
                while let Some(ch) = self.peek() {
                    if ch == ':' || ch.is_whitespace() {
                        break;
                    }
                    if matches!(ch, ',' | '{' | '}' | '[' | ']') {
                        return self.error("malformed unquoted struct field name");
                    }
                    self.bump();
                }
                if start == self.pos {
                    self.error("expected struct field name")
                } else {
                    Ok(self.raw[start..self.pos].to_string())
                }
            }
        }
    }

    fn json_string(&mut self) -> Result<String> {
        let start = self.pos;
        self.bump();
        let mut escaped = false;
        while let Some(ch) = self.bump() {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                return serde_json::from_str(&self.raw[start..self.pos]).map_err(|error| {
                    HailError::InvalidFormat(format!(
                        "invalid quoted VType name at byte {start}: {error}"
                    ))
                });
            }
        }
        self.error("unterminated quoted VType name")
    }

    fn backtick_string(&mut self) -> Result<String> {
        self.expect('`')?;
        let mut value = String::new();
        loop {
            match self.bump() {
                Some('`') => return Ok(value),
                Some('\\') => match self.bump() {
                    Some(ch @ ('`' | '\\')) => value.push(ch),
                    Some(ch) => {
                        return self.error(&format!("unsupported backtick escape \\{ch}"));
                    }
                    None => return self.error("unterminated backtick escape"),
                },
                Some(ch) => value.push(ch),
                None => return self.error("unterminated backtick VType name"),
            }
        }
    }

    fn identifier(&mut self, what: &str) -> Result<String> {
        self.skip_ws();
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                self.bump();
            } else {
                break;
            }
        }
        if start == self.pos {
            self.error(&format!("expected {what}"))
        } else {
            Ok(self.raw[start..self.pos].to_string())
        }
    }

    fn expect(&mut self, expected: char) -> Result<()> {
        self.skip_ws();
        if self.consume(expected) {
            Ok(())
        } else {
            self.error(&format!("expected {expected:?}"))
        }
    }

    fn consume(&mut self, expected: char) -> bool {
        if self.peek() == Some(expected) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn skip_ws(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.bump();
        }
    }

    fn peek(&self) -> Option<char> {
        self.raw[self.pos..].chars().next()
    }

    fn bump(&mut self) -> Option<char> {
        let ch = self.peek()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn eof(&self) -> bool {
        self.pos == self.raw.len()
    }

    fn error<T>(&self, message: &str) -> Result<T> {
        Err(HailError::InvalidFormat(format!(
            "invalid VType at byte {}: {message}",
            self.pos
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_all_nested_logical_distinctions_and_order() {
        let raw = r#"Struct{s:String,b:Binary,a:Array[Int32],set:Set[String],dict:Dict[String,Binary],tuple:Tuple[Int64,Binary],locus:Locus(GRCh38),interval:Interval[Locus("GRCh37")],"odd:name":bool,`tick\`name`:str}"#;
        let parsed = VTypeParser::parse(raw).unwrap();
        let LogicalType::Struct { fields } = parsed else {
            panic!("expected struct")
        };
        assert_eq!(fields.len(), 10);
        assert_eq!(fields[0].name, "s");
        assert!(matches!(fields[0].logical_type, LogicalType::String));
        assert!(matches!(fields[1].logical_type, LogicalType::Binary));
        assert!(matches!(fields[3].logical_type, LogicalType::Set { .. }));
        assert!(matches!(fields[4].logical_type, LogicalType::Dict { .. }));
        assert!(matches!(fields[5].logical_type, LogicalType::Tuple { .. }));
        assert_eq!(fields[8].name, "odd:name");
        assert_eq!(fields[9].name, "tick`name");
    }

    #[test]
    fn accepts_aliases_whitespace_and_required_markers() {
        assert_eq!(
            VTypeParser::parse(" +Array[+str] ").unwrap(),
            LogicalType::Array {
                element: Box::new(LogicalType::String)
            }
        );
    }

    #[test]
    fn rejects_unknown_malformed_and_trailing_syntax() {
        for raw in [
            "Any",
            "Array[]",
            "Dict[String]",
            "Dict[String,Binary,Int32]",
            "Struct{x:Int32,}",
            "Locus()",
            "Int32 garbage",
            "Tuple[Int32",
        ] {
            assert!(VTypeParser::parse(raw).is_err(), "accepted {raw:?}");
        }
    }
}
