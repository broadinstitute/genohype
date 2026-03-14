use crate::codec::EncodedValue;

/// Extract a field from an EncodedValue struct (exact match)
pub fn get_field<'a>(value: &'a EncodedValue, name: &str) -> Option<&'a EncodedValue> {
    if let EncodedValue::Struct(fields) = value {
        for (field_name, field_value) in fields {
            if field_name == name {
                return Some(field_value);
            }
        }
    }
    None
}

/// Extract a field from an EncodedValue struct (case-insensitive)
pub fn get_field_ci<'a>(value: &'a EncodedValue, name: &str) -> Option<&'a EncodedValue> {
    if let EncodedValue::Struct(fields) = value {
        let name_lower = name.to_lowercase();
        for (field_name, field_value) in fields {
            if field_name.to_lowercase() == name_lower {
                return Some(field_value);
            }
        }
    }
    None
}

/// Try multiple field names (for handling different table schemas)
pub fn get_field_any<'a>(value: &'a EncodedValue, names: &[&str]) -> Option<&'a EncodedValue> {
    for name in names {
        if let Some(field) = get_field(value, name) {
            return Some(field);
        }
    }
    // Fall back to case-insensitive search with first name
    if let Some(first) = names.first() {
        return get_field_ci(value, first);
    }
    None
}

/// Extract a nested field using dot notation (e.g., "locus.contig")
pub fn get_nested_field<'a>(value: &'a EncodedValue, path: &str) -> Option<&'a EncodedValue> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value;
    for part in parts {
        current = get_field(current, part)?;
    }
    Some(current)
}

/// Extract as string (handles both Binary and String variants)
pub fn as_string(value: &EncodedValue) -> Option<String> {
    match value {
        EncodedValue::Binary(b) => String::from_utf8(b.clone()).ok(),
        _ => None,
    }
}

/// Extract as i32
pub fn as_i32(value: &EncodedValue) -> Option<i32> {
    match value {
        EncodedValue::Int32(i) => Some(*i),
        EncodedValue::Int64(i) => (*i).try_into().ok(),
        _ => None,
    }
}

/// Extract as f64
pub fn as_f64(value: &EncodedValue) -> Option<f64> {
    match value {
        EncodedValue::Float64(f) => Some(*f),
        EncodedValue::Float32(f) => Some(*f as f64),
        _ => None,
    }
}

/// A typed variant association record
#[derive(Debug, Clone)]
pub struct VariantAssociation {
    pub contig: String,
    pub position: i32,
    pub ref_allele: String,
    pub alt_allele: String,
    pub pvalue: f64,
    pub beta: f64,
    pub se: f64,
    pub af: Option<f64>,
    pub ac: Option<i32>,
    pub n_cases: Option<i32>,
    pub n_controls: Option<i32>,
    // Case/control breakdown fields
    pub ac_cases: Option<f64>,
    pub ac_controls: Option<f64>,
    pub af_cases: Option<f64>,
    pub af_controls: Option<f64>,
    // Total allele count across all individuals (AC_Allele2)
    pub association_ac: Option<f64>,
}

impl VariantAssociation {
    /// Parse from an EncodedValue row
    pub fn from_encoded(value: &EncodedValue) -> Option<Self> {
        let locus = get_field(value, "locus")?;
        let contig = as_string(get_field(locus, "contig")?)?;
        let position = as_i32(get_field(locus, "position")?)?;

        let alleles = get_field(value, "alleles")?;
        let (ref_allele, alt_allele) = if let EncodedValue::Array(arr) = alleles {
            (as_string(arr.first()?)?, as_string(arr.get(1)?)?)
        } else {
            return None;
        };

        // Extract association_ac (AC_Allele2) - can be int or float
        let association_ac = get_field(value, "AC_Allele2").and_then(|v| match v {
            EncodedValue::Float64(f) => Some(*f),
            EncodedValue::Float32(f) => Some(*f as f64),
            EncodedValue::Int64(i) => Some(*i as f64),
            EncodedValue::Int32(i) => Some(*i as f64),
            _ => None,
        });

        Some(Self {
            contig,
            position,
            ref_allele,
            alt_allele,
            // Handle different field naming conventions (Pvalue vs pvalue, BETA vs beta, etc.)
            pvalue: as_f64(get_field_any(value, &["pvalue", "Pvalue", "PVALUE"])?)?,
            beta: as_f64(get_field_any(value, &["beta", "BETA", "Beta"])?)?,
            se: as_f64(get_field_any(value, &["se", "SE", "Se"])?)?,
            af: get_field_any(value, &["af", "AF", "AF_Allele2", "af_allele2"])
                .and_then(as_f64)
                .or_else(|| get_field(value, "AF_case").and_then(as_f64)),
            ac: get_field_any(value, &["ac", "AC", "AC_Allele2"]).and_then(as_i32),
            n_cases: get_field_any(value, &["n_cases", "N_cases"]).and_then(as_i32),
            n_controls: get_field_any(value, &["n_controls", "N_controls"]).and_then(as_i32),
            // Case/control breakdown
            ac_cases: get_field_any(value, &["AC_case", "ac_case"]).and_then(as_f64),
            ac_controls: get_field_any(value, &["AC_ctrl", "ac_ctrl"]).and_then(as_f64),
            af_cases: get_field_any(value, &["AF_case", "af_case"]).and_then(as_f64),
            af_controls: get_field_any(value, &["AF_ctrl", "af_ctrl"]).and_then(as_f64),
            association_ac,
        })
    }

    /// Generate variant_id in chr-pos-ref-alt format
    pub fn variant_id(&self) -> String {
        format!(
            "{}-{}-{}-{}",
            self.contig, self.position, self.ref_allele, self.alt_allele
        )
    }
}
