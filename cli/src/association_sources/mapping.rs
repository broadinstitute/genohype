use crate::cli::{AssociationBuildArg, AssociationMappingArgs};
use genohype_association::NormalizedFieldMappingV1;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
#[cfg(test)]
use std::path::Path;
use std::str::FromStr;

pub const LOGICAL_FIELDS: [LogicalField; 13] = [
    LogicalField::Contig,
    LogicalField::Position,
    LogicalField::Ref,
    LogicalField::Alt,
    LogicalField::Pvalue,
    LogicalField::NegLog10P,
    LogicalField::Beta,
    LogicalField::Se,
    LogicalField::Af,
    LogicalField::Ac,
    LogicalField::Gene,
    LogicalField::Consequence,
    LogicalField::Series,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogicalField {
    Contig,
    Position,
    Ref,
    Alt,
    Pvalue,
    NegLog10P,
    Beta,
    Se,
    Af,
    Ac,
    Gene,
    Consequence,
    Series,
}

impl LogicalField {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Contig => "contig",
            Self::Position => "position",
            Self::Ref => "ref",
            Self::Alt => "alt",
            Self::Pvalue => "pvalue",
            Self::NegLog10P => "neg_log10_p",
            Self::Beta => "beta",
            Self::Se => "se",
            Self::Af => "af",
            Self::Ac => "ac",
            Self::Gene => "gene",
            Self::Consequence => "consequence",
            Self::Series => "series",
        }
    }
}

impl fmt::Display for LogicalField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for LogicalField {
    type Err = MappingError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        LOGICAL_FIELDS
            .into_iter()
            .find(|field| field.as_str() == value)
            .ok_or_else(|| MappingError::new(format!("unknown logical field {value:?}")))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MappingInput {
    pub fields: BTreeMap<LogicalField, String>,
    pub assembly: Option<AssociationBuildArg>,
}

impl MappingInput {
    pub fn complete(&self) -> Result<NormalizedFieldMappingV1, MappingError> {
        let get = |field| self.fields.get(&field).cloned();
        let mapping = NormalizedFieldMappingV1 {
            contig: get(LogicalField::Contig)
                .ok_or_else(|| MappingError::new("mapping requires logical field contig"))?,
            position: get(LogicalField::Position)
                .ok_or_else(|| MappingError::new("mapping requires logical field position"))?,
            ref_allele: get(LogicalField::Ref),
            alt_allele: get(LogicalField::Alt),
            pvalue: get(LogicalField::Pvalue),
            neg_log10_p: get(LogicalField::NegLog10P),
            beta: get(LogicalField::Beta),
            se: get(LogicalField::Se),
            af: get(LogicalField::Af),
            ac: get(LogicalField::Ac),
            gene: get(LogicalField::Gene),
            consequence: get(LogicalField::Consequence),
            series: get(LogicalField::Series),
        };
        mapping
            .validate()
            .map_err(|error| MappingError::new(error.to_string()))?;
        Ok(mapping)
    }

    pub fn unresolved_required(&self) -> Vec<String> {
        let mut unresolved = Vec::new();
        for field in [LogicalField::Contig, LogicalField::Position] {
            if !self.fields.contains_key(&field) {
                unresolved.push(field.to_string());
            }
        }
        if !self.fields.contains_key(&LogicalField::Pvalue)
            && !self.fields.contains_key(&LogicalField::NegLog10P)
        {
            unresolved.push("pvalue_or_neg_log10_p".to_string());
        }
        if self.assembly.is_none() {
            unresolved.push("assembly".to_string());
        }
        unresolved
    }

    pub fn validate_partial(&self) -> Result<(), MappingError> {
        let mut sources = BTreeSet::new();
        for source in self.fields.values() {
            validate_source_name(source)?;
            if !sources.insert(source.as_str()) {
                return Err(MappingError::new(format!(
                    "source field {source:?} is mapped more than once"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MappingFile {
    schema_version: String,
    #[serde(default)]
    assembly: Option<String>,
    #[serde(default)]
    fields: BTreeMap<String, String>,
}

pub fn load_mapping(
    args: &AssociationMappingArgs,
    cli_build: Option<AssociationBuildArg>,
) -> Result<MappingInput, MappingError> {
    let mut result = MappingInput::default();
    if let Some(path) = &args.mapping_file {
        let bytes = fs::read(path).map_err(|error| {
            MappingError::new(format!(
                "could not read mapping file {}: {error}",
                path.display()
            ))
        })?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| MappingError::new("mapping file must be UTF-8"))?;
        let file: MappingFile = toml::from_str(text)
            .map_err(|error| MappingError::new(format!("invalid mapping TOML: {error}")))?;
        if file.schema_version != "1.0" {
            return Err(MappingError::new(format!(
                "unsupported mapping schema_version {:?}; expected \"1.0\"",
                file.schema_version
            )));
        }
        result.assembly = file.assembly.as_deref().map(parse_assembly).transpose()?;
        for (logical, source) in file.fields {
            let logical = LogicalField::from_str(&logical)?;
            validate_source_name(&source)?;
            result.fields.insert(logical, source);
        }
        // File-layer errors are not repairable by a later CLI override.
        result.validate_partial()?;
    }

    let cli_fields = parse_cli_layer(&args.mappings)?;
    result.fields.extend(cli_fields);
    if let (Some(file_build), Some(cli_build)) = (result.assembly, cli_build) {
        if file_build != cli_build {
            return Err(MappingError::new(format!(
                "--build {cli_build} conflicts with mapping-file assembly {file_build}"
            )));
        }
    }
    if cli_build.is_some() {
        result.assembly = cli_build;
    }
    result.validate_partial()?;
    Ok(result)
}

fn parse_cli_layer(values: &[String]) -> Result<BTreeMap<LogicalField, String>, MappingError> {
    let mut result = BTreeMap::new();
    for value in values {
        let (logical, source) = value.split_once('=').ok_or_else(|| {
            MappingError::new(format!("invalid --map {value:?}; expected LOGICAL=SOURCE"))
        })?;
        if logical.is_empty() || source.is_empty() || source.contains('=') {
            return Err(MappingError::new(format!(
                "invalid --map {value:?}; both sides must be non-empty and contain one '='"
            )));
        }
        let logical = LogicalField::from_str(logical)?;
        validate_source_name(source)?;
        if result.insert(logical, source.to_string()).is_some() {
            return Err(MappingError::new(format!(
                "logical field {logical} is repeated in --map"
            )));
        }
    }
    Ok(result)
}

fn validate_source_name(source: &str) -> Result<(), MappingError> {
    if source.is_empty() || source.trim() != source || source.len() > 256 {
        return Err(MappingError::new(
            "source field names must be non-empty, trimmed, and at most 256 UTF-8 bytes",
        ));
    }
    if source
        .bytes()
        .any(|byte| byte == 0 || byte == b'\n' || byte == b'\r')
    {
        return Err(MappingError::new(
            "source field names cannot contain NUL or newlines",
        ));
    }
    Ok(())
}

fn parse_assembly(value: &str) -> Result<AssociationBuildArg, MappingError> {
    match value {
        "GRCh37" => Ok(AssociationBuildArg::Grch37),
        "GRCh38" => Ok(AssociationBuildArg::Grch38),
        _ => Err(MappingError::new(format!(
            "mapping-file assembly must be exactly GRCh37 or GRCh38, not {value:?}"
        ))),
    }
}

pub fn mapping_pairs(mapping: &NormalizedFieldMappingV1) -> Vec<(LogicalField, &str)> {
    let mut result = vec![
        (LogicalField::Contig, mapping.contig.as_str()),
        (LogicalField::Position, mapping.position.as_str()),
    ];
    let optional = [
        (LogicalField::Ref, mapping.ref_allele.as_deref()),
        (LogicalField::Alt, mapping.alt_allele.as_deref()),
        (LogicalField::Pvalue, mapping.pvalue.as_deref()),
        (LogicalField::NegLog10P, mapping.neg_log10_p.as_deref()),
        (LogicalField::Beta, mapping.beta.as_deref()),
        (LogicalField::Se, mapping.se.as_deref()),
        (LogicalField::Af, mapping.af.as_deref()),
        (LogicalField::Ac, mapping.ac.as_deref()),
        (LogicalField::Gene, mapping.gene.as_deref()),
        (LogicalField::Consequence, mapping.consequence.as_deref()),
        (LogicalField::Series, mapping.series.as_deref()),
    ];
    result.extend(
        optional
            .into_iter()
            .filter_map(|(logical, source)| source.map(|source| (logical, source))),
    );
    result.sort_by_key(|(logical, _)| *logical);
    result
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingError {
    message: String,
}

impl MappingError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for MappingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl std::error::Error for MappingError {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn args(values: &[&str], file: Option<&Path>) -> AssociationMappingArgs {
        AssociationMappingArgs {
            mappings: values.iter().map(|value| value.to_string()).collect(),
            mapping_file: file.map(Path::to_path_buf),
        }
    }

    #[test]
    fn strict_file_loads_before_cli_overrides() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("mapping.toml");
        fs::write(
            &path,
            "schema_version = \"1.0\"\nassembly = \"GRCh38\"\n[fields]\ncontig = \"chrom\"\nposition = \"bp\"\npvalue = \"old_p\"\n",
        )
        .unwrap();
        let loaded = load_mapping(
            &args(&["pvalue=P", "beta=BETA"], Some(&path)),
            Some(AssociationBuildArg::Grch38),
        )
        .unwrap();
        assert_eq!(loaded.fields[&LogicalField::Contig], "chrom");
        assert_eq!(loaded.fields[&LogicalField::Pvalue], "P");
        assert_eq!(loaded.fields[&LogicalField::Beta], "BETA");
    }

    #[test]
    fn duplicate_cli_keys_unknown_names_and_empty_sides_fail() {
        assert!(load_mapping(&args(&["pvalue=P", "pvalue=P2"], None), None).is_err());
        assert!(load_mapping(&args(&["chromosome=CHR"], None), None).is_err());
        assert!(load_mapping(&args(&["pvalue="], None), None).is_err());
        assert!(load_mapping(&args(&["=P"], None), None).is_err());
    }

    #[test]
    fn strict_toml_rejects_unknown_root_and_field_keys_and_build_conflicts() {
        let temp = tempdir().unwrap();
        let unknown_root = temp.path().join("root.toml");
        fs::write(
            &unknown_root,
            "schema_version = \"1.0\"\nsurprise = true\n[fields]\ncontig = \"CHR\"\n",
        )
        .unwrap();
        assert!(load_mapping(&args(&[], Some(&unknown_root)), None).is_err());

        let unknown_field = temp.path().join("field.toml");
        fs::write(
            &unknown_field,
            "schema_version = \"1.0\"\n[fields]\nchromosome = \"CHR\"\n",
        )
        .unwrap();
        assert!(load_mapping(&args(&[], Some(&unknown_field)), None).is_err());

        let duplicate_source = temp.path().join("duplicate-source.toml");
        fs::write(
            &duplicate_source,
            "schema_version = \"1.0\"\n[fields]\ncontig = \"X\"\nposition = \"X\"\n",
        )
        .unwrap();
        assert!(load_mapping(&args(&["position=POS"], Some(&duplicate_source)), None).is_err());

        let conflict = temp.path().join("conflict.toml");
        fs::write(
            &conflict,
            "schema_version = \"1.0\"\nassembly = \"GRCh37\"\n",
        )
        .unwrap();
        assert!(load_mapping(
            &args(&[], Some(&conflict)),
            Some(AssociationBuildArg::Grch38)
        )
        .is_err());
    }

    #[test]
    fn complete_mapping_requires_coordinates_and_significance() {
        let mut input = MappingInput::default();
        input.fields.insert(LogicalField::Contig, "CHR".into());
        assert!(input.complete().is_err());
        input.fields.insert(LogicalField::Position, "POS".into());
        input.fields.insert(LogicalField::Pvalue, "P".into());
        assert!(input.complete().is_ok());
    }
}
