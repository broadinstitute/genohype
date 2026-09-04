use crate::association_sources::delimited::DelimitedAssociationSource;
use crate::association_sources::mapping::load_mapping;
use crate::association_sources::{detect_source, DetectedAssociationSource};
use crate::cli::AssociationViewArgs;
use crate::{HailError, Result};
use std::path::Path;

/// Validated handoff kept deliberately small so the session-lifecycle slice can
/// replace only this boundary without changing the stable CLI or source parser.
struct PreparedAssociationView {
    #[allow(dead_code)]
    args: AssociationViewArgs,
    #[allow(dead_code)]
    detected: DetectedAssociationSource,
}

pub fn run(args: AssociationViewArgs) -> Result<()> {
    if !args.threshold.is_finite() || args.threshold <= 0.0 || args.threshold > 1.0 {
        return Err(HailError::InvalidFormat(
            "--threshold must be finite and in 0 < P <= 1".into(),
        ));
    }
    let detected = detect_source(&args.source).map_err(invalid)?;
    let mapping = load_mapping(&args.mapping, Some(args.build)).map_err(invalid)?;
    let normalized = mapping.complete().map_err(invalid)?;

    // Validate flat headers and the exact mapping before handing off. Hail and
    // VCF validation is owned by their dedicated adapters.
    if matches!(detected, DetectedAssociationSource::Delimited { .. }) {
        DelimitedAssociationSource::open(
            Path::new(&args.source),
            detected,
            normalized,
            args.build.as_str(),
        )
        .map_err(invalid)?;
    }

    handoff_to_session_lifecycle(PreparedAssociationView { args, detected })
}

fn handoff_to_session_lifecycle(_prepared: PreparedAssociationView) -> Result<()> {
    Err(HailError::InvalidFormat(
        "association view input is valid, but indexed-session build/serve integration is not available in this implementation slice; no viewer was started"
            .into(),
    ))
}

fn invalid(error: impl ToString) -> HailError {
    HailError::InvalidFormat(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{AssociationBuildArg, AssociationMappingArgs};
    use tempfile::tempdir;

    fn args(source: String) -> AssociationViewArgs {
        AssociationViewArgs {
            source,
            build: AssociationBuildArg::Grch38,
            mapping: AssociationMappingArgs {
                mappings: vec![
                    "contig=CHR".into(),
                    "position=POS".into(),
                    "pvalue=P".into(),
                ],
                mapping_file: None,
            },
            threshold: 5e-8,
            top_hits: 100,
            width: 3000,
            height: 800,
            no_open: true,
            port: Some(0),
            rebuild: false,
            ephemeral: false,
            retention_hours: 168,
            cleanup_expired: false,
        }
    }

    #[test]
    fn temporary_handoff_fails_clearly_and_never_claims_success() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("input.tsv");
        std::fs::write(&path, "CHR\tPOS\tP\nchr1\t1\t0.5\n").unwrap();
        let error = run(args(path.display().to_string())).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("input is valid"));
        assert!(message.contains("no viewer was started"));
    }

    #[test]
    fn invalid_mapping_or_threshold_fails_before_handoff() {
        let mut invalid_threshold = args("input.tsv".into());
        invalid_threshold.threshold = 0.0;
        assert!(run(invalid_threshold)
            .unwrap_err()
            .to_string()
            .contains("threshold"));

        let mut invalid_mapping = args("input.tsv".into());
        invalid_mapping.mapping.mappings.clear();
        assert!(run(invalid_mapping)
            .unwrap_err()
            .to_string()
            .contains("mapping requires"));
    }
}
