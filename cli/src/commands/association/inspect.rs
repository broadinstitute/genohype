use crate::association_sources::inspect::{inspect_delimited, print_human_report};
use crate::association_sources::mapping::load_mapping;
use crate::association_sources::{detect_source, DetectedAssociationSource};
use crate::cli::AssociationInspectArgs;
use crate::{HailError, Result};

pub fn run(args: AssociationInspectArgs) -> Result<()> {
    let detected = detect_source(&args.source).map_err(invalid)?;
    let mapping = load_mapping(&args.mapping, args.build).map_err(invalid)?;
    let report = match detected {
        detected @ DetectedAssociationSource::Delimited { .. } => {
            inspect_delimited(&args.source, detected, &mapping, args.sample_rows)
                .map_err(invalid)?
        }
        DetectedAssociationSource::Hail => {
            return Err(HailError::InvalidFormat(
                "bounded Hail association inspection belongs to the projected Hail adapter slice and is not available in this build"
                    .into(),
            ));
        }
        DetectedAssociationSource::Vcf => {
            return Err(HailError::InvalidFormat(
                "bounded VCF association inspection belongs to the INFO-aware VCF adapter slice and is not available in this build"
                    .into(),
            ));
        }
    };
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report)
                .map_err(|error| HailError::InvalidFormat(error.to_string()))?
        );
    } else {
        print_human_report(&report);
    }
    Ok(())
}

fn invalid(error: impl ToString) -> HailError {
    HailError::InvalidFormat(error.to_string())
}
