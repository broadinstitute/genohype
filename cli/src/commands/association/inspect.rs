use crate::association_sources::inspect::{inspect_delimited, inspect_hail, print_human_report};
use crate::association_sources::mapping::load_mapping;
use crate::association_sources::vcf::inspect_vcf;
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
            inspect_hail(&args.source, &mapping, args.sample_rows).map_err(invalid)?
        }
        detected @ DetectedAssociationSource::Vcf => {
            inspect_vcf(&args.source, detected, &mapping, args.sample_rows).map_err(invalid)?
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
