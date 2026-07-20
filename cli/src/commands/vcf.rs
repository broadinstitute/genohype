//! VCF command handlers.

use crate::Result;
use genohype_core::vcf::index::{build_tabix_index, write_tabix_index};
use indicatif::{ProgressBar, ProgressStyle};
use std::time::Instant;

/// Run the `vcf index` command: build a tabix index for a BGZF-compressed VCF.
pub fn run_vcf_index(vcf_path: &str, output: Option<&str>) -> Result<()> {
    let output_path = match output {
        Some(p) => p.to_string(),
        None => format!("{}.tbi", vcf_path),
    };

    println!("Building tabix index for {}", vcf_path);

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] {msg}").unwrap(),
    );
    pb.set_message("Reading records...");

    let start = Instant::now();
    let index = build_tabix_index(
        vcf_path,
        Some(&|count| {
            if count % 100_000 == 0 {
                pb.set_message(format!("{} records indexed", count));
            }
        }),
    )?;

    pb.finish_with_message(format!(
        "Indexing complete in {:.1}s",
        start.elapsed().as_secs_f64()
    ));

    write_tabix_index(&index, &output_path)?;
    println!("Wrote index to {}", output_path);

    Ok(())
}
