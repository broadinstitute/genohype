//! Loci generation handler.
//!
//! Processes loci generation jobs for Manhattan plot regions.

use crate::distributed::message::LociSpec;
use crate::Result;

/// Process a loci generation job.
pub fn process_loci(spec: &LociSpec) -> Result<usize> {
    use crate::manhattan::aggregate::generate_loci_standalone;

    println!("Processing loci generation job:");
    println!("  Output dir: {}", spec.output_dir);
    println!("  Exome: {:?}", spec.exome_results);
    println!("  Genome: {:?}", spec.genome_results);
    println!("  Gene burden: {:?}", spec.gene_burden);
    println!("  Window: {}bp", spec.locus_window);
    println!("  Threshold: {}", spec.threshold);
    println!("  Gene threshold: {}", spec.gene_threshold);

    let loci = generate_loci_standalone(
        &spec.output_dir,
        spec.exome_results.as_deref(),
        spec.genome_results.as_deref(),
        spec.gene_burden.as_deref(),
        spec.locus_window,
        spec.threshold,
        spec.gene_threshold,
        8, // threads per worker
        spec.locus_plots,
        spec.min_variants_per_locus,
    )?;

    println!("Generated {} locus plots", loci.len());
    Ok(loci.len())
}
