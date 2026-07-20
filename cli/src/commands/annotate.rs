//! VEP annotation command.

use crate::cli::AnnotateArgs;
use crate::commands::utils::{
    parse_export_filters, parse_export_intervals, progress_style_spinner,
};
use fastvep_annotate::AnnotationContext;
use fastvep_io::output::{self, DEFAULT_CSQ_FIELDS};
use genohype_core::query::QueryEngine;
use genohype_core::vep::row_to_variation_feature;
use genohype_core::Result;
use indicatif::ProgressBar;
use owo_colors::OwoColorize;
use std::io::{self, BufWriter, Write};

pub fn run_annotate(args: AnnotateArgs) -> Result<()> {
    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    eprintln!(
        "{} {} {}",
        "Annotating".green(),
        args.common.input.bright_white(),
        "with fastVEP".green(),
    );

    // Initialize annotation context
    eprintln!("{} {}", "Loading GFF3:".cyan(), args.gff3.bright_white());
    if let Some(ref fasta) = args.fasta {
        eprintln!("{} {}", "Loading FASTA:".cyan(), fasta.bright_white());
    }

    let context = AnnotationContext::new(
        Some(args.gff3.as_str()),
        args.fasta.as_deref(),
        args.sa_dir.as_deref(),
        args.distance,
    )
    .map_err(|e| {
        genohype_core::HailError::InvalidFormat(format!("Failed to init VEP context: {}", e))
    })?;

    // Open the query engine
    let engine = QueryEngine::open_path(&args.common.input)?;
    eprintln!(
        "{} {}",
        "Partitions:".cyan(),
        engine.num_partitions().to_string().bright_white()
    );
    if !where_filters.is_empty() {
        eprintln!(
            "{} {:?}",
            "Filters:".cyan(),
            where_filters
                .iter()
                .map(|r| r.field_path_str())
                .collect::<Vec<_>>()
        );
    }
    if let Some(ref ivl) = intervals {
        eprintln!(
            "{} {} intervals",
            "Interval filter:".cyan(),
            ivl.len().to_string().bright_white()
        );
    }
    if let Some(l) = args.common.limit {
        eprintln!("{} {}", "Row limit:".cyan(), l.to_string().bright_white());
    }
    eprintln!();

    let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;
    let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
        Box::new(iterator.take(n))
    } else {
        Box::new(iterator)
    };

    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    let mut total_rows = 0u64;
    let mut annotated_rows = 0u64;
    let mut intergenic_rows = 0u64;

    match args.output_format.as_str() {
        "vcf" => {
            let writer: Box<dyn Write> = if let Some(ref path) = args.output {
                Box::new(BufWriter::new(
                    std::fs::File::create(path).map_err(|e| genohype_core::HailError::Io(e))?,
                ))
            } else {
                Box::new(BufWriter::new(io::stdout().lock()))
            };
            let mut writer = writer;

            // Write VCF header
            writeln!(writer, "##fileformat=VCFv4.2")
                .map_err(|e| genohype_core::HailError::Io(e))?;
            writeln!(writer, "##source=genohype-annotate+fastVEP")
                .map_err(|e| genohype_core::HailError::Io(e))?;
            writeln!(writer, "{}", output::csq_header_line(DEFAULT_CSQ_FIELDS))
                .map_err(|e| genohype_core::HailError::Io(e))?;
            writeln!(writer, "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO")
                .map_err(|e| genohype_core::HailError::Io(e))?;

            for row_result in iterator {
                let row = row_result?;
                total_rows += 1;

                let mut vf = match row_to_variation_feature(&row) {
                    Ok(vf) => vf,
                    Err(e) => {
                        eprintln!("Warning: skipping row {}: {}", total_rows, e);
                        continue;
                    }
                };

                context
                    .annotate_variant(&mut vf, args.pick, &[])
                    .map_err(|e| {
                        genohype_core::HailError::InvalidFormat(format!("Annotation error: {}", e))
                    })?;

                // Apply pick filtering
                if args.pick {
                    let mut kept = Vec::new();
                    let mut has_any = false;
                    for tv in vf.transcript_variations.drain(..) {
                        if !has_any || tv.canonical {
                            has_any = true;
                            kept.push(tv);
                        }
                    }
                    vf.transcript_variations = kept;
                }

                // Track intergenic vs annotated
                let is_intergenic = vf.transcript_variations.iter().all(|tv| {
                    tv.allele_annotations.iter().all(|aa| {
                        aa.consequences.len() == 1
                            && aa.consequences[0] == fastvep_core::Consequence::IntergenicVariant
                    })
                });
                if is_intergenic {
                    intergenic_rows += 1;
                } else {
                    annotated_rows += 1;
                }

                // Format CSQ string
                let csq = output::format_csq(&vf, DEFAULT_CSQ_FIELDS);

                // Reconstruct VCF line from original row fields
                let (chrom, pos, id, ref_allele, alt_alleles) = extract_vcf_fields(&row);
                write!(
                    writer,
                    "{}\t{}\t{}\t{}\t{}\t.\t.\tCSQ={}\n",
                    chrom, pos, id, ref_allele, alt_alleles, csq
                )
                .map_err(|e| genohype_core::HailError::Io(e))?;

                if total_rows % 1000 == 0 {
                    pb.set_message(format!("{} variants processed...", total_rows));
                }
            }

            writer
                .flush()
                .map_err(|e| genohype_core::HailError::Io(e))?;
        }
        "json" => {
            let writer: Box<dyn Write> = if let Some(ref path) = args.output {
                Box::new(BufWriter::new(
                    std::fs::File::create(path).map_err(|e| genohype_core::HailError::Io(e))?,
                ))
            } else {
                Box::new(BufWriter::new(io::stdout().lock()))
            };
            let mut writer = writer;

            for row_result in iterator {
                let row = row_result?;
                total_rows += 1;

                let mut vf = match row_to_variation_feature(&row) {
                    Ok(vf) => vf,
                    Err(e) => {
                        eprintln!("Warning: skipping row {}: {}", total_rows, e);
                        continue;
                    }
                };

                context
                    .annotate_variant(&mut vf, args.pick, &[])
                    .map_err(|e| {
                        genohype_core::HailError::InvalidFormat(format!("Annotation error: {}", e))
                    })?;

                if args.pick {
                    let mut kept = Vec::new();
                    let mut has_any = false;
                    for tv in vf.transcript_variations.drain(..) {
                        if !has_any || tv.canonical {
                            has_any = true;
                            kept.push(tv);
                        }
                    }
                    vf.transcript_variations = kept;
                }

                let is_intergenic = vf.transcript_variations.iter().all(|tv| {
                    tv.allele_annotations.iter().all(|aa| {
                        aa.consequences.len() == 1
                            && aa.consequences[0] == fastvep_core::Consequence::IntergenicVariant
                    })
                });
                if is_intergenic {
                    intergenic_rows += 1;
                } else {
                    annotated_rows += 1;
                }

                let json = output::format_json(&vf, false);
                serde_json::to_writer(&mut writer, &json).map_err(|e| {
                    genohype_core::HailError::InvalidFormat(format!("JSON write error: {}", e))
                })?;
                writeln!(writer).map_err(|e| genohype_core::HailError::Io(e))?;

                if total_rows % 1000 == 0 {
                    pb.set_message(format!("{} variants processed...", total_rows));
                }
            }

            writer
                .flush()
                .map_err(|e| genohype_core::HailError::Io(e))?;
        }
        "tab" => {
            let writer: Box<dyn Write> = if let Some(ref path) = args.output {
                Box::new(BufWriter::new(
                    std::fs::File::create(path).map_err(|e| genohype_core::HailError::Io(e))?,
                ))
            } else {
                Box::new(BufWriter::new(io::stdout().lock()))
            };
            let mut writer = writer;

            // Write tab header
            writeln!(
                writer,
                "#Uploaded_variation\tLocation\tAllele\tGene\tFeature\tFeature_type\tConsequence\tIMPACT"
            )
            .map_err(|e| genohype_core::HailError::Io(e))?;

            for row_result in iterator {
                let row = row_result?;
                total_rows += 1;

                let mut vf = match row_to_variation_feature(&row) {
                    Ok(vf) => vf,
                    Err(e) => {
                        eprintln!("Warning: skipping row {}: {}", total_rows, e);
                        continue;
                    }
                };

                context
                    .annotate_variant(&mut vf, args.pick, &[])
                    .map_err(|e| {
                        genohype_core::HailError::InvalidFormat(format!("Annotation error: {}", e))
                    })?;

                if args.pick {
                    let mut kept = Vec::new();
                    let mut has_any = false;
                    for tv in vf.transcript_variations.drain(..) {
                        if !has_any || tv.canonical {
                            has_any = true;
                            kept.push(tv);
                        }
                    }
                    vf.transcript_variations = kept;
                }

                let is_intergenic = vf.transcript_variations.iter().all(|tv| {
                    tv.allele_annotations.iter().all(|aa| {
                        aa.consequences.len() == 1
                            && aa.consequences[0] == fastvep_core::Consequence::IntergenicVariant
                    })
                });
                if is_intergenic {
                    intergenic_rows += 1;
                } else {
                    annotated_rows += 1;
                }

                for line in output::format_tab_line(
                    &vf,
                    &output::LoadedSupplementarySpecs::new(&[], &[]),
                    false,
                ) {
                    writeln!(writer, "{}", line).map_err(|e| genohype_core::HailError::Io(e))?;
                }

                if total_rows % 1000 == 0 {
                    pb.set_message(format!("{} variants processed...", total_rows));
                }
            }

            writer
                .flush()
                .map_err(|e| genohype_core::HailError::Io(e))?;
        }
        _ => unreachable!("clap enforces valid output formats"),
    }

    pb.finish_and_clear();

    // Print summary to stderr
    eprintln!();
    eprintln!("{}", "Annotation complete!".green().bold());
    eprintln!(
        "  {} {}",
        "Total variants:".cyan(),
        total_rows.to_string().bright_white()
    );
    eprintln!(
        "  {} {}",
        "Annotated (genic):".cyan(),
        annotated_rows.to_string().bright_white()
    );
    eprintln!(
        "  {} {}",
        "Intergenic:".cyan(),
        intergenic_rows.to_string().bright_white()
    );
    if let Some(ref path) = args.output {
        eprintln!("  {} {}", "Output file:".cyan(), path.bright_white());
    }

    Ok(())
}

/// Extract VCF-compatible fields from an EncodedValue row for output.
fn extract_vcf_fields(
    row: &genohype_core::codec::encoded_type::EncodedValue,
) -> (String, String, String, String, String) {
    use genohype_core::codec::encoded_type::EncodedValue;

    let fields = match row {
        EncodedValue::Struct(f) => f,
        _ => return (".".into(), ".".into(), ".".into(), ".".into(), ".".into()),
    };

    let locus = fields.iter().find(|(k, _)| k == "locus");
    let (chrom, pos) = if let Some((_, EncodedValue::Struct(lf))) = locus {
        let c = lf
            .iter()
            .find(|(k, _)| k == "contig")
            .and_then(|(_, v)| v.as_string())
            .unwrap_or_else(|| ".".into());
        let p = lf
            .iter()
            .find(|(k, _)| k == "position")
            .and_then(|(_, v)| v.as_i32())
            .map(|v| v.to_string())
            .unwrap_or_else(|| ".".into());
        (c, p)
    } else {
        (".".into(), ".".into())
    };

    let alleles = fields.iter().find(|(k, _)| k == "alleles");
    let (ref_allele, alt_alleles) = if let Some((_, EncodedValue::Array(arr))) = alleles {
        let strs: Vec<String> = arr.iter().filter_map(|v| v.as_string()).collect();
        if strs.is_empty() {
            (".".into(), ".".into())
        } else {
            let r = strs[0].clone();
            let a = if strs.len() > 1 {
                strs[1..].join(",")
            } else {
                ".".into()
            };
            (r, a)
        }
    } else {
        (".".into(), ".".into())
    };

    let id = fields
        .iter()
        .find(|(k, _)| k == "rsid")
        .and_then(|(_, v)| v.as_string())
        .unwrap_or_else(|| ".".into());

    (chrom, pos, id, ref_allele, alt_alleles)
}
