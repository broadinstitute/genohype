use crate::cli::{ManifestAccessArg, ManifestCommands, ManifestInventoryArgs};
use crate::{HailError, Result};
use genohype_core::io::{inventory_hail_table_path, InventoryAccess, InventoryOptions};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const DIGEST_LABEL: &str = "genohype.source-manifest.sha256.v1";

pub fn run_manifest_command(command: ManifestCommands) -> Result<()> {
    match command {
        ManifestCommands::Inventory(args) => run_inventory(args),
    }
}

fn run_inventory(args: ManifestInventoryArgs) -> Result<()> {
    let output = output_path(&args.output, true)?;
    let digest_name = match (&output, args.digest_output.as_deref()) {
        (Some(_), None) => Some(format!("{}.sha256", args.output)),
        (None, None) => None,
        (_, Some(path)) => Some(path.to_string()),
    };
    let digest_output = digest_name
        .as_deref()
        .map(|path| output_path(path, false))
        .transpose()?
        .flatten();

    if let (Some(manifest), Some(digest)) = (&output, &digest_output) {
        if absolute_lexical(manifest)? == absolute_lexical(digest)? {
            return invalid("manifest and digest output paths must be distinct");
        }
    }
    for path in output.iter().chain(digest_output.iter()) {
        if path.exists() {
            return invalid(format!(
                "refusing to overwrite existing output: {}",
                path.display()
            ));
        }
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                return invalid(format!(
                    "output parent directory does not exist: {}",
                    parent.display()
                ));
            }
        }
    }

    let access = match args.access {
        ManifestAccessArg::Sequential => InventoryAccess::Sequential,
        ManifestAccessArg::Indexed => InventoryAccess::Indexed,
    };
    let result = inventory_hail_table_path(
        &args.table,
        &InventoryOptions {
            access,
            max_objects: args.max_objects,
            ..InventoryOptions::default()
        },
    )?;
    let mut manifest_bytes = result.manifest.canonical_bytes()?;
    manifest_bytes.push(b'\n');
    let digest_bytes = format!("{DIGEST_LABEL} {}\n", result.digest_sha256).into_bytes();

    let mut manifest_file = match &output {
        Some(path) => Some(create_new(path)?),
        None => None,
    };
    let mut digest_file = match &digest_output {
        Some(path) => match create_new(path) {
            Ok(file) => Some(file),
            Err(error) => {
                if let Some(path) = &output {
                    let _ = fs::remove_file(path);
                }
                return Err(error);
            }
        },
        None => None,
    };

    let write_result = (|| -> Result<()> {
        if let Some(file) = manifest_file.as_mut() {
            file.write_all(&manifest_bytes)?;
            file.sync_all()?;
        }
        if let Some(file) = digest_file.as_mut() {
            file.write_all(&digest_bytes)?;
            file.sync_all()?;
        }
        if output.is_none() {
            let mut stdout = io::stdout().lock();
            stdout.write_all(&manifest_bytes)?;
            stdout.flush()?;
        }
        Ok(())
    })();
    if let Err(error) = write_result {
        for path in output.iter().chain(digest_output.iter()) {
            let _ = fs::remove_file(path);
        }
        return Err(error);
    }

    eprintln!("source manifest inventory complete");
    eprintln!("  root: {}", result.root);
    eprintln!("  access: {}", result.access.as_str());
    eprintln!("  objects: {}", result.manifest.objects.len());
    eprintln!(
        "  ignored: directory_markers={} hadoop_checksums={} unrelated={} sequential_index_objects={}",
        result.ignored.directory_markers,
        result.ignored.hadoop_checksums,
        result.ignored.unrelated,
        result.ignored.sequential_index_objects
    );
    eprintln!(
        "  manifest: {}",
        output
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "stdout".to_string())
    );
    eprintln!("  digest: {}", result.digest_sha256);
    Ok(())
}

fn output_path(value: &str, allow_stdout: bool) -> Result<Option<PathBuf>> {
    if value == "-" {
        if allow_stdout {
            return Ok(None);
        }
        return invalid("digest output must be a local file, not stdout");
    }
    if value.contains("://") {
        return invalid("source-manifest outputs must be local files");
    }
    if value.is_empty() {
        return invalid("output path must not be empty");
    }
    Ok(Some(PathBuf::from(value)))
}

fn absolute_lexical(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    Ok(normalized)
}

fn create_new(path: &Path) -> Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(HailError::Io)
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(HailError::InvalidFormat(message.into()))
}
