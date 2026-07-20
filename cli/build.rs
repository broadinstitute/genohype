use std::{env, process::Command};

fn main() {
    let git_hash = env::var("GENOHYPE_GIT_SHA")
        .ok()
        .filter(|sha| !sha.trim().is_empty())
        .or_else(git_hash_from_checkout)
        .map(|sha| sha.chars().take(7).collect::<String>())
        .unwrap_or_else(|| "unknown".to_string());
    let package_version = env::var("CARGO_PKG_VERSION").expect("Cargo provides CARGO_PKG_VERSION");
    let build_version = format!("{package_version} ({git_hash})");

    println!("cargo:rustc-env=GIT_HASH={git_hash}");
    println!("cargo:rustc-env=GENOHYPE_VERSION={build_version}");
    println!("cargo:rerun-if-env-changed=GENOHYPE_GIT_SHA");
    emit_git_rerun_path("HEAD");
    emit_git_rerun_path("index");

    // Re-run if embedded dashboard files change (for rust-embed).
    println!("cargo:rerun-if-changed=static");
}

fn emit_git_rerun_path(path: &str) {
    let git_path = Command::new("git")
        .args(["rev-parse", "--git-path", path])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok());

    if let Some(git_path) = git_path {
        println!("cargo:rerun-if-changed={}", git_path.trim());
    }
}

fn git_hash_from_checkout() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--verify", "HEAD"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    String::from_utf8(output.stdout)
        .ok()
        .map(|sha| sha.trim().to_string())
        .filter(|sha| !sha.is_empty())
}
