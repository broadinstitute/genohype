use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=GENOHYPE_E2E_REVISION");
    println!("cargo:rerun-if-env-changed=GENOHYPE_E2E_BUILD_ID");

    let revision = std::env::var("GENOHYPE_E2E_REVISION").unwrap_or_else(|_| {
        let output = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .expect("GENOHYPE_E2E_REVISION is unset and git is unavailable");
        assert!(output.status.success(), "git rev-parse HEAD failed");
        String::from_utf8(output.stdout)
            .expect("git revision is not UTF-8")
            .trim()
            .to_string()
    });
    assert!(
        revision.len() == 40
            && revision
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "GENOHYPE_E2E_REVISION must be a lowercase, full 40-character Git revision"
    );

    let build_id =
        std::env::var("GENOHYPE_E2E_BUILD_ID").unwrap_or_else(|_| "fixture-dev".to_string());
    assert!(
        !build_id.is_empty()
            && build_id.len() <= 32
            && !build_id.starts_with('-')
            && !build_id.ends_with('-')
            && build_id
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'),
        "GENOHYPE_E2E_BUILD_ID must be a lowercase ASCII identifier of at most 32 characters"
    );

    println!("cargo:rustc-env=GENOHYPE_E2E_REVISION={revision}");
    println!("cargo:rustc-env=GENOHYPE_E2E_BUILD_ID={build_id}");
    println!("cargo:rustc-env=GENOHYPE_E2E_BUILD_IDENTITY=e2e:{revision}:{build_id}");
}
