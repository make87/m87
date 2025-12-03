fn main() {
    // Check if features were explicitly provided via CLI/Cargo.toml
    let agent_from_cli = std::env::var("CARGO_FEATURE_AGENT").is_ok();
    let manager_from_cli = std::env::var("CARGO_FEATURE_MANAGER").is_ok();

    // Only auto-detect features based on OS if none were explicitly specified
    if !agent_from_cli && !manager_from_cli {
        let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();

        match target_os.as_str() {
            "linux" => {
                // Linux gets both agent and manager capabilities
                println!("cargo:rustc-cfg=feature=\"agent\"");
                println!("cargo:rustc-cfg=feature=\"manager\"");
            }
            "macos" | "windows" => {
                // macOS and Windows get manager-only capabilities
                println!("cargo:rustc-cfg=feature=\"manager\"");
            }
            _ => {
                // Other platforms default to manager-only
                println!("cargo:rustc-cfg=feature=\"manager\"");
            }
        }
    }

    // Capture git commit hash for version info
    let git_commit = std::process::Command::new("git")
        .args(&["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // Check if working directory is dirty (has uncommitted changes)
    let is_dirty = std::process::Command::new("git")
        .args(&["status", "--porcelain"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false);

    let git_commit_with_dirty = if is_dirty {
        format!("{}-dirty", git_commit)
    } else {
        git_commit
    };

    println!("cargo:rustc-env=GIT_COMMIT={}", git_commit_with_dirty);

    // Capture rustc version for version info
    let rustc_version = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .and_then(|s| {
            // Extract just the version number (e.g., "1.75.0" from "rustc 1.75.0 (82e1608df 2023-12-21)")
            s.split_whitespace()
                .nth(1)
                .map(|v| v.to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=RUSTC_VERSION={}", rustc_version);

    // Re-run build script if git HEAD changes
    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=../.git/refs");
}
