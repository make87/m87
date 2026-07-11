use anyhow::{Result, anyhow};
use self_update::cargo_crate_version;
use self_update::version::bump_is_greater;
use serde::Deserialize;
use std::fs::File;
use tracing::{error, info};

const GITHUB_LATEST_RELEASE_URL: &str = "https://api.github.com/repos/make87/m87/releases/latest";

fn arch_bin_name() -> &'static str {
    #[cfg(target_arch = "x86_64")]
    {
        "m87-x86_64-unknown-linux-musl"
    }

    #[cfg(target_arch = "aarch64")]
    {
        "m87-aarch64-unknown-linux-musl"
    }

    // #[cfg(target_arch = "riscv64")]
    // {
    //     "m87-riscv64gc-unknown-linux-musl"
    // }
}

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: String,
    assets: Vec<GitHubAsset>,
}

#[derive(Debug, Deserialize)]
struct GitHubAsset {
    name: String,
    browser_download_url: String,
}

pub async fn update(interactive: bool) -> Result<bool> {
    if interactive {
        println!("Checking for updates...");
    }
    let current_version = cargo_crate_version!();
    let asset_name = arch_bin_name();

    // Fetch the "latest" release from GitHub (the one explicitly tagged as latest)
    let client = reqwest::Client::new();
    let release: GitHubRelease = client
        .get(GITHUB_LATEST_RELEASE_URL)
        .header("User-Agent", "m87-client")
        .header("Accept", "application/vnd.github+json")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let new_version = release.tag_name.trim_start_matches('v');

    // Check if update is needed
    if !bump_is_greater(current_version, new_version)? {
        if interactive {
            println!("You are already running the latest version (v{})", current_version);
        }
        return Ok(false);
    }

    // Prefer the gzip-compressed asset (`<name>.gz`, ~2.5x smaller — matters on
    // LTE), falling back to the raw binary for releases that only publish it.
    // This is intentionally additive: releases keep publishing the raw asset so
    // older clients still work, and this client works against either.
    let gz_name = format!("{asset_name}.gz");
    let (asset, is_gz) = release
        .assets
        .iter()
        .find(|a| a.name == gz_name)
        .map(|a| (a, true))
        .or_else(|| release.assets.iter().find(|a| a.name == asset_name).map(|a| (a, false)))
        .ok_or_else(|| {
            anyhow!("Neither '{}' nor '{}' found in release", gz_name, asset_name)
        })?;

    if interactive {
        println!("New release found: v{} → v{}", current_version, new_version);
        println!("Downloading {}...", asset.name);
    }

    // Create temp directory for download
    let tmp_dir = self_update::TempDir::new()?;
    let download_path = tmp_dir.path().join(&asset.name);

    let tmp_file = File::create(&download_path)?;
    self_update::Download::from_url(&asset.browser_download_url)
        .set_header(reqwest::header::ACCEPT, "application/octet-stream".parse()?)
        .show_progress(interactive)
        .download_to(tmp_file)?;

    // If compressed, let self_update gunzip it (ArchiveKind::Plain + Gz — a bare
    // single-file .gz, not a tar). It writes the decompressed file into the dir
    // with the `.gz` extension stripped, i.e. `<asset_name>`.
    let bin_path = if is_gz {
        if interactive {
            println!("Decompressing...");
        }
        self_update::Extract::from_source(&download_path)
            .archive(self_update::ArchiveKind::Plain(Some(self_update::Compression::Gz)))
            .extract_into(tmp_dir.path())?;
        tmp_dir.path().join(asset_name)
    } else {
        download_path
    };

    // Make executable on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin_path, std::fs::Permissions::from_mode(0o755))?;
    }

    // Replace the current binary
    if interactive {
        println!("Replacing binary...");
    }
    self_update::self_replace::self_replace(&bin_path)?;

    if interactive {
        println!("Updated from v{} → v{}", current_version, new_version);
    }
    Ok(true)
}

/// Helper for daemon use — silently apply and exit if updated.
pub async fn daemon_check_and_update() -> Result<()> {
    match update(false).await {
        Ok(true) => {
            info!("Device updated; exiting for restart via systemd");
            std::process::exit(1); // throw error code on exit so systemd restarts "on-failure"
        }
        Ok(false) => {}
        Err(e) => error!("Update check failed: {:?}", e),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arch_bin_name_format() {
        let name = arch_bin_name();
        assert!(name.starts_with("m87-"));
        assert!(name.contains("-unknown-linux-musl"));
    }

    #[test]
    fn test_arch_bin_name_known_arch() {
        let name = arch_bin_name();
        let known_archs = [
            "m87-x86_64-unknown-linux-musl",
            "m87-aarch64-unknown-linux-musl",
            "m87-riscv64gc-unknown-linux-musl",
        ];
        assert!(
            known_archs.contains(&name),
            "Unknown architecture: {}",
            name
        );
    }

    #[test]
    fn test_github_release_deserialization() {
        let json = r#"{
            "tag_name": "v1.2.3",
            "assets": [
                {"name": "m87-x86_64-unknown-linux-musl", "browser_download_url": "https://example.com/download1"},
                {"name": "m87-aarch64-unknown-linux-musl", "browser_download_url": "https://example.com/download2"}
            ]
        }"#;

        let release: GitHubRelease = serde_json::from_str(json).unwrap();
        assert_eq!(release.tag_name, "v1.2.3");
        assert_eq!(release.assets.len(), 2);
        assert_eq!(release.assets[0].name, "m87-x86_64-unknown-linux-musl");
        assert!(
            release.assets[0]
                .browser_download_url
                .starts_with("https://")
        );
    }

    #[test]
    fn test_github_release_deserialization_empty_assets() {
        let json = r#"{"tag_name": "v0.0.1", "assets": []}"#;
        let release: GitHubRelease = serde_json::from_str(json).unwrap();
        assert_eq!(release.tag_name, "v0.0.1");
        assert!(release.assets.is_empty());
    }

    // Verifies our use of self_update's gzip extraction: a bare `<name>.gz`
    // extracts to `<name>` (extension stripped) with the original bytes. Guards
    // the ArchiveKind/output-path assumptions the updater relies on.
    #[test]
    fn test_self_update_extracts_plain_gz() {
        use flate2::{write::GzEncoder, Compression};
        use std::io::Write;

        let dir = self_update::TempDir::new().unwrap();
        let gz = dir.path().join("m87-x86_64-unknown-linux-musl.gz");
        let payload = b"\x7fELF not-really-a-binary-but-enough-bytes-to-round-trip";

        let mut enc = GzEncoder::new(File::create(&gz).unwrap(), Compression::best());
        enc.write_all(payload).unwrap();
        enc.finish().unwrap();

        self_update::Extract::from_source(&gz)
            .archive(self_update::ArchiveKind::Plain(Some(self_update::Compression::Gz)))
            .extract_into(dir.path())
            .unwrap();

        let out = dir.path().join("m87-x86_64-unknown-linux-musl");
        assert_eq!(std::fs::read(&out).unwrap(), payload);
    }

    #[test]
    fn test_version_strip_prefix() {
        let tag = "v1.2.3";
        let version = tag.trim_start_matches('v');
        assert_eq!(version, "1.2.3");

        let tag_no_v = "1.2.3";
        let version = tag_no_v.trim_start_matches('v');
        assert_eq!(version, "1.2.3");
    }

    // Integration test: actually fetches from GitHub API
    // Run with: cargo test --package m87-client -- --ignored test_fetch_latest_release
    #[tokio::test]
    #[ignore] // Ignored by default since it requires network
    async fn test_fetch_latest_release_from_github() {
        let client = reqwest::Client::new();
        let response = client
            .get(GITHUB_LATEST_RELEASE_URL)
            .header("User-Agent", "m87-client-test")
            .header("Accept", "application/vnd.github+json")
            .send()
            .await
            .expect("Failed to fetch release");

        assert!(
            response.status().is_success(),
            "GitHub API returned error: {}",
            response.status()
        );

        let release: GitHubRelease = response.json().await.expect("Failed to parse release JSON");

        // Verify we got a valid release
        assert!(
            release.tag_name.starts_with('v'),
            "Tag should start with 'v': {}",
            release.tag_name
        );
        assert!(!release.assets.is_empty(), "Release should have assets");

        // Check that our architecture's binary exists
        let asset_name = arch_bin_name();
        let our_asset = release.assets.iter().find(|a| a.name == asset_name);
        assert!(
            our_asset.is_some(),
            "Release should contain asset for current arch: {}",
            asset_name
        );
    }
}
