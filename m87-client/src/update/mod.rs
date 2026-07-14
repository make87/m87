use anyhow::{Context, Result, anyhow};
use futures::StreamExt;
use self_update::cargo_crate_version;
use self_update::version::bump_is_greater;
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tracing::{error, info, warn};

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

    // Resumable download — the fleet runs on ~10 KB/s LTE where a one-shot pull
    // reliably times out. Stream with HTTP Range resume so drops/stalls just
    // continue from the bytes already on disk.
    let dl_client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .build()?;
    download_resumable(
        &dl_client,
        &asset.browser_download_url,
        &download_path,
        interactive,
    )
    .await
    .with_context(|| format!("downloading {}", asset.name))?;

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

/// Total expected size of a download response: the `Content-Range` total for a
/// `206 Partial Content`, else `Content-Length` for a full `200`.
fn response_total(resp: &reqwest::Response) -> Option<u64> {
    if resp.status() == reqwest::StatusCode::PARTIAL_CONTENT {
        resp.headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.rsplit('/').next())
            .and_then(|s| s.trim().parse::<u64>().ok())
    } else {
        resp.content_length()
    }
}

async fn open_output(dest: &Path, append: bool) -> std::io::Result<tokio::fs::File> {
    tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .truncate(!append)
        .open(dest)
        .await
}

/// Download `url` to `dest`, resuming across drops/stalls via HTTP `Range`.
///
/// The device fleet runs on ~10 KB/s LTE where a one-shot pull reliably times
/// out, so: no overall request timeout (a multi-MB file legitimately takes
/// minutes), but a per-chunk *stall* timeout aborts a frozen connection so we
/// reconnect and continue from the bytes already on disk. Each attempt
/// re-requests the (stable) GitHub URL, which issues a fresh signed CDN
/// redirect, so resuming keeps working even after a previous signed URL expires.
async fn download_resumable(
    client: &reqwest::Client,
    url: &str,
    dest: &Path,
    interactive: bool,
) -> Result<()> {
    const MAX_ATTEMPTS: u32 = 300;
    const STALL: Duration = Duration::from_secs(60);
    const RETRY_DELAY: Duration = Duration::from_secs(5);

    let mut total: Option<u64> = None;
    let mut attempt: u32 = 0;

    loop {
        let have = tokio::fs::metadata(dest).await.map(|m| m.len()).unwrap_or(0);
        if let Some(t) = total {
            if have >= t {
                return Ok(()); // complete
            }
        }

        attempt += 1;
        if attempt > MAX_ATTEMPTS {
            return Err(anyhow!(
                "download did not complete after {MAX_ATTEMPTS} attempts ({have} bytes)"
            ));
        }

        // Ask for the remaining bytes (whole file on the first attempt).
        let mut req = client
            .get(url)
            .header(reqwest::header::ACCEPT, "application/octet-stream");
        if have > 0 {
            req = req.header(reqwest::header::RANGE, format!("bytes={have}-"));
        }

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                warn!("update download: connect failed at {have} bytes: {e}");
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            }
        };
        // 416 means we already have the whole file.
        if have > 0 && resp.status() == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
            return Ok(());
        }
        let resp = match resp.error_for_status() {
            Ok(r) => r,
            Err(e) => {
                warn!("update download: http error at {have} bytes: {e}");
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            }
        };

        // Append only when the server honored our Range (206); if it ignored it
        // (200 with have>0) we must restart from 0 (open_output truncates).
        let resuming = resp.status() == reqwest::StatusCode::PARTIAL_CONTENT;
        let append = have > 0 && resuming;
        if total.is_none() {
            total = response_total(&resp);
        }

        let mut file = open_output(dest, append)
            .await
            .context("opening update download file")?;

        let mut body = resp.bytes_stream();
        let mut got = 0u64;
        let mut clean_end = false;
        loop {
            match tokio::time::timeout(STALL, body.next()).await {
                Ok(Some(Ok(chunk))) => {
                    if let Err(e) = file.write_all(&chunk).await {
                        warn!("update download: write error: {e}");
                        break;
                    }
                    got += chunk.len() as u64;
                }
                Ok(Some(Err(e))) => {
                    warn!("update download: stream error at {}: {e}", have + got);
                    break;
                }
                Ok(None) => {
                    clean_end = true;
                    break;
                }
                Err(_) => {
                    warn!("update download: stalled at {} bytes, resuming", have + got);
                    break;
                }
            }
        }
        let _ = file.flush().await;

        if interactive {
            match total {
                Some(t) => println!("  {} / {} bytes", have + got, t),
                None => println!("  {} bytes", have + got),
            }
        }

        // A clean body end on an open-ended range means we reached EOF; verify
        // against the known total when we have one.
        if clean_end && total.map_or(true, |t| have + got >= t) {
            return Ok(());
        }
        if got == 0 {
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }
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

    async fn read_http_request(s: &mut tokio::net::TcpStream) -> String {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        loop {
            let n = s.read(&mut tmp).await.unwrap();
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        String::from_utf8_lossy(&buf).to_string()
    }

    /// The whole point of the rewrite: a dropped connection mid-download must be
    /// resumed via `Range`, not restarted. Server sends half the body then drops
    /// on request 1, and serves the remainder as `206` on the `Range` retry.
    #[tokio::test]
    async fn download_resumable_resumes_after_drop() {
        use tokio::net::TcpListener;

        let payload: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
        let total = payload.len();
        let half = total / 2;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv_payload = payload.clone();

        let server = tokio::spawn(async move {
            // Request 1: send half the body under a full Content-Length, then drop.
            let (mut s, _) = listener.accept().await.unwrap();
            read_http_request(&mut s).await;
            let hdr = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nAccept-Ranges: bytes\r\n\r\n"
            );
            s.write_all(hdr.as_bytes()).await.unwrap();
            s.write_all(&srv_payload[..half]).await.unwrap();
            s.flush().await.unwrap();
            drop(s); // connection drops mid-body

            // Request 2: must be a Range resume for the remainder → serve 206.
            let (mut s, _) = listener.accept().await.unwrap();
            let req = read_http_request(&mut s).await;
            assert!(
                req.to_lowercase().contains(&format!("range: bytes={half}-")),
                "expected a resume Range request, got:\n{req}"
            );
            let rem = total - half;
            let hdr = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {rem}\r\nContent-Range: bytes {half}-{}/{total}\r\n\r\n",
                total - 1
            );
            s.write_all(hdr.as_bytes()).await.unwrap();
            s.write_all(&srv_payload[half..]).await.unwrap();
            s.flush().await.unwrap();
        });

        let dir = self_update::TempDir::new().unwrap();
        let dest = dir.path().join("m87");
        let client = reqwest::Client::builder().build().unwrap();
        download_resumable(&client, &format!("http://{addr}/bin"), &dest, false)
            .await
            .expect("resumable download should complete across the drop");

        assert_eq!(std::fs::read(&dest).unwrap(), payload);
        server.await.unwrap();
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

        let mut enc = GzEncoder::new(std::fs::File::create(&gz).unwrap(), Compression::best());
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
