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

/// Directory used to stage an update, always a sibling of the running binary.
///
/// This must live on the SAME filesystem as the binary it replaces. On the
/// device fleet `/tmp` is a tmpfs (RAM) mounted on a different filesystem than
/// `~/.local/bin`, which has two consequences we must avoid:
///
///  * `self_replace` (and any `mv`) degrades from an atomic `rename()` to a
///    multi-megabyte copy. If the process is killed mid-copy the binary on disk
///    is left truncated, which crash-loops until systemd's start limit gives up
///    — a bricked device with no way back in.
///  * a tmpfs partial is lost on reboot, so a slow LTE download can never make
///    progress across the restarts it is most likely to hit.
///
/// Staging next to the target fixes both: the install is an atomic rename, and
/// the partial persists so the next run resumes instead of restarting.
fn update_work_dir(exe_path: &Path) -> Result<std::path::PathBuf> {
    let dir = exe_path
        .parent()
        .ok_or_else(|| anyhow!("executable has no parent directory"))?
        .join(".m87-update");
    Ok(dir)
}

/// Version-keyed name for a staged download, so a partial for one release is
/// never spliced onto a different one when the target moves mid-download.
fn staged_name(asset_name: &str, version: &str) -> String {
    format!("{asset_name}-{version}")
}

/// Delete staged files that do not belong to `keep_version`.
///
/// Bounds the staging dir on small SD cards and guarantees a stale partial from
/// an abandoned target can't be resumed into the wrong binary. Best-effort:
/// individual failures are ignored so cleanup never blocks an update.
fn clean_stale_staged(work_dir: &Path, asset_name: &str, keep_version: &str) -> usize {
    let keep = staged_name(asset_name, keep_version);
    let mut removed = 0;
    let Ok(entries) = std::fs::read_dir(work_dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // Only touch our own staging artifacts, and never the one in flight.
        if !name.starts_with(asset_name) {
            continue;
        }
        if name == keep || name == format!("{keep}.gz") {
            continue;
        }
        if std::fs::remove_file(entry.path()).is_ok() {
            removed += 1;
        }
    }
    removed
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

    // Stage NEXT TO the binary we are replacing (see `update_work_dir`): same
    // filesystem => atomic rename on install, and the partial survives a reboot
    // so a slow LTE download resumes instead of restarting from zero.
    let exe_path = crate::util::command::current_exe_path()?;
    let work_dir = update_work_dir(&exe_path)?;
    std::fs::create_dir_all(&work_dir)
        .with_context(|| format!("creating update staging dir {}", work_dir.display()))?;
    // Drop partials for other versions before adding another.
    clean_stale_staged(&work_dir, asset_name, new_version);

    let staged = staged_name(asset_name, new_version);
    let download_path = if is_gz {
        work_dir.join(format!("{staged}.gz"))
    } else {
        work_dir.join(&staged)
    };

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
    // with the `.gz` extension stripped, i.e. `<staged>`.
    let bin_path = if is_gz {
        if interactive {
            println!("Decompressing...");
        }
        self_update::Extract::from_source(&download_path)
            .archive(self_update::ArchiveKind::Plain(Some(self_update::Compression::Gz)))
            .extract_into(&work_dir)?;
        work_dir.join(&staged)
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

    // Installed successfully — drop the staging artifacts so the dir doesn't
    // accumulate release binaries on a small SD card. Best-effort: a failure
    // here must not turn a successful update into an error.
    let _ = std::fs::remove_file(&bin_path);
    let _ = std::fs::remove_file(work_dir.join(format!("{staged}.gz")));

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
    download_resumable_with(client, url, dest, interactive, Duration::from_secs(60)).await
}

/// Implementation of [`download_resumable`] with an injectable per-chunk stall
/// timeout (tests use a short one so the stall→resume path is fast to exercise).
async fn download_resumable_with(
    client: &reqwest::Client,
    url: &str,
    dest: &Path,
    interactive: bool,
    stall: Duration,
) -> Result<()> {
    const MAX_ATTEMPTS: u32 = 300;
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
            match tokio::time::timeout(stall, body.next()).await {
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

    // ── Update staging: same-filesystem + resumable ──────────────────────────
    //
    // Regression guard for the bricked-device failure mode: staging in /tmp
    // (a tmpfs on a DIFFERENT filesystem than ~/.local/bin on the fleet) turns
    // the install into a multi-MB copy over the live binary instead of an
    // atomic rename, and throws the partial away on every reboot.

    #[test]
    fn staging_dir_is_a_sibling_of_the_binary_it_replaces() {
        let exe = Path::new("/home/pi/.local/bin/m87");
        let work = update_work_dir(exe).unwrap();

        // Same parent directory => same filesystem => `self_replace` is an
        // atomic rename() and can never leave a truncated binary behind.
        assert_eq!(
            work.parent().unwrap(),
            exe.parent().unwrap(),
            "staging dir must sit beside the target binary, not in /tmp"
        );
        assert!(
            !work.starts_with("/tmp"),
            "staging must never land in tmpfs: {work:?}"
        );
    }

    #[test]
    fn staged_name_is_version_keyed() {
        // Version-keying is what stops a partial for one release being resumed
        // into a different one when the target moves mid-download.
        assert_ne!(
            staged_name("m87-aarch64-unknown-linux-musl", "0.8.7"),
            staged_name("m87-aarch64-unknown-linux-musl", "0.8.8"),
        );
    }

    #[test]
    fn stale_partials_for_other_versions_are_cleaned_but_current_is_kept() {
        let td = tempfile::TempDir::new().unwrap();
        let dir = td.path();
        let asset = "m87-aarch64-unknown-linux-musl";

        let keep = staged_name(asset, "0.8.7");
        std::fs::write(dir.join(format!("{keep}.gz")), b"in-flight partial").unwrap();
        std::fs::write(dir.join(&keep), b"extracted").unwrap();
        std::fs::write(dir.join(staged_name(asset, "0.8.5")), b"stale").unwrap();
        std::fs::write(dir.join(format!("{}.gz", staged_name(asset, "0.8.6"))), b"stale").unwrap();
        // Unrelated file must be left alone.
        std::fs::write(dir.join("m87-previous"), b"backup").unwrap();

        let removed = clean_stale_staged(dir, asset, "0.8.7");

        assert_eq!(removed, 2, "both stale-version artifacts must be removed");
        assert!(dir.join(format!("{keep}.gz")).exists(), "in-flight partial must survive");
        assert!(dir.join(&keep).exists(), "current extracted binary must survive");
        assert!(!dir.join(staged_name(asset, "0.8.5")).exists());
        assert!(dir.join("m87-previous").exists(), "unrelated files untouched");
    }

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

    fn parse_range_start(req: &str) -> Option<u64> {
        req.lines()
            .find(|l| l.to_lowercase().starts_with("range:"))
            .and_then(|l| l.split("bytes=").nth(1))
            .and_then(|s| s.split('-').next())
            .and_then(|s| s.trim().parse::<u64>().ok())
    }

    /// End-to-end "update to a new version over a bad connection": a flaky server
    /// STALLS on the first request (sends a third, then hangs), DROPS on the
    /// second (sends more, then closes mid-body), and only completes on the
    /// third. The download must resume through both failures, and the gzipped
    /// "new binary" must decompress to exactly the original bytes.
    #[tokio::test]
    async fn self_update_download_survives_flaky_connection() {
        use flate2::{write::GzEncoder, Compression};
        use std::io::Write;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use tokio::net::TcpListener;

        // The "new version" binary and its gzip release asset.
        let binary: Vec<u8> = (0..6000u32).map(|i| (i.wrapping_mul(31) % 253) as u8).collect();
        let mut enc = GzEncoder::new(Vec::new(), Compression::best());
        enc.write_all(&binary).unwrap();
        let gz = enc.finish().unwrap();
        let total = gz.len();
        let third = total / 3;
        let two_thirds = 2 * total / 3;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let gz_srv = Arc::new(gz);
        let hits = Arc::new(AtomicUsize::new(0));

        let server = tokio::spawn(async move {
            loop {
                let (mut s, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let n = hits.fetch_add(1, Ordering::SeqCst);
                let gz_srv = gz_srv.clone();
                tokio::spawn(async move {
                    let req = read_http_request(&mut s).await;
                    let start = parse_range_start(&req).unwrap_or(0) as usize;
                    let partial = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{}/{total}\r\n\r\n",
                        total - start,
                        total - 1
                    );
                    match n {
                        // STALL: first third, then hold the socket open (no more
                        // data, no close) until well past the client's stall.
                        0 => {
                            let hdr = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nAccept-Ranges: bytes\r\n\r\n"
                            );
                            let _ = s.write_all(hdr.as_bytes()).await;
                            let _ = s.write_all(&gz_srv[..third]).await;
                            let _ = s.flush().await;
                            tokio::time::sleep(Duration::from_secs(3)).await;
                        }
                        // DROP: some more, then close mid-body.
                        1 => {
                            let _ = s.write_all(partial.as_bytes()).await;
                            let _ = s.write_all(&gz_srv[start..two_thirds]).await;
                            let _ = s.flush().await;
                        }
                        // COMPLETE: the remainder.
                        _ => {
                            let _ = s.write_all(partial.as_bytes()).await;
                            let _ = s.write_all(&gz_srv[start..]).await;
                            let _ = s.flush().await;
                        }
                    }
                });
            }
        });

        let dir = self_update::TempDir::new().unwrap();
        let gz_path = dir.path().join("m87-x86_64-unknown-linux-musl.gz");
        let client = reqwest::Client::builder().build().unwrap();
        download_resumable_with(
            &client,
            &format!("http://{addr}/asset"),
            &gz_path,
            false,
            Duration::from_millis(300), // short stall so the stall→resume path is quick
        )
        .await
        .expect("download must complete despite a stall and a drop");

        // Decompress exactly as update() does and verify byte-for-byte integrity.
        self_update::Extract::from_source(&gz_path)
            .archive(self_update::ArchiveKind::Plain(Some(self_update::Compression::Gz)))
            .extract_into(dir.path())
            .unwrap();
        let out = dir.path().join("m87-x86_64-unknown-linux-musl");
        assert_eq!(
            std::fs::read(&out).unwrap(),
            binary,
            "decompressed new-version binary must match after a flaky download"
        );

        server.abort();
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
