use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use filetime::{FileTime, set_file_times};
use russh::keys::ssh_key;
use russh_sftp::client::fs::{DirEntry, Metadata};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::sleep;
use tracing::{error, info};

use russh::client::{Config as ClientConfig, Handler};
use russh_sftp::client::SftpSession;

use crate::devices;
use crate::streams::quic::open_quic_io;
use crate::streams::stream_type::StreamType;
use crate::util::shutdown::SHUTDOWN;
use crate::{auth::AuthManager, config::Config};

#[derive(Debug, Clone)]
pub enum LocalOrRemotePath {
    Local(PathBuf),
    Remote { device: String, path: String },
}

impl LocalOrRemotePath {
    pub fn parse(s: &str) -> Self {
        if let Some((dev, path)) = s.split_once(":") {
            LocalOrRemotePath::Remote {
                device: dev.to_string(),
                path: path.to_string(),
            }
        } else {
            LocalOrRemotePath::Local(PathBuf::from(s))
        }
    }

    pub fn from_path(base: &LocalOrRemotePath, full: &Path) -> Self {
        match base {
            LocalOrRemotePath::Local(_) => LocalOrRemotePath::Local(full.to_path_buf()),

            LocalOrRemotePath::Remote { device, .. } => LocalOrRemotePath::Remote {
                device: device.clone(),
                path: full.to_string_lossy().into_owned(),
            },
        }
    }
}

pub async fn open_sftp_session(device_name: &str) -> anyhow::Result<SftpSession> {
    let cfg = Config::load()?;
    let token = AuthManager::get_cli_token().await?;
    let resolved = devices::resolve_device_short_id_cached(&device_name).await?;

    // open raw tunnel through HTTPS upgrade
    let stream_type = StreamType::Ssh {
        token: token.to_string(),
    };
    let (_, io) = open_quic_io(
        &resolved.host,
        &token,
        &resolved.short_id,
        stream_type,
        cfg.trust_invalid_server_cert,
    )
    .await
    .context("Failed to connect to RAW metrics stream")?;

    // minimal ssh client config
    let mut config = ClientConfig::default();

    config.inactivity_timeout = Some(std::time::Duration::from_secs(10));
    config.window_size = 4 * 1024 * 1024; // OK > 1MB
    config.channel_buffer_size = 4 * 1024 * 1024; // OK > 1MB
    config.maximum_packet_size = 65535; // MUST stay <= 65535
    let config = Arc::new(config);
    let sh = DummyHandler {};

    // connect SSH over the raw IO
    let mut session = russh::client::connect_stream(config, io, sh).await?;

    // authenticate with "none" (your SSH server already trusts RBAC via tunnel)
    session.authenticate_none("m87").await?;

    let channel = session.channel_open_session().await.unwrap();
    channel.request_subsystem(true, "sftp").await.unwrap();
    let sftp = SftpSession::new(channel.into_stream()).await.unwrap();
    Ok(sftp)
}

struct DummyHandler;

impl Handler for DummyHandler {
    type Error = anyhow::Error;

    fn check_server_key(
        &mut self,
        _server_public_key: &ssh_key::PublicKey,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send {
        async { Ok(true) }
    }
}

pub async fn list(path: &str) -> Result<Vec<DirEntry>> {
    let path = LocalOrRemotePath::parse(path);

    let (device_name, remote_path) = match path {
        LocalOrRemotePath::Remote { device, path } => (device, path),
        _ => bail!("path must be <device>:<path>"),
    };

    let sftp = open_sftp_session(&device_name).await?;

    let items = sftp.read_dir(&remote_path).await?;
    let files = items.into_iter().map(|file| file).collect();

    Ok(files)
}

pub async fn copy(src: &str, dst: &str) -> Result<()> {
    let src_path = LocalOrRemotePath::parse(src);
    let dst_path = LocalOrRemotePath::parse(dst);

    let mut sftp_src = maybe_open_sftp(&src_path).await?;
    let mut sftp_dst = maybe_open_sftp(&dst_path).await?;

    copy_file(&src_path, &dst_path, &mut sftp_src, &mut sftp_dst).await
}

async fn copy_file(
    src: &LocalOrRemotePath,
    dst: &LocalOrRemotePath,
    sftp_src: &mut Option<SftpSession>,
    sftp_dst: &mut Option<SftpSession>,
) -> Result<()> {
    match (src, dst) {
        (LocalOrRemotePath::Local(src), LocalOrRemotePath::Remote { path: dst, .. }) => {
            let mut local_file = tokio::fs::File::open(src)
                .await
                .with_context(|| format!("open local file {src:?}"))?;

            let meta = local_file.metadata().await?;
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);

            let sftp = sftp_dst.as_ref().unwrap();
            if let Some(parent) = Path::new(dst).parent().and_then(|p| p.to_str()) {
                sftp.create_dir(parent).await.ok();
            }

            let mut remote_file = sftp.create(dst.clone()).await?;

            copy_chunked(&mut local_file, &mut remote_file).await?;
            sync_remote_mtime(sftp, dst, mtime).await;
        }

        (LocalOrRemotePath::Remote { path: src, .. }, LocalOrRemotePath::Local(dst)) => {
            let sftp = sftp_src.as_ref().unwrap();
            let remote_meta = sftp.metadata(src.clone()).await?;
            let mut remote_file = sftp.open(src.clone()).await?;

            if let Some(parent) = dst.parent() {
                tokio::fs::create_dir_all(parent).await.ok();
            }

            let mut local_file = tokio::fs::File::create(dst)
                .await
                .with_context(|| format!("create local file {dst:?}"))?;

            copy_chunked(&mut remote_file, &mut local_file).await?;
            sync_local_mtime(dst, &remote_meta).await;
        }

        (
            LocalOrRemotePath::Remote { path: src, .. },
            LocalOrRemotePath::Remote { path: dst, .. },
        ) => {
            let from = sftp_src.as_ref().unwrap();
            let to = sftp_dst.as_ref().unwrap();

            let remote_meta = from.metadata(src.clone()).await?;
            let mtime = remote_meta.mtime.unwrap_or(0) as u64;

            let mut from_file = from.open(src.clone()).await?;
            if let Some(parent) = Path::new(dst).parent().and_then(|p| p.to_str()) {
                to.create_dir(parent).await.ok();
            }

            let mut to_file = to.create(dst.clone()).await?;
            copy_chunked(&mut from_file, &mut to_file).await?;
            sync_remote_mtime(to, dst, mtime).await;
        }

        (LocalOrRemotePath::Local(src), LocalOrRemotePath::Local(dst)) => {
            if let Some(parent) = dst.parent() {
                tokio::fs::create_dir_all(parent).await.ok();
            }

            let mut src_file = tokio::fs::File::open(src).await?;
            let mut dst_file = tokio::fs::File::create(dst).await?;
            copy_chunked(&mut src_file, &mut dst_file).await?;
        }
    }

    Ok(())
}

async fn delete_file(full: &LocalOrRemotePath, sftp: &mut Option<SftpSession>) -> Result<()> {
    match full {
        LocalOrRemotePath::Local(p) => {
            if p.is_file() {
                tokio::fs::remove_file(p)
                    .await
                    .with_context(|| format!("remove local file {p:?}"))?;
            }
        }

        LocalOrRemotePath::Remote { path, .. } => {
            let sftp = sftp
                .as_ref()
                .context("SFTP session required for remote delete")?;
            sftp.remove_file(path.clone())
                .await
                .with_context(|| format!("remove remote file {path}"))?;
        }
    }

    Ok(())
}

async fn maybe_open_sftp(p: &LocalOrRemotePath) -> Result<Option<SftpSession>> {
    match p {
        LocalOrRemotePath::Local(_) => Ok(None),
        LocalOrRemotePath::Remote { device, .. } => {
            let sftp = open_sftp_session(device).await?;
            Ok(Some(sftp))
        }
    }
}

#[derive(Debug, Clone)]
struct FileInfo {
    /// Cheap fingerprint: "<size>:<mtime_secs>".
    fingerprint: String,
}

#[derive(Debug)]
struct FileTree {
    root: PathBuf,
    /// Relative path (using `/`) -> FileInfo
    files: HashMap<String, FileInfo>,
}

fn fingerprint(size: u64, mtime: Option<SystemTime>) -> String {
    let secs = mtime
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("{size}:{secs}")
}

/// Check if a relative path matches any exclusion pattern.
/// Patterns support simple glob matching:
/// - `*.log` matches files ending in .log
/// - `.git` matches exact name
/// - `node_modules` matches exact name
fn matches_exclude(rel_path: &str, excludes: &[String]) -> bool {
    if excludes.is_empty() {
        return false;
    }

    // Get the filename and path components
    let path = Path::new(rel_path);
    let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

    for pattern in excludes {
        // Check if any path component matches the pattern exactly
        for component in path.components() {
            if let std::path::Component::Normal(name) = component {
                if let Some(name_str) = name.to_str() {
                    if name_str == pattern {
                        return true;
                    }
                }
            }
        }

        // Simple glob matching for patterns with *
        if pattern.contains('*') {
            let parts: Vec<&str> = pattern.split('*').collect();
            if parts.len() == 2 {
                let (prefix, suffix) = (parts[0], parts[1]);
                if filename.starts_with(prefix) && filename.ends_with(suffix) {
                    return true;
                }
            }
        }
    }

    false
}

async fn read_local_tree(root: &Path, excludes: &[String]) -> Result<FileTree> {
    let root = root.to_path_buf();
    let mut files = HashMap::new();
    let mut stack = vec![root.clone()];

    while let Some(dir) = stack.pop() {
        let mut rd = tokio::fs::read_dir(&dir).await?;

        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            let rel = path
                .strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .to_string();

            // Skip excluded paths
            if matches_exclude(&rel, excludes) {
                continue;
            }

            let meta = entry.metadata().await?;
            if meta.is_dir() {
                stack.push(path);
            } else if meta.is_file() {
                let fp = fingerprint(meta.len(), meta.modified().ok());
                files.insert(rel, FileInfo { fingerprint: fp });
            }
        }
    }

    Ok(FileTree { root, files })
}

async fn read_remote_tree(sftp: &SftpSession, root: &str, excludes: &[String]) -> Result<FileTree> {
    let mut files = HashMap::new();
    let root_path = PathBuf::from(root);

    // Stack holds (base, rel)
    let mut stack = vec![(root.to_string(), "".to_string())];

    while let Some((base, rel)) = stack.pop() {
        // Skip excluded paths
        if !rel.is_empty() && matches_exclude(&rel, excludes) {
            continue;
        }

        // Construct full path
        let path = if rel.is_empty() {
            base.clone()
        } else {
            format!("{base}/{rel}")
        };

        // Try reading as directory
        let mut dir = match sftp.read_dir(path.clone()).await {
            Ok(d) => d,
            Err(_) => {
                // Not a directory → treat as file
                let meta = sftp.metadata(path.clone()).await?;
                if !meta.is_dir() {
                    let fp = fingerprint(meta.len(), meta.modified().ok());
                    files.insert(rel.clone(), FileInfo { fingerprint: fp });
                }
                continue;
            }
        };

        while let Some(entry) = dir.next() {
            let name = entry.file_name();
            if name == "." || name == ".." {
                continue;
            }

            let child_rel = if rel.is_empty() {
                name.clone()
            } else {
                format!("{rel}/{name}")
            };

            // Skip excluded paths
            if matches_exclude(&child_rel, excludes) {
                continue;
            }

            let meta = entry.metadata();

            if meta.is_dir() {
                stack.push((base.clone(), child_rel));
            } else {
                let fp = fingerprint(meta.len(), meta.modified().ok());
                files.insert(child_rel, FileInfo { fingerprint: fp });
            }
        }
    }

    Ok(FileTree {
        root: root_path,
        files,
    })
}

pub async fn sync(
    src: &str,
    dst: &str,
    delete: bool,
    dry_run: bool,
    excludes: &[String],
) -> Result<()> {
    let src_path = LocalOrRemotePath::parse(src);
    let dst_path = LocalOrRemotePath::parse(dst);

    let mut sftp_src = maybe_open_sftp(&src_path).await?;
    let mut sftp_dst = maybe_open_sftp(&dst_path).await?;
    tracing::info!("[done] Connected");

    let src_tree = match &src_path {
        LocalOrRemotePath::Local(p) => read_local_tree(p, excludes).await?,
        LocalOrRemotePath::Remote { path, .. } => {
            let sftp = sftp_src
                .as_ref()
                .context("SFTP src required for remote sync")?;
            read_remote_tree(sftp, path, excludes).await?
        }
    };

    let dst_tree = match &dst_path {
        LocalOrRemotePath::Local(p) => {
            if !p.exists() {
                if !dry_run {
                    tokio::fs::create_dir_all(p).await?;
                }
                FileTree {
                    root: p.clone(),
                    files: HashMap::new(),
                }
            } else {
                read_local_tree(p, excludes).await?
            }
        }
        LocalOrRemotePath::Remote { path, .. } => {
            let sftp = sftp_dst
                .as_ref()
                .context("SFTP dst required for remote sync")?;
            match read_remote_tree(sftp, path, excludes).await {
                Ok(tree) => tree,
                Err(_) => {
                    // Destination doesn't exist - create it and treat as empty
                    if !dry_run {
                        sftp.create_dir(path.clone()).await.ok();
                    }
                    FileTree {
                        root: PathBuf::from(path),
                        files: HashMap::new(),
                    }
                }
            }
        }
    };

    // Copy missing/changed
    for (rel, src_info) in &src_tree.files {
        match dst_tree.files.get(rel) {
            Some(dst_info) if dst_info.fingerprint == src_info.fingerprint => {
                // unchanged
            }
            _ => {
                let src_full = src_tree.root.join(rel);
                let dst_full = dst_tree.root.join(rel);

                if dry_run {
                    info!("[dry-run] would upload {}", rel);
                } else {
                    info!("uploading {}", rel);
                    copy_file(
                        &LocalOrRemotePath::from_path(&src_path, &src_full),
                        &LocalOrRemotePath::from_path(&dst_path, &dst_full),
                        &mut sftp_src,
                        &mut sftp_dst,
                    )
                    .await?;
                }
            }
        }
    }

    // Delete extra files on dst
    if delete {
        let src_keys: HashSet<_> = src_tree.files.keys().cloned().collect();

        for (rel, _) in &dst_tree.files {
            if !src_keys.contains(rel) {
                let dst_full = dst_tree.root.join(rel);

                if dry_run {
                    info!("[dry-run] would delete {}", rel);
                } else {
                    info!("deleting {}", rel);
                    delete_file(
                        &LocalOrRemotePath::from_path(&dst_path, &dst_full),
                        &mut sftp_dst,
                    )
                    .await?;
                }
            }
        }
    }

    Ok(())
}

pub async fn watch_sync(src: &str, dst: &str, delete: bool, excludes: &[String]) -> Result<()> {
    info!("Starting periodic watch-sync…");

    // Initial run (never dry-run for watch mode)
    sync(src, dst, delete, false, excludes).await?;

    let interval = Duration::from_secs(2);

    loop {
        tokio::select! {
            _ = sleep(interval) => {
                if let Err(e) = sync(src, dst, delete, false, excludes).await {
                    error!("sync failed: {e:#}");
                }
            }

            _ = SHUTDOWN.cancelled() => {
                info!("Shutdown requested — closing SSH tunnel");
                return Ok(());
            }
        }
    }
}

async fn sync_remote_mtime(sftp: &SftpSession, remote_path: &str, src_mtime: u64) {
    let mut attrs = Metadata::default();
    attrs.mtime = Some(src_mtime as u32);
    let _ = sftp.set_metadata(remote_path.to_string(), attrs).await;
}

// Helper: sync local mtime to match remote metadata
async fn sync_local_mtime(local_path: &Path, remote_meta: &Metadata) {
    if let Some(mtime) = remote_meta.mtime {
        let ft = FileTime::from_unix_time(mtime as i64, 0);
        let p = local_path.to_owned();

        let _ = tokio::task::spawn_blocking(move || set_file_times(&p, FileTime::now(), ft)).await;
    }
}

async fn copy_chunked<R, W>(mut reader: R, mut writer: W) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut buf = vec![0u8; 1024 * 1024]; // 1 MB chunks
    loop {
        tokio::select! {
            _ = SHUTDOWN.cancelled() => {
                return Err(anyhow::anyhow!("aborted"));
            }

            n = reader.read(&mut buf) => {
                let n = n?;
                if n == 0 {
                    break;
                }

                writer.write_all(&buf[..n]).await?;
            }
        }
    }

    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- LocalOrRemotePath::parse tests ---

    #[test]
    fn test_local_or_remote_path_parse_local() {
        let path = LocalOrRemotePath::parse("foo/bar/baz");
        match path {
            LocalOrRemotePath::Local(p) => {
                assert_eq!(p, PathBuf::from("foo/bar/baz"));
            }
            _ => panic!("Expected Local variant"),
        }
    }

    #[test]
    fn test_local_or_remote_path_parse_remote() {
        let path = LocalOrRemotePath::parse("mydevice:/home/user/file.txt");
        match path {
            LocalOrRemotePath::Remote { device, path } => {
                assert_eq!(device, "mydevice");
                assert_eq!(path, "/home/user/file.txt");
            }
            _ => panic!("Expected Remote variant"),
        }
    }

    #[test]
    fn test_local_or_remote_path_parse_empty() {
        let path = LocalOrRemotePath::parse("");
        match path {
            LocalOrRemotePath::Local(p) => {
                assert_eq!(p, PathBuf::from(""));
            }
            _ => panic!("Expected Local variant"),
        }
    }

    #[test]
    fn test_local_or_remote_path_parse_remote_empty_path() {
        let path = LocalOrRemotePath::parse("device:");
        match path {
            LocalOrRemotePath::Remote { device, path } => {
                assert_eq!(device, "device");
                assert_eq!(path, "");
            }
            _ => panic!("Expected Remote variant"),
        }
    }

    // --- LocalOrRemotePath::from_path tests ---

    #[test]
    fn test_local_or_remote_path_from_path_local() {
        let base = LocalOrRemotePath::Local(PathBuf::from("/base"));
        let full = Path::new("/base/subdir/file.txt");

        let result = LocalOrRemotePath::from_path(&base, full);
        match result {
            LocalOrRemotePath::Local(p) => {
                assert_eq!(p, PathBuf::from("/base/subdir/file.txt"));
            }
            _ => panic!("Expected Local variant"),
        }
    }

    #[test]
    fn test_local_or_remote_path_from_path_remote() {
        let base = LocalOrRemotePath::Remote {
            device: "mydevice".to_string(),
            path: "/base".to_string(),
        };
        let full = Path::new("/base/subdir/file.txt");

        let result = LocalOrRemotePath::from_path(&base, full);
        match result {
            LocalOrRemotePath::Remote { device, path } => {
                assert_eq!(device, "mydevice");
                assert_eq!(path, "/base/subdir/file.txt");
            }
            _ => panic!("Expected Remote variant"),
        }
    }

    // --- fingerprint tests ---

    #[test]
    fn test_fingerprint_basic() {
        let mtime = Some(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1700000000));
        let fp = fingerprint(12345, mtime);
        assert_eq!(fp, "12345:1700000000");
    }

    #[test]
    fn test_fingerprint_no_mtime() {
        let fp = fingerprint(999, None);
        assert_eq!(fp, "999:0");
    }

    #[test]
    fn test_fingerprint_zero_size() {
        let mtime = Some(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(100));
        let fp = fingerprint(0, mtime);
        assert_eq!(fp, "0:100");
    }

    // --- matches_exclude tests ---

    #[test]
    fn test_matches_exclude_exact_name() {
        let excludes = vec!["node_modules".to_string()];
        assert!(matches_exclude("node_modules", &excludes));
        assert!(matches_exclude("src/node_modules/foo", &excludes));
        assert!(!matches_exclude("node_modules_backup", &excludes));
    }

    #[test]
    fn test_matches_exclude_glob_suffix() {
        let excludes = vec!["*.log".to_string()];
        assert!(matches_exclude("app.log", &excludes));
        assert!(matches_exclude("debug.log", &excludes));
        assert!(!matches_exclude("log.txt", &excludes));
    }

    #[test]
    fn test_matches_exclude_glob_prefix() {
        let excludes = vec!["test_*".to_string()];
        assert!(matches_exclude("test_file.rs", &excludes));
        assert!(matches_exclude("test_", &excludes));
        assert!(!matches_exclude("my_test", &excludes));
    }

    #[test]
    fn test_matches_exclude_nested_component() {
        let excludes = vec!["build".to_string()];
        assert!(matches_exclude("build", &excludes));
        assert!(matches_exclude("src/build/output", &excludes));
        assert!(matches_exclude("project/build", &excludes));
    }

    #[test]
    fn test_matches_exclude_empty_list() {
        let excludes: Vec<String> = vec![];
        assert!(!matches_exclude("anything", &excludes));
        assert!(!matches_exclude("node_modules", &excludes));
    }

    #[test]
    fn test_matches_exclude_multiple_patterns() {
        let excludes = vec![
            "node_modules".to_string(),
            "*.log".to_string(),
            ".git".to_string(),
        ];
        assert!(matches_exclude("node_modules", &excludes));
        assert!(matches_exclude("app.log", &excludes));
        assert!(matches_exclude(".git", &excludes));
        assert!(!matches_exclude("src/main.rs", &excludes));
    }
}
