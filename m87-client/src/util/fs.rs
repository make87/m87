// src/util/fs.rs – SFTP server implementation for m87 over russh-sftp

use std::{
    collections::HashMap,
    path::{Component, Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

use tokio::{
    fs,
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
    sync::Mutex,
};

use russh_sftp::protocol::{
    Attrs, Data, File, FileAttributes, Handle, Name, OpenFlags, Status, StatusCode,
    Version,
};

/// Global handle counter – we give each open file a unique string handle.
static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);

/// One open file on the server.
struct OpenFile {
    path: PathBuf,
    file: fs::File,
}

pub struct DirListing {
    pub idx: usize,
    pub entries: Vec<File>,
}
/// SFTP handler state – one instance per SSH connection.
pub struct M87SftpHandler {
    root: PathBuf,
    // handle -> OpenFile
    open_files: Arc<Mutex<HashMap<String, OpenFile>>>,
    // just to keep track of negotiated version if you ever care
    version: Option<u32>,
    dir_handles: Arc<Mutex<HashMap<String, DirListing>>>,
}

impl M87SftpHandler {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            open_files: Arc::new(Mutex::new(HashMap::new())),
            version: None,
            dir_handles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn next_handle() -> String {
        NEXT_HANDLE.fetch_add(1, Ordering::Relaxed).to_string()
    }

    /// Simple path sanitiser: strips leading `/`, normalises `.` / `..`,
    /// and joins under `self.root`. Prevents escaping via `..` but does
    /// not defend against symlink tricks (good enough for now).
    fn resolve_path(&self, path: &str) -> Result<PathBuf, StatusCode> {
        let mut clean = PathBuf::new();
        for comp in Path::new(path).components() {
            match comp {
                Component::RootDir => {}
                Component::CurDir => {}
                Component::ParentDir => {
                    clean.pop();
                }
                Component::Normal(seg) => {
                    clean.push(seg);
                }
                _ => {}
            }
        }

        let full = self.root.join(clean);
        if !full.starts_with(&self.root) {
            return Err(StatusCode::PermissionDenied);
        }
        Ok(full)
    }

    fn make_status_ok(&self, id: u32) -> Status {
        Status {
            id,
            status_code: StatusCode::Ok,
            error_message: "OK".to_string(),
            language_tag: "en-US".to_string(),
        }
    }

    fn make_status_err(&self, id: u32, code: StatusCode, msg: &str) -> Status {
        Status {
            id,
            status_code: code,
            error_message: msg.to_string(),
            language_tag: "en-US".to_string(),
        }
    }

    fn attrs_from_metadata(&self, id: u32, meta: &std::fs::Metadata) -> Attrs {
        // FileAttributes has a From<Metadata> impl (crate’s “simplification”),
        // so we just delegate to that.
        let fa = FileAttributes::from(meta);
        Attrs { id, attrs: fa }
    }
}

impl Default for M87SftpHandler {
    fn default() -> Self {
        Self::new(PathBuf::from("/"))
    }
}

impl russh_sftp::server::Handler for M87SftpHandler {
    type Error = StatusCode;

    fn unimplemented(&self) -> Self::Error {
        StatusCode::OpUnsupported
    }

    // -------------------------------------------------------------------------
    // Session init
    // -------------------------------------------------------------------------

    async fn init(
        &mut self,
        version: u32,
        extensions: HashMap<String, String>,
    ) -> Result<Version, Self::Error> {
        self.version = Some(version);
        tracing::debug!(?version, ?extensions, "SFTP init");
        Ok(Version::new())
    }

    // -------------------------------------------------------------------------
    // Open / close / read / write (core for IDE + sync)
    // -------------------------------------------------------------------------

    async fn open(
        &mut self,
        id: u32,
        filename: String,
        flags: OpenFlags,
        _attrs: FileAttributes,
    ) -> Result<Handle, Self::Error> {
        let path = self.resolve_path(&filename)?;
        println!("PWD = {}", std::env::current_dir().unwrap().display());
        println!(
            "Is /home/phillip/test2 visible? {}",
            Path::new("/home/phillip/test2").exists()
        );

        let mut open_options = fs::OpenOptions::new();

        // Translate OpenFlags (v3 is a bit weird; this is “good enough”).
        if flags.contains(OpenFlags::READ) {
            open_options.read(true);
        }
        if flags.contains(OpenFlags::WRITE) {
            open_options.write(true);
        }
        if flags.contains(OpenFlags::CREATE) {
            open_options.create(true);
        }
        if flags.contains(OpenFlags::TRUNCATE) {
            open_options.truncate(true);
        }

        let file = match open_options.open(&path).await {
            Ok(f) => f,
            Err(e) => {
                tracing::error!(?e, ?path, "SFTP open failed");
                return Err(StatusCode::Failure);
            }
        };

        let handle_id = Self::next_handle();
        let handle_str = handle_id.clone();

        {
            let mut map = self.open_files.lock().await;
            map.insert(
                handle_str.clone(),
                OpenFile {
                    path: path.clone(),
                    file,
                },
            );
        }

        Ok(Handle {
            id,
            handle: handle_str,
        })
    }

    async fn close(&mut self, id: u32, handle: String) -> Result<Status, Self::Error> {
        {
            let mut files = self.open_files.lock().await;
            if files.remove(&handle).is_some() {
                return Ok(self.make_status_ok(id));
            }
        }
        {
            let mut dirs = self.dir_handles.lock().await;
            if dirs.remove(&handle).is_some() {
                return Ok(self.make_status_ok(id));
            }
        }

        Ok(self.make_status_err(id, StatusCode::NoSuchFile, "invalid handle"))
    }

    async fn read(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        len: u32,
    ) -> Result<Data, Self::Error> {
        let mut map = self.open_files.lock().await;
        let of = match map.get_mut(&handle) {
            Some(of) => of,
            None => return Err(StatusCode::NoSuchFile),
        };

        if let Err(e) = of.file.seek(std::io::SeekFrom::Start(offset)).await {
            tracing::error!(?e, "SFTP read: seek failed");
            return Err(StatusCode::Failure);
        }

        let mut buf = vec![0u8; len as usize];
        let n = match of.file.read(&mut buf).await {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(?e, "SFTP read failed");
                return Err(StatusCode::Failure);
            }
        };

        buf.truncate(n);

        Ok(Data { id, data: buf })
    }

    async fn write(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<Status, Self::Error> {
        let mut map = self.open_files.lock().await;
        let of = match map.get_mut(&handle) {
            Some(of) => of,
            None => return Err(StatusCode::NoSuchFile),
        };

        if let Err(e) = of.file.seek(std::io::SeekFrom::Start(offset)).await {
            tracing::error!(?e, "SFTP write: seek failed");
            return Err(StatusCode::Failure);
        }

        if let Err(e) = of.file.write_all(&data).await {
            tracing::error!(?e, "SFTP write failed");
            return Err(StatusCode::Failure);
        }

        Ok(self.make_status_ok(id))
    }

    // -------------------------------------------------------------------------
    // Path ops: stat / lstat / fstat / realpath
    // -------------------------------------------------------------------------

    async fn stat(&mut self, id: u32, path: String) -> Result<Attrs, Self::Error> {
        let full = self.resolve_path(&path)?;
        // let meta = match fs::metadata(&full).await {
        //     Ok(m) => m,
        //     Err(_) => return Err(StatusCode::NoSuchFile),
        // };

        // NOTE: this is blocking metadata; we convert to std::fs::Metadata.
        let meta_std = match std::fs::metadata(&full) {
            Ok(m) => m,
            Err(_) => return Err(StatusCode::Failure),
        };

        Ok(self.attrs_from_metadata(id, &meta_std))
    }

    async fn lstat(&mut self, id: u32, path: String) -> Result<Attrs, Self::Error> {
        let full = self.resolve_path(&path)?;
        let meta_std = match std::fs::symlink_metadata(&full) {
            Ok(m) => m,
            Err(_) => return Err(StatusCode::NoSuchFile),
        };

        Ok(self.attrs_from_metadata(id, &meta_std))
    }

    async fn fstat(&mut self, id: u32, handle: String) -> Result<Attrs, Self::Error> {
        let map = self.open_files.lock().await;
        let of = match map.get(&handle) {
            Some(of) => of,
            None => return Err(StatusCode::NoSuchFile),
        };

        // fstat is defined on the handle itself; we use its metadata.
        let meta_std = match std::fs::metadata(&of.path) {
            Ok(m) => m,
            Err(_) => return Err(StatusCode::Failure),
        };

        Ok(self.attrs_from_metadata(id, &meta_std))
    }

    async fn realpath(&mut self, id: u32, path: String) -> Result<Name, Self::Error> {
        // For IDEs it’s enough to normalise and return a single entry.
        let full = self
            .resolve_path(&path)
            .map_err(|_| StatusCode::NoSuchFile)?;

        let display = full
            .strip_prefix(&self.root)
            .unwrap_or(&full)
            .to_string_lossy()
            .into_owned();
        let name = if display.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", display)
        };

        Ok(Name {
            id,
            files: vec![File::new(name, FileAttributes::default())],
        })
    }

    // -------------------------------------------------------------------------
    // Directory ops: opendir / readdir / mkdir / rmdir
    // -------------------------------------------------------------------------

    async fn opendir(&mut self, id: u32, path: String) -> Result<Handle, Self::Error> {
        let full = self.resolve_path(&path)?;
        let mut rd = match fs::read_dir(&full).await {
            Ok(rd) => rd,
            Err(_) => return Err(StatusCode::NoSuchFile),
        };

        // Preload entries into a temporary in-memory dir listing.
        let mut files = Vec::new();
        while let Ok(Some(entry)) = rd.next_entry().await {
            let file_name = entry.file_name().to_string_lossy().into_owned();
            let meta_std = match entry.metadata().await {
                Ok(_) => {
                    // Convert to std::fs::Metadata; tokio uses the same underlying
                    // representation so we cheat a bit by re-stat’ing.
                    match std::fs::metadata(entry.path()) {
                        Ok(m) => m,
                        Err(_) => continue,
                    }
                }
                Err(_) => continue,
            };

            let fa = FileAttributes::from(&meta_std);
            files.push(File::new(file_name, fa));
        }

        // We encode the directory listing into a synthetic “handle” entry.
        // Simple (non-streaming) approach: client calls READDIR once and gets all.
        let handle_id = Self::next_handle();
        let handle_str = handle_id.clone();

        self.dir_handles.lock().await.insert(
            handle_str.clone(),
            DirListing {
                idx: 0,
                entries: files, // the vector you built above
            },
        );

        // We actually return the file list in first READDIR call, see below.
        Ok(Handle {
            id,
            handle: handle_str,
        })
    }

    async fn readdir(&mut self, id: u32, handle: String) -> Result<Name, Self::Error> {
        let mut dirs = self.dir_handles.lock().await;

        let listing = match dirs.get_mut(&handle) {
            Some(l) => l,
            None => return Err(StatusCode::NoSuchFile),
        };

        // If already sent everything → EOF
        if listing.idx >= listing.entries.len() {
            return Err(StatusCode::Eof);
        }

        // CHUNK SIZE:
        // Many servers use 50–200 entries per packet. 100 is safe.
        const CHUNK: usize = 100;
        let end = (listing.idx + CHUNK).min(listing.entries.len());
        let slice = listing.entries[listing.idx..end].to_vec();

        listing.idx = end;

        Ok(Name { id, files: slice })
    }

    async fn mkdir(
        &mut self,
        id: u32,
        path: String,
        _attrs: FileAttributes,
    ) -> Result<Status, Self::Error> {
        let full = self.resolve_path(&path)?;
        match fs::create_dir(&full).await {
            Ok(_) => Ok(self.make_status_ok(id)),
            Err(_) => Ok(self.make_status_err(id, StatusCode::Failure, "mkdir failed")),
        }
    }

    async fn rmdir(&mut self, id: u32, path: String) -> Result<Status, Self::Error> {
        let full = self.resolve_path(&path)?;
        match fs::remove_dir(&full).await {
            Ok(_) => Ok(self.make_status_ok(id)),
            Err(_) => Ok(self.make_status_err(id, StatusCode::Failure, "rmdir failed")),
        }
    }

    // -------------------------------------------------------------------------
    // File ops: remove / rename
    // -------------------------------------------------------------------------

    async fn remove(&mut self, id: u32, path: String) -> Result<Status, Self::Error> {
        let full = self.resolve_path(&path)?;
        match fs::remove_file(&full).await {
            Ok(_) => Ok(self.make_status_ok(id)),
            Err(_) => Ok(self.make_status_err(id, StatusCode::Failure, "remove failed")),
        }
    }

    async fn rename(
        &mut self,
        id: u32,
        oldpath: String,
        newpath: String,
    ) -> Result<Status, Self::Error> {
        let old_full = self.resolve_path(&oldpath)?;
        let new_full = self.resolve_path(&newpath)?;
        match fs::rename(&old_full, &new_full).await {
            Ok(_) => Ok(self.make_status_ok(id)),
            Err(_) => Ok(self.make_status_err(id, StatusCode::Failure, "rename failed")),
        }
    }

    // -------------------------------------------------------------------------
    // setstat / fsetstat – we just pretend success (most IDEs don’t care)
    // -------------------------------------------------------------------------

    async fn setstat(
        &mut self,
        id: u32,
        path: String,
        attrs: FileAttributes,
    ) -> Result<Status, Self::Error> {
        let full = self.resolve_path(&path)?;
        if let Err(e) = apply_mtime(&full, &attrs) {
            tracing::error!("setstat mtime failed: {:?}", e);
            return Ok(self.make_status_err(id, StatusCode::Failure, "setstat failed"));
        }
        Ok(self.make_status_ok(id))
    }

    async fn fsetstat(
        &mut self,
        id: u32,
        handle: String,
        attrs: FileAttributes,
    ) -> Result<Status, Self::Error> {
        let map = self.open_files.lock().await;
        if let Some(of) = map.get(&handle) {
            if let Err(e) = apply_mtime(&of.path, &attrs) {
                tracing::error!("fsetstat mtime failed: {:?}", e);
                return Ok(self.make_status_err(id, StatusCode::Failure, "fsetstat failed"));
            }
            return Ok(self.make_status_ok(id));
        }
        Ok(self.make_status_err(id, StatusCode::NoSuchFile, "invalid handle"))
    }

    // -------------------------------------------------------------------------
    // Everything else (symlink/readlink, extensions) – left unsupported.
    // -------------------------------------------------------------------------
}

/// Entry point from your SSH handler:
///
/// ```ignore
/// tokio::spawn(async move {
///     if let Err(e) = run_sftp_server(root_dir, ch.into_stream()).await {
///         tracing::error!(?e, "SFTP server error");
///     }
/// });
/// ```
pub async fn run_sftp_server<S>(root: PathBuf, stream: S) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let handler = M87SftpHandler::new(root);
    russh_sftp::server::run(stream, handler).await;
    Ok(())
}

fn apply_mtime(path: &Path, attrs: &FileAttributes) -> std::io::Result<()> {
    if let Some(mtime) = attrs.mtime {
        let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(mtime as u64);

        let file = std::fs::File::options().write(true).open(path)?;

        let times = std::fs::FileTimes::new().set_modified(ts).set_accessed(ts); // optional – keep accessed same as modified

        file.set_times(times)?;
    }

    Ok(())
}
