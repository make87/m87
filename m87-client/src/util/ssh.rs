use std::{collections::HashMap, io::Write, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use portable_pty::{native_pty_system, Child, CommandBuilder, MasterPty, PtySize};
use russh::keys::ssh_key::rand_core::OsRng;
use russh::keys::PublicKey;
use tokio::{io, net::TcpStream, process::Command, sync::Mutex, task};
use tracing::{error, info, warn};

use russh::server::{self, Auth, Config as ServerConfig, Handle, Msg, Session};
use russh::{Channel, ChannelId};

use crate::util::fs::run_sftp_server;

/// One PTY-backed shell session per SSH channel.
/// Reader and writer are separated to avoid lock contention.
struct PtySession {
    master: Box<dyn MasterPty + Send>,
    #[allow(dead_code)]
    child: Box<dyn Child + Send>,
    rows: u32,
    cols: u32,
}

/// Separate reader for PTY output (moved to its own Arc<Mutex<>>)
type PtyReader = Arc<std::sync::Mutex<Box<dyn std::io::Read + Send>>>;

/// Separate writer for PTY input (moved to its own Arc<Mutex<>>)
type PtyWriter = Arc<std::sync::Mutex<Box<dyn std::io::Write + Send>>>;

impl PtySession {
    fn resize(&mut self, rows: u32, cols: u32) {
        let _ = self.master.resize(PtySize {
            rows: rows as u16,
            cols: cols as u16,
            pixel_width: 0,
            pixel_height: 0,
        });
        self.rows = rows;
        self.cols = cols;
    }
}

/// Server-side SSH handler state.
///
/// One instance per SSH connection (russh does this for you).
pub struct M87SshHandler {
    /// Root directory for SFTP and exec/shell (e.g. `/` or some chroot-ish base).
    root_dir: PathBuf,
    /// Session channels we've opened; used to hand off SFTP, etc.
    session_channels: HashMap<ChannelId, Channel<Msg>>,
    /// Global async handle used to send data/events to channels.
    handle: Option<Handle>,
    /// PTY sessions keyed by SSH channel id (for resize).
    ptys: HashMap<ChannelId, Arc<Mutex<PtySession>>>,
    /// PTY writers keyed by SSH channel id (separate lock from reader).
    pty_writers: HashMap<ChannelId, PtyWriter>,
    /// Cached PTY size requested before shell starts.
    pty_sizes: HashMap<ChannelId, (u32, u32)>,
    /// Default login shell for PTY sessions.
    default_shell: String,
}

impl M87SshHandler {
    pub fn new(root_dir: PathBuf) -> Self {
        Self {
            root_dir,
            session_channels: HashMap::new(),
            handle: None,
            ptys: HashMap::new(),
            pty_writers: HashMap::new(),
            pty_sizes: HashMap::new(),
            default_shell: "/bin/bash".to_string(),
        }
    }

    /// Spawns a PTY shell and returns the reader (for output).
    /// The writer is stored internally for use by the data handler.
    fn spawn_pty_shell_for_channel(
        &mut self,
        channel: ChannelId,
    ) -> Result<PtyReader> {
        let (cols, rows) = self.pty_sizes.get(&channel).copied().unwrap_or((80, 24));

        let pty_system = native_pty_system();
        let pair = pty_system.openpty(PtySize {
            rows: rows as u16,
            cols: cols as u16,
            pixel_width: 0,
            pixel_height: 0,
        })?;

        let mut cmd = CommandBuilder::new(&self.default_shell);
        cmd.env("TERM", "xterm-256color");

        let child = pair.slave.spawn_command(cmd)?;
        drop(pair.slave);

        let reader = pair.master.try_clone_reader()?;
        let writer = pair.master.take_writer()?;

        let master = pair.master;
        let pty_session = PtySession {
            master,
            child,
            rows,
            cols,
        };

        // Store session (for resize) and writer (for input) separately
        let session_arc = Arc::new(Mutex::new(pty_session));
        self.ptys.insert(channel, session_arc);

        // Store writer with its own lock
        let writer_arc: PtyWriter = Arc::new(std::sync::Mutex::new(writer));
        self.pty_writers.insert(channel, writer_arc);

        // Return reader with its own lock
        let reader_arc: PtyReader = Arc::new(std::sync::Mutex::new(reader));
        Ok(reader_arc)
    }
}

/// Get or create the SSH keys directory.
fn ssh_keys_dir() -> Result<PathBuf> {
    let dir = dirs::config_dir()
        .ok_or_else(|| anyhow!("No config directory found"))?
        .join("m87")
        .join("ssh_keys");
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// Load an existing SSH host key or generate a new one for this device.
fn load_or_generate_host_key() -> Result<russh::keys::PrivateKey> {
    let key_path = ssh_keys_dir()?.join("host_key.pem");

    if key_path.exists() {
        let pem = std::fs::read_to_string(&key_path)?;
        let key = russh::keys::PrivateKey::from_openssh(&pem)
            .map_err(|e| anyhow!("Failed to parse host key: {}", e))?;
        info!("Loaded existing SSH host key");
        Ok(key)
    } else {
        let key = russh::keys::PrivateKey::random(&mut OsRng, russh::keys::Algorithm::Ed25519)
            .map_err(|e| anyhow!("Failed to generate key: {}", e))?;

        // Serialize to OpenSSH format
        let pem = key
            .to_openssh(russh::keys::ssh_key::LineEnding::LF)
            .map_err(|e| anyhow!("Failed to serialize key: {}", e))?;

        // Save with restricted permissions (0o600 on Unix)
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .mode(0o600)
                .open(&key_path)?
                .write_all(pem.as_bytes())?;
        }
        #[cfg(not(unix))]
        std::fs::write(&key_path, &pem)?;

        info!("Generated new SSH host key");
        Ok(key)
    }
}

/// Build a minimal SSH server config with persistent host key.
pub fn make_server_config() -> Arc<ServerConfig> {
    let mut config = ServerConfig::default();
    config.server_id = russh::SshId::Standard("SSH-2.0-m87-ssh".to_string());
    config.inactivity_timeout = Some(Duration::from_secs(600));
    config.auth_rejection_time = Duration::from_millis(0);
    config.window_size = 4 * 1024 * 1024; // OK > 1MB
    config.channel_buffer_size = 4 * 1024 * 1024; // OK > 1MB
    config.maximum_packet_size = 65535; // MUST stay <= 65535

    let host_key = load_or_generate_host_key().unwrap_or_else(|e| {
        warn!("Failed to load/save host key: {}, using ephemeral key", e);
        russh::keys::PrivateKey::random(&mut OsRng, russh::keys::Algorithm::Ed25519)
            .expect("failed to generate SSH host key")
    });
    config.keys.push(host_key);
    Arc::new(config)
}

impl server::Handler for M87SshHandler {
    type Error = anyhow::Error;

    // ------------------- AUTH -------------------

    async fn auth_none(&mut self, _user: &str) -> Result<Auth, Self::Error> {
        Ok(Auth::Accept)
    }

    async fn auth_publickey(&mut self, _user: &str, _key: &PublicKey) -> Result<Auth, Self::Error> {
        Ok(Auth::Accept)
    }

    // ------------------- CHANNEL OPEN -------------------

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        session: &mut Session,
    ) -> Result<bool, Self::Error> {
        let id = channel.id();

        if self.handle.is_none() {
            self.handle = Some(session.handle().clone());
        }

        self.session_channels.insert(id, channel);
        session.channel_success(id)?;
        Ok(true)
    }

    async fn channel_close(
        &mut self,
        channel: ChannelId,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.session_channels.remove(&channel);
        self.ptys.remove(&channel);
        self.pty_writers.remove(&channel);
        self.pty_sizes.remove(&channel);
        Ok(())
    }

    // ------------------- direct-tcpip -------------------

    async fn channel_open_direct_tcpip(
        &mut self,
        channel: Channel<Msg>,
        host_to_connect: &str,
        port_to_connect: u32,
        _origin_addr: &str,
        _origin_port: u32,
        _session: &mut Session,
    ) -> Result<bool, Self::Error> {
        let target = format!("{host_to_connect}:{port_to_connect}");
        let mut tcp = TcpStream::connect(&target).await?;
        let mut chan_stream = channel.into_stream();

        tokio::spawn(async move {
            let _ = io::copy_bidirectional(&mut chan_stream, &mut tcp).await;
        });

        Ok(true)
    }

    // ------------------- PTY / Shell -------------------

    async fn pty_request(
        &mut self,
        channel: ChannelId,
        _term: &str,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _modes: &[(russh::Pty, u32)],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.pty_sizes.insert(channel, (col_width, row_height));
        session.channel_success(channel)?;
        Ok(())
    }

    async fn window_change_request(
        &mut self,
        channel: ChannelId,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        if let Some(pty) = self.ptys.get(&channel) {
            pty.lock().await.resize(row_height, col_width);
        }
        Ok(())
    }

    async fn shell_request(
        &mut self,
        channel: ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let reader = self.spawn_pty_shell_for_channel(channel)?;

        session.channel_success(channel)?;

        let handle = self.handle.clone().unwrap();

        tokio::spawn(async move {
            tracing::debug!("PTY reader task started for channel {:?}", channel);
            loop {
                let read_result = task::spawn_blocking({
                    let reader = reader.clone();
                    move || {
                        use std::io::Read;
                        let mut buf = [0u8; 8192];
                        // Use std::sync::Mutex::lock() instead of blocking_lock()
                        let mut guard = reader.lock().unwrap();
                        match guard.read(&mut buf) {
                            Ok(0) => {
                                tracing::debug!("PTY read returned 0 bytes (EOF)");
                                None
                            }
                            Ok(n) => {
                                tracing::debug!("PTY read {} bytes", n);
                                Some(buf[..n].to_vec())
                            }
                            Err(e) => {
                                tracing::debug!("PTY read error: {:?}", e);
                                None
                            }
                        }
                    }
                })
                .await
                .ok()
                .flatten();

                match read_result {
                    Some(data) => {
                        tracing::debug!("Sending {} bytes to SSH channel", data.len());
                        if handle.data(channel, data.into()).await.is_err() {
                            tracing::debug!("Failed to send data to SSH channel");
                            break;
                        }
                    }
                    None => {
                        tracing::debug!("PTY reader loop ending");
                        break;
                    }
                }
            }

            tracing::debug!("PTY reader task ending, sending EOF");
            let _ = handle.eof(channel).await;
            let _ = handle.close(channel).await;
        });

        Ok(())
    }

    async fn data(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        tracing::debug!("SSH data received: {} bytes for channel {:?}", data.len(), channel);
        if let Some(writer) = self.pty_writers.get(&channel) {
            let writer = writer.clone();
            let buf = data.to_vec();
            task::spawn_blocking(move || {
                use std::io::Write;
                // Use std::sync::Mutex::lock() - separate lock from reader
                let mut guard = writer.lock().unwrap();
                let _ = guard.write_all(&buf);
                let _ = guard.flush();
            });
        } else {
            tracing::warn!("No PTY writer found for channel {:?}", channel);
        }
        Ok(())
    }

    // ------------------- EXEC -------------------

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let cmd = String::from_utf8_lossy(data).to_string();
        let handle = self.handle.clone().unwrap();

        session.channel_success(channel)?;

        let cwd = self.root_dir.clone();

        tokio::spawn(async move {
            use std::process::Stdio;
            let output = Command::new("/bin/sh")
                .arg("-c")
                .arg(&cmd)
                .current_dir(cwd)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .await;

            let exit_code = match output {
                Ok(out) => {
                    if !out.stdout.is_empty() {
                        let _ = handle.data(channel, out.stdout.into()).await;
                    }
                    if !out.stderr.is_empty() {
                        let _ = handle.data(channel, out.stderr.into()).await;
                    }
                    out.status.code().unwrap_or(1) as u32
                }
                Err(e) => {
                    let msg = format!("command failed: {e}\n");
                    let _ = handle.data(channel, msg.into_bytes().into()).await;
                    1
                }
            };

            // Send exit status (required by SSH protocol for exec)
            let _ = handle.exit_status_request(channel, exit_code).await;
            let _ = handle.eof(channel).await;
            let _ = handle.close(channel).await;
        });

        Ok(())
    }

    // ------------------- SUBSYSTEMS (SFTP) -------------------

    async fn subsystem_request(
        &mut self,
        channel: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if name != "sftp" {
            session.channel_failure(channel)?;
            return Ok(());
        }

        let Some(ch) = self.session_channels.remove(&channel) else {
            session.channel_failure(channel)?;
            return Ok(());
        };

        session.channel_success(channel)?;

        let root = self.root_dir.clone();
        tokio::spawn(async move {
            if let Err(e) = run_sftp_server(root, ch.into_stream()).await {
                error!("SFTP server error: {e:?}");
            }
        });

        Ok(())
    }
}
