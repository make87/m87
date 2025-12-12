use std::{collections::HashMap, io::Write, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use portable_pty::{Child, CommandBuilder, MasterPty, PtySize, native_pty_system};
use russh::keys::PublicKey;
use russh::keys::ssh_key::rand_core::OsRng;
use tokio::{io, net::TcpStream, sync::Mutex, task};
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
    /// Environment variables requested by the client (per channel)
    env_vars: HashMap<ChannelId, HashMap<String, String>>,
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
            default_shell: default_shell(),
            env_vars: HashMap::new(),
        }
    }

    /// Spawns a PTY shell and returns the reader (for output).
    /// The writer is stored internally for use by the data handler.
    fn spawn_pty_shell_for_channel(&mut self, channel: ChannelId) -> Result<PtyReader> {
        let (cols, rows) = self.pty_sizes.get(&channel).copied().unwrap_or((80, 24));

        let pty_system = native_pty_system();
        let pair = pty_system.openpty(PtySize {
            rows: rows as u16,
            cols: cols as u16,
            pixel_width: 0,
            pixel_height: 0,
        })?;

        // Build shell command
        let mut cmd = CommandBuilder::new(&self.default_shell);

        // Apply env vars requested via SSH
        if let Some(envs) = self.env_vars.get(&channel) {
            for (k, v) in envs {
                cmd.env(k, v);
            }
        }

        // Fallback TERM if none provided
        cmd.env("TERM", "xterm-256color");

        let child = pair.slave.spawn_command(cmd)?;
        drop(pair.slave);

        let reader = pair.master.try_clone_reader()?;
        let writer = pair.master.take_writer()?;

        let pty_session = PtySession {
            master: pair.master,
            child,
            rows,
            cols,
        };

        let session_arc = Arc::new(Mutex::new(pty_session));
        self.ptys.insert(channel, session_arc);

        let writer_arc: PtyWriter = Arc::new(std::sync::Mutex::new(writer));
        self.pty_writers.insert(channel, writer_arc);

        Ok(Arc::new(std::sync::Mutex::new(reader)))
    }

    fn exec_with_pty(&mut self, channel: ChannelId, cmd: &str) -> Result<()> {
        let (cols, rows) = self.pty_sizes.get(&channel).copied().unwrap_or((80, 24));

        let pty_system = native_pty_system();
        let pair = pty_system.openpty(PtySize {
            rows: rows as u16,
            cols: cols as u16,
            pixel_width: 0,
            pixel_height: 0,
        })?;

        let mut command = CommandBuilder::new(&self.default_shell);
        command.arg("-c");
        command.arg(cmd);

        // apply env vars
        if let Some(envs) = self.env_vars.get(&channel) {
            for (k, v) in envs {
                command.env(k, v);
            }
        }

        let child = pair.slave.spawn_command(command)?;
        drop(pair.slave);

        let reader = pair.master.try_clone_reader()?;
        let writer = pair.master.take_writer()?;

        let pty_session = PtySession {
            master: pair.master,
            child,
            rows,
            cols,
        };

        let pty_arc = Arc::new(Mutex::new(pty_session));
        self.ptys.insert(channel, pty_arc.clone());
        self.pty_writers
            .insert(channel, Arc::new(std::sync::Mutex::new(writer)));

        let handle = self.handle.clone().unwrap();

        // stdout → SSH
        tokio::spawn({
            let reader = Arc::new(std::sync::Mutex::new(reader));
            let handle = handle.clone();
            async move {
                loop {
                    let data = tokio::task::spawn_blocking({
                        let reader = reader.clone();
                        move || {
                            use std::io::Read;
                            let mut buf = [0u8; 8192];
                            let mut guard = reader.lock().unwrap();
                            guard
                                .read(&mut buf)
                                .ok()
                                .filter(|&n| n > 0)
                                .map(|n| buf[..n].to_vec())
                        }
                    })
                    .await
                    .ok()
                    .flatten();

                    match data {
                        Some(bytes) => {
                            if handle.data(channel, bytes.into()).await.is_err() {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                continue;
                            }
                        }
                        None => {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn exec_without_pty(&mut self, channel: ChannelId, cmd: &str) -> Result<()> {
        use std::process::Stdio;

        let handle = self.handle.clone().unwrap();
        let cwd = self.root_dir.clone();

        let Some(ch) = self.session_channels.remove(&channel) else {
            return Ok(());
        };

        let mut child = match tokio::process::Command::new(&self.default_shell)
            .arg("-c")
            .arg(cmd)
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
        {
            Ok(c) => c,
            Err(e) => {
                let msg = format!("failed to spawn command: {e}\n");
                let _ = handle.data(channel, msg.into()).await;
                let _ = handle.exit_status_request(channel, 1).await;
                let _ = handle.eof(channel).await;
                let _ = handle.close(channel).await;
                return Ok(());
            }
        };

        let stdin = child.stdin.take().unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let mut chan_stream = ch.into_stream();

        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let mut proc_buf = [0u8; 8192];
            let mut chan_buf = [0u8; 8192];
            let mut stdin_opt = Some(stdin);
            let mut stdout_done = false;

            loop {
                tokio::select! {
                    result = stdout.read(&mut proc_buf), if !stdout_done => {
                        match result {
                            Ok(0) => stdout_done = true,
                            Ok(n) => {
                                if chan_stream.write_all(&proc_buf[..n]).await.is_err() {
                                    break;
                                }
                                let _ = chan_stream.flush().await;
                            }
                            Err(_) => stdout_done = true,
                        }
                    }

                    result = chan_stream.read(&mut chan_buf), if stdin_opt.is_some() => {
                        match result {
                            Ok(0) => {
                                stdin_opt = None;
                            }
                            Ok(n) => {
                                if let Some(ref mut stdin) = stdin_opt {
                                    let _ = stdin.write_all(&chan_buf[..n]).await;
                                }
                            }
                            Err(_) => {
                                stdin_opt = None;
                            }
                        }
                    }

                    else => break,
                }
            }

            let exit_code = child.wait().await.ok().and_then(|s| s.code()).unwrap_or(1) as u32;

            let _ = handle.exit_status_request(channel, exit_code).await;
            let _ = handle.eof(channel).await;
            let _ = handle.close(channel).await;
        });

        Ok(())
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
    config.inactivity_timeout = Some(Duration::from_hours(2));
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
        // session.channel_success(id)?;
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
        self.env_vars.remove(&channel);
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
        term: &str,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _modes: &[(russh::Pty, u32)],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.pty_sizes.insert(channel, (col_width, row_height));

        // Save TERM so we can apply it when spawning shell
        self.env_vars
            .entry(channel)
            .or_default()
            .insert("TERM".to_string(), term.to_string());

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
        // Spawn PTY + shell
        let reader = self.spawn_pty_shell_for_channel(channel)?;
        session.channel_success(channel)?;

        let handle = self.handle.clone().unwrap();
        let pty_session = self.ptys.get(&channel).unwrap().clone();

        // -------- PTY → SSH (stdout) --------
        tokio::spawn({
            let reader = reader.clone();
            let handle = handle.clone();
            async move {
                loop {
                    let data = tokio::task::spawn_blocking({
                        let reader = reader.clone();
                        move || {
                            use std::io::Read;
                            let mut buf = [0u8; 8192];
                            let mut guard = reader.lock().unwrap();

                            match guard.read(&mut buf) {
                                Ok(n) if n > 0 => Some(buf[..n].to_vec()),
                                Ok(_) => None, // NOT EOF for PTY
                                Err(_) => None,
                            }
                        }
                    })
                    .await
                    .ok()
                    .flatten();

                    if let Some(bytes) = data {
                        if handle.data(channel, bytes.into()).await.is_err() {
                            break;
                        }
                    } else {
                        // PTY idle — do NOT exit
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
        });

        // -------- Shell lifetime → SSH close --------
        tokio::spawn(async move {
            let exit_code = tokio::task::spawn_blocking(move || {
                let mut guard = futures::executor::block_on(pty_session.lock());
                guard.child.wait().ok().and_then(|s| Some(s.exit_code()))
            })
            .await
            .ok()
            .flatten()
            .unwrap_or(0);

            let _ = handle.exit_status_request(channel, exit_code).await;
            let _ = handle.eof(channel).await;
            let _ = handle.close(channel).await;
        });

        Ok(())
    }

    async fn signal(
        &mut self,
        channel: ChannelId,
        signal_name: russh::Sig,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        tracing::debug!("SSH signal {:?} on {:?}", signal_name, channel);

        if let Some(pty) = self.ptys.get(&channel) {
            // Best-effort: send SIGINT to the PTY child
            let _ = pty.lock().await.child.kill();
        }
        Ok(())
    }

    async fn env_request(
        &mut self,
        channel: ChannelId,
        variable_name: &str,
        variable_value: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.env_vars
            .entry(channel)
            .or_default()
            .insert(variable_name.to_string(), variable_value.to_string());

        session.channel_success(channel)?;
        Ok(())
    }

    async fn data(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        tracing::debug!(
            "SSH data received: {} bytes for channel {:?}",
            data.len(),
            channel
        );

        // Route to PTY writer (for shell sessions)
        // Note: exec sessions use Channel::into_stream() so data flows directly
        if let Some(writer) = self.pty_writers.get(&channel) {
            let writer = writer.clone();
            let buf = data.to_vec();
            task::spawn_blocking(move || {
                use std::io::Write;
                let mut guard = writer.lock().unwrap();
                let _ = guard.write_all(&buf);
                let _ = guard.flush();
            });
        } else {
            tracing::debug!("No writer found for channel {:?}", channel);
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

        session.channel_success(channel)?;

        if self.pty_sizes.contains_key(&channel) {
            // PTY-backed exec (for Zed / VS Code terminals)
            self.exec_with_pty(channel, &cmd)?;
        } else {
            // Pipe-based exec (scp-style, one-shot commands, etc.)
            self.exec_without_pty(channel, &cmd).await?;
        }

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

#[cfg(unix)]
fn default_shell() -> String {
    use std::{ffi::CStr, path::Path};
    // 1. $SHELL (most reliable)
    if let Ok(shell) = std::env::var("SHELL") {
        if !shell.is_empty() {
            return shell;
        }
    }

    // 2. /etc/passwd via libc
    unsafe {
        let uid = libc::geteuid();
        let pwd = libc::getpwuid(uid);
        if !pwd.is_null() {
            let shell = CStr::from_ptr((*pwd).pw_shell);
            if let Ok(shell) = shell.to_str() {
                if !shell.is_empty() {
                    return shell.to_string();
                }
            }
        }
    }

    // 3. Common fallbacks
    if Path::new("/bin/bash").exists() {
        "/bin/bash".to_string()
    } else {
        "/bin/sh".to_string()
    }
}
