use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use portable_pty::{native_pty_system, Child, CommandBuilder, MasterPty, PtySize};
use russh::keys::ssh_key::rand_core::OsRng;
use russh::keys::PublicKey;
use tokio::{io, net::TcpStream, process::Command, sync::Mutex, task};
use tracing::error;

use russh::server::{self, Auth, Config as ServerConfig, Handle, Msg, Session};
use russh::{Channel, ChannelId};

use crate::util::fs::run_sftp_server;

/// One PTY-backed shell session per SSH channel.
struct PtySession {
    master: Box<dyn MasterPty + Send>,
    reader: Box<dyn std::io::Read + Send>,
    writer: Box<dyn std::io::Write + Send>,
    #[allow(dead_code)]
    child: Box<dyn Child + Send>,
    rows: u32,
    cols: u32,
}

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
    /// Session channels we’ve opened; used to hand off SFTP, etc.
    session_channels: HashMap<ChannelId, Channel<Msg>>,
    /// Global async handle used to send data/events to channels.
    handle: Option<Handle>,
    /// PTY sessions keyed by SSH channel id.
    ptys: HashMap<ChannelId, Arc<Mutex<PtySession>>>,
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
            pty_sizes: HashMap::new(),
            default_shell: "/bin/bash".to_string(),
        }
    }

    fn spawn_pty_shell_for_channel(
        &mut self,
        channel: ChannelId,
    ) -> Result<Arc<Mutex<PtySession>>> {
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
            reader,
            writer,
            child,
            rows,
            cols,
        };

        let arc = Arc::new(Mutex::new(pty_session));
        self.ptys.insert(channel, arc.clone());
        Ok(arc)
    }
}

/// Build a minimal SSH server config (host key handled outside).
pub fn make_server_config() -> Arc<ServerConfig> {
    let mut config = ServerConfig::default();
    config.server_id = russh::SshId::Standard("SSH-2.0-m87-ssh".to_string());
    config.inactivity_timeout = Some(Duration::from_secs(600));
    config.auth_rejection_time = Duration::from_millis(0);
    config.window_size = 4 * 1024 * 1024; // OK > 1MB
    config.channel_buffer_size = 4 * 1024 * 1024; // OK > 1MB
    config.maximum_packet_size = 65535; // MUST stay <= 65535

    let host_key = russh::keys::PrivateKey::random(&mut OsRng, russh::keys::Algorithm::Ed25519)
        .expect("failed to generate SSH host key");
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
        let pty = self.spawn_pty_shell_for_channel(channel)?;

        session.channel_success(channel)?;

        let handle = self.handle.clone().unwrap();

        tokio::spawn(async move {
            let mut buf = [0u8; 8192];

            loop {
                let n = task::spawn_blocking({
                    let pty = pty.clone();
                    move || {
                        use std::io::Read;
                        let mut guard = pty.blocking_lock();
                        guard.reader.read(&mut buf)
                    }
                })
                .await
                .unwrap_or(Ok(0))
                .unwrap_or(0);

                if n == 0 {
                    break;
                }

                if handle
                    .data(channel, buf[..n].to_vec().into())
                    .await
                    .is_err()
                {
                    break;
                }
            }

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
        if let Some(pty) = self.ptys.get(&channel) {
            let pty = pty.clone();
            let buf = data.to_vec();
            task::spawn_blocking(move || {
                use std::io::Write;
                let mut guard = pty.blocking_lock();
                let _ = guard.writer.write_all(&buf);
            });
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

            match output {
                Ok(out) => {
                    if !out.stdout.is_empty() {
                        let _ = handle.data(channel, out.stdout.into()).await;
                    }
                    if !out.stderr.is_empty() {
                        let _ = handle.data(channel, out.stderr.into()).await;
                    }
                }
                Err(e) => {
                    let msg = format!("command failed: {e}\n");
                    let _ = handle.data(channel, msg.into_bytes().into()).await;
                }
            }

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
