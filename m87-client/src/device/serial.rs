use anyhow::{Context, Result};
use libc;
use std::ffi::CStr;
use std::os::fd::{FromRawFd, RawFd};
use tokio::io::split;
use tokio::task;
use tracing::info;

use crate::streams::quic::open_quic_io;
use crate::streams::stream_type::StreamType;
use crate::{auth::AuthManager, config::Config, devices, util::shutdown::SHUTDOWN};

// Create a PTY pair (master + slave)
fn open_pty() -> Result<(RawFd, String)> {
    unsafe {
        let mut master: libc::c_int = 0;
        let mut slave: libc::c_int = 0;

        if libc::openpty(
            &mut master as *mut _,
            &mut slave as *mut _,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        ) != 0
        {
            return Err(anyhow::anyhow!("openpty failed"));
        }

        let name_ptr = libc::ttyname(slave);
        if name_ptr.is_null() {
            return Err(anyhow::anyhow!("ttyname failed"));
        }

        let slave_name = CStr::from_ptr(name_ptr).to_string_lossy().into_owned();
        Ok((master, slave_name))
    }
}

pub async fn open_serial(device: &str, port: &str, baud: u32) -> Result<()> {
    let cfg = Config::load()?;
    let token = AuthManager::get_cli_token().await?;
    let dev = devices::get_device_by_name(device).await?;
    let host = cfg.get_server_hostname();

    let stream_type = StreamType::Serial {
        token: token.to_string(),
        baud: Some(baud),
        name: port.to_string(),
    };
    let (_, remote_io) = open_quic_io(
        &host,
        &dev.short_id,
        stream_type,
        cfg.trust_invalid_server_cert,
    )
    .await
    .context("Failed to connect to RAW metrics stream")?;

    let (master_fd, slave_path) = open_pty()?;

    info!("Local virtual serial device: {}", slave_path);

    // Convert master FD → tokio file
    let master = unsafe { tokio::fs::File::from_raw_fd(master_fd) };

    let (mut m_read, mut m_write) = split(master);
    let (mut r_read, mut r_write) = split(remote_io);

    let t1 = task::spawn(async move { tokio::io::copy(&mut r_read, &mut m_write).await });
    let t2 = task::spawn(async move { tokio::io::copy(&mut m_read, &mut r_write).await });

    tokio::select! {
        a = t1 => { a??; }
        b = t2 => { b??; }
        _ = SHUTDOWN.cancelled() => { }
    }

    Ok(())
}
