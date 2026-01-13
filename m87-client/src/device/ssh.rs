use anyhow::{Context, Result};
use std::{env, fs, path::PathBuf};
use tokio::{
    io::{self, AsyncWriteExt},
    signal, try_join,
};

use crate::{
    auth::AuthManager,
    config::Config,
    devices,
    streams::{quic::open_quic_io, stream_type::StreamType},
    util::command::current_exe_path,
};

// This function is an SSH ProxyCommand transport.
// It must NEVER spawn `ssh` or assume a TTY.
pub async fn connect_device_ssh(device_name: &str) -> Result<()> {
    let config = Config::load()?;
    let resolved = devices::resolve_device_short_id_cached(device_name).await?;

    let token = AuthManager::get_cli_token().await?;

    let (conn, mut quic) = open_quic_io(
        &resolved.host,
        &token,
        &resolved.short_id,
        StreamType::Ssh {
            token: token.to_string(),
        },
        config.trust_invalid_server_cert,
    )
    .await
    .context("Failed to connect to device")?;

    let mut stdin = io::stdin();
    let mut stdout = io::stdout();

    // stdin → QUIC
    let to_remote = async {
        io::copy(&mut stdin, &mut quic.send).await?;
        let _ = quic.send.finish();
        Result::<()>::Ok(())
    };

    // QUIC → stdout
    let to_local = async {
        io::copy(&mut quic.recv, &mut stdout).await?;
        let _ = stdout.shutdown().await;
        Result::<()>::Ok(())
    };

    tokio::select! {
        res = async {
            try_join!(to_remote, to_local)
        } => {
            res?;
        }
        _ = signal::ctrl_c() => {
            // local shutdown / Ctrl-C / service exit
        }

        _ = conn.closed() => {
            // remote device disconnected
        }
    }

    // best-effort cleanup
    let _ = quic.send.finish();
    Ok(())
}

pub fn exec_ssh(target: &str, args: &[String]) -> Result<()> {
    let host = if target.contains('.') {
        target.to_string()
    } else {
        format!("{target}.m87")
    };

    let status = std::process::Command::new("ssh")
        .arg(host)
        .args(args)
        .status()?;

    if !status.success() {
        anyhow::bail!("ssh exited with {}", status);
    }
    Ok(())
}

fn m87_ssh_block() -> String {
    let exe_path = current_exe_path()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "m87".to_string());

    format!(
        r#"
Host *.m87
    ProxyCommand {} ssh %h %r --transport
"#,
        exe_path
    )
}

fn ssh_config_path() -> Result<PathBuf> {
    let home = env::var("HOME")
        .or_else(|_| env::var("USERPROFILE"))
        .context("Cannot determine home directory")?;

    Ok(PathBuf::from(home).join(".ssh").join("config"))
}

fn ensure_ssh_dir(path: &PathBuf) -> Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).context("Failed to create ~/.ssh")?;
    }
    Ok(())
}

pub fn ssh_enable() -> Result<()> {
    let path = ssh_config_path()?;
    ensure_ssh_dir(&path)?;

    let mut contents = fs::read_to_string(&path).unwrap_or_default();

    if contents.contains("Host *.m87") {
        return Ok(()); // already enabled
    }

    if !contents.ends_with('\n') {
        contents.push('\n');
    }

    contents.push_str(&m87_ssh_block());
    fs::write(&path, contents).context("Failed to write SSH config")?;

    Ok(())
}

pub fn ssh_disable() -> Result<()> {
    let path = ssh_config_path()?;

    let contents = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(_) => return Ok(()), // nothing to do
    };

    let mut lines = contents.lines().peekable();
    let mut out = String::new();
    let mut skip = false;

    while let Some(line) = lines.next() {
        if line.trim() == "Host *.m87" {
            skip = true;
            continue;
        }

        if skip {
            if !line.starts_with(' ') && !line.starts_with('\t') {
                skip = false;
            } else {
                continue;
            }
        }

        if !skip {
            out.push_str(line);
            out.push('\n');
        }
    }

    fs::write(&path, out).context("Failed to update SSH config")?;
    Ok(())
}
