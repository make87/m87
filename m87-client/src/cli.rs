use anyhow::bail;
use clap::{Parser, Subcommand};

use crate::auth;
use crate::config::Config;
use crate::device;
use crate::devices;
use crate::update;
use crate::util;

/// Represents a parsed device path (either local or remote)
struct DevicePath {
    device: Option<String>,  // None = local, Some(name) = remote
    path: String,
}

/// Parse a path string into DevicePath, detecting device:path syntax
fn parse_device_path(input: &str) -> DevicePath {
    // Check for device:path pattern
    if let Some(colon_pos) = input.find(':') {
        // Handle Windows drive letters (e.g., C:\path)
        // If it's a single char followed by colon and backslash, treat as local Windows path
        if colon_pos == 1 && input.len() > 2 && &input[2..3] == "\\" {
            DevicePath {
                device: None,
                path: input.to_string(),
            }
        } else {
            DevicePath {
                device: Some(input[..colon_pos].to_string()),
                path: input[colon_pos + 1..].to_string(),
            }
        }
    } else {
        DevicePath {
            device: None,
            path: input.to_string(),
        }
    }
}

#[derive(Parser)]
#[command(name = "m87")]
#[command(version, about = "m87 CLI - Unified CLI for the make87 platform", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Post-installation authentication and role selection
    Login {
        /// Configure device as agent (can be managed remotely)
        #[arg(long)]
        agent: bool,

        /// Configure device as manager (can manage other devices)
        #[arg(long)]
        manager: bool,
    },

    /// Logout and deauthenticate this device
    Logout,

    /// Manage local agent service (requires agent role)
    #[command(subcommand)]
    Agent(AgentCommands),

    /// Manage devices and groups (requires manager role)
    #[command(subcommand)]
    Devices(DevicesCommands),

    /// Manage active port tunnels
    #[command(subcommand)]
    Tunnels(TunnelsCommands),

    /// Show CLI version information
    Version,

    /// Update the CLI to the latest version
    Update {
        /// Update to specific version
        #[arg(long)]
        version: Option<String>,
    },

    /// Copy files between local and remote devices (SCP-style)
    Cp {
        /// Source path (<path> for local, <device>:<path> for remote)
        source: String,

        /// Destination path (<path> for local, <device>:<path> for remote)
        dest: String,
    },

    /// Sync files between local and remote devices (rsync-style)
    Sync {
        /// Source path (<path> for local, <device>:<path> for remote)
        source: String,

        /// Destination path (<path> for local, <device>:<path> for remote)
        dest: String,
    },

    /// Remote device commands (device-first syntax)
    #[command(external_subcommand)]
    Device(Vec<String>),
}

#[derive(Subcommand)]
enum AgentCommands {
    /// Run the agent daemon (blocking, used by systemd service)
    Run,

    /// Start the agent service now (does not persist across reboots)
    Start,

    /// Stop the agent service now (does not change auto-start configuration)
    Stop,

    /// Restart the agent service
    Restart,

    /// Configure service to auto-start on boot (does not start now)
    Enable {
        /// Enable AND start service immediately
        #[arg(long)]
        now: bool,
    },

    /// Remove auto-start on boot (does not stop running service)
    Disable {
        /// Disable AND stop service immediately
        #[arg(long)]
        now: bool,
    },

    /// Show local agent service status and configuration
    Status,
}

#[derive(Subcommand)]
enum DevicesCommands {
    /// List all accessible devices
    List,

    /// Show detailed information about a specific device
    Show {
        /// Device name or ID
        device: String,
    },

    /// Approve a pending device to join the organization
    Approve {
        /// Device name or ID
        device: String,
    },

    /// Reject a pending device registration
    Reject {
        /// Device name or ID
        device: String,
    },
}

#[derive(Subcommand)]
enum TunnelsCommands {
    /// List all active tunnels
    List,

    /// Close an active tunnel
    Close {
        /// Tunnel ID to close
        id: String,
    },
}

pub async fn cli() -> anyhow::Result<()> {
    // TODO: Fix device name collision issue
    // Currently, if a device is named the same as a built-in command (e.g., "agent", "login", "devices"),
    // the CLI will interpret it as the built-in command instead of a device name.
    //
    // Example of the problem:
    //   m87 agent ssh  <- This triggers the agent subcommand, NOT ssh to device named "agent"
    //
    // Potential solutions:
    // 1. Check if second arg matches known device commands (ssh, tunnel, sync, etc.) and treat as device command
    // 2. Try parsing as device command first, fall back to built-in commands
    // 3. Reserve certain names and validate during device registration
    // 4. Use a prefix like @ or : for device names (changes API spec)
    //
    // Recommended: Solution 1 - disambiguate based on second argument pattern
    // This preserves the API spec while allowing any device name.

    let cli = Cli::parse();

    match cli.command {
        Commands::Login { agent, manager } => {
            if !agent && !manager {
                bail!("Please specify at least one role: --agent or --manager");
            }

            if agent {
                println!("Registering device as agent...");
                let config = Config::load()?;
                let sysinfo = util::system_info::get_system_info(config.enable_geo_lookup).await?;
                auth::register_device(None, sysinfo).await?;
                println!("Device registered as agent successfully");
            }

            if manager {
                println!("Logging in as manager...");
                auth::login_cli().await?;
                println!("Logged in as manager successfully");
            }
        }

        Commands::Logout => {
            println!("Logging out...");
            auth::logout_cli().await?;
            auth::logout_device().await?;
            println!("Logged out successfully");
        }

        Commands::Agent(cmd) => match cmd {
            AgentCommands::Run => {
                device::agent::run().await?;
            }
            AgentCommands::Start => {
                device::agent::start().await?;
            }
            AgentCommands::Stop => {
                device::agent::stop().await?;
            }
            AgentCommands::Restart => {
                device::agent::restart().await?;
            }
            AgentCommands::Enable { now } => {
                device::agent::enable(now).await?;
            }
            AgentCommands::Disable { now } => {
                device::agent::disable(now).await?;
            }
            AgentCommands::Status => {
                device::agent::status().await?;
            }
        },

        Commands::Devices(cmd) => match cmd {
            DevicesCommands::List => {
                let devices = devices::list_devices().await?;
                println!("{:#?}", devices);
            }
            DevicesCommands::Show { device } => {
                eprintln!("Error: 'devices show' command is not yet implemented");
                eprintln!("Would show details for device: {}", device);
                bail!("Not implemented");
            }
            DevicesCommands::Approve { device } => {
                println!("Approving device: {}", device);
                auth::accept_auth_request(&device).await?;
                println!("Device approved successfully");
            }
            DevicesCommands::Reject { device } => {
                println!("Rejecting device: {}", device);
                auth::reject_auth_request(&device).await?;
                println!("Device rejected successfully");
            }
        },

        Commands::Tunnels(cmd) => match cmd {
            TunnelsCommands::List => {
                eprintln!("Error: 'tunnels list' command is not yet implemented");
                eprintln!("Would list all active tunnels");
                bail!("Not implemented");
            }
            TunnelsCommands::Close { id } => {
                eprintln!("Error: 'tunnels close' command is not yet implemented");
                eprintln!("Would close tunnel with ID: {}", id);
                bail!("Not implemented");
            }
        },

        Commands::Version => {
            println!("m87 CLI v{}", env!("CARGO_PKG_VERSION"));
            println!("Build: {}", option_env!("GIT_COMMIT").unwrap_or("unknown"));
            println!("Rust: {}", env!("CARGO_PKG_RUST_VERSION"));
            println!("Platform: {}/{}", std::env::consts::OS, std::env::consts::ARCH);
        }

        Commands::Update { version } => {
            if let Some(v) = version {
                println!("Note: Specific version updates not yet supported, updating to latest version");
                eprintln!("Requested version: {}", v);
            }

            println!("Checking for updates...");
            let success = update::update(true).await?;
            if success {
                println!("Update successful");
            } else {
                println!("Already at latest version");
            }
        }

        Commands::Cp { source, dest } => {
            handle_cp_command(&source, &dest).await?;
        }

        Commands::Sync { source, dest } => {
            handle_sync_command(&source, &dest).await?;
        }

        Commands::Device(args) => {
            handle_device_command(args).await?;
        }
    }

    Ok(())
}

async fn handle_cp_command(source: &str, dest: &str) -> anyhow::Result<()> {
    let src_path = parse_device_path(source);
    let dst_path = parse_device_path(dest);

    match (&src_path.device, &dst_path.device) {
        (None, None) => {
            bail!("At least one path must specify a device (use <device>:<path> syntax)");
        }
        (Some(src_dev), Some(dst_dev)) => {
            // Remote to remote copy
            eprintln!("Error: 'cp' command is not yet implemented");
            eprintln!("Would copy from '{}:{}' to '{}:{}'",
                     src_dev, src_path.path, dst_dev, dst_path.path);
            bail!("Not implemented");
        }
        (None, Some(dst_dev)) => {
            // Local to remote copy
            eprintln!("Error: 'cp' command is not yet implemented");
            eprintln!("Would copy local '{}' to '{}:{}'",
                     src_path.path, dst_dev, dst_path.path);
            bail!("Not implemented");
        }
        (Some(src_dev), None) => {
            // Remote to local copy
            eprintln!("Error: 'cp' command is not yet implemented");
            eprintln!("Would copy from '{}:{}' to local '{}'",
                     src_dev, src_path.path, dst_path.path);
            bail!("Not implemented");
        }
    }
}

async fn handle_sync_command(source: &str, dest: &str) -> anyhow::Result<()> {
    let src_path = parse_device_path(source);
    let dst_path = parse_device_path(dest);

    match (&src_path.device, &dst_path.device) {
        (None, None) => {
            bail!("At least one path must specify a device (use <device>:<path> syntax)");
        }
        (Some(src_dev), Some(dst_dev)) => {
            // Remote to remote sync
            eprintln!("Error: 'sync' command is not yet implemented");
            eprintln!("Would sync from '{}:{}' to '{}:{}'",
                     src_dev, src_path.path, dst_dev, dst_path.path);
            bail!("Not implemented");
        }
        (None, Some(dst_dev)) => {
            // Local to remote sync
            eprintln!("Error: 'sync' command is not yet implemented");
            eprintln!("Would sync local '{}' to '{}:{}'",
                     src_path.path, dst_dev, dst_path.path);
            bail!("Not implemented");
        }
        (Some(src_dev), None) => {
            // Remote to local sync
            eprintln!("Error: 'sync' command is not yet implemented");
            eprintln!("Would sync from '{}:{}' to local '{}'",
                     src_dev, src_path.path, dst_path.path);
            bail!("Not implemented");
        }
    }
}

async fn handle_device_command(args: Vec<String>) -> anyhow::Result<()> {
    if args.is_empty() {
        bail!("Device name required. Usage: m87 <device> <command> [args...]");
    }

    let device_name = &args[0];

    if args.len() < 2 {
        bail!("Command required. Usage: m87 {} <command> [args...]", device_name);
    }

    let command = &args[1];
    let remaining_args = &args[2..];

    match command.as_str() {
        "shell" => {
            if !remaining_args.is_empty() {
                bail!("The shell command does not accept any options or flags");
            }
            eprintln!("Error: 'shell' command is not yet implemented for device '{}'", device_name);
            eprintln!("Would open interactive shell session");
            bail!("Not implemented");
        }

        "tunnel" => {
            // Handle tunnel creation only
            if remaining_args.is_empty() {
                bail!("Tunnel command requires arguments. Usage: m87 {} tunnel <remote>:<local>", device_name);
            }

            let first_arg = &remaining_args[0];

            // Create tunnel: <remote>:<local>
            if !first_arg.contains(':') {
                bail!("Invalid tunnel format. Expected <remote-port>:<local-port>");
            }
            eprintln!("Error: 'tunnel' command is not yet implemented for device '{}'", device_name);
            eprintln!("Would create tunnel: {}", first_arg);

            // Parse additional flags
            for arg in remaining_args.iter().skip(1) {
                match arg.as_str() {
                    "--background" | "-b" => eprintln!("  Run in background: true"),
                    "--persist" => eprintln!("  Persistent (survives reboots): true"),
                    _ if arg.starts_with("--name") => {
                        eprintln!("  Tunnel name specified");
                    }
                    _ => {}
                }
            }
            bail!("Not implemented");
        }

        "tunnels" => {
            // Handle tunnels close <id> only
            if remaining_args.len() < 2 {
                bail!("Usage: m87 {} tunnels close <id>", device_name);
            }

            if remaining_args[0] != "close" {
                bail!("Unknown tunnels subcommand. Usage: m87 {} tunnels close <id>", device_name);
            }

            let tunnel_id = &remaining_args[1];
            eprintln!("Error: 'tunnels close' command is not yet implemented for device '{}'", device_name);
            eprintln!("Would close tunnel with ID: {}", tunnel_id);
            bail!("Not implemented");
        }

        "ls" => {
            eprintln!("Error: 'ls' command is not yet implemented for device '{}'", device_name);
            eprintln!("Would execute: ls {}", remaining_args.join(" "));
            bail!("Not implemented");
        }

        "docker" => {
            eprintln!("Error: 'docker' command is not yet implemented for device '{}'", device_name);
            eprintln!("Would set DOCKER_HOST=ssh://user@{}", device_name);
            eprintln!("Would execute: docker {}", remaining_args.join(" "));
            bail!("Not implemented");
        }

        "logs" => {
            eprintln!("Error: 'logs' command is not yet implemented for device '{}'", device_name);

            // Parse logs arguments
            let mut follow = false;
            let mut tail = 100;

            let mut i = 0;
            while i < remaining_args.len() {
                let arg = &remaining_args[i];
                match arg.as_str() {
                    "-f" => follow = true,
                    "--tail" => {
                        if i + 1 < remaining_args.len() {
                            if let Ok(n) = remaining_args[i + 1].parse::<usize>() {
                                tail = n;
                            }
                            i += 1;
                        }
                    }
                    _ => {}
                }
                i += 1;
            }

            eprintln!("Would stream logs from device");
            eprintln!("  Follow: {}, Tail: {}", follow, tail);
            bail!("Not implemented");
        }

        "stats" => {
            eprintln!("Error: 'stats' command is not yet implemented for device '{}'", device_name);
            eprintln!("Would show resource statistics");
            bail!("Not implemented");
        }

        "cmd" => {
            // Look for -- separator
            let cmd_start = remaining_args.iter().position(|s| s == "--");

            if cmd_start.is_none() {
                bail!("Usage: m87 {} cmd [-i] -- '<command>'", device_name);
            }

            let mut interactive = false;

            // Check for -i flag before --
            for i in 0..cmd_start.unwrap() {
                if remaining_args[i] == "-i" || remaining_args[i] == "--interactive" {
                    interactive = true;
                }
            }

            let command_args = &remaining_args[cmd_start.unwrap() + 1..];

            eprintln!("Error: 'cmd' command is not yet implemented for device '{}'", device_name);
            eprintln!("Would execute command (interactive={}): {}", interactive, command_args.join(" "));
            bail!("Not implemented");
        }

        _ => {
            bail!("Unknown command '{}' for device '{}'. Available commands: shell, tunnel, tunnels, ls, docker, logs, stats, cmd",
                  command, device_name);
        }
    }
}