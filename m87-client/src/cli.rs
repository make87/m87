use anyhow::bail;
use clap::{Parser, Subcommand};

use crate::auth;
use crate::config::Config;
use crate::device;
use crate::device::tunnel;
use crate::devices;
use crate::tui;
use crate::update;
use crate::util;

/// Represents a parsed device path (either local or remote)
struct DevicePath {
    device: Option<String>, // None = local, Some(name) = remote
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
    /// Authenticate with make87 (defaults to manager login)
    Login {
        /// Configure device as agent for remote management (Linux only, headless flow)
        #[cfg(feature = "agent")]
        #[arg(long)]
        agent: bool,

        /// Organization ID to register agent under (only with --agent)
        #[cfg(feature = "agent")]
        #[arg(long = "org-id", conflicts_with = "email")]
        org_id: Option<String>,

        /// Email address to register agent under (only with --agent)
        #[cfg(feature = "agent")]
        #[arg(long, conflicts_with = "org_id")]
        email: Option<String>,
    },

    /// Logout and deauthenticate this device
    Logout,

    /// Manage local agent service (requires root privileges - use sudo)
    #[cfg(feature = "agent")]
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

    #[command(external_subcommand)]
    Device(Vec<String>),
}

#[derive(Parser, Debug)]
pub struct DeviceRoot {
    /// Device name or ID
    pub device: String,

    #[command(subcommand)]
    pub command: DeviceCommand,
}

#[derive(Subcommand, Debug)]
pub enum DeviceCommand {
    Shell,
    Tunnel {
        port: u16,
        #[arg(long)]
        background: bool,
        #[arg(long)]
        persist: bool,
        #[arg(long)]
        name: String,
        #[arg(long)]
        ips: Option<String>,
        #[arg(long)]
        add_own_ip: bool,
        #[arg(long)]
        open: bool,
        #[arg(long)]
        local_port: Option<u16>,
    },
    Tunnels,
    Ls {
        path: Vec<String>,
    },
    Docker {
        args: Vec<String>,
    },
    Logs {
        #[arg(short = 'f', long)]
        follow: bool,
        #[arg(long, default_value = "100")]
        tail: usize,
    },
    Stats,
    Cmd {
        #[arg(short = 'i', long)]
        interactive: bool,
        #[arg(required = true, last = true)]
        command: Vec<String>,
    },
}

#[cfg(feature = "agent")]
#[derive(Subcommand)]
enum AgentCommands {
    /// Run the agent daemon (blocking, used by systemd service)
    Run,

    /// Start the agent service now (requires sudo)
    Start,

    /// Stop the agent service now (requires sudo)
    Stop,

    /// Restart the agent service (requires sudo)
    Restart,

    /// Configure service to auto-start on boot (requires sudo)
    Enable {
        /// Enable AND start service immediately
        #[arg(long)]
        now: bool,
    },

    /// Remove auto-start on boot (requires sudo)
    Disable {
        /// Disable AND stop service immediately
        #[arg(long)]
        now: bool,
    },

    /// Show local agent service status
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
        Commands::Login {
            #[cfg(feature = "agent")]
            agent,
            #[cfg(feature = "agent")]
            org_id,
            #[cfg(feature = "agent")]
            email,
        } => {
            #[cfg(feature = "agent")]
            if agent {
                // Determine owner_scope from provided flags
                let owner_scope = org_id.or(email);

                // Agent registration flow (headless, requires approval)
                println!("Registering device as agent...");
                let config = Config::load()?;
                let sysinfo = util::system_info::get_system_info(config.enable_geo_lookup).await?;
                auth::register_device(owner_scope, sysinfo).await?;
                println!("Device registered as agent successfully");
            } else {
                // Default: Manager login flow (OAuth)
                println!("Logging in as manager...");
                auth::login_cli().await?;
                println!("Logged in as manager successfully");
            }

            #[cfg(not(feature = "agent"))]
            {
                // Manager-only builds: always do manager login
                println!("Logging in as manager...");
                auth::login_cli().await?;
                println!("Logged in as manager successfully");
            }
        }

        Commands::Logout => {
            println!("Logging out...");
            auth::logout_cli().await?;
            #[cfg(feature = "agent")]
            auth::logout_device().await?;
            println!("Logged out successfully");
        }

        #[cfg(feature = "agent")]
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
            println!("Version: {}", env!("CARGO_PKG_VERSION"));
            println!("Build: {}", env!("GIT_COMMIT"));
            println!("Rust: {}", env!("RUSTC_VERSION"));
            println!(
                "Platform: {}/{}",
                std::env::consts::OS,
                std::env::consts::ARCH
            );
        }

        Commands::Update { version } => {
            if let Some(v) = version {
                println!(
                    "Note: Specific version updates not yet supported, updating to latest version"
                );
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
            let parsed = DeviceRoot::try_parse_from(
                std::iter::once("m87-dev").chain(args.iter().map(|s| s.as_str())),
            )?;
            handle_device_command(parsed).await?;
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
            eprintln!(
                "Would copy from '{}:{}' to '{}:{}'",
                src_dev, src_path.path, dst_dev, dst_path.path
            );
            bail!("Not implemented");
        }
        (None, Some(dst_dev)) => {
            // Local to remote copy
            eprintln!("Error: 'cp' command is not yet implemented");
            eprintln!(
                "Would copy local '{}' to '{}:{}'",
                src_path.path, dst_dev, dst_path.path
            );
            bail!("Not implemented");
        }
        (Some(src_dev), None) => {
            // Remote to local copy
            eprintln!("Error: 'cp' command is not yet implemented");
            eprintln!(
                "Would copy from '{}:{}' to local '{}'",
                src_dev, src_path.path, dst_path.path
            );
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
            eprintln!(
                "Would sync from '{}:{}' to '{}:{}'",
                src_dev, src_path.path, dst_dev, dst_path.path
            );
            bail!("Not implemented");
        }
        (None, Some(dst_dev)) => {
            // Local to remote sync
            eprintln!("Error: 'sync' command is not yet implemented");
            eprintln!(
                "Would sync local '{}' to '{}:{}'",
                src_path.path, dst_dev, dst_path.path
            );
            bail!("Not implemented");
        }
        (Some(src_dev), None) => {
            // Remote to local sync
            eprintln!("Error: 'sync' command is not yet implemented");
            eprintln!(
                "Would sync from '{}:{}' to local '{}'",
                src_dev, src_path.path, dst_path.path
            );
            bail!("Not implemented");
        }
    }
}

async fn handle_device_command(cmd: DeviceRoot) -> anyhow::Result<()> {
    let device = cmd.device;

    match cmd.command {
        DeviceCommand::Shell => {
            let _ = tui::shell::run_shell(&device).await?;
            Ok(())
        }

        DeviceCommand::Tunnel {
            port,
            background,
            persist,
            name,
            ips,
            add_own_ip,
            open,
            local_port,
        } => {
            // split by ,
            let ips = ips.map(|f| f.split(",").map(|s| s.to_string()).collect::<Vec<String>>());
            let _ = tunnel::create_tunnel(&device, port, ips, add_own_ip, open, &name).await?;
            if let Some(localport) = local_port {
                let _ = tunnel::open_local_tunnel(&device, &name, localport).await?;
                if !persist {
                    let _ = tunnel::delete_tunnel(&device, port).await?;
                }
            }
            Ok(())
        }

        DeviceCommand::Tunnels => {
            let _ = tui::tunnels::show_forwards_tui(&device).await?;
            Ok(())
        }

        // DeviceCommand::Tunnels {
        //     cmd: DeviceTunnelsCommand::List,
        // } => {
        //     let tunnels = tunnel::list_tunnels(&device).await?;
        //     println!("Tunnels on {}: {:#?}", device, tunnels);
        //     Ok(())
        // }

        // DeviceCommand::Tunnels {
        //     cmd: DeviceTunnelsCommand::Close { port },
        // } => {
        //     let tunnels = tunnel::delete_tunnel(&device, port).await?;
        //     println!("Closed tunnel on {}: {}", device, port);
        //     Ok(())
        // }
        DeviceCommand::Ls { path } => {
            println!("Would run ls on {} with {:?}", device, path);
            bail!("Not implemented");
        }

        DeviceCommand::Docker { args } => {
            println!("Would run docker on {} with {:?}", device, args);
            bail!("Not implemented");
        }

        DeviceCommand::Logs { follow, tail } => {
            tui::logs::run_logs(&device).await?;
            Ok(())
        }

        DeviceCommand::Stats => {
            tui::metrics::run_metrics(&device).await?;
            Ok(())
        }

        DeviceCommand::Cmd {
            interactive,
            command,
        } => {
            println!(
                "Would exec '{}' on {} (interactive={interactive})",
                command.join(" "),
                device
            );
            bail!("Not implemented");
        }
    }
}
