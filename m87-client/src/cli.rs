use anyhow::{bail, Ok};
use clap::{Parser, Subcommand};

use crate::app;
use crate::auth;
use crate::config;
use crate::device;
use crate::devices;
use crate::stack;
use crate::update;
use crate::util;

#[derive(Parser)]
#[command(name = "m87")]
#[command(version, about = "Unified CLI and device for the make87 platform", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Login and authenticate this device
    Login {
        /// Configure device as agent (can be managed remotely)
        #[arg(long)]
        agent: bool,

        /// Configure device as manager (can manage other devices)
        #[arg(long)]
        manager: bool,
    },

    /// Logout and deauthenticate this device
    Logout {
        /// Skip confirmation prompt
        #[arg(long)]
        force: bool,
    },

    /// Manage local agent service
    #[command(subcommand)]
    Agent(AgentCommands),

    /// Manage devices and groups
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

    /// Remote device commands (device-first syntax)
    #[command(external_subcommand)]
    Device(Vec<String>),
}

#[derive(Subcommand)]
enum AgentCommands {
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
    List {
        /// Filter by status (online, offline, pending, all)
        #[arg(long)]
        status: Option<String>,
    },

    /// Show detailed information about a specific device
    Show {
        /// Device name or ID
        device: String,
    },

    /// Approve a pending device to join the organization
    Approve {
        /// Device name or ID
        device: String,

        /// Add device to groups on approval
        #[arg(long)]
        groups: Option<String>,
    },

    /// Reject a pending device registration
    Reject {
        /// Device name or ID
        device: String,

        /// Optional reason for rejection (for audit logs)
        #[arg(long)]
        reason: Option<String>,
    },
}

#[derive(Subcommand)]
enum TunnelsCommands {
    /// List all active tunnels
    List {
        /// Filter by device name
        device: Option<String>,

        /// Filter by device name (alternative syntax)
        #[arg(long)]
        device_flag: Option<String>,
    },

    /// Close an active tunnel
    Close {
        /// Tunnel ID to close
        id: Option<String>,

        /// Close all tunnels to specific device
        #[arg(long)]
        device: Option<String>,
    },
}

// Remote device command enums
#[derive(Subcommand)]
enum RemoteDeviceCommands {
    /// Open interactive SSH session to device
    Ssh {
        /// Command to execute (optional)
        #[arg(last = true)]
        command: Vec<String>,
    },

    /// Create port tunnel from remote device to local machine
    Tunnel {
        /// Port mapping (remote:local)
        ports: String,

        /// Run tunnel in background (ephemeral)
        #[arg(short, long)]
        background: bool,

        /// Create persistent tunnel that survives reboots
        #[arg(long)]
        persist: bool,

        /// Assign name to tunnel for easier management
        #[arg(long)]
        name: Option<String>,

        /// List active tunnels for this device
        #[command(subcommand)]
        subcommand: Option<TunnelSubCommands>,
    },

    /// Sync files from local device to remote device
    Sync {
        /// Local path
        local_path: String,

        /// Remote path
        remote_path: String,

        /// Continuous sync on file changes
        #[arg(short, long)]
        watch: bool,

        /// Exclude patterns (can be repeated)
        #[arg(long)]
        exclude: Vec<String>,

        /// Delete remote files not in local
        #[arg(long)]
        delete: bool,

        /// Show what would be synced without doing it
        #[arg(long)]
        dry_run: bool,
    },

    /// Copy file from local to remote (one-time, scp-like)
    Copy {
        /// Local path
        local_path: String,

        /// Remote path
        remote_path: String,
    },

    /// List files and directories on remote device
    Ls {
        /// Arguments to pass to ls command
        #[arg(allow_hyphen_values = true, trailing_var_arg = true)]
        args: Vec<String>,
    },

    /// Run docker commands on remote device with local context
    Docker {
        /// Docker command arguments
        #[arg(allow_hyphen_values = true, trailing_var_arg = true)]
        args: Vec<String>,
    },

    /// Stream logs from device
    Logs {
        /// Container name (optional)
        container: Option<String>,

        /// Stream logs in real-time
        #[arg(short, long)]
        follow: bool,

        /// Show logs since duration (e.g., 1h, 30m, 2d)
        #[arg(long)]
        since: Option<String>,

        /// Show last n lines
        #[arg(long, default_value = "100")]
        tail: usize,

        /// Show timestamps
        #[arg(long)]
        timestamps: bool,
    },

    /// Show resource usage statistics
    Stats {
        /// Continuous updates (like top)
        #[arg(short, long)]
        watch: bool,

        /// Update interval for watch mode
        #[arg(long, default_value = "2s")]
        interval: String,
    },

    /// Execute arbitrary command on remote device
    Cmd {
        /// Allocate PTY for interactive commands
        #[arg(short, long)]
        interactive: bool,

        /// Command to execute (after --)
        #[arg(last = true)]
        command: Vec<String>,
    },
}

#[derive(Subcommand)]
enum TunnelSubCommands {
    /// List active tunnels for this device
    List,

    /// Close tunnel(s) for this device
    Close {
        /// Tunnel ID to close
        id: Option<String>,

        /// Close all tunnels
        #[arg(long)]
        all: bool,
    },
}

pub async fn cli() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Login { agent, manager } => {
            // Stub: Not implemented
            bail!("Command 'login' is not yet implemented");
        }

        Commands::Logout { force } => {
            // Stub: Not implemented
            bail!("Command 'logout' is not yet implemented");
        }

        Commands::Agent(cmd) => match cmd {
            AgentCommands::Start => {
                // Stub: Not implemented
                bail!("Command 'agent start' is not yet implemented");
            }
            AgentCommands::Stop => {
                // Stub: Not implemented
                bail!("Command 'agent stop' is not yet implemented");
            }
            AgentCommands::Restart => {
                // Stub: Not implemented
                bail!("Command 'agent restart' is not yet implemented");
            }
            AgentCommands::Enable { now } => {
                // Stub: Not implemented
                bail!("Command 'agent enable' is not yet implemented");
            }
            AgentCommands::Disable { now } => {
                // Stub: Not implemented
                bail!("Command 'agent disable' is not yet implemented");
            }
            AgentCommands::Status => {
                // Stub: Not implemented
                bail!("Command 'agent status' is not yet implemented");
            }
        },

        Commands::Devices(cmd) => match cmd {
            DevicesCommands::List { status } => {
                // Stub: Not implemented
                bail!("Command 'devices list' is not yet implemented");
            }
            DevicesCommands::Show { device } => {
                // Stub: Not implemented
                bail!("Command 'devices show' is not yet implemented");
            }
            DevicesCommands::Approve { device, groups } => {
                // Stub: Not implemented
                bail!("Command 'devices approve' is not yet implemented");
            }
            DevicesCommands::Reject { device, reason } => {
                // Stub: Not implemented
                bail!("Command 'devices reject' is not yet implemented");
            }
        },

        Commands::Tunnels(cmd) => match cmd {
            TunnelsCommands::List { device, device_flag } => {
                // Stub: Not implemented
                bail!("Command 'tunnels list' is not yet implemented");
            }
            TunnelsCommands::Close { id, device } => {
                // Stub: Not implemented
                bail!("Command 'tunnels close' is not yet implemented");
            }
        },

        Commands::Version => {
            println!("m87 CLI v{}", env!("CARGO_PKG_VERSION"));
            println!("Build: {}", env!("CARGO_PKG_VERSION"));
            // TODO: Add more version info (Go version, platform, etc.)
        }

        Commands::Update { version } => {
            // Stub: Not implemented
            bail!("Command 'update' is not yet implemented");
        }

        Commands::Device(args) => {
            // This handles the device-first syntax: m87 <device> <command> [args...]
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
                "ssh" => {
                    // Stub: Not implemented
                    bail!("Command 'ssh' is not yet implemented for device '{}'", device_name);
                }
                "tunnel" => {
                    // Stub: Not implemented
                    bail!("Command 'tunnel' is not yet implemented for device '{}'", device_name);
                }
                "sync" => {
                    // Stub: Not implemented
                    bail!("Command 'sync' is not yet implemented for device '{}'", device_name);
                }
                "copy" => {
                    // Stub: Not implemented
                    bail!("Command 'copy' is not yet implemented for device '{}'", device_name);
                }
                "ls" => {
                    // Stub: Not implemented
                    bail!("Command 'ls' is not yet implemented for device '{}'", device_name);
                }
                "docker" => {
                    // Stub: Not implemented
                    bail!("Command 'docker' is not yet implemented for device '{}'", device_name);
                }
                "logs" => {
                    // Stub: Not implemented
                    bail!("Command 'logs' is not yet implemented for device '{}'", device_name);
                }
                "stats" => {
                    // Stub: Not implemented
                    bail!("Command 'stats' is not yet implemented for device '{}'", device_name);
                }
                "cmd" => {
                    // Stub: Not implemented
                    bail!("Command 'cmd' is not yet implemented for device '{}'", device_name);
                }
                _ => {
                    bail!("Unknown command '{}' for device '{}'. Use 'm87 --help' for available commands", command, device_name);
                }
            }
        }
    }

    Ok(())
}
