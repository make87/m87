use anyhow::bail;
use clap::{Parser, Subcommand};

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
    Logout {
        /// Skip confirmation prompt
        #[arg(long)]
        force: bool,
    },

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
        #[arg(long, value_parser = ["online", "offline", "pending", "all"])]
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

        /// Add device to groups on approval (comma-separated)
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
    /// List all active tunnels, optionally filtered by device
    List {
        /// Filter by device name (positional argument or --device flag)
        #[arg(value_name = "DEVICE")]
        device: Option<String>,

        /// Filter by device name (using --device flag)
        #[arg(long = "device", conflicts_with = "device")]
        device_flag: Option<String>,
    },

    /// Close an active tunnel
    Close {
        /// Tunnel ID to close (mutually exclusive with --device)
        #[arg(value_name = "ID", conflicts_with = "device")]
        id: Option<String>,

        /// Close all tunnels to specific device
        #[arg(long, conflicts_with = "id")]
        device: Option<String>,
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
            eprintln!("Error: 'login' command is not yet implemented");
            eprintln!("Would configure device with roles: agent={}, manager={}", agent, manager);
            bail!("Not implemented");
        }

        Commands::Logout { force } => {
            eprintln!("Error: 'logout' command is not yet implemented");
            eprintln!("Would logout device (force={})", force);
            bail!("Not implemented");
        }

        Commands::Agent(cmd) => match cmd {
            AgentCommands::Start => {
                eprintln!("Error: 'agent start' command is not yet implemented");
                eprintln!("Would run: systemctl start m87-client.service");
                bail!("Not implemented");
            }
            AgentCommands::Stop => {
                eprintln!("Error: 'agent stop' command is not yet implemented");
                eprintln!("Would run: systemctl stop m87-client.service");
                bail!("Not implemented");
            }
            AgentCommands::Restart => {
                eprintln!("Error: 'agent restart' command is not yet implemented");
                eprintln!("Would run: systemctl restart m87-client.service");
                bail!("Not implemented");
            }
            AgentCommands::Enable { now } => {
                eprintln!("Error: 'agent enable' command is not yet implemented");
                if now {
                    eprintln!("Would run: systemctl enable --now m87-client.service");
                } else {
                    eprintln!("Would run: systemctl enable m87-client.service");
                }
                bail!("Not implemented");
            }
            AgentCommands::Disable { now } => {
                eprintln!("Error: 'agent disable' command is not yet implemented");
                if now {
                    eprintln!("Would run: systemctl disable --now m87-client.service");
                } else {
                    eprintln!("Would run: systemctl disable m87-client.service");
                }
                bail!("Not implemented");
            }
            AgentCommands::Status => {
                eprintln!("Error: 'agent status' command is not yet implemented");
                eprintln!("Would run: systemctl status m87-client.service");
                bail!("Not implemented");
            }
        },

        Commands::Devices(cmd) => match cmd {
            DevicesCommands::List { status } => {
                eprintln!("Error: 'devices list' command is not yet implemented");
                if let Some(s) = status {
                    eprintln!("Would list devices with status filter: {}", s);
                } else {
                    eprintln!("Would list all devices (online and offline, excluding pending)");
                }
                bail!("Not implemented");
            }
            DevicesCommands::Show { device } => {
                eprintln!("Error: 'devices show' command is not yet implemented");
                eprintln!("Would show details for device: {}", device);
                bail!("Not implemented");
            }
            DevicesCommands::Approve { device, groups } => {
                eprintln!("Error: 'devices approve' command is not yet implemented");
                eprintln!("Would approve device: {}", device);
                if let Some(g) = groups {
                    eprintln!("Would add to groups: {}", g);
                }
                bail!("Not implemented");
            }
            DevicesCommands::Reject { device, reason } => {
                eprintln!("Error: 'devices reject' command is not yet implemented");
                eprintln!("Would reject device: {}", device);
                if let Some(r) = reason {
                    eprintln!("Reason: {}", r);
                }
                bail!("Not implemented");
            }
        },

        Commands::Tunnels(cmd) => match cmd {
            TunnelsCommands::List { device, device_flag } => {
                let filter_device = device.or(device_flag);
                eprintln!("Error: 'tunnels list' command is not yet implemented");
                if let Some(d) = filter_device {
                    eprintln!("Would list tunnels for device: {}", d);
                } else {
                    eprintln!("Would list all active tunnels");
                }
                bail!("Not implemented");
            }
            TunnelsCommands::Close { id, device } => {
                eprintln!("Error: 'tunnels close' command is not yet implemented");
                if let Some(tunnel_id) = id {
                    eprintln!("Would close tunnel with ID: {}", tunnel_id);
                } else if let Some(dev) = device {
                    eprintln!("Would close all tunnels to device: {}", dev);
                } else {
                    eprintln!("Error: Must specify either tunnel ID or --device");
                }
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
            eprintln!("Error: 'update' command is not yet implemented");
            if let Some(v) = version {
                eprintln!("Would update to version: {}", v);
            } else {
                eprintln!("Would update to latest version");
            }
            bail!("Not implemented");
        }

        Commands::Device(args) => {
            handle_device_command(args).await?;
        }
    }

    Ok(())
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
        "ssh" => {
            eprintln!("Error: 'ssh' command is not yet implemented for device '{}'", device_name);
            if !remaining_args.is_empty() && remaining_args[0] == "--" {
                let cmd_args = &remaining_args[1..];
                eprintln!("Would execute SSH command: {:?}", cmd_args.join(" "));
            } else {
                eprintln!("Would open interactive SSH session");
            }
            bail!("Not implemented");
        }

        "tunnel" => {
            // Handle tunnel subcommands: list, close, or create tunnel
            if remaining_args.is_empty() {
                bail!("Tunnel command requires arguments. Usage: m87 {} tunnel <remote>:<local> | list | close", device_name);
            }

            let first_arg = &remaining_args[0];

            match first_arg.as_str() {
                "list" => {
                    eprintln!("Error: 'tunnel list' command is not yet implemented for device '{}'", device_name);
                    eprintln!("Would list all tunnels for device: {}", device_name);
                    bail!("Not implemented");
                }
                "close" => {
                    if remaining_args.len() < 2 {
                        bail!("Usage: m87 {} tunnel close <id|--all>", device_name);
                    }
                    eprintln!("Error: 'tunnel close' command is not yet implemented for device '{}'", device_name);
                    if remaining_args[1] == "--all" {
                        eprintln!("Would close all tunnels for device: {}", device_name);
                    } else {
                        eprintln!("Would close tunnel with ID: {}", remaining_args[1]);
                    }
                    bail!("Not implemented");
                }
                _ => {
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
            }
        }

        "sync" => {
            if remaining_args.len() < 2 {
                bail!("Usage: m87 {} sync <local-path> <remote-path> [options]", device_name);
            }
            eprintln!("Error: 'sync' command is not yet implemented for device '{}'", device_name);
            eprintln!("Would sync from '{}' to '{}'", remaining_args[0], remaining_args[1]);

            // Check for flags
            for arg in remaining_args.iter().skip(2) {
                match arg.as_str() {
                    "--watch" | "-w" => eprintln!("  Watch mode: enabled"),
                    "--delete" => eprintln!("  Delete mode: enabled"),
                    "--dry-run" => eprintln!("  Dry run: enabled"),
                    _ if arg.starts_with("--exclude") => eprintln!("  Exclude pattern specified"),
                    _ => {}
                }
            }
            bail!("Not implemented");
        }

        "copy" => {
            if remaining_args.len() < 2 {
                bail!("Usage: m87 {} copy <local-path> <remote-path>", device_name);
            }
            eprintln!("Error: 'copy' command is not yet implemented for device '{}'", device_name);
            eprintln!("Would copy '{}' to '{}'", remaining_args[0], remaining_args[1]);
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
            let mut container: Option<&str> = None;
            let mut follow = false;
            let mut since: Option<&str> = None;
            let mut tail = 100;
            let mut timestamps = false;

            let mut i = 0;
            while i < remaining_args.len() {
                let arg = &remaining_args[i];
                match arg.as_str() {
                    "-f" | "--follow" => follow = true,
                    "--timestamps" => timestamps = true,
                    "--since" => {
                        if i + 1 < remaining_args.len() {
                            since = Some(&remaining_args[i + 1]);
                            i += 1;
                        }
                    }
                    "--tail" => {
                        if i + 1 < remaining_args.len() {
                            if let Ok(n) = remaining_args[i + 1].parse::<usize>() {
                                tail = n;
                            }
                            i += 1;
                        }
                    }
                    _ if !arg.starts_with("-") && container.is_none() => {
                        container = Some(arg);
                    }
                    _ => {}
                }
                i += 1;
            }

            if let Some(c) = container {
                eprintln!("Would stream logs for container: {}", c);
            } else {
                eprintln!("Would stream system logs");
            }
            eprintln!("  Follow: {}, Since: {:?}, Tail: {}, Timestamps: {}",
                     follow, since, tail, timestamps);
            bail!("Not implemented");
        }

        "stats" => {
            eprintln!("Error: 'stats' command is not yet implemented for device '{}'", device_name);

            let mut watch = false;
            let mut interval = "2s";

            let mut i = 0;
            while i < remaining_args.len() {
                let arg = &remaining_args[i];
                match arg.as_str() {
                    "-w" | "--watch" => watch = true,
                    "--interval" => {
                        if i + 1 < remaining_args.len() {
                            interval = &remaining_args[i + 1];
                            i += 1;
                        }
                    }
                    _ => {}
                }
                i += 1;
            }

            eprintln!("Would show resource statistics (watch={}, interval={})", watch, interval);
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
            bail!("Unknown command '{}' for device '{}'. Available commands: ssh, tunnel, sync, copy, ls, docker, logs, stats, cmd",
                  command, device_name);
        }
    }
}