use anyhow::Context;
use anyhow::bail;
use clap::{CommandFactory, Parser, Subcommand};

use crate::auth;
use crate::device;
use crate::device::serial;
use crate::device::tunnel;
use crate::devices;
use crate::tui;
use crate::update;
#[cfg(feature = "agent")]
use crate::util;
use crate::util::logging::init_logging;
use crate::util::tls::set_tls_provider;

/// Print help with dynamically generated device commands section
fn print_help_with_device_commands() {
    let mut cmd = Cli::command();

    // Get device subcommands dynamically from DeviceCommand enum
    let device_cmd = DeviceRoot::command();
    let subcommands: Vec<_> = device_cmd
        .get_subcommands()
        .filter(|sc| sc.get_name() != "help") // Skip the auto-generated help subcommand
        .map(|sc| {
            format!(
                "    {:12} {}",
                sc.get_name(),
                sc.get_about().map(|s| s.to_string()).unwrap_or_default()
            )
        })
        .collect();

    let device_help = format!(
        "DEVICE COMMANDS:\n  \
         Run commands on a specific device: m87 <DEVICE> <COMMAND>\n\n\
         {}\n\n  \
         Examples:\n    \
         m87 my-device shell\n    \
         m87 my-device tunnel 8080\n    \
         m87 my-device docker ps\n    \
         m87 my-device exec -- ls -la",
        subcommands.join("\n")
    );

    cmd = cmd.after_help(device_help);
    let _ = cmd.print_help();
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
    /// Authenticate with make87 (manager login via browser)
    Login,

    /// Logout and deauthenticate this device
    Logout,

    /// Manage local agent service (requires root privileges - use sudo)
    #[cfg(feature = "agent")]
    #[command(subcommand)]
    Agent(AgentCommands),

    /// Manage devices and groups (requires manager role)
    #[command(subcommand)]
    Devices(DevicesCommands),

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

        /// Delete files from destination that are not present in source default false
        #[arg(long, default_value_t = false)]
        delete: bool,

        /// Watch for changes and sync automatically
        #[arg(long, default_value_t = false)]
        watch: bool,
    },

    Ls {
        path: String,
    },

    #[command(external_subcommand)]
    Device(Vec<String>),

    #[command(subcommand)]
    Ssh(SshCommands),
}

#[derive(Subcommand)]
enum SshCommands {
    Enable,
    Disable,
    #[command(external_subcommand)]
    Connect(Vec<String>),
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
    /// Open interactive shell on the device
    Shell,
    /// Forward remote port(s) to localhost
    Tunnel {
        /// Remote target as [ip:]port[/proto] (e.g., "8080" or "192.168.1.50:554" or "192.168.1.50:554/tcp" or "192.168.1.50:554/udp" or "192.168.1.50:554/udp+mcast")
        targets: Vec<String>,
    },
    /// Run docker commands on the device
    Docker {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Stream container logs from the device
    Logs {
        #[arg(short = 'f', long)]
        follow: bool,
        #[arg(long, default_value = "100")]
        tail: usize,
    },
    /// Show device system metrics
    Stats,
    /// Execute a command on the device
    Exec {
        /// Keep stdin open (for responding to prompts)
        #[arg(short = 'i', long)]
        stdin: bool,
        /// Allocate a pseudo-TTY (for TUI apps like vim, htop)
        #[arg(short = 't', long)]
        tty: bool,
        #[arg(required = true, last = true)]
        command: Vec<String>,
    },
    /// Connect to a serial device
    Serial {
        /// path to serial device (e.g., "/dev/ttyUSB0")
        path: String,
        /// Optional baud rate (defaults to 115200)
        baud: Option<u32>,
    },
}

#[cfg(feature = "agent")]
#[derive(Subcommand)]
enum AgentCommands {
    /// Register this device as an agent (headless flow, requires approval)
    Login {
        /// Organization ID to register agent under
        #[arg(long = "org-id", conflicts_with = "email")]
        org_id: Option<String>,

        /// Email address to register agent under
        #[arg(long, conflicts_with = "org_id")]
        email: Option<String>,
    },

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

pub async fn cli() -> anyhow::Result<()> {
    // Handle help before full parsing to inject device commands section
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 2 && (args[1] == "--help" || args[1] == "-h" || args[1] == "help") {
        print_help_with_device_commands();
        return Ok(());
    }

    let cli = Cli::parse();
    init_logging("info");
    set_tls_provider();

    match cli.command {
        Commands::Login => {
            println!("Logging in as manager...");
            auth::login_cli().await?;
            println!("Logged in as manager successfully");
        }

        Commands::Logout => {
            println!("Logging out...");
            auth::logout_cli().await?;
            #[cfg(feature = "agent")]
            auth::logout_device().await?;
            println!("Logged out successfully");
        }

        Commands::Ssh(cmd) => match cmd {
            SshCommands::Enable => {
                println!("Enabling SSH...");
                device::ssh::ssh_enable()?;
                println!(
                    "SSH enabled successfully. You can now connect to device via ssh m87-<device_name>"
                );
            }
            SshCommands::Disable => {
                println!("Disabling SSH...");
                device::ssh::ssh_disable()?;
                println!("SSH disabled successfully");
            }
            SshCommands::Connect(args) => {
                if args.is_empty() {
                    anyhow::bail!("missing ssh target");
                }

                let mut transport = false;
                let mut rest = Vec::new();

                for arg in args {
                    if arg == "--transport" {
                        transport = true;
                    } else {
                        rest.push(arg);
                    }
                }

                let device = rest.first().context("missing ssh target")?.as_str();

                let forwarded_args = &rest[1..];

                if transport {
                    // INTERNAL PATH — raw transport
                    device::ssh::connect_device_ssh(device).await?;
                } else {
                    // USER PATH — behaves exactly like ssh
                    device::ssh::exec_ssh(device, forwarded_args)?;
                }
            }
        },

        #[cfg(feature = "agent")]
        Commands::Agent(cmd) => match cmd {
            AgentCommands::Login { org_id, email } => {
                let owner_scope = org_id.or(email);
                println!("Registering device as agent...");
                let sysinfo = util::system_info::get_system_info().await?;
                auth::register_device(owner_scope, sysinfo).await?;
                println!("Device registered as agent successfully");
            }
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
                let requests = devices::list_auth_requests().await?;
                tui::devices::print_devices_table(&devices, &requests);
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
            let _ = device::fs::copy(&source, &dest).await?;
        }

        Commands::Sync {
            source,
            dest,
            delete,
            watch,
        } => {
            if watch {
                device::fs::watch_sync(&source, &dest, delete).await?;
            } else {
                device::fs::sync(&source, &dest, delete).await?;
            }
        }
        Commands::Ls { path } => {
            let resp = device::fs::list(&path).await?;
            tui::fs::print_dir_entries(&resp);
        }

        Commands::Device(args) => {
            let parsed = match DeviceRoot::try_parse_from(
                std::iter::once("m87").chain(args.iter().map(|s| s.as_str())),
            ) {
                Ok(p) => p,
                Err(e) => e.exit(), // Clean exit for help/version, error message for parse errors
            };
            handle_device_command(parsed).await?;
        }
    }

    Ok(())
}

async fn handle_device_command(cmd: DeviceRoot) -> anyhow::Result<()> {
    let device = cmd.device;

    match cmd.command {
        DeviceCommand::Shell => {
            let _ = tui::shell::run_shell(&device).await?;
            Ok(())
        }

        DeviceCommand::Tunnel { targets } => {
            tunnel::open_local_tunnel(&device, targets).await?;
            Ok(())
        }

        DeviceCommand::Docker { args } => {
            device::docker::run_docker_command(&device, args.clone()).await?;
            Ok(())
        }

        DeviceCommand::Logs { follow: _, tail: _ } => {
            tui::logs::run_logs(&device).await?;
            Ok(())
        }

        DeviceCommand::Stats => {
            tui::metrics::run_metrics(&device).await?;
            Ok(())
        }

        DeviceCommand::Exec {
            stdin,
            tty,
            command,
        } => {
            tui::exec::run_exec(&device, command, stdin, tty).await?;
            Ok(())
        }

        #[cfg(unix)]
        DeviceCommand::Serial { path, baud } => {
            let baud = baud.unwrap_or(115200);
            serial::open_serial(&device, &path, baud).await?;
            Ok(())
        }
    }
}
