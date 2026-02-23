use std::path::PathBuf;

use anyhow::Context;
use anyhow::bail;
use clap::{CommandFactory, Parser, Subcommand};
use m87_shared::roles::Role;

use crate::auth;
use crate::config::Config;
use crate::device;
use crate::device::deploy::DeploymentUpdateArgs;
use crate::device::deploy::SpecType;
use crate::device::forward;
use crate::device::serial;
use crate::devices;
use crate::org;
use crate::tui;
use crate::update;
#[cfg(feature = "runtime")]
use crate::util;
use crate::util::logging::init_logging;
use crate::util::tls::set_tls_provider;

/// Save owner_reference to config if org_id or email is provided
#[cfg(feature = "runtime")]
fn save_owner_if_provided(org_id: Option<String>, email: Option<String>) -> anyhow::Result<()> {
    if let Some(owner) = org_id.or(email) {
        let mut cfg = Config::load()?;
        cfg.owner_reference = Some(owner);
        cfg.save()?;
    }
    Ok(())
}

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
         m87 my-device forward 8080\n    \
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
    /// Enable verbose logging
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Authenticate with make87 via browser
    Login,

    /// Logout and deauthenticate this device
    Logout,

    /// Manage local runtime service (requires root privileges - use sudo)
    #[cfg(feature = "runtime")]
    #[command(subcommand)]
    Runtime(RuntimeCommands),

    /// Internal commands for privileged operations (hidden from help)
    #[cfg(feature = "runtime")]
    #[command(subcommand, hide = true)]
    Internal(InternalCommands),

    /// Manage devices and view pending registrations
    #[command(subcommand)]
    Devices(DevicesCommands),

    /// Show CLI version information
    Version,

    /// Update the CLI to the latest version
    Update,

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

        /// Delete files from destination that are not present in source
        #[arg(long, default_value_t = false)]
        delete: bool,

        /// Watch for changes and sync automatically
        #[arg(long, default_value_t = false)]
        watch: bool,

        /// Show what would be done without making changes
        #[arg(long, short = 'n', default_value_t = false)]
        dry_run: bool,

        /// Exclude files matching pattern (can be used multiple times)
        #[arg(long, short = 'e', action = clap::ArgAction::Append)]
        exclude: Vec<String>,
    },

    Ls {
        path: String,
    },

    #[command(external_subcommand)]
    Device(Vec<String>),

    #[command(subcommand)]
    Ssh(SshCommands),

    #[command(subcommand)]
    Config(ConfigCommands),

    #[command(subcommand)]
    Org(OrgCommands),

    /// Start MCP server (Model Context Protocol) for AI agent integration
    ///
    /// The server runs on stdin/stdout and exposes m87 platform commands
    /// as MCP tools for programmatic AI agent access.
    Mcp,
}

#[derive(Subcommand)]
enum OrgCommands {
    /// Manage human members of the org
    #[clap(subcommand)]
    Members(MemberAction),
    /// Manage devices owned by the org
    #[clap(subcommand)]
    Devices(OrgDeviceAction),
    Create {
        id: String,
        owner_email: String,
    },
    Delete {
        id: String,
    },
    Update {
        id: String,
        new_id: String,
    },
    List,
    //     Invites {
    //         #[clap(subcommand)]
    //         action: InviteAction,
    //     },
}

// #[derive(Subcommand)]
// enum InviteAction {
//     List,
//     Accept { invite_id: String },
//     Reject { invite_id: String },
// }

#[derive(Subcommand)]
enum OrgDeviceAction {
    Add {
        device_name: String,
        #[arg(long)]
        org_id: Option<String>,
    },
    Remove {
        device_name: String,
        #[arg(long)]
        org_id: Option<String>,
    },
    List {
        #[arg(long)]
        org_id: Option<String>,
    },
}

#[derive(Subcommand)]
enum MemberAction {
    Add {
        /// Email address of the user to add
        email: String,
        /// Role of the user to add
        #[arg(value_parser = parse_role)]
        role: Role,
        /// Optional organization ID to add the user to. Otherwise will be attempted to auto resolve
        #[arg(long)]
        org_id: Option<String>,
    },
    Update {
        email: String,
        #[arg(value_parser = parse_role)]
        role: Role,
        #[arg(long)]
        org_id: Option<String>,
    },
    Remove {
        email: String,
        #[arg(long)]
        org_id: Option<String>,
    },

    List {
        #[arg(long)]
        org_id: Option<String>,
    },
}

#[derive(Subcommand)]
enum ConfigCommands {
    Set {
        /// Override API URL (e.g. https://eu.public.make87.dev)
        #[arg(long)]
        runtime_server_url: Option<String>,

        /// Set owner reference (email or org id)
        #[arg(long)]
        owner_reference: Option<String>,

        #[arg(long)]
        make87_api_url: Option<String>,

        #[arg(long)]
        make87_app_url: Option<String>,

        #[arg(long)]
        trust_invalid_server_cert: Option<bool>,
    },

    Show,
    File,
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
    Forward {
        /// Port forwarding target(s). Supports single ports and ranges.
        /// Examples:
        ///   8080                    - forward single port
        ///   8080-8090               - forward port range (same local/remote)
        ///   8080-8090:9080-9090     - map local range to different remote range
        ///   8080:192.168.1.50:9080  - forward to specific host
        ///   8080-8090:192.168.1.50:9080-9090/tcp - range with host and protocol
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
    #[clap(alias = "stats")]
    Metrics,
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

    Status,

    Audit {
        // rfc date like 2026-01-31 or 2026-01-31T13:00:00
        #[arg(long)]
        until: Option<String>,
        #[arg(long)]
        since: Option<String>,
        #[arg(long, default_value = "100")]
        max: u32,
        #[arg(long, default_value = "false")]
        details: bool,
    },

    /// Add a run spec to a deployment (defaults to active deployment)
    Deploy(DeployArgs),

    /// Remove a run spec from a deployment (defaults to active deployment)
    Undeploy(UndeployArgs),

    /// Manage deployments on the device
    #[command(subcommand)]
    Deployment(DeploymentCommand),

    #[clap(subcommand)]
    Access(AccessAction),
}

fn parse_role(s: &str) -> Result<Role, String> {
    Role::from_str(s)
}

#[derive(Subcommand, Debug)]
pub enum AccessAction {
    Add {
        email_or_org_id: String,
        #[arg(value_parser = parse_role)]
        role: Role,
    },
    Remove {
        email_or_org_id: String,
    },
    List,
    Update {
        email_or_org_id: String,
        #[arg(value_parser = parse_role)]
        role: Role,
    },
}

#[derive(Parser, Debug)]
pub struct DeployArgs {
    /// File to add (docker-compose.yml or run spec yaml)
    pub file: PathBuf,

    /// Spec type (auto detects by default)
    #[arg(long, value_enum, default_value_t = SpecType::Auto)]
    pub r#type: SpecType,

    /// Optional display name for the run spec
    #[arg(long)]
    pub name: Option<String>,

    /// Add to a specific deployment (otherwise active deployment)
    #[arg(long)]
    pub deployment_id: Option<String>,
}

#[derive(Parser, Debug)]
pub struct UndeployArgs {
    /// File path or run spec name
    pub job_id: String,

    /// Add to a specific deployment (otherwise active deployment)
    #[arg(long)]
    pub deployment_id: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum DeploymentCommand {
    /// List deployments for this device
    List,

    /// Create a new deployment
    New {
        /// Make this deployment active immediately
        #[arg(long)]
        active: bool,
    },

    /// Show details for a deployment (includes run specs)
    Show {
        /// specifiv deplyoment to show. Defautls to the active deployment
        #[arg(long)]
        deployment_id: Option<String>,

        /// Output YAML (optional)
        #[arg(long)]
        yaml: bool,
    },

    /// Remove a deployment
    Rm {
        deployment_id: String,

        /// Do not prompt (if you later add prompts)
        #[arg(long)]
        force: bool,
    },

    /// Print the currently active deployment
    Active,

    /// Set the active deployment
    Activate { deployment_id: String },
    /// Set the active deployment
    Status {
        /// specifiv deplyoment to show. Defautls to the active deployment
        #[arg(long)]
        deployment_id: Option<String>,

        /// Show logs of the steps
        #[arg(long)]
        logs: bool,
    },

    /// Clone an existing deployment into a new one
    Clone {
        deployment_id: String,

        /// Make the cloned deployment active immediately
        #[arg(long)]
        active: bool,
    },

    /// Update a deployment (remove/replace/move/rename specs; change name)
    Update(DeploymentUpdateArgs),
}

#[cfg(feature = "runtime")]
#[derive(Subcommand)]
enum RuntimeCommands {
    /// Register this device as a runtime (headless flow, requires approval)
    Login {
        /// Organization ID to register runtime under
        #[arg(long = "org-id", conflicts_with = "email")]
        org_id: Option<String>,

        /// Email address to register runtime under
        #[arg(long, conflicts_with = "org_id")]
        email: Option<String>,
    },

    Logout,
    /// Run the runtime daemon (blocking, used by systemd service)
    Run {
        /// Organization ID to register runtime under
        #[arg(long = "org-id", conflicts_with = "email")]
        org_id: Option<String>,

        /// Email address to register runtime under
        #[arg(long, conflicts_with = "org_id")]
        email: Option<String>,
    },

    /// Start the runtime service now (requires sudo)
    Start {
        /// Organization ID to register runtime under
        #[arg(long = "org-id", conflicts_with = "email")]
        org_id: Option<String>,

        /// Email address to register runtime under
        #[arg(long, conflicts_with = "org_id")]
        email: Option<String>,
    },

    /// Stop the runtime service now (requires sudo)
    Stop,

    /// Restart the runtime service (requires sudo)
    Restart {
        /// Organization ID to register runtime under
        #[arg(long = "org-id", conflicts_with = "email")]
        org_id: Option<String>,

        /// Email address to register runtime under
        #[arg(long, conflicts_with = "org_id")]
        email: Option<String>,
    },

    /// Configure service to auto-start on boot (requires sudo)
    Enable {
        /// Enable AND start service immediately
        #[arg(long)]
        now: bool,

        /// Organization ID to register runtime under
        #[arg(long = "org-id", conflicts_with = "email")]
        org_id: Option<String>,

        /// Email address to register runtime under
        #[arg(long, conflicts_with = "org_id")]
        email: Option<String>,
    },

    /// Remove auto-start on boot (requires sudo)
    Disable {
        /// Disable AND stop service immediately
        #[arg(long)]
        now: bool,
    },

    /// Show local runtime service status
    Status,
}

/// Hidden internal commands for privileged operations (not shown in help)
#[cfg(feature = "runtime")]
#[derive(Subcommand)]
enum InternalCommands {
    /// Install/update runtime service file and optionally enable it (must be run as root)
    RuntimeSetupPrivileged {
        /// Username to run the service as
        #[arg(long)]
        user: String,

        /// User's home directory
        #[arg(long)]
        home: String,

        /// Path to the m87 executable
        #[arg(long)]
        exe_path: String,

        /// Enable service to start on boot
        #[arg(long)]
        enable: bool,

        /// Enable and start the service immediately
        #[arg(long)]
        enable_now: bool,

        /// Only restart if service was already running
        #[arg(long)]
        restart_if_running: bool,
    },

    /// Stop the runtime service (must be run as root)
    RuntimeStopPrivileged,

    /// Disable the runtime service (must be run as root)
    RuntimeDisablePrivileged {
        /// Also stop the service immediately
        #[arg(long)]
        now: bool,
    },
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
    // if is runtime run aso set to verbose
    let is_run = match &cli.command {
        #[cfg(feature = "runtime")]
        Commands::Runtime(RuntimeCommands::Run { .. }) => true,
        _ => false,
    };
    if cli.verbose || is_run {
        init_logging("info");
    } else {
        init_logging("warn");
    }
    set_tls_provider();

    match cli.command {
        Commands::Login => {
            tracing::info!("Logging in...");
            auth::login_cli().await?;
            tracing::info!("Logged in successfully");
        }

        Commands::Logout => {
            tracing::info!("Logging out...");
            auth::logout_cli().await?;
            tracing::info!("Logged out successfully");
        }

        Commands::Ssh(cmd) => match cmd {
            SshCommands::Enable => {
                tracing::info!("Enabling SSH...");
                device::ssh::ssh_enable()?;
                tracing::info!(
                    "SSH enabled successfully. You can now connect to device via ssh <device_name>.m87"
                );
            }
            SshCommands::Disable => {
                tracing::info!("Disabling SSH...");
                device::ssh::ssh_disable()?;
                tracing::info!("SSH disabled successfully");
            }
            SshCommands::Connect(args) => {
                if args.is_empty() {
                    anyhow::bail!("missing ssh target");
                }

                let mut transport = false;
                let mut positional = Vec::new();

                for arg in args {
                    if arg == "--transport" {
                        transport = true;
                    } else {
                        positional.push(arg);
                    }
                }

                let host = positional.get(0).context("missing ssh host")?;

                let _user = positional.get(1); // ignored for now

                let device = host.strip_suffix(".m87").unwrap_or(host);
                println!("Connecting to device {}", device);
                tracing::info!("[done]");
                if transport {
                    // INTERNAL: ProxyCommand path
                    device::ssh::connect_device_ssh(device).await?;
                } else {
                    // USER: behave exactly like ssh
                    device::ssh::exec_ssh(host, &positional[1..])?;
                }
            }
        },

        #[cfg(feature = "runtime")]
        Commands::Runtime(cmd) => match cmd {
            RuntimeCommands::Login { org_id, email } => {
                let owner_scope = org_id.or(email);
                tracing::info!("Registering device as runtime...");
                let sysinfo = util::system_info::get_system_info().await?;
                auth::register_device(owner_scope, sysinfo).await?;
                tracing::info!("Device registered as runtime successfully");
            }
            RuntimeCommands::Logout => {
                auth::logout_device().await?;
                tracing::info!("Logged out successfully");
            }
            RuntimeCommands::Run { org_id, email } => {
                save_owner_if_provided(org_id, email)?;
                crate::runtime::run().await?;
            }
            RuntimeCommands::Start { org_id, email } => {
                save_owner_if_provided(org_id, email)?;
                crate::runtime::start().await?;
            }
            RuntimeCommands::Stop => {
                crate::runtime::stop().await?;
            }
            RuntimeCommands::Restart { org_id, email } => {
                save_owner_if_provided(org_id, email)?;
                crate::runtime::restart().await?;
            }
            RuntimeCommands::Enable { now, org_id, email } => {
                save_owner_if_provided(org_id, email)?;
                crate::runtime::enable(now).await?;
            }
            RuntimeCommands::Disable { now } => {
                crate::runtime::disable(now).await?;
            }
            RuntimeCommands::Status => {
                crate::runtime::status().await?;
            }
        },

        #[cfg(feature = "runtime")]
        Commands::Internal(cmd) => match cmd {
            InternalCommands::RuntimeSetupPrivileged {
                user,
                home,
                exe_path,
                enable,
                enable_now,
                restart_if_running,
            } => {
                crate::runtime::internal_setup_privileged(
                    &user,
                    &home,
                    &exe_path,
                    enable,
                    enable_now,
                    restart_if_running,
                )
                .await?;
            }
            InternalCommands::RuntimeStopPrivileged => {
                crate::runtime::internal_stop_privileged().await?;
            }
            InternalCommands::RuntimeDisablePrivileged { now } => {
                crate::runtime::internal_disable_privileged(now).await?;
            }
        },

        Commands::Devices(cmd) => match cmd {
            DevicesCommands::List => {
                let devices = devices::list_devices().await?;
                let requests = auth::list_auth_requests().await?;
                tui::device::print_devices_table(&devices, &requests);
            }
            DevicesCommands::Show { device } => {
                eprintln!("Error: 'devices show' command is not yet implemented");
                eprintln!("Would show details for device: {}", device);
                bail!("Not implemented");
            }
            DevicesCommands::Approve { device } => {
                tracing::info!("Approving device: {}", device);
                auth::accept_auth_request(&device).await?;
                tracing::info!("Device approved successfully");
            }
            DevicesCommands::Reject { device } => {
                tracing::info!("Rejecting device: {}", device);
                auth::reject_auth_request(&device).await?;
                tracing::info!("Device rejected successfully");
            }
        },

        Commands::Version => {
            tracing::info!("[done]");
            println!("Version: {}", env!("CARGO_PKG_VERSION"));
            println!("Build: {}", env!("GIT_COMMIT"));
            println!("Rust: {}", env!("RUSTC_VERSION"));
            println!(
                "Platform: {}/{}",
                std::env::consts::OS,
                std::env::consts::ARCH
            );
        }

        Commands::Update => {
            update::update(true).await?;
        }

        Commands::Cp { source, dest } => {
            let _ = device::fs::copy(&source, &dest).await?;
        }

        Commands::Sync {
            source,
            dest,
            delete,
            watch,
            dry_run,
            exclude,
        } => {
            if watch {
                if dry_run {
                    anyhow::bail!("--dry-run cannot be used with --watch");
                }
                device::fs::watch_sync(&source, &dest, delete, &exclude).await?;
            } else {
                device::fs::sync(&source, &dest, delete, dry_run, &exclude).await?;
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

        Commands::Config(cmd) => match cmd {
            ConfigCommands::Set {
                runtime_server_url,
                owner_reference,
                make87_api_url,
                make87_app_url,
                trust_invalid_server_cert,
            } => {
                let mut cfg = Config::load().context("Failed to load config")?;

                if let Some(url) = runtime_server_url {
                    cfg.runtime_server_url = Some(url);
                }

                if let Some(owner) = owner_reference {
                    cfg.owner_reference = Some(owner);
                }

                if let Some(url) = make87_api_url {
                    cfg.make87_api_url = url;
                }

                if let Some(url) = make87_app_url {
                    cfg.make87_app_url = url;
                }

                if let Some(trust) = trust_invalid_server_cert {
                    cfg.trust_invalid_server_cert = trust;
                }

                cfg.save().context("Failed to save config")?;
                tracing::info!("Config updated");
            }
            ConfigCommands::Show => {
                let cfg = Config::load().context("Failed to load config")?;
                tracing::info!("Config laoded");
                println!("{:#?}", cfg);
            }
            ConfigCommands::File => {
                let path = Config::config_file_path().context("Failed to get config path")?;
                tracing::info!("Config path loaded");
                println!("{:#?}", path);
            }
        },

        Commands::Org(cmd) => match cmd {
            OrgCommands::List => {
                let orgs = org::list_organizations().await?;
                tui::org::print_device_organizations(&orgs);
            }
            OrgCommands::Create { id, owner_email } => {
                let _ = org::create_organization(&id, &owner_email).await?;
                println!("Organization created");
            }
            OrgCommands::Delete { id } => {
                let _ = org::delete_organization(&id).await?;
                println!("Organization deleted");
            }
            OrgCommands::Update { id, new_id } => {
                let _ = org::update_organization(&id, &new_id).await?;
                println!("Organization updated");
            }
            OrgCommands::Members(action) => match action {
                MemberAction::List { org_id } => {
                    let members = org::list_members(org_id).await?;
                    tui::user::print_users(&members);
                }
                MemberAction::Add {
                    email,
                    org_id,
                    role,
                } => {
                    let _ = org::add_member(org_id, &email, role).await?;
                    println!("User added");
                }
                MemberAction::Update {
                    email,
                    org_id,
                    role,
                } => {
                    let _ = org::add_member(org_id, &email, role).await?;
                    println!("User updated");
                }
                MemberAction::Remove { org_id, email } => {
                    let _ = org::remove_member(org_id, &email).await?;
                    println!("User removed");
                }
            },
            OrgCommands::Devices(action) => match action {
                OrgDeviceAction::List { org_id } => {
                    let devices = org::list_devices(org_id).await?;
                    tui::device::print_devices_table(&devices, &vec![]);
                }
                OrgDeviceAction::Add {
                    org_id,
                    device_name: device_id,
                } => {
                    let _ = org::add_device(org_id, &device_id).await?;
                    println!("Device added");
                }
                OrgDeviceAction::Remove {
                    org_id,
                    device_name: device_id,
                } => {
                    let _ = org::remove_device(org_id, &device_id).await?;
                    println!("Device removed");
                }
            },
            // OrgCommands::Invites { action } => match action {
            //     InviteAction::List => {
            //         let invites = org::list_invites().await?;
            //         println!("{:#?}", invites);
            //     }
            //     InviteAction::Accept { invite_id } => {
            //         let invite = org::handle_invite(&invite_id, true).await?;
            //         println!("{:#?}", invite);
            //     }
            //     InviteAction::Reject { invite_id } => {
            //         let invite = org::handle_invite(&invite_id, false).await?;
            //         println!("{:#?}", invite);
            //     }
            // },
        },
        Commands::Mcp => {
            crate::mcp::run_mcp_server().await?;
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

        DeviceCommand::Forward { targets } => {
            forward::open_local_forward(&device, targets).await?;
            Ok(())
        }

        DeviceCommand::Docker { args } => {
            device::docker::run_docker_command(&device, args.clone()).await?;
            Ok(())
        }

        DeviceCommand::Logs { follow: _, tail: _ } => {
            tui::log::run_logs(&device).await?;
            Ok(())
        }

        DeviceCommand::Metrics => {
            tui::metric::run_metrics(&device).await?;
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

        DeviceCommand::Status => {
            let status = devices::get_device_status(&device).await?;
            tui::device::print_device_status(&device, &status);
            Ok(())
        }

        DeviceCommand::Audit {
            until,
            since,
            max,
            details,
        } => {
            let logs = devices::get_audit_logs(&device, until, since, max).await?;

            tracing::info!("Received audit logs");
            tui::device::print_deployment_reports(&logs, details);
            Ok(())
        }

        DeviceCommand::Access(action) => match action {
            AccessAction::List => {
                let users = devices::get_device_users(&device).await?;
                tui::user::print_users(&users);
                Ok(())
            }
            AccessAction::Add {
                email_or_org_id,
                role,
            } => {
                let _ = devices::add_access(&device, &email_or_org_id, role).await?;
                tracing::info!("Added access");
                Ok(())
            }
            AccessAction::Remove { email_or_org_id } => {
                let _ = devices::remove_access(&device, &email_or_org_id).await?;
                tracing::info!("Removed access");
                Ok(())
            }
            AccessAction::Update {
                email_or_org_id,
                role,
            } => {
                let _ = devices::update_access(&device, &email_or_org_id, role).await?;
                tracing::info!("Updated access");
                Ok(())
            }
        },

        DeviceCommand::Deploy(args) => {
            let _ = device::deploy::deploy_file(
                &device,
                args.file,
                args.r#type,
                args.name,
                args.deployment_id,
            )
            .await?;

            tracing::info!("Added job spec to deployment");
            Ok(())
        }

        DeviceCommand::Undeploy(args) => {
            let _ = device::deploy::undeploy_file(&device, args.job_id.clone(), args.deployment_id)
                .await?;

            tracing::info!("Removed {} from deployment", args.job_id);
            Ok(())
        }

        DeviceCommand::Deployment(cmd) => match cmd {
            DeploymentCommand::List => {
                let deployments = device::deploy::get_deployments(&device).await?;

                tracing::info!("Loaded deployments");
                tui::deploy::print_revision_list_short(&deployments);
                Ok(())
            }

            DeploymentCommand::New { active } => {
                let _active = active;

                let deployment = device::deploy::create_deployment(&device, active).await?;

                tracing::info!("Created deployment");
                tui::deploy::print_revision_verbose(&deployment);
                Ok(())
            }

            DeploymentCommand::Status {
                deployment_id,
                logs,
            } => {
                let deployment_id = match deployment_id {
                    Some(d) => d,
                    None => match device::deploy::get_active_deployment_id(&device).await? {
                        Some(d) => d,
                        None => {
                            tracing::error!(
                                "No active deployment set for device {}. You either need to activate one or specify an --deployment-id",
                                device
                            );
                            return Ok(());
                        }
                    },
                };
                let snapshot =
                    device::deploy::get_deployment_snapshot(&device, &deployment_id).await?;

                tracing::info!("Received deployment reports");
                let mut config = tui::helper::RenderOpts::default();
                config.show_logs_inline = logs;
                tui::deploy::print_deployment_status_snapshot(&snapshot, &config);
                Ok(())
            }

            DeploymentCommand::Show {
                deployment_id,
                yaml,
            } => {
                let deployment_id = match deployment_id {
                    Some(d) => d,
                    None => match device::deploy::get_active_deployment_id(&device).await? {
                        Some(d) => d,
                        None => {
                            tracing::error!(
                                "No active deployment set for device {}. You either need to activate one or specify an --deployment-id",
                                device
                            );
                            return Ok(());
                        }
                    },
                };
                let deployment = device::deploy::get_deployment(&device, &deployment_id).await?;
                tracing::info!("Loaded deployment");
                match yaml {
                    true => tui::deploy::print_revision_verbose(&deployment),
                    false => {
                        tui::deploy::print_revision_short_detail(&deployment);
                    }
                }
                Ok(())
            }

            DeploymentCommand::Rm {
                deployment_id,
                force,
            } => {
                if !force {
                    println!(
                        "Are you sure you want to remove deployment {}?",
                        deployment_id
                    );
                    println!("This action cannot be undone.");
                    println!("Type 'y' to confirm:");
                    let mut input = String::new();
                    std::io::stdin().read_line(&mut input).unwrap();
                    if input.trim() != "y" {
                        println!("Aborted.");
                        return Ok(());
                    }
                }
                let _ = device::deploy::remove_deployment(&device, deployment_id).await?;
                tracing::info!("Successfully removed deployment");
                Ok(())
            }

            DeploymentCommand::Active => {
                let deployment_id = device::deploy::get_active_deployment_id(&device).await?;
                match deployment_id {
                    Some(id) => tracing::info!("Active deployment ID: {}", id),
                    None => tracing::info!("No active deployment"),
                }
                Ok(())
            }

            DeploymentCommand::Activate { deployment_id } => {
                let _ = device::deploy::deployment_active_set(&device, deployment_id).await?;
                tracing::info!("Successfully activated deployment");

                Ok(())
            }

            DeploymentCommand::Clone {
                deployment_id,
                active,
            } => {
                let deployment =
                    device::deploy::clone_deployment(&device, deployment_id, active).await?;
                tracing::info!(
                    "Successfully cloned deployment. New ID {}",
                    deployment.id.clone().unwrap()
                );
                tui::deploy::print_revision_short(&deployment);
                Ok(())
            }

            DeploymentCommand::Update(args) => {
                // Validate intent: require at least one operation flag
                //

                let deployment = device::deploy::deployment_update(&device, args).await?;
                tracing::info!(
                    "Successfully updated deployment. New ID {}",
                    deployment.id.clone().unwrap()
                );
                tui::deploy::print_revision_short_detail(&deployment);
                Ok(())
            }
        },
    }
}
