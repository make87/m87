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

    /// Manage login profiles to switch between accounts
    ///
    /// Each profile keeps its own config and credentials, so you can stay
    /// logged into several accounts at once and switch without re-login.
    #[command(subcommand)]
    Profile(ProfileCommands),

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
enum ProfileCommands {
    /// List all profiles and their login state
    List,

    /// Show the name of the currently active profile
    Current,

    /// Create a new profile and switch to it (then run `m87 login`)
    Add {
        /// Name for the new profile
        name: String,
    },

    /// Switch to an existing profile
    Use {
        /// Name of the profile to switch to
        name: String,
    },

    /// Remove a profile and its stored credentials
    Remove {
        /// Name of the profile to remove
        name: String,
    },

    /// Rename an existing profile
    Rename {
        /// Current profile name
        old: String,
        /// New profile name
        new: String,
    },
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
    /// Unified logs view across services, observers, and jobs.
    ///
    /// Default (no flags): last 200 events from history, all units, all kinds.
    /// Pass an id to scope to one unit (service / observer / job-def) or to
    /// a specific job run-id. Use `--follow` to switch to a live observe
    /// stream instead of history.
    Logs(LogsArgs),
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

    /// Show device health.
    ///
    /// Without --since, prints the current snapshot (per-observe
    /// liveness/health, open incidents). With --since, also aggregates
    /// events over the window.
    ///
    /// Exit code: 0 = healthy, 1 = issues detected, 2 = command failed.
    Status(StatusArgs),

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

    /// Deploy a service, observer, or job definition (upsert by id).
    /// Pass --replace-all to atomically replace the entire state.
    Deploy(DeployArgs),

    /// Remove a unit from the device's spec by id
    Undeploy(UndeployArgs),

    /// Start a unit (runs startup steps). Works for services and observers.
    Start {
        /// Unit id
        id: String,
    },

    /// Stop a unit (runs stop steps). Works for services and observers.
    Stop {
        /// Unit id
        id: String,
        /// Skip stop steps and tear down immediately
        #[arg(long)]
        force: bool,
    },

    /// Pause a unit — suspends observe polling without stopping the process.
    Pause {
        /// Unit id
        id: String,
    },

    /// Resume a paused or stopped unit.
    Resume {
        /// Unit id
        id: String,
    },

    /// Restart a unit (stop then start).
    Restart {
        /// Unit id
        id: String,
        /// Skip stop steps
        #[arg(long)]
        force: bool,
    },

    /// List all services, observers, and job definitions in the active deployment
    Units {
        /// Output as JSON (serialized `DeploymentRevision`)
        #[arg(long)]
        json: bool,
    },

    /// Show the current step state of all units (or one unit): which steps passed,
    /// failed, or are pending, plus the latest health/liveness check result.
    /// Use --logs to include captured step output inline.
    ///
    /// For live streaming observe logs use: m87 <device> logs
    /// For historical step logs use:        m87 <device> logs [unit] --steps
    Health {
        /// Optional unit id — show state for this unit only
        unit: Option<String>,
        /// Include captured step output inline
        #[arg(long)]
        logs: bool,
        /// Output as JSON (serialized `DeploymentStatusSnapshot`)
        #[arg(long)]
        json: bool,
    },

    /// Show the raw YAML spec of what is currently deployed on this device.
    Spec {
        /// Output as JSON instead of YAML
        #[arg(long)]
        json: bool,
    },

    /// Trigger and inspect job runs
    #[command(subcommand)]
    Job(JobCommand),

    #[clap(subcommand)]
    Access(AccessAction),
}

// ---------------------------------------------------------------------------
// Job commands
// ---------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum JobCommand {
    /// Trigger a run of a job definition
    Trigger {
        /// Job definition id
        id: String,
        /// Environment variable overrides in KEY=value format
        #[arg(long = "env", value_parser = parse_kv_pair, action = clap::ArgAction::Append)]
        env: Vec<(String, String)>,
        /// Output the triggered `JobRun` as JSON
        #[arg(long)]
        json: bool,
    },
    /// List job runs for this device (optionally filter by job definition id)
    List {
        /// Filter by job definition id
        #[arg(long)]
        job: Option<String>,
        /// Output as JSON (serialized `Vec<JobRun>`)
        #[arg(long)]
        json: bool,
    },
    /// Show status of a specific job run
    Status {
        /// Job run id
        run_id: String,
        /// Output as JSON (serialized `JobRun`)
        #[arg(long)]
        json: bool,
    },
    /// Show step execution logs for a specific job run
    Logs {
        /// Job run id
        run_id: String,
        /// Output as JSON (serialized `Vec<DeployReport>`)
        #[arg(long)]
        json: bool,
    },
    /// List all job definitions in the active deployment
    Defs {
        /// Output as JSON (serialized `Vec<JobDef>`)
        #[arg(long)]
        json: bool,
    },
}

fn parse_kv_pair(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| format!("expected KEY=value, got '{s}'"))
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
pub struct LogsArgs {
    /// Unit id (service, observer, or job-def) — or a job run id.
    /// If omitted, shows events across all units.
    pub id: Option<String>,

    /// Restrict to service + observer events only.
    /// Composable with --jobs; if neither is set, both are included.
    #[arg(long)]
    pub services: bool,

    /// Restrict to job (def + run) events only.
    /// Composable with --services; if neither is set, both are included.
    #[arg(long)]
    pub jobs: bool,

    /// Only show failed events.
    #[arg(long)]
    pub failed: bool,

    /// Start of the window: `30m`, `1h`, `24h`, `7d`, or an absolute
    /// timestamp like `2026-05-25T13:00:00Z`.
    #[arg(long)]
    pub since: Option<String>,

    /// End of the window (defaults to now). Same formats as --since.
    #[arg(long)]
    pub until: Option<String>,

    /// Last N events (default 200 for history; ignored with --follow).
    #[arg(short = 'n', long, default_value = "200")]
    pub tail: usize,

    /// Switch to a live observe stream instead of history.
    /// Not compatible with --jobs (jobs have no live stream).
    #[arg(short = 'f', long)]
    pub follow: bool,

    /// Expand each event's captured stdout/stderr tail underneath the row.
    #[arg(long)]
    pub logs: bool,

    /// Output as NDJSON, one event per line.
    #[arg(long)]
    pub json: bool,
}

#[derive(Parser, Debug)]
pub struct StatusArgs {
    /// Start of the aggregation window: `30m`, `1h`, `24h`, `7d`, or an
    /// absolute timestamp. When omitted, only the current snapshot is shown.
    #[arg(long)]
    pub since: Option<String>,

    /// End of the aggregation window (defaults to now).
    #[arg(long)]
    pub until: Option<String>,

    /// One-line summary suitable for shell `if` conditions.
    /// Exit code mirrors the health state (0 ok, 1 issues).
    #[arg(short = 's', long)]
    pub short: bool,

    /// No output; exit code only (0 ok, 1 issues, 2 command failed).
    #[arg(short = 'q', long)]
    pub quiet: bool,

    /// Output the full summary as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Parser, Debug)]
pub struct DeployArgs {
    /// File to deploy: docker-compose.yml, a single service / observer / job YAML,
    /// or a full revision YAML with services / observers / jobs sections.
    /// The type is auto-detected; use --replace-all to atomically replace the
    /// entire device state from a full revision file.
    pub file: PathBuf,

    /// Spec type (auto detects by default)
    #[arg(long, value_enum, default_value_t = SpecType::Auto)]
    pub r#type: SpecType,

    /// Optional display name for the run spec
    #[arg(long)]
    pub name: Option<String>,

    /// Atomically replace the entire device state with this file
    #[arg(long)]
    pub replace_all: bool,
}

#[derive(Parser, Debug)]
pub struct UndeployArgs {
    /// Unit id to remove from the device's spec.
    pub job_id: String,
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
    List {
        /// Output as JSON: `{"devices": [...], "auth_requests": [...]}`
        #[arg(long)]
        json: bool,
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
            DevicesCommands::List { json } => {
                let devices = devices::list_devices().await?;
                let requests = auth::list_auth_requests().await?;
                if json {
                    let combined = serde_json::json!({
                        "devices": devices,
                        "auth_requests": requests,
                    });
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&combined)
                            .context("failed to serialize devices+requests as JSON")?
                    );
                } else {
                    tui::device::print_devices_table(&devices, &requests);
                }
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
        Commands::Profile(cmd) => handle_profile_command(cmd)?,

        Commands::Mcp => {
            crate::mcp::run_mcp_server().await?;
        }
    }

    Ok(())
}

fn handle_profile_command(cmd: ProfileCommands) -> anyhow::Result<()> {
    use crate::config::profile;

    match cmd {
        ProfileCommands::List => {
            let profiles = profile::list_profiles()?;
            println!(
                "  {:<3} {:<20} {:<10} {}",
                "", "PROFILE", "LOGGED IN", "OWNER"
            );
            for p in profiles {
                let marker = if p.active { "*" } else { " " };
                let logged_in = if p.logged_in { "yes" } else { "no" };
                let owner = p.owner_reference.as_deref().unwrap_or("-");
                println!(
                    "  {:<3} {:<20} {:<10} {}",
                    marker, p.name, logged_in, owner
                );
            }
            println!("\n* = active profile");
        }

        ProfileCommands::Current => {
            println!("{}", profile::active_profile()?);
        }

        ProfileCommands::Add { name } => {
            profile::create_profile(&name)?;
            profile::switch_profile(&name)?;
            println!("Created and switched to profile '{name}'.");
            println!("Run `m87 login` to authenticate this profile.");
        }

        ProfileCommands::Use { name } => {
            profile::switch_profile(&name)?;
            println!("Switched to profile '{name}'.");
            if !profile::list_profiles()?
                .into_iter()
                .any(|p| p.name == name && p.logged_in)
            {
                println!("This profile is not logged in yet. Run `m87 login`.");
            }
        }

        ProfileCommands::Remove { name } => {
            profile::remove_profile(&name)?;
            println!("Removed profile '{name}'.");
        }

        ProfileCommands::Rename { old, new } => {
            profile::rename_profile(&old, &new)?;
            println!("Renamed profile '{old}' to '{new}'.");
        }
    }

    Ok(())
}

async fn handle_device_command(cmd: DeviceRoot) -> anyhow::Result<()> {
    use device::deploy as dp;
    use m87_shared::deploy_spec::Lifecycle;

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

        DeviceCommand::Logs(args) => {
            // Mode resolution:
            //   --follow                       → live stream
            //   any selector / filter / id /   → history (default 200 events)
            //     --json / --logs
            //   bare `logs` (or just --tail N) → live stream  (back-compat
            //                                     with the pre-unified `logs`
            //                                     command, which streamed by
            //                                     default and silently ignored
            //                                     --tail without --steps)
            //
            // `--json` and `--logs` count as history-implying because the
            // live stream isn't structured output and doesn't have per-event
            // tails to expand. `--tail` is intentionally treated as bare-live
            // compatible so older `m87 dev logs --tail 10` invocations on a
            // device with no spec continue to succeed (return empty stream).
            let bare_invocation = args.id.is_none()
                && !args.services
                && !args.jobs
                && !args.failed
                && args.since.is_none()
                && args.until.is_none()
                && !args.json
                && !args.logs;

            if args.follow || bare_invocation {
                if args.follow {
                    if args.jobs {
                        bail!("--follow is not compatible with --jobs (jobs have no live stream)");
                    }
                    if args.failed || args.since.is_some() || args.until.is_some() {
                        bail!(
                            "--follow shows the live observe stream; --failed / --since / --until \
                             only apply to history. Omit --follow to query history."
                        );
                    }
                }
                if let Some(ref uid) = args.id {
                    tracing::warn!(
                        "Live log filtering by unit is not yet supported — streaming all logs. \
                         Drop --follow and pass `{uid}` to see this unit's recorded history."
                    );
                }
                tui::log::run_logs(&device).await?;
                return Ok(());
            }

            // History path.
            use crate::device::events::{aggregate_events, EventFilter};
            use crate::util::time::{now_ms, parse_time};

            let now = now_ms();
            let since_ms = args
                .since
                .as_deref()
                .map(|s| parse_time(s, now))
                .transpose()?;
            let until_ms = args
                .until
                .as_deref()
                .map(|s| parse_time(s, now))
                .transpose()?;

            let reports = dp::get_active_revision_reports(&device).await?;
            let filter = EventFilter {
                id: args.id.as_deref(),
                services: args.services,
                jobs: args.jobs,
                failed_only: args.failed,
                since_ms,
                until_ms,
            };
            let events = aggregate_events(reports, &filter, args.tail);

            if args.json {
                tui::events::print_events_ndjson(&events);
            } else {
                tui::events::print_events_table(&events, args.logs);
            }
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

        DeviceCommand::Status(args) => run_status(&device, args).await,

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
            if args.replace_all {
                dp::deploy_file_replace_all(&device, args.file).await?;
                tracing::info!("Replaced entire device spec");
            } else {
                dp::deploy_file(&device, args.file, args.r#type, args.name).await?;
                tracing::info!("Deployed unit to device");
            }
            Ok(())
        }

        DeviceCommand::Undeploy(args) => {
            dp::undeploy_file(&device, args.job_id.clone()).await?;
            tracing::info!("Removed {} from device spec", args.job_id);
            Ok(())
        }

        // ── Flat lifecycle commands ────────────────────────────────────────
        DeviceCommand::Start { id } => {
            dp::send_lifecycle(&device, &id, Lifecycle::Running).await?;
            tracing::info!("Start requested for '{id}' — will apply on next heartbeat");
            Ok(())
        }

        DeviceCommand::Stop { id, force: _ } => {
            dp::send_lifecycle(&device, &id, Lifecycle::Stopped).await?;
            tracing::info!("Stop requested for '{id}' — will apply on next heartbeat");
            Ok(())
        }

        DeviceCommand::Pause { id } => {
            dp::send_lifecycle(&device, &id, Lifecycle::Paused).await?;
            tracing::info!(
                "Pause requested for '{id}' — observe polling suspended on next heartbeat"
            );
            Ok(())
        }

        DeviceCommand::Resume { id } => {
            dp::send_lifecycle(&device, &id, Lifecycle::Running).await?;
            tracing::info!("Resume requested for '{id}' — will apply on next heartbeat");
            Ok(())
        }

        DeviceCommand::Restart { id, force: _ } => {
            // Two sequential lifecycle updates: server queues both, device
            // applies them in order across successive heartbeat ticks.
            dp::send_lifecycle(&device, &id, Lifecycle::Stopped).await?;
            dp::send_lifecycle(&device, &id, Lifecycle::Running).await?;
            tracing::info!("Restart requested for '{id}'");
            Ok(())
        }

        DeviceCommand::Units { json } => {
            let rev = dp::get_active_revision(&device).await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&rev)
                        .context("failed to serialize revision as JSON")?
                );
            } else {
                tui::deploy::print_units_list(&rev);
            }
            Ok(())
        }

        // ── Health snapshot ────────────────────────────────────────────────
        // Current step state (pending/success/failed) + health/liveness.
        // NOT the same as:
        //   status      — device connectivity + observation history
        //   logs --steps — historical step log entries
        DeviceCommand::Health { unit, logs, json } => {
            let deployment_id = dp::get_active_deployment_id(&device)
                .await?
                .context("no active deployment on device")?;
            let snapshot = dp::get_deployment_snapshot(&device, &deployment_id).await?;
            let mut opts = tui::helper::RenderOpts::default();
            opts.show_logs_inline = logs;
            // Filter snapshot to a single unit if requested
            let filtered = match &unit {
                None => snapshot,
                Some(id) => {
                    use m87_shared::deploy_spec::DeploymentStatusSnapshot;
                    let runs = snapshot
                        .runs
                        .into_iter()
                        .filter(|r| &r.run_id == id)
                        .collect();
                    DeploymentStatusSnapshot { runs, ..snapshot }
                }
            };
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&filtered)
                        .context("failed to serialize snapshot as JSON")?
                );
            } else {
                tui::deploy::print_deployment_status_snapshot(&filtered, &opts);
            }
            Ok(())
        }

        // ── Spec viewer ────────────────────────────────────────────────────
        // The raw spec that is deployed — what services/observers/jobs are
        // defined. Use `units` for the runtime lifecycle view.
        DeviceCommand::Spec { json } => {
            let rev = dp::get_active_revision(&device).await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&rev)
                        .context("failed to serialize revision as JSON")?
                );
            } else {
                tui::deploy::print_revision_verbose(&rev);
            }
            Ok(())
        }

        // ───────────────────────────────────────────────────────────────────
        // NOTE: the multi-revision `deployment` subcommand and the `rollback`
        // command have been removed. Each device has a single in-place spec
        // edited by `m87 <dev> deploy` / `m87 <dev> undeploy`. Use `spec` /
        // `units` to inspect it.
        // ───────────────────────────────────────────────────────────────────

        /* removed_block_start
                let deployments = device::deploy::get_deployments(&device).await?;

                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&deployments)
                            .context("failed to serialize deployments as JSON")?
                    );
                } else {
                    tracing::info!("Loaded deployments");
                    tui::deploy::print_revision_list_short(&deployments);
                }
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
                // Hint: prefer `m87 <device> health [--logs]` for the active deployment.
                // This command still works for inspecting non-active revisions by id.
                let deployment_id = match deployment_id {
                    Some(d) => d,
                    None => match device::deploy::get_active_deployment_id(&device).await? {
                        Some(d) => d,
                        None => {
                            tracing::error!("No active deployment. Try: m87 {} health", device);
                            return Ok(());
                        }
                    },
                };
                let snapshot =
                    device::deploy::get_deployment_snapshot(&device, &deployment_id).await?;
                let mut config = tui::helper::RenderOpts::default();
                config.show_logs_inline = logs;
                tui::deploy::print_deployment_status_snapshot(&snapshot, &config);
                Ok(())
            }

            DeploymentCommand::Show {
                deployment_id,
                yaml,
            } => {
                // Hint: prefer `m87 <device> spec` for the active deployment.
                // This command still works for inspecting non-active revisions by id.
                let deployment_id = match deployment_id {
                    Some(d) => d,
                    None => match device::deploy::get_active_deployment_id(&device).await? {
                        Some(d) => d,
                        None => {
                            tracing::error!("No active deployment. Try: m87 {} spec", device);
                            return Ok(());
                        }
                    },
                };
                let deployment = device::deploy::get_deployment(&device, &deployment_id).await?;
                match yaml {
                    true => tui::deploy::print_revision_verbose(&deployment),
                    false => tui::deploy::print_revision_short_detail(&deployment),
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
                let deployment = device::deploy::deployment_update(&device, args).await?;
                tracing::info!(
                    "Successfully updated deployment. New ID {}",
                    deployment.id.clone().unwrap()
                );
                tui::deploy::print_revision_short_detail(&deployment);
                Ok(())
            }
        },
        */

        // ── Job commands ───────────────────────────────────────────────────
        DeviceCommand::Job(cmd) => match cmd {
            JobCommand::Trigger { id, env, json } => {
                use std::collections::BTreeMap;
                let env_overrides: BTreeMap<String, String> = env.into_iter().collect();
                let run = dp::trigger_job(&device, &id, env_overrides).await?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&run)
                            .context("failed to serialize job run as JSON")?
                    );
                } else {
                    tracing::info!("Triggered job '{}' → run id: {}", id, run.run_id);
                    tui::deploy::print_job_run(&run);
                }
                Ok(())
            }
            JobCommand::List { job, json } => {
                let runs = dp::list_job_runs(&device, job.as_deref()).await?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&runs)
                            .context("failed to serialize job runs as JSON")?
                    );
                } else {
                    tui::deploy::print_job_run_list(&runs);
                }
                Ok(())
            }
            JobCommand::Status { run_id, json } => {
                let run = dp::get_job_run(&device, &run_id).await?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&run)
                            .context("failed to serialize job run as JSON")?
                    );
                } else {
                    tui::deploy::print_job_run(&run);
                }
                Ok(())
            }
            JobCommand::Logs { run_id, json } => {
                let reports = dp::get_unit_step_logs(&device, Some(&run_id)).await?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&reports)
                            .context("failed to serialize step logs as JSON")?
                    );
                } else {
                    tui::deploy::print_step_logs(Some(&run_id), &reports);
                }
                Ok(())
            }
            JobCommand::Defs { json } => {
                let rev = dp::get_active_revision(&device).await?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&rev.jobs)
                            .context("failed to serialize job defs as JSON")?
                    );
                } else {
                    tui::deploy::print_job_defs_list(&rev);
                }
                Ok(())
            }
        },

    }
}

// ---------------------------------------------------------------------------
// `m87 <device> status` — runs the snapshot + optional windowed aggregate
// and exits with a status-coded process result:
//   0 = healthy
//   1 = issues detected (anything in `current_issues` or window failures)
//   2 = command itself failed (handled by the caller's `?` propagation)
// ---------------------------------------------------------------------------

async fn run_status(device: &str, args: StatusArgs) -> anyhow::Result<()> {
    use crate::device::deploy as dp;
    use crate::device::events::{aggregate_events, EventFilter};
    use crate::device::status::{attach_window, summarize};
    use crate::util::time::{now_ms, parse_time};

    let status = devices::get_device_status(device).await?;
    let mut summary = summarize(device, &status);

    // Optional windowed aggregate.
    if args.since.is_some() || args.until.is_some() {
        let now = now_ms();
        let since_ms = args
            .since
            .as_deref()
            .map(|s| parse_time(s, now))
            .transpose()?
            .unwrap_or(0);
        let until_ms = args
            .until
            .as_deref()
            .map(|s| parse_time(s, now))
            .transpose()?
            .unwrap_or(now);

        // Best-effort: if there's no active deployment we just leave the
        // window out instead of failing the whole status call.
        if let Ok(reports) = dp::get_active_revision_reports(device).await {
            let filter = EventFilter {
                since_ms: Some(since_ms),
                until_ms: Some(until_ms),
                ..Default::default()
            };
            let events = aggregate_events(reports, &filter, 0);
            attach_window(&mut summary, &events, since_ms, until_ms);
        }
    }

    let healthy = summary.is_healthy();

    if args.quiet {
        // No output. Exit code only.
    } else if args.json {
        let json = serde_json::to_string_pretty(&summary)
            .context("failed to serialize status summary as JSON")?;
        println!("{json}");
    } else if args.short {
        println!("{}", summary.short_line());
    } else {
        // Default: pretty current-state table (existing renderer) plus a
        // window section when present. The existing renderer takes the raw
        // server `DeviceStatus`, not our summary, so we hand it through.
        tui::device::print_device_status(device, &status);
        if let Some(w) = &summary.window {
            use crate::util::time::format_ms;
            println!();
            println!(
                "Window  {} → {}   ({} events, {} failures)",
                format_ms(w.since_ms),
                format_ms(w.until_ms),
                w.total_events,
                w.total_failures
            );
            if w.units.is_empty() {
                println!("  (no events in window)");
            } else {
                println!(
                    "  {:<22}  {:<10}  {:>8}  {:>8}  {:>6}  {}",
                    "UNIT", "KIND", "FAILURES", "SUCCESS", "STARTS", "LAST"
                );
                for u in &w.units {
                    let last = u
                        .last_event_ms
                        .map(|t| format_ms(t))
                        .unwrap_or_else(|| "-".to_string());
                    println!(
                        "  {:<22}  {:<10}  {:>8}  {:>8}  {:>6}  {}",
                        u.unit, u.category, u.failures, u.successes, u.starts, last
                    );
                }
            }
        }
    }

    if !healthy {
        std::process::exit(1);
    }
    Ok(())
}
