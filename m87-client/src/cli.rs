use anyhow::Ok;
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
    /// Local device management commands
    #[command(subcommand)]
    Device(DeviceCommands),

    /// Remote devices management commands
    #[command(subcommand)]
    Devices(DevicesCommands),

    /// Application management commands
    #[command(subcommand)]
    App(AppCommands),

    /// Stack management commands
    #[command(subcommand)]
    Stack(StackCommands),

    /// Update the m87 CLI to the latest version
    Update,

    /// Command to manage server and authenticate agianst it
    #[command(subcommand)]
    Server(ServerCommands),

    /// Show version information
    Version,
}

#[derive(Subcommand)]
enum ConfigCommands {
    /// Clear all config from the system
    Clear,
}

#[derive(Subcommand)]
enum DevicesCommands {
    /// List all devices
    List,

    /// SSH commands
    #[command(subcommand)]
    Ssh(SSHCommands),

    Metrics {
        #[arg(short, long)]
        id: String,
    },
}

#[derive(Subcommand)]
enum SSHCommands {
    /// Connect to a device via SSH
    Connect {
        #[arg(short, long)]
        id: String,
    },

    Url {
        #[arg(short, long)]
        id: String,
    },
}

#[derive(Subcommand)]
enum DeviceCommands {
    /// Run the device daemon
    Run {
        #[arg(short, long)]
        user_email: Option<String>,
        #[arg(short, long)]
        organization_id: Option<String>,
    },

    /// Install the device as a system service
    Install {
        #[arg(short, long)]
        user_email: Option<String>,
        #[arg(short, long)]
        organization_id: Option<String>,
    },

    /// Uninstall the device service
    Uninstall,

    /// Check device status
    Status,
    /// Get credentials for the device
    Register {
        #[arg(short, long)]
        user_email: Option<String>,
        #[arg(short, long)]
        organization_id: Option<String>,
    },
    /// Remove the credentials for the device
    Unregister,

    /// Configuration management commands
    #[command(subcommand)]
    Config(ConfigCommands),
}

#[derive(Subcommand)]
enum AppCommands {
    /// Build an application
    Build {
        /// Path to the application directory
        #[arg(default_value = ".")]
        path: String,
    },

    /// Push an application to the registry
    Push {
        /// Application name
        name: String,

        /// Application version
        #[arg(short, long)]
        version: Option<String>,
    },

    /// Run an application
    Run {
        /// Application name
        name: String,

        /// Additional arguments to pass to the application
        #[arg(last = true)]
        args: Vec<String>,
    },
}

#[derive(Subcommand)]
enum StackCommands {
    /// Pull a stack configuration
    Pull {
        /// Stack name
        name: String,
    },

    /// Watch for stack changes
    Watch {
        /// Stack name
        name: String,
    },
}

#[derive(Subcommand)]
enum ServerCommands {
    /// Log in to the platform
    Login,

    /// Check authentication status
    Status,

    /// Log out of the platform
    Logout,

    /// Manage authentication requests to the server
    #[command(subcommand)]
    Requests(AuthRequestCommands),
}

#[derive(Subcommand)]
enum AuthRequestCommands {
    /// Request a control tunnel token
    List,
    /// Accept a control tunnel token request
    Accept {
        /// the id of the request
        #[arg(long)]
        request_id: String,
    },
    /// Reject a control tunnel token request
    Reject {
        /// the id of the request
        #[arg(long)]
        request_id: String,
    },
}

pub async fn cli() -> anyhow::Result<()> {
    // Initialize tracing
    // fmt()
    //     .with_env_filter(
    //         EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    //     )
    //     .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Device(cmd) => match cmd {
            DeviceCommands::Config(cmd) => match cmd {
                ConfigCommands::Clear => config::Config::clear()?,
            },
            DeviceCommands::Run {
                user_email,
                organization_id,
            } => {
                let owner_ref = match user_email.is_some() {
                    true => user_email,
                    false => match organization_id.is_some() {
                        true => organization_id,
                        false => None,
                    },
                };
                device::run(owner_ref).await?
            }
            DeviceCommands::Install {
                user_email,
                organization_id,
            } => {
                let owner_ref = match user_email.is_some() {
                    true => user_email,
                    false => match organization_id.is_some() {
                        true => organization_id,
                        false => None,
                    },
                };
                device::install(owner_ref).await?
            }
            DeviceCommands::Uninstall => device::uninstall().await?,
            DeviceCommands::Status => device::status().await?,
            DeviceCommands::Unregister => auth::logout_device().await?,
            DeviceCommands::Register {
                user_email,
                organization_id,
            } => {
                let owner_ref = match user_email.is_some() {
                    true => user_email,
                    false => match organization_id.is_some() {
                        true => organization_id,
                        false => None,
                    },
                };
                let config = config::Config::load()?;
                let sysinfo = util::system_info::get_system_info(config.enable_geo_lookup).await?;
                auth::register_device(owner_ref, sysinfo).await?
            }
        },
        Commands::Devices(cmd) => match cmd {
            DevicesCommands::List => {
                let devices = devices::list_devices().await?;
                println!("{:?}", devices);
                Ok(())
            }
            DevicesCommands::Metrics { id } => devices::metrics(&id).await,
            DevicesCommands::Ssh(cmd) => match cmd {
                SSHCommands::Connect { id } => Ok(()),
                SSHCommands::Url { id } => Ok(()),
            },
        }?,
        Commands::App(cmd) => match cmd {
            AppCommands::Build { path } => app::build(&path).await?,
            AppCommands::Push { name, version } => app::push(&name, version.as_deref()).await?,
            AppCommands::Run { name, args } => app::run(&name, &args).await?,
        },
        Commands::Stack(cmd) => match cmd {
            StackCommands::Pull { name } => stack::pull(&name).await?,
            StackCommands::Watch { name } => stack::watch(&name).await?,
        },
        Commands::Update => {
            let success = update::update(true).await?;
            if success {
                println!("Update successful");
            } else {
                println!("Update failed");
            }
        }
        Commands::Server(cmd) => match cmd {
            ServerCommands::Login => {
                // Inline the previous backend::auth wrapper behavior and call the auth manager directly.
                auth::login_cli().await?
            }
            ServerCommands::Status => auth::status().await?,
            ServerCommands::Logout => auth::logout_cli().await?,
            ServerCommands::Requests(cmd) => match cmd {
                AuthRequestCommands::List => {
                    let requests = auth::list_auth_requests().await?;
                    println!("{:?}", requests);
                    Ok(())
                }
                AuthRequestCommands::Accept { request_id } => {
                    auth::accept_auth_request(&request_id).await
                }
                AuthRequestCommands::Reject { request_id } => {
                    auth::reject_auth_request(&request_id).await
                }
            }?,
        },
        Commands::Version => {
            println!("m87 version {}", env!("CARGO_PKG_VERSION"));
        }
    }

    Ok(())
}
