use anyhow::Ok;
use clap::{Parser, Subcommand};
use tracing_subscriber::{fmt, EnvFilter};

mod agent;
mod app;
mod auth;
mod config;
mod node;
mod rest;
mod server;
mod stack;
mod update;
mod util;

#[derive(Parser)]
#[command(name = "m87")]
#[command(version, about = "Unified CLI and agent for the make87 platform", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Agent management commands
    #[command(subcommand)]
    Agent(AgentCommands),

    /// Node management commands
    #[command(subcommand)]
    Node(NodeCommands),

    /// Application management commands
    #[command(subcommand)]
    App(AppCommands),

    /// Stack management commands
    #[command(subcommand)]
    Stack(StackCommands),

    /// Update the m87 CLI to the latest version
    Update,

    /// Authentication commands
    #[command(subcommand)]
    Auth(AuthCommands),

    /// Show version information
    Version,
}

#[derive(Subcommand)]
enum NodeCommands {
    /// List all nodes
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
    /// Connect to a node via SSH
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
enum AgentCommands {
    /// Run the agent daemon
    Run {
        #[arg(short, long)]
        user_email: Option<String>,
        #[arg(short, long)]
        organization_id: Option<String>,
    },

    /// Install the agent as a system service
    Install {
        #[arg(short, long)]
        user_email: Option<String>,
        #[arg(short, long)]
        organization_id: Option<String>,
    },

    /// Uninstall the agent service
    Uninstall,

    /// Check agent status
    Status,
    /// Get credentials for the agent
    Login {
        #[arg(short, long)]
        user_email: Option<String>,
        #[arg(short, long)]
        organization_id: Option<String>,
    },
    /// Remove the credentials for the agent
    Logout,
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
enum AuthCommands {
    /// Log in to the platform
    Login,

    /// Check authentication status
    Status,

    /// Log out of the platform
    Logout,

    /// Manage authentication requests
    #[command(subcommand)]
    Request(AuthRequestCommands),
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    // fmt()
    //     .with_env_filter(
    //         EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    //     )
    //     .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Agent(cmd) => match cmd {
            AgentCommands::Run {
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
                agent::run(owner_ref).await?
            }
            AgentCommands::Install {
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
                agent::install(owner_ref).await?
            }
            AgentCommands::Uninstall => agent::uninstall().await?,
            AgentCommands::Status => agent::status().await?,
            AgentCommands::Logout => auth::logout_agent().await?,
            AgentCommands::Login {
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
                auth::login_agent(owner_ref).await?
            }
        },
        Commands::Node(cmd) => match cmd {
            NodeCommands::List => {
                let nodes = node::list_nodes().await?;
                println!("{:?}", nodes);
                Ok(())
            }
            NodeCommands::Metrics { id } => node::metrics(&id).await,
            NodeCommands::Ssh(cmd) => match cmd {
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
        Commands::Auth(cmd) => match cmd {
            AuthCommands::Login => {
                // Inline the previous backend::auth wrapper behavior and call the auth manager directly.
                auth::login_cli().await?
            }
            AuthCommands::Status => auth::status().await?,
            AuthCommands::Logout => auth::logout_cli().await?,
            AuthCommands::Request(cmd) => match cmd {
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
