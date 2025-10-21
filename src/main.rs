use clap::{Parser, Subcommand};
use tracing_subscriber;

mod agent;
mod app;
mod backend;
mod config;
mod logs;
mod stack;
mod update;

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
    
    /// Application management commands
    #[command(subcommand)]
    App(AppCommands),
    
    /// Stack management commands
    #[command(subcommand)]
    Stack(StackCommands),
    
    /// Update the m87 CLI to the latest version
    Update,
    
    /// View and manage logs
    Logs {
        /// Follow log output
        #[arg(short, long)]
        follow: bool,
        
        /// Number of lines to show
        #[arg(short, long, default_value = "100")]
        lines: usize,
    },
    
    /// Show version information
    Version,
}

#[derive(Subcommand)]
enum AgentCommands {
    /// Run the agent daemon
    Run {
        /// Run in foreground mode
        #[arg(short, long)]
        foreground: bool,
    },
    
    /// Install the agent as a system service
    Install,
    
    /// Uninstall the agent service
    Uninstall,
    
    /// Check agent status
    Status,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Agent(cmd) => match cmd {
            AgentCommands::Run { foreground } => agent::run(foreground).await?,
            AgentCommands::Install => agent::install().await?,
            AgentCommands::Uninstall => agent::uninstall().await?,
            AgentCommands::Status => agent::status().await?,
        },
        Commands::App(cmd) => match cmd {
            AppCommands::Build { path } => app::build(&path).await?,
            AppCommands::Push { name, version } => app::push(&name, version.as_deref()).await?,
            AppCommands::Run { name, args } => app::run(&name, &args).await?,
        },
        Commands::Stack(cmd) => match cmd {
            StackCommands::Pull { name } => stack::pull(&name).await?,
            StackCommands::Watch { name } => stack::watch(&name).await?,
        },
        Commands::Update => update::update().await?,
        Commands::Logs { follow, lines } => logs::view(follow, lines).await?,
        Commands::Version => {
            println!("m87 version {}", env!("CARGO_PKG_VERSION"));
        }
    }
    
    Ok(())
}
