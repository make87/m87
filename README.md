# make87-client

Unified CLI and agent for connecting edge systems to the make87 platform. Provides secure remote access, monitoring, and container orchestration for nodes running anywhere.

## Features

- **Agent Management**: Run a background daemon that connects to the make87 backend via WebSocket
- **Application Management**: Build, push, and run containerized applications
- **Stack Management**: Pull and watch stack configurations
- **Self-Update**: Update the CLI to the latest version
- **Log Management**: View and follow agent logs

## Installation

### From Source

```bash
cargo build --release
sudo cp target/release/m87 /usr/local/bin/
```

## Usage

### Agent Commands

The agent runs as a background daemon, connecting to the make87 backend via WebSocket to sync instructions, updates, and logs.

```bash
# Run the agent in foreground mode
m87 agent run --foreground

# Run the agent in background mode
m87 agent run

# Install the agent as a system service
m87 agent install

# Check agent status
m87 agent status

# Uninstall the agent service
m87 agent uninstall
```

### Application Commands

```bash
# Build an application
m87 app build [path]

# Push an application to the registry
m87 app push <name> [--version <version>]

# Run an application
m87 app run <name> [-- args...]
```

### Stack Commands

```bash
# Pull a stack configuration
m87 stack pull <name>

# Watch for stack changes
m87 stack watch <name>
```

### Other Commands

```bash
# Update the CLI to the latest version
m87 update

# View logs
m87 logs [--follow] [--lines <count>]

# Show version information
m87 version
```

## Architecture

The project is organized into the following modules:

- **agent**: Agent daemon and service management
- **app**: Application build, push, and run functionality
- **stack**: Stack configuration management
- **update**: Self-update functionality
- **logs**: Log viewing and management
- **config**: Configuration file management
- **backend**: WebSocket communication with the make87 backend

## Configuration

The agent stores its configuration in:

- Linux/macOS: `~/.config/m87/config.json`
- Windows: `%APPDATA%\m87\config.json`

Example configuration:

```json
{
  "backend_url": "wss://api.make87.io/ws",
  "agent_id": null,
  "log_level": "info"
}
```

## Development

### Building

```bash
cargo build
```

### Testing

```bash
cargo test
```

### Running

```bash
cargo run -- [command]
```
