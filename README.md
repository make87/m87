# gravity

Unified CLI and agent for the **make87** platform — enabling secure remote access, monitoring, and container orchestration for edge systems anywhere.

## Features

- **Agent Management** – Run or install the background agent daemon that connects to the make87 backend
- **Application Management** – Build, push, and run containerized applications
- **Stack Management** – Pull and run versioned Docker Compose files that you define on make87
- **Authentication** – Log in, register, and manage credentials
- **Self-Update** – Seamlessly update the CLI to the latest release

## Installation

### From Source

```bash
cargo build --release
sudo cp target/release/gravity /usr/local/bin/
```

## Usage

### Agent Commands

The agent runs as a background daemon connecting to the make87 backend via WebSocket to sync instructions, logs, and updates.

```bash
# Run the agent interactively
gravity agent run

# Run the agent in headless mode (non-interactive)
gravity agent run --headless

# Install the agent as a system service
gravity agent install

# Check service status
gravity agent status

# Uninstall the agent service
gravity agent uninstall
```

Optional flags for `run` and `install`:

```bash
--user-email <email>
--organization-id <org_id>
```

### Application Commands

```bash
# Build an application (defaults to current directory)
gravity app build [path]

# Push an application to the registry
gravity app push <name> [--version <version>]

# Run an application with optional args
gravity app run <name> [-- args...]
```

### Stack Commands

```bash
# Pull a compose by reference (name:version)
gravity stack pull <name>:<version>

# Run and watch a compose (apply updates)
gravity stack watch <name>
```

### Authentication Commands

```bash
# Log in via OAuth or stored credentials
gravity auth login

# Register a new user or organization
gravity auth register [--user-email <email>] [--organization-id <org_id>]

# Show authentication status
gravity auth status

# Log out and clear credentials
gravity auth logout
```

### Other Commands

```bash
# Update gravity to the latest version
gravity update

# Show version info
gravity version
```

## Architecture

Modules overview:

- **agent** – Agent runtime and system service logic
- **app** – Application build/push/run handling
- **stack** – Stack synchronization and watcher
- **auth** – Login, registration, and token management
- **update** – Self-update logic
- **config** – Config file management
- **server / util** – Shared backend and helper utilities

## Configuration

Configuration is stored in:

- **Linux/macOS**: `~/.config/gravity/config.json`
- **Windows**: `%APPDATA%\gravity\config.json`

Example:

```json
{
  "api_url": "https://api.make87.com",
  "node_id": null,
  "log_level": "info"
}
```

## Development

### Build

```bash
cargo build
```

### Test

```bash
cargo test
```

### Run

```bash
cargo run -- [command]
```
