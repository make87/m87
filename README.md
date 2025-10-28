# make87 Community Platform

Unified tooling for the **make87** platform — enabling secure remote access, monitoring, and container orchestration for edge systems anywhere.

## Components

This repository contains two main components:

### 🖥️ m87 (Client)
Command-line interface and agent for edge systems. Provides tools for:
- Agent management and daemon operations
- Application building, pushing, and running
- Stack synchronization and management
- Authentication and credential management
- Self-update capabilities

[Learn more →](./m87-client/README.md)

### 🌐 m87-server (Server)
Server component for the make87 platform.

[Learn more →](./m87-server/README.md)

## Installation

### Quick Install (Recommended)

Install the m87 client with a single command:

```bash
curl -fsSL https://github.com/make87/make87/releases/latest/download/install.sh | bash
```

This will automatically:
- Detect your system architecture (x86_64 or ARM64)
- Download the latest release
- Verify the binary checksum
- Install to `/usr/local/bin/m87`

**Pin to a specific version:**
```bash
curl -fsSL https://github.com/make87/make87/releases/download/v1.2.3/install.sh | bash
```

> **Coming soon:** `curl -fsSL get.make87.com | bash`

### Pre-built Binaries

Download the latest release for your platform from the [Releases](../../releases) page:

**Linux x86_64 (AMD64):**
```bash
# Download m87 client
wget https://github.com/make87/make87/releases/latest/download/m87-x86_64-unknown-linux-gnu
chmod +x m87-x86_64-unknown-linux-gnu
sudo mv m87-x86_64-unknown-linux-gnu /usr/local/bin/m87

# Download m87-server
wget https://github.com/make87/make87/releases/latest/download/m87-server-x86_64-unknown-linux-gnu
chmod +x m87-server-x86_64-unknown-linux-gnu
sudo mv m87-server-x86_64-unknown-linux-gnu /usr/local/bin/m87-server
```

**Linux ARM64:**
```bash
# Download m87 client
wget https://github.com/make87/make87/releases/latest/download/m87-aarch64-unknown-linux-gnu
chmod +x m87-aarch64-unknown-linux-gnu
sudo mv m87-aarch64-unknown-linux-gnu /usr/local/bin/m87

# Download m87-server
wget https://github.com/make87/make87/releases/latest/download/m87-server-aarch64-unknown-linux-gnu
chmod +x m87-server-aarch64-unknown-linux-gnu
sudo mv m87-server-aarch64-unknown-linux-gnu /usr/local/bin/m87-server
```

### From Source

#### Prerequisites
- Rust 1.70 or later
- Cargo

#### Build All Components

```bash
# Clone the repository
git clone https://github.com/make87/make87.git
cd make87

# Build both binaries
cargo build --release --workspace

# Install binaries
sudo cp target/release/m87 /usr/local/bin/
sudo cp target/release/m87-server /usr/local/bin/
```

#### Build Individual Components

```bash
# Build only the client
cargo build --release -p m87-client

# Build only the server
cargo build --release -p m87-server
```

### Build Optimizations

Release builds are optimized for minimal resource footprint while maintaining safety and reliability for long-running edge agents:

**Enabled Optimizations:**
- **LTO (Link-Time Optimization)**: Full cross-crate optimization and dead code elimination
- **Single codegen unit**: Better optimization at the cost of longer compile times
- **Stripped binaries**: Debug symbols removed for smaller size
- **Optimization level 3**: Maximum performance optimizations

**Disabled for Safety:**
- **Overflow checks**: ENABLED (default) - Prevents integer overflow bugs and security vulnerabilities
- **Panic unwinding**: ENABLED (default) - Allows proper resource cleanup via Drop implementations

**Binary sizes:**
- `m87` client: ~9.2 MB (optimized, stripped, with safety checks)
- `m87-server`: ~350 KB (optimized, stripped, with safety checks)

These optimizations balance performance with reliability - the binaries are lightweight and efficient while maintaining critical runtime safety checks for production edge deployments.

## Quick Start

### Client (m87)

```bash
# Log in to make87
m87 auth login

# Run the agent
m87 agent run

# Check version
m87 version
```

See [m87-client/README.md](./m87-client/README.md) for detailed usage.

### Server (m87-server)

```bash
# Start the server
m87-server
```

See [m87-server/README.md](./m87-server/README.md) for detailed configuration.

## Development

### Project Structure

```
make87/
├── m87-client/         # Client CLI and agent
├── m87-server/         # Server component
├── Cargo.toml          # Workspace configuration
└── README.md           # This file
```

### Building

```bash
# Build all components
cargo build --workspace

# Run tests
cargo test --workspace

# Check formatting
cargo fmt --check

# Run clippy
cargo clippy --workspace
```

### Running Locally

```bash
# Run client
cargo run -p m87-client -- [command]

# Run server
cargo run -p m87-server
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

## License

Apache-2.0 - See [LICENSE](./LICENSE) for details.

## Support

- **Documentation**: [docs.make87.com](https://docs.make87.com)
- **Issues**: [GitHub Issues](../../issues)
- **Community**: [Discord](https://discord.gg/make87)
