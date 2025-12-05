# m87

CLI for [make87](https://make87.com).

## Overview

m87 connects edge devices to the make87 platform and provides remote access to them.

**On edge devices**: Run the m87 agent to register the device and accept incoming connections.

**From your workstation**: Use m87 to access registered devices — shell, port forwarding, file transfer, container
management.

## Requirements

- Linux (amd64, arm64) — CLI and agent
- macOS (amd64, arm64) — CLI only

## Install

Linux:

```sh
curl -fsSL https://get.make87.com | sh
```

Or download from [releases](https://github.com/make87/make87/releases).

## Quick Start

```sh
m87 login                      # authenticate via browser
m87 devices list               # list accessible devices
m87 <device> shell             # open shell on device
m87 <device> tunnel 8080       # forward port 8080
```

## Commands

### Remote Device Access

```
m87 <device> shell             # interactive shell
m87 <device> exec -- <cmd>     # run command
m87 <device> tunnel <ports>    # port forwarding (see below)
m87 <device> docker <args>     # docker passthrough
m87 <device> logs [-f]         # container logs
m87 <device> stats             # system metrics
m87 <device> serial <name>     # serial mount forwarding
```

### File Transfer

```
m87 cp <device>:/path ./local  # copy from device
m87 cp ./local <device>:/path  # copy to device
m87 sync ./src <device>:/dst   # rsync-style sync
m87 sync --watch ./src <device>:/dst
```

### Device Management

```
m87 login
m87 logout
m87 devices list
m87 devices approve <device>
m87 update
```

### Running as Agent (Linux)

To make a device remotely accessible:

```
m87 agent login           # register device with make87
m87 agent run             # run agent (blocking)
```

Or use systemd:

```
sudo m87 agent enable --now    # install and start service
sudo m87 agent status
sudo m87 agent stop
```

`sudo` is required to manage systemd services. The agent itself runs as your user, not root.

## Port Forwarding

Format: `[local:]remote[/protocol]`

```sh
m87 <device> tunnel 8080              # localhost:8080 → device:8080
m87 <device> tunnel 3000:8080         # localhost:3000 → device:8080
m87 <device> tunnel 192.168.1.5:80    # forward to host on device's LAN
m87 <device> tunnel 8080/udp          # UDP (default: tcp)
m87 <device> tunnel 8080 9090 3000    # multiple ports
```

See [examples/features/tunnels](./examples/features/tunnels/) for more.

## Building

Requires Rust 1.85+

```sh
git clone https://github.com/make87/make87
cd make87
cargo build --release -p m87-client
```

Binary: `target/release/m87`

Build configuration is auto-detected by OS:

- Linux: full functionality (CLI + agent)
- macOS: CLI only

## Documentation

- [examples/](./examples/) — usage examples
- [examples/features/](./examples/features/) — per-feature docs

## License

Apache-2.0
