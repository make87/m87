# m87

CLI for [make87](https://make87.com).

## Overview

m87 connects edge devices to the make87 platform and provides remote access to them.

**On edge devices**: Run the m87 runtime to register the device and accept incoming connections.

**From your workstation**: Use m87 to access registered devices — shell, port forwarding, file transfer, container
management.

## Requirements

- Linux (amd64, arm64) — CLI and runtime
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
m87 <device> forward 8080      # forward port 8080
```

## Commands

### Remote Device Access

```
m87 <device> shell             # interactive shell
m87 <device> exec -- <cmd>     # run command
m87 <device> forward <ports>   # port forwarding (see below)
m87 <device> docker <args>     # docker passthrough
m87 <device> logs              # logs from the runtime and observed containers
m87 <device> stats             # system metrics
m87 <device> serial <name>     # serial mount forwarding
```

### Remote Device Logs

```
m87 <device> observe docker <container_name>             # make the runtime observe the container logs
m87 <device> logs                                        # logs will now also show <container_name>
m87 <device> observe docker <container_name> -r          # remove the container from the list of observed containers

```

### File Transfer

```
m87 cp <device>:/path ./local  # copy from device
m87 cp ./local <device>:/path  # copy to device
m87 sync ./src <device>:/dst   # rsync-style sync
m87 sync --watch ./src <device>:/dst
```

## SSH

```
m87 ssh enable                 # enable ssh host resolving
ssh <device>.m87               # now you can use ssh like you would normally
```

### Device Management

```
m87 login
m87 logout
m87 devices list
m87 devices approve <device>
m87 update
```

### Running as Runtime (Linux)

To make a device remotely accessible:

```sh
m87 runtime run --email you@example.com   # register and run runtime (waits for approval)
```

Then approve the device from your workstation with `m87 devices approve <request-id>`.

#### Systemd Service

```sh
m87 runtime enable --now    # install, enable and start (prompts for sudo)
m87 runtime status          # show service status
m87 runtime stop            # stop the service
m87 runtime disable --now   # disable and stop
```

The CLI automatically handles privilege escalation invoking `sudo`. The runtime service runs as your user, not root.

## Port Forwarding

Format: `[local:]remote[/protocol]`

```sh
m87 <device> forward 8080              # localhost:8080 → device:8080
m87 <device> forward 3000:8080         # localhost:3000 → device:8080
m87 <device> forward 192.168.1.5:80    # forward to host on device's LAN
m87 <device> forward 8080/udp          # UDP (default: tcp)
m87 <device> forward 8080 9090 3000    # multiple ports
```

See [examples/features/forward](./examples/features/forward/) for more.

## Building

Requires Rust 1.85+

```sh
git clone https://github.com/make87/make87
cd make87
cargo build --release -p m87-client
```

Binary: `target/release/m87`

Build configuration is auto-detected by OS:

- Linux: full functionality (CLI + runtime)
- macOS: CLI only

## Documentation

- [examples/](./examples/) — usage examples
- [examples/features/](./examples/features/) — per-feature docs

## License

Apache-2.0
