# make87

Remote access for edge devices. One CLI for shell, tunnels, file transfer, and container management.

## Quick Start

Linux:

```sh
curl -fsSL https://get.make87.com | sh
```

```sh
m87 login
m87 devices list
m87 <device> shell
```

See [m87-client/](./m87-client/) for full documentation.

## Packages

| Package                     | Description |
|-----------------------------|-------------|
| [m87-client](./m87-client/) | CLI and agent for edge devices |
| [m87-server](./m87-server/) | Backend server (self-host or use make87.com) |
| [m87-shared](./m87-shared/) | Internal shared types |

## Building

Requires Rust 1.85+

```sh
cargo build --release
```

## License

- m87-client, m87-shared: Apache-2.0
- m87-server: AGPL-3.0-or-later
