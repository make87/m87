# m87-server

Backend server for [make87](https://make87.com).

## Overview

m87-server handles device registration, authentication, and tunnel relay. It connects m87 agents on edge devices to m87 CLI users.

Can be self-hosted for on-premise deployments or used with the make87 platform.

## Requirements

- MongoDB
- Docker (optional, for containerized deployment)

## Quick Start

With Docker Compose:

```sh
git clone https://github.com/make87/make87
cd make87/m87-server
docker compose up -d
```

Or download binaries from [releases](https://github.com/make87/make87/releases) and run with your own MongoDB.

## Configuration

Environment variables (see `docker-compose.yml`):

| Variable         | Default                    | Description                           |
| ---------------- | -------------------------- | ------------------------------------- |
| `PUBLIC_ADDRESS` | `localhost`                | Public hostname for this server       |
| `MONGO_URI`      | `mongodb://mongo:27017`    | MongoDB connection string             |
| `OAUTH_ISSUER`   | `https://auth.make87.com/` | OAuth provider URL                    |
| `OAUTH_AUDIENCE` | `https://auth.make87.com`  | OAuth audience                        |
| `FORWARD_SECRET` | —                          | Secret for signing tunnel tokens      |
| `UNIFIED_PORT`   | `8084`                     | Agent/tunnel port (expose as 443)     |
| `ADMIN_EMAILS`   | —                          | Comma-separated admin email addresses |

## Ports

- **443 → 8084**: Agent connections and tunnel traffic (TLS)
- **8085**: REST API

## Building

Requires Rust 1.85+

```sh
cargo build --release -p m87-server
```

Binary: `target/release/m87-server`

## Docker

```sh
docker pull ghcr.io/make87/m87-server:latest
```

Or build locally:

```sh
docker build -t m87-server -f m87-server/Dockerfile .
```

## License

AGPL-3.0-or-later
