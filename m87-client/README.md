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

Or download from [releases](https://github.com/make87/m87/releases).

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
m87 <device> status            # status of the device (crashes, health and incidents)
m87 <device> shell             # interactive shell
m87 <device> exec -- <cmd>     # run command
m87 <device> forward <ports>   # port forwarding (see below)
m87 <device> docker <args>     # docker passthrough
m87 <device> logs              # logs from the runtime and observed containers
m87 <device> metrics           # system metrics
m87 <device> serial <name>     # serial mount forwarding
m87 <device> audit --details   # audit logs on who interacted with the device
```

### Deployment

Deploy services, observers, and job definitions to a device — even when it is
offline. The device picks up changes as soon as it comes back online.

**Units** come in three kinds:

| Kind         | Description                                                                                      |
| ------------ | ------------------------------------------------------------------------------------------------ |
| **service**  | Long-running process. Has startup steps, optional stop steps, optional health / liveness checks. |
| **observer** | Polling-only unit. No startup steps — just health / liveness checks that run on a schedule.      |
| **job**      | One-shot definition triggered explicitly. Runs once per trigger; each run is tracked separately. |

#### Deploying

```sh
# Deploy a single unit — type is auto-detected from the YAML
m87 <device> deploy ./web-server.yaml
m87 <device> deploy ./api-health.yaml      # observer if it has no steps
m87 <device> deploy ./db-migrate.yaml      # job definition
m87 <device> deploy ./docker-compose.yml   # docker compose — auto-converted

# Atomically replace the entire device state from a full revision file
# (contains services / observers / job_defs sections)
m87 <device> deploy ./my-stack.yaml --replace-all

# Remove a unit from the active deployment
m87 <device> undeploy web-server

# List everything currently deployed
m87 <device> units

# Show full status (health, step outcomes, last update times)
m87 <device> health             # all units
m87 <device> health web-server  # one unit
m87 <device> health --logs      # with captured step output inline
```

#### Runtime lifecycle control

Change a unit's state at runtime — no YAML file or revision bump required.
Changes are delivered to the device on its next heartbeat (typically within
a few seconds).

```sh
m87 <device> start web-server       # run startup steps (if not already running)
m87 <device> stop web-server        # run stop steps and tear down
m87 <device> stop web-server --force  # skip stop steps, tear down immediately
m87 <device> pause web-server       # suspend observe polling; process keeps running
m87 <device> resume web-server      # resume a paused unit
m87 <device> restart web-server     # stop then start
```

#### Triggering jobs

```sh
# Trigger a job run (creates a tracked run instance)
m87 <device> job trigger db-migrate
m87 <device> job trigger db-migrate --env TARGET=prod --env DRY_RUN=false

# Inspect runs
m87 <device> job list                       # all runs across all jobs
m87 <device> job list --job db-migrate      # runs for one job definition
m87 <device> job status <run-id>            # status, times, error
m87 <device> job logs <run-id>              # step-by-step output for this run

# List job definitions (not runs)
m87 <device> job defs
```

#### Inspecting the deployed spec

```sh
m87 <device> spec               # raw YAML of what is currently deployed
m87 <device> spec --json        # JSON format
```

#### Logs

**Three distinct log surfaces — each shows something different:**

```sh
# 1. Live observe-follow logs  (running service / observer output right now)
m87 <device> logs
m87 <device> logs -f

# 2. Step execution history  (start / stop / restart events with captured output)
m87 <device> logs --steps                   # all units
m87 <device> logs web-server --steps        # one unit

# 3. Job run step output  (output from a specific triggered job run)
m87 <device> job logs <run-id>
```

#### Rollback

```sh
m87 <device> rollback                       # activate previous revision
m87 <device> rollback --list               # show revision history
m87 <device> rollback --to <revision-id>   # jump to a specific revision
```

#### YAML format reference

A **service** YAML:

```yaml
# web-server.yaml
id: web-server
steps:
  - name: start
    run: docker compose up -d
stop:
  steps:
    - name: stop
      run: docker compose down
observe:
  liveness:
    every: 30s
    observe: docker compose ps | grep -q Up
restart: on_failure # auto-restart when liveness fails (default)
```

An **observer** YAML (no `steps` — purely polling):

```yaml
# api-health.yaml
id: api-health
observe:
  health:
    every: 60s
    observe: curl -sf http://api.internal/health
    fails_after: 3 # trigger after 3 consecutive failures
```

A **job** YAML:

```yaml
# db-migrate.yaml
id: db-migrate
steps:
  - name: migrate
    run: ./migrate.sh
    timeout: 10m
  - name: verify
    run: ./verify.sh
```

A **full revision** file (atomic multi-unit deploy with `--replace-all`):

```yaml
# my-stack.yaml
services:
  - id: web-server
    steps:
      - name: start
        run: docker compose up -d
    stop:
      steps:
        - name: stop
          run: docker compose down
    observe:
      liveness:
        every: 30s
        observe: docker compose ps | grep -q Up

observers:
  - id: api-health
    observe:
      health:
        every: 60s
        observe: curl -sf http://api.internal/health

job_defs:
  - id: db-migrate
    steps:
      - name: migrate
        run: ./migrate.sh

rollback:
  on_health_failure: consecutive(3)
  stabilization_period_secs: 60
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
m87 login                       # authenticate via browser
m87 logout                      # clear local credentials
m87 devices list                # list accessible devices
m87 devices approve <device>    # approve a pending device registration
```

### Updating

```sh
m87 update                      # download and install the latest m87 binary
```

After updating, restart the runtime to use the new version:

```sh
m87 runtime restart             # restart the runtime service
```

To update and restart a remote device:

```sh
m87 <device> exec -it -- 'm87 update && m87 runtime restart'
```

### Running as Runtime (Linux)

To make a device remotely accessible:

```sh
m87 runtime run --email you@example.com   # register and run runtime (waits for approval)
```

Then approve the device from your workstation with `m87 devices approve <request-id>`.

#### Systemd Service

```sh
m87 runtime start           # enable at boot and start immediately
m87 runtime stop            # stop the service (keeps enabled at boot)
m87 runtime restart         # restart the service (starts if stopped)
m87 runtime status          # show service status
m87 runtime enable          # enable at boot (without starting)
m87 runtime enable --now    # enable at boot and start immediately
m87 runtime disable         # disable at boot (keeps running)
m87 runtime disable --now   # disable at boot and stop
```

The CLI automatically handles privilege escalation by invoking `sudo`. The runtime service runs as your user, not root.

**Command behavior:**

- `start` / `enable --now`: Installs the service file, enables it to start on boot, and starts it immediately
- `stop`: Stops the running service but keeps it enabled for next boot
- `restart`: Matches systemd behavior — restarts if running, starts if stopped
- `enable`: Only enables the service to start on boot (doesn't start it now)
- `disable`: Only disables the service from starting on boot (doesn't stop it now)

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

## MCP Server

m87 includes a built-in [MCP](https://modelcontextprotocol.io) server, exposing all platform commands as tools for AI agents.

Add to your MCP client config (e.g. Claude Code, Claude Desktop):

```json
{
  "m87": {
    "type": "stdio",
    "command": "m87",
    "args": ["mcp"]
  }
}
```

Or with an absolute path if `m87` isn't in your `PATH`:

```json
{
  "m87": {
    "type": "stdio",
    "command": "/path/to/m87",
    "args": ["mcp"]
  }
}
```

## Building

Requires Rust 1.85+

```sh
git clone https://github.com/make87/m87
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
