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
m87 <device> status            # health snapshot (--short / --quiet / --json / --since)
m87 <device> logs              # last 200 events (history); -f for live stream
m87 <device> shell             # interactive shell
m87 <device> exec -- <cmd>     # run command
m87 <device> forward <ports>   # port forwarding (see below)
m87 <device> docker <args>     # docker passthrough
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

# List job definitions (not runs)
m87 <device> job defs
```

Step-by-step output for a specific run is part of the unified `logs`
command — pass the run id as a positional:

```sh
m87 <device> logs <run-id>                  # everything for that run
m87 <device> logs <run-id> --logs           # + captured stdout/stderr tails
```

#### Inspecting the deployed spec

```sh
m87 <device> spec               # raw YAML of what is currently deployed
m87 <device> spec --json        # JSON format
```

#### Logs

A single `logs` command answers three different questions depending on
the flags you pass:

| Question                          | Command                            |
| --------------------------------- | ---------------------------------- |
| What's been going on lately?      | `m87 <dev> logs`                   |
| What happened to this unit?       | `m87 <dev> logs <unit-or-run-id>`  |
| What's happening right now?       | `m87 <dev> logs --follow`          |

By default, `logs` shows the **last 200 events** from history, sorted by
time, across services, observers, and jobs. The positional id scopes to
one unit (service / observer / job-def id) or to a specific job run.

```sh
# Recent history
m87 <device> logs                              # last 200 events, all units
m87 <device> logs web-server                   # last 200 events for one unit
m87 <device> logs <run-id>                     # everything for one job run
m87 <device> logs --tail 50                    # last 50

# Kind scoping (additive — omit both = all)
m87 <device> logs --services                   # services + observers only
m87 <device> logs --jobs                       # job defs + runs only

# Failures-only and time windows
m87 <device> logs --failed                     # only failed events
m87 <device> logs --failed --since 1h          # failures in the last hour
m87 <device> logs --since 2026-05-25T13:00:00Z # since an absolute timestamp
m87 <device> logs web-server --logs --since 1h # include captured stdout/stderr

# Live stream (not history)
m87 <device> logs --follow                     # live observe stream
m87 <device> logs -f                           # short form

# JSON for scripting (NDJSON — one event per line)
m87 <device> logs --failed --since 1h --json
```

Time formats accepted by `--since` / `--until`: relative durations like
`30s`, `5m`, `1h`, `24h`, `7d`, `2w`; or any RFC 3339 timestamp
(`2026-05-25T13:00:00Z`); or a date alone (`2026-05-25`, treated as
UTC midnight).

#### Status

`status` answers a single question: **is the device healthy?**

Without flags it prints a snapshot — per-observe liveness/health, open
incidents. With `--since`, it also aggregates events over the window
(failure / success / restart counts per unit). The exit code reflects
the answer, which makes it scriptable.

```sh
m87 <device> status                         # snapshot table
m87 <device> status --since 24h             # snapshot + 24h aggregate
m87 <device> status --since 1h --json       # JSON for scripts

m87 <device> status --short                 # one-line summary
m87 <device> status --quiet                 # no output, exit code only
```

**Exit codes:**

| Code | Meaning                                                        |
| ---: | -------------------------------------------------------------- |
|    0 | Healthy — no current issues, no failures in the window         |
|    1 | Issues detected (unhealthy unit, open incident, or window has failures) |
|    2 | The command itself failed (network / auth / no such device)    |

So you can write health gates:

```sh
m87 dev1 status --quiet && echo "healthy" || page-oncall
```

A typical drill-down from an alarm:

```sh
m87 dev1 status                                   # see what's red
m87 dev1 status --since 1h                        # has it been bad for long?
m87 dev1 logs web-server --failed --since 1h --logs   # show actual errors
```

#### Reverting a change

Each device has a single in-place spec — there is no revision history to
flip between. To revert, redeploy the previous YAML:

```sh
m87 <device> deploy ./last-known-good.yaml --replace-all
```

`--replace-all` atomically swaps the entire spec; anything not in the new
file is removed.

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

### Profiles

Profiles let you stay logged into several accounts on one machine and switch
between them — no more deleting your config and logging back in each time.

Each profile keeps its own credentials and config. The `default` profile is
the one you already use (existing installs need no migration); additional
profiles are created on demand.

```sh
m87 profile list                # show all profiles (* marks the active one)
m87 profile current             # print the active profile name
m87 profile add work            # create a profile and switch to it
m87 profile use default         # switch to an existing profile
m87 profile rename work office  # rename a profile
m87 profile remove work         # delete a profile and its credentials
```

Typical flow — add a second account and switch back and forth:

```sh
m87 profile add work            # create + switch to "work"
m87 login                       # authenticate the work account
m87 devices list                # work account's devices

m87 profile use default         # back to your original account — no re-login
m87 devices list                # original account's devices
```

Set `M87_PROFILE` to override the active profile for a single command (handy
in scripts), without changing the persisted default:

```sh
M87_PROFILE=work m87 devices list
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
