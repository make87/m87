# Remote Commands

Run commands on remote devices.

## Overview

m87 exec allows you to execute commands on remote devices, with optional stdin forwarding and TTY support for interactive applications.

## Basic Usage

```bash
# Run a simple command
m87 <device> exec -- ls -la

# Run with stdin forwarding (for prompts)
m87 <device> exec -i -- sudo apt upgrade

# Run with TTY for interactive apps
m87 <device> exec -it -- vim config.yaml
```

## Modes

| Flags | Mode | Use Case |
|-------|------|----------|
| (none) | Output only | Simple commands, scripts |
| `-i` | Stdin forwarding | Respond to prompts (Y/n) |
| `-it` | Full TTY | TUI apps (vim, htop, less) |

## Examples

### System Administration
```bash
# Check disk usage
m87 rpi exec -- df -h

# Update packages (needs stdin for confirmation)
m87 rpi exec -i -- 'sudo apt update && sudo apt upgrade'

# View system logs
m87 rpi exec -- journalctl -n 100
```

### Docker Management
```bash
# List containers
m87 rpi exec -- docker ps -a

# View container logs
m87 rpi exec -- docker logs myapp

# Stop all containers
m87 rpi exec -- 'docker stop $(docker ps -q)'
```

### Interactive Applications
```bash
# Edit a file with vim
m87 rpi exec -it -- vim /etc/hosts

# Monitor with htop
m87 rpi exec -it -- htop

# Browse files with less
m87 rpi exec -it -- less /var/log/syslog
```

### Chained Commands
```bash
# Multiple commands with &&
m87 rpi exec -- 'cd /app && git pull && npm install'

# Pipeline
m87 rpi exec -- 'ps aux | grep nginx'
```

## Shell Quoting

Commands are interpreted by your local shell first. Use single quotes to send commands literally:

```bash
# Local shell expands $(...)
m87 rpi exec -- docker kill $(docker ps -q)  # Runs docker ps -q locally!

# Single quotes send literally to remote
m87 rpi exec -- 'docker kill $(docker ps -q)'  # Correct: expands on remote
```

## Flags

- `-i, --stdin` - Keep stdin open for responding to prompts
- `-t, --tty` - Allocate pseudo-TTY for TUI applications

## Ctrl+C Behavior

| Mode | Ctrl+C Effect |
|------|---------------|
| No flags / `-i` | Terminates connection, exits with code 130 |
| `-t` / `-it` | Sent to remote app (e.g., cancel in vim) |

In TTY mode, Ctrl+C is forwarded to the remote application as a raw keystroke. To forcefully disconnect, close your terminal or use other means.

## Process Cleanup

When the connection closes (Ctrl+C, network drop, etc.), the remote process is automatically terminated. No orphaned processes are left on the device.

## Advanced

For a persistent interactive shell, use `m87 <device> shell` instead.
