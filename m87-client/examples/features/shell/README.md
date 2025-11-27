# Interactive Shell

Open a persistent shell session on a remote device.

## Overview

`m87 <device> shell` provides a full interactive shell with PTY support, similar to SSH.

## Basic Usage

```bash
m87 <device> shell
```

This opens an interactive bash session on the remote device.

## Examples

```bash
# Connect to a Raspberry Pi
m87 rpi shell

# You're now in a remote shell
pi@raspberrypi:~ $ ls -la
pi@raspberrypi:~ $ htop
pi@raspberrypi:~ $ vim config.yaml
pi@raspberrypi:~ $ exit
```

## Features

- Full PTY support (colors, cursor control)
- Works with TUI applications (vim, htop, less, nano)
- Uses user's default shell defined in $SHELL
- Ctrl+D exits the shell

## Shell vs Exec

| Feature | `shell` | `exec` |
|---------|---------|--------|
| Persistent session | Yes | No |
| Multiple commands | Yes | Single command |
| Interactive apps | Always | With `-it` flags |
| Scripting | No | Yes |

Use `shell` for interactive work. Use `exec` for scripting and automation.

## Examples

### Interactive Administration
```bash
m87 rpi shell
# Now explore, edit files, install packages interactively
```

### Quick One-off Commands
```bash
# Use exec for non-interactive commands
m87 rpi exec -- df -h
m87 rpi exec -- docker ps
```

## See Also

- [exec/](../exec/) - Run single commands
- [stats/](../stats/) - System metrics dashboard
