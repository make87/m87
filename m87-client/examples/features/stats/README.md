# System Stats

Real-time system metrics dashboard for remote devices.

## Overview

`m87 <device> stats` displays a TUI dashboard with live system metrics from the remote device.

## Basic Usage

```bash
m87 <device> stats
```

## Dashboard Metrics

| Metric | Description |
|--------|-------------|
| CPU | Per-core usage percentages |
| Memory | Used/total RAM, swap usage |
| Disk | Filesystem usage per mount point |
| Network | Bytes sent/received per interface |
| Temperatures | CPU/GPU temperatures (if available) |

## Examples

```bash
# Monitor a Raspberry Pi
m87 rpi stats

# Monitor a server
m87 db-server stats
```

## Controls

| Key | Action |
|-----|--------|
| `q` | Quit |
| `Ctrl+C` | Quit |

## Use Cases

### Quick Health Check
```bash
m87 rpi stats
# Glance at CPU, memory, disk usage
# Press q to exit
```

### Debugging Performance Issues
```bash
# Check if device is resource-constrained
m87 edge-node stats
```

### Monitoring During Deployment
```bash
# Terminal 1: Watch metrics
m87 rpi stats

# Terminal 2: Deploy application
m87 sync ./app rpi:/home/pi/myapp
m87 rpi exec -- 'cd /home/pi/myapp && npm install'
```

## See Also

- [shell/](../shell/) - Interactive shell for detailed inspection
- [exec/](../exec/) - Run diagnostic commands
