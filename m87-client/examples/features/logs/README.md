# Log Streaming

Stream m87 runtime logs from remote devices.

## Overview

`m87 <device> logs` streams logs from the m87 runtime running on the remote device. This shows runtime activity and diagnostics, not general system logs.

## Basic Usage

```bash
# Stream logs
m87 <device> logs

# Follow logs (continuous stream)
m87 <device> logs -f

# Show last N lines
m87 <device> logs --tail 50
```

## Flags

| Flag | Description |
|------|-------------|
| `-f, --follow` | Stream logs continuously |
| `--tail <n>` | Number of lines to show (default: 100) |

## Examples

### View Recent Logs
```bash
# Last 100 lines (default)
m87 rpi logs

# Last 500 lines
m87 rpi logs --tail 500
```

### Follow Logs
```bash
# Continuous stream
m87 rpi logs -f

# Press Ctrl+C to stop
```

### Debug Runtime Issues
```bash
# Follow runtime logs while troubleshooting
m87 rpi logs -f

# Check for recent runtime errors
m87 rpi logs --tail 200 | grep -i error
```

## Use Cases

### Monitor Runtime Activity
```bash
# Watch runtime startup and connections
m87 rpi logs -f --tail 50
```

### Diagnose Connection Problems
```bash
# Check runtime logs for connectivity issues
m87 rpi logs --tail 100
```

## System Logs

For general system logs (journalctl, application logs), use exec:

```bash
# System journal
m87 rpi exec -- journalctl -f

# Application logs
m87 rpi exec -- tail -f /var/log/myapp/app.log

# Docker container logs
m87 rpi exec -- docker logs -f mycontainer
```

## See Also

- [stats/](../stats/) - System metrics dashboard
- [exec/](../exec/) - Run commands for system logs
