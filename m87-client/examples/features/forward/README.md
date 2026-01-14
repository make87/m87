# Port Forwarding

Forward ports from remote devices to your local machine.

## Overview

m87 port forwarding allows you to forward ports from remote devices to your local machine, making remote services accessible as if they were running locally.

## Syntax

```
m87 <device> forward <TARGETS>...
```

Each target follows the format: `[local_port:]remote_target[/protocol]`

Where:
- `remote_target` is `[host:]port`
- `protocol` is `tcp` (default) or `udp`

## Basic Usage

```bash
# Forward remote port 8080 to same local port (8080)
m87 <device> forward 8080

# Forward remote port 8080 to local port 3000
m87 <device> forward 3000:8080

# Explicit TCP (same as default)
m87 <device> forward 8080/tcp

# UDP forwarding
m87 <device> forward 53/udp
```

You can now access the service at its mapped port, e.g., `localhost:3000`.

## Network Device Forwarding

Forward ports from any device on the remote device's network, not just localhost:

```bash
# Forward port 554 from IP camera at 192.168.1.50 (via the remote device)
m87 <device> forward 192.168.1.50:554

# Same, but expose locally on port 8554
m87 <device> forward 8554:192.168.1.50:554
```

This enables access to devices that are only reachable from the remote device's network.

## Common Use Cases

- Access web UIs running on remote devices
- Connect to databases on remote servers
- Debug remote applications locally
- Access admin panels
- Stream from IP cameras on remote LANs
- Reach network devices (routers, switches) through a jump host

## Examples

### Access Remote Web Server
```bash
# Remote device running nginx on port 80, expose locally on 8080
m87 rpi forward 8080:80

# Access at http://localhost:8080
```

### Connect to Remote Database
```bash
# Remote PostgreSQL on port 5432
m87 db-server forward 5432

# Connect locally
psql -h localhost -p 5432 -U myuser mydb
```

### Multiple Forwards (Single Command)
```bash
# Forward web UI (3000) and API (8000) in one command
m87 device forward 3000 8000

# Or with different local ports
m87 device forward 3000:3000 8080:8000
```

### Access IP Camera via Remote Device
```bash
# Pi is on the same LAN as an IP camera at 192.168.1.50
m87 rpi forward 8554:192.168.1.50:554

# View RTSP stream locally
ffplay rtsp://localhost:8554/stream
vlc rtsp://localhost:8554/stream
```

### Access Router Admin Panel
```bash
# Router at 192.168.1.1 only accessible from office network
m87 office-pc forward 8080:192.168.1.1:80

# Open http://localhost:8080 in browser
```

## Security

- All traffic is encrypted through the m87 secure channel
- No need to expose ports publicly
- Authentication handled by m87

## Advanced

For persistent forwards or more complex scenarios, see the [use-cases/](../../use-cases/) directory.
