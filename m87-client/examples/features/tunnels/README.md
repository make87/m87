# Tunnels

Create secure tunnels to access services on remote devices.

## Overview

m87 tunnels allow you to forward ports from remote devices to your local machine, making remote services accessible as if they were running locally.

## Basic Usage

```bash
# Forward remote port 8080 to same local port (8080)
m87 <device> tunnel 8080

# Forward remote port 8080 to local port 3000
m87 <device> tunnel 8080 3000
```

You can now access the service at its mapped port, e.g., `localhost:3000`.

## Network Device Tunneling

Forward ports from any device on the remote device's network, not just localhost:

```bash
# Forward port 554 from IP camera at 192.168.1.50 (via the remote device)
m87 <device> tunnel 192.168.1.50:554

# Same, but expose locally on port 8554
m87 <device> tunnel 192.168.1.50:554 8554
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
# Remote device running nginx on port 80
m87 rpi tunnel 80 8080

# Access at http://localhost:8080
```

### Connect to Remote Database
```bash
# Remote PostgreSQL on port 5432
m87 db-server tunnel 5432 5432

# Connect locally
psql -h localhost -p 5432 -U myuser mydb
```

### Multiple Tunnels
```bash
# Terminal 1: Forward web UI
m87 device tunnel 3000 3000

# Terminal 2: Forward API
m87 device tunnel 8000 8000
```

### Access IP Camera via Remote Device
```bash
# Pi is on the same LAN as an IP camera at 192.168.1.50
m87 rpi tunnel 192.168.1.50:554 8554

# View RTSP stream locally
ffplay rtsp://localhost:8554/stream
vlc rtsp://localhost:8554/stream
```

### Access Router Admin Panel
```bash
# Router at 192.168.1.1 only accessible from office network
m87 office-pc tunnel 192.168.1.1:80 8080

# Open http://localhost:8080 in browser
```

## Security

- All traffic is encrypted through the m87 tunnel
- No need to expose ports publicly
- Authentication handled by m87

## Advanced

For persistent tunnels or more complex scenarios, see the [use-cases/](../../use-cases/) directory.