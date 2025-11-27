# File Operations

Transfer and list files on remote devices.

## Overview

m87 provides file operations using `device:path` syntax to reference remote locations:
- `m87 sync` - Synchronize directories (rsync-style)
- `m87 ls` - List remote files

## Path Syntax

```
<device>:<path>    Remote path on device
<path>             Local path
```

Examples:
- `rpi:/home/pi/app` - Remote directory on device "rpi"
- `./src` - Local directory

## Sync

Synchronize files between local and remote:

```bash
# Push local directory to remote
m87 sync ./src rpi:/home/pi/app

# Pull remote directory to local
m87 sync rpi:/var/log ./logs

# Delete files not in source
m87 sync ./deploy rpi:/app --delete

# Watch for changes and auto-sync
m87 sync ./src rpi:/home/pi/app --watch
```

### Flags

| Flag | Description |
|------|-------------|
| `--delete` | Remove files from destination not present in source |
| `--watch` | Continuously sync on file changes (polls every 2s) |

## List Files

List contents of a remote directory:

```bash
m87 ls rpi:/home/pi
m87 ls rpi:/var/log
```

## Examples

### Deploy Application
```bash
# Sync source code to device
m87 sync ./app rpi:/home/pi/myapp

# Connect and restart
m87 rpi exec -- 'cd /home/pi/myapp && npm install && pm2 restart all'
```

### Development Workflow
```bash
# Watch and sync during development
m87 sync ./src rpi:/home/pi/project --watch

# In another terminal, watch logs
m87 rpi logs -f
```

### Backup Remote Files
```bash
# Pull logs locally
m87 sync rpi:/var/log/myapp ./backups/logs

# Pull config files
m87 sync rpi:/etc/myapp ./backups/config
```

### Clean Deploy
```bash
# Sync with delete to mirror source exactly
m87 sync ./dist rpi:/var/www/html --delete
```

## Notes

- File transfers use SFTP over the m87 secure tunnel
- Large files are transferred efficiently without loading into memory
- `--watch` mode polls for changes every 2 seconds

## See Also

- [exec/](../exec/) - Run commands after file transfer
- [shell/](../shell/) - Interactive file browsing
