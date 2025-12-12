# File Operations

Transfer and list files on remote devices.

## Overview

m87 provides file operations using `device:path` syntax to reference remote locations:
- `m87 cp` - Copy individual files (scp-style)
- `m87 sync` - Synchronize directories (rsync-style)
- `m87 ls` - List remote files

## Path Syntax

```
<device>:<path>    Remote path on device
<path>             Local path
```

Remote paths use scp-style resolution:
- **Relative paths** (no leading `/`) resolve to the user's home directory
- **Absolute paths** (starting with `/`) are used as-is
- **Tilde expansion** (`~`) expands to home directory

Examples:
- `rpi:app` - Remote `~/app` directory on device "rpi"
- `rpi:/etc/config` - Absolute path `/etc/config`
- `rpi:~/logs` - Explicit home directory path
- `./src` - Local directory

## Copy (cp)

Copy individual files between local and remote:

```bash
# Copy local file to remote (relative path -> ~/file.txt)
m87 cp ./config.json rpi:config.json

# Copy local file to absolute path
m87 cp ./config.json rpi:/etc/myapp/config.json

# Copy remote file to local
m87 cp rpi:logs/app.log ./app.log

# Copy between remote devices
m87 cp rpi:data.db jetson:backup/data.db
```

## Sync

Synchronize directories between local and remote:

```bash
# Push local directory to remote home
m87 sync ./src rpi:app

# Push to absolute path
m87 sync ./src rpi:/home/pi/app

# Pull remote directory to local
m87 sync rpi:/var/log ./logs

# Delete files not in source
m87 sync ./deploy rpi:app --delete

# Watch for changes and auto-sync
m87 sync ./src rpi:app --watch

# Preview what would be synced (dry run)
m87 sync ./src rpi:app --dry-run

# Exclude files matching patterns
m87 sync ./src rpi:app --exclude node_modules --exclude "*.log"

# Combine flags
m87 sync ./src rpi:app --delete --exclude .git --exclude "*.tmp"
```

### Flags

| Flag | Short | Description |
|------|-------|-------------|
| `--delete` | | Remove files from destination not present in source |
| `--watch` | | Continuously sync on file changes (polls every 2s) |
| `--dry-run` | `-n` | Show what would be synced without making changes |
| `--exclude` | `-e` | Exclude files matching pattern (can be used multiple times) |

### Exclude Patterns

The `--exclude` flag supports:
- Exact names: `--exclude node_modules` (matches any path component)
- Wildcards: `--exclude "*.log"` (matches `app.log`, `error.log`, etc.)

Common excludes:
```bash
m87 sync ./project rpi:project \
  --exclude node_modules \
  --exclude .git \
  --exclude __pycache__ \
  --exclude "*.pyc" \
  --exclude ".env"
```

## List Files

List contents of a remote directory:

```bash
m87 ls rpi:projects
m87 ls rpi:/var/log
```

## Examples

### Deploy Application
```bash
# Sync source code to device
m87 sync ./app rpi:myapp

# Connect and restart
m87 rpi exec -- 'cd ~/myapp && npm install && pm2 restart all'
```

### Development Workflow
```bash
# Watch and sync during development, excluding build artifacts
m87 sync ./src rpi:project --watch --exclude node_modules --exclude dist

# In another terminal, watch logs
m87 rpi logs -f
```

### Backup Remote Files
```bash
# Pull logs locally
m87 sync rpi:/var/log/myapp ./backups/logs

# Pull config files
m87 sync rpi:/etc/myapp ./backups/config

# Copy a single config file
m87 cp rpi:/etc/myapp/config.yaml ./config-backup.yaml
```

### Clean Deploy
```bash
# Preview what will change
m87 sync ./dist rpi:www --delete --dry-run

# If satisfied, run the actual sync
m87 sync ./dist rpi:www --delete
```

### Quick File Transfer
```bash
# Upload a script
m87 cp ./deploy.sh rpi:deploy.sh
m87 rpi exec -- chmod +x ~/deploy.sh

# Download a log file
m87 cp rpi:/var/log/app.log ./debug.log
```

## Notes

- File transfers use SFTP over the m87 secure tunnel
- Large files are transferred efficiently without loading into memory
- `--watch` mode polls for changes every 2 seconds
- Relative remote paths (without leading `/`) resolve to home directory
- Use `--dry-run` to preview sync operations before executing

## See Also

- [exec/](../exec/) - Run commands after file transfer
- [shell/](../shell/) - Interactive file browsing
