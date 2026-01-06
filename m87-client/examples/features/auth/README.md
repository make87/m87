# Authentication

Authenticate with the make87 platform.

## Overview

m87 supports two authentication modes:
- **Manager login** (default): OAuth2 browser flow for managing devices from your computer
- **Agent registration** (Linux only): Headless device registration for autonomous agents

Nothing prevents you from running an agent and a manager on the same device.

## Manager Login

```bash
# Opens browser for OAuth2 authentication
m87 login
```

After authentication, your CLI is authorized to manage devices across your organization.

## Agent Registration (Linux)

Register a device as an agent to enable remote management:

```bash
# Register under your account (prompts for org selection)
m87 login --agent

# Register under specific organization
m87 login --agent --org-id <org-id>

# Register under specific user email
m87 login --agent --email admin@example.com
```

After registration, the device appears in `m87 devices list` with status "pending" until approved by a manager.

## Logout

```bash
# Remove all local credentials
m87 logout
```

This clears both manager and agent credentials from the device.

## Flags

| Flag | Description |
|------|-------------|
| `--agent` | Register device as agent (Linux only) |
| `--org-id <id>` | Organization ID for agent registration |
| `--email <email>` | User email for agent registration |

## Workflow

### Manager Setup
```bash
m87 login                    # Authenticate as manager
m87 devices list             # View all devices
m87 devices approve rpi      # Approve pending agent
```

### Agent Setup (on the device)
```bash
m87 login --agent                        # Register this device
# Wait for manager approval
sudo "$(which m87)" agent enable --now   # Start agent service and persist after boot
```

## See Also

- [devices/](../devices/) - Device management commands
