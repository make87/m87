# Authentication

Authenticate with the make87 platform.

## Overview

m87 supports two authentication modes:
- **Manager login** (default): OAuth2 browser flow for managing devices from your computer
- **Runtime registration** (Linux only): Headless device registration for autonomous runtimes

Nothing prevents you from running a runtime and a manager on the same device.

## Manager Login

```bash
# Opens browser for OAuth2 authentication
m87 login
```

After authentication, your CLI is authorized to manage devices across your organization.

## Runtime Registration (Linux)

Register a device as a runtime to enable remote management:

```bash
# Register and run the runtime (prompts for org selection)
m87 runtime run

# Register under specific organization
m87 runtime run --org-id <org-id>

# Register under specific user email
m87 runtime run --email admin@example.com
```

After registration, the device appears in `m87 devices list` with status "pending" until approved by a manager.

## Logout

```bash
# Remove all local credentials
m87 logout
```

This clears both manager and runtime credentials from the device.

## Flags

| Flag | Description |
|------|-------------|
| `--org-id <id>` | Organization ID for runtime registration |
| `--email <email>` | User email for runtime registration |

## Workflow

### Manager Setup
```bash
m87 login                    # Authenticate as manager
m87 devices list             # View all devices
m87 devices approve rpi      # Approve pending runtime
```

### Runtime Setup (on the device)
```bash
m87 runtime run              # Register and run this device
# Wait for manager approval
m87 runtime enable --now     # Install, enable and start service (prompts for sudo)
```

## See Also

- [devices/](../devices/) - Device management commands
