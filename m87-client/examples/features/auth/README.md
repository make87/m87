# Authentication

Authenticate with the make87 platform.

## Overview

m87 supports two authentication modes:
- **Login** (default): OAuth2 browser flow for managing devices from your computer
- **Runtime registration** (Linux only): Headless device registration for autonomous runtimes

Nothing prevents you from running a runtime and using m87 command line on the same device.

## Login

```bash
# Opens browser for OAuth2 authentication
m87 login
```

After authentication, m87 command line is authorized to manage devices across your organization.

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

After registration, the device appears in `m87 devices list` with status "pending" until approved.

## Logout

```bash
# Remove all local credentials
m87 logout
```

This clears all credentials from the device.

## Flags

| Flag | Description |
|------|-------------|
| `--org-id <id>` | Organization ID for runtime registration |
| `--email <email>` | User email for runtime registration |

## Workflow

### Workstation Setup
```bash
m87 login                    # Authenticate
m87 devices list             # View all devices
m87 devices approve rpi      # Approve pending runtime
```

### Runtime Setup (on the device)
```bash
m87 runtime run              # Register and run this device
# Wait for approval
m87 runtime enable --now     # Install, enable and start service (prompts for sudo)
```

## See Also

- [devices/](../devices/) - Device management commands
