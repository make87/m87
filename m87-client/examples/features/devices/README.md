# Device Management

List and manage devices in your organization.

## Overview

The `m87 devices` commands let you view all connected devices and approve or reject pending registrations.

## Basic Usage

```bash
# List all devices
m87 devices list

# Approve a pending device
m87 devices approve <device>

# Reject a pending device
m87 devices reject <device>
```

## List Devices

```bash
m87 devices list
```

Output:
```
DEVICE ID   NAME            STATUS   ARCH    OS                               IP              LAST SEEN
abc123      rpi-garage      online   arm64   Debian GNU/Linux 12 (bookworm)   192.168.1.50    2 min ago
def456      office-server   offline  x86_64  Ubuntu 24.04 LTS                 10.0.0.100      3 days ago
ghi789      edge-node       pending  arm64   Raspbian GNU/Linux 11            -               just now
```

### Status Values

| Status | Description |
|--------|-------------|
| `online` | Device is connected and reachable |
| `offline` | Device is registered but not currently connected |
| `pending` | Device awaiting approval |

## Approve Device

Allow a pending device to join your organization:

```bash
m87 devices approve rpi-garage
```

Once approved, the device can be accessed via `m87 <device> shell`, `m87 <device> exec`, etc.

You can also approve devices via the [make87 Platform](https://app.make87.com).

## Reject Device

Deny a pending device registration:

```bash
m87 devices reject unknown-device
```

The device will need to re-register if rejected.

## Examples

### Fleet Overview
```bash
# Check which devices are online
m87 devices list
```

### Approve New Runtime
```bash
# Device registered via `m87 runtime run` on remote machine
m87 devices list             # Shows device as "pending"
m87 devices approve rpi      # Allow access
m87 rpi shell                # Connect to the device
```

## See Also

- [auth/](../auth/) - Authentication and runtime registration
- [shell/](../shell/) - Interactive shell access
