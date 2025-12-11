//! Device list and output parsing utilities

use regex::Regex;

/// Information about a device from the CLI output
#[derive(Debug, Clone)]
pub struct DeviceInfo {
    pub short_id: String,
    pub name: String,
    pub status: String,
}

/// Parse device list CLI output into structured data
///
/// Actual CLI output format (fixed-width columns):
/// ```text
/// DEVICE ID   NAME            STATUS   ARCH    OS                               IP              LAST SEEN            PENDING  REQUEST ID
/// edc785      4d2b877982f8    online   arm64   Debian GNU/Linux 12              154.20.186.39   51 sec ago
/// ```
///
/// Column positions (0-indexed):
/// - DEVICE ID: 0-11 (12 chars)
/// - NAME: 12-27 (16 chars)
/// - STATUS: 28-36 (9 chars)
/// - Rest varies...
pub fn parse_devices_list(output: &str) -> Vec<DeviceInfo> {
    output
        .lines()
        .filter_map(|line| {
            // Skip header line
            if line.starts_with("DEVICE ID") || line.starts_with("SHORT_ID") || line.trim().is_empty() {
                return None;
            }

            // Parse fixed-width columns
            // DEVICE ID is columns 0-11, NAME is 12-27, STATUS is 28-36
            if line.len() >= 36 {
                let short_id = line[0..12].trim().to_string();
                let name = line[12..28].trim().to_string();
                let status = line[28..37].trim().to_string();

                // Skip if short_id is empty (happens for pending devices without ID)
                if short_id.is_empty() {
                    return None;
                }

                Some(DeviceInfo {
                    short_id,
                    name,
                    status,
                })
            } else {
                // Fallback to whitespace splitting for short lines
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 3 {
                    Some(DeviceInfo {
                        short_id: parts[0].to_string(),
                        name: parts[1].to_string(),
                        status: parts[2].to_string(),
                    })
                } else {
                    None
                }
            }
        })
        .collect()
}

/// Find device by name in parsed list
pub fn find_device_by_name<'a>(devices: &'a [DeviceInfo], name: &str) -> Option<&'a DeviceInfo> {
    devices.iter().find(|d| d.name == name)
}

/// Find device by short_id in parsed list
pub fn find_device_by_short_id<'a>(
    devices: &'a [DeviceInfo],
    short_id: &str,
) -> Option<&'a DeviceInfo> {
    devices.iter().find(|d| d.short_id == short_id)
}

/// Extract auth request UUIDs from device list output
///
/// Looks for UUID patterns that appear as pending auth requests
pub fn extract_auth_requests(output: &str) -> Vec<String> {
    let uuid_regex =
        Regex::new(r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})").unwrap();

    uuid_regex
        .captures_iter(output)
        .map(|cap| cap[1].to_string())
        .collect()
}

/// Find devices with a specific status
pub fn find_devices_by_status<'a>(devices: &'a [DeviceInfo], status: &str) -> Vec<&'a DeviceInfo> {
    devices.iter().filter(|d| d.status == status).collect()
}

/// Find the first registered/online device
pub fn find_registered_device(devices: &[DeviceInfo]) -> Option<&DeviceInfo> {
    devices.iter().find(|d| d.status == "online" || d.status == "registered")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_devices_list() {
        // Test with actual CLI output format (fixed-width columns)
        let output = "DEVICE ID   NAME            STATUS   ARCH    OS                               IP              LAST SEEN            PENDING  REQUEST ID\nedc785      4d2b877982f8    online   arm64   Debian GNU/Linux 12              154.20.186.39   51 sec ago\n";
        let devices = parse_devices_list(output);
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].short_id, "edc785");
        assert_eq!(devices[0].name, "4d2b877982f8");
        assert_eq!(devices[0].status, "online");
    }

    #[test]
    fn test_parse_devices_list_pending() {
        // Test with pending device (no device ID)
        let output = "DEVICE ID   NAME            STATUS   ARCH    OS                               IP              LAST SEEN            PENDING  REQUEST ID\n            4d2b877982f8    pending  arm64   Debian GNU/Linux 12              154.20.186.39                        yes      4a1b6213-24ab-4742-a5d7-913be9ad61ec\n";
        let devices = parse_devices_list(output);
        // Pending devices without ID should be skipped
        assert_eq!(devices.len(), 0);
    }

    #[test]
    fn test_extract_auth_requests() {
        let output =
            "some text 550e8400-e29b-41d4-a716-446655440000 more text 6ba7b810-9dad-11d1-80b4-00c04fd430c8";
        let uuids = extract_auth_requests(output);
        assert_eq!(uuids.len(), 2);
        assert_eq!(uuids[0], "550e8400-e29b-41d4-a716-446655440000");
    }
}
