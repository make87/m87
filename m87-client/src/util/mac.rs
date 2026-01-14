use std::fs;
use std::process::Command;

/// Check if a MAC address is valid (not all zeros)
pub fn is_valid_mac(mac: &str) -> bool {
    let trimmed = mac.trim();
    !trimmed.is_empty() && trimmed != "00:00:00:00:00:00"
}

/// Parse MAC address from `ip link` output line containing "link/ether"
pub fn parse_mac_from_ip_link_line(line: &str) -> Option<String> {
    if !line.contains("link/ether") {
        return None;
    }
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() > 1 {
        Some(parts[1].to_string())
    } else {
        None
    }
}

/// Try reading MAC from /sys/class/net or ip link
pub fn get_mac_address() -> Option<String> {
    // Try sysfs first
    if let Ok(entries) = fs::read_dir("/sys/class/net/") {
        for entry in entries.flatten() {
            let path = entry.path().join("address");
            if let Ok(addr) = fs::read_to_string(&path) {
                let mac = addr.trim().to_string();
                if is_valid_mac(&mac) {
                    return Some(mac);
                }
            }
        }
    }

    // Fallback: use `ip link` command
    if let Ok(out) = Command::new("ip").arg("link").output() {
        let text = String::from_utf8_lossy(&out.stdout);
        for line in text.lines() {
            if let Some(mac) = parse_mac_from_ip_link_line(line) {
                return Some(mac);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_mac_valid() {
        assert!(is_valid_mac("aa:bb:cc:dd:ee:ff"));
        assert!(is_valid_mac("00:11:22:33:44:55"));
    }

    #[test]
    fn test_is_valid_mac_all_zeros() {
        assert!(!is_valid_mac("00:00:00:00:00:00"));
    }

    #[test]
    fn test_is_valid_mac_empty() {
        assert!(!is_valid_mac(""));
        assert!(!is_valid_mac("   "));
    }

    #[test]
    fn test_is_valid_mac_with_whitespace() {
        assert!(is_valid_mac("  aa:bb:cc:dd:ee:ff  "));
        assert!(!is_valid_mac("  00:00:00:00:00:00  "));
    }

    #[test]
    fn test_parse_mac_from_ip_link_line_valid() {
        let line = "    link/ether aa:bb:cc:dd:ee:ff brd ff:ff:ff:ff:ff:ff";
        assert_eq!(
            parse_mac_from_ip_link_line(line),
            Some("aa:bb:cc:dd:ee:ff".to_string())
        );
    }

    #[test]
    fn test_parse_mac_from_ip_link_line_no_ether() {
        let line = "2: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500";
        assert_eq!(parse_mac_from_ip_link_line(line), None);
    }

    #[test]
    fn test_parse_mac_from_ip_link_line_link_loopback() {
        let line = "    link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00";
        assert_eq!(parse_mac_from_ip_link_line(line), None);
    }

    #[test]
    fn test_parse_mac_from_ip_link_line_empty() {
        assert_eq!(parse_mac_from_ip_link_line(""), None);
    }

    #[test]
    fn test_get_mac_address_returns_something() {
        // On a real Linux system, this should return a MAC address
        // We just check it doesn't panic and returns a plausible value
        let mac = get_mac_address();
        if let Some(m) = mac {
            assert!(m.contains(':'));
            assert!(is_valid_mac(&m));
        }
    }
}
