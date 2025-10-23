use std::fs;
use std::process::Command;

/// Try reading MAC from /sys/class/net or ip link
pub fn get_mac_address() -> Option<String> {
    // Try sysfs first
    if let Ok(entries) = fs::read_dir("/sys/class/net/") {
        for entry in entries.flatten() {
            let path = entry.path().join("address");
            if let Ok(addr) = fs::read_to_string(&path) {
                let mac = addr.trim().to_string();
                if mac != "00:00:00:00:00:00" {
                    return Some(mac);
                }
            }
        }
    }

    // Fallback: use `ip link` command
    if let Ok(out) = Command::new("ip").arg("link").output() {
        let text = String::from_utf8_lossy(&out.stdout);
        for line in text.lines() {
            if line.contains("link/ether") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() > 1 {
                    return Some(parts[1].to_string());
                }
            }
        }
    }

    None
}
