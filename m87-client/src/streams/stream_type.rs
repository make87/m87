use serde::{Deserialize, Serialize};
use std::{fmt, num::ParseIntError};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UdpTarget {
    pub remote_host: String,
    pub remote_port: u16,
    pub local_port: u16,
}

impl UdpTarget {
    pub fn to_stream_type(&self, token: &str) -> StreamType {
        StreamType::Tunnel {
            token: token.to_string(),
            target: TunnelTarget::Udp(self.clone()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TcpTarget {
    pub remote_host: String,
    pub remote_port: u16,
    pub local_port: u16,
}

impl TcpTarget {
    pub fn to_stream_type(&self, token: &str) -> StreamType {
        StreamType::Tunnel {
            token: token.to_string(),
            target: TunnelTarget::Tcp(self.clone()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SocketTarget {
    pub local_path: String,
    pub remote_path: String,
}

impl SocketTarget {
    pub fn to_stream_type(&self, token: &str) -> StreamType {
        StreamType::Tunnel {
            token: token.to_string(),
            target: TunnelTarget::Socket(self.clone()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct VpnTarget {
    pub cidr: Option<String>,
    pub mtu: Option<u32>,
}

impl VpnTarget {
    pub fn to_stream_type(&self, token: &str) -> StreamType {
        StreamType::Tunnel {
            token: token.to_string(),
            target: TunnelTarget::Vpn(self.clone()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TunnelTarget {
    Tcp(TcpTarget),
    Udp(UdpTarget),
    Socket(SocketTarget),
    Vpn(VpnTarget),
}

#[derive(Debug)]
pub enum TunnelParseError {
    InvalidProtocol(String),
    InvalidSyntax(String),
    InvalidPort(ParseIntError),
    InvalidRange(String),
}

impl fmt::Display for TunnelParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TunnelParseError::InvalidProtocol(p) => write!(f, "invalid protocol '{}'", p),
            TunnelParseError::InvalidSyntax(s) => write!(f, "invalid tunnel spec '{}'", s),
            TunnelParseError::InvalidPort(e) => write!(f, "invalid number: {}", e),
            TunnelParseError::InvalidRange(r) => write!(f, "invalid port range: {}", r),
        }
    }
}

impl std::error::Error for TunnelParseError {}

impl From<ParseIntError> for TunnelParseError {
    fn from(e: ParseIntError) -> Self {
        TunnelParseError::InvalidPort(e)
    }
}

/// Parse a port spec that may be a single port or a range (e.g., "8080" or "8080-8090")
/// Returns (start, end) where start == end for single ports
fn parse_port_spec(s: &str) -> Result<(u16, u16), TunnelParseError> {
    if let Some((start_str, end_str)) = s.split_once('-') {
        let start: u16 = start_str.parse()?;
        let end: u16 = end_str.parse()?;
        if end < start {
            return Err(TunnelParseError::InvalidRange(format!(
                "end port {} is less than start port {}",
                end, start
            )));
        }
        Ok((start, end))
    } else {
        let port: u16 = s.parse()?;
        Ok((port, port))
    }
}

// Examples accepted (SSH -L style: local_port:remote_host:remote_port):
// "8080"                         -> forward local:8080 to remote 127.0.0.1:8080
// "8080:1337"                    -> forward local:8080 to remote 127.0.0.1:1337
// "1554:192.168.0.101:554"       -> forward local:1554 to remote 192.168.0.101:554
// "8080/tcp"                     -> TCP only
// "8080:1337/udp"                -> UDP only
// "1554:192.168.0.101:554/tcp"   -> TCP to specific host
// "8080-8090"                    -> forward local:8080-8090 to remote 8080-8090
// "8080-8090:9080-9090"          -> forward local:8080-8090 to remote 9080-9090 (offset mapping)
// "8080-8090:192.168.0.101:9080-9090/tcp" -> TCP range to specific host with offset
// /var/run/jtop.sock             -> forward local jtop socket to remote jtop socket
// /var/run/jtop.sock:/var/run/remote.sock -> forward local jtop socket to remote jtop socket
impl TunnelTarget {
    pub fn from_list(specs: Vec<String>) -> Result<Vec<Self>, TunnelParseError> {
        // CASE 1: empty input → default to VPN
        if specs.is_empty() {
            return Ok(vec![TunnelTarget::Vpn(VpnTarget {
                cidr: None,
                mtu: None,
            })]);
        }

        let mut out = Vec::new();

        for token in specs {
            // CASE 2: explicit "vpn"
            if token.eq_ignore_ascii_case("vpn") {
                out.push(TunnelTarget::Vpn(VpnTarget {
                    cidr: None,
                    mtu: None,
                }));
                continue;
            }

            //
            // CASE 3: UNIX socket forwarding
            //
            if token.starts_with('/') {
                let (local, remote) = match token.split_once(':') {
                    Some((l, r)) => (l.to_string(), r.to_string()),
                    None => {
                        let p = token.to_string();
                        (p.clone(), p)
                    }
                };

                out.push(TunnelTarget::Socket(SocketTarget {
                    local_path: local,
                    remote_path: remote,
                }));

                continue;
            }

            //
            // CASE 4: TCP/UDP parsing (with port range support)
            //
            let mut parts = token.split('/');
            let body = parts.next().unwrap();
            let proto = parts.next();

            let protocol = match proto {
                Some("tcp") => Some("tcp"),
                Some("udp") => Some("udp"),
                Some(other) => {
                    return Err(TunnelParseError::InvalidProtocol(other.to_string()));
                }
                None => None,
            };

            let nums: Vec<&str> = body.split(':').collect();

            // Parse port specs (may be single ports or ranges)
            let (local_start, local_end, remote_host, remote_start, _remote_end) =
                match nums.as_slice() {
                    // "8080" or "8080-8090" → local range to same remote range
                    [lp] => {
                        let (l_start, l_end) = parse_port_spec(lp)?;
                        (l_start, l_end, "127.0.0.1".to_string(), l_start, l_end)
                    }

                    // "8080:9080" or "8080-8090:9080-9090" → local to remote (with optional ranges)
                    [lp, rp] => {
                        let (l_start, l_end) = parse_port_spec(lp)?;
                        let (r_start, r_end) = parse_port_spec(rp)?;

                        // Validate range sizes match
                        let l_size = l_end - l_start;
                        let r_size = r_end - r_start;
                        if l_size != r_size {
                            return Err(TunnelParseError::InvalidRange(format!(
                                "local range size ({}) does not match remote range size ({})",
                                l_size + 1,
                                r_size + 1
                            )));
                        }

                        (l_start, l_end, "127.0.0.1".to_string(), r_start, r_end)
                    }

                    // "8080:host:9080" or "8080-8090:host:9080-9090" → local to host:remote
                    [lp, host, rp] => {
                        let (l_start, l_end) = parse_port_spec(lp)?;
                        let (r_start, r_end) = parse_port_spec(rp)?;

                        // Validate range sizes match
                        let l_size = l_end - l_start;
                        let r_size = r_end - r_start;
                        if l_size != r_size {
                            return Err(TunnelParseError::InvalidRange(format!(
                                "local range size ({}) does not match remote range size ({})",
                                l_size + 1,
                                r_size + 1
                            )));
                        }

                        (l_start, l_end, (*host).to_string(), r_start, r_end)
                    }

                    _ => {
                        return Err(TunnelParseError::InvalidSyntax(body.to_string()));
                    }
                };

            // Expand ranges into individual targets
            let range_size = local_end - local_start;
            for offset in 0..=range_size {
                let local_port = local_start + offset;
                let remote_port = remote_start + offset;

                match protocol {
                    Some("tcp") => out.push(TunnelTarget::Tcp(TcpTarget {
                        local_port,
                        remote_host: remote_host.clone(),
                        remote_port,
                    })),
                    Some("udp") => out.push(TunnelTarget::Udp(UdpTarget {
                        local_port,
                        remote_host: remote_host.clone(),
                        remote_port,
                    })),
                    None => out.push(TunnelTarget::Tcp(TcpTarget {
                        local_port,
                        remote_host: remote_host.clone(),
                        remote_port,
                    })),
                    _ => unreachable!(),
                }
            }
        }

        Ok(out)
    }

    pub fn to_stream_type(&self, token: &str) -> StreamType {
        StreamType::Tunnel {
            token: token.to_string(),
            target: self.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum StreamType {
    Terminal {
        token: String,
        term: Option<String>,
    },
    Exec {
        token: String,
    },
    Logs {
        token: String,
    },
    Tunnel {
        token: String,
        target: TunnelTarget,
    },
    Serial {
        token: String,
        name: String,
        baud: Option<u32>,
    },
    Metrics {
        token: String,
    },
    Docker {
        token: String,
    },
    Ssh {
        token: String,
    },
}

impl StreamType {
    pub fn variant_name(&self) -> &'static str {
        match self {
            StreamType::Terminal { .. } => "Terminal",
            StreamType::Exec { .. } => "Exec",
            StreamType::Logs { .. } => "Logs",
            StreamType::Tunnel { .. } => "Tunnel",
            StreamType::Serial { .. } => "Serial",
            StreamType::Metrics { .. } => "Metrics",
            StreamType::Docker { .. } => "Docker",
            StreamType::Ssh { .. } => "Ssh",
        }
    }

    pub fn get_token(&self) -> &str {
        match self {
            StreamType::Terminal { token, .. } => token,
            StreamType::Exec { token, .. } => token,
            StreamType::Logs { token, .. } => token,
            StreamType::Tunnel { token, .. } => token,
            StreamType::Serial { token, .. } => token,
            StreamType::Metrics { token } => token,
            StreamType::Docker { token } => token,
            StreamType::Ssh { token } => token,
        }
    }

    pub async fn from_incoming_stream(recv: &mut quinn::RecvStream) -> anyhow::Result<StreamType> {
        // length header
        let mut len_buf = [0u8; 4];
        recv.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        // json body
        let mut buf = vec![0u8; len];
        recv.read_exact(&mut buf).await?;

        // deserialize directly into enum
        let msg: StreamType = serde_json::from_slice(&buf)?;
        Ok(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_port() {
        let targets = TunnelTarget::from_list(vec!["8080".to_string()]).unwrap();
        assert_eq!(targets.len(), 1);
        match &targets[0] {
            TunnelTarget::Tcp(t) => {
                assert_eq!(t.local_port, 8080);
                assert_eq!(t.remote_port, 8080);
                assert_eq!(t.remote_host, "127.0.0.1");
            }
            _ => panic!("Expected TcpTarget"),
        }
    }

    #[test]
    fn test_parse_port_range_same() {
        let targets = TunnelTarget::from_list(vec!["8080-8082".to_string()]).unwrap();
        assert_eq!(targets.len(), 3);

        for (i, target) in targets.iter().enumerate() {
            match target {
                TunnelTarget::Tcp(t) => {
                    assert_eq!(t.local_port, 8080 + i as u16);
                    assert_eq!(t.remote_port, 8080 + i as u16);
                    assert_eq!(t.remote_host, "127.0.0.1");
                }
                _ => panic!("Expected TcpTarget"),
            }
        }
    }

    #[test]
    fn test_parse_port_range_offset() {
        let targets = TunnelTarget::from_list(vec!["8080-8082:9080-9082".to_string()]).unwrap();
        assert_eq!(targets.len(), 3);

        for (i, target) in targets.iter().enumerate() {
            match target {
                TunnelTarget::Tcp(t) => {
                    assert_eq!(t.local_port, 8080 + i as u16);
                    assert_eq!(t.remote_port, 9080 + i as u16);
                    assert_eq!(t.remote_host, "127.0.0.1");
                }
                _ => panic!("Expected TcpTarget"),
            }
        }
    }

    #[test]
    fn test_parse_port_range_with_host() {
        let targets =
            TunnelTarget::from_list(vec!["8080-8082:192.168.1.50:9080-9082".to_string()]).unwrap();
        assert_eq!(targets.len(), 3);

        for (i, target) in targets.iter().enumerate() {
            match target {
                TunnelTarget::Tcp(t) => {
                    assert_eq!(t.local_port, 8080 + i as u16);
                    assert_eq!(t.remote_port, 9080 + i as u16);
                    assert_eq!(t.remote_host, "192.168.1.50");
                }
                _ => panic!("Expected TcpTarget"),
            }
        }
    }

    #[test]
    fn test_parse_port_range_udp() {
        let targets = TunnelTarget::from_list(vec!["8080-8082/udp".to_string()]).unwrap();
        assert_eq!(targets.len(), 3);

        for (i, target) in targets.iter().enumerate() {
            match target {
                TunnelTarget::Udp(t) => {
                    assert_eq!(t.local_port, 8080 + i as u16);
                    assert_eq!(t.remote_port, 8080 + i as u16);
                }
                _ => panic!("Expected UdpTarget"),
            }
        }
    }

    #[test]
    fn test_parse_port_range_mismatch() {
        let result = TunnelTarget::from_list(vec!["8080-8082:9080-9085".to_string()]);
        assert!(result.is_err());
        match result {
            Err(TunnelParseError::InvalidRange(msg)) => {
                assert!(msg.contains("does not match"));
            }
            _ => panic!("Expected InvalidRange error"),
        }
    }

    #[test]
    fn test_parse_port_range_invalid_order() {
        let result = TunnelTarget::from_list(vec!["8090-8080".to_string()]);
        assert!(result.is_err());
        match result {
            Err(TunnelParseError::InvalidRange(msg)) => {
                assert!(msg.contains("less than"));
            }
            _ => panic!("Expected InvalidRange error"),
        }
    }

    #[test]
    fn test_parse_mixed_single_and_range() {
        let targets =
            TunnelTarget::from_list(vec!["8080".to_string(), "3000-3002".to_string()]).unwrap();
        assert_eq!(targets.len(), 4); // 1 + 3

        match &targets[0] {
            TunnelTarget::Tcp(t) => {
                assert_eq!(t.local_port, 8080);
            }
            _ => panic!("Expected TcpTarget"),
        }

        for (i, target) in targets[1..].iter().enumerate() {
            match target {
                TunnelTarget::Tcp(t) => {
                    assert_eq!(t.local_port, 3000 + i as u16);
                }
                _ => panic!("Expected TcpTarget"),
            }
        }
    }

    #[test]
    fn test_existing_single_port_with_remote() {
        // Ensure existing functionality still works
        let targets = TunnelTarget::from_list(vec!["8080:9090".to_string()]).unwrap();
        assert_eq!(targets.len(), 1);
        match &targets[0] {
            TunnelTarget::Tcp(t) => {
                assert_eq!(t.local_port, 8080);
                assert_eq!(t.remote_port, 9090);
            }
            _ => panic!("Expected TcpTarget"),
        }
    }

    #[test]
    fn test_existing_single_port_with_host() {
        // Ensure existing functionality still works
        let targets =
            TunnelTarget::from_list(vec!["8080:192.168.1.50:9090".to_string()]).unwrap();
        assert_eq!(targets.len(), 1);
        match &targets[0] {
            TunnelTarget::Tcp(t) => {
                assert_eq!(t.local_port, 8080);
                assert_eq!(t.remote_port, 9090);
                assert_eq!(t.remote_host, "192.168.1.50");
            }
            _ => panic!("Expected TcpTarget"),
        }
    }
}
