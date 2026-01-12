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
        StreamType::Forward {
            token: token.to_string(),
            target: ForwardTarget::Udp(self.clone()),
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
        StreamType::Forward {
            token: token.to_string(),
            target: ForwardTarget::Tcp(self.clone()),
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
        StreamType::Forward {
            token: token.to_string(),
            target: ForwardTarget::Socket(self.clone()),
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
        StreamType::Forward {
            token: token.to_string(),
            target: ForwardTarget::Vpn(self.clone()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ForwardTarget {
    Tcp(TcpTarget),
    Udp(UdpTarget),
    Socket(SocketTarget),
    Vpn(VpnTarget),
}

#[derive(Debug)]
pub enum ForwardParseError {
    InvalidProtocol(String),
    InvalidSyntax(String),
    InvalidPort(ParseIntError),
    InvalidRange(String),
}

impl fmt::Display for ForwardParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ForwardParseError::InvalidProtocol(p) => write!(f, "invalid protocol '{}'", p),
            ForwardParseError::InvalidSyntax(s) => write!(f, "invalid forward spec '{}'", s),
            ForwardParseError::InvalidPort(e) => write!(f, "invalid number: {}", e),
            ForwardParseError::InvalidRange(r) => write!(f, "invalid port range: {}", r),
        }
    }
}

impl std::error::Error for ForwardParseError {}

impl From<ParseIntError> for ForwardParseError {
    fn from(e: ParseIntError) -> Self {
        ForwardParseError::InvalidPort(e)
    }
}

/// Parse a port spec that may be a single port or a range (e.g., "8080" or "8080-8090")
/// Returns (start, end) where start == end for single ports
fn parse_port_spec(s: &str) -> Result<(u16, u16), ForwardParseError> {
    if let Some((start_str, end_str)) = s.split_once('-') {
        let start: u16 = start_str.parse()?;
        let end: u16 = end_str.parse()?;
        if end < start {
            return Err(ForwardParseError::InvalidRange(format!(
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
impl ForwardTarget {
    pub fn from_list(specs: Vec<String>) -> Result<Vec<Self>, ForwardParseError> {
        // CASE 1: empty input → default to VPN
        if specs.is_empty() {
            return Ok(vec![ForwardTarget::Vpn(VpnTarget {
                cidr: None,
                mtu: None,
            })]);
        }

        let mut out = Vec::new();

        for token in specs {
            // CASE 2: explicit "vpn"
            if token.eq_ignore_ascii_case("vpn") {
                out.push(ForwardTarget::Vpn(VpnTarget {
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

                out.push(ForwardTarget::Socket(SocketTarget {
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
                    return Err(ForwardParseError::InvalidProtocol(other.to_string()));
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
                            return Err(ForwardParseError::InvalidRange(format!(
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
                            return Err(ForwardParseError::InvalidRange(format!(
                                "local range size ({}) does not match remote range size ({})",
                                l_size + 1,
                                r_size + 1
                            )));
                        }

                        (l_start, l_end, (*host).to_string(), r_start, r_end)
                    }

                    _ => {
                        return Err(ForwardParseError::InvalidSyntax(body.to_string()));
                    }
                };

            // Expand ranges into individual targets
            let range_size = local_end - local_start;
            for offset in 0..=range_size {
                let local_port = local_start + offset;
                let remote_port = remote_start + offset;

                match protocol {
                    Some("tcp") => out.push(ForwardTarget::Tcp(TcpTarget {
                        local_port,
                        remote_host: remote_host.clone(),
                        remote_port,
                    })),
                    Some("udp") => out.push(ForwardTarget::Udp(UdpTarget {
                        local_port,
                        remote_host: remote_host.clone(),
                        remote_port,
                    })),
                    None => out.push(ForwardTarget::Tcp(TcpTarget {
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
        StreamType::Forward {
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
    Forward {
        token: String,
        target: ForwardTarget,
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
            StreamType::Forward { .. } => "Forward",
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
            StreamType::Forward { token, .. } => token,
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
        let targets = ForwardTarget::from_list(vec!["8080".to_string()]).unwrap();
        assert_eq!(targets.len(), 1);
        match &targets[0] {
            ForwardTarget::Tcp(t) => {
                assert_eq!(t.local_port, 8080);
                assert_eq!(t.remote_port, 8080);
                assert_eq!(t.remote_host, "127.0.0.1");
            }
            _ => panic!("Expected TcpTarget"),
        }
    }

    #[test]
    fn test_parse_port_range_same() {
        let targets = ForwardTarget::from_list(vec!["8080-8082".to_string()]).unwrap();
        assert_eq!(targets.len(), 3);

        for (i, target) in targets.iter().enumerate() {
            match target {
                ForwardTarget::Tcp(t) => {
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
        let targets = ForwardTarget::from_list(vec!["8080-8082:9080-9082".to_string()]).unwrap();
        assert_eq!(targets.len(), 3);

        for (i, target) in targets.iter().enumerate() {
            match target {
                ForwardTarget::Tcp(t) => {
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
            ForwardTarget::from_list(vec!["8080-8082:192.168.1.50:9080-9082".to_string()]).unwrap();
        assert_eq!(targets.len(), 3);

        for (i, target) in targets.iter().enumerate() {
            match target {
                ForwardTarget::Tcp(t) => {
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
        let targets = ForwardTarget::from_list(vec!["8080-8082/udp".to_string()]).unwrap();
        assert_eq!(targets.len(), 3);

        for (i, target) in targets.iter().enumerate() {
            match target {
                ForwardTarget::Udp(t) => {
                    assert_eq!(t.local_port, 8080 + i as u16);
                    assert_eq!(t.remote_port, 8080 + i as u16);
                }
                _ => panic!("Expected UdpTarget"),
            }
        }
    }

    #[test]
    fn test_parse_port_range_mismatch() {
        let result = ForwardTarget::from_list(vec!["8080-8082:9080-9085".to_string()]);
        assert!(result.is_err());
        match result {
            Err(ForwardParseError::InvalidRange(msg)) => {
                assert!(msg.contains("does not match"));
            }
            _ => panic!("Expected InvalidRange error"),
        }
    }

    #[test]
    fn test_parse_port_range_invalid_order() {
        let result = ForwardTarget::from_list(vec!["8090-8080".to_string()]);
        assert!(result.is_err());
        match result {
            Err(ForwardParseError::InvalidRange(msg)) => {
                assert!(msg.contains("less than"));
            }
            _ => panic!("Expected InvalidRange error"),
        }
    }

    #[test]
    fn test_parse_mixed_single_and_range() {
        let targets =
            ForwardTarget::from_list(vec!["8080".to_string(), "3000-3002".to_string()]).unwrap();
        assert_eq!(targets.len(), 4); // 1 + 3

        match &targets[0] {
            ForwardTarget::Tcp(t) => {
                assert_eq!(t.local_port, 8080);
            }
            _ => panic!("Expected TcpTarget"),
        }

        for (i, target) in targets[1..].iter().enumerate() {
            match target {
                ForwardTarget::Tcp(t) => {
                    assert_eq!(t.local_port, 3000 + i as u16);
                }
                _ => panic!("Expected TcpTarget"),
            }
        }
    }

    #[test]
    fn test_existing_single_port_with_remote() {
        // Ensure existing functionality still works
        let targets = ForwardTarget::from_list(vec!["8080:9090".to_string()]).unwrap();
        assert_eq!(targets.len(), 1);
        match &targets[0] {
            ForwardTarget::Tcp(t) => {
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
            ForwardTarget::from_list(vec!["8080:192.168.1.50:9090".to_string()]).unwrap();
        assert_eq!(targets.len(), 1);
        match &targets[0] {
            ForwardTarget::Tcp(t) => {
                assert_eq!(t.local_port, 8080);
                assert_eq!(t.remote_port, 9090);
                assert_eq!(t.remote_host, "192.168.1.50");
            }
            _ => panic!("Expected TcpTarget"),
        }
    }

    // --- StreamType helper tests ---

    #[test]
    fn test_variant_name_all_variants() {
        let token = "test-token".to_string();

        assert_eq!(
            StreamType::Terminal {
                token: token.clone(),
                term: None
            }
            .variant_name(),
            "Terminal"
        );
        assert_eq!(
            StreamType::Exec {
                token: token.clone()
            }
            .variant_name(),
            "Exec"
        );
        assert_eq!(
            StreamType::Logs {
                token: token.clone()
            }
            .variant_name(),
            "Logs"
        );
        assert_eq!(
            StreamType::Forward {
                token: token.clone(),
                target: ForwardTarget::Tcp(TcpTarget {
                    remote_host: "127.0.0.1".to_string(),
                    remote_port: 80,
                    local_port: 80
                })
            }
            .variant_name(),
            "Forward"
        );
        assert_eq!(
            StreamType::Serial {
                token: token.clone(),
                name: "ttyUSB0".to_string(),
                baud: None
            }
            .variant_name(),
            "Serial"
        );
        assert_eq!(
            StreamType::Metrics {
                token: token.clone()
            }
            .variant_name(),
            "Metrics"
        );
        assert_eq!(
            StreamType::Docker {
                token: token.clone()
            }
            .variant_name(),
            "Docker"
        );
        assert_eq!(StreamType::Ssh { token }.variant_name(), "Ssh");
    }

    #[test]
    fn test_get_token_from_all_variants() {
        let token = "my-unique-token".to_string();

        assert_eq!(
            StreamType::Terminal {
                token: token.clone(),
                term: Some("xterm".to_string())
            }
            .get_token(),
            "my-unique-token"
        );
        assert_eq!(
            StreamType::Exec {
                token: token.clone()
            }
            .get_token(),
            "my-unique-token"
        );
        assert_eq!(
            StreamType::Logs {
                token: token.clone()
            }
            .get_token(),
            "my-unique-token"
        );
        assert_eq!(
            StreamType::Metrics {
                token: token.clone()
            }
            .get_token(),
            "my-unique-token"
        );
        assert_eq!(
            StreamType::Docker {
                token: token.clone()
            }
            .get_token(),
            "my-unique-token"
        );
        assert_eq!(StreamType::Ssh { token }.get_token(), "my-unique-token");
    }

    // --- ForwardParseError Display tests ---

    #[test]
    fn test_forward_parse_error_display_protocol() {
        let err = ForwardParseError::InvalidProtocol("xyz".to_string());
        assert_eq!(format!("{}", err), "invalid protocol 'xyz'");
    }

    #[test]
    fn test_forward_parse_error_display_syntax() {
        let err = ForwardParseError::InvalidSyntax("a:b:c:d:e".to_string());
        assert_eq!(format!("{}", err), "invalid forward spec 'a:b:c:d:e'");
    }

    #[test]
    fn test_forward_parse_error_display_range() {
        let err = ForwardParseError::InvalidRange("end < start".to_string());
        assert_eq!(format!("{}", err), "invalid port range: end < start");
    }

    #[test]
    fn test_forward_parse_error_from_parse_int() {
        let parse_err: Result<u16, _> = "not_a_number".parse();
        let forward_err: ForwardParseError = parse_err.unwrap_err().into();
        let display = format!("{}", forward_err);
        assert!(display.starts_with("invalid number:"));
    }

    #[test]
    fn test_forward_parse_error_is_std_error() {
        let err: Box<dyn std::error::Error> =
            Box::new(ForwardParseError::InvalidProtocol("test".to_string()));
        // Just verify it implements std::error::Error
        assert!(!err.to_string().is_empty());
    }

    // --- Edge case tests ---

    #[test]
    fn test_empty_specs_defaults_to_vpn() {
        let targets = ForwardTarget::from_list(vec![]).unwrap();
        assert_eq!(targets.len(), 1);
        match &targets[0] {
            ForwardTarget::Vpn(v) => {
                assert!(v.cidr.is_none());
                assert!(v.mtu.is_none());
            }
            _ => panic!("Expected VpnTarget"),
        }
    }

    #[test]
    fn test_explicit_vpn_keyword() {
        let targets = ForwardTarget::from_list(vec!["vpn".to_string()]).unwrap();
        assert_eq!(targets.len(), 1);
        matches!(&targets[0], ForwardTarget::Vpn(_));
    }

    #[test]
    fn test_vpn_keyword_case_insensitive() {
        let targets = ForwardTarget::from_list(vec!["VPN".to_string()]).unwrap();
        assert_eq!(targets.len(), 1);
        matches!(&targets[0], ForwardTarget::Vpn(_));

        let targets = ForwardTarget::from_list(vec!["Vpn".to_string()]).unwrap();
        assert_eq!(targets.len(), 1);
        matches!(&targets[0], ForwardTarget::Vpn(_));
    }

    #[test]
    fn test_socket_path_simple() {
        let targets = ForwardTarget::from_list(vec!["/var/run/test.sock".to_string()]).unwrap();
        assert_eq!(targets.len(), 1);
        match &targets[0] {
            ForwardTarget::Socket(s) => {
                assert_eq!(s.local_path, "/var/run/test.sock");
                assert_eq!(s.remote_path, "/var/run/test.sock");
            }
            _ => panic!("Expected SocketTarget"),
        }
    }

    #[test]
    fn test_socket_path_with_remote() {
        let targets =
            ForwardTarget::from_list(vec!["/local/path.sock:/remote/path.sock".to_string()])
                .unwrap();
        assert_eq!(targets.len(), 1);
        match &targets[0] {
            ForwardTarget::Socket(s) => {
                assert_eq!(s.local_path, "/local/path.sock");
                assert_eq!(s.remote_path, "/remote/path.sock");
            }
            _ => panic!("Expected SocketTarget"),
        }
    }

    #[test]
    fn test_port_boundary_min() {
        let targets = ForwardTarget::from_list(vec!["1".to_string()]).unwrap();
        assert_eq!(targets.len(), 1);
        match &targets[0] {
            ForwardTarget::Tcp(t) => {
                assert_eq!(t.local_port, 1);
                assert_eq!(t.remote_port, 1);
            }
            _ => panic!("Expected TcpTarget"),
        }
    }

    #[test]
    fn test_port_boundary_max() {
        let targets = ForwardTarget::from_list(vec!["65535".to_string()]).unwrap();
        assert_eq!(targets.len(), 1);
        match &targets[0] {
            ForwardTarget::Tcp(t) => {
                assert_eq!(t.local_port, 65535);
                assert_eq!(t.remote_port, 65535);
            }
            _ => panic!("Expected TcpTarget"),
        }
    }

    #[test]
    fn test_port_overflow() {
        let result = ForwardTarget::from_list(vec!["65536".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_protocol() {
        let result = ForwardTarget::from_list(vec!["8080/xyz".to_string()]);
        assert!(result.is_err());
        match result {
            Err(ForwardParseError::InvalidProtocol(p)) => assert_eq!(p, "xyz"),
            _ => panic!("Expected InvalidProtocol error"),
        }
    }

    #[test]
    fn test_tcp_protocol_explicit() {
        let targets = ForwardTarget::from_list(vec!["8080/tcp".to_string()]).unwrap();
        assert_eq!(targets.len(), 1);
        matches!(&targets[0], ForwardTarget::Tcp(_));
    }

    #[test]
    fn test_to_stream_type_methods() {
        let tcp = TcpTarget {
            remote_host: "localhost".to_string(),
            remote_port: 80,
            local_port: 8080,
        };
        let stream = tcp.to_stream_type("token123");
        assert_eq!(stream.get_token(), "token123");
        assert_eq!(stream.variant_name(), "Forward");

        let udp = UdpTarget {
            remote_host: "localhost".to_string(),
            remote_port: 53,
            local_port: 5353,
        };
        let stream = udp.to_stream_type("token456");
        assert_eq!(stream.get_token(), "token456");

        let socket = SocketTarget {
            local_path: "/tmp/a.sock".to_string(),
            remote_path: "/tmp/b.sock".to_string(),
        };
        let stream = socket.to_stream_type("token789");
        assert_eq!(stream.get_token(), "token789");

        let vpn = VpnTarget {
            cidr: Some("10.0.0.0/24".to_string()),
            mtu: Some(1400),
        };
        let stream = vpn.to_stream_type("tokenvpn");
        assert_eq!(stream.get_token(), "tokenvpn");
    }

    #[test]
    fn test_forward_target_to_stream_type() {
        let target = ForwardTarget::Tcp(TcpTarget {
            remote_host: "example.com".to_string(),
            remote_port: 443,
            local_port: 8443,
        });
        let stream = target.to_stream_type("mytoken");
        match stream {
            StreamType::Forward { token, target: _ } => {
                assert_eq!(token, "mytoken");
            }
            _ => panic!("Expected Forward variant"),
        }
    }
}
