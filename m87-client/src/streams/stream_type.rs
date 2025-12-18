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
}

impl fmt::Display for TunnelParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TunnelParseError::InvalidProtocol(p) => write!(f, "invalid protocol '{}'", p),
            TunnelParseError::InvalidSyntax(s) => write!(f, "invalid tunnel spec '{}'", s),
            TunnelParseError::InvalidPort(e) => write!(f, "invalid number: {}", e),
        }
    }
}

impl std::error::Error for TunnelParseError {}

impl From<ParseIntError> for TunnelParseError {
    fn from(e: ParseIntError) -> Self {
        TunnelParseError::InvalidPort(e)
    }
}

// Examples accepted (SSH -L style: local_port:remote_host:remote_port):
// "8080"                         -> forward local:8080 to remote 127.0.0.1:8080
// "8080:1337"                    -> forward local:8080 to remote 127.0.0.1:1337
// "1554:192.168.0.101:554"       -> forward local:1554 to remote 192.168.0.101:554
// "8080/tcp"                     -> TCP only
// "8080:1337/udp"                -> UDP only
// "1554:192.168.0.101:554/tcp"   -> TCP to specific host
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
            // CASE 4: TCP/UDP parsing
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

            let (local_port, remote_host, remote_port) = match nums.as_slice() {
                [lp] => ((*lp).parse()?, "127.0.0.1".to_string(), (*lp).parse()?),

                [lp, rp] => ((*lp).parse()?, "127.0.0.1".to_string(), (*rp).parse()?),

                [lp, host, rp] => ((*lp).parse()?, (*host).to_string(), (*rp).parse()?),

                _ => {
                    return Err(TunnelParseError::InvalidSyntax(body.to_string()));
                }
            };

            match protocol {
                Some("tcp") => out.push(TunnelTarget::Tcp(TcpTarget {
                    local_port,
                    remote_host,
                    remote_port,
                })),
                Some("udp") => out.push(TunnelTarget::Udp(UdpTarget {
                    local_port,
                    remote_host,
                    remote_port,
                })),
                None => out.push(TunnelTarget::Tcp(TcpTarget {
                    local_port,
                    remote_host,
                    remote_port,
                })),
                _ => unreachable!(),
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
