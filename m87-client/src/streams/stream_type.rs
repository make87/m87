use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Protocols {
    Tcp,
    Udp,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Additions {
    // inticator for multicast forwarding
    MCAST,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum StreamType {
    Terminal {
        token: String,
    },
    Exec {
        token: String,
    },
    Logs {
        token: String,
    },
    Port {
        token: String,
        port: u16,
        protocol: Protocols,
        host: Option<String>,
        addition: Option<Additions>,
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
    pub fn get_token(&self) -> &str {
        match self {
            StreamType::Terminal { token } => token,
            StreamType::Exec { token, .. } => token,
            StreamType::Logs { token, .. } => token,
            StreamType::Port { token, .. } => token,
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
