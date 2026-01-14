use bytes::{BufMut, BytesMut};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

/// Layout: [family: u8][port: u16][ip bytes]
/// family = 4 => IPv4 (4 bytes), 6 => IPv6 (16 bytes)
pub fn encode_socket_addr(buf: &mut BytesMut, addr: SocketAddr) {
    match addr {
        SocketAddr::V4(v4) => {
            buf.put_u8(4);
            buf.put_u16(v4.port());
            buf.extend_from_slice(&v4.ip().octets());
        }
        SocketAddr::V6(v6) => {
            buf.put_u8(6);
            buf.put_u16(v6.port());
            buf.extend_from_slice(&v6.ip().octets());
        }
    }
}

/// Returns (addr, header_len) on success
pub fn decode_socket_addr(buf: &[u8]) -> Option<(SocketAddr, usize)> {
    if buf.len() < 1 + 2 {
        return None;
    }

    let fam = buf[0];
    let port = u16::from_be_bytes([buf[1], buf[2]]);

    match fam {
        4 => {
            if buf.len() < 1 + 2 + 4 {
                return None;
            }
            let ip = Ipv4Addr::new(buf[3], buf[4], buf[5], buf[6]);
            let addr = SocketAddr::V4(SocketAddrV4::new(ip, port));
            Some((addr, 1 + 2 + 4))
        }
        6 => {
            if buf.len() < 1 + 2 + 16 {
                return None;
            }
            let ip = Ipv6Addr::new(
                u16::from_be_bytes([buf[3], buf[4]]),
                u16::from_be_bytes([buf[5], buf[6]]),
                u16::from_be_bytes([buf[7], buf[8]]),
                u16::from_be_bytes([buf[9], buf[10]]),
                u16::from_be_bytes([buf[11], buf[12]]),
                u16::from_be_bytes([buf[13], buf[14]]),
                u16::from_be_bytes([buf[15], buf[16]]),
                u16::from_be_bytes([buf[17], buf[18]]),
            );
            let addr = SocketAddr::V6(SocketAddrV6::new(ip, port, 0, 0));
            Some((addr, 1 + 2 + 16))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ipv4_roundtrip() {
        let addr: SocketAddr = "192.168.1.100:8080".parse().unwrap();
        let mut buf = BytesMut::new();
        encode_socket_addr(&mut buf, addr);

        let (decoded, len) = decode_socket_addr(&buf).unwrap();
        assert_eq!(decoded, addr);
        assert_eq!(len, 7); // 1 (family) + 2 (port) + 4 (IPv4)
    }

    #[test]
    fn test_ipv4_localhost() {
        let addr: SocketAddr = "127.0.0.1:443".parse().unwrap();
        let mut buf = BytesMut::new();
        encode_socket_addr(&mut buf, addr);

        let (decoded, _) = decode_socket_addr(&buf).unwrap();
        assert_eq!(decoded, addr);
    }

    #[test]
    fn test_ipv6_roundtrip() {
        let addr: SocketAddr = "[2001:db8::1]:9000".parse().unwrap();
        let mut buf = BytesMut::new();
        encode_socket_addr(&mut buf, addr);

        let (decoded, len) = decode_socket_addr(&buf).unwrap();
        assert_eq!(decoded, addr);
        assert_eq!(len, 19); // 1 (family) + 2 (port) + 16 (IPv6)
    }

    #[test]
    fn test_ipv6_localhost() {
        let addr: SocketAddr = "[::1]:80".parse().unwrap();
        let mut buf = BytesMut::new();
        encode_socket_addr(&mut buf, addr);

        let (decoded, _) = decode_socket_addr(&buf).unwrap();
        assert_eq!(decoded, addr);
    }

    #[test]
    fn test_decode_truncated_buffer() {
        // Too short for header
        assert!(decode_socket_addr(&[]).is_none());
        assert!(decode_socket_addr(&[4]).is_none());
        assert!(decode_socket_addr(&[4, 0]).is_none());

        // IPv4 header but missing IP bytes
        assert!(decode_socket_addr(&[4, 0, 80, 127]).is_none());

        // IPv6 header but missing IP bytes
        assert!(decode_socket_addr(&[6, 0, 80, 0, 0, 0, 0]).is_none());
    }

    #[test]
    fn test_decode_invalid_family() {
        // Family byte is neither 4 nor 6
        let buf = [0, 0, 80, 127, 0, 0, 1];
        assert!(decode_socket_addr(&buf).is_none());

        let buf = [5, 0, 80, 127, 0, 0, 1];
        assert!(decode_socket_addr(&buf).is_none());
    }
}
