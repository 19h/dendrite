//! BEP 14 local service discovery over bounded UDP multicast packets.

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr as _,
};

use dendrite_core::Sha1Hash;
use socket2::{Domain, Protocol, Socket, Type};
use thiserror::Error;
use tokio::net::UdpSocket;

pub const LSD_PORT: u16 = 6771;
pub const LSD_PACKET_LIMIT: usize = 1_400;
const LSD_GROUP: Ipv4Addr = Ipv4Addr::new(239, 192, 152, 143);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LsdAnnounce {
    pub port: u16,
    pub info_hashes: Vec<Sha1Hash>,
    pub cookie: Option<String>,
}

#[derive(Debug, Error)]
pub enum LsdError {
    #[error("local discovery I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("local discovery packet is malformed")]
    Malformed,
    #[error("local discovery packet exceeds {LSD_PACKET_LIMIT} bytes")]
    PacketLimit,
    #[error("local discovery packet has no valid info hash")]
    NoInfoHash,
}

#[derive(Debug)]
pub struct LsdService {
    socket: UdpSocket,
    peer_port: u16,
    cookie: String,
}

impl LsdService {
    pub fn bind(peer_port: u16, cookie: String) -> Result<Self, LsdError> {
        if peer_port == 0 || cookie.len() > 64 || cookie.contains(['\r', '\n']) {
            return Err(LsdError::Malformed);
        }
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_address(true)?;
        socket.set_nonblocking(true)?;
        socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, LSD_PORT).into())?;
        socket.join_multicast_v4(&LSD_GROUP, &Ipv4Addr::UNSPECIFIED)?;
        socket.set_multicast_loop_v4(true)?;
        socket.set_multicast_ttl_v4(1)?;
        let socket = UdpSocket::from_std(socket.into())?;
        Ok(Self {
            socket,
            peer_port,
            cookie,
        })
    }

    pub async fn announce(&self, hashes: &[Sha1Hash]) -> Result<(), LsdError> {
        let packet = encode_announce(self.peer_port, hashes, Some(&self.cookie))?;
        self.socket
            .send_to(&packet, SocketAddrV4::new(LSD_GROUP, LSD_PORT))
            .await?;
        Ok(())
    }

    pub async fn receive(&self) -> Result<(LsdAnnounce, SocketAddr), LsdError> {
        let mut packet = [0_u8; LSD_PACKET_LIMIT + 1];
        let (length, source) = self.socket.recv_from(&mut packet).await?;
        if length > LSD_PACKET_LIMIT {
            return Err(LsdError::PacketLimit);
        }
        let announce = decode_announce(&packet[..length])?;
        if announce.cookie.as_deref() == Some(&self.cookie) {
            return Err(LsdError::Malformed);
        }
        Ok((announce, source))
    }
}

pub fn encode_announce(
    port: u16,
    hashes: &[Sha1Hash],
    cookie: Option<&str>,
) -> Result<Vec<u8>, LsdError> {
    if port == 0
        || hashes.is_empty()
        || cookie.is_some_and(|value| value.len() > 64 || value.contains(['\r', '\n']))
    {
        return Err(LsdError::Malformed);
    }
    let mut packet =
        format!("BT-SEARCH * HTTP/1.1\r\nHost: {LSD_GROUP}:{LSD_PORT}\r\nPort: {port}\r\n");
    for hash in hashes {
        packet.push_str("Infohash: ");
        packet.push_str(&hash.to_string());
        packet.push_str("\r\n");
    }
    if let Some(cookie) = cookie {
        packet.push_str("cookie: ");
        packet.push_str(cookie);
        packet.push_str("\r\n");
    }
    packet.push_str("\r\n\r\n");
    if packet.len() > LSD_PACKET_LIMIT {
        return Err(LsdError::PacketLimit);
    }
    Ok(packet.into_bytes())
}

pub fn decode_announce(packet: &[u8]) -> Result<LsdAnnounce, LsdError> {
    if packet.len() > LSD_PACKET_LIMIT {
        return Err(LsdError::PacketLimit);
    }
    let text = std::str::from_utf8(packet).map_err(|_| LsdError::Malformed)?;
    let mut lines = text.split("\r\n");
    if lines.next() != Some("BT-SEARCH * HTTP/1.1") {
        return Err(LsdError::Malformed);
    }
    let mut port = None;
    let mut hashes = Vec::new();
    let mut cookie = None;
    for line in lines {
        if line.is_empty() {
            break;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(LsdError::Malformed);
        };
        let value = value.trim();
        if name.eq_ignore_ascii_case("port") {
            let parsed = value.parse::<u16>().map_err(|_| LsdError::Malformed)?;
            if parsed == 0 || port.replace(parsed).is_some() {
                return Err(LsdError::Malformed);
            }
        } else if name.eq_ignore_ascii_case("infohash") {
            let hash = Sha1Hash::from_str(value).map_err(|_| LsdError::Malformed)?;
            if !hashes.contains(&hash) {
                hashes.push(hash);
            }
        } else if name.eq_ignore_ascii_case("cookie")
            && (value.len() > 64 || cookie.replace(value.to_owned()).is_some())
        {
            return Err(LsdError::Malformed);
        }
    }
    if hashes.is_empty() {
        return Err(LsdError::NoInfoHash);
    }
    Ok(LsdAnnounce {
        port: port.ok_or(LsdError::Malformed)?,
        info_hashes: hashes,
        cookie,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn announce_round_trips_multiple_hashes_and_cookie() -> Result<(), LsdError> {
        let hashes = [Sha1Hash::from_bytes([1; 20]), Sha1Hash::from_bytes([2; 20])];
        let encoded = encode_announce(6881, &hashes, Some("instance-7"))?;
        assert!(encoded.len() <= LSD_PACKET_LIMIT);
        assert_eq!(
            decode_announce(&encoded)?,
            LsdAnnounce {
                port: 6881,
                info_hashes: hashes.to_vec(),
                cookie: Some("instance-7".to_owned()),
            }
        );
        Ok(())
    }

    #[test]
    fn malformed_and_oversized_announces_are_rejected() {
        assert!(decode_announce(b"GET / HTTP/1.1\r\n\r\n").is_err());
        assert!(decode_announce(&vec![b'x'; LSD_PACKET_LIMIT + 1]).is_err());
        assert!(encode_announce(6881, &[], None).is_err());
    }

    #[tokio::test]
    async fn multicast_services_discover_each_other() -> Result<(), Box<dyn std::error::Error>> {
        let sender = LsdService::bind(6881, "sender-cookie".to_owned())?;
        let receiver = LsdService::bind(6882, "receiver-cookie".to_owned())?;
        let hash = Sha1Hash::from_bytes([9; 20]);
        sender.announce(&[hash]).await?;
        let (announce, source) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Ok(received) = receiver.receive().await {
                    return received;
                }
            }
        })
        .await?;
        assert_eq!(announce.info_hashes, [hash]);
        assert_eq!(announce.port, 6881);
        assert!(source.ip().is_ipv4());
        Ok(())
    }
}
