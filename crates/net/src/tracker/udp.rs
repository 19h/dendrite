use std::{io, net::SocketAddr, time::Duration};

use tokio::{net::UdpSocket, time::timeout};
use url::Url;

use super::{
    AnnounceEvent, TrackerAnnounce, TrackerCodecError, TrackerRequest, decode_announce_response,
    decode_announce_response_ipv6, decode_connect_response,
};

const PROTOCOL_ID: u64 = 0x0417_2710_1980;
const CONNECT_BYTES: usize = 16;
const ANNOUNCE_BYTES: usize = 98;
const RETRIES: u32 = 3;
const MAX_DATAGRAM_BYTES: usize = 65_507;

#[derive(Clone, Copy, Debug)]
pub struct UdpTrackerClient {
    response_limit: usize,
    attempt_timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum UdpTrackerError {
    #[error("tracker URL must use UDP")]
    Scheme,
    #[error("UDP tracker URL has no host")]
    Host,
    #[error("UDP tracker URL has no port")]
    Port,
    #[error("UDP tracker address resolution failed: {0}")]
    Resolve(#[source] io::Error),
    #[error("UDP tracker hostname resolved to no addresses")]
    NoAddress,
    #[error("UDP tracker I/O failed: {0}")]
    Io(#[source] io::Error),
    #[error("UDP tracker timed out after {0} attempts")]
    Timeout(u32),
    #[error("UDP tracker response limit must be at least 20 bytes")]
    ResponseLimit,
    #[error(transparent)]
    Codec(#[from] TrackerCodecError),
}

impl UdpTrackerClient {
    pub fn new(response_limit: usize) -> Result<Self, UdpTrackerError> {
        Self::with_timeout(response_limit, Duration::from_secs(5))
    }

    pub fn with_timeout(
        response_limit: usize,
        attempt_timeout: Duration,
    ) -> Result<Self, UdpTrackerError> {
        if response_limit < 20 {
            return Err(UdpTrackerError::ResponseLimit);
        }
        Ok(Self {
            response_limit: response_limit.min(MAX_DATAGRAM_BYTES),
            attempt_timeout,
        })
    }

    pub async fn announce(
        &self,
        tracker: &Url,
        request: TrackerRequest,
    ) -> Result<TrackerAnnounce, UdpTrackerError> {
        if tracker.scheme() != "udp" {
            return Err(UdpTrackerError::Scheme);
        }
        let host = tracker.host_str().ok_or(UdpTrackerError::Host)?;
        let port = tracker.port().ok_or(UdpTrackerError::Port)?;
        let addresses: Vec<_> = tokio::net::lookup_host((host, port))
            .await
            .map_err(UdpTrackerError::Resolve)?
            .collect();
        let address = addresses
            .into_iter()
            .next()
            .ok_or(UdpTrackerError::NoAddress)?;
        self.announce_to(address, request).await
    }

    async fn announce_to(
        &self,
        address: SocketAddr,
        request: TrackerRequest,
    ) -> Result<TrackerAnnounce, UdpTrackerError> {
        let bind = if address.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let socket = UdpSocket::bind(bind).await.map_err(UdpTrackerError::Io)?;
        socket.connect(address).await.map_err(UdpTrackerError::Io)?;

        let connect_transaction: u32 = rand::random();
        let connect = encode_connect(connect_transaction);
        let response = self.exchange(&socket, &connect).await?;
        let connection_id = decode_connect_response(&response, connect_transaction)?.connection_id;

        let announce_transaction: u32 = rand::random();
        let announce = encode_announce(connection_id, announce_transaction, request);
        let response = self.exchange(&socket, &announce).await?;
        let decoded = if address.is_ipv4() {
            decode_announce_response(&response, announce_transaction)?
        } else {
            decode_announce_response_ipv6(&response, announce_transaction)?
        };
        Ok(TrackerAnnounce {
            interval: Duration::from_secs(u64::from(decoded.interval_seconds)),
            minimum_interval: None,
            complete: Some(decoded.seeders),
            incomplete: Some(decoded.leechers),
            warning: None,
            peers: decoded.peers,
        })
    }

    async fn exchange(
        &self,
        socket: &UdpSocket,
        request: &[u8],
    ) -> Result<Vec<u8>, UdpTrackerError> {
        let mut response = vec![0_u8; self.response_limit];
        for attempt in 0..RETRIES {
            socket.send(request).await.map_err(UdpTrackerError::Io)?;
            let wait = self.attempt_timeout.saturating_mul(1_u32 << attempt);
            match timeout(wait, socket.recv(&mut response)).await {
                Ok(Ok(received)) => {
                    response.truncate(received);
                    return Ok(response);
                }
                Ok(Err(error)) => return Err(UdpTrackerError::Io(error)),
                Err(_) => {}
            }
        }
        Err(UdpTrackerError::Timeout(RETRIES))
    }
}

fn encode_connect(transaction: u32) -> [u8; CONNECT_BYTES] {
    let mut output = [0_u8; CONNECT_BYTES];
    output[..8].copy_from_slice(&PROTOCOL_ID.to_be_bytes());
    output[12..].copy_from_slice(&transaction.to_be_bytes());
    output
}

fn encode_announce(
    connection_id: u64,
    transaction: u32,
    request: TrackerRequest,
) -> [u8; ANNOUNCE_BYTES] {
    let mut output = [0_u8; ANNOUNCE_BYTES];
    output[..8].copy_from_slice(&connection_id.to_be_bytes());
    output[8..12].copy_from_slice(&1_u32.to_be_bytes());
    output[12..16].copy_from_slice(&transaction.to_be_bytes());
    output[16..36].copy_from_slice(request.info_hash.as_bytes());
    output[36..56].copy_from_slice(request.peer_id.as_bytes());
    output[56..64].copy_from_slice(&request.downloaded.to_be_bytes());
    output[64..72].copy_from_slice(&request.left.to_be_bytes());
    output[72..80].copy_from_slice(&request.uploaded.to_be_bytes());
    let event = match request.event {
        AnnounceEvent::None => 0_u32,
        AnnounceEvent::Completed => 1,
        AnnounceEvent::Started => 2,
        AnnounceEvent::Stopped => 3,
    };
    output[80..84].copy_from_slice(&event.to_be_bytes());
    output[88..92].copy_from_slice(&rand::random::<u32>().to_be_bytes());
    output[92..96].copy_from_slice(&i32::from(request.numwant).to_be_bytes());
    output[96..].copy_from_slice(&request.port.to_be_bytes());
    output
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use dendrite_core::Sha1Hash;

    use crate::peer::PeerId;

    use super::*;

    #[tokio::test]
    async fn announces_against_a_local_udp_tracker() -> Result<(), Box<dyn std::error::Error>> {
        let tracker = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = tracker.local_addr()?;
        let server = tokio::spawn(async move {
            let mut packet = [0_u8; 256];
            let (received, client) = tracker.recv_from(&mut packet).await?;
            if received != CONNECT_BYTES || packet[..8] != PROTOCOL_ID.to_be_bytes() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad connect"));
            }
            let transaction = &packet[12..16];
            let mut response = [0_u8; 16];
            response[4..8].copy_from_slice(transaction);
            response[8..].copy_from_slice(&7_u64.to_be_bytes());
            tracker.send_to(&response, client).await?;

            let (received, client) = tracker.recv_from(&mut packet).await?;
            if received != ANNOUNCE_BYTES || packet[8..12] != 1_u32.to_be_bytes() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad announce"));
            }
            let transaction = &packet[12..16];
            let mut response = Vec::from([0_u8; 20]);
            response[..4].copy_from_slice(&1_u32.to_be_bytes());
            response[4..8].copy_from_slice(transaction);
            response[8..12].copy_from_slice(&60_u32.to_be_bytes());
            response.extend_from_slice(&[127, 0, 0, 1]);
            response.extend_from_slice(&6881_u16.to_be_bytes());
            tracker.send_to(&response, client).await?;
            Ok::<_, io::Error>(())
        });

        let client = UdpTrackerClient::with_timeout(1024, Duration::from_millis(100))?;
        let response = client
            .announce(
                &Url::parse(&format!("udp://{address}/announce"))?,
                TrackerRequest {
                    info_hash: Sha1Hash::from_bytes([1; 20]),
                    peer_id: PeerId::from_bytes([2; 20]),
                    port: 6881,
                    uploaded: 0,
                    downloaded: 0,
                    left: 1,
                    event: AnnounceEvent::Started,
                    numwant: 50,
                },
            )
            .await?;
        assert_eq!(response.peers, [SocketAddr::from(([127, 0, 0, 1], 6881))]);
        server.await??;
        Ok(())
    }
}
