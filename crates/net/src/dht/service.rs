use std::{
    collections::{HashSet, VecDeque},
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};

use dendrite_core::Sha1Hash;
use thiserror::Error;
use tokio::{
    net::UdpSocket,
    sync::Mutex,
    time::{Instant, timeout_at},
};

use super::{DhtCodecError, DhtMessage, NodeId, decode_message, encode_get_peers_query};

const MAX_PACKETS_PER_QUERY: usize = 256;
const MAX_DHT_PEERS: usize = 1_024;

#[derive(Clone, Debug)]
pub struct DhtClient {
    node_id: NodeId,
    query_limit: usize,
    packet_limit: usize,
    query_timeout: Duration,
    socket: Option<std::sync::Arc<Mutex<UdpSocket>>>,
}

#[derive(Debug, Error)]
pub enum DhtServiceError {
    #[error("DHT bootstrap list is empty")]
    NoBootstrap,
    #[error("DHT query limit and packet limit must be nonzero")]
    InvalidLimit,
    #[error("DHT I/O failed: {0}")]
    Io(#[source] io::Error),
    #[error(transparent)]
    Codec(#[from] DhtCodecError),
    #[error("DHT lookup exhausted its bounded routing frontier without peers")]
    Exhausted,
}

impl DhtClient {
    pub fn new(
        query_limit: usize,
        packet_limit: usize,
        query_timeout: Duration,
    ) -> Result<Self, DhtServiceError> {
        if query_limit == 0 || packet_limit == 0 {
            return Err(DhtServiceError::InvalidLimit);
        }
        Ok(Self {
            node_id: NodeId::from_bytes(rand::random()),
            query_limit,
            packet_limit: packet_limit.min(65_507),
            query_timeout,
            socket: None,
        })
    }

    /// Bind a reusable DHT socket. Lookups are serialized so transaction
    /// responses cannot be consumed by a different concurrent lookup.
    pub async fn bind(
        address: SocketAddr,
        query_limit: usize,
        packet_limit: usize,
        query_timeout: Duration,
    ) -> Result<Self, DhtServiceError> {
        let mut client = Self::new(query_limit, packet_limit, query_timeout)?;
        let socket = UdpSocket::bind(address)
            .await
            .map_err(DhtServiceError::Io)?;
        client.socket = Some(std::sync::Arc::new(Mutex::new(socket)));
        Ok(client)
    }

    pub async fn get_peers(
        &self,
        info_hash: Sha1Hash,
        bootstrap: &[SocketAddr],
    ) -> Result<Vec<SocketAddr>, DhtServiceError> {
        if let Some(socket) = &self.socket {
            let socket = socket.lock().await;
            return self.lookup(&socket, info_hash, bootstrap).await;
        }
        let first = bootstrap.first().ok_or(DhtServiceError::NoBootstrap)?;
        let bind = match first.ip() {
            IpAddr::V4(_) => SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0),
            IpAddr::V6(_) => SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 0),
        };
        let socket = UdpSocket::bind(bind).await.map_err(DhtServiceError::Io)?;
        self.lookup(&socket, info_hash, bootstrap).await
    }

    async fn lookup(
        &self,
        socket: &UdpSocket,
        info_hash: Sha1Hash,
        bootstrap: &[SocketAddr],
    ) -> Result<Vec<SocketAddr>, DhtServiceError> {
        if bootstrap.is_empty() {
            return Err(DhtServiceError::NoBootstrap);
        }
        let ipv4 = socket.local_addr().map_err(DhtServiceError::Io)?.is_ipv4();
        let mut frontier: VecDeque<_> = bootstrap
            .iter()
            .copied()
            .filter(|address| address.is_ipv4() == ipv4)
            .collect();
        let mut visited = HashSet::new();
        let mut queries = 0_usize;
        while let Some(address) = frontier.pop_front() {
            if queries >= self.query_limit || !visited.insert(address) {
                continue;
            }
            queries += 1;
            let transaction: [u8; 4] = rand::random();
            let query = encode_get_peers_query(&transaction, self.node_id, info_hash);
            socket
                .send_to(&query, address)
                .await
                .map_err(DhtServiceError::Io)?;
            let Some(response) = self.receive_response(socket, address, &transaction).await? else {
                continue;
            };
            let peers = bounded_public_peers(response.peers);
            if !peers.is_empty() {
                return Ok(peers);
            }
            for node in response.nodes {
                if node.address.is_ipv4() == ipv4 && !visited.contains(&node.address) {
                    frontier.push_back(node.address);
                }
            }
        }
        Err(DhtServiceError::Exhausted)
    }

    async fn receive_response(
        &self,
        socket: &UdpSocket,
        expected_source: SocketAddr,
        transaction: &[u8],
    ) -> Result<Option<super::DhtResponse>, DhtServiceError> {
        let mut packet = vec![0_u8; self.packet_limit];
        let deadline = Instant::now() + self.query_timeout;
        for _ in 0..MAX_PACKETS_PER_QUERY {
            let Ok(result) = timeout_at(deadline, socket.recv_from(&mut packet)).await else {
                return Ok(None);
            };
            let (length, source) = result.map_err(DhtServiceError::Io)?;
            if source != expected_source {
                continue;
            }
            let Ok(message) = decode_message(&packet[..length]) else {
                continue;
            };
            if let DhtMessage::Response {
                transaction: response_transaction,
                response,
            } = message
                && response_transaction.as_ref() == transaction
            {
                return Ok(Some(response));
            }
        }
        Ok(None)
    }
}

fn bounded_public_peers(peers: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let mut unique = HashSet::with_capacity(peers.len().min(MAX_DHT_PEERS));
    peers
        .into_iter()
        .filter(|address| public_peer_address(*address))
        .filter(|address| unique.insert(*address))
        .take(MAX_DHT_PEERS)
        .collect()
}

fn public_peer_address(address: SocketAddr) -> bool {
    if address.port() == 0 {
        return false;
    }
    match address.ip() {
        IpAddr::V4(ip) => {
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_broadcast()
                && !ip.is_unspecified()
                && !ip.is_multicast()
        }
        IpAddr::V6(ip) => {
            !ip.is_loopback()
                && !ip.is_unique_local()
                && !ip.is_unicast_link_local()
                && !ip.is_unspecified()
                && !ip.is_multicast()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn discovers_peers_from_a_local_dht_node() -> Result<(), Box<dyn std::error::Error>> {
        let node = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let node_address = node.local_addr()?;
        let server = tokio::spawn(async move {
            let mut packet = [0_u8; 1024];
            let (length, source) = node.recv_from(&mut packet).await?;
            let DhtMessage::Query { transaction, .. } = decode_message(&packet[..length])
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
            else {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "not a query"));
            };
            let mut response = b"d1:rd2:id20:012345678901234567896:valuesl6:".to_vec();
            response.extend_from_slice(&[8, 8, 8, 8]);
            response.extend_from_slice(&6881_u16.to_be_bytes());
            response.extend_from_slice(b"ee1:t");
            response.extend_from_slice(transaction.len().to_string().as_bytes());
            response.push(b':');
            response.extend_from_slice(&transaction);
            response.extend_from_slice(b"1:y1:re");
            node.send_to(&response, source).await?;
            Ok::<_, io::Error>(())
        });

        let client = DhtClient::bind(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            8,
            4096,
            Duration::from_millis(100),
        )
        .await?;
        let peers = client
            .get_peers(Sha1Hash::from_bytes([9; 20]), &[node_address])
            .await?;
        assert_eq!(peers, [SocketAddr::from(([8, 8, 8, 8], 6881))]);
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn mismatched_address_family_is_skipped_without_hanging()
    -> Result<(), Box<dyn std::error::Error>> {
        let client = DhtClient::bind(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            8,
            4096,
            Duration::from_millis(10),
        )
        .await?;
        let result = client
            .get_peers(
                Sha1Hash::from_bytes([4; 20]),
                &[SocketAddr::from((Ipv6Addr::LOCALHOST, 6881))],
            )
            .await;
        assert!(matches!(result, Err(DhtServiceError::Exhausted)));
        Ok(())
    }

    #[tokio::test]
    async fn unbound_client_rebinds_after_bootstrap_or_interface_change()
    -> Result<(), Box<dyn std::error::Error>> {
        let client = DhtClient::new(8, 4096, Duration::from_millis(100))?;
        for last_octet in [1_u8, 2] {
            let node = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let node_address = node.local_addr()?;
            let server = tokio::spawn(async move {
                let mut packet = [0_u8; 1024];
                let (length, source) = node.recv_from(&mut packet).await?;
                let DhtMessage::Query { transaction, .. } = decode_message(&packet[..length])
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
                else {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "not a query"));
                };
                let mut response = b"d1:rd2:id20:012345678901234567896:valuesl6:".to_vec();
                response.extend_from_slice(&[8, 8, 8, last_octet]);
                response.extend_from_slice(&6881_u16.to_be_bytes());
                response.extend_from_slice(b"ee1:t");
                response.extend_from_slice(transaction.len().to_string().as_bytes());
                response.push(b':');
                response.extend_from_slice(&transaction);
                response.extend_from_slice(b"1:y1:re");
                node.send_to(&response, source).await?;
                Ok::<_, io::Error>(())
            });
            let peers = client
                .get_peers(Sha1Hash::from_bytes([last_octet; 20]), &[node_address])
                .await?;
            assert_eq!(peers, [SocketAddr::from(([8, 8, 8, last_octet], 6881))]);
            server.await??;
        }
        Ok(())
    }

    #[test]
    fn bounds_are_mandatory() {
        assert!(matches!(
            DhtClient::new(0, 1024, Duration::from_secs(1)),
            Err(DhtServiceError::InvalidLimit)
        ));
    }

    #[test]
    fn peer_floods_are_deduplicated_bounded_and_private_addresses_are_dropped() {
        let mut peers = vec![
            SocketAddr::from(([10, 0, 0, 1], 6881)),
            SocketAddr::from(([127, 0, 0, 1], 6881)),
            SocketAddr::from(([8, 8, 8, 8], 0)),
        ];
        for index in 0..MAX_DHT_PEERS + 32 {
            let address = SocketAddr::from((
                [
                    11,
                    u8::try_from(index >> 8).unwrap_or(u8::MAX),
                    u8::try_from(index & 0xff).unwrap_or(u8::MAX),
                    1,
                ],
                6881,
            ));
            peers.push(address);
            peers.push(address);
        }
        let bounded = bounded_public_peers(peers);
        assert_eq!(bounded.len(), MAX_DHT_PEERS);
        assert!(bounded.iter().all(|address| public_peer_address(*address)));
        assert_eq!(
            bounded.iter().copied().collect::<HashSet<_>>().len(),
            bounded.len()
        );
    }
}
