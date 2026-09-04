use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use dendrite_core::Sha1Hash;
use thiserror::Error;
use tokio::{
    net::UdpSocket,
    sync::Mutex,
    time::{Instant, timeout_at},
};

use super::{
    DhtCodecError, DhtMessage, NodeId, decode_message, encode_announce_peer_query,
    encode_get_peers_query,
};

/// Queries kept in flight during a lookup.
const LOOKUP_CONCURRENCY: usize = 8;
/// A lookup stops once this many of the closest responders have been heard
/// from and no closer node remains.
const CLOSEST_NODES: usize = 16;
const MAX_DHT_PEERS: usize = 2_048;
/// Responsive nodes remembered between lookups so later lookups skip the
/// bootstrap walk.
const NODE_CACHE_LIMIT: usize = 512;

#[derive(Clone, Debug)]
pub struct DhtClient {
    node_id: NodeId,
    query_limit: usize,
    packet_limit: usize,
    query_timeout: Duration,
    socket: Option<Arc<Mutex<UdpSocket>>>,
    known_nodes: Arc<std::sync::Mutex<VecDeque<SocketAddr>>>,
}

/// Result of an iterative lookup: the peers found and the closest nodes that
/// handed out announce tokens.
#[derive(Clone, Debug, Default)]
pub struct DhtLookup {
    pub peers: Vec<SocketAddr>,
    pub announce_targets: Vec<(SocketAddr, Bytes)>,
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
            known_nodes: Arc::new(std::sync::Mutex::new(VecDeque::new())),
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
        client.socket = Some(Arc::new(Mutex::new(socket)));
        Ok(client)
    }

    pub async fn get_peers(
        &self,
        info_hash: Sha1Hash,
        bootstrap: &[SocketAddr],
    ) -> Result<Vec<SocketAddr>, DhtServiceError> {
        let lookup = self.lookup_with_socket(info_hash, bootstrap, None).await?;
        if lookup.peers.is_empty() {
            return Err(DhtServiceError::Exhausted);
        }
        Ok(lookup.peers)
    }

    /// Looks up peers and announces this node's `port` for `info_hash` to the
    /// closest nodes that supplied tokens, so other clients can find us.
    pub async fn get_peers_and_announce(
        &self,
        info_hash: Sha1Hash,
        bootstrap: &[SocketAddr],
        port: u16,
    ) -> Result<Vec<SocketAddr>, DhtServiceError> {
        let lookup = self
            .lookup_with_socket(info_hash, bootstrap, Some(port))
            .await?;
        if lookup.peers.is_empty() {
            return Err(DhtServiceError::Exhausted);
        }
        Ok(lookup.peers)
    }

    async fn lookup_with_socket(
        &self,
        info_hash: Sha1Hash,
        bootstrap: &[SocketAddr],
        announce_port: Option<u16>,
    ) -> Result<DhtLookup, DhtServiceError> {
        if let Some(socket) = &self.socket {
            let socket = socket.lock().await;
            return self
                .lookup_with_retry(&socket, info_hash, bootstrap, announce_port)
                .await;
        }
        let first = bootstrap.first().ok_or(DhtServiceError::NoBootstrap)?;
        let bind = match first.ip() {
            IpAddr::V4(_) => SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0),
            IpAddr::V6(_) => SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 0),
        };
        let socket = UdpSocket::bind(bind).await.map_err(DhtServiceError::Io)?;
        self.lookup_with_retry(&socket, info_hash, bootstrap, announce_port)
            .await
    }

    /// A cold walk from the bootstrap nodes sometimes gets no answer within
    /// one query timeout (first-contact rate limiting, slow bootstrap hosts);
    /// one more attempt with a doubled timeout recovers that case.
    async fn lookup_with_retry(
        &self,
        socket: &UdpSocket,
        info_hash: Sha1Hash,
        bootstrap: &[SocketAddr],
        announce_port: Option<u16>,
    ) -> Result<DhtLookup, DhtServiceError> {
        let first = self
            .lookup(
                socket,
                info_hash,
                bootstrap,
                announce_port,
                self.query_timeout,
            )
            .await?;
        if !first.peers.is_empty() || self.known_node_count() > 0 {
            return Ok(first);
        }
        self.lookup(
            socket,
            info_hash,
            bootstrap,
            announce_port,
            self.query_timeout.saturating_mul(2),
        )
        .await
    }

    fn known_node_count(&self) -> usize {
        self.known_nodes.lock().map_or(0, |known| known.len())
    }

    /// Iterative Kademlia-style lookup: keeps `LOOKUP_CONCURRENCY` queries in
    /// flight, always dispatching to the closest unqueried node, collecting
    /// peers from every responder, and stopping once the `CLOSEST_NODES`
    /// nearest responders have answered and nothing closer remains.
    #[allow(clippy::too_many_lines)] // The dispatch/receive loop shares all lookup state.
    async fn lookup(
        &self,
        socket: &UdpSocket,
        info_hash: Sha1Hash,
        bootstrap: &[SocketAddr],
        announce_port: Option<u16>,
        query_timeout: Duration,
    ) -> Result<DhtLookup, DhtServiceError> {
        if bootstrap.is_empty() {
            return Err(DhtServiceError::NoBootstrap);
        }
        let ipv4 = socket.local_addr().map_err(DhtServiceError::Io)?.is_ipv4();
        let mut seeds: VecDeque<SocketAddr> = VecDeque::new();
        let mut seeded = HashSet::new();
        if let Ok(known) = self.known_nodes.lock() {
            for address in known.iter().rev() {
                if address.is_ipv4() == ipv4 && seeded.insert(*address) {
                    seeds.push_back(*address);
                }
            }
        }
        for address in bootstrap {
            if address.is_ipv4() == ipv4 && seeded.insert(*address) {
                seeds.push_back(*address);
            }
        }
        let mut candidates: BTreeMap<[u8; 20], SocketAddr> = BTreeMap::new();
        let mut queried: HashSet<SocketAddr> = HashSet::new();
        let mut in_flight: HashMap<[u8; 4], (SocketAddr, Instant)> = HashMap::new();
        let mut responders: BTreeMap<[u8; 20], (SocketAddr, Option<Bytes>)> = BTreeMap::new();
        let mut peers: HashSet<SocketAddr> = HashSet::new();
        let mut sent = 0_usize;
        let mut packet = vec![0_u8; self.packet_limit];
        loop {
            while in_flight.len() < LOOKUP_CONCURRENCY && sent < self.query_limit {
                let next = if let Some(address) = seeds.pop_front() {
                    Some(address)
                } else if let Some((distance, address)) = candidates.pop_first() {
                    let farthest_close = responders
                        .iter()
                        .nth(CLOSEST_NODES - 1)
                        .map(|(distance, _)| *distance);
                    if farthest_close.is_some_and(|limit| distance > limit) {
                        // Every remaining candidate is farther than the nodes
                        // already heard from; the lookup has converged.
                        candidates.clear();
                        None
                    } else {
                        Some(address)
                    }
                } else {
                    None
                };
                let Some(address) = next else {
                    break;
                };
                if !queried.insert(address) {
                    continue;
                }
                let transaction: [u8; 4] = rand::random();
                let query = encode_get_peers_query(&transaction, self.node_id, info_hash);
                if socket.send_to(&query, address).await.is_err() {
                    continue;
                }
                in_flight.insert(transaction, (address, Instant::now()));
                sent += 1;
            }
            if in_flight.is_empty() || peers.len() >= MAX_DHT_PEERS {
                break;
            }
            let oldest = in_flight
                .values()
                .map(|(_, sent)| *sent)
                .min()
                .unwrap_or_else(Instant::now);
            let deadline = oldest + query_timeout;
            let Ok(received) = timeout_at(deadline, socket.recv_from(&mut packet)).await else {
                let now = Instant::now();
                in_flight.retain(|_, (_, at)| now.duration_since(*at) < query_timeout);
                continue;
            };
            let (length, source) = received.map_err(DhtServiceError::Io)?;
            let Ok(DhtMessage::Response {
                transaction,
                response,
            }) = decode_message(&packet[..length])
            else {
                continue;
            };
            let Ok(key) = <[u8; 4]>::try_from(transaction.as_ref()) else {
                continue;
            };
            let Some((expected, _)) = in_flight.get(&key).copied() else {
                continue;
            };
            if expected != source {
                continue;
            }
            in_flight.remove(&key);
            responders.insert(distance(response.id, info_hash), (source, response.token));
            while responders.len() > CLOSEST_NODES * 2 {
                responders.pop_last();
            }
            for peer in response.peers {
                if public_peer_address(peer) {
                    peers.insert(peer);
                }
            }
            for node in response.nodes {
                if node.address.is_ipv4() == ipv4 && !queried.contains(&node.address) {
                    candidates
                        .entry(distance(node.id, info_hash))
                        .or_insert(node.address);
                }
            }
        }
        self.remember_nodes(responders.values().map(|(address, _)| *address));
        let announce_targets: Vec<(SocketAddr, Bytes)> = responders
            .values()
            .filter_map(|(address, token)| token.clone().map(|token| (*address, token)))
            .take(CLOSEST_NODES / 2)
            .collect();
        if let Some(port) = announce_port {
            for (address, token) in &announce_targets {
                let transaction: [u8; 4] = rand::random();
                let query =
                    encode_announce_peer_query(&transaction, self.node_id, info_hash, port, token);
                let _result_ignored = socket.send_to(&query, *address).await;
            }
        }
        let mut peers: Vec<SocketAddr> = peers.into_iter().take(MAX_DHT_PEERS).collect();
        peers.sort_unstable();
        Ok(DhtLookup {
            peers,
            announce_targets,
        })
    }

    fn remember_nodes(&self, nodes: impl Iterator<Item = SocketAddr>) {
        let Ok(mut known) = self.known_nodes.lock() else {
            return;
        };
        for address in nodes {
            if let Some(position) = known.iter().position(|entry| *entry == address) {
                known.remove(position);
            }
            known.push_back(address);
            while known.len() > NODE_CACHE_LIMIT {
                known.pop_front();
            }
        }
    }
}

fn distance(node: NodeId, info_hash: Sha1Hash) -> [u8; 20] {
    let mut output = [0_u8; 20];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = node.as_bytes()[index] ^ info_hash.as_bytes()[index];
    }
    output
}

#[cfg(test)]
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

    #[tokio::test]
    async fn lookup_walks_closer_nodes_collects_all_peers_and_announces()
    -> Result<(), Box<dyn std::error::Error>> {
        let info_hash = Sha1Hash::from_bytes([9; 20]);
        let far = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let near = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let far_address = far.local_addr()?;
        let near_address = near.local_addr()?;
        let respond = |node: UdpSocket, id: [u8; 20], peer: [u8; 4], nodes: Vec<SocketAddr>| async move {
            let mut packet = [0_u8; 1024];
            let (length, source) = node.recv_from(&mut packet).await?;
            let DhtMessage::Query { transaction, .. } = decode_message(&packet[..length])
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
            else {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "not a query"));
            };
            let mut response = b"d1:rd2:id20:".to_vec();
            response.extend_from_slice(&id);
            if !nodes.is_empty() {
                response.extend_from_slice(format!("5:nodes{}:", nodes.len() * 26).as_bytes());
                for (index, address) in nodes.iter().enumerate() {
                    let SocketAddr::V4(v4) = address else {
                        return Err(io::Error::other("v4 only"));
                    };
                    let mut node_id = [0_u8; 20];
                    node_id[0] = 9;
                    node_id[19] = u8::try_from(index).unwrap_or(0);
                    response.extend_from_slice(&node_id);
                    response.extend_from_slice(&v4.ip().octets());
                    response.extend_from_slice(&v4.port().to_be_bytes());
                }
            }
            response.extend_from_slice(b"5:token4:abcd6:valuesl6:");
            response.extend_from_slice(&peer);
            response.extend_from_slice(&6881_u16.to_be_bytes());
            response.extend_from_slice(b"ee1:t");
            response.extend_from_slice(transaction.len().to_string().as_bytes());
            response.push(b':');
            response.extend_from_slice(&transaction);
            response.extend_from_slice(b"1:y1:re");
            node.send_to(&response, source).await?;
            // The announce arrives as a second query carrying our token.
            let (length, _) = node.recv_from(&mut packet).await?;
            let announced = matches!(
                decode_message(&packet[..length]),
                Ok(DhtMessage::Query {
                    query: super::super::DhtQuery::AnnouncePeer { port: 6001, .. },
                    ..
                })
            );
            Ok::<bool, io::Error>(announced)
        };
        let far_task = tokio::spawn(respond(far, [0xff; 20], [8, 8, 8, 8], vec![near_address]));
        let near_task = tokio::spawn(respond(near, [9; 20], [8, 8, 4, 4], Vec::new()));
        let client = DhtClient::bind(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            16,
            4096,
            Duration::from_millis(200),
        )
        .await?;
        let mut peers = client
            .get_peers_and_announce(info_hash, &[far_address], 6001)
            .await?;
        peers.sort_unstable();
        assert_eq!(
            peers,
            [
                SocketAddr::from(([8, 8, 4, 4], 6881)),
                SocketAddr::from(([8, 8, 8, 8], 6881)),
            ]
        );
        assert!(far_task.await??, "far node did not receive an announce");
        assert!(near_task.await??, "near node did not receive an announce");
        assert_eq!(
            client
                .known_nodes
                .lock()
                .map(|known| known.len())
                .unwrap_or(0),
            2,
            "responsive nodes are cached for the next lookup"
        );
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
