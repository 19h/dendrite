//! Supervised torrent actors and their bounded command interface.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU16, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use dendrite_core::{PiecePicker, SelectionMode, Sha1Hash, TorrentId, TorrentPath, TorrentState};
use dendrite_metainfo::{BencodeLimits, BencodeValue, FileEntry, Magnet, Metainfo, decode};
use dendrite_net::{
    dht::DhtClient,
    extension::{
        HolePunchKind, HolePunchMessage, LOCAL_HOLEPUNCH_EXTENSION_ID, LOCAL_METADATA_EXTENSION_ID,
        LOCAL_PEX_EXTENSION_ID, METADATA_BLOCK_BYTES, MetadataMessage, decode_extension_handshake,
        decode_holepunch_message, decode_metadata_message, decode_pex_message,
        encode_extension_handshake, encode_holepunch_message, encode_metadata_data,
        encode_metadata_reject, encode_metadata_request,
    },
    lsd::LsdService,
    peer::{
        BlockRequest, EncryptionPolicy, Handshake, HashRequest, PeerCodecLimits, PeerConnection,
        PeerEvent, PeerId, PeerMessage, PeerSender,
    },
    tracker::{AnnounceEvent, HttpTrackerClient, TrackerRequest, UdpTrackerClient},
    utp::UtpEndpoint,
};
use dendrite_persistence::{StateStoreHandle, StoreError, TorrentRecord};
use dendrite_storage::{StorageError, StorageHandle};
use reqwest::{
    Client, StatusCode,
    header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_RANGE, RANGE},
};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWrite},
    net::TcpListener,
    sync::{Mutex, Semaphore, broadcast, mpsc, oneshot},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, warn};
use url::Url;

const COMMAND_CAPACITY: usize = 256;
const EVENT_CAPACITY: usize = 4096;
const PEER_LIMIT_PER_ANNOUNCE: u16 = 80;
const BLOCK_BYTES: usize = 16 * 1024;
const BLOCK_PIPELINE: usize = 8;
const PEER_COMMAND_CAPACITY: usize = 64;
const ACTIVE_PEER_LIMIT: usize = 32;
const INCOMING_PEER_LIMIT: usize = 256;
const PEER_MESSAGE_TIMEOUT: Duration = Duration::from_secs(30);
const DHT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);
const SWARM_RETRY_MIN: Duration = Duration::from_secs(1);
const SWARM_RETRY_MAX: Duration = Duration::from_secs(30);
const ACCEPT_ERROR_BACKOFF_MIN: Duration = Duration::from_millis(10);
const ACCEPT_ERROR_BACKOFF_MAX: Duration = Duration::from_secs(1);

struct PeerWorkerHandle {
    commands: mpsc::Sender<PeerWorkerCommand>,
    bitfield: Option<Vec<u8>>,
    idle: bool,
}

struct PeerWorkerContext {
    worker: usize,
    address: SocketAddr,
    info_hash: Sha1Hash,
    piece_count: usize,
    services: Services,
    events: mpsc::Sender<PeerWorkerEvent>,
    cancellation: CancellationToken,
    allow_pex: bool,
    force_utp: bool,
    torrent_id: TorrentId,
}

struct SwarmState {
    workers: HashMap<usize, PeerWorkerHandle>,
    assignments: HashMap<usize, (usize, CancellationToken)>,
    picker: PiecePicker,
    connecting: usize,
    last_error: Option<String>,
    addresses: HashSet<SocketAddr>,
    next_worker: usize,
    event_sender: mpsc::Sender<PeerWorkerEvent>,
    info_hash: Sha1Hash,
    piece_count: usize,
    services: Services,
    cancellation: CancellationToken,
    allow_pex: bool,
    torrent_id: TorrentId,
    peer_limit: usize,
}

enum PeerWorkerCommand {
    Download {
        piece: usize,
        length: usize,
        cancellation: CancellationToken,
    },
    Have {
        piece: u32,
    },
    Shutdown,
}

enum PeerWorkerEvent {
    Ready {
        worker: usize,
        bitfield: Vec<u8>,
        peers: Vec<SocketAddr>,
    },
    Complete {
        worker: usize,
        piece: usize,
        result: PieceResult,
    },
    Gone {
        worker: usize,
        error: String,
    },
    Peers {
        worker: usize,
        peers: Vec<SocketAddr>,
    },
    Have {
        worker: usize,
        piece: u32,
    },
    HolePunch {
        worker: usize,
        address: SocketAddr,
    },
}

enum PeerWorkerInput {
    Command(PeerWorkerCommand),
    Event(PeerEvent),
}

struct PeerEventForwarder<'a> {
    events: &'a mpsc::Sender<PeerWorkerEvent>,
    worker: usize,
    allow_extensions: bool,
}

enum PieceResult {
    Data(Bytes),
    Cancelled,
    Failed(String),
}

#[derive(Clone)]
pub struct EngineHandle {
    commands: mpsc::Sender<EngineCommand>,
    events: broadcast::Sender<EngineEvent>,
    services: Services,
}

impl std::fmt::Debug for EngineHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EngineHandle")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct EngineEvent {
    pub torrent_id: TorrentId,
    pub state: TorrentState,
    pub detail: Option<String>,
}

#[derive(Clone, Debug)]
pub struct EngineOptions {
    pub tracker_response_limit: usize,
    pub metainfo_limit: usize,
    pub dht_bootstrap: Vec<SocketAddr>,
    pub dht: Option<DhtClient>,
    pub utp: Option<UtpEndpoint>,
    pub peer_port: u16,
    pub encryption: EncryptionPolicy,
    pub peer_connection_limit: usize,
    pub allow_private_web_seeds: bool,
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("torrent engine stopped")]
    Stopped,
}

enum EngineCommand {
    Start(TorrentId),
    Pause {
        id: TorrentId,
        reply: oneshot::Sender<()>,
    },
    Recheck(TorrentId),
    Forget {
        id: TorrentId,
        reply: oneshot::Sender<()>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

struct ActiveActor {
    generation: u64,
    cancellation: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
struct Services {
    store: StateStoreHandle,
    storage: StorageHandle,
    tracker_response_limit: usize,
    metainfo_limit: usize,
    peer_message_timeout: Duration,
    allow_private_web_seeds: bool,
    dht_bootstrap: Vec<SocketAddr>,
    dht: Option<DhtClient>,
    utp: Option<UtpEndpoint>,
    peer_port: u16,
    advertised_peer_port: Arc<AtomicU16>,
    peer_id: PeerId,
    events: broadcast::Sender<EngineEvent>,
    peer_slots: Arc<Semaphore>,
    per_torrent_peer_limit: usize,
    lsd_cookie: String,
    encryption: EncryptionPolicy,
    rendezvous: Arc<Mutex<HashMap<(Sha1Hash, SocketAddr), RendezvousPeer>>>,
    connected_peers: Arc<AtomicUsize>,
    torrent_peers: Arc<std::sync::Mutex<HashMap<TorrentId, usize>>>,
    payload_claims: Arc<std::sync::Mutex<HashMap<TorrentId, Vec<TorrentPath>>>>,
    shutdown: CancellationToken,
    tasks: TaskTracker,
}

#[derive(Clone)]
struct RendezvousPeer {
    session: u64,
    extension_id: u8,
    sender: PeerSender,
}

struct ConnectionGuard {
    connected: Arc<AtomicUsize>,
    torrents: Arc<std::sync::Mutex<HashMap<TorrentId, usize>>>,
    torrent_id: TorrentId,
}

struct PayloadClaim {
    claims: Arc<std::sync::Mutex<HashMap<TorrentId, Vec<TorrentPath>>>>,
    torrent_id: TorrentId,
}

impl Drop for PayloadClaim {
    fn drop(&mut self) {
        if let Ok(mut claims) = self.claims.lock() {
            claims.remove(&self.torrent_id);
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.connected.fetch_sub(1, Ordering::AcqRel);
        if let Ok(mut peers) = self.torrents.lock()
            && let Some(count) = peers.get_mut(&self.torrent_id)
        {
            *count = count.saturating_sub(1);
            if *count == 0 {
                peers.remove(&self.torrent_id);
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ActorMode {
    Start,
    Recheck,
}

#[derive(Debug, Error)]
enum ActorError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("torrent does not exist")]
    Missing,
    #[error("magnet URI is missing or invalid: {0}")]
    Magnet(String),
    #[error("metadata exchange failed: {0}")]
    Metadata(String),
    #[error("torrent has no v1-compatible peer-wire hash")]
    V2PeerWire,
    #[error("piece index is outside the torrent layout")]
    PieceIndex,
    #[error("metainfo is invalid: {0}")]
    Metainfo(String),
    #[error("torrent has no usable HTTP(S) or UDP tracker")]
    NoTracker,
    #[error("trackers returned no connectable peers")]
    NoPeers,
    #[error("peer session failed: {0}")]
    Peer(String),
    #[error("web seed failed: {0}")]
    WebSeed(String),
    #[error("payload path {path} is owned by torrent {owner}")]
    PathConflict { path: String, owner: TorrentId },
    #[error("torrent arithmetic overflow")]
    Arithmetic,
    #[error("torrent actor was cancelled")]
    Cancelled,
}

impl EngineHandle {
    #[must_use]
    pub fn start(
        store: StateStoreHandle,
        storage: StorageHandle,
        tracker_response_limit: usize,
        metainfo_limit: usize,
        dht_bootstrap: Vec<SocketAddr>,
        utp: Option<UtpEndpoint>,
        peer_port: u16,
    ) -> Self {
        Self::start_configured(
            store,
            storage,
            EngineOptions {
                tracker_response_limit,
                metainfo_limit,
                dht_bootstrap,
                dht: None,
                utp,
                peer_port,
                encryption: EncryptionPolicy::Disabled,
                peer_connection_limit: INCOMING_PEER_LIMIT,
                allow_private_web_seeds: false,
            },
        )
    }

    #[must_use]
    pub fn start_configured(
        store: StateStoreHandle,
        storage: StorageHandle,
        options: EngineOptions,
    ) -> Self {
        let (commands, receiver) = mpsc::channel(COMMAND_CAPACITY);
        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        let services = Services {
            store,
            storage,
            tracker_response_limit: options.tracker_response_limit,
            metainfo_limit: options.metainfo_limit,
            peer_message_timeout: PEER_MESSAGE_TIMEOUT,
            allow_private_web_seeds: options.allow_private_web_seeds,
            dht_bootstrap: options.dht_bootstrap,
            dht: options.dht,
            utp: options.utp,
            peer_port: options.peer_port,
            advertised_peer_port: Arc::new(AtomicU16::new(options.peer_port)),
            peer_id: generate_peer_id(),
            events: events.clone(),
            peer_slots: Arc::new(Semaphore::new(options.peer_connection_limit.max(1))),
            per_torrent_peer_limit: per_torrent_peer_limit(options.peer_connection_limit),
            lsd_cookie: format!("dendrite-{:016x}", rand::random::<u64>()),
            encryption: options.encryption,
            rendezvous: Arc::new(Mutex::new(HashMap::new())),
            connected_peers: Arc::new(AtomicUsize::new(0)),
            torrent_peers: Arc::new(std::sync::Mutex::new(HashMap::new())),
            payload_claims: Arc::new(std::sync::Mutex::new(HashMap::new())),
            shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
        };
        let (completions, completion_receiver) = mpsc::unbounded_channel();
        tokio::spawn(supervisor(
            receiver,
            completion_receiver,
            completions,
            services.clone(),
        ));
        services.tasks.spawn(run_lsd_announcer(services.clone()));
        Self {
            commands,
            events,
            services,
        }
    }

    pub async fn resume(&self, id: TorrentId) -> Result<(), EngineError> {
        self.commands
            .send(EngineCommand::Start(id))
            .await
            .map_err(|_| EngineError::Stopped)
    }

    pub async fn pause(&self, id: TorrentId) -> Result<(), EngineError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(EngineCommand::Pause { id, reply })
            .await
            .map_err(|_| EngineError::Stopped)?;
        response.await.map_err(|_| EngineError::Stopped)
    }

    pub async fn recheck(&self, id: TorrentId) -> Result<(), EngineError> {
        self.commands
            .send(EngineCommand::Recheck(id))
            .await
            .map_err(|_| EngineError::Stopped)
    }

    /// Cancel an actor without writing another state transition. This is used
    /// immediately before deleting its durable record.
    pub async fn forget(&self, id: TorrentId) -> Result<(), EngineError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(EngineCommand::Forget { id, reply })
            .await
            .map_err(|_| EngineError::Stopped)?;
        response.await.map_err(|_| EngineError::Stopped)
    }

    /// Cancels all actors and background network services and waits until
    /// torrent actors have stopped mutating durable state.
    pub async fn shutdown(&self) -> Result<(), EngineError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(EngineCommand::Shutdown { reply })
            .await
            .map_err(|_| EngineError::Stopped)?;
        response.await.map_err(|_| EngineError::Stopped)
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<EngineEvent> {
        self.events.subscribe()
    }

    /// Start bounded TCP and uTP accept loops for incoming downloads. The
    /// listener remains owned by the engine task for the daemon lifetime.
    pub fn serve_incoming(&self, tcp: TcpListener) {
        let services = self.services.clone();
        self.services.tasks.spawn(accept_tcp_peers(tcp, services));
        if let Some(utp) = self.services.utp.clone() {
            let services = self.services.clone();
            self.services.tasks.spawn(accept_utp_peers(utp, services));
        }
    }

    /// Update the public port reported to trackers after a successfully
    /// correlated NAT mapping. Local discovery continues to advertise the
    /// actual LAN listening port.
    pub fn set_advertised_peer_port(&self, port: u16) {
        if port != 0 {
            self.services
                .advertised_peer_port
                .store(port, Ordering::Release);
        }
    }

    #[must_use]
    pub fn connected_peers(&self) -> usize {
        self.services.connected_peers.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn torrent_peer_count(&self, id: TorrentId) -> usize {
        self.services
            .torrent_peers
            .lock()
            .ok()
            .and_then(|peers| peers.get(&id).copied())
            .unwrap_or(0)
    }
}

fn track_connection(services: &Services, torrent_id: TorrentId) -> ConnectionGuard {
    services.connected_peers.fetch_add(1, Ordering::AcqRel);
    if let Ok(mut peers) = services.torrent_peers.lock() {
        *peers.entry(torrent_id).or_default() += 1;
    }
    ConnectionGuard {
        connected: services.connected_peers.clone(),
        torrents: services.torrent_peers.clone(),
        torrent_id,
    }
}

async fn run_lsd_announcer(services: Services) {
    let Ok(discovery) = LsdService::bind(services.peer_port, services.lsd_cookie.clone()) else {
        debug!("local discovery is unavailable");
        return;
    };
    let mut interval = tokio::time::interval(Duration::from_mins(5));
    let mut last_announce = None::<Instant>;
    loop {
        tokio::select! {
            () = services.shutdown.cancelled() => return,
            _ = interval.tick() => {
                let hashes = active_lsd_hashes(&services).await;
                for chunk in hashes.chunks(20) {
                    if let Err(error) = discovery.announce(chunk).await {
                        debug!(%error, "local discovery announce failed");
                    }
                }
                if !hashes.is_empty() {
                    last_announce = Some(Instant::now());
                }
            }
            received = discovery.receive() => {
                let Ok((announce, _)) = received else {
                    continue;
                };
                if last_announce.is_some_and(|sent| sent.elapsed() < Duration::from_mins(1)) {
                    continue;
                }
                let active = active_lsd_hashes(&services).await;
                let matching: Vec<_> = active
                    .into_iter()
                    .filter(|hash| announce.info_hashes.contains(hash))
                    .collect();
                if !matching.is_empty() && discovery.announce(&matching).await.is_ok() {
                    last_announce = Some(Instant::now());
                }
            }
        }
    }
}

async fn active_lsd_hashes(services: &Services) -> Vec<Sha1Hash> {
    let Ok(records) = services.store.list_torrents().await else {
        return Vec::new();
    };
    records
        .into_iter()
        .filter(|record| {
            matches!(
                record.state,
                TorrentState::Downloading | TorrentState::Seeding
            ) && !record.raw_metainfo.is_empty()
        })
        .filter_map(|record| {
            let metainfo = Metainfo::parse(
                &record.raw_metainfo,
                BencodeLimits {
                    input_bytes: services.metainfo_limit,
                    byte_string_bytes: services.metainfo_limit,
                    ..BencodeLimits::default()
                },
            )
            .ok()?;
            (!metainfo.private)
                .then(|| wire_info_hash(&metainfo).ok())
                .flatten()
        })
        .collect()
}

async fn discover_lsd_peer(info_hash: Sha1Hash, services: &Services) -> Vec<SocketAddr> {
    let Ok(discovery) = LsdService::bind(services.peer_port, services.lsd_cookie.clone()) else {
        return Vec::new();
    };
    if discovery.announce(&[info_hash]).await.is_err() {
        return Vec::new();
    }
    let deadline = tokio::time::sleep(Duration::from_secs(1));
    tokio::pin!(deadline);
    let mut peers = HashSet::new();
    loop {
        tokio::select! {
            () = &mut deadline => return peers.into_iter().collect(),
            received = discovery.receive() => {
                let Ok((announce, source)) = received else {
                    continue;
                };
                if announce.info_hashes.contains(&info_hash) {
                    peers.insert(SocketAddr::new(source.ip(), announce.port));
                }
            }
        }
    }
}

async fn accept_tcp_peers(listener: TcpListener, services: Services) {
    let mut error_backoff = ACCEPT_ERROR_BACKOFF_MIN;
    loop {
        let permit = tokio::select! {
            () = services.shutdown.cancelled() => return,
            permit = services.peer_slots.clone().acquire_owned() => permit,
        };
        let Ok(permit) = permit else {
            return;
        };
        let accepted = tokio::select! {
            () = services.shutdown.cancelled() => return,
            accepted = listener.accept() => accepted,
        };
        match accepted {
            Ok((stream, address)) => {
                error_backoff = ACCEPT_ERROR_BACKOFF_MIN;
                let services = services.clone();
                let tasks = services.tasks.clone();
                tasks.spawn(async move {
                    if let Err(error) = serve_incoming_stream(stream, address, &services).await {
                        debug!(%address, %error, "incoming TCP peer stopped");
                    }
                    drop(permit);
                });
            }
            Err(error) => {
                warn!(%error, "incoming TCP accept failed");
                drop(permit);
                tokio::select! {
                    () = services.shutdown.cancelled() => return,
                    () = tokio::time::sleep(error_backoff) => {}
                }
                error_backoff = next_accept_error_backoff(error_backoff);
            }
        }
    }
}

async fn accept_utp_peers(endpoint: UtpEndpoint, services: Services) {
    let mut error_backoff = ACCEPT_ERROR_BACKOFF_MIN;
    loop {
        let permit = tokio::select! {
            () = services.shutdown.cancelled() => return,
            permit = services.peer_slots.clone().acquire_owned() => permit,
        };
        let Ok(permit) = permit else {
            return;
        };
        let accepted = tokio::select! {
            () = services.shutdown.cancelled() => return,
            accepted = endpoint.accept_stream() => accepted,
        };
        match accepted {
            Ok(stream) => {
                error_backoff = ACCEPT_ERROR_BACKOFF_MIN;
                let address = stream.remote_addr();
                let services = services.clone();
                let tasks = services.tasks.clone();
                tasks.spawn(async move {
                    if let Err(error) = serve_incoming_stream(stream, address, &services).await {
                        debug!(%address, %error, "incoming uTP peer stopped");
                    }
                    drop(permit);
                });
            }
            Err(error) => {
                warn!(%error, "incoming uTP accept failed");
                drop(permit);
                tokio::select! {
                    () = services.shutdown.cancelled() => return,
                    () = tokio::time::sleep(error_backoff) => {}
                }
                error_backoff = next_accept_error_backoff(error_backoff);
            }
        }
    }
}

fn next_accept_error_backoff(current: Duration) -> Duration {
    current.saturating_mul(2).min(ACCEPT_ERROR_BACKOFF_MAX)
}

async fn serve_incoming_stream<S>(
    mut stream: S,
    address: SocketAddr,
    services: &Services,
) -> Result<(), ActorError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut prefix = [0_u8; 20];
    tokio::select! {
        () = services.shutdown.cancelled() => return Err(ActorError::Cancelled),
        result = tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut prefix)) => {
            result
                .map_err(|_| ActorError::Peer("incoming handshake timed out".to_owned()))?
                .map_err(|error| ActorError::Peer(error.to_string()))?;
        }
    }
    if prefix == *b"\x13BitTorrent protocol" {
        if services.encryption == EncryptionPolicy::Required {
            return Err(ActorError::Peer(
                "plaintext peer rejected by encryption policy".to_owned(),
            ));
        }
        let mut encoded = [0_u8; dendrite_net::peer::HANDSHAKE_BYTES];
        encoded[..prefix.len()].copy_from_slice(&prefix);
        tokio::select! {
            () = services.shutdown.cancelled() => return Err(ActorError::Cancelled),
            result = tokio::time::timeout(
                Duration::from_secs(10),
                stream.read_exact(&mut encoded[prefix.len()..]),
            ) => {
                result
                    .map_err(|_| ActorError::Peer("incoming handshake timed out".to_owned()))?
                    .map_err(|error| ActorError::Peer(error.to_string()))?;
            }
        }
        let remote =
            Handshake::decode(&encoded).map_err(|error| ActorError::Peer(error.to_string()))?;
        return finish_incoming_stream(stream, address, remote, services).await;
    }
    if services.encryption == EncryptionPolicy::Disabled {
        return Err(ActorError::Peer(
            "encrypted peer rejected by encryption policy".to_owned(),
        ));
    }
    let candidates = incoming_wire_hashes(services).await?;
    let (mut encrypted, selected) = tokio::time::timeout(
        Duration::from_secs(10),
        dendrite_net::mse::respond_prefixed(stream, &prefix, &candidates),
    )
    .await
    .map_err(|_| ActorError::Peer("encrypted handshake timed out".to_owned()))?
    .map_err(|error| ActorError::Peer(error.to_string()))?;
    let remote = PeerConnection::receive_handshake(&mut encrypted)
        .await
        .map_err(|error| ActorError::Peer(error.to_string()))?;
    if remote.info_hash != selected {
        return Err(ActorError::Peer(
            "encrypted peer handshake changed its info hash".to_owned(),
        ));
    }
    finish_incoming_stream(encrypted, address, remote, services).await
}

async fn incoming_wire_hashes(services: &Services) -> Result<Vec<Sha1Hash>, ActorError> {
    let records = services.store.list_torrents().await?;
    Ok(records
        .into_iter()
        .filter(|record| {
            matches!(
                record.state,
                TorrentState::Downloading | TorrentState::Seeding
            )
        })
        .filter_map(|record| {
            record.v1_info_hash.or_else(|| {
                record.v2_info_hash.map(|hash| {
                    let mut truncated = [0_u8; 20];
                    truncated.copy_from_slice(&hash.as_bytes()[..20]);
                    Sha1Hash::from_bytes(truncated)
                })
            })
        })
        .collect())
}

async fn finish_incoming_stream<S>(
    stream: S,
    address: SocketAddr,
    remote: Handshake,
    services: &Services,
) -> Result<(), ActorError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (record, metainfo) = find_incoming_torrent(remote.info_hash, services).await?;
    let pieces = piece_count(&metainfo)?;
    if record.completed_pieces.len() != pieces.div_ceil(8) {
        return Err(ActorError::Peer(
            "stored completion bitfield has the wrong length".to_owned(),
        ));
    }
    let mut reserved = [0_u8; 8];
    reserved[5] |= 0x10;
    let mut peer = PeerConnection::accept_incoming(
        stream,
        remote,
        Handshake {
            reserved,
            info_hash: remote.info_hash,
            peer_id: services.peer_id,
        },
        PeerCodecLimits {
            bitfield_bytes: record.completed_pieces.len(),
            extension_bytes: services.metainfo_limit,
            frame_bytes: services.metainfo_limit.max(BLOCK_BYTES).saturating_add(64),
            ..PeerCodecLimits::default()
        },
    )
    .await
    .map_err(|error| ActorError::Peer(error.to_string()))?;
    let _connection = track_connection(services, record.id);
    peer.send(PeerMessage::Bitfield(Bytes::copy_from_slice(
        &record.completed_pieces,
    )))
    .await
    .map_err(|error| ActorError::Peer(error.to_string()))?;

    let info = raw_info_dictionary(&record.raw_metainfo, services.metainfo_limit)?;
    run_incoming_peer(
        &mut peer,
        address,
        remote,
        IncomingSeed {
            record,
            metainfo,
            pieces,
            info,
            interested: false,
            remote_metadata_id: None,
            remote_holepunch_id: None,
            cached_piece: None,
        },
        services,
    )
    .await
}

struct IncomingSeed {
    record: TorrentRecord,
    metainfo: Metainfo,
    pieces: usize,
    info: Bytes,
    interested: bool,
    remote_metadata_id: Option<u8>,
    remote_holepunch_id: Option<u8>,
    cached_piece: Option<(usize, Bytes)>,
}

async fn run_incoming_peer(
    peer: &mut PeerConnection,
    address: SocketAddr,
    remote: Handshake,
    mut seed: IncomingSeed,
    services: &Services,
) -> Result<(), ActorError> {
    let session = rand::random();
    loop {
        let event = tokio::select! {
            () = services.shutdown.cancelled() => break,
            event = next_peer_event(peer) => event?,
        };
        match event {
            PeerEvent::Message(PeerMessage::Interested) => {
                seed.interested = true;
                peer.send(PeerMessage::Unchoke)
                    .await
                    .map_err(|error| ActorError::Peer(error.to_string()))?;
            }
            PeerEvent::Message(PeerMessage::NotInterested) => seed.interested = false,
            PeerEvent::Message(PeerMessage::Request(request)) if seed.interested => {
                serve_piece_request(peer, remote, &mut seed, request, services).await?;
            }
            PeerEvent::Message(PeerMessage::Extended {
                extension_id: 0,
                payload,
            }) => {
                let handshake = decode_extension_handshake(&payload, services.metainfo_limit)
                    .map_err(|error| ActorError::Peer(error.to_string()))?;
                seed.remote_metadata_id = handshake.metadata_extension_id;
                seed.remote_holepunch_id = handshake.holepunch_extension_id;
                if let Some(extension_id) = handshake.holepunch_extension_id {
                    services.rendezvous.lock().await.insert(
                        (remote.info_hash, address),
                        RendezvousPeer {
                            session,
                            extension_id,
                            sender: peer.sender(),
                        },
                    );
                }
                peer.send(PeerMessage::Extended {
                    extension_id: 0,
                    payload: encode_extension_handshake(Some(seed.info.len())),
                })
                .await
                .map_err(|error| ActorError::Peer(error.to_string()))?;
            }
            PeerEvent::Message(PeerMessage::Extended {
                extension_id: LOCAL_METADATA_EXTENSION_ID,
                payload,
            }) => {
                serve_metadata_request(
                    peer,
                    seed.remote_metadata_id,
                    &seed.info,
                    &payload,
                    services.metainfo_limit,
                )
                .await?;
            }
            PeerEvent::Message(PeerMessage::HashRequest(request)) => {
                serve_hash_request(peer, &seed.metainfo, request).await?;
            }
            PeerEvent::Message(PeerMessage::Extended {
                extension_id: LOCAL_HOLEPUNCH_EXTENSION_ID,
                payload,
            }) => {
                handle_seed_holepunch(peer, address, remote.info_hash, &seed, services, &payload)
                    .await?;
            }
            PeerEvent::Disconnected => break,
            PeerEvent::Failed(error) => {
                unregister_rendezvous_peer(services, remote.info_hash, address, session).await;
                return Err(ActorError::Peer(error));
            }
            _ => {}
        }
    }
    unregister_rendezvous_peer(services, remote.info_hash, address, session).await;
    Ok(())
}

async fn unregister_rendezvous_peer(
    services: &Services,
    info_hash: Sha1Hash,
    address: SocketAddr,
    session: u64,
) {
    let mut peers = services.rendezvous.lock().await;
    if peers
        .get(&(info_hash, address))
        .is_some_and(|peer| peer.session == session)
    {
        peers.remove(&(info_hash, address));
    }
}

async fn handle_seed_holepunch(
    peer: &PeerConnection,
    requester_address: SocketAddr,
    info_hash: Sha1Hash,
    seed: &IncomingSeed,
    services: &Services,
    payload: &[u8],
) -> Result<(), ActorError> {
    let message =
        decode_holepunch_message(payload).map_err(|error| ActorError::Peer(error.to_string()))?;
    match message.kind {
        HolePunchKind::Connect => {
            let seed = IncomingSeed {
                record: seed.record.clone(),
                metainfo: seed.metainfo.clone(),
                pieces: seed.pieces,
                info: seed.info.clone(),
                interested: false,
                remote_metadata_id: None,
                remote_holepunch_id: None,
                cached_piece: None,
            };
            let services = services.clone();
            let tasks = services.tasks.clone();
            tasks.spawn(async move {
                if let Err(error) = serve_hole_punched_seed(message.address, seed, &services).await
                {
                    debug!(%error, "hole-punched seed connection failed");
                }
            });
        }
        HolePunchKind::Rendezvous => {
            let target = services
                .rendezvous
                .lock()
                .await
                .get(&(info_hash, message.address))
                .cloned();
            if let (Some(requester_extension), Some(target)) = (seed.remote_holepunch_id, target) {
                target
                    .sender
                    .send(PeerMessage::Extended {
                        extension_id: target.extension_id,
                        payload: encode_holepunch_message(HolePunchMessage {
                            kind: HolePunchKind::Connect,
                            address: requester_address,
                            error_code: 0,
                        })
                        .map_err(|error| ActorError::Peer(error.to_string()))?,
                    })
                    .await
                    .map_err(|error| ActorError::Peer(error.to_string()))?;
                peer.send(PeerMessage::Extended {
                    extension_id: requester_extension,
                    payload: encode_holepunch_message(HolePunchMessage {
                        kind: HolePunchKind::Connect,
                        address: message.address,
                        error_code: 0,
                    })
                    .map_err(|error| ActorError::Peer(error.to_string()))?,
                })
                .await
                .map_err(|error| ActorError::Peer(error.to_string()))?;
            } else if let Some(extension_id) = seed.remote_holepunch_id {
                peer.send(PeerMessage::Extended {
                    extension_id,
                    payload: encode_holepunch_message(HolePunchMessage {
                        kind: HolePunchKind::Error,
                        address: message.address,
                        error_code: 2,
                    })
                    .map_err(|error| ActorError::Peer(error.to_string()))?,
                })
                .await
                .map_err(|error| ActorError::Peer(error.to_string()))?;
            }
        }
        HolePunchKind::Error => {}
    }
    Ok(())
}

async fn serve_hole_punched_seed(
    address: SocketAddr,
    seed: IncomingSeed,
    services: &Services,
) -> Result<(), ActorError> {
    let _permit = services
        .peer_slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ActorError::Cancelled)?;
    let endpoint = services
        .utp
        .as_ref()
        .ok_or_else(|| ActorError::Peer("hole punch requires a uTP endpoint".to_owned()))?;
    let info_hash = wire_info_hash(&seed.metainfo)?;
    let mut reserved = [0_u8; 8];
    reserved[5] |= 0x10;
    let mut peer = endpoint
        .connect_peer(
            address,
            Handshake {
                reserved,
                info_hash,
                peer_id: services.peer_id,
            },
            PeerCodecLimits::default(),
        )
        .await
        .map_err(|error| ActorError::Peer(error.to_string()))?;
    let _connection = track_connection(services, seed.record.id);
    peer.send(PeerMessage::Bitfield(Bytes::copy_from_slice(
        &seed.record.completed_pieces,
    )))
    .await
    .map_err(|error| ActorError::Peer(error.to_string()))?;
    let remote = Handshake {
        reserved: [0; 8],
        info_hash,
        peer_id: PeerId::from_bytes([0; 20]),
    };
    run_hole_seed_peer(&mut peer, remote, seed, services).await
}

async fn run_hole_seed_peer(
    peer: &mut PeerConnection,
    remote: Handshake,
    mut seed: IncomingSeed,
    services: &Services,
) -> Result<(), ActorError> {
    loop {
        match next_peer_event(peer).await? {
            PeerEvent::Message(PeerMessage::Interested) => {
                seed.interested = true;
                peer.send(PeerMessage::Unchoke)
                    .await
                    .map_err(|error| ActorError::Peer(error.to_string()))?;
            }
            PeerEvent::Message(PeerMessage::NotInterested) => seed.interested = false,
            PeerEvent::Message(PeerMessage::Request(request)) if seed.interested => {
                serve_piece_request(peer, remote, &mut seed, request, services).await?;
            }
            PeerEvent::Message(PeerMessage::HashRequest(request)) => {
                serve_hash_request(peer, &seed.metainfo, request).await?;
            }
            PeerEvent::Disconnected => return Ok(()),
            PeerEvent::Failed(error) => return Err(ActorError::Peer(error)),
            _ => {}
        }
    }
}

async fn serve_piece_request(
    peer: &PeerConnection,
    remote: Handshake,
    seed: &mut IncomingSeed,
    request: BlockRequest,
    services: &Services,
) -> Result<(), ActorError> {
    let index = usize::try_from(request.piece).map_err(|_| ActorError::Arithmetic)?;
    let length = usize::try_from(request.length).map_err(|_| ActorError::Arithmetic)?;
    if length == 0
        || length > BLOCK_BYTES
        || index >= seed.pieces
        || !bit_is_set(&seed.record.completed_pieces, index)
    {
        return reject_request(peer, remote, request).await;
    }
    if seed
        .cached_piece
        .as_ref()
        .is_none_or(|(piece, _)| *piece != index)
    {
        let Some(data) = read_piece(&seed.metainfo, index, &services.storage).await? else {
            return reject_request(peer, remote, request).await;
        };
        if !verify_piece(&seed.metainfo, index, &data)? {
            return Err(ActorError::Peer(
                "refusing to upload a corrupt completed piece".to_owned(),
            ));
        }
        seed.cached_piece = Some((index, data));
    }
    let begin = usize::try_from(request.begin).map_err(|_| ActorError::Arithmetic)?;
    let end = begin.checked_add(length).ok_or(ActorError::Arithmetic)?;
    let Some(block) = seed
        .cached_piece
        .as_ref()
        .and_then(|(_, piece)| piece.get(begin..end))
    else {
        return reject_request(peer, remote, request).await;
    };
    peer.send(PeerMessage::Piece {
        piece: request.piece,
        begin: request.begin,
        block: Bytes::copy_from_slice(block),
    })
    .await
    .map_err(|error| ActorError::Peer(error.to_string()))?;
    let uploaded = u64::try_from(block.len()).map_err(|_| ActorError::Arithmetic)?;
    if services
        .store
        .increment_uploaded(seed.record.id, uploaded)
        .await?
    {
        Ok(())
    } else {
        Err(ActorError::Missing)
    }
}

async fn find_incoming_torrent(
    info_hash: Sha1Hash,
    services: &Services,
) -> Result<(TorrentRecord, Metainfo), ActorError> {
    for record in services.store.list_torrents().await? {
        if !matches!(
            record.state,
            TorrentState::Downloading | TorrentState::Seeding
        ) || record.raw_metainfo.is_empty()
        {
            continue;
        }
        let Ok(metainfo) = Metainfo::parse(
            &record.raw_metainfo,
            BencodeLimits {
                input_bytes: services.metainfo_limit,
                byte_string_bytes: services.metainfo_limit,
                ..BencodeLimits::default()
            },
        ) else {
            continue;
        };
        if wire_info_hash(&metainfo)? == info_hash {
            return Ok((record, metainfo));
        }
    }
    Err(ActorError::Peer(
        "incoming info hash is not active".to_owned(),
    ))
}

fn raw_info_dictionary(raw: &[u8], limit: usize) -> Result<Bytes, ActorError> {
    let root = decode(
        raw,
        BencodeLimits {
            input_bytes: limit,
            byte_string_bytes: limit,
            ..BencodeLimits::default()
        },
    )
    .map_err(|error| ActorError::Metainfo(error.to_string()))?;
    let BencodeValue::Dictionary(fields) = root.value else {
        return Err(ActorError::Metainfo(
            "metainfo root is not a dictionary".to_owned(),
        ));
    };
    let info = fields
        .iter()
        .find_map(|(key, value)| (*key == b"info").then_some(value))
        .ok_or_else(|| ActorError::Metainfo("metainfo has no info dictionary".to_owned()))?;
    Ok(Bytes::copy_from_slice(&raw[info.span.clone()]))
}

async fn reject_request(
    peer: &PeerConnection,
    remote: Handshake,
    request: BlockRequest,
) -> Result<(), ActorError> {
    if remote.reserved[7] & 0x04 != 0 {
        peer.send(PeerMessage::Reject(request))
            .await
            .map_err(|error| ActorError::Peer(error.to_string()))?;
    }
    Ok(())
}

async fn serve_metadata_request(
    peer: &PeerConnection,
    remote_extension_id: Option<u8>,
    info: &[u8],
    payload: &[u8],
    metainfo_limit: usize,
) -> Result<(), ActorError> {
    let message = decode_metadata_message(payload, metainfo_limit)
        .map_err(|error| ActorError::Peer(error.to_string()))?;
    let MetadataMessage::Request { piece } = message else {
        return Ok(());
    };
    let Some(extension_id) = remote_extension_id else {
        return Ok(());
    };
    let start = usize::try_from(piece)
        .ok()
        .and_then(|piece| piece.checked_mul(METADATA_BLOCK_BYTES));
    let response = start
        .and_then(|start| {
            info.get(start..info.len().min(start.saturating_add(METADATA_BLOCK_BYTES)))
        })
        .map_or_else(
            || encode_metadata_reject(piece),
            |block| encode_metadata_data(piece, info.len(), block),
        );
    peer.send(PeerMessage::Extended {
        extension_id,
        payload: response,
    })
    .await
    .map_err(|error| ActorError::Peer(error.to_string()))
}

async fn serve_hash_request(
    peer: &PeerConnection,
    metainfo: &Metainfo,
    request: HashRequest,
) -> Result<(), ActorError> {
    let piece_layer_base = (metainfo.piece_length.get() / 16_384).ilog2();
    let layer = metainfo.piece_layers.get(&request.pieces_root);
    if request.base_layer != piece_layer_base
        || request.length < 2
        || !request.length.is_power_of_two()
        || request.length > 512
        || !request.index.is_multiple_of(request.length)
        || layer.is_none()
    {
        peer.send(PeerMessage::HashReject(request))
            .await
            .map_err(|error| ActorError::Peer(error.to_string()))?;
        return Ok(());
    }
    let layer = layer.ok_or(ActorError::Arithmetic)?;
    let start = usize::try_from(request.index).map_err(|_| ActorError::Arithmetic)?;
    let length = usize::try_from(request.length).map_err(|_| ActorError::Arithmetic)?;
    let Some(end) = start.checked_add(length) else {
        return Err(ActorError::Arithmetic);
    };
    let width = layer.len().max(1).next_power_of_two();
    let tree_height = width.ilog2();
    if start >= width || end > width || request.proof_layers > tree_height {
        peer.send(PeerMessage::HashReject(request))
            .await
            .map_err(|error| ActorError::Peer(error.to_string()))?;
        return Ok(());
    }
    let zero = v2_zero_hash(request.base_layer);
    let mut current = Vec::with_capacity(width);
    current.extend_from_slice(layer);
    current.resize(width, zero);
    let mut levels = vec![current];
    while levels.last().is_some_and(|level| level.len() > 1) {
        let parent = levels
            .last()
            .ok_or(ActorError::Arithmetic)?
            .chunks_exact(2)
            .map(|pair| v2_hash_pair(pair[0], pair[1]))
            .collect();
        levels.push(parent);
    }
    let proof_count = usize::try_from(request.proof_layers.saturating_sub(request.length.ilog2()))
        .map_err(|_| ActorError::Arithmetic)?;
    let mut hashes = Vec::with_capacity(length.saturating_add(proof_count).saturating_mul(32));
    for hash in &levels[0][start..end] {
        hashes.extend_from_slice(hash.as_bytes());
    }
    let mut node = start / length;
    for level in request.length.ilog2()..request.proof_layers {
        let level = usize::try_from(level).map_err(|_| ActorError::Arithmetic)?;
        let sibling = node ^ 1;
        let hash = levels
            .get(level)
            .and_then(|hashes| hashes.get(sibling))
            .ok_or(ActorError::Arithmetic)?;
        hashes.extend_from_slice(hash.as_bytes());
        node /= 2;
    }
    peer.send(PeerMessage::Hashes {
        request,
        hashes: Bytes::from(hashes),
    })
    .await
    .map_err(|error| ActorError::Peer(error.to_string()))
}

async fn supervisor(
    mut receiver: mpsc::Receiver<EngineCommand>,
    mut completion_receiver: mpsc::UnboundedReceiver<(TorrentId, u64)>,
    completions: mpsc::UnboundedSender<(TorrentId, u64)>,
    services: Services,
) {
    let mut active = HashMap::<TorrentId, ActiveActor>::new();
    let mut next_generation = 0_u64;
    loop {
        let command = tokio::select! {
            command = receiver.recv() => match command {
                Some(command) => command,
                None => break,
            },
            completion = completion_receiver.recv() => {
                if let Some((id, generation)) = completion
                    && active.get(&id).is_some_and(|actor| actor.generation == generation)
                {
                    active.remove(&id);
                }
                continue;
            }
        };
        match command {
            EngineCommand::Start(id) => {
                stop_actor(&mut active, id).await;
                next_generation = next_generation.wrapping_add(1);
                spawn_actor(
                    id,
                    ActorMode::Start,
                    &mut active,
                    next_generation,
                    completions.clone(),
                    services.clone(),
                );
            }
            EngineCommand::Recheck(id) => {
                stop_actor(&mut active, id).await;
                next_generation = next_generation.wrapping_add(1);
                spawn_actor(
                    id,
                    ActorMode::Recheck,
                    &mut active,
                    next_generation,
                    completions.clone(),
                    services.clone(),
                );
            }
            EngineCommand::Pause { id, reply } => {
                stop_actor(&mut active, id).await;
                if let Err(error) = set_state(&services, id, TorrentState::Stopped).await {
                    warn!(%id, %error, "failed to persist paused state");
                }
                let _result_ignored = reply.send(());
            }
            EngineCommand::Forget { id, reply } => {
                stop_actor(&mut active, id).await;
                let _result_ignored = reply.send(());
            }
            EngineCommand::Shutdown { reply } => {
                stop_all_actors(&mut active).await;
                stop_background_tasks(&services).await;
                let _result_ignored = reply.send(());
                return;
            }
        }
    }
    stop_all_actors(&mut active).await;
    stop_background_tasks(&services).await;
}

async fn stop_background_tasks(services: &Services) {
    services.shutdown.cancel();
    services.tasks.close();
    services.tasks.wait().await;
}

async fn stop_all_actors(active: &mut HashMap<TorrentId, ActiveActor>) {
    let actors: Vec<_> = active.drain().map(|(_, actor)| actor).collect();
    for actor in &actors {
        actor.cancellation.cancel();
    }
    for actor in actors {
        let _result_ignored = actor.task.await;
    }
}

async fn stop_actor(active: &mut HashMap<TorrentId, ActiveActor>, id: TorrentId) {
    if let Some(actor) = active.remove(&id) {
        actor.cancellation.cancel();
        let _result_ignored = actor.task.await;
    }
}

fn spawn_actor(
    id: TorrentId,
    mode: ActorMode,
    active: &mut HashMap<TorrentId, ActiveActor>,
    generation: u64,
    completions: mpsc::UnboundedSender<(TorrentId, u64)>,
    services: Services,
) {
    let cancellation = CancellationToken::new();
    let actor_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        let result = run_actor(id, mode, &services, &actor_cancellation).await;
        if let Err(error) = result
            && !matches!(error, ActorError::Cancelled)
        {
            warn!(%id, %error, "torrent actor failed");
            let _result_ignored = set_error(&services, id, error.to_string()).await;
        }
        let _result_ignored = completions.send((id, generation));
    });
    active.insert(
        id,
        ActiveActor {
            generation,
            cancellation,
            task,
        },
    );
}

async fn run_actor(
    id: TorrentId,
    mode: ActorMode,
    services: &Services,
    cancellation: &CancellationToken,
) -> Result<(), ActorError> {
    let mut record = services
        .store
        .get_torrent(id)
        .await?
        .ok_or(ActorError::Missing)?;
    if record.raw_metainfo.is_empty() {
        acquire_magnet_metadata(&mut record, services, cancellation).await?;
    }
    let metainfo = Metainfo::parse(&record.raw_metainfo, BencodeLimits::default())
        .map_err(|error| ActorError::Metainfo(error.to_string()))?;
    ensure_exclusive_payload_paths(&record, &metainfo, services).await?;
    let _payload_claim = claim_active_payload_paths(&record, &metainfo, services)?;
    normalize_completion(&mut record, piece_count(&metainfo)?);
    match mode {
        ActorMode::Recheck => recheck(&metainfo, &mut record, services, cancellation).await,
        ActorMode::Start => download(&metainfo, &mut record, services, cancellation).await,
    }
}

async fn ensure_exclusive_payload_paths(
    record: &TorrentRecord,
    metainfo: &Metainfo,
    services: &Services,
) -> Result<(), ActorError> {
    let current_key = (record.added_at_unix_ms, record.id);
    let current_paths: Vec<_> = metainfo
        .files
        .iter()
        .filter(|file| !file.padding)
        .map(|file| &file.path)
        .collect();
    for other in services.store.list_torrents().await? {
        if other.id == record.id
            || other.raw_metainfo.is_empty()
            || (other.added_at_unix_ms, other.id) > current_key
        {
            continue;
        }
        let Ok(other_metainfo) = Metainfo::parse(
            &other.raw_metainfo,
            BencodeLimits {
                input_bytes: services.metainfo_limit,
                byte_string_bytes: services.metainfo_limit,
                ..BencodeLimits::default()
            },
        ) else {
            continue;
        };
        for current in &current_paths {
            if let Some(conflict) = other_metainfo
                .files
                .iter()
                .filter(|file| !file.padding)
                .map(|file| &file.path)
                .find(|other| payload_paths_conflict(current, other))
            {
                return Err(ActorError::PathConflict {
                    path: conflict.to_string(),
                    owner: other.id,
                });
            }
        }
    }
    Ok(())
}

fn claim_active_payload_paths(
    record: &TorrentRecord,
    metainfo: &Metainfo,
    services: &Services,
) -> Result<PayloadClaim, ActorError> {
    let paths: Vec<_> = metainfo
        .files
        .iter()
        .filter(|file| !file.padding)
        .map(|file| file.path.clone())
        .collect();
    let mut claims = services
        .payload_claims
        .lock()
        .map_err(|_| ActorError::Peer("payload ownership registry was poisoned".to_owned()))?;
    for (owner, owned_paths) in claims.iter() {
        if *owner == record.id {
            continue;
        }
        if let Some(conflict) = paths.iter().find(|path| {
            owned_paths
                .iter()
                .any(|owned| payload_paths_conflict(path, owned))
        }) {
            return Err(ActorError::PathConflict {
                path: conflict.to_string(),
                owner: *owner,
            });
        }
    }
    claims.insert(record.id, paths);
    drop(claims);
    Ok(PayloadClaim {
        claims: services.payload_claims.clone(),
        torrent_id: record.id,
    })
}

fn payload_paths_conflict(left: &TorrentPath, right: &TorrentPath) -> bool {
    let left: Vec<_> = left
        .components()
        .iter()
        .map(|component| component.to_lowercase())
        .collect();
    let right: Vec<_> = right
        .components()
        .iter()
        .map(|component| component.to_lowercase())
        .collect();
    left.starts_with(&right) || right.starts_with(&left)
}

async fn acquire_magnet_metadata(
    record: &mut TorrentRecord,
    services: &Services,
    cancellation: &CancellationToken,
) -> Result<(), ActorError> {
    let uri = record
        .magnet_uri
        .as_deref()
        .ok_or_else(|| ActorError::Magnet("record has no URI".to_owned()))?;
    let magnet = Magnet::parse(uri).map_err(|error| ActorError::Magnet(error.to_string()))?;
    let info_hash = if let Some(hash) = magnet.v1_info_hash {
        hash
    } else {
        let hash = magnet.v2_info_hash.ok_or(ActorError::V2PeerWire)?;
        let truncated: [u8; 20] = hash.as_bytes()[..20]
            .try_into()
            .map_err(|_| ActorError::Arithmetic)?;
        Sha1Hash::from_bytes(truncated)
    };
    let trackers: Vec<Vec<String>> = magnet
        .trackers
        .iter()
        .cloned()
        .map(|tracker| vec![tracker])
        .collect();
    let peers = discover_with_dht(
        &trackers,
        record,
        services,
        info_hash,
        0,
        true,
        AnnounceEvent::Started,
    )
    .await?;
    acquire_metadata_from_peers(record, &magnet, info_hash, peers, services, cancellation).await
}

async fn acquire_metadata_from_peers(
    record: &mut TorrentRecord,
    magnet: &Magnet,
    info_hash: Sha1Hash,
    peers: Vec<SocketAddr>,
    services: &Services,
    cancellation: &CancellationToken,
) -> Result<(), ActorError> {
    let mut last_error = None;
    for address in peers {
        cancelled(cancellation)?;
        match fetch_metadata(address, info_hash, services, cancellation).await {
            Ok((info, mut peer)) => {
                let validated =
                    validate_acquired_metadata(&info, &mut peer, magnet, services).await;
                peer.shutdown();
                let (raw, parsed) = match validated {
                    Ok(validated) => validated,
                    Err(ActorError::Cancelled) => return Err(ActorError::Cancelled),
                    Err(error) => {
                        last_error = Some(error);
                        continue;
                    }
                };
                record.name = parsed.name;
                record.total_length = parsed.total_length;
                record.v1_info_hash = parsed.v1_info_hash;
                record.v2_info_hash = parsed.v2_info_hash;
                record.raw_metainfo = raw;
                replace_record(services, record.clone()).await?;
                return Ok(());
            }
            Err(ActorError::Cancelled) => return Err(ActorError::Cancelled),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or(ActorError::NoPeers))
}

async fn validate_acquired_metadata(
    info: &[u8],
    peer: &mut PeerConnection,
    magnet: &Magnet,
    services: &Services,
) -> Result<(Vec<u8>, Metainfo), ActorError> {
    if let Some(expected) = magnet.v1_info_hash {
        let actual_v1: [u8; 20] = Sha1::digest(info).into();
        if Sha1Hash::from_bytes(actual_v1) != expected {
            return Err(ActorError::Metadata(
                "peer returned metadata with the wrong SHA-1 info hash".to_owned(),
            ));
        }
    }
    let limits = BencodeLimits {
        input_bytes: services.metainfo_limit,
        byte_string_bytes: services.metainfo_limit,
        ..BencodeLimits::default()
    };
    let raw_without_layers = wrap_info_dictionary(info, magnet.trackers.first(), &BTreeMap::new());
    let preliminary = Metainfo::parse_allow_missing_piece_layers(&raw_without_layers, limits)
        .map_err(|error| ActorError::Metainfo(error.to_string()))?;
    let layers = fetch_piece_layers(peer, &preliminary, services.metainfo_limit).await?;
    let raw = wrap_info_dictionary(info, magnet.trackers.first(), &layers);
    let parsed =
        Metainfo::parse(&raw, limits).map_err(|error| ActorError::Metainfo(error.to_string()))?;
    if parsed.v1_info_hash != magnet.v1_info_hash {
        return Err(ActorError::Metadata(
            "parsed metadata does not match the magnet v1 identity".to_owned(),
        ));
    }
    if let Some(expected) = magnet.v2_info_hash {
        let actual: [u8; 32] = Sha256::digest(info).into();
        if dendrite_core::Sha256Hash::from_bytes(actual) != expected {
            return Err(ActorError::Metadata(
                "parsed metadata does not match the magnet v2 identity".to_owned(),
            ));
        }
    }
    Ok((raw, parsed))
}

async fn fetch_metadata(
    address: SocketAddr,
    info_hash: Sha1Hash,
    services: &Services,
    cancellation: &CancellationToken,
) -> Result<(Vec<u8>, PeerConnection), ActorError> {
    let mut reserved = [0_u8; 8];
    reserved[5] |= 0x10;
    let mut peer = connect_outgoing_peer(
        address,
        Handshake {
            reserved,
            info_hash,
            peer_id: services.peer_id,
        },
        PeerCodecLimits {
            extension_bytes: services.metainfo_limit,
            frame_bytes: services.metainfo_limit.saturating_add(64),
            ..PeerCodecLimits::default()
        },
        services,
    )
    .await
    .map_err(|error| ActorError::Metadata(error.to_string()))?;
    peer.send(PeerMessage::Extended {
        extension_id: 0,
        payload: encode_extension_handshake(None),
    })
    .await
    .map_err(|error| ActorError::Metadata(error.to_string()))?;

    let (remote_extension_id, total_size) =
        negotiate_metadata(&mut peer, services.metainfo_limit, cancellation).await?;
    let piece_count = total_size.div_ceil(METADATA_BLOCK_BYTES);
    let mut metadata = Vec::with_capacity(total_size);
    for piece in 0..piece_count {
        cancelled(cancellation)?;
        let piece = u32::try_from(piece).map_err(|_| ActorError::Arithmetic)?;
        peer.send(PeerMessage::Extended {
            extension_id: remote_extension_id,
            payload: encode_metadata_request(piece),
        })
        .await
        .map_err(|error| ActorError::Metadata(error.to_string()))?;
        let remaining = total_size.saturating_sub(metadata.len());
        let block = receive_metadata_piece(
            &mut peer,
            piece,
            total_size,
            remaining.min(METADATA_BLOCK_BYTES),
            services.metainfo_limit,
        )
        .await?;
        metadata.extend_from_slice(&block);
    }
    Ok((metadata, peer))
}

async fn negotiate_metadata(
    peer: &mut PeerConnection,
    metainfo_limit: usize,
    cancellation: &CancellationToken,
) -> Result<(u8, usize), ActorError> {
    loop {
        cancelled(cancellation)?;
        match next_peer_event(peer).await? {
            PeerEvent::Message(PeerMessage::Extended {
                extension_id: 0,
                payload,
            }) => {
                let handshake = decode_extension_handshake(&payload, metainfo_limit)
                    .map_err(|error| ActorError::Metadata(error.to_string()))?;
                let extension_id = handshake.metadata_extension_id.ok_or_else(|| {
                    ActorError::Metadata("peer does not advertise ut_metadata".to_owned())
                })?;
                let size = handshake.metadata_size.ok_or_else(|| {
                    ActorError::Metadata("peer did not provide metadata_size".to_owned())
                })?;
                if size == 0 {
                    return Err(ActorError::Metadata("metadata_size is zero".to_owned()));
                }
                return Ok((extension_id, size));
            }
            PeerEvent::Disconnected | PeerEvent::Failed(_) => {
                return Err(ActorError::Metadata(
                    "peer disconnected during extension handshake".to_owned(),
                ));
            }
            _ => {}
        }
    }
}

async fn receive_metadata_piece(
    peer: &mut PeerConnection,
    piece: u32,
    total_size: usize,
    expected_length: usize,
    metainfo_limit: usize,
) -> Result<Bytes, ActorError> {
    loop {
        match next_peer_event(peer).await? {
            PeerEvent::Message(PeerMessage::Extended {
                extension_id: LOCAL_METADATA_EXTENSION_ID,
                payload,
            }) => match decode_metadata_message(&payload, metainfo_limit)
                .map_err(|error| ActorError::Metadata(error.to_string()))?
            {
                MetadataMessage::Data {
                    piece: response_piece,
                    total_size: response_size,
                    block,
                } if response_piece == piece
                    && response_size == total_size
                    && block.len() == expected_length =>
                {
                    return Ok(block);
                }
                MetadataMessage::Reject {
                    piece: response_piece,
                } if response_piece == piece => {
                    return Err(ActorError::Metadata(
                        "peer rejected a metadata request".to_owned(),
                    ));
                }
                _ => {
                    return Err(ActorError::Metadata(
                        "peer returned an unexpected metadata message".to_owned(),
                    ));
                }
            },
            PeerEvent::Disconnected | PeerEvent::Failed(_) => {
                return Err(ActorError::Metadata(
                    "peer disconnected during metadata exchange".to_owned(),
                ));
            }
            _ => {}
        }
    }
}

async fn fetch_piece_layers(
    peer: &mut PeerConnection,
    metainfo: &Metainfo,
    metainfo_limit: usize,
) -> Result<BTreeMap<dendrite_core::Sha256Hash, Vec<dendrite_core::Sha256Hash>>, ActorError> {
    let mut layers = BTreeMap::new();
    for file in metainfo
        .files
        .iter()
        .filter(|file| !file.padding && file.length > u64::from(metainfo.piece_length.get()))
    {
        let root = file
            .pieces_root
            .ok_or_else(|| ActorError::Metainfo("large v2 file has no pieces root".to_owned()))?;
        if layers.contains_key(&root) {
            continue;
        }
        let count = usize::try_from(file.length.div_ceil(u64::from(metainfo.piece_length.get())))
            .map_err(|_| ActorError::Arithmetic)?;
        let block_bytes = u32::try_from(BLOCK_BYTES).map_err(|_| ActorError::Arithmetic)?;
        let base_layer = (metainfo.piece_length.get() / block_bytes).ilog2();
        let tree_width = count.next_power_of_two();
        let mut hashes = Vec::with_capacity(count);
        while hashes.len() < count {
            let index = hashes.len();
            let remaining = count - index;
            let request_length = if remaining > 512 {
                512
            } else {
                remaining.next_power_of_two().max(2)
            };
            let proof_layers = tree_width.ilog2();
            let request = HashRequest {
                pieces_root: root,
                base_layer,
                index: u32::try_from(index).map_err(|_| ActorError::Arithmetic)?,
                length: u32::try_from(request_length).map_err(|_| ActorError::Arithmetic)?,
                proof_layers,
            };
            peer.send(PeerMessage::HashRequest(request))
                .await
                .map_err(|error| ActorError::Metadata(error.to_string()))?;
            let needed = remaining.min(request_length);
            let received = receive_hash_response(peer, request, needed, metainfo_limit).await?;
            hashes.extend(received);
        }
        layers.insert(root, hashes);
    }
    Ok(layers)
}

async fn receive_hash_response(
    peer: &mut PeerConnection,
    request: HashRequest,
    needed: usize,
    metainfo_limit: usize,
) -> Result<Vec<dendrite_core::Sha256Hash>, ActorError> {
    loop {
        match next_peer_event(peer).await? {
            PeerEvent::Message(PeerMessage::Hashes {
                request: response,
                hashes,
            }) if response == request => {
                if hashes.len() > metainfo_limit || hashes.len() < needed.saturating_mul(32) {
                    return Err(ActorError::Metadata(
                        "peer returned an invalid v2 hash response length".to_owned(),
                    ));
                }
                return hashes
                    .chunks_exact(32)
                    .take(needed)
                    .map(|chunk| {
                        let hash: [u8; 32] =
                            chunk.try_into().map_err(|_| ActorError::Arithmetic)?;
                        Ok(dendrite_core::Sha256Hash::from_bytes(hash))
                    })
                    .collect();
            }
            PeerEvent::Message(PeerMessage::HashReject(response)) if response == request => {
                return Err(ActorError::Metadata(
                    "peer rejected a v2 hash request".to_owned(),
                ));
            }
            PeerEvent::Disconnected | PeerEvent::Failed(_) => {
                return Err(ActorError::Metadata(
                    "peer disconnected during v2 hash exchange".to_owned(),
                ));
            }
            _ => {}
        }
    }
}

fn wrap_info_dictionary(
    info: &[u8],
    tracker: Option<&String>,
    piece_layers: &BTreeMap<dendrite_core::Sha256Hash, Vec<dendrite_core::Sha256Hash>>,
) -> Vec<u8> {
    let mut metainfo = vec![b'd'];
    if let Some(tracker) = tracker {
        metainfo.extend_from_slice(format!("8:announce{}:{tracker}", tracker.len()).as_bytes());
    }
    metainfo.extend_from_slice(b"4:info");
    metainfo.extend_from_slice(info);
    if !piece_layers.is_empty() {
        metainfo.extend_from_slice(b"12:piece layersd");
        for (root, hashes) in piece_layers {
            metainfo.extend_from_slice(b"32:");
            metainfo.extend_from_slice(root.as_bytes());
            metainfo.extend_from_slice((hashes.len() * 32).to_string().as_bytes());
            metainfo.push(b':');
            for hash in hashes {
                metainfo.extend_from_slice(hash.as_bytes());
            }
        }
        metainfo.push(b'e');
    }
    metainfo.push(b'e');
    metainfo
}

async fn download(
    metainfo: &Metainfo,
    record: &mut TorrentRecord,
    services: &Services,
    cancellation: &CancellationToken,
) -> Result<(), ActorError> {
    let info_hash = wire_info_hash(metainfo)?;
    update_record_state(record, TorrentState::Starting, services).await?;
    update_record_state(record, TorrentState::Downloading, services).await?;
    let peer_result = if metainfo.web_seeds.is_empty() {
        download_from_peers_with_retry(metainfo, record, services, cancellation, info_hash).await
    } else {
        download_from_peer_round(
            metainfo,
            record,
            services,
            cancellation,
            info_hash,
            AnnounceEvent::Started,
        )
        .await
    };
    if !all_complete(&record.completed_pieces, piece_count(metainfo)?) {
        if metainfo.web_seeds.is_empty() {
            peer_result?;
        }
        download_from_web_seeds(metainfo, record, services, cancellation).await?;
    }
    update_record_state(record, TorrentState::Seeding, services).await
}

async fn download_from_peers_with_retry(
    metainfo: &Metainfo,
    record: &mut TorrentRecord,
    services: &Services,
    cancellation: &CancellationToken,
    info_hash: Sha1Hash,
) -> Result<(), ActorError> {
    let mut delay = SWARM_RETRY_MIN;
    let mut announce_event = AnnounceEvent::Started;
    loop {
        cancelled(cancellation)?;
        let downloaded_before = record.downloaded;
        match download_from_peer_round(
            metainfo,
            record,
            services,
            cancellation,
            info_hash,
            announce_event,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(ActorError::Cancelled) => return Err(ActorError::Cancelled),
            Err(error) if retryable_peer_failure(&error) => {
                debug!(%error, ?delay, "peer swarm exhausted; retrying discovery");
            }
            Err(error) => return Err(error),
        }
        if all_complete(&record.completed_pieces, piece_count(metainfo)?) {
            return Ok(());
        }
        announce_event = AnnounceEvent::None;
        if record.downloaded > downloaded_before {
            delay = SWARM_RETRY_MIN;
        } else {
            delay = delay.saturating_mul(2).min(SWARM_RETRY_MAX);
        }
        tokio::select! {
            () = cancellation.cancelled() => return Err(ActorError::Cancelled),
            () = tokio::time::sleep(delay) => {}
        }
    }
}

fn retryable_peer_failure(error: &ActorError) -> bool {
    matches!(
        error,
        ActorError::NoTracker | ActorError::NoPeers | ActorError::Peer(_)
    )
}

async fn download_from_peer_round(
    metainfo: &Metainfo,
    record: &mut TorrentRecord,
    services: &Services,
    cancellation: &CancellationToken,
    info_hash: Sha1Hash,
    announce_event: AnnounceEvent,
) -> Result<(), ActorError> {
    let peers = discover_peers(metainfo, record, services, info_hash, announce_event).await?;
    run_peer_swarm(peers, info_hash, metainfo, record, services, cancellation).await
}

async fn download_from_web_seeds(
    metainfo: &Metainfo,
    record: &mut TorrentRecord,
    services: &Services,
    cancellation: &CancellationToken,
) -> Result<(), ActorError> {
    let mut seeds: Vec<Url> = metainfo
        .web_seeds
        .iter()
        .filter_map(|seed| Url::parse(seed).ok())
        .filter(|seed| matches!(seed.scheme(), "http" | "https"))
        .collect();
    if seeds.is_empty() {
        return Err(ActorError::WebSeed(
            "metainfo has no valid HTTP(S) web-seed URL".to_owned(),
        ));
    }
    for piece in 0..piece_count(metainfo)? {
        cancelled(cancellation)?;
        if bit_is_set(&record.completed_pieces, piece) {
            continue;
        }
        let mut accepted = None;
        let mut index = 0_usize;
        let mut last_error = None;
        while index < seeds.len() {
            match fetch_web_seed_piece(
                &seeds[index],
                metainfo,
                piece,
                services.allow_private_web_seeds,
            )
            .await
            {
                Ok(data) if verify_piece(metainfo, piece, &data)? => {
                    accepted = Some(data);
                    break;
                }
                Ok(_) => {
                    last_error =
                        Some("web seed returned data that failed piece verification".to_owned());
                    seeds.remove(index);
                }
                Err(error) => {
                    last_error = Some(error.to_string());
                    index += 1;
                }
            }
        }
        let data = accepted.ok_or_else(|| {
            ActorError::WebSeed(last_error.unwrap_or_else(|| "all web seeds failed".to_owned()))
        })?;
        write_piece(metainfo, piece, data, &services.storage).await?;
        set_bit(&mut record.completed_pieces, piece);
        record.downloaded = completed_bytes(metainfo, &record.completed_pieces)?;
        replace_record(services, record.clone()).await?;
    }
    Ok(())
}

async fn fetch_web_seed_piece(
    seed: &Url,
    metainfo: &Metainfo,
    piece: usize,
    allow_private: bool,
) -> Result<Bytes, ActorError> {
    let client = web_seed_client(seed, allow_private).await?;
    let length = piece_length(metainfo, piece)?;
    let mut output = BytesMut::with_capacity(length);
    if metainfo.v1_piece_hashes.is_empty() {
        let (file, _, offset) = v2_piece_location(metainfo, piece)?;
        if file.padding {
            output.resize(length, 0);
        } else {
            let url = web_seed_file_url(seed, metainfo, file)?;
            output.extend_from_slice(
                &fetch_http_range(&client, url, offset, length, file.length).await?,
            );
        }
    } else {
        let start = piece_start(metainfo, piece)?;
        for segment in file_segments(wire_files(metainfo), start, length)? {
            if segment.file.padding {
                output.resize(output.len().saturating_add(segment.length), 0);
            } else {
                let url = web_seed_file_url(seed, metainfo, segment.file)?;
                output.extend_from_slice(
                    &fetch_http_range(
                        &client,
                        url,
                        segment.file_offset,
                        segment.length,
                        segment.file.length,
                    )
                    .await?,
                );
            }
        }
    }
    if output.len() != length {
        return Err(ActorError::WebSeed(
            "web seed assembled a piece with the wrong length".to_owned(),
        ));
    }
    Ok(output.freeze())
}

async fn web_seed_client(seed: &Url, allow_private: bool) -> Result<Client, ActorError> {
    let host = seed
        .host_str()
        .ok_or_else(|| ActorError::WebSeed("web seed URL has no host".to_owned()))?;
    let port = seed
        .port_or_known_default()
        .ok_or_else(|| ActorError::WebSeed("web seed URL has no usable port".to_owned()))?;
    let mut builder = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(PEER_MESSAGE_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none());
    if !allow_private {
        let addresses: Vec<_> = tokio::net::lookup_host((host, port))
            .await
            .map_err(|error| ActorError::WebSeed(format!("web seed DNS failed: {error}")))?
            .collect();
        if addresses.is_empty() {
            return Err(ActorError::WebSeed(
                "web seed DNS returned no addresses".to_owned(),
            ));
        }
        if addresses
            .iter()
            .any(|address| !public_web_seed_ip(address.ip()))
        {
            return Err(ActorError::WebSeed(
                "web seed resolved to a private, local, or multicast address".to_owned(),
            ));
        }
        builder = builder.resolve_to_addrs(host, &addresses);
    }
    builder
        .build()
        .map_err(|error| ActorError::WebSeed(error.to_string()))
}

fn public_web_seed_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ip) => public_web_seed_ipv4(ip),
        std::net::IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return public_web_seed_ipv4(mapped);
            }
            let segments = ip.segments();
            !(ip.is_loopback()
                || ip.is_multicast()
                || ip.is_unspecified()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || (segments[0] == 0x2001 && segments[1] == 0x0db8))
                && !(segments[0] == 0x2001 && segments[1] == 0x0002)
                && segments[0] & 0xffc0 != 0xfec0
        }
    }
}

fn public_web_seed_ipv4(ip: std::net::Ipv4Addr) -> bool {
    let [first, second, third, _] = ip.octets();
    !(first == 0
        || first == 10
        || first == 127
        || (first == 100 && (64..=127).contains(&second))
        || (first == 169 && second == 254)
        || (first == 172 && (16..=31).contains(&second))
        || (first == 192 && second == 0 && third == 0)
        || (first == 192 && second == 0 && third == 2)
        || (first == 192 && second == 168)
        || (first == 198 && (second == 18 || second == 19))
        || (first == 198 && second == 51 && third == 100)
        || (first == 203 && second == 0 && third == 113)
        || first >= 224)
}

fn web_seed_file_url(seed: &Url, metainfo: &Metainfo, file: &FileEntry) -> Result<Url, ActorError> {
    let content_files = metainfo.files.iter().filter(|file| !file.padding).count();
    if content_files == 1 && !seed.path().ends_with('/') {
        return Ok(seed.clone());
    }
    if !seed.path().ends_with('/') {
        return Err(ActorError::WebSeed(
            "multi-file web seed URL must end with '/'".to_owned(),
        ));
    }
    let mut url = seed.clone();
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|()| ActorError::WebSeed("web seed URL cannot contain paths".to_owned()))?;
        segments.pop_if_empty();
        for component in file.path.components() {
            segments.push(component);
        }
    }
    Ok(url)
}

async fn fetch_http_range(
    client: &Client,
    url: Url,
    offset: u64,
    length: usize,
    file_length: u64,
) -> Result<Bytes, ActorError> {
    let length_u64 = u64::try_from(length).map_err(|_| ActorError::Arithmetic)?;
    let end = offset
        .checked_add(length_u64)
        .and_then(|end| end.checked_sub(1))
        .ok_or(ActorError::Arithmetic)?;
    let mut response = client
        .get(url)
        .header(ACCEPT_ENCODING, "identity")
        .header(RANGE, format!("bytes={offset}-{end}"))
        .send()
        .await
        .map_err(|error| ActorError::WebSeed(error.to_string()))?;
    let full_file = offset == 0 && length_u64 == file_length;
    if response.status() != StatusCode::PARTIAL_CONTENT
        && !(full_file && response.status() == StatusCode::OK)
    {
        return Err(ActorError::WebSeed(format!(
            "web seed returned HTTP {}",
            response.status()
        )));
    }
    if response
        .headers()
        .get(CONTENT_ENCODING)
        .is_some_and(|encoding| encoding.as_bytes() != b"identity")
    {
        return Err(ActorError::WebSeed(
            "web seed returned encoded content despite an identity request".to_owned(),
        ));
    }
    if response.status() == StatusCode::PARTIAL_CONTENT {
        let expected = format!("bytes {offset}-{end}/{file_length}");
        if response
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            != Some(expected.as_str())
        {
            return Err(ActorError::WebSeed(
                "web seed returned a missing or mismatched Content-Range".to_owned(),
            ));
        }
    }
    if response
        .content_length()
        .is_some_and(|actual| actual != length_u64)
    {
        return Err(ActorError::WebSeed(
            "web seed declared the wrong range length".to_owned(),
        ));
    }
    let mut output = BytesMut::with_capacity(length);
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| ActorError::WebSeed(error.to_string()))?
    {
        if output.len().saturating_add(chunk.len()) > length {
            return Err(ActorError::WebSeed(
                "web seed response exceeded the requested range".to_owned(),
            ));
        }
        output.extend_from_slice(&chunk);
    }
    if output.len() != length {
        return Err(ActorError::WebSeed(
            "web seed returned a short range".to_owned(),
        ));
    }
    Ok(output.freeze())
}

async fn run_peer_swarm(
    peers: Vec<SocketAddr>,
    info_hash: Sha1Hash,
    metainfo: &Metainfo,
    record: &mut TorrentRecord,
    services: &Services,
    cancellation: &CancellationToken,
) -> Result<(), ActorError> {
    let pieces = piece_count(metainfo)?;
    let worker_count = peers.len().min(services.per_torrent_peer_limit);
    let event_capacity = worker_count.saturating_mul(4).max(1);
    let (event_sender, mut events) = mpsc::channel(event_capacity);
    let mut picker = PiecePicker::new(pieces, 4);
    for index in 0..pieces {
        if bit_is_set(&record.completed_pieces, index) {
            picker
                .mark_complete(index)
                .map_err(|error| ActorError::Peer(error.to_string()))?;
        }
    }
    let mut swarm = SwarmState {
        workers: HashMap::with_capacity(worker_count),
        assignments: HashMap::new(),
        picker,
        connecting: 0,
        last_error: None,
        addresses: HashSet::with_capacity(worker_count),
        next_worker: 0,
        event_sender,
        info_hash,
        piece_count: pieces,
        services: services.clone(),
        cancellation: cancellation.child_token(),
        allow_pex: !metainfo.private,
        torrent_id: record.id,
        peer_limit: services.per_torrent_peer_limit,
    };
    for address in peers.into_iter().take(worker_count) {
        spawn_swarm_worker(&mut swarm, address);
    }
    loop {
        if cancellation.is_cancelled() {
            shutdown_swarm(&swarm);
            return Err(ActorError::Cancelled);
        }
        if all_complete(&record.completed_pieces, pieces) {
            shutdown_swarm(&swarm);
            return Ok(());
        }
        let scheduled = schedule_pieces(
            &mut swarm.workers,
            &mut swarm.assignments,
            &mut swarm.picker,
            metainfo,
        )?;
        if scheduled == 0
            && swarm.connecting == 0
            && swarm.assignments.is_empty()
            && swarm.workers.is_empty()
        {
            shutdown_swarm(&swarm);
            return Err(swarm
                .last_error
                .map_or(ActorError::NoPeers, ActorError::Peer));
        }
        let event = tokio::select! {
            () = cancellation.cancelled() => {
                shutdown_swarm(&swarm);
                return Err(ActorError::Cancelled);
            },
            event = events.recv() => event.ok_or(ActorError::NoPeers)?,
        };
        handle_worker_event(&mut swarm, event, metainfo, record, services).await?;
    }
}

fn shutdown_swarm(swarm: &SwarmState) {
    swarm.cancellation.cancel();
    stop_workers(&swarm.workers, &swarm.assignments);
}

fn spawn_swarm_worker(swarm: &mut SwarmState, address: SocketAddr) {
    spawn_swarm_worker_with_transport(swarm, address, false);
}

fn spawn_swarm_worker_with_transport(swarm: &mut SwarmState, address: SocketAddr, force_utp: bool) {
    if swarm.workers.len() >= swarm.peer_limit || !swarm.addresses.insert(address) {
        return;
    }
    let worker = swarm.next_worker;
    swarm.next_worker = swarm.next_worker.saturating_add(1);
    let (commands, receiver) = mpsc::channel(PEER_COMMAND_CAPACITY);
    swarm.workers.insert(
        worker,
        PeerWorkerHandle {
            commands,
            bitfield: None,
            idle: false,
        },
    );
    swarm.connecting = swarm.connecting.saturating_add(1);
    swarm.services.tasks.spawn(peer_worker(
        PeerWorkerContext {
            worker,
            address,
            info_hash: swarm.info_hash,
            piece_count: swarm.piece_count,
            services: swarm.services.clone(),
            events: swarm.event_sender.clone(),
            cancellation: swarm.cancellation.child_token(),
            allow_pex: swarm.allow_pex,
            force_utp,
            torrent_id: swarm.torrent_id,
        },
        receiver,
    ));
}

fn per_torrent_peer_limit(global_limit: usize) -> usize {
    global_limit.div_ceil(4).clamp(1, ACTIVE_PEER_LIMIT)
}

async fn handle_worker_event(
    swarm: &mut SwarmState,
    event: PeerWorkerEvent,
    metainfo: &Metainfo,
    record: &mut TorrentRecord,
    services: &Services,
) -> Result<(), ActorError> {
    match event {
        PeerWorkerEvent::Ready {
            worker,
            bitfield,
            peers,
        } => {
            swarm.connecting = swarm.connecting.saturating_sub(1);
            swarm
                .picker
                .add_peer_bitfield(&bitfield)
                .map_err(|error| ActorError::Peer(error.to_string()))?;
            if let Some(handle) = swarm.workers.get_mut(&worker) {
                handle.bitfield = Some(bitfield);
                handle.idle = true;
            }
            for address in peers {
                spawn_swarm_worker(swarm, address);
            }
        }
        PeerWorkerEvent::Complete {
            worker,
            piece,
            result,
        } => {
            swarm.assignments.remove(&worker);
            if let Some(handle) = swarm.workers.get_mut(&worker) {
                handle.idle = true;
            }
            apply_piece_result(swarm, worker, piece, result, metainfo, record, services).await?;
        }
        PeerWorkerEvent::Gone { worker, error } => {
            swarm.connecting = swarm.connecting.saturating_sub(usize::from(
                swarm
                    .workers
                    .get(&worker)
                    .is_some_and(|handle| handle.bitfield.is_none()),
            ));
            if let Some((piece, _)) = swarm.assignments.remove(&worker) {
                swarm
                    .picker
                    .mark_request_failed(piece)
                    .map_err(|failure| ActorError::Peer(failure.to_string()))?;
            }
            remove_worker(worker, &mut swarm.workers, &mut swarm.picker)?;
            swarm.last_error = Some(error);
        }
        PeerWorkerEvent::Peers { worker, peers } => {
            if swarm.workers.contains_key(&worker) {
                for address in peers {
                    spawn_swarm_worker(swarm, address);
                }
            }
        }
        PeerWorkerEvent::Have { worker, piece } => {
            let piece = usize::try_from(piece).map_err(|_| ActorError::Arithmetic)?;
            if piece >= swarm.piece_count {
                return Err(ActorError::Peer(
                    "peer announced a piece outside the torrent".to_owned(),
                ));
            }
            let Some(handle) = swarm.workers.get_mut(&worker) else {
                return Ok(());
            };
            let Some(bitfield) = handle.bitfield.as_mut() else {
                return Ok(());
            };
            if !bit_is_set(bitfield, piece) {
                set_bit(bitfield, piece);
                swarm
                    .picker
                    .add_peer_piece(piece)
                    .map_err(|error| ActorError::Peer(error.to_string()))?;
            }
            if bit_is_set(&record.completed_pieces, piece) {
                let _result_ignored = handle.commands.try_send(PeerWorkerCommand::Have {
                    piece: u32::try_from(piece).map_err(|_| ActorError::Arithmetic)?,
                });
            }
        }
        PeerWorkerEvent::HolePunch { worker, address } => {
            if swarm.workers.contains_key(&worker) {
                spawn_swarm_worker_with_transport(swarm, address, true);
            }
        }
    }
    Ok(())
}

async fn discover_peers(
    metainfo: &Metainfo,
    record: &TorrentRecord,
    services: &Services,
    info_hash: Sha1Hash,
    announce_event: AnnounceEvent,
) -> Result<Vec<SocketAddr>, ActorError> {
    discover_with_dht(
        &metainfo.trackers,
        record,
        services,
        info_hash,
        metainfo.total_length.saturating_sub(record.downloaded),
        !metainfo.private,
        announce_event,
    )
    .await
}

async fn discover_with_dht(
    trackers: &[Vec<String>],
    record: &TorrentRecord,
    services: &Services,
    info_hash: Sha1Hash,
    left: u64,
    allow_dht: bool,
    announce_event: AnnounceEvent,
) -> Result<Vec<SocketAddr>, ActorError> {
    let mut peers = HashSet::new();
    let tracker_error =
        match discover_tracker_peers(trackers, record, services, info_hash, left, announce_event)
            .await
        {
            Ok(discovered) => {
                peers.extend(discovered);
                None
            }
            Err(error) => Some(error),
        };
    let mut dht_error = None;
    if allow_dht && !services.dht_bootstrap.is_empty() {
        let client = if let Some(client) = &services.dht {
            client.clone()
        } else {
            DhtClient::new(128, 65_507, Duration::from_secs(2))
                .map_err(|error| ActorError::Peer(error.to_string()))?
        };
        match tokio::time::timeout(
            DHT_DISCOVERY_TIMEOUT,
            client.get_peers(info_hash, &services.dht_bootstrap),
        )
        .await
        {
            Ok(Ok(discovered)) => {
                debug!(peers = discovered.len(), "DHT peer discovery succeeded");
                peers.extend(discovered);
            }
            Ok(Err(error)) => dht_error = Some(error.to_string()),
            Err(_) => dht_error = Some("DHT lookup timed out".to_owned()),
        }
    }
    if peers.is_empty() && allow_dht {
        peers.extend(discover_lsd_peer(info_hash, services).await);
    }
    if !peers.is_empty() {
        return Ok(peers.into_iter().collect());
    }
    if let Some(error) = dht_error {
        Err(ActorError::Peer(format!(
            "tracker discovery failed ({}); DHT failed ({error}); local discovery found no peers",
            tracker_error.map_or_else(|| "no peers".to_owned(), |error| error.to_string())
        )))
    } else {
        Err(tracker_error.unwrap_or(ActorError::NoPeers))
    }
}

async fn discover_tracker_peers(
    trackers: &[Vec<String>],
    record: &TorrentRecord,
    services: &Services,
    info_hash: Sha1Hash,
    left: u64,
    announce_event: AnnounceEvent,
) -> Result<Vec<SocketAddr>, ActorError> {
    let tracker = HttpTrackerClient::new(services.tracker_response_limit)
        .map_err(|error| ActorError::Peer(error.to_string()))?;
    let udp_tracker = UdpTrackerClient::new(services.tracker_response_limit)
        .map_err(|error| ActorError::Peer(error.to_string()))?;
    let mut peers = HashSet::new();
    let peer_id = services.peer_id.as_bytes();
    let request = TrackerRequest {
        info_hash,
        peer_id: services.peer_id,
        port: services.advertised_peer_port.load(Ordering::Acquire),
        uploaded: record.uploaded,
        downloaded: record.downloaded,
        left,
        event: announce_event,
        numwant: PEER_LIMIT_PER_ANNOUNCE,
        key: u32::from_be_bytes([peer_id[16], peer_id[17], peer_id[18], peer_id[19]]),
        support_crypto: !matches!(services.encryption, EncryptionPolicy::Disabled),
    };
    let mut attempted = false;
    for tier in trackers {
        for tracker_url in tier {
            let Ok(url) = Url::parse(tracker_url) else {
                continue;
            };
            let announce = match url.scheme() {
                "http" | "https" => {
                    attempted = true;
                    tracker
                        .announce(&url, request)
                        .await
                        .map_err(|error| error.to_string())
                }
                "udp" => {
                    attempted = true;
                    udp_tracker
                        .announce(&url, request)
                        .await
                        .map_err(|error| error.to_string())
                }
                _ => continue,
            };
            match announce {
                Ok(announce) => {
                    debug!(
                        tracker = %url,
                        peers = announce.peers.len(),
                        "tracker announce succeeded"
                    );
                    peers.extend(announce.peers);
                }
                Err(error) => debug!(tracker = %url, %error, "tracker announce failed"),
            }
        }
    }
    if !attempted {
        return Err(ActorError::NoTracker);
    }
    if peers.is_empty() {
        return Err(ActorError::NoPeers);
    }
    Ok(peers.into_iter().collect())
}

async fn peer_worker(context: PeerWorkerContext, mut commands: mpsc::Receiver<PeerWorkerCommand>) {
    let Ok(_permit) = context.services.peer_slots.clone().acquire_owned().await else {
        return;
    };
    let Some(mut peer) = establish_peer_worker(&context).await else {
        return;
    };
    let _connection = track_connection(&context.services, context.torrent_id);
    loop {
        let input = tokio::select! {
            () = context.cancellation.cancelled() => break,
            command = commands.recv() => match command {
                Some(command) => PeerWorkerInput::Command(command),
                None => break,
            },
            event = next_peer_event_with_timeout(
                &mut peer,
                context.services.peer_message_timeout.saturating_mul(4),
            ) => match event {
                Ok(event) => PeerWorkerInput::Event(event),
                Err(error) => {
                    let _result_ignored = context.events.send(PeerWorkerEvent::Gone {
                        worker: context.worker,
                        error: error.to_string(),
                    }).await;
                    break;
                }
            },
        };
        match input {
            PeerWorkerInput::Command(PeerWorkerCommand::Download {
                piece,
                length,
                cancellation: piece_cancellation,
            }) => {
                let result = match download_piece_blocks(
                    &mut peer,
                    piece,
                    length,
                    &piece_cancellation,
                    context.services.peer_message_timeout,
                    PeerEventForwarder {
                        events: &context.events,
                        worker: context.worker,
                        allow_extensions: context.allow_pex,
                    },
                )
                .await
                {
                    Ok(data) => PieceResult::Data(data),
                    Err(ActorError::Cancelled) => PieceResult::Cancelled,
                    Err(error) => {
                        debug!(
                            address = %context.address,
                            piece,
                            %error,
                            "piece download failed"
                        );
                        PieceResult::Failed(error.to_string())
                    }
                };
                let failed = matches!(result, PieceResult::Failed(_));
                if context
                    .events
                    .send(PeerWorkerEvent::Complete {
                        worker: context.worker,
                        piece,
                        result,
                    })
                    .await
                    .is_err()
                    || failed
                {
                    break;
                }
            }
            PeerWorkerInput::Command(PeerWorkerCommand::Have { piece }) => {
                debug!(address = %context.address, piece, "announcing completed piece to peer");
                if let Err(error) = peer.send(PeerMessage::Have(piece)).await {
                    let _result_ignored = context
                        .events
                        .send(PeerWorkerEvent::Gone {
                            worker: context.worker,
                            error: error.to_string(),
                        })
                        .await;
                    break;
                }
            }
            PeerWorkerInput::Command(PeerWorkerCommand::Shutdown) => break,
            PeerWorkerInput::Event(event) => {
                if !forward_idle_peer_event(&context, event).await {
                    break;
                }
            }
        }
    }
    peer.shutdown();
}

async fn establish_peer_worker(context: &PeerWorkerContext) -> Option<PeerConnection> {
    let result = connect_peer_worker(
        context.address,
        context.info_hash,
        context.piece_count,
        &context.services,
        context.allow_pex,
        context.force_utp,
    )
    .await;
    let (peer, bitfield, peers) = match result {
        Ok(ready) => ready,
        Err(error) => {
            debug!(address = %context.address, %error, "peer connection failed");
            let _result_ignored = context
                .events
                .send(PeerWorkerEvent::Gone {
                    worker: context.worker,
                    error: error.to_string(),
                })
                .await;
            return None;
        }
    };
    debug!(address = %context.address, "peer connection ready");
    if context
        .events
        .send(PeerWorkerEvent::Ready {
            worker: context.worker,
            bitfield,
            peers,
        })
        .await
        .is_err()
    {
        return None;
    }
    Some(peer)
}

async fn forward_idle_peer_event(context: &PeerWorkerContext, event: PeerEvent) -> bool {
    let worker_event = match event {
        PeerEvent::Message(PeerMessage::Have(piece)) => {
            debug!(address = %context.address, piece, "peer announced piece availability");
            Some(PeerWorkerEvent::Have {
                worker: context.worker,
                piece,
            })
        }
        PeerEvent::Message(PeerMessage::Extended {
            extension_id: LOCAL_PEX_EXTENSION_ID,
            payload,
        }) if context.allow_pex => match pex_addresses(&payload) {
            Ok(peers) => Some(PeerWorkerEvent::Peers {
                worker: context.worker,
                peers,
            }),
            Err(error) => return report_peer_gone(context, error.to_string()).await,
        },
        PeerEvent::Message(PeerMessage::Extended {
            extension_id: LOCAL_HOLEPUNCH_EXTENSION_ID,
            payload,
        }) if context.allow_pex => match decode_holepunch_message(&payload) {
            Ok(message) if message.kind == HolePunchKind::Connect => {
                Some(PeerWorkerEvent::HolePunch {
                    worker: context.worker,
                    address: message.address,
                })
            }
            Ok(_) => None,
            Err(error) => return report_peer_gone(context, error.to_string()).await,
        },
        PeerEvent::Disconnected => {
            return report_peer_gone(context, "peer disconnected".to_owned()).await;
        }
        PeerEvent::Failed(error) => return report_peer_gone(context, error).await,
        _ => None,
    };
    match worker_event {
        Some(event) => context.events.send(event).await.is_ok(),
        None => true,
    }
}

async fn report_peer_gone(context: &PeerWorkerContext, error: String) -> bool {
    let _result_ignored = context
        .events
        .send(PeerWorkerEvent::Gone {
            worker: context.worker,
            error,
        })
        .await;
    false
}

async fn connect_peer_worker(
    address: SocketAddr,
    info_hash: Sha1Hash,
    piece_count: usize,
    services: &Services,
    allow_pex: bool,
    force_utp: bool,
) -> Result<(PeerConnection, Vec<u8>, Vec<SocketAddr>), ActorError> {
    let mut reserved = [0_u8; 8];
    reserved[5] |= 0x10;
    reserved[7] |= 0x01;
    let handshake = Handshake {
        reserved,
        info_hash,
        peer_id: services.peer_id,
    };
    let mut peer = if force_utp {
        services
            .utp
            .as_ref()
            .ok_or_else(|| ActorError::Peer("hole punch requires a uTP endpoint".to_owned()))?
            .connect_peer(address, handshake, PeerCodecLimits::default())
            .await
            .map_err(|error| ActorError::Peer(error.to_string()))?
    } else {
        connect_outgoing_peer(address, handshake, PeerCodecLimits::default(), services)
            .await
            .map_err(|error| ActorError::Peer(error.to_string()))?
    };
    if allow_pex && peer.remote_supports_extensions() {
        peer.send(PeerMessage::Extended {
            extension_id: 0,
            payload: encode_extension_handshake(None),
        })
        .await
        .map_err(|error| ActorError::Peer(error.to_string()))?;
    }
    peer.send(PeerMessage::Interested)
        .await
        .map_err(|error| ActorError::Peer(error.to_string()))?;
    let (available, peers) = await_unchoke(&mut peer, piece_count).await?;
    Ok((
        peer,
        available.unwrap_or_else(|| vec![0; piece_count.div_ceil(8)]),
        peers,
    ))
}

fn schedule_pieces(
    workers: &mut HashMap<usize, PeerWorkerHandle>,
    assignments: &mut HashMap<usize, (usize, CancellationToken)>,
    picker: &mut PiecePicker,
    metainfo: &Metainfo,
) -> Result<usize, ActorError> {
    let mut idle: Vec<_> = workers
        .iter()
        .filter_map(|(worker, handle)| handle.idle.then_some(*worker))
        .collect();
    idle.sort_unstable();
    let mut scheduled = 0_usize;
    for worker in idle {
        let Some(handle) = workers.get_mut(&worker) else {
            continue;
        };
        let Some(bitfield) = handle.bitfield.as_deref() else {
            continue;
        };
        let Some(piece) = picker
            .select(bitfield, SelectionMode::RarestFirst)
            .map_err(|error| ActorError::Peer(error.to_string()))?
        else {
            continue;
        };
        let cancellation = CancellationToken::new();
        handle
            .commands
            .try_send(PeerWorkerCommand::Download {
                piece,
                length: piece_length(metainfo, piece)?,
                cancellation: cancellation.clone(),
            })
            .map_err(|error| ActorError::Peer(error.to_string()))?;
        handle.idle = false;
        assignments.insert(worker, (piece, cancellation));
        scheduled += 1;
    }
    Ok(scheduled)
}

async fn apply_piece_result(
    swarm: &mut SwarmState,
    worker: usize,
    piece: usize,
    result: PieceResult,
    metainfo: &Metainfo,
    record: &mut TorrentRecord,
    services: &Services,
) -> Result<(), ActorError> {
    match result {
        PieceResult::Data(_data) if bit_is_set(&record.completed_pieces, piece) => {
            swarm
                .picker
                .mark_complete(piece)
                .map_err(|error| ActorError::Peer(error.to_string()))?;
        }
        PieceResult::Data(data) => {
            if !verify_piece(metainfo, piece, &data)? {
                swarm
                    .picker
                    .mark_request_failed(piece)
                    .map_err(|error| ActorError::Peer(error.to_string()))?;
                swarm.last_error = Some(format!("piece {piece} failed its integrity check"));
                remove_worker(worker, &mut swarm.workers, &mut swarm.picker)?;
                return Ok(());
            }
            write_piece(metainfo, piece, data, &services.storage).await?;
            set_bit(&mut record.completed_pieces, piece);
            swarm
                .picker
                .mark_complete(piece)
                .map_err(|error| ActorError::Peer(error.to_string()))?;
            record.downloaded = completed_bytes(metainfo, &record.completed_pieces)?;
            replace_record(services, record.clone()).await?;
            let wire_piece = u32::try_from(piece).map_err(|_| ActorError::Arithmetic)?;
            for handle in swarm.workers.values() {
                let _result_ignored = handle
                    .commands
                    .try_send(PeerWorkerCommand::Have { piece: wire_piece });
            }
            for (assigned_piece, token) in swarm.assignments.values() {
                if *assigned_piece == piece {
                    token.cancel();
                }
            }
        }
        PieceResult::Cancelled => {
            swarm
                .picker
                .mark_request_failed(piece)
                .map_err(|error| ActorError::Peer(error.to_string()))?;
        }
        PieceResult::Failed(error) => {
            swarm
                .picker
                .mark_request_failed(piece)
                .map_err(|failure| ActorError::Peer(failure.to_string()))?;
            swarm.last_error = Some(error);
            remove_worker(worker, &mut swarm.workers, &mut swarm.picker)?;
        }
    }
    Ok(())
}

fn remove_worker(
    worker: usize,
    workers: &mut HashMap<usize, PeerWorkerHandle>,
    picker: &mut PiecePicker,
) -> Result<(), ActorError> {
    if let Some(handle) = workers.remove(&worker) {
        let _result_ignored = handle.commands.try_send(PeerWorkerCommand::Shutdown);
        if let Some(bitfield) = handle.bitfield {
            picker
                .remove_peer_bitfield(&bitfield)
                .map_err(|error| ActorError::Peer(error.to_string()))?;
        }
    }
    Ok(())
}

fn stop_workers(
    workers: &HashMap<usize, PeerWorkerHandle>,
    assignments: &HashMap<usize, (usize, CancellationToken)>,
) {
    for (_, token) in assignments.values() {
        token.cancel();
    }
    for handle in workers.values() {
        let _result_ignored = handle.commands.try_send(PeerWorkerCommand::Shutdown);
    }
}

async fn download_piece_blocks(
    peer: &mut PeerConnection,
    piece: usize,
    length: usize,
    cancellation: &CancellationToken,
    message_timeout: Duration,
    forwarder: PeerEventForwarder<'_>,
) -> Result<Bytes, ActorError> {
    let piece = u32::try_from(piece).map_err(|_| ActorError::Arithmetic)?;
    let mut output = vec![0_u8; length];
    let mut pending = HashMap::<u32, u32>::new();
    let mut next_begin = 0_usize;
    let mut received = 0_usize;
    fill_request_pipeline(peer, piece, length, &mut next_begin, &mut pending).await?;
    while received < length {
        let event = tokio::select! {
            () = cancellation.cancelled() => {
                cancel_pending(peer, piece, &pending).await;
                return Err(ActorError::Cancelled);
            }
            event = next_peer_event_with_timeout(peer, message_timeout) => event?,
        };
        match event {
            PeerEvent::Message(PeerMessage::Piece {
                piece: response_piece,
                begin,
                block,
            }) if response_piece == piece => {
                let expected = pending.remove(&begin).ok_or_else(|| {
                    ActorError::Peer("peer returned an unsolicited block".to_owned())
                })?;
                if usize::try_from(expected).ok() != Some(block.len()) {
                    return Err(ActorError::Peer(
                        "peer returned a block with the wrong length".to_owned(),
                    ));
                }
                let start = usize::try_from(begin).map_err(|_| ActorError::Arithmetic)?;
                let end = start
                    .checked_add(block.len())
                    .filter(|end| *end <= output.len())
                    .ok_or(ActorError::Arithmetic)?;
                output[start..end].copy_from_slice(&block);
                received = received
                    .checked_add(block.len())
                    .ok_or(ActorError::Arithmetic)?;
                fill_request_pipeline(peer, piece, length, &mut next_begin, &mut pending).await?;
            }
            PeerEvent::Message(PeerMessage::Choke) => {
                return Err(ActorError::Peer(
                    "peer choked outstanding requests".to_owned(),
                ));
            }
            PeerEvent::Disconnected => {
                return Err(ActorError::Peer(
                    "peer disconnected during piece transfer".to_owned(),
                ));
            }
            PeerEvent::Failed(error) => {
                return Err(ActorError::Peer(format!(
                    "peer session failed during piece transfer: {error}"
                )));
            }
            PeerEvent::Message(PeerMessage::Piece { .. }) => {
                return Err(ActorError::Peer(
                    "peer returned a block for another piece".to_owned(),
                ));
            }
            event => forward_transfer_peer_event(event, &forwarder).await?,
        }
    }
    Ok(Bytes::from(output))
}

async fn forward_transfer_peer_event(
    event: PeerEvent,
    forwarder: &PeerEventForwarder<'_>,
) -> Result<(), ActorError> {
    let worker_event = match event {
        PeerEvent::Message(PeerMessage::Have(piece)) => Some(PeerWorkerEvent::Have {
            worker: forwarder.worker,
            piece,
        }),
        PeerEvent::Message(PeerMessage::Extended {
            extension_id: LOCAL_PEX_EXTENSION_ID,
            payload,
        }) if forwarder.allow_extensions => Some(PeerWorkerEvent::Peers {
            worker: forwarder.worker,
            peers: pex_addresses(&payload)?,
        }),
        PeerEvent::Message(PeerMessage::Extended {
            extension_id: LOCAL_HOLEPUNCH_EXTENSION_ID,
            payload,
        }) if forwarder.allow_extensions => {
            let message = decode_holepunch_message(&payload)
                .map_err(|error| ActorError::Peer(error.to_string()))?;
            (message.kind == HolePunchKind::Connect).then_some(PeerWorkerEvent::HolePunch {
                worker: forwarder.worker,
                address: message.address,
            })
        }
        _ => None,
    };
    if let Some(event) = worker_event {
        forwarder
            .events
            .send(event)
            .await
            .map_err(|_| ActorError::Cancelled)?;
    }
    Ok(())
}

async fn fill_request_pipeline(
    peer: &PeerConnection,
    piece: u32,
    length: usize,
    next_begin: &mut usize,
    pending: &mut HashMap<u32, u32>,
) -> Result<(), ActorError> {
    while pending.len() < BLOCK_PIPELINE && *next_begin < length {
        let block_length = BLOCK_BYTES.min(length - *next_begin);
        let begin = u32::try_from(*next_begin).map_err(|_| ActorError::Arithmetic)?;
        let wire_length = u32::try_from(block_length).map_err(|_| ActorError::Arithmetic)?;
        peer.send(PeerMessage::Request(BlockRequest {
            piece,
            begin,
            length: wire_length,
        }))
        .await
        .map_err(|error| ActorError::Peer(error.to_string()))?;
        pending.insert(begin, wire_length);
        *next_begin = next_begin
            .checked_add(block_length)
            .ok_or(ActorError::Arithmetic)?;
    }
    Ok(())
}

async fn cancel_pending(peer: &PeerConnection, piece: u32, pending: &HashMap<u32, u32>) {
    for (begin, length) in pending {
        let _result_ignored = peer
            .send(PeerMessage::Cancel(BlockRequest {
                piece,
                begin: *begin,
                length: *length,
            }))
            .await;
    }
}

async fn connect_outgoing_peer(
    address: SocketAddr,
    handshake: Handshake,
    limits: PeerCodecLimits,
    services: &Services,
) -> Result<PeerConnection, dendrite_net::peer::PeerSessionError> {
    if services.encryption != EncryptionPolicy::Disabled {
        match PeerConnection::connect_encrypted(address, handshake, limits).await {
            Ok(peer) => return Ok(peer),
            Err(error) if services.encryption == EncryptionPolicy::Required => return Err(error),
            Err(error) => {
                debug!(%address, %error, "encrypted peer connection failed; trying plaintext");
            }
        }
    }
    match PeerConnection::connect(address, handshake, limits).await {
        Ok(peer) => Ok(peer),
        Err(tcp_error) => {
            let Some(utp) = &services.utp else {
                return Err(tcp_error);
            };
            utp.connect_peer(address, handshake, limits)
                .await
                .map_err(|_| tcp_error)
        }
    }
}

async fn await_unchoke(
    peer: &mut PeerConnection,
    piece_count: usize,
) -> Result<(Option<Vec<u8>>, Vec<SocketAddr>), ActorError> {
    let expected_bitfield = piece_count.div_ceil(8);
    let mut available = None;
    let mut peers = Vec::new();
    loop {
        match next_peer_event(peer).await? {
            PeerEvent::Message(PeerMessage::Unchoke) => return Ok((available, peers)),
            PeerEvent::Message(PeerMessage::Bitfield(bits)) => {
                if bits.len() != expected_bitfield {
                    return Err(ActorError::Peer(
                        "peer bitfield has wrong length".to_owned(),
                    ));
                }
                if let Some(last) = bits.last()
                    && !piece_count.is_multiple_of(8)
                    && last & (u8::MAX >> (piece_count % 8)) != 0
                {
                    return Err(ActorError::Peer(
                        "peer bitfield sets reserved trailing bits".to_owned(),
                    ));
                }
                available = Some(bits.to_vec());
            }
            PeerEvent::Message(PeerMessage::Have(piece)) => {
                let piece = usize::try_from(piece).map_err(|_| ActorError::Arithmetic)?;
                if piece >= piece_count {
                    return Err(ActorError::Peer(
                        "peer announced a piece outside the torrent".to_owned(),
                    ));
                }
                let bits = available.get_or_insert_with(|| vec![0; expected_bitfield]);
                bits[piece / 8] |= 0x80 >> (piece % 8);
                debug!(piece, "peer announced a piece before unchoking");
            }
            PeerEvent::Message(PeerMessage::Extended {
                extension_id: LOCAL_PEX_EXTENSION_ID,
                payload,
            }) => peers.extend(pex_addresses(&payload)?),
            PeerEvent::Message(PeerMessage::Extended {
                extension_id: LOCAL_HOLEPUNCH_EXTENSION_ID,
                payload,
            }) => {
                let message = decode_holepunch_message(&payload)
                    .map_err(|error| ActorError::Peer(error.to_string()))?;
                if message.kind == HolePunchKind::Connect {
                    peers.push(message.address);
                }
            }
            PeerEvent::Disconnected => {
                return Err(ActorError::Peer(
                    "peer disconnected before unchoking".to_owned(),
                ));
            }
            PeerEvent::Failed(error) => {
                return Err(ActorError::Peer(format!(
                    "peer session failed before unchoking: {error}"
                )));
            }
            _ => {}
        }
    }
}

fn pex_addresses(payload: &[u8]) -> Result<Vec<SocketAddr>, ActorError> {
    let message = decode_pex_message(payload)
        .map_err(|error| ActorError::Peer(format!("invalid PEX update: {error}")))?;
    Ok(message
        .added
        .into_iter()
        .map(|peer| peer.address)
        .filter(|address| address.port() != 0 && !address.ip().is_unspecified())
        .collect())
}

async fn next_peer_event(peer: &mut PeerConnection) -> Result<PeerEvent, ActorError> {
    next_peer_event_with_timeout(peer, PEER_MESSAGE_TIMEOUT).await
}

async fn next_peer_event_with_timeout(
    peer: &mut PeerConnection,
    timeout: Duration,
) -> Result<PeerEvent, ActorError> {
    tokio::time::timeout(timeout, peer.next_event())
        .await
        .map_err(|_| ActorError::Peer("peer message timed out".to_owned()))?
        .ok_or_else(|| ActorError::Peer("peer event channel closed".to_owned()))
}

async fn recheck(
    metainfo: &Metainfo,
    record: &mut TorrentRecord,
    services: &Services,
    cancellation: &CancellationToken,
) -> Result<(), ActorError> {
    update_record_state(record, TorrentState::Checking, services).await?;
    record.completed_pieces.fill(0);
    let pieces = piece_count(metainfo)?;
    for index in 0..pieces {
        cancelled(cancellation)?;
        if let Some(piece) = read_piece(metainfo, index, &services.storage).await?
            && verify_piece(metainfo, index, &piece)?
        {
            set_bit(&mut record.completed_pieces, index);
        }
        if index % 64 == 63 {
            record.downloaded = completed_bytes(metainfo, &record.completed_pieces)?;
            replace_record(services, record.clone()).await?;
        }
    }
    record.downloaded = completed_bytes(metainfo, &record.completed_pieces)?;
    let state = if all_complete(&record.completed_pieces, pieces) {
        TorrentState::Seeding
    } else {
        TorrentState::Stopped
    };
    update_record_state(record, state, services).await
}

async fn read_piece(
    metainfo: &Metainfo,
    index: usize,
    storage: &StorageHandle,
) -> Result<Option<Bytes>, ActorError> {
    if metainfo.v1_piece_hashes.is_empty() {
        let (file, _, offset) = v2_piece_location(metainfo, index)?;
        let length = piece_length(metainfo, index)?;
        if file.padding {
            return Ok(Some(Bytes::from(vec![0; length])));
        }
        return match storage.read(file.path.clone(), offset, length).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(StorageError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(None)
            }
            Err(StorageError::ShortRead { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        };
    }
    let length = piece_length(metainfo, index)?;
    let start = piece_start(metainfo, index)?;
    let mut output = BytesMut::with_capacity(length);
    for segment in file_segments(wire_files(metainfo), start, length)? {
        if segment.file.padding {
            output.resize(output.len() + segment.length, 0);
            continue;
        }
        match storage
            .read(
                segment.file.path.clone(),
                segment.file_offset,
                segment.length,
            )
            .await
        {
            Ok(bytes) => output.extend_from_slice(&bytes),
            Err(StorageError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(StorageError::ShortRead { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        }
    }
    if output.len() != length {
        return Ok(None);
    }
    Ok(Some(output.freeze()))
}

async fn write_piece(
    metainfo: &Metainfo,
    index: usize,
    piece: Bytes,
    storage: &StorageHandle,
) -> Result<(), ActorError> {
    if metainfo.v1_piece_hashes.is_empty() {
        let (file, _, offset) = v2_piece_location(metainfo, index)?;
        if !file.padding {
            storage
                .write(file.path.clone(), offset, piece, file.length)
                .await?;
            storage.sync(file.path.clone()).await?;
        }
        return Ok(());
    }
    let start = piece_start(metainfo, index)?;
    let segments = file_segments(wire_files(metainfo), start, piece.len())?;
    let mut consumed = 0_usize;
    let mut touched = HashSet::new();
    for segment in segments {
        let end = consumed
            .checked_add(segment.length)
            .ok_or(ActorError::Arithmetic)?;
        if !segment.file.padding {
            storage
                .write(
                    segment.file.path.clone(),
                    segment.file_offset,
                    piece.slice(consumed..end),
                    segment.file.length,
                )
                .await?;
            touched.insert(segment.file.path.clone());
        }
        consumed = end;
    }
    if consumed != piece.len() {
        return Err(ActorError::Arithmetic);
    }
    for path in touched {
        storage.sync(path).await?;
    }
    Ok(())
}

struct FileSegment<'a> {
    file: &'a FileEntry,
    file_offset: u64,
    length: usize,
}

fn file_segments(
    files: &[FileEntry],
    start: u64,
    length: usize,
) -> Result<Vec<FileSegment<'_>>, ActorError> {
    let end = start
        .checked_add(u64::try_from(length).map_err(|_| ActorError::Arithmetic)?)
        .ok_or(ActorError::Arithmetic)?;
    let mut file_start = 0_u64;
    let mut segments = Vec::new();
    for file in files {
        let file_end = file_start
            .checked_add(file.length)
            .ok_or(ActorError::Arithmetic)?;
        let overlap_start = start.max(file_start);
        let overlap_end = end.min(file_end);
        if overlap_start < overlap_end {
            segments.push(FileSegment {
                file,
                file_offset: overlap_start - file_start,
                length: usize::try_from(overlap_end - overlap_start)
                    .map_err(|_| ActorError::Arithmetic)?,
            });
        }
        file_start = file_end;
        if file_start >= end {
            break;
        }
    }
    Ok(segments)
}

fn wire_files(metainfo: &Metainfo) -> &[FileEntry] {
    if metainfo.v1_piece_hashes.is_empty() {
        &metainfo.files
    } else {
        &metainfo.v1_files
    }
}

fn piece_start(metainfo: &Metainfo, index: usize) -> Result<u64, ActorError> {
    u64::try_from(index)
        .ok()
        .and_then(|value| value.checked_mul(u64::from(metainfo.piece_length.get())))
        .ok_or(ActorError::Arithmetic)
}

fn piece_length(metainfo: &Metainfo, index: usize) -> Result<usize, ActorError> {
    if metainfo.v1_piece_hashes.is_empty() {
        let (file, _, offset) = v2_piece_location(metainfo, index)?;
        return usize::try_from(
            file.length
                .saturating_sub(offset)
                .min(u64::from(metainfo.piece_length.get())),
        )
        .map_err(|_| ActorError::Arithmetic);
    }
    let start = piece_start(metainfo, index)?;
    let remaining = metainfo
        .piece_space_length
        .checked_sub(start)
        .ok_or(ActorError::Arithmetic)?;
    usize::try_from(remaining.min(u64::from(metainfo.piece_length.get())))
        .map_err(|_| ActorError::Arithmetic)
}

fn piece_count(metainfo: &Metainfo) -> Result<usize, ActorError> {
    if !metainfo.v1_piece_hashes.is_empty() {
        return Ok(metainfo.v1_piece_hashes.len());
    }
    metainfo.files.iter().try_fold(0_usize, |total, file| {
        let pieces = file.length.div_ceil(u64::from(metainfo.piece_length.get()));
        let pieces = usize::try_from(pieces).map_err(|_| ActorError::Arithmetic)?;
        total.checked_add(pieces).ok_or(ActorError::Arithmetic)
    })
}

fn v2_piece_location(
    metainfo: &Metainfo,
    index: usize,
) -> Result<(&FileEntry, usize, u64), ActorError> {
    let mut first_piece = 0_usize;
    for file in &metainfo.files {
        let pieces = usize::try_from(file.length.div_ceil(u64::from(metainfo.piece_length.get())))
            .map_err(|_| ActorError::Arithmetic)?;
        let end = first_piece
            .checked_add(pieces)
            .ok_or(ActorError::Arithmetic)?;
        if (first_piece..end).contains(&index) {
            let local = index - first_piece;
            let offset = u64::try_from(local)
                .ok()
                .and_then(|value| value.checked_mul(u64::from(metainfo.piece_length.get())))
                .ok_or(ActorError::Arithmetic)?;
            return Ok((file, local, offset));
        }
        first_piece = end;
    }
    Err(ActorError::PieceIndex)
}

fn wire_info_hash(metainfo: &Metainfo) -> Result<Sha1Hash, ActorError> {
    if let Some(hash) = metainfo.v1_info_hash {
        return Ok(hash);
    }
    let hash = metainfo.v2_info_hash.ok_or(ActorError::V2PeerWire)?;
    let truncated: [u8; 20] = hash.as_bytes()[..20]
        .try_into()
        .map_err(|_| ActorError::Arithmetic)?;
    Ok(Sha1Hash::from_bytes(truncated))
}

fn verify_piece(metainfo: &Metainfo, index: usize, piece: &[u8]) -> Result<bool, ActorError> {
    if let Some(expected) = metainfo.v1_piece_hashes.get(index) {
        let actual: [u8; 20] = Sha1::digest(piece).into();
        if Sha1Hash::from_bytes(actual) != *expected {
            return Ok(false);
        }
    }
    if metainfo.v2_info_hash.is_some() {
        let Some(target) = v2_verification_target(metainfo, index, piece)? else {
            return Ok(true);
        };
        let file = target.file;
        let local_index = target.local_index;
        let root = file.pieces_root.ok_or_else(|| {
            ActorError::Metainfo("non-empty v2 file has no pieces root".to_owned())
        })?;
        let expected = if file.length > u64::from(metainfo.piece_length.get()) {
            metainfo
                .piece_layers
                .get(&root)
                .and_then(|layer| layer.get(local_index))
                .copied()
                .ok_or_else(|| ActorError::Metainfo("v2 piece layer is incomplete".to_owned()))?
        } else {
            root
        };
        if v2_piece_root(target.data, metainfo.piece_length.get()) != expected {
            return Ok(false);
        }
    }
    Ok(true)
}

struct V2VerificationTarget<'a, 'b> {
    file: &'a FileEntry,
    local_index: usize,
    data: &'b [u8],
}

fn v2_verification_target<'a, 'b>(
    metainfo: &'a Metainfo,
    index: usize,
    piece: &'b [u8],
) -> Result<Option<V2VerificationTarget<'a, 'b>>, ActorError> {
    if metainfo.v1_piece_hashes.is_empty() {
        let (file, local, _) = v2_piece_location(metainfo, index)?;
        return Ok((!file.padding).then_some(V2VerificationTarget {
            file,
            local_index: local,
            data: piece,
        }));
    }
    let piece_start = piece_start(metainfo, index)?;
    let piece_end = piece_start
        .checked_add(u64::try_from(piece.len()).map_err(|_| ActorError::Arithmetic)?)
        .ok_or(ActorError::Arithmetic)?;
    let mut file_start = 0_u64;
    for v1_file in &metainfo.v1_files {
        let file_end = file_start
            .checked_add(v1_file.length)
            .ok_or(ActorError::Arithmetic)?;
        let overlap_start = piece_start.max(file_start);
        let overlap_end = piece_end.min(file_end);
        if !v1_file.padding && overlap_start < overlap_end {
            let file = metainfo
                .files
                .iter()
                .find(|file| file.path == v1_file.path && file.length == v1_file.length)
                .ok_or_else(|| ActorError::Metainfo("hybrid layouts diverged".to_owned()))?;
            let local_index = usize::try_from(
                (overlap_start - file_start) / u64::from(metainfo.piece_length.get()),
            )
            .map_err(|_| ActorError::Arithmetic)?;
            let data_start =
                usize::try_from(overlap_start - piece_start).map_err(|_| ActorError::Arithmetic)?;
            let data_end =
                usize::try_from(overlap_end - piece_start).map_err(|_| ActorError::Arithmetic)?;
            let data = piece
                .get(data_start..data_end)
                .ok_or(ActorError::Arithmetic)?;
            return Ok(Some(V2VerificationTarget {
                file,
                local_index,
                data,
            }));
        }
        file_start = file_end;
    }
    Ok(None)
}

fn v2_piece_root(piece: &[u8], piece_length: u32) -> dendrite_core::Sha256Hash {
    let leaf_count = usize::try_from(piece_length / (16 * 1024)).map_or(1, |value| value);
    let mut hashes: Vec<_> = piece
        .chunks(16 * 1024)
        .map(|block| dendrite_core::Sha256Hash::from_bytes(Sha256::digest(block).into()))
        .collect();
    hashes.resize(leaf_count, dendrite_core::Sha256Hash::from_bytes([0; 32]));
    while hashes.len() > 1 {
        hashes = hashes
            .chunks_exact(2)
            .map(|pair| {
                let mut digest = Sha256::new();
                digest.update(pair[0].as_bytes());
                digest.update(pair[1].as_bytes());
                dendrite_core::Sha256Hash::from_bytes(digest.finalize().into())
            })
            .collect();
    }
    hashes[0]
}

fn v2_zero_hash(layer: u32) -> dendrite_core::Sha256Hash {
    let mut hash = dendrite_core::Sha256Hash::from_bytes([0; 32]);
    for _ in 0..layer {
        hash = v2_hash_pair(hash, hash);
    }
    hash
}

fn v2_hash_pair(
    left: dendrite_core::Sha256Hash,
    right: dendrite_core::Sha256Hash,
) -> dendrite_core::Sha256Hash {
    let mut digest = Sha256::new();
    digest.update(left.as_bytes());
    digest.update(right.as_bytes());
    dendrite_core::Sha256Hash::from_bytes(digest.finalize().into())
}

fn completed_bytes(metainfo: &Metainfo, completed: &[u8]) -> Result<u64, ActorError> {
    (0..piece_count(metainfo)?)
        .filter(|index| bit_is_set(completed, *index))
        .try_fold(0_u64, |total, index| {
            let length = piece_content_length(metainfo, index)?;
            total.checked_add(length).ok_or(ActorError::Arithmetic)
        })
}

fn piece_content_length(metainfo: &Metainfo, index: usize) -> Result<u64, ActorError> {
    if metainfo.v1_piece_hashes.is_empty() {
        let (file, _, _) = v2_piece_location(metainfo, index)?;
        return if file.padding {
            Ok(0)
        } else {
            u64::try_from(piece_length(metainfo, index)?).map_err(|_| ActorError::Arithmetic)
        };
    }
    let start = piece_start(metainfo, index)?;
    file_segments(wire_files(metainfo), start, piece_length(metainfo, index)?)?
        .into_iter()
        .filter(|segment| !segment.file.padding)
        .try_fold(0_u64, |total, segment| {
            total
                .checked_add(u64::try_from(segment.length).map_err(|_| ActorError::Arithmetic)?)
                .ok_or(ActorError::Arithmetic)
        })
}

async fn update_record_state(
    record: &mut TorrentRecord,
    state: TorrentState,
    services: &Services,
) -> Result<(), ActorError> {
    record.state = state;
    replace_record(services, record.clone()).await?;
    let _subscriber_count = services.events.send(EngineEvent {
        torrent_id: record.id,
        state,
        detail: None,
    });
    Ok(())
}

async fn set_state(
    services: &Services,
    id: TorrentId,
    state: TorrentState,
) -> Result<(), ActorError> {
    let mut record = services
        .store
        .get_torrent(id)
        .await?
        .ok_or(ActorError::Missing)?;
    update_record_state(&mut record, state, services).await
}

async fn set_error(services: &Services, id: TorrentId, detail: String) -> Result<(), ActorError> {
    let mut record = services
        .store
        .get_torrent(id)
        .await?
        .ok_or(ActorError::Missing)?;
    record.state = TorrentState::Error;
    replace_record(services, record).await?;
    let _subscriber_count = services.events.send(EngineEvent {
        torrent_id: id,
        state: TorrentState::Error,
        detail: Some(detail),
    });
    Ok(())
}

async fn replace_record(services: &Services, record: TorrentRecord) -> Result<(), ActorError> {
    if services.store.replace_torrent(record).await? {
        Ok(())
    } else {
        Err(ActorError::Missing)
    }
}

fn normalize_completion(record: &mut TorrentRecord, pieces: usize) {
    let bytes = pieces.div_ceil(8);
    if record.completed_pieces.len() != bytes {
        record.completed_pieces = vec![0; bytes];
        record.downloaded = 0;
    }
}

fn all_complete(bitfield: &[u8], pieces: usize) -> bool {
    (0..pieces).all(|index| bit_is_set(bitfield, index))
}

#[cfg(test)]
fn complete_bitfield(pieces: usize) -> Vec<u8> {
    let mut bitfield = vec![u8::MAX; pieces.div_ceil(8)];
    let spare = bitfield.len().saturating_mul(8).saturating_sub(pieces);
    if let Some(last) = bitfield.last_mut() {
        *last &= u8::MAX << spare;
    }
    bitfield
}

fn bit_is_set(bitfield: &[u8], index: usize) -> bool {
    bitfield
        .get(index / 8)
        .is_some_and(|byte| byte & (0x80 >> (index % 8)) != 0)
}

fn set_bit(bitfield: &mut [u8], index: usize) {
    if let Some(byte) = bitfield.get_mut(index / 8) {
        *byte |= 0x80 >> (index % 8);
    }
}

fn cancelled(token: &CancellationToken) -> Result<(), ActorError> {
    if token.is_cancelled() {
        Err(ActorError::Cancelled)
    } else {
        Ok(())
    }
}

fn generate_peer_id() -> PeerId {
    const PREFIX: &[u8; 8] = b"-SY2000-";
    const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let random: [u8; 12] = rand::random();
    let mut id = [0_u8; 20];
    id[..8].copy_from_slice(PREFIX);
    for (output, value) in id[8..].iter_mut().zip(random) {
        *output = ALPHABET[usize::from(value) % ALPHABET.len()];
    }
    PeerId::from_bytes(id)
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr},
        process::Command,
    };

    use dendrite_persistence::StateStore;
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpListener,
    };

    use super::*;

    const CRASH_TEST_PAYLOAD: &[u8] = b"crash-consistent payload";

    #[derive(Clone, Copy, Debug)]
    enum MaliciousBlock {
        WrongPiece,
        WrongLength,
        UnsolicitedOffset,
        Duplicate,
        Oversized,
    }

    #[test]
    fn file_segments_cross_boundaries_without_escape() -> Result<(), Box<dyn std::error::Error>> {
        let files = vec![
            FileEntry {
                path: TorrentPath::new(["root".to_owned(), "a".to_owned()])?,
                length: 3,
                pieces_root: None,
                padding: false,
            },
            FileEntry {
                path: TorrentPath::new(["root".to_owned(), "b".to_owned()])?,
                length: 5,
                pieces_root: None,
                padding: false,
            },
        ];
        let segments = file_segments(&files, 2, 4)?;
        assert_eq!(segments.len(), 2);
        assert_eq!((segments[0].file_offset, segments[0].length), (2, 1));
        assert_eq!((segments[1].file_offset, segments[1].length), (0, 3));
        Ok(())
    }

    #[tokio::test]
    async fn overlapping_payload_owner_survives_recheck_and_state_deletion_races()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let owner_payload = Bytes::from_static(b"owner payload remains authoritative");
        let contender_payload = Bytes::from_static(b"different torrent, identical output path");
        let owner_raw = multi_piece_v1_metainfo("shared.bin", std::slice::from_ref(&owner_payload));
        let contender_raw =
            multi_piece_v1_metainfo("shared.bin", std::slice::from_ref(&contender_payload));
        let owner_metainfo = Metainfo::parse(&owner_raw, BencodeLimits::default())?;
        let contender_metainfo = Metainfo::parse(&contender_raw, BencodeLimits::default())?;
        let mut owner = test_record(&owner_metainfo, owner_raw);
        owner.added_at_unix_ms = 1;
        owner.state = TorrentState::Seeding;
        owner.completed_pieces = complete_bitfield(1);
        owner.downloaded = owner_metainfo.total_length;
        let mut contender = test_record(&contender_metainfo, contender_raw);
        contender.added_at_unix_ms = 2;
        contender.state = TorrentState::Checking;

        let directory = tempfile::tempdir()?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 16)?;
        store.put_torrent(owner.clone()).await?;
        store.put_torrent(contender.clone()).await?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let storage = StorageHandle::start_portable(&downloads, 16)?;
        let shared = owner_metainfo.files[0].path.clone();
        storage
            .write(
                shared.clone(),
                0,
                owner_payload.clone(),
                u64::try_from(owner_payload.len())?,
            )
            .await?;
        let services = test_services(store.clone(), storage.clone(), "overlap");

        let owner_claim = claim_active_payload_paths(&owner, &owner_metainfo, &services)?;
        let mut clock_rollback_contender = contender.clone();
        clock_rollback_contender.added_at_unix_ms = 0;
        assert!(matches!(
            claim_active_payload_paths(
                &clock_rollback_contender,
                &contender_metainfo,
                &services
            ),
            Err(ActorError::PathConflict { owner: id, .. }) if id == owner.id
        ));
        drop(owner_claim);
        drop(claim_active_payload_paths(
            &clock_rollback_contender,
            &contender_metainfo,
            &services,
        )?);

        assert!(matches!(
            run_actor(
                contender.id,
                ActorMode::Recheck,
                &services,
                &CancellationToken::new()
            )
            .await,
            Err(ActorError::PathConflict { owner: id, .. }) if id == owner.id
        ));
        assert_eq!(
            storage.read(shared.clone(), 0, owner_payload.len()).await?,
            owner_payload
        );

        assert!(store.remove_torrent(owner.id).await?);
        assert_eq!(
            storage.read(shared.clone(), 0, owner_payload.len()).await?,
            owner_payload,
            "forgetting state must never delete shared payload"
        );
        run_actor(
            contender.id,
            ActorMode::Recheck,
            &services,
            &CancellationToken::new(),
        )
        .await?;
        let checked = store
            .get_torrent(contender.id)
            .await?
            .ok_or("contender disappeared")?;
        assert_eq!(checked.completed_pieces, vec![0]);
        assert_eq!(checked.downloaded, 0);
        assert_eq!(
            storage.read(shared, 0, owner_payload.len()).await?,
            owner_payload
        );

        let directory_path = TorrentPath::new(["shared.bin".to_owned()])?;
        let nested_path = TorrentPath::new(["shared.bin".to_owned(), "nested.bin".to_owned()])?;
        assert!(payload_paths_conflict(&directory_path, &nested_path));
        let case_variant = TorrentPath::new(["SHARED.BIN".to_owned()])?;
        assert!(payload_paths_conflict(&directory_path, &case_variant));
        Ok(())
    }

    #[test]
    fn peer_ids_have_stable_prefix() {
        assert_eq!(&generate_peer_id().as_bytes()[..8], b"-SY2000-");
    }

    #[test]
    fn repeated_accept_failures_back_off_without_overflowing() {
        let mut delay = ACCEPT_ERROR_BACKOFF_MIN;
        for _ in 0..32 {
            let next = next_accept_error_backoff(delay);
            assert!(next >= delay);
            assert!(next <= ACCEPT_ERROR_BACKOFF_MAX);
            delay = next;
        }
        assert_eq!(delay, ACCEPT_ERROR_BACKOFF_MAX);
    }

    #[test]
    fn v2_multifile_layout_aligns_every_file_to_a_piece() -> Result<(), Box<dyn std::error::Error>>
    {
        let first = b"first file";
        let second = b"second file has a different length";
        let raw = two_file_v2_metainfo(first, second);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        assert_eq!(piece_count(&metainfo)?, 2);
        let (first_file, first_local, first_offset) = v2_piece_location(&metainfo, 0)?;
        let (second_file, second_local, second_offset) = v2_piece_location(&metainfo, 1)?;
        assert_eq!((first_local, first_offset), (0, 0));
        assert_eq!((second_local, second_offset), (0, 0));
        assert_ne!(first_file.path, second_file.path);
        assert!(verify_piece(&metainfo, 0, first)?);
        assert!(verify_piece(&metainfo, 1, second)?);
        assert!(!verify_piece(&metainfo, 1, first)?);
        Ok(())
    }

    #[test]
    fn hybrid_layout_verifies_both_hashes_and_excludes_padding_from_progress()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = b"alpha";
        let second = b"the second aligned file";
        let (raw, first_piece, second_piece) = hybrid_metainfo(first, second);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        assert_eq!(metainfo.total_length, (first.len() + second.len()) as u64);
        assert_eq!(
            metainfo.piece_space_length,
            (BLOCK_BYTES + second.len()) as u64
        );
        assert!(verify_piece(&metainfo, 0, &first_piece)?);
        assert!(verify_piece(&metainfo, 1, &second_piece)?);
        let mut corrupted = first_piece;
        corrupted[0] ^= 0xff;
        assert!(!verify_piece(&metainfo, 0, &corrupted)?);
        assert_eq!(
            completed_bytes(&metainfo, &[0b1100_0000])?,
            metainfo.total_length
        );
        Ok(())
    }

    #[tokio::test]
    async fn hybrid_swarm_writes_aligned_files_without_padding_payloads()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let first = b"alpha";
        let second = b"the second aligned file";
        let (raw, first_piece, second_piece) = hybrid_metainfo(first, second);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let first_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let second_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addresses = vec![first_listener.local_addr()?, second_listener.local_addr()?];
        let first_peer = tokio::spawn(fake_single_piece_peer(
            first_listener,
            info_hash,
            0,
            Bytes::from(first_piece),
        ));
        let second_peer = tokio::spawn(fake_single_piece_peer(
            second_listener,
            info_hash,
            1,
            Bytes::from(second_piece),
        ));
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start(&downloads, 32)?;
        let (event_sender, _) = broadcast::channel(16);
        let services = Services {
            store: store.clone(),
            storage,
            tracker_response_limit: 64 * 1024,
            metainfo_limit: 64 * 1024,
            peer_message_timeout: PEER_MESSAGE_TIMEOUT,
            allow_private_web_seeds: false,
            dht_bootstrap: Vec::new(),
            dht: None,
            utp: None,
            peer_port: 6881,
            advertised_peer_port: Arc::new(AtomicU16::new(6881)),
            peer_id: generate_peer_id(),
            events: event_sender,
            peer_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            per_torrent_peer_limit: per_torrent_peer_limit(INCOMING_PEER_LIMIT),
            lsd_cookie: "pex-test".to_owned(),
            encryption: EncryptionPolicy::Disabled,
            rendezvous: Arc::new(Mutex::new(HashMap::new())),
            connected_peers: Arc::new(AtomicUsize::new(0)),
            torrent_peers: Arc::new(std::sync::Mutex::new(HashMap::new())),
            payload_claims: Arc::new(std::sync::Mutex::new(HashMap::new())),
            shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
        };
        let mut record = test_record(&metainfo, raw);
        normalize_completion(&mut record, 2);
        store.put_torrent(record.clone()).await?;
        run_peer_swarm(
            addresses,
            info_hash,
            &metainfo,
            &mut record,
            &services,
            &CancellationToken::new(),
        )
        .await?;
        assert_eq!(tokio::fs::read(downloads.join("root/a")).await?, first);
        assert_eq!(tokio::fs::read(downloads.join("root/b")).await?, second);
        assert!(!downloads.join("root/.pad").exists());
        first_peer.await.map_err(|error| error.to_string())??;
        second_peer.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn incoming_tcp_seed_serves_verified_payload_metadata_and_counts_uploads()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"incoming TCP peers receive only verified seed data");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw = single_file_metainfo("http://127.0.0.1/announce", "seed.bin", &payload, digest);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start(&downloads, 32)?;
        storage
            .write(
                metainfo.files[0].path.clone(),
                0,
                payload.clone(),
                payload.len() as u64,
            )
            .await?;
        let mut record = test_record(&metainfo, raw.clone());
        record.state = TorrentState::Seeding;
        record.completed_pieces = complete_bitfield(1);
        store.put_torrent(record.clone()).await?;
        let engine = EngineHandle::start(
            store.clone(),
            storage,
            64 * 1024,
            64 * 1024,
            Vec::new(),
            None,
            6881,
        );
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        engine.serve_incoming(listener);
        let mut reserved = [0_u8; 8];
        reserved[5] |= 0x10;
        let mut peer = PeerConnection::connect(
            address,
            Handshake {
                reserved,
                info_hash,
                peer_id: PeerId::from_bytes([0x31; 20]),
            },
            PeerCodecLimits::default(),
        )
        .await?;
        wait_for_seed_bitfield(&mut peer, &[0x80]).await?;

        peer.send(PeerMessage::Extended {
            extension_id: 0,
            payload: encode_extension_handshake(None),
        })
        .await?;
        wait_for_metadata_handshake(&mut peer, raw_info_dictionary(&raw, raw.len())?.len()).await?;
        peer.send(PeerMessage::Extended {
            extension_id: LOCAL_METADATA_EXTENSION_ID,
            payload: encode_metadata_request(0),
        })
        .await?;
        let served_info = wait_for_metadata_data(&mut peer).await?;
        assert_eq!(served_info, raw_info_dictionary(&raw, raw.len())?);

        peer.send(PeerMessage::Interested).await?;
        wait_for_unchoke(&mut peer).await?;
        peer.send(PeerMessage::Request(BlockRequest {
            piece: 0,
            begin: 0,
            length: u32::try_from(payload.len())?,
        }))
        .await?;
        assert_eq!(wait_for_piece(&mut peer).await?, payload);
        engine.shutdown().await?;
        peer.shutdown();
        let updated = store
            .get_torrent(record.id)
            .await?
            .ok_or("seed record disappeared")?;
        assert_eq!(updated.uploaded, payload.len() as u64);
        Ok(())
    }

    #[tokio::test]
    async fn incoming_peer_limit_backpressures_connection_flood_and_shutdown_releases_it()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"bounded incoming seed");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw = single_file_metainfo(
            "http://127.0.0.1/announce",
            "bounded-seed.bin",
            &payload,
            digest,
        );
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 16)?;
        let storage = StorageHandle::start_portable(&downloads, 16)?;
        write_piece(&metainfo, 0, payload, &storage).await?;
        let mut record = test_record(&metainfo, raw);
        record.state = TorrentState::Seeding;
        record.completed_pieces = complete_bitfield(1);
        record.downloaded = metainfo.total_length;
        let id = record.id;
        store.put_torrent(record).await?;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let conflict = TcpListener::bind(address).await;
        assert!(matches!(
            conflict,
            Err(ref error) if error.kind() == std::io::ErrorKind::AddrInUse
        ));
        let engine = EngineHandle::start_configured(
            store,
            storage,
            EngineOptions {
                tracker_response_limit: 64 * 1024,
                metainfo_limit: 64 * 1024,
                dht_bootstrap: Vec::new(),
                dht: None,
                utp: None,
                peer_port: address.port(),
                encryption: EncryptionPolicy::Disabled,
                peer_connection_limit: 2,
                allow_private_web_seeds: false,
            },
        );
        engine.serve_incoming(listener);
        let mut clients = Vec::new();
        for peer in 0..16_u8 {
            clients.push(tokio::spawn(PeerConnection::connect(
                address,
                Handshake {
                    reserved: [0; 8],
                    info_hash,
                    peer_id: PeerId::from_bytes([peer; 20]),
                },
                PeerCodecLimits::default(),
            )));
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while engine.connected_peers() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "incoming peer limit was not reached")?;
        assert_eq!(engine.connected_peers(), 2);
        assert_eq!(engine.torrent_peer_count(id), 2);
        engine.shutdown().await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            while engine.connected_peers() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "incoming peers outlived engine shutdown")?;
        for client in clients {
            client.abort();
        }
        Ok(())
    }

    #[tokio::test]
    async fn nat_remapping_updates_advertised_port_and_rejects_zero()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 8)?;
        let engine = EngineHandle::start(
            store,
            StorageHandle::start_portable(&downloads, 8)?,
            1024,
            1024,
            Vec::new(),
            None,
            6881,
        );
        engine.set_advertised_peer_port(45_000);
        assert_eq!(
            engine.services.advertised_peer_port.load(Ordering::Acquire),
            45_000
        );
        engine.set_advertised_peer_port(46_000);
        assert_eq!(
            engine.services.advertised_peer_port.load(Ordering::Acquire),
            46_000
        );
        engine.set_advertised_peer_port(0);
        assert_eq!(
            engine.services.advertised_peer_port.load(Ordering::Acquire),
            46_000
        );
        engine.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn required_mse_policy_serves_encrypted_incoming_peer_wire()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"MSE encrypted incoming seed data");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw = single_file_metainfo("http://127.0.0.1/announce", "mse.bin", &payload, digest);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start(&downloads, 32)?;
        storage
            .write(
                metainfo.files[0].path.clone(),
                0,
                payload.clone(),
                payload.len() as u64,
            )
            .await?;
        let mut record = test_record(&metainfo, raw);
        record.state = TorrentState::Seeding;
        record.completed_pieces = complete_bitfield(1);
        store.put_torrent(record).await?;
        let engine = EngineHandle::start_configured(
            store,
            storage,
            EngineOptions {
                tracker_response_limit: 64 * 1024,
                metainfo_limit: 64 * 1024,
                dht_bootstrap: Vec::new(),
                dht: None,
                utp: None,
                peer_port: 6881,
                encryption: EncryptionPolicy::Required,
                peer_connection_limit: INCOMING_PEER_LIMIT,
                allow_private_web_seeds: false,
            },
        );
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        engine.serve_incoming(listener);
        let mut peer = PeerConnection::connect_encrypted(
            address,
            Handshake {
                reserved: [0; 8],
                info_hash,
                peer_id: PeerId::from_bytes([0x61; 20]),
            },
            PeerCodecLimits::default(),
        )
        .await?;
        wait_for_seed_bitfield(&mut peer, &[0x80]).await?;
        peer.send(PeerMessage::Interested).await?;
        wait_for_unchoke(&mut peer).await?;
        peer.send(PeerMessage::Request(BlockRequest {
            piece: 0,
            begin: 0,
            length: u32::try_from(payload.len())?,
        }))
        .await?;
        assert_eq!(wait_for_piece(&mut peer).await?, payload);
        peer.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn incoming_utp_seed_serves_verified_payload()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"incoming uTP seed payload");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw = single_file_metainfo("http://127.0.0.1/announce", "utp.bin", &payload, digest);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start(&downloads, 32)?;
        storage
            .write(
                metainfo.files[0].path.clone(),
                0,
                payload.clone(),
                payload.len() as u64,
            )
            .await?;
        let mut record = test_record(&metainfo, raw);
        record.state = TorrentState::Seeding;
        record.completed_pieces = complete_bitfield(1);
        store.put_torrent(record).await?;
        let server_utp = UtpEndpoint::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
        let tcp_listener = TcpListener::bind(server_utp.local_addr()).await?;
        let engine = EngineHandle::start(
            store,
            storage,
            64 * 1024,
            64 * 1024,
            Vec::new(),
            Some(server_utp.clone()),
            server_utp.local_addr().port(),
        );
        engine.serve_incoming(tcp_listener);
        let client_utp = UtpEndpoint::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
        let mut peer = client_utp
            .connect_peer(
                server_utp.local_addr(),
                Handshake {
                    reserved: [0; 8],
                    info_hash,
                    peer_id: PeerId::from_bytes([0x41; 20]),
                },
                PeerCodecLimits::default(),
            )
            .await?;
        wait_for_seed_bitfield(&mut peer, &[0x80]).await?;
        peer.send(PeerMessage::Interested).await?;
        wait_for_unchoke(&mut peer).await?;
        peer.send(PeerMessage::Request(BlockRequest {
            piece: 0,
            begin: 0,
            length: u32::try_from(payload.len())?,
        }))
        .await?;
        assert_eq!(wait_for_piece(&mut peer).await?, payload);
        peer.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn holepunch_connect_initiates_a_utp_seed_session()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"hole-punched uTP seed payload");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw = single_file_metainfo("http://127.0.0.1/announce", "hole.bin", &payload, digest);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start(&downloads, 32)?;
        storage
            .write(
                metainfo.files[0].path.clone(),
                0,
                payload.clone(),
                payload.len() as u64,
            )
            .await?;
        let mut record = test_record(&metainfo, raw);
        record.state = TorrentState::Seeding;
        record.completed_pieces = complete_bitfield(1);
        store.put_torrent(record).await?;
        let server_utp = UtpEndpoint::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
        let tcp_listener = TcpListener::bind(server_utp.local_addr()).await?;
        let tcp_address = tcp_listener.local_addr()?;
        let engine = EngineHandle::start(
            store,
            storage,
            64 * 1024,
            64 * 1024,
            Vec::new(),
            Some(server_utp),
            tcp_address.port(),
        );
        engine.serve_incoming(tcp_listener);

        let target = UtpEndpoint::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
        let target_address = target.local_addr();
        let expected_payload = payload.clone();
        let target_task = tokio::spawn(async move {
            let mut peer = target
                .accept_peer(
                    Handshake {
                        reserved: [0; 8],
                        info_hash,
                        peer_id: PeerId::from_bytes([0x52; 20]),
                    },
                    PeerCodecLimits::default(),
                )
                .await?;
            wait_for_seed_bitfield(&mut peer, &[0x80]).await?;
            peer.send(PeerMessage::Interested).await?;
            wait_for_unchoke(&mut peer).await?;
            peer.send(PeerMessage::Request(BlockRequest {
                piece: 0,
                begin: 0,
                length: u32::try_from(expected_payload.len())?,
            }))
            .await?;
            let received = wait_for_piece(&mut peer).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(received)
        });

        let mut relay = PeerConnection::connect(
            tcp_address,
            Handshake {
                reserved: [0; 8],
                info_hash,
                peer_id: PeerId::from_bytes([0x51; 20]),
            },
            PeerCodecLimits::default(),
        )
        .await?;
        wait_for_seed_bitfield(&mut relay, &[0x80]).await?;
        relay
            .send(PeerMessage::Extended {
                extension_id: 0,
                payload: encode_extension_handshake(None),
            })
            .await?;
        relay
            .send(PeerMessage::Extended {
                extension_id: LOCAL_HOLEPUNCH_EXTENSION_ID,
                payload: encode_holepunch_message(HolePunchMessage {
                    kind: HolePunchKind::Connect,
                    address: target_address,
                    error_code: 0,
                })?,
            })
            .await?;
        let received = tokio::time::timeout(Duration::from_secs(5), target_task)
            .await
            .map_err(|_| "hole-punch session timed out")?
            .map_err(|error| error.to_string())??;
        assert_eq!(received, payload);
        relay.shutdown();
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn holepunch_rendezvous_relays_observed_endpoints_to_both_peers()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"rendezvous payload");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw = single_file_metainfo("http://127.0.0.1/announce", "relay.bin", &payload, digest);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let info_size = raw_info_dictionary(&raw, 64 * 1024)?.len();
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start(&downloads, 32)?;
        storage
            .write(
                metainfo.files[0].path.clone(),
                0,
                payload,
                metainfo.total_length,
            )
            .await?;
        let mut record = test_record(&metainfo, raw);
        record.state = TorrentState::Seeding;
        record.completed_pieces = complete_bitfield(1);
        store.put_torrent(record).await?;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let engine = EngineHandle::start(
            store,
            storage,
            64 * 1024,
            64 * 1024,
            Vec::new(),
            None,
            address.port(),
        );
        engine.serve_incoming(listener);

        let mut target = PeerConnection::connect(
            address,
            Handshake {
                reserved: [0; 8],
                info_hash,
                peer_id: PeerId::from_bytes([0x71; 20]),
            },
            PeerCodecLimits::default(),
        )
        .await?;
        wait_for_seed_bitfield(&mut target, &[0x80]).await?;
        target
            .send(PeerMessage::Extended {
                extension_id: 0,
                payload: encode_extension_handshake(None),
            })
            .await?;
        wait_for_metadata_handshake(&mut target, info_size).await?;
        let target_address = engine
            .services
            .rendezvous
            .lock()
            .await
            .keys()
            .find_map(|(hash, address)| (*hash == info_hash).then_some(*address))
            .ok_or("target was not registered for rendezvous")?;

        let mut requester = PeerConnection::connect(
            address,
            Handshake {
                reserved: [0; 8],
                info_hash,
                peer_id: PeerId::from_bytes([0x72; 20]),
            },
            PeerCodecLimits::default(),
        )
        .await?;
        wait_for_seed_bitfield(&mut requester, &[0x80]).await?;
        requester
            .send(PeerMessage::Extended {
                extension_id: 0,
                payload: encode_extension_handshake(None),
            })
            .await?;
        wait_for_metadata_handshake(&mut requester, info_size).await?;
        let requester_address = engine
            .services
            .rendezvous
            .lock()
            .await
            .keys()
            .find_map(|(hash, candidate)| {
                (*hash == info_hash && *candidate != target_address).then_some(*candidate)
            })
            .ok_or("requester was not registered for rendezvous")?;
        requester
            .send(PeerMessage::Extended {
                extension_id: LOCAL_HOLEPUNCH_EXTENSION_ID,
                payload: encode_holepunch_message(HolePunchMessage {
                    kind: HolePunchKind::Rendezvous,
                    address: target_address,
                    error_code: 0,
                })?,
            })
            .await?;
        assert_eq!(
            wait_for_holepunch(&mut requester).await?,
            HolePunchMessage {
                kind: HolePunchKind::Connect,
                address: target_address,
                error_code: 0,
            }
        );
        assert_eq!(
            wait_for_holepunch(&mut target).await?,
            HolePunchMessage {
                kind: HolePunchKind::Connect,
                address: requester_address,
                error_code: 0,
            }
        );
        requester.shutdown();
        target.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn pex_bootstrap_expands_the_live_swarm()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"PEX supplied this otherwise undiscoverable peer");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw = single_file_metainfo("http://127.0.0.1/announce", "pex.bin", &payload, digest);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let payload_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let payload_address = payload_listener.local_addr()?;
        let bootstrap_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let bootstrap_address = bootstrap_listener.local_addr()?;
        let payload_peer = tokio::spawn(fake_peer(payload_listener, info_hash, payload.clone()));
        let bootstrap_peer = tokio::spawn(fake_pex_bootstrap(
            bootstrap_listener,
            info_hash,
            payload_address,
        ));
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start(&downloads, 32)?;
        let (event_sender, _) = broadcast::channel(16);
        let services = Services {
            store: store.clone(),
            storage,
            tracker_response_limit: 64 * 1024,
            metainfo_limit: 64 * 1024,
            peer_message_timeout: PEER_MESSAGE_TIMEOUT,
            allow_private_web_seeds: false,
            dht_bootstrap: Vec::new(),
            dht: None,
            utp: None,
            peer_port: 6881,
            advertised_peer_port: Arc::new(AtomicU16::new(6881)),
            peer_id: generate_peer_id(),
            events: event_sender,
            peer_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            per_torrent_peer_limit: per_torrent_peer_limit(INCOMING_PEER_LIMIT),
            lsd_cookie: "swarm-test".to_owned(),
            encryption: EncryptionPolicy::Disabled,
            rendezvous: Arc::new(Mutex::new(HashMap::new())),
            connected_peers: Arc::new(AtomicUsize::new(0)),
            torrent_peers: Arc::new(std::sync::Mutex::new(HashMap::new())),
            payload_claims: Arc::new(std::sync::Mutex::new(HashMap::new())),
            shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
        };
        let mut record = test_record(&metainfo, raw);
        normalize_completion(&mut record, 1);
        store.put_torrent(record.clone()).await?;
        run_peer_swarm(
            vec![bootstrap_address],
            info_hash,
            &metainfo,
            &mut record,
            &services,
            &CancellationToken::new(),
        )
        .await?;
        assert_eq!(tokio::fs::read(downloads.join("pex.bin")).await?, payload);
        bootstrap_peer.await.map_err(|error| error.to_string())??;
        payload_peer.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn tracker_discovery_combines_all_successful_tiers()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let first_tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let second_tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let first_url = format!("http://{}/announce", first_tracker.local_addr()?);
        let second_url = format!("http://{}/announce", second_tracker.local_addr()?);
        let first_peer = SocketAddr::from(([127, 0, 0, 1], 61_001));
        let second_peer = SocketAddr::from(([127, 0, 0, 1], 61_002));
        let first_task = tokio::spawn(fake_tracker(first_tracker, first_peer));
        let second_task = tokio::spawn(fake_tracker(second_tracker, second_peer));
        let payload = Bytes::from_static(b"tracker tier aggregation");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw = single_file_metainfo(&first_url, "tiers.bin", &payload, digest);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let record = test_record(&metainfo, raw);
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 8)?;
        let services = test_services(
            store,
            StorageHandle::start_portable(&downloads, 8)?,
            "tracker-tiers-test",
        );

        let peers = discover_tracker_peers(
            &[vec![first_url], vec![second_url]],
            &record,
            &services,
            info_hash,
            metainfo.total_length,
            AnnounceEvent::Started,
        )
        .await?;

        assert_eq!(
            peers.into_iter().collect::<HashSet<_>>(),
            HashSet::from([first_peer, second_peer])
        );
        first_task.await.map_err(|error| error.to_string())??;
        second_task.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn trackerless_public_swarm_discovers_and_downloads_over_lsd()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"LSD supplied this LAN peer");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw = single_file_metainfo("http://127.0.0.1:1/announce", "lsd.bin", &payload, digest);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let peer_listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
        let peer_address = peer_listener.local_addr()?;
        let peer_task = tokio::spawn(fake_peer(peer_listener, info_hash, payload.clone()));
        let discovery = LsdService::bind(peer_address.port(), "lsd-seeder-test".to_owned())?;
        let responder = tokio::spawn(async move {
            loop {
                let (announce, _) = discovery.receive().await?;
                if announce.info_hashes.contains(&info_hash) {
                    discovery.announce(&[info_hash]).await?;
                    return Ok::<_, dendrite_net::lsd::LsdError>(());
                }
            }
        });
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start(&downloads, 32)?;
        let (event_sender, _) = broadcast::channel(16);
        let services = Services {
            store: store.clone(),
            storage,
            tracker_response_limit: 64 * 1024,
            metainfo_limit: 64 * 1024,
            peer_message_timeout: PEER_MESSAGE_TIMEOUT,
            allow_private_web_seeds: false,
            dht_bootstrap: Vec::new(),
            dht: None,
            utp: None,
            peer_port: 46_881,
            advertised_peer_port: Arc::new(AtomicU16::new(46_881)),
            peer_id: generate_peer_id(),
            events: event_sender,
            peer_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            per_torrent_peer_limit: per_torrent_peer_limit(INCOMING_PEER_LIMIT),
            lsd_cookie: "lsd-downloader-test".to_owned(),
            encryption: EncryptionPolicy::Disabled,
            rendezvous: Arc::new(Mutex::new(HashMap::new())),
            connected_peers: Arc::new(AtomicUsize::new(0)),
            torrent_peers: Arc::new(std::sync::Mutex::new(HashMap::new())),
            payload_claims: Arc::new(std::sync::Mutex::new(HashMap::new())),
            shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
        };
        let mut record = test_record(&metainfo, raw);
        normalize_completion(&mut record, 1);
        store.put_torrent(record.clone()).await?;
        let peers = discover_peers(
            &metainfo,
            &record,
            &services,
            info_hash,
            AnnounceEvent::Started,
        )
        .await?;
        assert!(
            peers
                .iter()
                .any(|address| address.port() == peer_address.port())
        );
        run_peer_swarm(
            peers,
            info_hash,
            &metainfo,
            &mut record,
            &services,
            &CancellationToken::new(),
        )
        .await?;
        assert_eq!(tokio::fs::read(downloads.join("lsd.bin")).await?, payload);
        responder.await??;
        peer_task.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn required_mse_policy_downloads_from_encrypted_peer()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"MSE encrypted outbound download");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw =
            single_file_metainfo("http://127.0.0.1/announce", "mse-out.bin", &payload, digest);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(fake_encrypted_peer(listener, info_hash, payload.clone()));
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start(&downloads, 32)?;
        let (event_sender, _) = broadcast::channel(16);
        let services = Services {
            store: store.clone(),
            storage,
            tracker_response_limit: 64 * 1024,
            metainfo_limit: 64 * 1024,
            peer_message_timeout: PEER_MESSAGE_TIMEOUT,
            allow_private_web_seeds: false,
            dht_bootstrap: Vec::new(),
            dht: None,
            utp: None,
            peer_port: 6881,
            advertised_peer_port: Arc::new(AtomicU16::new(6881)),
            peer_id: generate_peer_id(),
            events: event_sender,
            peer_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            per_torrent_peer_limit: per_torrent_peer_limit(INCOMING_PEER_LIMIT),
            lsd_cookie: "mse-out-test".to_owned(),
            encryption: EncryptionPolicy::Required,
            rendezvous: Arc::new(Mutex::new(HashMap::new())),
            connected_peers: Arc::new(AtomicUsize::new(0)),
            torrent_peers: Arc::new(std::sync::Mutex::new(HashMap::new())),
            payload_claims: Arc::new(std::sync::Mutex::new(HashMap::new())),
            shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
        };
        let mut record = test_record(&metainfo, raw);
        normalize_completion(&mut record, 1);
        store.put_torrent(record.clone()).await?;
        run_peer_swarm(
            vec![address],
            info_hash,
            &metainfo,
            &mut record,
            &services,
            &CancellationToken::new(),
        )
        .await?;
        assert_eq!(
            tokio::fs::read(downloads.join("mse-out.bin")).await?,
            payload
        );
        server.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn multifile_web_seed_fallback_maps_ranges_and_padding_safely()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let first = b"web seed alpha";
        let second = b"web seed beta with a distinct length";
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let base = format!("http://{}/", listener.local_addr()?);
        let server = tokio::spawn(fake_multifile_web_seed(
            listener,
            first.to_vec(),
            second.to_vec(),
        ));
        let (mut raw, _, _) = hybrid_metainfo(first, second);
        if raw.pop() != Some(b'e') {
            return Err("invalid hybrid fixture".into());
        }
        raw.extend_from_slice(format!("8:url-list{}:{base}e", base.len()).as_bytes());
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start(&downloads, 32)?;
        let record = test_record(&metainfo, raw);
        let id = record.id;
        store.put_torrent(record).await?;
        let engine = EngineHandle::start_configured(
            store,
            storage,
            EngineOptions {
                tracker_response_limit: 64 * 1024,
                metainfo_limit: 64 * 1024,
                dht_bootstrap: Vec::new(),
                dht: None,
                utp: None,
                peer_port: 6881,
                encryption: EncryptionPolicy::Disabled,
                peer_connection_limit: INCOMING_PEER_LIMIT,
                allow_private_web_seeds: true,
            },
        );
        let mut events = engine.subscribe();
        engine.resume(id).await?;
        wait_for_seeding(&mut events).await?;
        assert_eq!(tokio::fs::read(downloads.join("root/a")).await?, first);
        assert_eq!(tokio::fs::read(downloads.join("root/b")).await?, second);
        assert!(!downloads.join("root/.pad").exists());
        server.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn web_seed_ssrf_and_http_range_attack_corpus_is_rejected()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for address in [
            "http://127.0.0.1:80/file",
            "http://10.0.0.1/file",
            "http://100.64.0.1/file",
            "http://169.254.169.254/latest/meta-data",
            "http://198.18.0.1/file",
            "http://[::1]/file",
            "http://[fe80::1]/file",
            "http://[::ffff:127.0.0.1]/file",
        ] {
            let url = Url::parse(address)?;
            assert!(
                web_seed_client(&url, false).await.is_err(),
                "private web seed {address} bypassed SSRF policy"
            );
        }
        assert!(public_web_seed_ip("8.8.8.8".parse()?));
        assert!(public_web_seed_ip("2606:4700:4700::1111".parse()?));

        let responses = [
            b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndata".to_vec(),
            b"HTTP/1.1 206 Partial Content\r\nContent-Length: 4\r\nContent-Range: bytes 0-3/8\r\nConnection: close\r\n\r\ndata".to_vec(),
            b"HTTP/1.1 206 Partial Content\r\nContent-Length: 4\r\nContent-Range: bytes 1-4/8\r\nConnection: close\r\n\r\nda".to_vec(),
            b"HTTP/1.1 206 Partial Content\r\nContent-Length: 5\r\nContent-Range: bytes 1-4/8\r\nConnection: close\r\n\r\ndata!".to_vec(),
            b"HTTP/1.1 206 Partial Content\r\nContent-Length: 4\r\nContent-Range: bytes 1-4/8\r\nContent-Encoding: gzip\r\nConnection: close\r\n\r\ndata".to_vec(),
            b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: http://169.254.169.254/\r\nConnection: close\r\n\r\n".to_vec(),
        ];
        for response in responses {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let url = Url::parse(&format!("http://{}/file", listener.local_addr()?))?;
            let server = tokio::spawn(fake_raw_http_response(listener, response));
            let client = web_seed_client(&url, true).await?;
            assert!(
                fetch_http_range(&client, url, 1, 4, 8).await.is_err(),
                "malformed web-seed range response was accepted"
            );
            server.await.map_err(|error| error.to_string())??;
        }
        Ok(())
    }

    #[tokio::test]
    async fn web_seed_content_change_never_commits_the_changed_piece()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let first = Bytes::from(vec![0x31; BLOCK_BYTES * BLOCK_PIPELINE]);
        let second = Bytes::from(vec![0x32; 257]);
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let seed = format!("http://{}/payload.bin", listener.local_addr()?);
        let server = tokio::spawn(fake_changing_web_seed(
            listener,
            first.clone(),
            second.len(),
        ));
        let mut raw = multi_piece_v1_metainfo("changing.bin", &[first.clone(), second.clone()]);
        if raw.pop() != Some(b'e') {
            return Err("invalid changing web-seed fixture".into());
        }
        raw.extend_from_slice(format!("8:url-list{}:{seed}e", seed.len()).as_bytes());
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 8)?;
        let storage = StorageHandle::start_portable(&downloads, 8)?;
        let mut services = test_services(store.clone(), storage, "changing-web-seed");
        services.allow_private_web_seeds = true;
        let mut record = test_record(&metainfo, raw);
        normalize_completion(&mut record, 2);
        store.put_torrent(record.clone()).await?;

        assert!(
            download_from_web_seeds(&metainfo, &mut record, &services, &CancellationToken::new())
                .await
                .is_err()
        );
        assert!(bit_is_set(&record.completed_pieces, 0));
        assert!(!bit_is_set(&record.completed_pieces, 1));
        assert_eq!(record.downloaded, first.len() as u64);
        let payload = tokio::fs::read(downloads.join("changing.bin")).await?;
        assert_eq!(&payload[..first.len()], first.as_ref());
        assert!(payload[first.len()..].iter().all(|byte| *byte == 0));
        server.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn swarm_schedules_distinct_peers_and_reassembles_reverse_order_blocks()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let first = Bytes::from(vec![0x11; BLOCK_BYTES * BLOCK_PIPELINE]);
        let second = Bytes::from(vec![0x22; BLOCK_BYTES * BLOCK_PIPELINE]);
        let raw = multi_piece_v1_metainfo("swarm.bin", &[first.clone(), second.clone()]);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let first_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let second_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addresses = vec![first_listener.local_addr()?, second_listener.local_addr()?];
        let first_peer = tokio::spawn(fake_pipelined_peer(
            first_listener,
            info_hash,
            0,
            first.clone(),
        ));
        let second_peer = tokio::spawn(fake_pipelined_peer(
            second_listener,
            info_hash,
            1,
            second.clone(),
        ));
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start(&downloads, 32)?;
        let (event_sender, _) = broadcast::channel(16);
        let services = Services {
            store: store.clone(),
            storage,
            tracker_response_limit: 64 * 1024,
            metainfo_limit: 64 * 1024,
            peer_message_timeout: PEER_MESSAGE_TIMEOUT,
            allow_private_web_seeds: false,
            dht_bootstrap: Vec::new(),
            dht: None,
            utp: None,
            peer_port: 6881,
            advertised_peer_port: Arc::new(AtomicU16::new(6881)),
            peer_id: generate_peer_id(),
            events: event_sender,
            peer_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            per_torrent_peer_limit: per_torrent_peer_limit(INCOMING_PEER_LIMIT),
            lsd_cookie: "endgame-test".to_owned(),
            encryption: EncryptionPolicy::Disabled,
            rendezvous: Arc::new(Mutex::new(HashMap::new())),
            connected_peers: Arc::new(AtomicUsize::new(0)),
            torrent_peers: Arc::new(std::sync::Mutex::new(HashMap::new())),
            payload_claims: Arc::new(std::sync::Mutex::new(HashMap::new())),
            shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
        };
        let mut record = test_record(&metainfo, raw);
        normalize_completion(&mut record, piece_count(&metainfo)?);
        store.put_torrent(record.clone()).await?;
        run_peer_swarm(
            addresses,
            info_hash,
            &metainfo,
            &mut record,
            &services,
            &CancellationToken::new(),
        )
        .await?;
        let written = tokio::fs::read(downloads.join("swarm.bin")).await?;
        assert_eq!(&written[..first.len()], first.as_ref());
        assert_eq!(&written[first.len()..], second.as_ref());
        first_peer.await.map_err(|error| error.to_string())??;
        second_peer.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn global_connection_pressure_cannot_starve_another_torrent()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let blocked_payload = Bytes::from_static(b"the first torrent never answers");
        let fast_payload = Bytes::from_static(b"the second torrent must still make progress");
        let blocked_raw =
            multi_piece_v1_metainfo("blocked.bin", std::slice::from_ref(&blocked_payload));
        let fast_raw = multi_piece_v1_metainfo("fast.bin", std::slice::from_ref(&fast_payload));
        let blocked_metainfo = Metainfo::parse(&blocked_raw, BencodeLimits::default())?;
        let fast_metainfo = Metainfo::parse(&fast_raw, BencodeLimits::default())?;
        let blocked_hash = wire_info_hash(&blocked_metainfo)?;
        let fast_hash = wire_info_hash(&fast_metainfo)?;

        let mut stalled_peers = tokio::task::JoinSet::new();
        let mut blocked_addresses = Vec::new();
        for _ in 0..8 {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            blocked_addresses.push(listener.local_addr()?);
            stalled_peers.spawn(fake_stalled_peer(listener, blocked_hash));
        }
        let fast_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let fast_address = fast_listener.local_addr()?;
        let fast_peer = tokio::spawn(fake_peer(fast_listener, fast_hash, fast_payload.clone()));

        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 16)?;
        let storage = StorageHandle::start_portable(&downloads, 16)?;
        let mut services = test_services(store.clone(), storage, "fairness");
        services.peer_slots = Arc::new(Semaphore::new(2));
        services.per_torrent_peer_limit = per_torrent_peer_limit(2);
        assert_eq!(services.per_torrent_peer_limit, 1);

        let mut blocked_record = test_record(&blocked_metainfo, blocked_raw);
        let mut fast_record = test_record(&fast_metainfo, fast_raw);
        normalize_completion(&mut blocked_record, 1);
        normalize_completion(&mut fast_record, 1);
        store.put_torrent(blocked_record.clone()).await?;
        store.put_torrent(fast_record.clone()).await?;
        let blocked_cancel = CancellationToken::new();
        let blocked_task = {
            let services = services.clone();
            let metainfo = blocked_metainfo.clone();
            let cancellation = blocked_cancel.clone();
            tokio::spawn(async move {
                run_peer_swarm(
                    blocked_addresses,
                    blocked_hash,
                    &metainfo,
                    &mut blocked_record,
                    &services,
                    &cancellation,
                )
                .await
            })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            while services.connected_peers.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "blocked torrent did not occupy its fair connection share")?;

        tokio::time::timeout(
            Duration::from_secs(2),
            run_peer_swarm(
                vec![fast_address],
                fast_hash,
                &fast_metainfo,
                &mut fast_record,
                &services,
                &CancellationToken::new(),
            ),
        )
        .await
        .map_err(|_| "second torrent was starved by queued connections")??;
        assert!(all_complete(&fast_record.completed_pieces, 1));
        assert_eq!(
            tokio::fs::read(downloads.join("fast.bin")).await?,
            fast_payload
        );

        blocked_cancel.cancel();
        assert!(matches!(
            blocked_task.await.map_err(|error| error.to_string())?,
            Err(ActorError::Cancelled)
        ));
        stalled_peers.abort_all();
        while stalled_peers.join_next().await.is_some() {}
        fast_peer.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn endgame_cancels_the_losing_duplicate_request()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"endgame duplicate cancellation");
        let raw = multi_piece_v1_metainfo("endgame.bin", std::slice::from_ref(&payload));
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let winner_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let loser_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addresses = vec![winner_listener.local_addr()?, loser_listener.local_addr()?];
        let winner = tokio::spawn(fake_delayed_peer(
            winner_listener,
            info_hash,
            payload.clone(),
        ));
        let loser = tokio::spawn(fake_endgame_loser(loser_listener, info_hash));
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start(&downloads, 32)?;
        let (event_sender, _) = broadcast::channel(16);
        let services = Services {
            store: store.clone(),
            storage,
            tracker_response_limit: 64 * 1024,
            metainfo_limit: 64 * 1024,
            peer_message_timeout: PEER_MESSAGE_TIMEOUT,
            allow_private_web_seeds: false,
            dht_bootstrap: Vec::new(),
            dht: None,
            utp: None,
            peer_port: 6881,
            advertised_peer_port: Arc::new(AtomicU16::new(6881)),
            peer_id: generate_peer_id(),
            events: event_sender,
            peer_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            per_torrent_peer_limit: per_torrent_peer_limit(INCOMING_PEER_LIMIT),
            lsd_cookie: "scheduler-test".to_owned(),
            encryption: EncryptionPolicy::Disabled,
            rendezvous: Arc::new(Mutex::new(HashMap::new())),
            connected_peers: Arc::new(AtomicUsize::new(0)),
            torrent_peers: Arc::new(std::sync::Mutex::new(HashMap::new())),
            payload_claims: Arc::new(std::sync::Mutex::new(HashMap::new())),
            shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
        };
        let mut record = test_record(&metainfo, raw);
        normalize_completion(&mut record, 1);
        store.put_torrent(record.clone()).await?;
        run_peer_swarm(
            addresses,
            info_hash,
            &metainfo,
            &mut record,
            &services,
            &CancellationToken::new(),
        )
        .await?;
        winner.await.map_err(|error| error.to_string())??;
        loser.await.map_err(|error| error.to_string())??;
        assert_eq!(
            tokio::fs::read(downloads.join("endgame.bin")).await?,
            payload
        );
        Ok(())
    }

    #[tokio::test]
    async fn v2_hash_exchange_completes_large_magnet_piece_layers()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let first = dendrite_core::Sha256Hash::from_bytes([1; 32]);
        let second = dendrite_core::Sha256Hash::from_bytes([2; 32]);
        let root = v2_parent_hash(first, second);
        let raw = large_v2_metainfo_without_layers(root);
        let preliminary =
            Metainfo::parse_allow_missing_piece_layers(&raw, BencodeLimits::default())?;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let info_hash = wire_info_hash(&preliminary)?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut peer = test_peer_connection(stream, info_hash).await?;
            loop {
                match peer.next_event().await {
                    Some(PeerEvent::Message(PeerMessage::HashRequest(request))) => {
                        let mut hashes = Vec::with_capacity(64);
                        hashes.extend_from_slice(first.as_bytes());
                        hashes.extend_from_slice(second.as_bytes());
                        peer.send(PeerMessage::Hashes {
                            request,
                            hashes: Bytes::from(hashes),
                        })
                        .await?;
                        return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
                    }
                    Some(PeerEvent::Failed(error)) => return Err(error.into()),
                    Some(PeerEvent::Disconnected) | None => {
                        return Err("hash peer disconnected".into());
                    }
                    _ => {}
                }
            }
        });
        let mut peer = PeerConnection::connect(
            address,
            Handshake {
                reserved: [0; 8],
                info_hash,
                peer_id: PeerId::from_bytes([4; 20]),
            },
            PeerCodecLimits::default(),
        )
        .await?;
        let layers = fetch_piece_layers(&mut peer, &preliminary, 64 * 1024).await?;
        assert_eq!(layers.get(&root), Some(&vec![first, second]));
        let info = metainfo_info_bytes(&raw)?;
        let complete = wrap_info_dictionary(info, None, &layers);
        assert!(Metainfo::parse(&complete, BencodeLimits::default()).is_ok());
        server.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn v2_seed_hash_response_contains_correlated_merkle_proof()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let first = dendrite_core::Sha256Hash::from_bytes([1; 32]);
        let second = dendrite_core::Sha256Hash::from_bytes([2; 32]);
        let third = dendrite_core::Sha256Hash::from_bytes([3; 32]);
        let proof = v2_hash_pair(third, v2_zero_hash(0));
        let root = v2_hash_pair(v2_hash_pair(first, second), proof);
        let raw_without_layers = three_piece_v2_metainfo_without_layers(root);
        let info = raw_info_dictionary(&raw_without_layers, 64 * 1024)?;
        let layers = BTreeMap::from([(root, vec![first, second, third])]);
        let raw = wrap_info_dictionary(&info, None, &layers);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut peer = test_peer_connection(stream, info_hash).await?;
            loop {
                match peer.next_event().await {
                    Some(PeerEvent::Message(PeerMessage::HashRequest(request))) => {
                        serve_hash_request(&peer, &metainfo, request).await?;
                        return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
                    }
                    Some(PeerEvent::Failed(error)) => return Err(error.into()),
                    Some(PeerEvent::Disconnected) | None => {
                        return Err("hash requester disconnected".into());
                    }
                    _ => {}
                }
            }
        });
        let mut client = PeerConnection::connect(
            address,
            Handshake {
                reserved: [0; 8],
                info_hash,
                peer_id: PeerId::from_bytes([8; 20]),
            },
            PeerCodecLimits::default(),
        )
        .await?;
        let request = HashRequest {
            pieces_root: root,
            base_layer: 0,
            index: 0,
            length: 2,
            proof_layers: 2,
        };
        client.send(PeerMessage::HashRequest(request)).await?;
        loop {
            match client.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Hashes {
                    request: response,
                    hashes,
                })) => {
                    assert_eq!(response, request);
                    assert_eq!(hashes.len(), 3 * 32);
                    assert_eq!(&hashes[..32], first.as_bytes());
                    assert_eq!(&hashes[32..64], second.as_bytes());
                    assert_eq!(&hashes[64..], proof.as_bytes());
                    let verified = v2_hash_pair(v2_hash_pair(first, second), proof);
                    assert_eq!(verified, root);
                    break;
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("hash seed disconnected".into()),
                _ => {}
            }
        }
        server.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn supervised_actor_downloads_and_verifies_a_piece()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"a rigorously verified local torrent payload");
        let piece_digest: [u8; 20] = Sha1::digest(&payload).into();
        let peer_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let peer_address = peer_listener.local_addr()?;
        let tracker_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let tracker_address = tracker_listener.local_addr()?;
        let tracker_url = format!("http://{tracker_address}/announce");
        let metainfo_bytes =
            single_file_metainfo(&tracker_url, "payload.bin", &payload, piece_digest);
        let metainfo = Metainfo::parse(&metainfo_bytes, BencodeLimits::default())?;
        let info_hash = metainfo.v1_info_hash.ok_or("missing v1 info hash")?;

        let tracker_task = tokio::spawn(fake_tracker(tracker_listener, peer_address));
        let peer_task = tokio::spawn(fake_superseed_peer(
            peer_listener,
            info_hash,
            payload.clone(),
        ));

        let directory = tempfile::tempdir()?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let download_directory = directory.path().join("downloads");
        std::fs::create_dir(&download_directory)?;
        let storage = StorageHandle::start(&download_directory, 32)?;
        let id = TorrentId::new();
        store
            .put_torrent(TorrentRecord {
                record_version: TorrentRecord::RECORD_VERSION,
                id,
                name: metainfo.name.clone(),
                state: TorrentState::Starting,
                v1_info_hash: metainfo.v1_info_hash,
                v2_info_hash: None,
                total_length: metainfo.total_length,
                raw_metainfo: metainfo_bytes,
                magnet_uri: None,
                completed_pieces: Vec::new(),
                downloaded: 0,
                uploaded: 0,
                added_at_unix_ms: 0,
            })
            .await?;
        let engine = EngineHandle::start(
            store.clone(),
            storage,
            64 * 1024,
            64 * 1024,
            Vec::new(),
            None,
            6881,
        );
        let mut events = engine.subscribe();
        engine.resume(id).await?;

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = events.recv().await.map_err(|error| error.to_string())?;
                if event.state == TorrentState::Error {
                    return Err(event
                        .detail
                        .unwrap_or_else(|| "unknown actor error".to_owned()));
                }
                if event.state == TorrentState::Seeding {
                    return Ok(());
                }
            }
        })
        .await
        .map_err(|_| "torrent actor timed out")??;

        let written = tokio::fs::read(directory.path().join("downloads/payload.bin")).await?;
        assert_eq!(written, payload);
        let record = store.get_torrent(id).await?.ok_or("record disappeared")?;
        assert_eq!(record.state, TorrentState::Seeding);
        assert_eq!(record.downloaded, payload.len() as u64);
        tracker_task.await.map_err(|error| error.to_string())??;
        peer_task.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn have_only_superseed_resumes_across_idle_announcements()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let first = Bytes::from(vec![0x31; BLOCK_BYTES * BLOCK_PIPELINE]);
        let second = Bytes::from(vec![0x32; BLOCK_BYTES * BLOCK_PIPELINE]);
        let third = Bytes::from(vec![0x33; BLOCK_BYTES * 2]);
        let raw = multi_piece_v1_metainfo(
            "delayed-superseed.bin",
            &[first.clone(), second.clone(), third.clone()],
        );
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let peer = tokio::spawn(fake_delayed_have_superseed(
            listener,
            info_hash,
            [second.clone(), third.clone()],
            Duration::from_millis(125),
        ));

        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start_portable(&downloads, 32)?;
        write_piece(&metainfo, 0, first.clone(), &storage).await?;
        let mut record = test_record(&metainfo, raw);
        normalize_completion(&mut record, 3);
        set_bit(&mut record.completed_pieces, 0);
        record.downloaded = first.len() as u64;
        store.put_torrent(record.clone()).await?;
        let mut services = test_services(store, storage, "delayed-superseed-test");
        services.peer_message_timeout = Duration::from_millis(50);

        tokio::time::timeout(
            Duration::from_secs(3),
            run_peer_swarm(
                vec![address],
                info_hash,
                &metainfo,
                &mut record,
                &services,
                &CancellationToken::new(),
            ),
        )
        .await
        .map_err(|_| "delayed superseed transfer timed out")??;

        assert_eq!(record.completed_pieces, [0b1110_0000]);
        assert_eq!(record.downloaded, metainfo.total_length);
        let mut expected = first.to_vec();
        expected.extend_from_slice(&second);
        expected.extend_from_slice(&third);
        assert_eq!(
            tokio::fs::read(downloads.join("delayed-superseed.bin")).await?,
            expected
        );
        peer.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn exhausted_swarm_reannounces_and_resumes_partial_progress()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let first = Bytes::from(vec![0x41; BLOCK_BYTES * BLOCK_PIPELINE]);
        let second = Bytes::from(vec![0x42; BLOCK_BYTES * 2]);
        let first_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let first_address = first_listener.local_addr()?;
        let second_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let second_address = second_listener.local_addr()?;
        let tracker_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let tracker_url = format!("http://{}/announce", tracker_listener.local_addr()?);
        let raw = multi_piece_v1_metainfo_with_tracker(
            &tracker_url,
            "reannounce.bin",
            &[first.clone(), second.clone()],
        );
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let tracker = tokio::spawn(fake_tracker_sequence(
            tracker_listener,
            [first_address, second_address],
        ));
        let first_peer = tokio::spawn(fake_piece_then_disconnect(
            first_listener,
            info_hash,
            0,
            first.clone(),
        ));
        let second_peer = tokio::spawn(fake_single_piece_peer(
            second_listener,
            info_hash,
            1,
            second.clone(),
        ));

        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start_portable(&downloads, 32)?;
        let services = test_services(store.clone(), storage, "reannounce-test");
        let mut record = test_record(&metainfo, raw);
        normalize_completion(&mut record, 2);
        store.put_torrent(record.clone()).await?;

        tokio::time::timeout(
            Duration::from_secs(5),
            download(&metainfo, &mut record, &services, &CancellationToken::new()),
        )
        .await
        .map_err(|_| "torrent did not recover after re-announcing")??;

        assert_eq!(record.state, TorrentState::Seeding);
        assert_eq!(record.completed_pieces, [0b1100_0000]);
        let mut expected = first.to_vec();
        expected.extend_from_slice(&second);
        assert_eq!(
            tokio::fs::read(downloads.join("reannounce.bin")).await?,
            expected
        );
        tracker.await.map_err(|error| error.to_string())??;
        first_peer.await.map_err(|error| error.to_string())??;
        second_peer.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn restart_resumes_a_persisted_starting_torrent()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"starting state restored after daemon restart");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let peer_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let peer_address = peer_listener.local_addr()?;
        let tracker_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let tracker_url = format!("http://{}/announce", tracker_listener.local_addr()?);
        let raw = single_file_metainfo(&tracker_url, "starting.bin", &payload, digest);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let tracker = tokio::spawn(fake_tracker(tracker_listener, peer_address));
        let peer = tokio::spawn(fake_peer(peer_listener, info_hash, payload.clone()));
        let directory = tempfile::tempdir()?;
        let database = directory.path().join("state.redb");
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let mut record = test_record(&metainfo, raw);
        record.state = TorrentState::Starting;
        let id = record.id;
        {
            let store = StateStore::open(&database)?;
            store.put_torrent(&record)?;
        }

        let store = StateStoreHandle::start(&database, 16)?;
        let engine = EngineHandle::start(
            store.clone(),
            StorageHandle::start_portable(&downloads, 16)?,
            64 * 1024,
            64 * 1024,
            Vec::new(),
            None,
            6881,
        );
        let mut events = engine.subscribe();
        engine.resume(id).await?;
        wait_for_seeding(&mut events).await?;
        assert_eq!(
            tokio::fs::read(downloads.join("starting.bin")).await?,
            payload
        );
        tracker.await.map_err(|error| error.to_string())??;
        peer.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn restart_rechecks_a_persisted_checking_torrent()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"checking state restored after daemon restart");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw = single_file_metainfo(
            "http://127.0.0.1/announce",
            "checking.bin",
            &payload,
            digest,
        );
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let directory = tempfile::tempdir()?;
        let database = directory.path().join("state.redb");
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        {
            let storage = StorageHandle::start_portable(&downloads, 8)?;
            write_piece(&metainfo, 0, payload, &storage).await?;
        }
        let mut record = test_record(&metainfo, raw);
        record.state = TorrentState::Checking;
        let id = record.id;
        {
            let store = StateStore::open(&database)?;
            store.put_torrent(&record)?;
        }

        let store = StateStoreHandle::start(&database, 16)?;
        let engine = EngineHandle::start(
            store.clone(),
            StorageHandle::start_portable(&downloads, 16)?,
            64 * 1024,
            64 * 1024,
            Vec::new(),
            None,
            6881,
        );
        let mut events = engine.subscribe();
        engine.recheck(id).await?;
        wait_for_seeding(&mut events).await?;
        let persisted = store.get_torrent(id).await?.ok_or("record disappeared")?;
        assert_eq!(persisted.completed_pieces, [0b1000_0000]);
        assert_eq!(persisted.downloaded, metainfo.total_length);
        Ok(())
    }

    #[tokio::test]
    async fn restart_preserves_partial_progress_and_downloads_only_missing_piece()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let first = Bytes::from(vec![0x11; BLOCK_BYTES * BLOCK_PIPELINE]);
        let second = Bytes::from_static(b"missing tail after restart");
        let peer_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let peer_address = peer_listener.local_addr()?;
        let tracker_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let tracker_url = format!("http://{}/announce", tracker_listener.local_addr()?);
        let raw = multi_piece_v1_metainfo_with_tracker(
            &tracker_url,
            "partial.bin",
            &[first.clone(), second.clone()],
        );
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let tracker = tokio::spawn(fake_tracker(tracker_listener, peer_address));
        let peer = tokio::spawn(fake_single_piece_peer(
            peer_listener,
            info_hash,
            1,
            second.clone(),
        ));
        let directory = tempfile::tempdir()?;
        let database = directory.path().join("state.redb");
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        {
            let storage = StorageHandle::start_portable(&downloads, 8)?;
            write_piece(&metainfo, 0, first.clone(), &storage).await?;
        }
        let mut record = test_record(&metainfo, raw);
        record.state = TorrentState::Downloading;
        record.completed_pieces = vec![0b1000_0000];
        record.downloaded = u64::try_from(first.len())?;
        let id = record.id;
        {
            let store = StateStore::open(&database)?;
            store.put_torrent(&record)?;
        }

        let store = StateStoreHandle::start(&database, 16)?;
        let engine = EngineHandle::start(
            store.clone(),
            StorageHandle::start_portable(&downloads, 16)?,
            64 * 1024,
            64 * 1024,
            Vec::new(),
            None,
            6881,
        );
        let mut events = engine.subscribe();
        engine.resume(id).await?;
        wait_for_seeding(&mut events).await?;
        let mut expected = first.to_vec();
        expected.extend_from_slice(&second);
        assert_eq!(
            tokio::fs::read(downloads.join("partial.bin")).await?,
            expected
        );
        let persisted = store.get_torrent(id).await?.ok_or("record disappeared")?;
        assert_eq!(persisted.completed_pieces, [0b1100_0000]);
        assert_eq!(persisted.downloaded, metainfo.total_length);
        tracker.await.map_err(|error| error.to_string())??;
        peer.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[cfg(feature = "fault-injection")]
    #[tokio::test]
    async fn storage_full_never_commits_piece_progress()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"this verified piece cannot fit on disk");
        let piece_digest: [u8; 20] = Sha1::digest(&payload).into();
        let peer_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let peer_address = peer_listener.local_addr()?;
        let tracker_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let tracker_url = format!("http://{}/announce", tracker_listener.local_addr()?);
        let raw = single_file_metainfo(&tracker_url, "full.bin", &payload, piece_digest);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let tracker_task = tokio::spawn(fake_tracker(tracker_listener, peer_address));
        let peer_task = tokio::spawn(fake_peer(peer_listener, info_hash, payload));

        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start_portable_with_write_budget(&downloads, 32, 0)?;
        let id = TorrentId::new();
        store
            .put_torrent(TorrentRecord {
                record_version: TorrentRecord::RECORD_VERSION,
                id,
                name: metainfo.name.clone(),
                state: TorrentState::Starting,
                v1_info_hash: metainfo.v1_info_hash,
                v2_info_hash: None,
                total_length: metainfo.total_length,
                raw_metainfo: raw,
                magnet_uri: None,
                completed_pieces: Vec::new(),
                downloaded: 0,
                uploaded: 0,
                added_at_unix_ms: 0,
            })
            .await?;
        let engine = EngineHandle::start(
            store.clone(),
            storage,
            64 * 1024,
            64 * 1024,
            Vec::new(),
            None,
            6881,
        );
        let mut events = engine.subscribe();
        engine.resume(id).await?;
        let detail = wait_for_error(&mut events).await?;
        assert!(detail.contains("injected storage-full fault"));

        let record = store.get_torrent(id).await?.ok_or("record disappeared")?;
        assert_eq!(record.state, TorrentState::Error);
        assert_eq!(record.completed_pieces, [0]);
        assert_eq!(record.downloaded, 0);
        assert!(!downloads.join("full.bin").exists());
        tracker_task.await.map_err(|error| error.to_string())??;
        peer_task.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[cfg(feature = "fault-injection")]
    #[tokio::test]
    async fn partial_multifile_enospc_or_eio_never_commits_and_recheck_recovers()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let first = b"first";
        let second = b"second-segment";
        let mut joined = first.to_vec();
        joined.extend_from_slice(second);
        let payload = Bytes::from(joined);
        let raw = two_file_v1_metainfo("root", "a", first, "b", second);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;

        for (case, kind) in [
            ("enospc", std::io::ErrorKind::StorageFull),
            ("eio", std::io::ErrorKind::Other),
        ] {
            let directory = tempfile::tempdir()?;
            let downloads = directory.path().join("downloads");
            std::fs::create_dir(&downloads)?;
            let database = directory.path().join("state.redb");
            let store = StateStoreHandle::start(&database, 8)?;
            let mut record = test_record(&metainfo, raw.clone());
            normalize_completion(&mut record, 1);
            store.put_torrent(record.clone()).await?;
            let faulting = StorageHandle::start_portable_with_write_fault(
                &downloads,
                8,
                u64::try_from(first.len())?,
                kind,
            )?;
            let error = match write_piece(&metainfo, 0, payload.clone(), &faulting).await {
                Ok(()) => return Err(format!("{case} did not interrupt the second file").into()),
                Err(error) => error,
            };
            assert!(matches!(
                error,
                ActorError::Storage(StorageError::Io(ref source)) if source.kind() == kind
            ));
            assert_eq!(tokio::fs::read(downloads.join("root/a")).await?, first);
            assert!(!downloads.join("root/b").exists());
            let unchanged = store
                .get_torrent(record.id)
                .await?
                .ok_or("record disappeared after partial write")?;
            assert_eq!(unchanged.completed_pieces, [0]);
            assert_eq!(unchanged.downloaded, 0);

            drop(faulting);
            let storage = StorageHandle::start_portable(&downloads, 8)?;
            let services = test_services(store.clone(), storage.clone(), case);
            recheck(&metainfo, &mut record, &services, &CancellationToken::new()).await?;
            assert_eq!(record.state, TorrentState::Stopped);
            assert_eq!(record.completed_pieces, [0]);
            assert_eq!(record.downloaded, 0);

            write_piece(&metainfo, 0, payload.clone(), &storage).await?;
            recheck(&metainfo, &mut record, &services, &CancellationToken::new()).await?;
            assert_eq!(record.state, TorrentState::Seeding);
            assert_eq!(record.completed_pieces, [0b1000_0000]);
            assert_eq!(record.downloaded, metainfo.total_length);
        }
        Ok(())
    }

    #[tokio::test]
    async fn pause_recheck_and_forget_win_against_inflight_peer_callbacks()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for action in ["pause", "recheck", "forget"] {
            let payload = Bytes::from_static(b"late peer callback must not win lifecycle race");
            let digest: [u8; 20] = Sha1::digest(&payload).into();
            let peer_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let peer_address = peer_listener.local_addr()?;
            let tracker_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let tracker_url = format!("http://{}/announce", tracker_listener.local_addr()?);
            let raw = single_file_metainfo(&tracker_url, "race.bin", &payload, digest);
            let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
            let info_hash = wire_info_hash(&metainfo)?;
            let tracker = tokio::spawn(fake_tracker(tracker_listener, peer_address));
            let (request_seen, request_waiter) = oneshot::channel();
            let peer = tokio::spawn(fake_signaled_delayed_peer(
                peer_listener,
                info_hash,
                payload,
                request_seen,
            ));

            let directory = tempfile::tempdir()?;
            let downloads = directory.path().join("downloads");
            std::fs::create_dir(&downloads)?;
            let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
            let storage = StorageHandle::start_portable(&downloads, 32)?;
            let mut record = test_record(&metainfo, raw);
            record.state = TorrentState::Starting;
            let id = record.id;
            store.put_torrent(record).await?;
            let engine = EngineHandle::start(
                store.clone(),
                storage,
                64 * 1024,
                64 * 1024,
                Vec::new(),
                None,
                6881,
            );
            let mut events = engine.subscribe();
            engine.resume(id).await?;
            tokio::time::timeout(Duration::from_secs(5), request_waiter)
                .await
                .map_err(|_| format!("{action} peer request timed out"))??;

            match action {
                "pause" => {
                    tokio::time::timeout(Duration::from_secs(5), engine.pause(id))
                        .await
                        .map_err(|_| "pause acknowledgement timed out")??;
                }
                "recheck" => {
                    engine.recheck(id).await?;
                    wait_for_state(&mut events, TorrentState::Stopped).await?;
                }
                "forget" => {
                    tokio::time::timeout(Duration::from_secs(5), engine.forget(id))
                        .await
                        .map_err(|_| "forget acknowledgement timed out")??;
                    assert!(store.remove_torrent(id).await?);
                }
                _ => return Err("unknown lifecycle action".into()),
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
            if action == "forget" {
                assert!(store.get_torrent(id).await?.is_none());
            } else {
                let persisted = store
                    .get_torrent(id)
                    .await?
                    .ok_or("lifecycle race removed the record")?;
                assert_eq!(persisted.state, TorrentState::Stopped, "action={action}");
                assert_eq!(persisted.downloaded, 0, "action={action}");
                assert_eq!(persisted.completed_pieces, [0], "action={action}");
            }
            assert!(!downloads.join("race.bin").exists());
            tracker.await.map_err(|error| error.to_string())??;
            peer.await.map_err(|error| error.to_string())??;
        }
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_cancels_inflight_download_before_acknowledging()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"shutdown must beat this delayed block");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let peer_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let peer_address = peer_listener.local_addr()?;
        let tracker_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let tracker_url = format!("http://{}/announce", tracker_listener.local_addr()?);
        let raw = single_file_metainfo(&tracker_url, "shutdown.bin", &payload, digest);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let tracker = tokio::spawn(fake_tracker(tracker_listener, peer_address));
        let (request_seen, request_waiter) = oneshot::channel();
        let peer = tokio::spawn(fake_signaled_delayed_peer(
            peer_listener,
            info_hash,
            payload,
            request_seen,
        ));
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 16)?;
        let mut record = test_record(&metainfo, raw);
        record.state = TorrentState::Starting;
        let id = record.id;
        store.put_torrent(record).await?;
        let engine = EngineHandle::start(
            store.clone(),
            StorageHandle::start_portable(&downloads, 16)?,
            64 * 1024,
            64 * 1024,
            Vec::new(),
            None,
            6881,
        );
        engine.resume(id).await?;
        tokio::time::timeout(Duration::from_secs(5), request_waiter)
            .await
            .map_err(|_| "shutdown peer request timed out")??;
        tokio::time::timeout(Duration::from_secs(5), engine.shutdown())
            .await
            .map_err(|_| "engine shutdown acknowledgement timed out")??;
        assert!(matches!(engine.resume(id).await, Err(EngineError::Stopped)));
        tokio::time::sleep(Duration::from_millis(250)).await;
        let persisted = store.get_torrent(id).await?.ok_or("record disappeared")?;
        assert_eq!(persisted.downloaded, 0);
        assert_eq!(persisted.completed_pieces, [0]);
        assert!(!downloads.join("shutdown.bin").exists());
        tracker.await.map_err(|error| error.to_string())??;
        peer.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn network_loss_mid_piece_retries_another_peer()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from(vec![0x5a; 8 * 1024]);
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw = single_file_metainfo(
            "http://127.0.0.1/announce",
            "network-recovery.bin",
            &payload,
            digest,
        );
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let broken_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let broken_address = broken_listener.local_addr()?;
        let healthy_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let healthy_address = healthy_listener.local_addr()?;
        let broken = tokio::spawn(fake_disconnect_mid_piece(broken_listener, info_hash));
        let healthy_payload = payload.clone();
        let healthy = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            fake_peer(healthy_listener, info_hash, healthy_payload).await
        });

        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let services = test_services(
            store.clone(),
            StorageHandle::start_portable(&downloads, 32)?,
            "network-loss-test",
        );
        let mut record = test_record(&metainfo, raw);
        normalize_completion(&mut record, 1);
        store.put_torrent(record.clone()).await?;
        tokio::time::timeout(
            Duration::from_secs(10),
            run_peer_swarm(
                vec![broken_address, healthy_address],
                info_hash,
                &metainfo,
                &mut record,
                &services,
                &CancellationToken::new(),
            ),
        )
        .await
        .map_err(|_| "network-loss recovery timed out")??;

        assert_eq!(
            tokio::fs::read(downloads.join("network-recovery.bin")).await?,
            payload
        );
        assert_eq!(record.completed_pieces, [0b1000_0000]);
        broken.await.map_err(|error| error.to_string())??;
        healthy.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn permanently_stalled_peer_times_out_and_healthy_peer_takes_over()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from(vec![0x3c; 8 * 1024]);
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw = single_file_metainfo(
            "http://127.0.0.1/announce",
            "stalled-recovery.bin",
            &payload,
            digest,
        );
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let stalled_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let stalled_address = stalled_listener.local_addr()?;
        let healthy_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let healthy_address = healthy_listener.local_addr()?;
        let stalled = tokio::spawn(fake_stalled_peer(stalled_listener, info_hash));
        let healthy_payload = payload.clone();
        let healthy = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            fake_peer(healthy_listener, info_hash, healthy_payload).await
        });

        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let mut services = test_services(
            store.clone(),
            StorageHandle::start_portable(&downloads, 32)?,
            "stalled-peer-test",
        );
        services.peer_message_timeout = Duration::from_millis(200);
        let mut record = test_record(&metainfo, raw);
        normalize_completion(&mut record, 1);
        store.put_torrent(record.clone()).await?;
        tokio::time::timeout(
            Duration::from_secs(5),
            run_peer_swarm(
                vec![stalled_address, healthy_address],
                info_hash,
                &metainfo,
                &mut record,
                &services,
                &CancellationToken::new(),
            ),
        )
        .await
        .map_err(|_| "stalled peer was not evicted")??;

        assert_eq!(
            tokio::fs::read(downloads.join("stalled-recovery.bin")).await?,
            payload
        );
        stalled.abort();
        let _result_ignored = stalled.await;
        healthy.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn malformed_duplicate_unsolicited_and_conflicting_blocks_are_isolated()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for behavior in [
            MaliciousBlock::WrongPiece,
            MaliciousBlock::WrongLength,
            MaliciousBlock::UnsolicitedOffset,
            MaliciousBlock::Duplicate,
            MaliciousBlock::Oversized,
        ] {
            let payload = Bytes::from(vec![0x69; BLOCK_BYTES * 2]);
            let raw =
                multi_piece_v1_metainfo("malicious-recovery.bin", std::slice::from_ref(&payload));
            let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
            let info_hash = wire_info_hash(&metainfo)?;
            let malicious_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let malicious_address = malicious_listener.local_addr()?;
            let healthy_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let healthy_address = healthy_listener.local_addr()?;
            let malicious = tokio::spawn(fake_malicious_block_peer(
                malicious_listener,
                info_hash,
                payload.clone(),
                behavior,
            ));
            let healthy_payload = payload.clone();
            let healthy = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(75)).await;
                fake_peer(healthy_listener, info_hash, healthy_payload).await
            });
            let directory = tempfile::tempdir()?;
            let downloads = directory.path().join("downloads");
            std::fs::create_dir(&downloads)?;
            let store = StateStoreHandle::start(&directory.path().join("state.redb"), 16)?;
            let services = test_services(
                store.clone(),
                StorageHandle::start_portable(&downloads, 16)?,
                "malicious-block-test",
            );
            let mut record = test_record(&metainfo, raw);
            normalize_completion(&mut record, 1);
            store.put_torrent(record.clone()).await?;
            tokio::time::timeout(
                Duration::from_secs(10),
                run_peer_swarm(
                    vec![malicious_address, healthy_address],
                    info_hash,
                    &metainfo,
                    &mut record,
                    &services,
                    &CancellationToken::new(),
                ),
            )
            .await
            .map_err(|_| format!("recovery from {behavior:?} timed out"))??;
            assert_eq!(
                tokio::fs::read(downloads.join("malicious-recovery.bin")).await?,
                payload,
                "behavior={behavior:?}"
            );
            malicious.await.map_err(|error| error.to_string())??;
            healthy.await.map_err(|error| error.to_string())??;
        }
        Ok(())
    }

    #[tokio::test]
    async fn corrupt_peer_is_rejected_before_healthy_retry_is_committed()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from(vec![0xa5; 8 * 1024]);
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw = single_file_metainfo(
            "http://127.0.0.1/announce",
            "corruption-recovery.bin",
            &payload,
            digest,
        );
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let corrupt_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let corrupt_address = corrupt_listener.local_addr()?;
        let healthy_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let healthy_address = healthy_listener.local_addr()?;
        let corrupt = tokio::spawn(fake_peer(
            corrupt_listener,
            info_hash,
            Bytes::from(vec![0xff; payload.len()]),
        ));
        let healthy_payload = payload.clone();
        let healthy = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            fake_peer(healthy_listener, info_hash, healthy_payload).await
        });

        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let services = test_services(
            store.clone(),
            StorageHandle::start_portable(&downloads, 32)?,
            "corruption-test",
        );
        let mut record = test_record(&metainfo, raw);
        normalize_completion(&mut record, 1);
        store.put_torrent(record.clone()).await?;
        tokio::time::timeout(
            Duration::from_secs(10),
            run_peer_swarm(
                vec![corrupt_address, healthy_address],
                info_hash,
                &metainfo,
                &mut record,
                &services,
                &CancellationToken::new(),
            ),
        )
        .await
        .map_err(|_| "corruption recovery timed out")??;

        assert_eq!(
            tokio::fs::read(downloads.join("corruption-recovery.bin")).await?,
            payload
        );
        assert_eq!(record.completed_pieces, [0b1000_0000]);
        corrupt.await.map_err(|error| error.to_string())??;
        healthy.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn recheck_clears_progress_for_corrupted_persisted_payload()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"payload expected to survive a restart intact");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw =
            single_file_metainfo("http://127.0.0.1/announce", "damaged.bin", &payload, digest);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start_portable(&downloads, 32)?;
        storage
            .write(
                metainfo.files[0].path.clone(),
                0,
                Bytes::from(vec![0xff; payload.len()]),
                metainfo.total_length,
            )
            .await?;
        storage.sync(metainfo.files[0].path.clone()).await?;

        let services = test_services(store.clone(), storage, "recheck-corruption-test");
        let mut record = test_record(&metainfo, raw);
        record.state = TorrentState::Seeding;
        record.completed_pieces = complete_bitfield(1);
        record.downloaded = metainfo.total_length;
        store.put_torrent(record.clone()).await?;
        recheck(&metainfo, &mut record, &services, &CancellationToken::new()).await?;

        assert_eq!(record.state, TorrentState::Stopped);
        assert_eq!(record.completed_pieces, [0]);
        assert_eq!(record.downloaded, 0);
        let persisted = store
            .get_torrent(record.id)
            .await?
            .ok_or("record disappeared")?;
        assert_eq!(persisted.state, TorrentState::Stopped);
        assert_eq!(persisted.completed_pieces, [0]);
        assert_eq!(persisted.downloaded, 0);
        Ok(())
    }

    #[tokio::test]
    async fn recheck_detects_deleted_truncated_replaced_and_modified_files()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"externally mutable payload");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw = single_file_metainfo(
            "http://127.0.0.1/announce",
            "external.bin",
            &payload,
            digest,
        );
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;

        for mutation in ["deleted", "truncated", "replaced", "modified"] {
            let directory = tempfile::tempdir()?;
            let downloads = directory.path().join("downloads");
            std::fs::create_dir(&downloads)?;
            let database = directory.path().join("state.redb");
            let payload_path = downloads.join("external.bin");
            {
                let storage = StorageHandle::start_portable(&downloads, 8)?;
                storage
                    .write(
                        metainfo.files[0].path.clone(),
                        0,
                        payload.clone(),
                        metainfo.total_length,
                    )
                    .await?;
                storage.sync(metainfo.files[0].path.clone()).await?;
            }
            let mut record = test_record(&metainfo, raw.clone());
            record.state = TorrentState::Seeding;
            record.completed_pieces = complete_bitfield(1);
            record.downloaded = metainfo.total_length;
            {
                let store = StateStore::open(&database)?;
                store.put_torrent(&record)?;
            }

            match mutation {
                "deleted" => std::fs::remove_file(&payload_path)?,
                "truncated" => std::fs::OpenOptions::new()
                    .write(true)
                    .open(&payload_path)?
                    .set_len(3)?,
                "replaced" => {
                    let replacement = downloads.join("replacement.tmp");
                    std::fs::write(&replacement, vec![0x55; payload.len()])?;
                    std::fs::remove_file(&payload_path)?;
                    std::fs::rename(replacement, &payload_path)?;
                }
                "modified" => {
                    let mut changed = payload.to_vec();
                    changed[0] ^= 0xff;
                    std::fs::write(&payload_path, changed)?;
                }
                _ => return Err("unknown external mutation".into()),
            }

            let store = StateStoreHandle::start(&database, 8)?;
            let storage = StorageHandle::start_portable(&downloads, 8)?;
            let services = test_services(store.clone(), storage, mutation);
            recheck(&metainfo, &mut record, &services, &CancellationToken::new()).await?;
            assert_eq!(record.state, TorrentState::Stopped, "mutation={mutation}");
            assert_eq!(record.completed_pieces, [0], "mutation={mutation}");
            assert_eq!(record.downloaded, 0, "mutation={mutation}");
            let persisted = store
                .get_torrent(record.id)
                .await?
                .ok_or("record disappeared after external mutation")?;
            assert_eq!(persisted.completed_pieces, [0], "mutation={mutation}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn crash_point_matrix_recovers_payload_and_progress_consistently()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (point, lose_unsynced_payload, expected_complete) in [
            ("before-write", false, false),
            ("after-write", false, true),
            ("after-write", true, false),
            ("after-sync", false, true),
            ("after-commit", false, true),
        ] {
            let directory = tempfile::tempdir()?;
            let downloads = directory.path().join("downloads");
            std::fs::create_dir(&downloads)?;
            let database = directory.path().join("state.redb");
            let digest: [u8; 20] = Sha1::digest(CRASH_TEST_PAYLOAD).into();
            let raw = single_file_metainfo(
                "http://127.0.0.1/announce",
                "crash.bin",
                CRASH_TEST_PAYLOAD,
                digest,
            );
            let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
            let mut initial = test_record(&metainfo, raw);
            initial.state = TorrentState::Starting;
            {
                let store = StateStore::open(&database)?;
                store.put_torrent(&initial)?;
            }

            let status = Command::new(std::env::current_exe()?)
                .arg("--ignored")
                .arg("--exact")
                .arg("tests::crash_boundary_child")
                .env("DENDRITE_CRASH_TEST_ROOT", directory.path())
                .env("DENDRITE_CRASH_TEST_POINT", point)
                .status()?;
            assert_eq!(status.code(), Some(86), "child did not crash at {point}");

            if lose_unsynced_payload {
                std::fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(downloads.join("crash.bin"))?;
            }

            let store = StateStoreHandle::start(&database, 8)?;
            let storage = StorageHandle::start_portable(&downloads, 8)?;
            let services = test_services(store.clone(), storage, "crash-recovery-test");
            let mut recovered = store
                .get_torrent(initial.id)
                .await?
                .ok_or("crashed child lost the torrent record")?;
            normalize_completion(&mut recovered, piece_count(&metainfo)?);
            recheck(
                &metainfo,
                &mut recovered,
                &services,
                &CancellationToken::new(),
            )
            .await?;
            assert_eq!(
                recovered.state == TorrentState::Seeding,
                expected_complete,
                "unexpected recovery state at {point} (lost={lose_unsynced_payload})"
            );
            assert_eq!(
                recovered.downloaded,
                if expected_complete {
                    metainfo.total_length
                } else {
                    0
                }
            );
            assert_eq!(
                bit_is_set(&recovered.completed_pieces, 0),
                expected_complete
            );
        }
        Ok(())
    }

    #[test]
    #[ignore = "subprocess helper for crash_point_matrix_recovers_payload_and_progress_consistently"]
    fn crash_boundary_child() -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::var_os("DENDRITE_CRASH_TEST_ROOT")
            .map(std::path::PathBuf::from)
            .ok_or("crash-test root is missing")?;
        let point = std::env::var("DENDRITE_CRASH_TEST_POINT")?;
        if point == "before-write" {
            std::process::exit(86);
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let downloads = root.join("downloads");
            let storage = StorageHandle::start_portable(&downloads, 8)?;
            let path = TorrentPath::new(["crash.bin".to_owned()])?;
            storage
                .write(
                    path.clone(),
                    0,
                    Bytes::from_static(CRASH_TEST_PAYLOAD),
                    u64::try_from(CRASH_TEST_PAYLOAD.len())?,
                )
                .await?;
            if point == "after-write" {
                std::process::exit(86);
            }
            storage.sync(path).await?;
            if point == "after-sync" {
                std::process::exit(86);
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        })?;

        if point != "after-commit" {
            return Err(format!("unknown crash point {point}").into());
        }
        let store = StateStore::open(&root.join("state.redb"))?;
        let mut record = store
            .list_torrents()?
            .into_iter()
            .next()
            .ok_or("crash-test record disappeared")?;
        record.completed_pieces = complete_bitfield(1);
        record.downloaded = record.total_length;
        record.state = TorrentState::Downloading;
        store.put_torrent(&record)?;
        std::process::exit(86);
    }

    #[tokio::test]
    async fn supervised_actor_fetches_magnet_metadata_before_transfer()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"metadata exchange precedes this verified payload");
        let piece_digest: [u8; 20] = Sha1::digest(&payload).into();
        let peer_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let peer_address = peer_listener.local_addr()?;
        let tracker_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let tracker_address = tracker_listener.local_addr()?;
        let tracker_url = format!("http://{tracker_address}/announce");
        let full_metainfo =
            single_file_metainfo(&tracker_url, "magnet.bin", &payload, piece_digest);
        let metainfo = Metainfo::parse(&full_metainfo, BencodeLimits::default())?;
        let info_hash = metainfo.v1_info_hash.ok_or("missing v1 info hash")?;
        let info = metainfo_info_bytes(&full_metainfo)?.to_vec();
        let mut magnet = Url::parse("magnet:?")?;
        magnet
            .query_pairs_mut()
            .append_pair("xt", &format!("urn:btih:{info_hash}"))
            .append_pair("dn", "magnet.bin")
            .append_pair("tr", &tracker_url);

        let tracker_task = tokio::spawn(fake_tracker_many(tracker_listener, peer_address, 2));
        let peer_task = tokio::spawn(fake_metadata_then_payload_peer(
            peer_listener,
            info_hash,
            Bytes::from(info),
            payload.clone(),
        ));
        let directory = tempfile::tempdir()?;
        let download_directory = directory.path().join("downloads");
        std::fs::create_dir(&download_directory)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start(&download_directory, 32)?;
        let id = TorrentId::new();
        store
            .put_torrent(TorrentRecord {
                record_version: TorrentRecord::RECORD_VERSION,
                id,
                name: "magnet.bin".to_owned(),
                state: TorrentState::Starting,
                v1_info_hash: Some(info_hash),
                v2_info_hash: None,
                total_length: 0,
                raw_metainfo: Vec::new(),
                magnet_uri: Some(magnet.to_string()),
                completed_pieces: Vec::new(),
                downloaded: 0,
                uploaded: 0,
                added_at_unix_ms: 0,
            })
            .await?;
        let engine = EngineHandle::start(
            store.clone(),
            storage,
            64 * 1024,
            64 * 1024,
            Vec::new(),
            None,
            6881,
        );
        let mut events = engine.subscribe();
        engine.resume(id).await?;
        wait_for_seeding(&mut events).await?;

        let record = store.get_torrent(id).await?.ok_or("record disappeared")?;
        assert!(!record.raw_metainfo.is_empty());
        assert_eq!(record.total_length, payload.len() as u64);
        assert_eq!(
            tokio::fs::read(download_directory.join("magnet.bin")).await?,
            payload
        );
        tracker_task.await.map_err(|error| error.to_string())??;
        peer_task.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn hostile_metadata_size_piece_and_block_confusion_is_isolated()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        const LIMIT: usize = METADATA_BLOCK_BYTES * 2;
        for attack in [
            MetadataAttack::ZeroAdvertised,
            MetadataAttack::OversizedAdvertised,
            MetadataAttack::WrongPiece,
            MetadataAttack::ChangedTotal,
            MetadataAttack::OversizedBlock,
            MetadataAttack::ConflictingDuplicate,
        ] {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let address = listener.local_addr()?;
            let info_hash = Sha1Hash::from_bytes([attack.tag(); 20]);
            let peer = tokio::spawn(fake_hostile_metadata_peer(
                listener, info_hash, attack, LIMIT,
            ));
            let directory = tempfile::tempdir()?;
            let store = StateStoreHandle::start(&directory.path().join("state.redb"), 4)?;
            let downloads = directory.path().join("downloads");
            std::fs::create_dir(&downloads)?;
            let storage = StorageHandle::start_portable(&downloads, 4)?;
            let mut services = test_services(store, storage, "hostile-metadata");
            services.metainfo_limit = LIMIT;
            let result = tokio::time::timeout(
                Duration::from_secs(2),
                fetch_metadata(address, info_hash, &services, &CancellationToken::new()),
            )
            .await
            .map_err(|_| format!("{attack:?} metadata peer was not bounded"))?;
            assert!(
                result.is_err(),
                "{attack:?} metadata peer unexpectedly succeeded"
            );
            peer.await.map_err(|error| error.to_string())??;
        }
        Ok(())
    }

    #[tokio::test]
    async fn magnet_metadata_retries_repeated_hash_failures_before_healthy_peer()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"healthy metadata payload");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let full = single_file_metainfo("http://unused.invalid/", "healthy.bin", &payload, digest);
        let parsed = Metainfo::parse(&full, BencodeLimits::default())?;
        let info_hash = parsed.v1_info_hash.ok_or("missing v1 hash")?;
        let info = Bytes::copy_from_slice(metainfo_info_bytes(&full)?);

        let bad_one_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let bad_two_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let healthy_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addresses = vec![
            bad_one_listener.local_addr()?,
            bad_two_listener.local_addr()?,
            healthy_listener.local_addr()?,
        ];
        let bad_one = tokio::spawn(fake_metadata_only_peer(
            bad_one_listener,
            info_hash,
            Bytes::from_static(b"d4:name3:bade"),
        ));
        let mut altered = info.to_vec();
        let last = altered.last_mut().ok_or("empty info dictionary")?;
        *last = if *last == b'e' { b'd' } else { b'e' };
        let bad_two = tokio::spawn(fake_metadata_only_peer(
            bad_two_listener,
            info_hash,
            Bytes::from(altered),
        ));
        let healthy = tokio::spawn(fake_metadata_only_peer(healthy_listener, info_hash, info));
        let mut magnet = Url::parse("magnet:?")?;
        magnet
            .query_pairs_mut()
            .append_pair("xt", &format!("urn:btih:{info_hash}"));
        let parsed_magnet = Magnet::parse(magnet.as_str())?;

        let directory = tempfile::tempdir()?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 8)?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let storage = StorageHandle::start_portable(&downloads, 8)?;
        let services = test_services(store.clone(), storage, "metadata-retry");
        let mut record = TorrentRecord {
            record_version: TorrentRecord::RECORD_VERSION,
            id: TorrentId::new(),
            name: info_hash.to_string(),
            state: TorrentState::Starting,
            v1_info_hash: Some(info_hash),
            v2_info_hash: None,
            total_length: 0,
            raw_metainfo: Vec::new(),
            magnet_uri: Some(magnet.to_string()),
            completed_pieces: Vec::new(),
            downloaded: 0,
            uploaded: 0,
            added_at_unix_ms: 0,
        };
        store.put_torrent(record.clone()).await?;
        acquire_metadata_from_peers(
            &mut record,
            &parsed_magnet,
            info_hash,
            addresses,
            &services,
            &CancellationToken::new(),
        )
        .await?;
        assert_eq!(record.name, "healthy.bin");
        assert_eq!(record.total_length, payload.len() as u64);
        assert!(!record.raw_metainfo.is_empty());
        bad_one.await.map_err(|error| error.to_string())??;
        bad_two.await.map_err(|error| error.to_string())??;
        healthy.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn supervised_actor_downloads_and_merkle_verifies_v2()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"a native v2 merkle-verified payload");
        let peer_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let peer_address = peer_listener.local_addr()?;
        let tracker_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let tracker_url = format!("http://{}/announce", tracker_listener.local_addr()?);
        let raw = single_file_v2_metainfo(&tracker_url, "root", "payload.bin", &payload);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let wire_hash = wire_info_hash(&metainfo)?;
        let tracker_task = tokio::spawn(fake_tracker(tracker_listener, peer_address));
        let peer_task = tokio::spawn(fake_peer(peer_listener, wire_hash, payload.clone()));
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start(&downloads, 32)?;
        let id = TorrentId::new();
        store
            .put_torrent(TorrentRecord {
                record_version: TorrentRecord::RECORD_VERSION,
                id,
                name: metainfo.name.clone(),
                state: TorrentState::Starting,
                v1_info_hash: None,
                v2_info_hash: metainfo.v2_info_hash,
                total_length: metainfo.total_length,
                raw_metainfo: raw,
                magnet_uri: None,
                completed_pieces: Vec::new(),
                downloaded: 0,
                uploaded: 0,
                added_at_unix_ms: 0,
            })
            .await?;
        let engine = EngineHandle::start(
            store.clone(),
            storage,
            64 * 1024,
            64 * 1024,
            Vec::new(),
            None,
            6881,
        );
        let mut events = engine.subscribe();
        engine.resume(id).await?;
        wait_for_seeding(&mut events).await?;
        assert_eq!(
            tokio::fs::read(downloads.join("root/payload.bin")).await?,
            payload
        );
        tracker_task.await.map_err(|error| error.to_string())??;
        peer_task.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    async fn wait_for_seeding(
        events: &mut broadcast::Receiver<EngineEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = events.recv().await.map_err(|error| error.to_string())?;
                if event.state == TorrentState::Error {
                    return Err(event
                        .detail
                        .unwrap_or_else(|| "unknown actor error".to_owned()));
                }
                if event.state == TorrentState::Seeding {
                    return Ok(());
                }
            }
        })
        .await
        .map_err(|_| "torrent actor timed out")??;
        Ok(())
    }

    #[cfg_attr(not(feature = "fault-injection"), allow(dead_code))]
    async fn wait_for_error(
        events: &mut broadcast::Receiver<EngineEvent>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = events.recv().await.map_err(|error| error.to_string())?;
                if event.state == TorrentState::Seeding {
                    return Err("torrent unexpectedly reached seeding".to_owned());
                }
                if event.state == TorrentState::Error {
                    return event
                        .detail
                        .ok_or_else(|| "actor error omitted its detail".to_owned());
                }
            }
        })
        .await
        .map_err(|_| "torrent actor error timed out")?
        .map_err(Into::into)
    }

    async fn wait_for_state(
        events: &mut broadcast::Receiver<EngineEvent>,
        expected: TorrentState,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = events.recv().await.map_err(|error| error.to_string())?;
                if event.state == TorrentState::Error {
                    return Err(event
                        .detail
                        .unwrap_or_else(|| "unknown actor error".to_owned()));
                }
                if event.state == expected {
                    return Ok(());
                }
            }
        })
        .await
        .map_err(|_| format!("torrent did not reach {expected:?}"))??;
        Ok(())
    }

    fn metainfo_info_bytes(
        input: &[u8],
    ) -> Result<&[u8], Box<dyn std::error::Error + Send + Sync>> {
        let root = decode(input, BencodeLimits::default())?;
        let info = root
            .value
            .dictionary_get(b"info")
            .ok_or("metainfo has no info dictionary")?;
        Ok(&input[info.span.clone()])
    }

    fn single_file_metainfo(
        tracker: &str,
        name: &str,
        payload: &[u8],
        piece_digest: [u8; 20],
    ) -> Vec<u8> {
        let mut info = format!(
            "d6:lengthi{}e4:name{}:{}12:piece lengthi16384e6:pieces20:",
            payload.len(),
            name.len(),
            name
        )
        .into_bytes();
        info.extend_from_slice(&piece_digest);
        info.push(b'e');
        let mut metainfo = format!("d8:announce{}:{}4:info", tracker.len(), tracker).into_bytes();
        metainfo.extend_from_slice(&info);
        metainfo.push(b'e');
        metainfo
    }

    fn multi_piece_v1_metainfo(name: &str, pieces: &[Bytes]) -> Vec<u8> {
        let length: usize = pieces.iter().map(Bytes::len).sum();
        let mut info = format!(
            "d6:lengthi{length}e4:name{}:{name}12:piece lengthi{}e6:pieces{}:",
            name.len(),
            BLOCK_BYTES * BLOCK_PIPELINE,
            pieces.len() * 20
        )
        .into_bytes();
        for piece in pieces {
            info.extend_from_slice(&Sha1::digest(piece));
        }
        info.extend_from_slice(b"ee");
        let mut metainfo = b"d4:info".to_vec();
        metainfo.extend_from_slice(&info);
        metainfo
    }

    fn multi_piece_v1_metainfo_with_tracker(
        tracker: &str,
        name: &str,
        pieces: &[Bytes],
    ) -> Vec<u8> {
        let length: usize = pieces.iter().map(Bytes::len).sum();
        let mut info = format!(
            "d6:lengthi{length}e4:name{}:{name}12:piece lengthi{}e6:pieces{}:",
            name.len(),
            BLOCK_BYTES * BLOCK_PIPELINE,
            pieces.len() * 20
        )
        .into_bytes();
        for piece in pieces {
            info.extend_from_slice(&Sha1::digest(piece));
        }
        info.push(b'e');
        let mut metainfo = format!("d8:announce{}:{}4:info", tracker.len(), tracker).into_bytes();
        metainfo.extend_from_slice(&info);
        metainfo.push(b'e');
        metainfo
    }

    #[cfg_attr(not(feature = "fault-injection"), allow(dead_code))]
    fn two_file_v1_metainfo(
        root: &str,
        first_name: &str,
        first: &[u8],
        second_name: &str,
        second: &[u8],
    ) -> Vec<u8> {
        let mut payload = first.to_vec();
        payload.extend_from_slice(second);
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let mut info = format!(
            "d5:filesld6:lengthi{}e4:pathl{}:{}eed6:lengthi{}e4:pathl{}:{}eee4:name{}:{}12:piece lengthi16384e6:pieces20:",
            first.len(),
            first_name.len(),
            first_name,
            second.len(),
            second_name.len(),
            second_name,
            root.len(),
            root,
        )
        .into_bytes();
        info.extend_from_slice(&digest);
        info.push(b'e');
        let mut metainfo = b"d4:info".to_vec();
        metainfo.extend_from_slice(&info);
        metainfo.push(b'e');
        metainfo
    }

    fn test_record(metainfo: &Metainfo, raw: Vec<u8>) -> TorrentRecord {
        TorrentRecord {
            record_version: TorrentRecord::RECORD_VERSION,
            id: TorrentId::new(),
            name: metainfo.name.clone(),
            state: TorrentState::Downloading,
            v1_info_hash: metainfo.v1_info_hash,
            v2_info_hash: metainfo.v2_info_hash,
            total_length: metainfo.total_length,
            raw_metainfo: raw,
            magnet_uri: None,
            completed_pieces: Vec::new(),
            downloaded: 0,
            uploaded: 0,
            added_at_unix_ms: 0,
        }
    }

    fn test_services(store: StateStoreHandle, storage: StorageHandle, cookie: &str) -> Services {
        let (events, _) = broadcast::channel(16);
        Services {
            store,
            storage,
            tracker_response_limit: 64 * 1024,
            metainfo_limit: 64 * 1024,
            peer_message_timeout: PEER_MESSAGE_TIMEOUT,
            allow_private_web_seeds: false,
            dht_bootstrap: Vec::new(),
            dht: None,
            utp: None,
            peer_port: 6881,
            advertised_peer_port: Arc::new(AtomicU16::new(6881)),
            peer_id: generate_peer_id(),
            events,
            peer_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            per_torrent_peer_limit: per_torrent_peer_limit(INCOMING_PEER_LIMIT),
            lsd_cookie: cookie.to_owned(),
            encryption: EncryptionPolicy::Disabled,
            rendezvous: Arc::new(Mutex::new(HashMap::new())),
            connected_peers: Arc::new(AtomicUsize::new(0)),
            torrent_peers: Arc::new(std::sync::Mutex::new(HashMap::new())),
            payload_claims: Arc::new(std::sync::Mutex::new(HashMap::new())),
            shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
        }
    }

    fn single_file_v2_metainfo(
        tracker: &str,
        name: &str,
        file_name: &str,
        payload: &[u8],
    ) -> Vec<u8> {
        let root: [u8; 32] = Sha256::digest(payload).into();
        let mut info = format!(
            "d9:file treed{}:{}d0:d6:lengthi{}e11:pieces root32:",
            file_name.len(),
            file_name,
            payload.len()
        )
        .into_bytes();
        info.extend_from_slice(&root);
        info.extend_from_slice(
            format!(
                "eee12:meta versioni2e4:name{}:{}12:piece lengthi16384ee",
                name.len(),
                name
            )
            .as_bytes(),
        );
        let mut metainfo = format!("d8:announce{}:{}4:info", tracker.len(), tracker).into_bytes();
        metainfo.extend_from_slice(&info);
        metainfo.push(b'e');
        metainfo
    }

    fn two_file_v2_metainfo(first: &[u8], second: &[u8]) -> Vec<u8> {
        let first_root: [u8; 32] = Sha256::digest(first).into();
        let second_root: [u8; 32] = Sha256::digest(second).into();
        let mut info = format!(
            "d9:file treed1:ad0:d6:lengthi{}e11:pieces root32:",
            first.len()
        )
        .into_bytes();
        info.extend_from_slice(&first_root);
        info.extend_from_slice(
            format!("ee1:bd0:d6:lengthi{}e11:pieces root32:", second.len()).as_bytes(),
        );
        info.extend_from_slice(&second_root);
        info.extend_from_slice(b"eee12:meta versioni2e4:name4:root12:piece lengthi16384ee");
        let mut metainfo = b"d4:info".to_vec();
        metainfo.extend_from_slice(&info);
        metainfo.push(b'e');
        metainfo
    }

    fn hybrid_metainfo(first: &[u8], second: &[u8]) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let padding = BLOCK_BYTES - first.len();
        let first_root: [u8; 32] = Sha256::digest(first).into();
        let second_root: [u8; 32] = Sha256::digest(second).into();
        let mut first_piece = first.to_vec();
        first_piece.resize(BLOCK_BYTES, 0);
        let second_piece = second.to_vec();
        let first_v1: [u8; 20] = Sha1::digest(&first_piece).into();
        let second_v1: [u8; 20] = Sha1::digest(&second_piece).into();
        let mut info = format!(
            "d9:file treed1:ad0:d6:lengthi{}e11:pieces root32:",
            first.len()
        )
        .into_bytes();
        info.extend_from_slice(&first_root);
        info.extend_from_slice(
            format!("ee1:bd0:d6:lengthi{}e11:pieces root32:", second.len()).as_bytes(),
        );
        info.extend_from_slice(&second_root);
        info.extend_from_slice(
            format!("eee5:filesld6:lengthi{}e4:pathl1:aee", first.len()).as_bytes(),
        );
        info.extend_from_slice(
            format!(
                "d4:attr1:p6:lengthi{padding}e4:pathl4:.pad{}:{padding}ee",
                padding.to_string().len()
            )
            .as_bytes(),
        );
        info.extend_from_slice(format!("d6:lengthi{}e4:pathl1:bee", second.len()).as_bytes());
        info.extend_from_slice(b"e12:meta versioni2e4:name4:root12:piece lengthi16384e6:pieces40:");
        info.extend_from_slice(&first_v1);
        info.extend_from_slice(&second_v1);
        info.extend_from_slice(b"ee");
        let mut metainfo = b"d4:info".to_vec();
        metainfo.extend_from_slice(&info);
        (metainfo, first_piece, second_piece)
    }

    fn large_v2_metainfo_without_layers(root: dendrite_core::Sha256Hash) -> Vec<u8> {
        let mut input = b"d4:infod9:file treed4:filed0:d6:lengthi32768e11:pieces root32:".to_vec();
        input.extend_from_slice(root.as_bytes());
        input.extend_from_slice(b"eee12:meta versioni2e4:name4:root12:piece lengthi16384eee");
        input
    }

    fn three_piece_v2_metainfo_without_layers(root: dendrite_core::Sha256Hash) -> Vec<u8> {
        let mut input = b"d4:infod9:file treed4:filed0:d6:lengthi49152e11:pieces root32:".to_vec();
        input.extend_from_slice(root.as_bytes());
        input.extend_from_slice(b"eee12:meta versioni2e4:name4:root12:piece lengthi16384eee");
        input
    }

    fn v2_parent_hash(
        first: dendrite_core::Sha256Hash,
        second: dendrite_core::Sha256Hash,
    ) -> dendrite_core::Sha256Hash {
        let mut digest = Sha256::new();
        digest.update(first.as_bytes());
        digest.update(second.as_bytes());
        dendrite_core::Sha256Hash::from_bytes(digest.finalize().into())
    }

    async fn fake_tracker(
        listener: TcpListener,
        peer: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (mut stream, _) = listener.accept().await?;
        let mut request = Vec::new();
        loop {
            let mut chunk = [0_u8; 1024];
            let read = stream.read(&mut chunk).await?;
            if read == 0 || request.len().saturating_add(read) > 16 * 1024 {
                return Err("invalid tracker request".into());
            }
            request.extend_from_slice(&chunk[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        let IpAddr::V4(ip) = peer.ip() else {
            return Err("test peer must use IPv4".into());
        };
        let mut body = b"d8:intervali60e5:peers6:".to_vec();
        body.extend_from_slice(&ip.octets());
        body.extend_from_slice(&peer.port().to_be_bytes());
        body.push(b'e');
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(headers.as_bytes()).await?;
        stream.write_all(&body).await?;
        Ok(())
    }

    async fn fake_tracker_sequence(
        listener: TcpListener,
        peers: [SocketAddr; 2],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (index, peer) in peers.into_iter().enumerate() {
            let (mut stream, _) = listener.accept().await?;
            let mut request = Vec::new();
            loop {
                let mut chunk = [0_u8; 1024];
                let read = stream.read(&mut chunk).await?;
                if read == 0 || request.len().saturating_add(read) > 16 * 1024 {
                    return Err("invalid tracker request".into());
                }
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let request = std::str::from_utf8(&request)?;
            if index == 0 && !request.contains("event=started") {
                return Err("initial announce did not carry the started event".into());
            }
            if index > 0 && request.contains("event=") {
                return Err("repeat announce incorrectly repeated a lifecycle event".into());
            }
            let IpAddr::V4(ip) = peer.ip() else {
                return Err("test peer must use IPv4".into());
            };
            let mut body = b"d8:intervali60e5:peers6:".to_vec();
            body.extend_from_slice(&ip.octets());
            body.extend_from_slice(&peer.port().to_be_bytes());
            body.push(b'e');
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(headers.as_bytes()).await?;
            stream.write_all(&body).await?;
        }
        Ok(())
    }

    async fn fake_raw_http_response(
        listener: TcpListener,
        response: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (mut stream, _) = listener.accept().await?;
        let mut request = Vec::new();
        loop {
            let mut chunk = [0_u8; 1024];
            let read = stream.read(&mut chunk).await?;
            if read == 0 || request.len().saturating_add(read) > 16 * 1024 {
                return Err("invalid HTTP request".into());
            }
            request.extend_from_slice(&chunk[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        stream.write_all(&response).await?;
        Ok(())
    }

    async fn fake_changing_web_seed(
        listener: TcpListener,
        first: Bytes,
        second_length: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let total = first.len() + second_length;
        for (offset, payload) in [
            (0_usize, first),
            (
                total - second_length,
                Bytes::from(vec![0xff; second_length]),
            ),
        ] {
            let (mut stream, _) = listener.accept().await?;
            let mut request = Vec::new();
            loop {
                let mut chunk = [0_u8; 1024];
                let read = stream.read(&mut chunk).await?;
                if read == 0 || request.len().saturating_add(read) > 16 * 1024 {
                    return Err("invalid changing web-seed request".into());
                }
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let end = offset + payload.len() - 1;
            let response = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {offset}-{end}/{total}\r\nConnection: close\r\n\r\n",
                payload.len()
            );
            stream.write_all(response.as_bytes()).await?;
            stream.write_all(&payload).await?;
        }
        Ok(())
    }

    async fn fake_multifile_web_seed(
        listener: TcpListener,
        first: Vec<u8>,
        second: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await?;
            let mut request = Vec::new();
            loop {
                let mut chunk = [0_u8; 1024];
                let read = stream.read(&mut chunk).await?;
                if read == 0 || request.len().saturating_add(read) > 16 * 1024 {
                    return Err("invalid web-seed request".into());
                }
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let text = std::str::from_utf8(&request)?;
            let path = text
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .ok_or("missing web-seed path")?;
            let payload = match path {
                "/root/a" => &first,
                "/root/b" => &second,
                _ => return Err(format!("unexpected web-seed path {path}").into()),
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                payload.len()
            );
            stream.write_all(response.as_bytes()).await?;
            stream.write_all(payload).await?;
        }
        Ok(())
    }

    async fn fake_tracker_many(
        listener: TcpListener,
        peer: SocketAddr,
        requests: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for _ in 0..requests {
            let (mut stream, _) = listener.accept().await?;
            let mut request = Vec::new();
            loop {
                let mut chunk = [0_u8; 1024];
                let read = stream.read(&mut chunk).await?;
                if read == 0 || request.len().saturating_add(read) > 16 * 1024 {
                    return Err("invalid tracker request".into());
                }
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let IpAddr::V4(ip) = peer.ip() else {
                return Err("test peer must use IPv4".into());
            };
            let mut body = b"d8:intervali60e5:peers6:".to_vec();
            body.extend_from_slice(&ip.octets());
            body.extend_from_slice(&peer.port().to_be_bytes());
            body.push(b'e');
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(headers.as_bytes()).await?;
            stream.write_all(&body).await?;
        }
        Ok(())
    }

    #[derive(Clone, Copy, Debug)]
    enum MetadataAttack {
        ZeroAdvertised,
        OversizedAdvertised,
        WrongPiece,
        ChangedTotal,
        OversizedBlock,
        ConflictingDuplicate,
    }

    async fn fake_metadata_only_peer(
        listener: TcpListener,
        info_hash: Sha1Hash,
        info: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let mut peer = test_peer_connection(stream, info_hash).await?;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Extended {
                    extension_id: 0, ..
                })) => break,
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
        peer.send(PeerMessage::Extended {
            extension_id: 0,
            payload: encode_extension_handshake(Some(info.len())),
        })
        .await?;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Extended {
                    extension_id: LOCAL_METADATA_EXTENSION_ID,
                    payload,
                })) => {
                    let MetadataMessage::Request { piece: 0 } =
                        decode_metadata_message(&payload, info.len())?
                    else {
                        continue;
                    };
                    peer.send(PeerMessage::Extended {
                        extension_id: LOCAL_METADATA_EXTENSION_ID,
                        payload: encode_metadata_data(0, info.len(), &info),
                    })
                    .await?;
                    return Ok(());
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
    }

    impl MetadataAttack {
        const fn tag(self) -> u8 {
            match self {
                Self::ZeroAdvertised => 1,
                Self::OversizedAdvertised => 2,
                Self::WrongPiece => 3,
                Self::ChangedTotal => 4,
                Self::OversizedBlock => 5,
                Self::ConflictingDuplicate => 6,
            }
        }
    }

    async fn fake_hostile_metadata_peer(
        listener: TcpListener,
        info_hash: Sha1Hash,
        attack: MetadataAttack,
        limit: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let mut peer = test_peer_connection(stream, info_hash).await?;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Extended {
                    extension_id: 0, ..
                })) => break,
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
        let total = if matches!(attack, MetadataAttack::ConflictingDuplicate) {
            METADATA_BLOCK_BYTES + 1
        } else {
            METADATA_BLOCK_BYTES
        };
        let advertised = match attack {
            MetadataAttack::ZeroAdvertised => 0,
            MetadataAttack::OversizedAdvertised => limit + 1,
            _ => total,
        };
        peer.send(PeerMessage::Extended {
            extension_id: 0,
            payload: encode_extension_handshake(Some(advertised)),
        })
        .await?;
        if matches!(
            attack,
            MetadataAttack::ZeroAdvertised | MetadataAttack::OversizedAdvertised
        ) {
            return Ok(());
        }

        let mut request_count = 0_usize;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Extended {
                    extension_id: LOCAL_METADATA_EXTENSION_ID,
                    payload,
                })) => {
                    let MetadataMessage::Request { piece } =
                        decode_metadata_message(&payload, limit)?
                    else {
                        continue;
                    };
                    request_count += 1;
                    let response = match attack {
                        MetadataAttack::WrongPiece => {
                            encode_metadata_data(piece + 1, total, &vec![0; METADATA_BLOCK_BYTES])
                        }
                        MetadataAttack::ChangedTotal => {
                            encode_metadata_data(piece, total + 1, &vec![0; METADATA_BLOCK_BYTES])
                        }
                        MetadataAttack::OversizedBlock => {
                            encode_metadata_data(piece, total, &vec![0; METADATA_BLOCK_BYTES + 1])
                        }
                        MetadataAttack::ConflictingDuplicate if request_count == 1 => {
                            encode_metadata_data(piece, total, &vec![0; METADATA_BLOCK_BYTES])
                        }
                        MetadataAttack::ConflictingDuplicate => {
                            encode_metadata_data(0, total, &[0xff])
                        }
                        MetadataAttack::ZeroAdvertised | MetadataAttack::OversizedAdvertised => {
                            return Err("unexpected request after invalid handshake".into());
                        }
                    };
                    peer.send(PeerMessage::Extended {
                        extension_id: LOCAL_METADATA_EXTENSION_ID,
                        payload: response,
                    })
                    .await?;
                    if !matches!(attack, MetadataAttack::ConflictingDuplicate) || request_count == 2
                    {
                        return Ok(());
                    }
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
    }

    async fn fake_metadata_then_payload_peer(
        listener: TcpListener,
        info_hash: Sha1Hash,
        info: Bytes,
        payload: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let mut peer = test_peer_connection(stream, info_hash).await?;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Extended {
                    extension_id: 0, ..
                })) => break,
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
        peer.send(PeerMessage::Extended {
            extension_id: 0,
            payload: encode_extension_handshake(Some(info.len())),
        })
        .await?;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Extended {
                    extension_id: LOCAL_METADATA_EXTENSION_ID,
                    payload: request,
                })) => {
                    let MetadataMessage::Request { piece: 0 } =
                        decode_metadata_message(&request, info.len())?
                    else {
                        continue;
                    };
                    peer.send(PeerMessage::Extended {
                        extension_id: LOCAL_METADATA_EXTENSION_ID,
                        payload: encode_metadata_data(0, info.len(), &info),
                    })
                    .await?;
                    break;
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
        drop(peer);
        let (stream, _) = listener.accept().await?;
        serve_payload_peer(stream, info_hash, payload).await
    }

    async fn test_peer_connection(
        stream: tokio::net::TcpStream,
        info_hash: Sha1Hash,
    ) -> Result<PeerConnection, Box<dyn std::error::Error + Send + Sync>> {
        Ok(PeerConnection::accept(
            stream,
            Handshake {
                reserved: [0; 8],
                info_hash,
                peer_id: PeerId::from_bytes(*b"-FAKE00-012345678901"),
            },
            PeerCodecLimits::default(),
        )
        .await?)
    }

    async fn fake_peer(
        listener: TcpListener,
        info_hash: Sha1Hash,
        payload: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        serve_payload_peer(stream, info_hash, payload).await
    }

    async fn fake_superseed_peer(
        listener: TcpListener,
        info_hash: Sha1Hash,
        payload: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let mut peer = test_peer_connection(stream, info_hash).await?;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Interested)) => break,
                Some(PeerEvent::Message(PeerMessage::Extended { .. })) => {
                    return Err("client sent an extension message without negotiation".into());
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
        peer.send(PeerMessage::Unchoke).await?;
        tokio::task::yield_now().await;
        peer.send(PeerMessage::Have(0)).await?;
        let mut sent_piece = false;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Request(request))) => {
                    let start = usize::try_from(request.begin)?;
                    let end = start
                        .checked_add(usize::try_from(request.length)?)
                        .ok_or("block range overflow")?;
                    let block = payload.get(start..end).ok_or("invalid block request")?;
                    peer.send(PeerMessage::Piece {
                        piece: request.piece,
                        begin: request.begin,
                        block: Bytes::copy_from_slice(block),
                    })
                    .await?;
                    sent_piece = end == payload.len();
                }
                Some(PeerEvent::Message(PeerMessage::Have(0))) if sent_piece => return Ok(()),
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
    }

    async fn fake_delayed_have_superseed(
        listener: TcpListener,
        info_hash: Sha1Hash,
        remaining_pieces: [Bytes; 2],
        delay: Duration,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let mut peer = test_peer_connection(stream, info_hash).await?;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Interested)) => break,
                Some(PeerEvent::Message(PeerMessage::Extended { .. })) => {
                    return Err("client sent an extension message without negotiation".into());
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
        peer.send(PeerMessage::Unchoke).await?;
        expect_no_superseed_request(&mut peer, delay).await?;
        peer.send(PeerMessage::Have(0)).await?;
        wait_for_superseed_have(&mut peer, 0).await?;
        expect_no_superseed_request(&mut peer, delay).await?;
        peer.send(PeerMessage::Have(1)).await?;
        serve_superseed_piece(&mut peer, 1, &remaining_pieces[0], true).await?;
        expect_no_superseed_request(&mut peer, delay).await?;
        peer.send(PeerMessage::Have(2)).await?;
        serve_superseed_piece(&mut peer, 2, &remaining_pieces[1], false).await
    }

    async fn serve_superseed_piece(
        peer: &mut PeerConnection,
        piece: u32,
        payload: &[u8],
        require_have: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut served = 0_usize;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Request(request))) => {
                    if request.piece != piece {
                        return Err("client requested a piece before it was advertised".into());
                    }
                    let start = usize::try_from(request.begin)?;
                    let end = start
                        .checked_add(usize::try_from(request.length)?)
                        .ok_or("block range overflow")?;
                    let block = payload.get(start..end).ok_or("invalid block request")?;
                    peer.send(PeerMessage::Piece {
                        piece: request.piece,
                        begin: request.begin,
                        block: Bytes::copy_from_slice(block),
                    })
                    .await?;
                    served = served.saturating_add(block.len());
                }
                Some(PeerEvent::Message(PeerMessage::Have(received)))
                    if received == piece && served == payload.len() =>
                {
                    return Ok(());
                }
                Some(PeerEvent::Message(PeerMessage::Extended { .. })) => {
                    return Err("client sent an extension message without negotiation".into());
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None
                    if !require_have && served == payload.len() =>
                {
                    return Ok(());
                }
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
    }

    async fn expect_no_superseed_request(
        peer: &mut PeerConnection,
        duration: Duration,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match tokio::time::timeout(duration, peer.next_event()).await {
            Err(_) => Ok(()),
            Ok(Some(PeerEvent::Message(PeerMessage::Extended { .. }))) => {
                Err("client sent an extension message without negotiation".into())
            }
            Ok(Some(PeerEvent::Message(PeerMessage::Request(_)))) => {
                Err("client requested a piece before it was advertised".into())
            }
            Ok(Some(PeerEvent::Failed(error))) => Err(error.into()),
            Ok(Some(PeerEvent::Disconnected) | None) => Err("peer disconnected".into()),
            Ok(Some(_)) => Err("client sent an unexpected message while superseed was idle".into()),
        }
    }

    async fn wait_for_superseed_have(
        peer: &mut PeerConnection,
        piece: u32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match peer.next_event().await {
                    Some(PeerEvent::Message(PeerMessage::Have(received))) if received == piece => {
                        return Ok(());
                    }
                    Some(PeerEvent::Message(PeerMessage::Extended { .. })) => {
                        return Err("client sent an extension message without negotiation".into());
                    }
                    Some(PeerEvent::Message(PeerMessage::Request(_))) => {
                        return Err("client requested an already-completed piece".into());
                    }
                    Some(PeerEvent::Failed(error)) => return Err(error.into()),
                    Some(PeerEvent::Disconnected) | None => {
                        return Err("peer disconnected".into());
                    }
                    _ => {}
                }
            }
        })
        .await
        .map_err(|_| "client did not acknowledge the advertised completed piece")?
    }

    async fn fake_disconnect_mid_piece(
        listener: TcpListener,
        info_hash: Sha1Hash,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let mut peer = test_peer_connection(stream, info_hash).await?;
        wait_for_interest(&mut peer).await?;
        peer.send(PeerMessage::Bitfield(Bytes::from_static(&[0x80])))
            .await?;
        peer.send(PeerMessage::Unchoke).await?;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Request(_))) => return Ok(()),
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => {
                    return Err("client disconnected before requesting the piece".into());
                }
                _ => {}
            }
        }
    }

    async fn fake_stalled_peer(
        listener: TcpListener,
        info_hash: Sha1Hash,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let mut peer = test_peer_connection(stream, info_hash).await?;
        wait_for_interest(&mut peer).await?;
        peer.send(PeerMessage::Bitfield(Bytes::from_static(&[0x80])))
            .await?;
        peer.send(PeerMessage::Unchoke).await?;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Request(_))) => break,
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => {
                    return Err("client disconnected before requesting the piece".into());
                }
                _ => {}
            }
        }
        std::future::pending::<()>().await;
        Ok(())
    }

    async fn fake_malicious_block_peer(
        listener: TcpListener,
        info_hash: Sha1Hash,
        payload: Bytes,
        behavior: MaliciousBlock,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let mut peer = test_peer_connection(stream, info_hash).await?;
        wait_for_interest(&mut peer).await?;
        peer.send(PeerMessage::Bitfield(Bytes::from_static(&[0x80])))
            .await?;
        peer.send(PeerMessage::Unchoke).await?;
        let request = loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Request(request))) => break request,
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => {
                    return Err("client disconnected before requesting the piece".into());
                }
                _ => {}
            }
        };
        let start = usize::try_from(request.begin)?;
        let requested = usize::try_from(request.length)?;
        let block = payload
            .get(start..start + requested)
            .ok_or("invalid block request")?;
        match behavior {
            MaliciousBlock::WrongPiece => {
                peer.send(PeerMessage::Piece {
                    piece: request.piece.saturating_add(1),
                    begin: request.begin,
                    block: Bytes::copy_from_slice(block),
                })
                .await?;
            }
            MaliciousBlock::WrongLength => {
                peer.send(PeerMessage::Piece {
                    piece: request.piece,
                    begin: request.begin,
                    block: Bytes::copy_from_slice(&block[..block.len() - 1]),
                })
                .await?;
            }
            MaliciousBlock::UnsolicitedOffset => {
                peer.send(PeerMessage::Piece {
                    piece: request.piece,
                    begin: request.begin.saturating_add(1),
                    block: Bytes::copy_from_slice(block),
                })
                .await?;
            }
            MaliciousBlock::Duplicate => {
                peer.send(PeerMessage::Piece {
                    piece: request.piece,
                    begin: request.begin,
                    block: Bytes::copy_from_slice(block),
                })
                .await?;
                peer.send(PeerMessage::Piece {
                    piece: request.piece,
                    begin: request.begin,
                    block: Bytes::from(vec![0xff; block.len()]),
                })
                .await?;
            }
            MaliciousBlock::Oversized => {
                peer.send(PeerMessage::Piece {
                    piece: request.piece,
                    begin: request.begin,
                    block: Bytes::from(vec![0; BLOCK_BYTES + 1]),
                })
                .await?;
            }
        }
        Ok(())
    }

    async fn fake_signaled_delayed_peer(
        listener: TcpListener,
        info_hash: Sha1Hash,
        payload: Bytes,
        request_seen: oneshot::Sender<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let mut peer = test_peer_connection(stream, info_hash).await?;
        wait_for_interest(&mut peer).await?;
        peer.send(PeerMessage::Bitfield(Bytes::from_static(&[0x80])))
            .await?;
        peer.send(PeerMessage::Unchoke).await?;
        let request = loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Request(request))) => break request,
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => {
                    return Err("client disconnected before requesting the piece".into());
                }
                _ => {}
            }
        };
        let _result_ignored = request_seen.send(());
        tokio::time::sleep(Duration::from_millis(150)).await;
        let start = usize::try_from(request.begin)?;
        let end = start
            .checked_add(usize::try_from(request.length)?)
            .ok_or("block range overflow")?;
        let block = payload.get(start..end).ok_or("invalid block request")?;
        let _result_ignored = peer
            .send(PeerMessage::Piece {
                piece: request.piece,
                begin: request.begin,
                block: Bytes::copy_from_slice(block),
            })
            .await;
        Ok(())
    }

    async fn fake_encrypted_peer(
        listener: TcpListener,
        info_hash: Sha1Hash,
        payload: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let (encrypted, selected) = dendrite_net::mse::respond(stream, &[info_hash]).await?;
        if selected != info_hash {
            return Err("MSE selected the wrong torrent".into());
        }
        let mut peer = PeerConnection::from_stream(
            encrypted,
            Handshake {
                reserved: [0; 8],
                info_hash,
                peer_id: PeerId::from_bytes([0x62; 20]),
            },
            PeerCodecLimits::default(),
        )
        .await?;
        wait_for_interest(&mut peer).await?;
        peer.send(PeerMessage::Bitfield(Bytes::from_static(&[0x80])))
            .await?;
        peer.send(PeerMessage::Unchoke).await?;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Request(request))) => {
                    let begin = usize::try_from(request.begin)?;
                    let end = begin
                        .checked_add(usize::try_from(request.length)?)
                        .ok_or("block range overflow")?;
                    let block = payload.get(begin..end).ok_or("invalid block request")?;
                    peer.send(PeerMessage::Piece {
                        piece: request.piece,
                        begin: request.begin,
                        block: Bytes::copy_from_slice(block),
                    })
                    .await?;
                    if end == payload.len() {
                        return Ok(());
                    }
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => {
                    return Err("encrypted client disconnected".into());
                }
                _ => {}
            }
        }
    }

    async fn fake_pex_bootstrap(
        listener: TcpListener,
        info_hash: Sha1Hash,
        discovered: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let mut reserved = [0_u8; 8];
        reserved[5] |= 0x10;
        let mut peer = PeerConnection::accept(
            stream,
            Handshake {
                reserved,
                info_hash,
                peer_id: PeerId::from_bytes(*b"-FAKE00-012345678901"),
            },
            PeerCodecLimits::default(),
        )
        .await?;
        let mut interested = false;
        let mut extension = false;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Interested)) => interested = true,
                Some(PeerEvent::Message(PeerMessage::Extended {
                    extension_id: 0, ..
                })) => extension = true,
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => {
                    return Err("PEX client disconnected".into());
                }
                _ => {}
            }
            if interested && extension {
                break;
            }
        }
        peer.send(PeerMessage::Bitfield(Bytes::from_static(&[0])))
            .await?;
        peer.send(PeerMessage::Extended {
            extension_id: LOCAL_PEX_EXTENSION_ID,
            payload: dendrite_net::extension::encode_pex_message(
                &dendrite_net::extension::PexMessage {
                    added: vec![dendrite_net::extension::PexPeer {
                        address: discovered,
                        flags: 0x14,
                    }],
                    dropped: Vec::new(),
                },
            ),
        })
        .await?;
        peer.send(PeerMessage::Unchoke).await?;
        Ok(())
    }

    async fn fake_piece_then_disconnect(
        listener: TcpListener,
        info_hash: Sha1Hash,
        piece: u32,
        payload: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let mut peer = test_peer_connection(stream, info_hash).await?;
        wait_for_interest(&mut peer).await?;
        let bitfield = if piece == 0 { 0x80 } else { 0x40 };
        peer.send(PeerMessage::Bitfield(Bytes::from(vec![bitfield])))
            .await?;
        peer.send(PeerMessage::Unchoke).await?;
        serve_superseed_piece(&mut peer, piece, &payload, true).await
    }

    async fn fake_single_piece_peer(
        listener: TcpListener,
        info_hash: Sha1Hash,
        piece: u32,
        payload: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let mut peer = test_peer_connection(stream, info_hash).await?;
        wait_for_interest(&mut peer).await?;
        let bitfield = if piece == 0 { 0x80 } else { 0x40 };
        peer.send(PeerMessage::Bitfield(Bytes::from(vec![bitfield])))
            .await?;
        peer.send(PeerMessage::Unchoke).await?;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Request(request))) => {
                    if request.piece != piece {
                        return Err("peer received a request for an unavailable piece".into());
                    }
                    let start = usize::try_from(request.begin)?;
                    let end = start
                        .checked_add(usize::try_from(request.length)?)
                        .ok_or("block range overflow")?;
                    let block = payload.get(start..end).ok_or("invalid block request")?;
                    peer.send(PeerMessage::Piece {
                        piece,
                        begin: request.begin,
                        block: Bytes::copy_from_slice(block),
                    })
                    .await?;
                    if end == payload.len() {
                        return Ok(());
                    }
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
    }

    async fn fake_pipelined_peer(
        listener: TcpListener,
        info_hash: Sha1Hash,
        piece: u32,
        payload: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let mut peer = test_peer_connection(stream, info_hash).await?;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Interested)) => break,
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
        let bitfield = if piece == 0 { 0b1000_0000 } else { 0b0100_0000 };
        peer.send(PeerMessage::Bitfield(Bytes::from(vec![bitfield])))
            .await?;
        peer.send(PeerMessage::Unchoke).await?;
        let mut requests = Vec::with_capacity(BLOCK_PIPELINE);
        while requests.len() < BLOCK_PIPELINE {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Request(request)))
                    if request.piece == piece =>
                {
                    requests.push(request);
                }
                Some(PeerEvent::Message(PeerMessage::Request(_))) => {
                    return Err("scheduler requested an unavailable piece".into());
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
        for request in requests.into_iter().rev() {
            let start = usize::try_from(request.begin)?;
            let end = start
                .checked_add(usize::try_from(request.length)?)
                .ok_or("block range overflow")?;
            let block = payload.get(start..end).ok_or("invalid block request")?;
            peer.send(PeerMessage::Piece {
                piece,
                begin: request.begin,
                block: Bytes::copy_from_slice(block),
            })
            .await?;
        }
        Ok(())
    }

    async fn fake_delayed_peer(
        listener: TcpListener,
        info_hash: Sha1Hash,
        payload: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let mut peer = test_peer_connection(stream, info_hash).await?;
        wait_for_interest(&mut peer).await?;
        peer.send(PeerMessage::Bitfield(Bytes::from_static(&[0x80])))
            .await?;
        peer.send(PeerMessage::Unchoke).await?;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Request(request))) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    peer.send(PeerMessage::Piece {
                        piece: request.piece,
                        begin: request.begin,
                        block: payload.clone(),
                    })
                    .await?;
                    return Ok(());
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
    }

    async fn fake_endgame_loser(
        listener: TcpListener,
        info_hash: Sha1Hash,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let mut peer = test_peer_connection(stream, info_hash).await?;
        wait_for_interest(&mut peer).await?;
        peer.send(PeerMessage::Bitfield(Bytes::from_static(&[0x80])))
            .await?;
        peer.send(PeerMessage::Unchoke).await?;
        let request = loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Request(request))) => break request,
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        };
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Cancel(cancel))) if cancel == request => {
                    return Ok(());
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => {
                    return Err("peer disconnected before cancel".into());
                }
                _ => {}
            }
        }
    }

    async fn wait_for_seed_bitfield(
        peer: &mut PeerConnection,
        expected: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Bitfield(bitfield))) => {
                    if bitfield.as_ref() != expected {
                        return Err("seed advertised an unexpected bitfield".into());
                    }
                    return Ok(());
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("seed disconnected".into()),
                _ => {}
            }
        }
    }

    async fn wait_for_metadata_handshake(
        peer: &mut PeerConnection,
        expected_size: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Extended {
                    extension_id: 0,
                    payload,
                })) => {
                    let handshake = decode_extension_handshake(&payload, expected_size)?;
                    if handshake.metadata_extension_id != Some(LOCAL_METADATA_EXTENSION_ID)
                        || handshake.metadata_size != Some(expected_size)
                    {
                        return Err("seed advertised unexpected metadata capabilities".into());
                    }
                    return Ok(());
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("seed disconnected".into()),
                _ => {}
            }
        }
    }

    async fn wait_for_metadata_data(
        peer: &mut PeerConnection,
    ) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Extended {
                    extension_id: LOCAL_METADATA_EXTENSION_ID,
                    payload,
                })) => match decode_metadata_message(&payload, 64 * 1024)? {
                    MetadataMessage::Data {
                        piece: 0, block, ..
                    } => return Ok(block),
                    _ => return Err("seed returned unexpected metadata".into()),
                },
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("seed disconnected".into()),
                _ => {}
            }
        }
    }

    async fn wait_for_holepunch(
        peer: &mut PeerConnection,
    ) -> Result<HolePunchMessage, Box<dyn std::error::Error + Send + Sync>> {
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Extended {
                    extension_id: LOCAL_HOLEPUNCH_EXTENSION_ID,
                    payload,
                })) => return Ok(decode_holepunch_message(&payload)?),
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
    }

    async fn wait_for_unchoke(
        peer: &mut PeerConnection,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Unchoke)) => return Ok(()),
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("seed disconnected".into()),
                _ => {}
            }
        }
    }

    async fn wait_for_piece(
        peer: &mut PeerConnection,
    ) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Piece {
                    piece: 0,
                    begin: 0,
                    block,
                })) => return Ok(block),
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("seed disconnected".into()),
                _ => {}
            }
        }
    }

    async fn wait_for_interest(
        peer: &mut PeerConnection,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Interested)) => return Ok(()),
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
    }

    async fn serve_payload_peer(
        stream: tokio::net::TcpStream,
        info_hash: Sha1Hash,
        payload: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut peer = test_peer_connection(stream, info_hash).await?;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Interested)) => break,
                Some(PeerEvent::Message(PeerMessage::Extended { .. })) => {
                    return Err("client sent an extension message without negotiation".into());
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
        peer.send(PeerMessage::Bitfield(Bytes::from_static(&[0x80])))
            .await?;
        peer.send(PeerMessage::Unchoke).await?;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Request(request))) => {
                    let start = usize::try_from(request.begin)?;
                    let end = start
                        .checked_add(usize::try_from(request.length)?)
                        .ok_or("block range overflow")?;
                    let block = payload.get(start..end).ok_or("invalid block request")?;
                    peer.send(PeerMessage::Piece {
                        piece: request.piece,
                        begin: request.begin,
                        block: Bytes::copy_from_slice(block),
                    })
                    .await?;
                    if end == payload.len() {
                        return Ok(());
                    }
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
    }
}
