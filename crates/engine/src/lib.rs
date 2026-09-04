//! Supervised torrent actors and their bounded command interface.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    future::Future,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU16, AtomicU64, AtomicUsize, Ordering},
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
        LOCAL_PEX_EXTENSION_ID, METADATA_BLOCK_BYTES, MetadataMessage, REQUEST_QUEUE_LIMIT,
        decode_extension_handshake, decode_holepunch_message, decode_metadata_message,
        decode_pex_message, encode_extension_handshake, encode_holepunch_message,
        encode_metadata_data, encode_metadata_reject, encode_metadata_request,
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
use futures_util::{StreamExt as _, TryStreamExt as _, stream, stream::FuturesUnordered};
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
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, warn};
use url::Url;

const COMMAND_CAPACITY: usize = 256;
const EVENT_CAPACITY: usize = 4096;
const PEER_LIMIT_PER_ANNOUNCE: u16 = 200;
const BLOCK_BYTES: usize = 16 * 1024;
const BLOCK_PIPELINE: usize = 128;
const BLOCK_PIPELINE_MAX: usize = REQUEST_QUEUE_LIMIT;
const PIPELINE_TARGET_SECONDS: u64 = 3;
const ASSIGNMENT_TARGET_MIN: usize = 2 * 1024 * 1024;
const ASSIGNMENT_TARGET_MAX: usize = 16 * 1024 * 1024;
const ASSIGNMENT_TARGET_SECONDS: u64 = 1;
const DEFAULT_DOWNLOAD_BUFFER_BYTES: usize = 2 * 1024 * 1024 * 1024;
const DEFAULT_PIECE_CACHE_BYTES: usize = 512 * 1024 * 1024;
const RATE_SAMPLE_INTERVAL: Duration = Duration::from_millis(250);
const PEER_COMMAND_CAPACITY: usize = 64;
const ACTIVE_PEER_LIMIT: usize = 512;
const PEER_CONNECT_CONCURRENCY: usize = 128;
const INCOMING_PEER_LIMIT: usize = 256;
const INCOMING_HANDSHAKE_LIMIT: usize = 256;
const TRACKER_ANNOUNCE_CONCURRENCY: usize = 128;
const DISCOVERY_EVENT_CAPACITY: usize = 256;
const METADATA_PEER_CONCURRENCY: usize = 32;
const METADATA_GLOBAL_CONCURRENCY: usize = 4;
const METADATA_REQUEST_PIPELINE: usize = 16;
const STORAGE_IO_CONCURRENCY: usize = 256;
const PIECE_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const PEER_RETENTION_INTERVAL: Duration = Duration::from_secs(10);
const PEER_REANNOUNCE_INTERVAL: Duration = Duration::from_secs(60);
const PREFERRED_SEED_PEERS: usize = 8;
const UPLOAD_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const UPLOAD_CHOKE_INTERVAL: Duration = Duration::from_secs(10);
const REGULAR_UPLOAD_SLOTS: usize = 16;
const OPTIMISTIC_UPLOAD_SLOTS: usize = 4;
const RECIPROCAL_BOOTSTRAP_BYTES: u64 = 8 * 1024 * 1024;
const REPUTATION_RETENTION: Duration = Duration::from_secs(60 * 60);
const KNOWN_CANDIDATE_LIMIT: usize = 8192;
const QUEUED_CANDIDATE_LIMIT: usize = 4096;
/// A candidate address may be dialled again this long after its last attempt,
/// so peers that dropped are recovered when discovery reports them again.
const CANDIDATE_RETRY_INTERVAL: Duration = Duration::from_secs(10 * 60);
const PEER_MESSAGE_TIMEOUT: Duration = Duration::from_secs(30);
const DHT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(25);
const DHT_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(15 * 60);
const TRACKER_INTERVAL_MIN: Duration = Duration::from_secs(60);
const TRACKER_INTERVAL_MAX: Duration = Duration::from_secs(4 * 60 * 60);
const SWARM_RETRY_MIN: Duration = Duration::from_secs(1);
const SWARM_RETRY_MAX: Duration = Duration::from_secs(30);
const ACCEPT_ERROR_BACKOFF_MIN: Duration = Duration::from_millis(10);
const ACCEPT_ERROR_BACKOFF_MAX: Duration = Duration::from_secs(1);

#[allow(clippy::struct_excessive_bools)] // Independent flags mirrored from the wire and scheduler.
struct PeerWorkerHandle {
    commands: mpsc::Sender<PeerWorkerCommand>,
    bitfield: Option<Vec<u8>>,
    idle: bool,
    choked: bool,
    address: SocketAddr,
    peer_key: Option<PeerKey>,
    seed: bool,
    useful_pieces: usize,
    verified_bytes: u64,
    connected_at: Instant,
    last_verified: Option<Instant>,
    cancellation: CancellationToken,
    /// Bytes of pieces currently assigned to this worker.
    assigned_bytes: usize,
    /// Assignment target derived from the worker's verified rate.
    target_bytes: usize,
    /// The worker asked for more pieces and has not been granted any since.
    wants_more: bool,
    /// Picker generation at which `select` last returned `None` for this
    /// worker; scheduling is skipped until the generation changes.
    skip_generation: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
struct PieceAssignment {
    piece: usize,
    length: usize,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct PeerKey {
    ip: IpAddr,
    peer_id: PeerId,
}

struct PeerWorkerContext {
    worker: usize,
    address: SocketAddr,
    info_hash: Sha1Hash,
    piece_count: usize,
    completed_pieces: Vec<u8>,
    services: Services,
    events: mpsc::Sender<PeerWorkerEvent>,
    cancellation: CancellationToken,
    allow_pex: bool,
    force_utp: bool,
    torrent_id: TorrentId,
}

struct SwarmState {
    workers: HashMap<usize, PeerWorkerHandle>,
    assignments: HashMap<usize, Vec<PieceAssignment>>,
    picker: PiecePicker,
    budget: BudgetAccount,
    schedule_dirty: bool,
    connecting: usize,
    last_error: Option<String>,
    writing: HashSet<usize>,
    candidates: VecDeque<PeerCandidate>,
    known_candidates: HashMap<PeerCandidate, Instant>,
    next_worker: usize,
    event_sender: mpsc::Sender<PeerWorkerEvent>,
    info_hash: Sha1Hash,
    piece_count: usize,
    completed_pieces: Vec<u8>,
    services: Services,
    cancellation: CancellationToken,
    allow_pex: bool,
    torrent_id: TorrentId,
    peer_limit: usize,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct PeerCandidate {
    address: SocketAddr,
    force_utp: bool,
}

enum DiscoveryEvent {
    Peers(Vec<SocketAddr>),
    /// A tracker asked to be re-announced no sooner than `interval` from now.
    TrackerInterval {
        url: String,
        interval: Duration,
    },
    Finished(Result<(), ActorError>),
}

enum DiscoverySourceResult {
    Tracker {
        url: Url,
        result: Result<(Vec<SocketAddr>, Duration), String>,
    },
    Dht(Result<Vec<SocketAddr>, String>),
    Lsd(Vec<SocketAddr>),
}

struct DiscoveryQuery<'a> {
    trackers: &'a [Vec<String>],
    record: &'a TorrentRecord,
    info_hash: Sha1Hash,
    left: u64,
    allow_dht: bool,
    /// Announce our listening port to the DHT after the lookup.
    dht_announce: bool,
    announce_event: AnnounceEvent,
    cancellation: CancellationToken,
}

enum SwarmLoopEvent {
    Worker(PeerWorkerEvent),
    Incoming(IncomingDownloadPeer),
    Discovery(Option<DiscoveryEvent>),
    Verified(PieceVerifyResult),
    Stored(PieceWriteResult),
    Flushed(PieceFlushResult),
    FlushTick,
    RetentionTick,
}

struct PieceWriteResult {
    worker: usize,
    piece: usize,
    bytes: u64,
    result: Result<HashSet<TorrentPath>, ActorError>,
}

struct PieceVerifyResult {
    worker: usize,
    piece: usize,
    data: Bytes,
    result: Result<bool, ActorError>,
}

type PieceVerifyFuture<'a> = Pin<Box<dyn Future<Output = PieceVerifyResult> + Send + 'a>>;

struct PieceFlushResult {
    pieces: Vec<usize>,
    result: Result<(), ActorError>,
}

type PieceWriteFuture<'a> = Pin<Box<dyn Future<Output = PieceWriteResult> + Send + 'a>>;
type PieceFlushFuture<'a> = Pin<Box<dyn Future<Output = PieceFlushResult> + Send + 'a>>;

enum PeerWorkerCommand {
    Download { piece: usize, length: usize },
    Cancel { piece: usize },
    Have { piece: u32 },
    Shutdown,
}

enum PeerWorkerEvent {
    Ready {
        worker: usize,
        peer_id: PeerId,
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
    ChokeState {
        worker: usize,
        choked: bool,
    },
    HolePunch {
        worker: usize,
        address: SocketAddr,
    },
    /// The worker's request pipeline is about to run dry.
    NeedPieces {
        worker: usize,
        rate_bytes_per_second: u64,
    },
}

enum PeerWorkerInput {
    Command(PeerWorkerCommand),
    Event(PeerEvent),
}

struct PeerEventForwarder {
    downloaded_bytes: Arc<AtomicU64>,
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

/// Upload economics; see the `[transfer]` configuration section.
#[derive(Clone, Copy, Debug)]
pub struct TransferPolicy {
    pub upload_slots: usize,
    pub optimistic_upload_slots: usize,
    /// Upload allowed per verified byte received while downloading; `0.0`
    /// disables the reciprocal cap.
    pub reciprocal_ratio: f64,
    /// Allowance every peer earns per hour of connection.
    pub reciprocal_bootstrap_bytes: u64,
    /// Global upload ceiling in bytes per second; `0` is unlimited.
    pub upload_rate_limit_bytes: u64,
    /// Uploaded/downloaded ratio at which a torrent chokes everyone; `0.0`
    /// is unlimited.
    pub torrent_max_upload_ratio: f64,
}

impl Default for TransferPolicy {
    fn default() -> Self {
        Self {
            upload_slots: REGULAR_UPLOAD_SLOTS,
            optimistic_upload_slots: OPTIMISTIC_UPLOAD_SLOTS,
            reciprocal_ratio: 1.0,
            reciprocal_bootstrap_bytes: RECIPROCAL_BOOTSTRAP_BYTES,
            upload_rate_limit_bytes: 0,
            torrent_max_upload_ratio: 0.0,
        }
    }
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
    /// Global bound on bytes of piece buffers assigned to downloading peers.
    pub download_buffer_bytes: usize,
    /// Global bound on verified pieces cached for uploads.
    pub piece_cache_bytes: usize,
    pub transfer: TransferPolicy,
    /// Cadence of the group fsync barrier that commits verified pieces.
    pub piece_flush_interval: Duration,
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
    outbound_slots: Arc<Semaphore>,
    incoming_handshake_slots: Arc<Semaphore>,
    metadata_slots: Arc<Semaphore>,
    per_torrent_peer_limit: usize,
    lsd_cookie: String,
    encryption: EncryptionPolicy,
    rendezvous: Arc<Mutex<HashMap<(Sha1Hash, SocketAddr), RendezvousPeer>>>,
    incoming_content: Arc<Mutex<HashMap<TorrentId, Arc<IncomingContent>>>>,
    incoming_swarms: Arc<std::sync::Mutex<HashMap<TorrentId, mpsc::Sender<IncomingDownloadPeer>>>>,
    upload_policy: Arc<std::sync::Mutex<UploadPolicy>>,
    connected_peers: Arc<AtomicUsize>,
    torrent_activity: Arc<std::sync::Mutex<HashMap<TorrentId, TorrentActivity>>>,
    payload_claims: Arc<std::sync::Mutex<HashMap<TorrentId, Vec<TorrentPath>>>>,
    download_budget: Arc<DownloadBudget>,
    piece_cache_budget: Arc<CacheBudget>,
    hash_slots: Arc<Semaphore>,
    transfer: TransferPolicy,
    piece_flush_interval: Duration,
    /// Addresses this daemon is reachable at, learned from local interfaces
    /// and from peers' `yourip`; candidates on our own peer port at these
    /// addresses are never dialled.
    self_addresses: Arc<std::sync::RwLock<HashSet<IpAddr>>>,
    shutdown: CancellationToken,
    tasks: TaskTracker,
}

/// Process-wide bound on bytes held by upload piece caches.
struct CacheBudget {
    used: AtomicU64,
    limit: u64,
}

impl CacheBudget {
    fn new(limit: usize) -> Self {
        Self {
            used: AtomicU64::new(0),
            limit: u64::try_from(limit).unwrap_or(u64::MAX),
        }
    }

    fn over_limit(&self) -> bool {
        self.used.load(Ordering::Acquire) > self.limit
    }

    fn add(&self, bytes: usize) {
        self.used
            .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::AcqRel);
    }

    fn release(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        let _result_ignored = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                Some(used.saturating_sub(bytes))
            });
    }
}

/// Verified pieces shared by every upload session of one torrent, evicted
/// oldest-first whenever the global cache budget is exceeded.
struct PieceCache {
    entries: HashMap<usize, Bytes>,
    order: VecDeque<usize>,
    bytes: usize,
    budget: Arc<CacheBudget>,
}

impl PieceCache {
    fn new(budget: Arc<CacheBudget>) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            budget,
        }
    }

    fn get(&mut self, piece: usize) -> Option<Bytes> {
        let data = self.entries.get(&piece)?.clone();
        if let Some(position) = self.order.iter().position(|entry| *entry == piece) {
            self.order.remove(position);
            self.order.push_back(piece);
        }
        Some(data)
    }

    fn insert(&mut self, piece: usize, data: Bytes) {
        if self.entries.contains_key(&piece) {
            return;
        }
        self.bytes = self.bytes.saturating_add(data.len());
        self.budget.add(data.len());
        self.entries.insert(piece, data);
        self.order.push_back(piece);
        while self.budget.over_limit() && self.order.len() > 1 {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(evicted.len());
                self.budget.release(evicted.len());
            }
        }
    }
}

impl Drop for PieceCache {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

/// Process-wide bound on piece buffers reserved for downloads.
struct DownloadBudget {
    used: AtomicU64,
    limit: u64,
}

impl DownloadBudget {
    fn new(limit: usize) -> Self {
        Self {
            used: AtomicU64::new(0),
            limit: u64::try_from(limit).unwrap_or(u64::MAX),
        }
    }

    fn try_reserve(&self, bytes: u64) -> bool {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|total| *total <= self.limit)
            })
            .is_ok()
    }

    fn force_reserve(&self, bytes: u64) {
        self.used.fetch_add(bytes, Ordering::AcqRel);
    }

    fn release(&self, bytes: u64) {
        let _result_ignored = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                Some(used.saturating_sub(bytes))
            });
    }

    #[cfg(test)]
    fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }
}

/// One swarm's view of the shared download budget. A swarm holding nothing may
/// always reserve one piece so a busy daemon cannot starve a torrent forever;
/// dropping the account returns everything it still holds.
struct BudgetAccount {
    shared: Arc<DownloadBudget>,
    held: u64,
}

impl BudgetAccount {
    fn new(shared: Arc<DownloadBudget>) -> Self {
        Self { shared, held: 0 }
    }

    fn reserve(&mut self, bytes: usize) -> bool {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        if self.held == 0 {
            self.shared.force_reserve(bytes);
        } else if !self.shared.try_reserve(bytes) {
            return false;
        }
        self.held = self.held.saturating_add(bytes);
        true
    }

    fn release(&mut self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX).min(self.held);
        self.held -= bytes;
        self.shared.release(bytes);
    }
}

impl Drop for BudgetAccount {
    fn drop(&mut self) {
        self.shared.release(self.held);
    }
}

#[derive(Clone)]
struct RendezvousPeer {
    session: u64,
    extension_id: u8,
    sender: PeerSender,
}

struct IncomingContent {
    metainfo: Metainfo,
    pieces: usize,
    info: Bytes,
    cache: std::sync::Mutex<PieceCache>,
}

struct IncomingDownloadPeer {
    peer: PeerConnection,
    address: SocketAddr,
    remote: Handshake,
    seed: IncomingSeed,
    completion: oneshot::Sender<()>,
}

struct IncomingSwarmGuard {
    routes: Arc<std::sync::Mutex<HashMap<TorrentId, mpsc::Sender<IncomingDownloadPeer>>>>,
    torrent_id: TorrentId,
    sender: mpsc::Sender<IncomingDownloadPeer>,
}

struct IncomingCompletion(Option<oneshot::Sender<()>>);

#[derive(Default)]
struct UploadPolicy {
    sessions: HashMap<u64, UploadSession>,
    reputation: HashMap<(TorrentId, PeerKey), PeerReputation>,
    round: u64,
    limiter: UploadLimiter,
}

/// Token bucket for the global upload ceiling; negative balances translate
/// into a delay before the next block is sent.
#[derive(Default)]
struct UploadLimiter {
    last_refill: Option<Instant>,
    tokens: i128,
}

impl UploadLimiter {
    /// Charges `bytes` against the bucket and returns how long the caller
    /// must wait before sending them.
    fn charge(&mut self, rate_bytes_per_second: u64, bytes: u64) -> Duration {
        if rate_bytes_per_second == 0 {
            return Duration::ZERO;
        }
        let now = Instant::now();
        let rate = i128::from(rate_bytes_per_second);
        if let Some(last) = self.last_refill {
            let elapsed_nanos = i128::try_from(now.duration_since(last).as_nanos()).unwrap_or(0);
            let refill = rate.saturating_mul(elapsed_nanos) / 1_000_000_000;
            self.tokens = self.tokens.saturating_add(refill).min(rate);
        } else {
            self.tokens = rate;
        }
        self.last_refill = Some(now);
        self.tokens = self.tokens.saturating_sub(i128::from(bytes));
        if self.tokens >= 0 {
            return Duration::ZERO;
        }
        let deficit = self.tokens.unsigned_abs();
        let nanos = deficit
            .saturating_mul(1_000_000_000)
            .checked_div(rate.unsigned_abs())
            .unwrap_or(0);
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }
}

#[allow(clippy::struct_excessive_bools)] // Wire state (interested/unchoked) and policy inputs are independent flags.
struct UploadSession {
    torrent_id: TorrentId,
    peer: PeerKey,
    sender: PeerSender,
    reciprocal: bool,
    interested: bool,
    /// The peer holds pieces we still need, so upload to it can be repaid.
    interesting: bool,
    unchoked: bool,
    uploaded: u64,
    sampled_uploaded: u64,
    recent_upload: u64,
}

#[derive(Clone, Copy)]
struct PeerReputation {
    verified_from: u64,
    uploaded_to: u64,
    failures: u32,
    /// Verified bytes since the last choke round, folded into `verified_rate`.
    recent_verified: u64,
    /// Bytes per second delivered by this peer over the last choke round.
    verified_rate: u64,
    first_seen: Instant,
    last_seen: Instant,
}

impl Default for PeerReputation {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            verified_from: 0,
            uploaded_to: 0,
            failures: 0,
            recent_verified: 0,
            verified_rate: 0,
            first_seen: now,
            last_seen: now,
        }
    }
}

#[derive(Clone, Copy)]
struct UploadSessionContext {
    session: u64,
    peer: PeerKey,
}

struct UploadSessionGuard {
    policy: Arc<std::sync::Mutex<UploadPolicy>>,
    session: u64,
}

struct ConnectionGuard {
    connected: Arc<AtomicUsize>,
    torrents: Arc<std::sync::Mutex<HashMap<TorrentId, TorrentActivity>>>,
    torrent_id: TorrentId,
    direction: PeerDirection,
    seed: bool,
}

struct ActiveDownloadGuard {
    torrents: Arc<std::sync::Mutex<HashMap<TorrentId, TorrentActivity>>>,
    torrent_id: TorrentId,
}

struct SeedPeerGuard {
    torrents: Arc<std::sync::Mutex<HashMap<TorrentId, TorrentActivity>>>,
    torrent_id: TorrentId,
}

#[derive(Default)]
struct TorrentActivity {
    peers: usize,
    inbound_peers: usize,
    outbound_peers: usize,
    seed_peers: usize,
    active_downloaders: usize,
    /// Updated per block by every downloading worker without taking the map
    /// lock; readers sample it.
    downloaded_bytes: Arc<AtomicU64>,
    uploaded_bytes: u64,
    pending_uploaded_bytes: u64,
}

#[derive(Clone, Copy)]
enum PeerDirection {
    Inbound,
    Outbound,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TorrentPeerStats {
    pub total: usize,
    pub inbound: usize,
    pub outbound: usize,
    pub seeds: usize,
    pub active_downloaders: usize,
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
            count.peers = count.peers.saturating_sub(1);
            match self.direction {
                PeerDirection::Inbound => {
                    count.inbound_peers = count.inbound_peers.saturating_sub(1);
                }
                PeerDirection::Outbound => {
                    count.outbound_peers = count.outbound_peers.saturating_sub(1);
                }
            }
            if self.seed {
                count.seed_peers = count.seed_peers.saturating_sub(1);
            }
        }
    }
}

impl Drop for ActiveDownloadGuard {
    fn drop(&mut self) {
        if let Ok(mut peers) = self.torrents.lock()
            && let Some(count) = peers.get_mut(&self.torrent_id)
        {
            count.active_downloaders = count.active_downloaders.saturating_sub(1);
        }
    }
}

impl Drop for SeedPeerGuard {
    fn drop(&mut self) {
        if let Ok(mut peers) = self.torrents.lock()
            && let Some(count) = peers.get_mut(&self.torrent_id)
        {
            count.seed_peers = count.seed_peers.saturating_sub(1);
        }
    }
}

impl Drop for IncomingSwarmGuard {
    fn drop(&mut self) {
        if let Ok(mut routes) = self.routes.lock()
            && routes
                .get(&self.torrent_id)
                .is_some_and(|sender| sender.same_channel(&self.sender))
        {
            routes.remove(&self.torrent_id);
        }
    }
}

impl Drop for IncomingCompletion {
    fn drop(&mut self) {
        if let Some(completion) = self.0.take() {
            let _result_ignored = completion.send(());
        }
    }
}

impl Drop for UploadSessionGuard {
    fn drop(&mut self) {
        if let Ok(mut policy) = self.policy.lock() {
            policy.sessions.remove(&self.session);
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
                download_buffer_bytes: DEFAULT_DOWNLOAD_BUFFER_BYTES,
                piece_cache_bytes: DEFAULT_PIECE_CACHE_BYTES,
                transfer: TransferPolicy::default(),
                piece_flush_interval: PIECE_FLUSH_INTERVAL,
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
        let peer_connection_limit = options.peer_connection_limit.max(1);
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
            peer_slots: Arc::new(Semaphore::new(peer_connection_limit)),
            outbound_slots: Arc::new(Semaphore::new(outbound_connection_limit(
                peer_connection_limit,
            ))),
            incoming_handshake_slots: Arc::new(Semaphore::new(
                peer_connection_limit.min(INCOMING_HANDSHAKE_LIMIT),
            )),
            metadata_slots: Arc::new(Semaphore::new(METADATA_GLOBAL_CONCURRENCY)),
            per_torrent_peer_limit: per_torrent_peer_limit(options.peer_connection_limit),
            lsd_cookie: format!("dendrite-{:016x}", rand::random::<u64>()),
            encryption: options.encryption,
            rendezvous: Arc::new(Mutex::new(HashMap::new())),
            incoming_content: Arc::new(Mutex::new(HashMap::new())),
            incoming_swarms: Arc::new(std::sync::Mutex::new(HashMap::new())),
            upload_policy: Arc::new(std::sync::Mutex::new(UploadPolicy::default())),
            connected_peers: Arc::new(AtomicUsize::new(0)),
            torrent_activity: Arc::new(std::sync::Mutex::new(HashMap::new())),
            payload_claims: Arc::new(std::sync::Mutex::new(HashMap::new())),
            download_budget: Arc::new(DownloadBudget::new(options.download_buffer_bytes.max(1))),
            piece_cache_budget: Arc::new(CacheBudget::new(options.piece_cache_bytes)),
            hash_slots: Arc::new(Semaphore::new(hash_concurrency())),
            transfer: options.transfer,
            piece_flush_interval: options.piece_flush_interval.max(Duration::from_millis(100)),
            self_addresses: Arc::new(std::sync::RwLock::new(local_outbound_addresses())),
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
        services
            .tasks
            .spawn(run_upload_accounting(services.clone()));
        services.tasks.spawn(run_upload_choker(services.clone()));
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
            .torrent_activity
            .lock()
            .ok()
            .and_then(|peers| peers.get(&id).map(|activity| activity.peers))
            .unwrap_or(0)
    }

    #[must_use]
    pub fn torrent_peer_stats(&self, id: TorrentId) -> TorrentPeerStats {
        self.services
            .torrent_activity
            .lock()
            .ok()
            .and_then(|peers| {
                peers.get(&id).map(|activity| TorrentPeerStats {
                    total: activity.peers,
                    inbound: activity.inbound_peers,
                    outbound: activity.outbound_peers,
                    seeds: activity.seed_peers,
                    active_downloaders: activity.active_downloaders,
                })
            })
            .unwrap_or_default()
    }

    #[must_use]
    pub fn torrent_downloaded_bytes(&self, id: TorrentId) -> u64 {
        self.services
            .torrent_activity
            .lock()
            .ok()
            .and_then(|peers| {
                peers
                    .get(&id)
                    .map(|activity| activity.downloaded_bytes.load(Ordering::Relaxed))
            })
            .unwrap_or(0)
    }

    #[must_use]
    pub fn torrent_uploaded_bytes(&self, id: TorrentId) -> u64 {
        self.services
            .torrent_activity
            .lock()
            .ok()
            .and_then(|peers| peers.get(&id).map(|activity| activity.uploaded_bytes))
            .unwrap_or(0)
    }
}

fn track_connection(
    services: &Services,
    torrent_id: TorrentId,
    direction: PeerDirection,
    seed: bool,
) -> ConnectionGuard {
    services.connected_peers.fetch_add(1, Ordering::AcqRel);
    if let Ok(mut peers) = services.torrent_activity.lock() {
        let activity = peers.entry(torrent_id).or_default();
        activity.peers += 1;
        match direction {
            PeerDirection::Inbound => activity.inbound_peers += 1,
            PeerDirection::Outbound => activity.outbound_peers += 1,
        }
        if seed {
            activity.seed_peers += 1;
        }
    }
    ConnectionGuard {
        connected: services.connected_peers.clone(),
        torrents: services.torrent_activity.clone(),
        torrent_id,
        direction,
        seed,
    }
}

fn track_active_download(services: &Services, torrent_id: TorrentId) -> ActiveDownloadGuard {
    if let Ok(mut peers) = services.torrent_activity.lock() {
        peers.entry(torrent_id).or_default().active_downloaders += 1;
    }
    ActiveDownloadGuard {
        torrents: services.torrent_activity.clone(),
        torrent_id,
    }
}

fn track_seed_peer(services: &Services, torrent_id: TorrentId) -> SeedPeerGuard {
    if let Ok(mut peers) = services.torrent_activity.lock() {
        peers.entry(torrent_id).or_default().seed_peers += 1;
    }
    SeedPeerGuard {
        torrents: services.torrent_activity.clone(),
        torrent_id,
    }
}

fn record_uploaded_block(services: &Services, torrent_id: TorrentId, bytes: u64) {
    if let Ok(mut torrents) = services.torrent_activity.lock() {
        let activity = torrents.entry(torrent_id).or_default();
        activity.uploaded_bytes = activity.uploaded_bytes.saturating_add(bytes);
        activity.pending_uploaded_bytes = activity.pending_uploaded_bytes.saturating_add(bytes);
    }
}

async fn run_upload_accounting(services: Services) {
    let mut interval = tokio::time::interval(UPLOAD_FLUSH_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    loop {
        tokio::select! {
            () = services.shutdown.cancelled() => {
                flush_upload_accounting(&services).await;
                return;
            }
            _ = interval.tick() => flush_upload_accounting(&services).await,
        }
    }
}

async fn flush_upload_accounting(services: &Services) {
    let pending = if let Ok(mut torrents) = services.torrent_activity.lock() {
        torrents
            .iter_mut()
            .filter_map(|(id, activity)| {
                let bytes = std::mem::take(&mut activity.pending_uploaded_bytes);
                (bytes > 0).then_some((*id, bytes))
            })
            .collect::<Vec<_>>()
    } else {
        return;
    };
    for (id, bytes) in pending {
        match services.store.increment_uploaded(id, bytes).await {
            Ok(_) => {}
            Err(error) => {
                warn!(torrent_id = %id, %error, "failed to persist batched upload accounting");
                if let Ok(mut torrents) = services.torrent_activity.lock() {
                    let activity = torrents.entry(id).or_default();
                    activity.pending_uploaded_bytes =
                        activity.pending_uploaded_bytes.saturating_add(bytes);
                }
            }
        }
    }
}

fn register_upload_session(
    services: &Services,
    torrent_id: TorrentId,
    peer: PeerKey,
    sender: PeerSender,
    reciprocal: bool,
) -> (UploadSessionContext, UploadSessionGuard) {
    let session = rand::random();
    if let Ok(mut policy) = services.upload_policy.lock() {
        policy.sessions.insert(
            session,
            UploadSession {
                torrent_id,
                peer,
                sender,
                reciprocal,
                interested: false,
                interesting: false,
                unchoked: false,
                uploaded: 0,
                sampled_uploaded: 0,
                recent_upload: 0,
            },
        );
    }
    (
        UploadSessionContext { session, peer },
        UploadSessionGuard {
            policy: services.upload_policy.clone(),
            session,
        },
    )
}

fn set_upload_interest(services: &Services, session: u64, interested: bool) -> bool {
    let Ok(mut policy) = services.upload_policy.lock() else {
        return false;
    };
    let Some((torrent_id, peer, reciprocal, already_unchoked, interesting)) =
        policy.sessions.get(&session).map(|entry| {
            (
                entry.torrent_id,
                entry.peer,
                entry.reciprocal,
                entry.unchoked,
                entry.interesting,
            )
        })
    else {
        return false;
    };
    let reputation = policy
        .reputation
        .get(&(torrent_id, peer))
        .copied()
        .unwrap_or_default();
    let transfer = services.transfer;
    let has_credit = !reciprocal || has_reciprocal_upload_credit(&transfer, reputation);
    let contributes = !reciprocal
        || (interesting
            && (reciprocal_contribution(reputation) > 0 || reputation.verified_rate > 0));
    let occupied = policy
        .sessions
        .values()
        .filter(|entry| entry.torrent_id == torrent_id && entry.interested && entry.unchoked)
        .count();
    let Some(entry) = policy.sessions.get_mut(&session) else {
        return false;
    };
    entry.interested = interested;
    let limit = if contributes {
        transfer
            .upload_slots
            .saturating_add(transfer.optimistic_upload_slots)
    } else {
        transfer.optimistic_upload_slots
    };
    entry.unchoked = interested && has_credit && (already_unchoked || occupied < limit);
    entry.unchoked
}

/// Records whether a peer holds pieces we still need. Regular upload slots
/// while downloading go only to such peers, because upload to anyone else
/// cannot be repaid in kind.
fn set_session_interesting(
    services: &Services,
    torrent_id: TorrentId,
    peer: PeerKey,
    interesting: bool,
) {
    if let Ok(mut policy) = services.upload_policy.lock() {
        for entry in policy.sessions.values_mut() {
            if entry.torrent_id == torrent_id && entry.peer == peer {
                entry.interesting = interesting;
            }
        }
    }
}

/// Delays the caller so global upload stays under the configured ceiling.
async fn throttle_upload(services: &Services, bytes: u64) {
    let rate = services.transfer.upload_rate_limit_bytes;
    if rate == 0 {
        return;
    }
    let wait = services
        .upload_policy
        .lock()
        .map_or(Duration::ZERO, |mut policy| {
            policy.limiter.charge(rate, bytes)
        });
    if !wait.is_zero() {
        tokio::time::sleep(wait).await;
    }
}

fn upload_allowed(services: &Services, session: u64) -> bool {
    let Ok(mut policy) = services.upload_policy.lock() else {
        return false;
    };
    let Some((torrent_id, peer, reciprocal, unchoked)) =
        policy.sessions.get(&session).map(|entry| {
            (
                entry.torrent_id,
                entry.peer,
                entry.reciprocal,
                entry.unchoked,
            )
        })
    else {
        return false;
    };
    let reputation = policy
        .reputation
        .get(&(torrent_id, peer))
        .copied()
        .unwrap_or_default();
    let allowed =
        unchoked && (!reciprocal || has_reciprocal_upload_credit(&services.transfer, reputation));
    if !allowed && let Some(entry) = policy.sessions.get_mut(&session) {
        entry.unchoked = false;
    }
    allowed
}

fn reciprocal_contribution(reputation: PeerReputation) -> u64 {
    reputation
        .verified_from
        .saturating_sub(reputation.uploaded_to)
        .saturating_sub(u64::from(reputation.failures).saturating_mul(1024 * 1024))
}

/// Upload allowance while downloading: a per-hour bootstrap so a new peer can
/// start reciprocating, plus `reciprocal_ratio` bytes per verified byte it has
/// delivered. A ratio of zero disables the cap.
fn has_reciprocal_upload_credit(transfer: &TransferPolicy, reputation: PeerReputation) -> bool {
    if transfer.reciprocal_ratio <= 0.0 {
        return true;
    }
    let hours = reputation.first_seen.elapsed().as_secs() / 3600 + 1;
    let bootstrap = transfer.reciprocal_bootstrap_bytes.saturating_mul(hours);
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let earned =
        (reputation.verified_from as f64 * transfer.reciprocal_ratio).min(u64::MAX as f64) as u64;
    reputation.uploaded_to < bootstrap.saturating_add(earned)
}

fn record_verified_download(services: &Services, torrent_id: TorrentId, peer: PeerKey, bytes: u64) {
    if let Ok(mut policy) = services.upload_policy.lock() {
        let reputation = policy.reputation.entry((torrent_id, peer)).or_default();
        reputation.verified_from = reputation.verified_from.saturating_add(bytes);
        reputation.recent_verified = reputation.recent_verified.saturating_add(bytes);
        reputation.last_seen = Instant::now();
    }
}

fn record_peer_failure(services: &Services, torrent_id: TorrentId, peer: PeerKey) {
    if let Ok(mut policy) = services.upload_policy.lock() {
        let reputation = policy.reputation.entry((torrent_id, peer)).or_default();
        reputation.failures = reputation.failures.saturating_add(1);
        reputation.last_seen = Instant::now();
    }
}

fn record_peer_upload(
    services: &Services,
    torrent_id: TorrentId,
    upload: UploadSessionContext,
    bytes: u64,
) {
    if let Ok(mut policy) = services.upload_policy.lock() {
        if let Some(session) = policy.sessions.get_mut(&upload.session) {
            session.uploaded = session.uploaded.saturating_add(bytes);
        }
        let reputation = policy
            .reputation
            .entry((torrent_id, upload.peer))
            .or_default();
        reputation.uploaded_to = reputation.uploaded_to.saturating_add(bytes);
        reputation.last_seen = Instant::now();
    }
}

struct UploadCandidate {
    session: u64,
    contribution_score: u64,
    verified_rate: u64,
    recent_upload: u64,
    eligible: bool,
    interesting: bool,
}

/// Chooses the sessions to unchoke for one torrent. While downloading, regular
/// slots go to interesting peers ordered by the rate at which they deliver
/// verified data (classic tit-for-tat), then by net contribution; optimistic
/// slots rotate through the remaining interesting peers with credit. While
/// seeding, recent upload rate orders the slots.
fn select_upload_sessions(
    state: TorrentState,
    candidates: &mut [UploadCandidate],
    round: u64,
    transfer: &TransferPolicy,
) -> HashSet<u64> {
    let downloading = match state {
        TorrentState::Downloading | TorrentState::Starting => {
            candidates.sort_by_key(|peer| {
                std::cmp::Reverse((
                    peer.verified_rate,
                    peer.contribution_score,
                    peer.recent_upload,
                ))
            });
            true
        }
        TorrentState::Seeding => {
            candidates.sort_by_key(|peer| std::cmp::Reverse(peer.recent_upload));
            false
        }
        _ => return HashSet::new(),
    };
    let mut desired = candidates
        .iter()
        .filter(|peer| {
            peer.eligible
                && (!downloading
                    || (peer.interesting
                        && (peer.contribution_score > 0 || peer.verified_rate > 0)))
        })
        .take(transfer.upload_slots)
        .map(|peer| peer.session)
        .collect::<HashSet<_>>();
    let optimistic = candidates
        .iter()
        .filter(|peer| {
            peer.eligible && (!downloading || peer.interesting) && !desired.contains(&peer.session)
        })
        .collect::<Vec<_>>();
    if !optimistic.is_empty() {
        let start = usize::try_from(round).unwrap_or(0) % optimistic.len();
        for offset in 0..transfer.optimistic_upload_slots.min(optimistic.len()) {
            desired.insert(optimistic[(start + offset) % optimistic.len()].session);
        }
    }
    desired
}

async fn run_upload_choker(services: Services) {
    let mut interval = tokio::time::interval(UPLOAD_CHOKE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    loop {
        tokio::select! {
            () = services.shutdown.cancelled() => return,
            _ = interval.tick() => rebalance_upload_slots(&services).await,
        }
    }
}

async fn rebalance_upload_slots(services: &Services) {
    let transfer = services.transfer;
    let summaries = services.store.list_summaries().await.unwrap_or_default();
    let states = summaries
        .iter()
        .map(|record| (record.id, record.state))
        .collect::<HashMap<_, _>>();
    #[allow(clippy::cast_precision_loss)]
    let capped = summaries
        .iter()
        .filter(|record| {
            transfer.torrent_max_upload_ratio > 0.0
                && record.uploaded as f64
                    >= transfer.torrent_max_upload_ratio * record.downloaded.max(1) as f64
        })
        .map(|record| record.id)
        .collect::<HashSet<_>>();
    let actions = {
        let Ok(mut policy) = services.upload_policy.lock() else {
            return;
        };
        let round_seconds = UPLOAD_CHOKE_INTERVAL.as_secs().max(1);
        for reputation in policy.reputation.values_mut() {
            reputation.verified_rate = reputation.recent_verified / round_seconds;
            reputation.recent_verified = 0;
        }
        let active: HashSet<(TorrentId, PeerKey)> = policy
            .sessions
            .values()
            .map(|entry| (entry.torrent_id, entry.peer))
            .collect();
        policy.reputation.retain(|key, reputation| {
            active.contains(key) || reputation.last_seen.elapsed() < REPUTATION_RETENTION
        });
        let mut by_torrent = HashMap::<TorrentId, Vec<UploadCandidate>>::new();
        let reputation = &policy.reputation;
        for (session, entry) in &policy.sessions {
            if !entry.interested || capped.contains(&entry.torrent_id) {
                continue;
            }
            let peer = reputation.get(&(entry.torrent_id, entry.peer));
            let reputation = peer.copied().unwrap_or_default();
            let reciprocal = matches!(
                states.get(&entry.torrent_id),
                Some(TorrentState::Downloading | TorrentState::Starting)
            );
            let contribution_score = reciprocal_contribution(reputation);
            by_torrent
                .entry(entry.torrent_id)
                .or_default()
                .push(UploadCandidate {
                    session: *session,
                    contribution_score,
                    verified_rate: reputation.verified_rate,
                    recent_upload: entry.uploaded.saturating_sub(entry.sampled_uploaded),
                    eligible: !reciprocal || has_reciprocal_upload_credit(&transfer, reputation),
                    interesting: entry.interesting,
                });
        }
        let mut desired = HashSet::new();
        for (torrent_id, candidates) in &mut by_torrent {
            let state = states
                .get(torrent_id)
                .copied()
                .unwrap_or(TorrentState::Stopped);
            desired.extend(select_upload_sessions(
                state,
                candidates,
                policy.round,
                &transfer,
            ));
        }
        policy.round = policy
            .round
            .wrapping_add(u64::try_from(transfer.optimistic_upload_slots.max(1)).unwrap_or(1));
        let mut actions = Vec::new();
        for (session, entry) in &mut policy.sessions {
            entry.reciprocal = matches!(
                states.get(&entry.torrent_id),
                Some(TorrentState::Downloading | TorrentState::Starting)
            );
            entry.recent_upload = entry.uploaded.saturating_sub(entry.sampled_uploaded);
            entry.sampled_uploaded = entry.uploaded;
            let unchoked = entry.interested && desired.contains(session);
            if entry.unchoked != unchoked {
                entry.unchoked = unchoked;
                actions.push((entry.sender.clone(), unchoked));
            }
        }
        actions
    };
    for (sender, unchoked) in actions {
        let message = if unchoked {
            PeerMessage::Unchoke
        } else {
            PeerMessage::Choke
        };
        let _result_ignored = sender.send(message).await;
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
    let Ok(records) = services.store.list_summaries().await else {
        return Vec::new();
    };
    let mut hashes = Vec::new();
    for record in records {
        if !matches!(
            record.state,
            TorrentState::Downloading | TorrentState::Seeding
        ) {
            continue;
        }
        let Some(hash) = stored_wire_info_hash(&record) else {
            continue;
        };
        // Local discovery must not announce private torrents; the content
        // cache answers that without re-parsing the metainfo.
        let private = if let Some(content) = services
            .incoming_content
            .lock()
            .await
            .get(&record.id)
            .cloned()
        {
            content.metainfo.private
        } else {
            match find_incoming_torrent(hash, services).await {
                Ok((_, content)) => content.metainfo.private,
                Err(_) => continue,
            }
        };
        if !private {
            hashes.push(hash);
        }
    }
    hashes
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
        let accepted = tokio::select! {
            () = services.shutdown.cancelled() => return,
            accepted = listener.accept() => accepted,
        };
        match accepted {
            Ok((stream, address)) => {
                error_backoff = ACCEPT_ERROR_BACKOFF_MIN;
                let Ok(handshake_permit) = services
                    .incoming_handshake_slots
                    .clone()
                    .try_acquire_owned()
                else {
                    continue;
                };
                let services = services.clone();
                let tasks = services.tasks.clone();
                tasks.spawn(async move {
                    if let Err(error) =
                        admit_incoming_stream(stream, address, &services, handshake_permit).await
                    {
                        debug!(%address, %error, "incoming TCP peer stopped");
                    }
                });
            }
            Err(error) => {
                warn!(%error, "incoming TCP accept failed");
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
        let accepted = tokio::select! {
            () = services.shutdown.cancelled() => return,
            accepted = endpoint.accept_stream() => accepted,
        };
        match accepted {
            Ok(stream) => {
                error_backoff = ACCEPT_ERROR_BACKOFF_MIN;
                let Ok(handshake_permit) = services
                    .incoming_handshake_slots
                    .clone()
                    .try_acquire_owned()
                else {
                    continue;
                };
                let address = stream.remote_addr();
                let services = services.clone();
                let tasks = services.tasks.clone();
                tasks.spawn(async move {
                    if let Err(error) =
                        admit_incoming_stream(stream, address, &services, handshake_permit).await
                    {
                        debug!(%address, %error, "incoming uTP peer stopped");
                    }
                });
            }
            Err(error) => {
                warn!(%error, "incoming uTP accept failed");
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

async fn admit_incoming_stream<S>(
    stream: S,
    address: SocketAddr,
    services: &Services,
    handshake_permit: OwnedSemaphorePermit,
) -> Result<(), ActorError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let permit = tokio::select! {
        () = services.shutdown.cancelled() => return Err(ActorError::Cancelled),
        permit = services.peer_slots.clone().acquire_owned() => permit,
    }
    .map_err(|_| ActorError::Cancelled)?;
    let result = serve_incoming_stream(stream, address, services, handshake_permit).await;
    drop(permit);
    result
}

async fn serve_incoming_stream<S>(
    mut stream: S,
    address: SocketAddr,
    services: &Services,
    handshake_permit: OwnedSemaphorePermit,
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
        drop(handshake_permit);
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
    drop(handshake_permit);
    finish_incoming_stream(encrypted, address, remote, services).await
}

async fn incoming_wire_hashes(services: &Services) -> Result<Vec<Sha1Hash>, ActorError> {
    let records = services.store.list_summaries().await?;
    Ok(records
        .into_iter()
        .filter(|record| {
            matches!(
                record.state,
                TorrentState::Downloading | TorrentState::Seeding
            )
        })
        .filter_map(|record| stored_wire_info_hash(&record))
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
    if remote.peer_id == services.peer_id {
        learn_self_address(services, address.ip());
        return Err(ActorError::Peer(
            "rejected a connection from this daemon to itself".to_owned(),
        ));
    }
    let (record, content) = find_incoming_torrent(remote.info_hash, services).await?;
    if record.completed_pieces.len() != content.pieces.div_ceil(8) {
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
    let _connection = track_connection(services, record.id, PeerDirection::Inbound, false);
    peer.send(PeerMessage::Bitfield(Bytes::copy_from_slice(
        &record.completed_pieces,
    )))
    .await
    .map_err(|error| ActorError::Peer(error.to_string()))?;

    let reciprocal = record.state != TorrentState::Seeding;
    let seed = IncomingSeed {
        torrent_id: record.id,
        reciprocal,
        completed_pieces: record.completed_pieces,
        content,
        interested: false,
        remote_metadata_id: None,
        remote_holepunch_id: None,
        remote_request_queue: None,
    };
    if record.state == TorrentState::Downloading
        && let Some(route) = services
            .incoming_swarms
            .lock()
            .ok()
            .and_then(|routes| routes.get(&record.id).cloned())
    {
        let (completion, completed) = oneshot::channel();
        let incoming = IncomingDownloadPeer {
            peer,
            address,
            remote,
            seed,
            completion,
        };
        match route.send(incoming).await {
            Ok(()) => {
                tokio::select! {
                    () = services.shutdown.cancelled() => return Ok(()),
                    _ = completed => return Ok(()),
                }
            }
            Err(error) => {
                let incoming = error.0;
                peer = incoming.peer;
                return run_incoming_peer(
                    &mut peer,
                    incoming.address,
                    incoming.remote,
                    incoming.seed,
                    services,
                )
                .await;
            }
        }
    }
    run_incoming_peer(&mut peer, address, remote, seed, services).await
}

struct IncomingSeed {
    torrent_id: TorrentId,
    reciprocal: bool,
    completed_pieces: Vec<u8>,
    content: Arc<IncomingContent>,
    interested: bool,
    remote_metadata_id: Option<u8>,
    remote_holepunch_id: Option<u8>,
    /// Outstanding requests the remote will queue for us (`reqq`).
    remote_request_queue: Option<usize>,
}

struct PeerUploadContext {
    address: SocketAddr,
    remote: Handshake,
    seed: IncomingSeed,
    session: UploadSessionContext,
    _guard: UploadSessionGuard,
}

#[allow(clippy::too_many_lines)] // One arm per inbound wire message keeps the upload protocol adjacent.
async fn run_incoming_peer(
    peer: &mut PeerConnection,
    address: SocketAddr,
    remote: Handshake,
    mut seed: IncomingSeed,
    services: &Services,
) -> Result<(), ActorError> {
    let (upload, _upload_guard) = register_upload_session(
        services,
        seed.torrent_id,
        PeerKey {
            ip: address.ip(),
            peer_id: remote.peer_id,
        },
        peer.sender(),
        seed.reciprocal,
    );
    let session = upload.session;
    loop {
        let event = tokio::select! {
            () = services.shutdown.cancelled() => break,
            event = next_peer_event(peer) => event?,
        };
        match event {
            PeerEvent::Message(PeerMessage::Interested) => {
                seed.interested = true;
                let message = if set_upload_interest(services, session, true) {
                    PeerMessage::Unchoke
                } else {
                    PeerMessage::Choke
                };
                peer.send(message)
                    .await
                    .map_err(|error| ActorError::Peer(error.to_string()))?;
            }
            PeerEvent::Message(PeerMessage::NotInterested) => {
                seed.interested = false;
                set_upload_interest(services, session, false);
                peer.send(PeerMessage::Choke)
                    .await
                    .map_err(|error| ActorError::Peer(error.to_string()))?;
            }
            PeerEvent::Message(PeerMessage::Request(request))
                if seed.interested && upload_allowed(services, session) =>
            {
                serve_piece_request(peer, remote, &mut seed, request, upload, services).await?;
            }
            PeerEvent::Message(PeerMessage::Extended {
                extension_id: 0,
                payload,
            }) => {
                let handshake = decode_extension_handshake(&payload, services.metainfo_limit)
                    .map_err(|error| ActorError::Peer(error.to_string()))?;
                seed.remote_metadata_id = handshake.metadata_extension_id;
                seed.remote_holepunch_id = handshake.holepunch_extension_id;
                seed.remote_request_queue = handshake.request_queue;
                if let Some(ip) = handshake.your_ip {
                    learn_self_address(services, ip);
                }
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
                    payload: encode_extension_handshake(Some(seed.content.info.len())),
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
                    &seed.content.info,
                    &payload,
                    services.metainfo_limit,
                )
                .await?;
            }
            PeerEvent::Message(PeerMessage::HashRequest(request)) => {
                serve_hash_request(peer, &seed.content.metainfo, request).await?;
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

async fn handle_peer_upload_event(
    peer: &PeerConnection,
    upload: &mut PeerUploadContext,
    services: &Services,
    event: PeerEvent,
) -> Result<Option<PeerEvent>, ActorError> {
    match event {
        PeerEvent::Message(PeerMessage::Interested) => {
            upload.seed.interested = true;
            let message = if set_upload_interest(services, upload.session.session, true) {
                PeerMessage::Unchoke
            } else {
                PeerMessage::Choke
            };
            peer.send(message)
                .await
                .map_err(|error| ActorError::Peer(error.to_string()))?;
            Ok(None)
        }
        PeerEvent::Message(PeerMessage::NotInterested) => {
            upload.seed.interested = false;
            set_upload_interest(services, upload.session.session, false);
            peer.send(PeerMessage::Choke)
                .await
                .map_err(|error| ActorError::Peer(error.to_string()))?;
            Ok(None)
        }
        PeerEvent::Message(PeerMessage::Request(request)) => {
            if upload.seed.interested && upload_allowed(services, upload.session.session) {
                serve_piece_request(
                    peer,
                    upload.remote,
                    &mut upload.seed,
                    request,
                    upload.session,
                    services,
                )
                .await?;
            } else {
                peer.send(PeerMessage::Choke)
                    .await
                    .map_err(|error| ActorError::Peer(error.to_string()))?;
            }
            Ok(None)
        }
        PeerEvent::Message(PeerMessage::Extended {
            extension_id: 0,
            payload,
        }) => {
            let handshake = decode_extension_handshake(&payload, services.metainfo_limit)
                .map_err(|error| ActorError::Peer(error.to_string()))?;
            upload.seed.remote_metadata_id = handshake.metadata_extension_id;
            upload.seed.remote_holepunch_id = handshake.holepunch_extension_id;
            upload.seed.remote_request_queue = handshake.request_queue;
            if let Some(ip) = handshake.your_ip {
                learn_self_address(services, ip);
            }
            peer.send(PeerMessage::Extended {
                extension_id: 0,
                payload: encode_extension_handshake(Some(upload.seed.content.info.len())),
            })
            .await
            .map_err(|error| ActorError::Peer(error.to_string()))?;
            Ok(None)
        }
        PeerEvent::Message(PeerMessage::Extended {
            extension_id: LOCAL_METADATA_EXTENSION_ID,
            payload,
        }) => {
            serve_metadata_request(
                peer,
                upload.seed.remote_metadata_id,
                &upload.seed.content.info,
                &payload,
                services.metainfo_limit,
            )
            .await?;
            Ok(None)
        }
        PeerEvent::Message(PeerMessage::HashRequest(request)) => {
            serve_hash_request(peer, &upload.seed.content.metainfo, request).await?;
            Ok(None)
        }
        PeerEvent::Message(PeerMessage::Extended {
            extension_id: LOCAL_HOLEPUNCH_EXTENSION_ID,
            payload,
        }) => {
            handle_seed_holepunch(
                peer,
                upload.address,
                upload.remote.info_hash,
                &upload.seed,
                services,
                &payload,
            )
            .await?;
            Ok(None)
        }
        event => Ok(Some(event)),
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
                torrent_id: seed.torrent_id,
                reciprocal: seed.reciprocal,
                completed_pieces: seed.completed_pieces.clone(),
                content: seed.content.clone(),
                interested: false,
                remote_metadata_id: None,
                remote_holepunch_id: None,
                remote_request_queue: None,
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
    let _outbound_permit = tokio::select! {
        () = services.shutdown.cancelled() => return Err(ActorError::Cancelled),
        permit = services.outbound_slots.clone().acquire_owned() => permit,
    }
    .map_err(|_| ActorError::Cancelled)?;
    let _permit = tokio::select! {
        () = services.shutdown.cancelled() => return Err(ActorError::Cancelled),
        permit = services.peer_slots.clone().acquire_owned() => permit,
    }
    .map_err(|_| ActorError::Cancelled)?;
    let endpoint = services
        .utp
        .as_ref()
        .ok_or_else(|| ActorError::Peer("hole punch requires a uTP endpoint".to_owned()))?;
    let info_hash = wire_info_hash(&seed.content.metainfo)?;
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
    let _connection = track_connection(services, seed.torrent_id, PeerDirection::Outbound, false);
    peer.send(PeerMessage::Bitfield(Bytes::copy_from_slice(
        &seed.completed_pieces,
    )))
    .await
    .map_err(|error| ActorError::Peer(error.to_string()))?;
    let remote = Handshake {
        reserved: [0; 8],
        info_hash,
        peer_id: PeerId::from_bytes([0; 20]),
    };
    run_hole_seed_peer(&mut peer, address, remote, seed, services).await
}

async fn run_hole_seed_peer(
    peer: &mut PeerConnection,
    address: SocketAddr,
    remote: Handshake,
    mut seed: IncomingSeed,
    services: &Services,
) -> Result<(), ActorError> {
    let (upload, _upload_guard) = register_upload_session(
        services,
        seed.torrent_id,
        PeerKey {
            ip: address.ip(),
            peer_id: peer.remote_peer_id(),
        },
        peer.sender(),
        seed.reciprocal,
    );
    loop {
        match next_peer_event(peer).await? {
            PeerEvent::Message(PeerMessage::Interested) => {
                seed.interested = true;
                let message = if set_upload_interest(services, upload.session, true) {
                    PeerMessage::Unchoke
                } else {
                    PeerMessage::Choke
                };
                peer.send(message)
                    .await
                    .map_err(|error| ActorError::Peer(error.to_string()))?;
            }
            PeerEvent::Message(PeerMessage::NotInterested) => {
                seed.interested = false;
                set_upload_interest(services, upload.session, false);
            }
            PeerEvent::Message(PeerMessage::Request(request))
                if seed.interested && upload_allowed(services, upload.session) =>
            {
                serve_piece_request(peer, remote, &mut seed, request, upload, services).await?;
            }
            PeerEvent::Message(PeerMessage::HashRequest(request)) => {
                serve_hash_request(peer, &seed.content.metainfo, request).await?;
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
    upload: UploadSessionContext,
    services: &Services,
) -> Result<(), ActorError> {
    let index = usize::try_from(request.piece).map_err(|_| ActorError::Arithmetic)?;
    let length = usize::try_from(request.length).map_err(|_| ActorError::Arithmetic)?;
    if length == 0
        || length > BLOCK_BYTES
        || index >= seed.content.pieces
        || !bit_is_set(&seed.completed_pieces, index)
    {
        return reject_request(peer, remote, request).await;
    }
    let cached = seed
        .content
        .cache
        .lock()
        .ok()
        .and_then(|mut cache| cache.get(index));
    let data = if let Some(data) = cached {
        data
    } else {
        let Some(data) = read_piece(&seed.content.metainfo, index, &services.storage).await? else {
            return reject_request(peer, remote, request).await;
        };
        let verification = prepare_verification(&seed.content.metainfo, index, data.len())?;
        if !verify_piece_offloaded(services, verification, data.clone()).await? {
            return Err(ActorError::Peer(
                "refusing to upload a corrupt completed piece".to_owned(),
            ));
        }
        if let Ok(mut cache) = seed.content.cache.lock() {
            cache.insert(index, data.clone());
        }
        data
    };
    let begin = usize::try_from(request.begin).map_err(|_| ActorError::Arithmetic)?;
    let end = begin.checked_add(length).ok_or(ActorError::Arithmetic)?;
    if end > data.len() {
        return reject_request(peer, remote, request).await;
    }
    let block = data.slice(begin..end);
    let uploaded = u64::try_from(block.len()).map_err(|_| ActorError::Arithmetic)?;
    throttle_upload(services, uploaded).await;
    peer.send(PeerMessage::Piece {
        piece: request.piece,
        begin: request.begin,
        block,
    })
    .await
    .map_err(|error| ActorError::Peer(error.to_string()))?;
    record_uploaded_block(services, seed.torrent_id, uploaded);
    record_peer_upload(services, seed.torrent_id, upload, uploaded);
    Ok(())
}

/// Resolves an incoming info hash to its active torrent and parsed content
/// through the store's in-memory index, parsing the metainfo at most once per
/// torrent activation.
async fn find_incoming_torrent(
    info_hash: Sha1Hash,
    services: &Services,
) -> Result<(TorrentRecord, Arc<IncomingContent>), ActorError> {
    let inactive = || ActorError::Peer("incoming info hash is not active".to_owned());
    let id = services
        .store
        .find_by_wire_hash(info_hash)
        .await?
        .ok_or_else(inactive)?;
    let record = services.store.get_summary(id).await?.ok_or_else(inactive)?;
    if !matches!(
        record.state,
        TorrentState::Downloading | TorrentState::Seeding
    ) {
        return Err(inactive());
    }
    if let Some(content) = services.incoming_content.lock().await.get(&id).cloned() {
        return Ok((record, content));
    }
    let raw = services
        .store
        .get_metainfo(id)
        .await?
        .filter(|raw| !raw.is_empty())
        .ok_or_else(inactive)?;
    let metainfo = Metainfo::parse(
        &raw,
        BencodeLimits {
            input_bytes: services.metainfo_limit,
            byte_string_bytes: services.metainfo_limit,
            ..BencodeLimits::default()
        },
    )
    .map_err(|error| ActorError::Metainfo(error.to_string()))?;
    let content = Arc::new(IncomingContent {
        pieces: piece_count(&metainfo)?,
        info: raw_info_dictionary(&raw, services.metainfo_limit)?,
        metainfo,
        cache: std::sync::Mutex::new(PieceCache::new(services.piece_cache_budget.clone())),
    });
    let mut cache = services.incoming_content.lock().await;
    let content = cache.entry(id).or_insert(content).clone();
    Ok((record, content))
}

fn stored_wire_info_hash(record: &TorrentRecord) -> Option<Sha1Hash> {
    dendrite_persistence::wire_info_hash(record)
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
                services.incoming_content.lock().await.remove(&id);
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
    let mut delay = SWARM_RETRY_MIN;
    let mut announce_event = AnnounceEvent::Started;
    loop {
        cancelled(cancellation)?;
        let round = start_peer_discovery(
            services,
            DiscoveryQuery {
                trackers: &trackers,
                record,
                info_hash,
                left: 0,
                allow_dht: true,
                dht_announce: false,
                announce_event,
                cancellation: cancellation.child_token(),
            },
        );
        let result = match round {
            Ok(discovery) => {
                acquire_metadata_from_discovery(
                    record,
                    &magnet,
                    info_hash,
                    discovery,
                    services,
                    cancellation,
                )
                .await
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(()) => return Ok(()),
            Err(ActorError::Cancelled) => return Err(ActorError::Cancelled),
            Err(error) if retryable_metadata_failure(&error) => {
                debug!(%error, ?delay, "magnet metadata round exhausted; retrying discovery");
            }
            Err(error) => return Err(error),
        }
        announce_event = AnnounceEvent::None;
        tokio::select! {
            () = cancellation.cancelled() => return Err(ActorError::Cancelled),
            () = tokio::time::sleep(delay) => {}
        }
        delay = delay.saturating_mul(2).min(SWARM_RETRY_MAX);
    }
}

fn retryable_metadata_failure(error: &ActorError) -> bool {
    matches!(
        error,
        ActorError::Metadata(_)
            | ActorError::Metainfo(_)
            | ActorError::NoTracker
            | ActorError::NoPeers
            | ActorError::Peer(_)
    )
}

async fn acquire_metadata_from_discovery(
    record: &mut TorrentRecord,
    magnet: &Magnet,
    info_hash: Sha1Hash,
    mut discovery: mpsc::Receiver<DiscoveryEvent>,
    services: &Services,
    cancellation: &CancellationToken,
) -> Result<(), ActorError> {
    let torrent_id = record.id;
    let attempts_cancellation = cancellation.child_token();
    let mut attempts = tokio::task::JoinSet::new();
    let mut candidates = VecDeque::new();
    let mut known = HashSet::new();
    let mut discovery_done = false;
    let mut last_error = None;
    loop {
        while attempts.len() < METADATA_PEER_CONCURRENCY {
            let Some(address) = candidates.pop_front() else {
                break;
            };
            let magnet = magnet.clone();
            let services = services.clone();
            let cancellation = attempts_cancellation.child_token();
            attempts.spawn(async move {
                let result = fetch_and_validate_metadata(
                    torrent_id,
                    address,
                    info_hash,
                    &magnet,
                    &services,
                    &cancellation,
                )
                .await;
                if let Err(error) = &result {
                    debug!(%torrent_id, %address, %error, "metadata peer failed");
                }
                result
            });
        }
        if discovery_done && attempts.is_empty() && candidates.is_empty() {
            return Err(last_error.unwrap_or(ActorError::NoPeers));
        }
        tokio::select! {
            () = cancellation.cancelled() => {
                attempts_cancellation.cancel();
                attempts.abort_all();
                return Err(ActorError::Cancelled);
            }
            event = discovery.recv(), if !discovery_done => match event {
                Some(DiscoveryEvent::Peers(peers)) => {
                    candidates.extend(peers.into_iter().filter(|address| known.insert(*address)));
                }
                Some(DiscoveryEvent::TrackerInterval { .. }) => {}
                Some(DiscoveryEvent::Finished(result)) => {
                    if let Err(error) = result {
                        last_error = Some(error);
                    }
                    discovery_done = true;
                }
                None => discovery_done = true,
            },
            joined = attempts.join_next(), if !attempts.is_empty() => match joined {
                Some(Ok(Ok((raw, parsed)))) => {
                    attempts_cancellation.cancel();
                    attempts.abort_all();
                    record.name = parsed.name;
                    record.total_length = parsed.total_length;
                    record.v1_info_hash = parsed.v1_info_hash;
                    record.v2_info_hash = parsed.v2_info_hash;
                    record.raw_metainfo = raw;
                    replace_record(services, record.clone()).await?;
                    return Ok(());
                }
                Some(Ok(Err(ActorError::Cancelled))) if cancellation.is_cancelled() => {
                    return Err(ActorError::Cancelled);
                }
                Some(Ok(Err(error))) => last_error = Some(error),
                Some(Err(error)) => last_error = Some(ActorError::Peer(error.to_string())),
                None => {}
            }
        }
    }
}

async fn fetch_and_validate_metadata(
    torrent_id: TorrentId,
    address: SocketAddr,
    info_hash: Sha1Hash,
    magnet: &Magnet,
    services: &Services,
    cancellation: &CancellationToken,
) -> Result<(Vec<u8>, Metainfo), ActorError> {
    let (info, mut peer, _metadata_slot, _connection) =
        fetch_metadata(torrent_id, address, info_hash, services, cancellation).await?;
    let result = validate_acquired_metadata(&info, &mut peer, magnet, services).await;
    peer.shutdown();
    result
}

#[cfg(test)]
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
        match fetch_metadata(record.id, address, info_hash, services, cancellation).await {
            Ok((info, mut peer, _metadata_slot, _connection)) => {
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
    let layers = if preliminary.v2_info_hash.is_some() {
        fetch_piece_layers(peer, &preliminary, services.metainfo_limit).await?
    } else {
        BTreeMap::new()
    };
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
    torrent_id: TorrentId,
    address: SocketAddr,
    info_hash: Sha1Hash,
    services: &Services,
    cancellation: &CancellationToken,
) -> Result<
    (
        Vec<u8>,
        PeerConnection,
        OwnedSemaphorePermit,
        ConnectionGuard,
    ),
    ActorError,
> {
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
    let connection = track_connection(services, torrent_id, PeerDirection::Outbound, false);
    peer.send(PeerMessage::Extended {
        extension_id: 0,
        payload: encode_extension_handshake(None),
    })
    .await
    .map_err(|error| ActorError::Metadata(error.to_string()))?;

    let (remote_extension_id, total_size) =
        negotiate_metadata(&mut peer, services.metainfo_limit, cancellation).await?;
    let metadata_slot = acquire_metadata_slot(services, cancellation).await?;
    let piece_count = total_size.div_ceil(METADATA_BLOCK_BYTES);
    let mut pieces = vec![None; piece_count];
    let mut requested = 0_usize;
    let mut received = 0_usize;
    while requested < piece_count && requested < METADATA_REQUEST_PIPELINE {
        cancelled(cancellation)?;
        let piece = u32::try_from(requested).map_err(|_| ActorError::Arithmetic)?;
        peer.send(PeerMessage::Extended {
            extension_id: remote_extension_id,
            payload: encode_metadata_request(piece),
        })
        .await
        .map_err(|error| ActorError::Metadata(error.to_string()))?;
        requested += 1;
    }
    while received < piece_count {
        cancelled(cancellation)?;
        let (piece, block) = receive_metadata_piece(
            &mut peer,
            requested,
            &pieces,
            total_size,
            services.metainfo_limit,
        )
        .await?;
        pieces[piece] = Some(block);
        received += 1;
        if requested < piece_count {
            let piece = u32::try_from(requested).map_err(|_| ActorError::Arithmetic)?;
            peer.send(PeerMessage::Extended {
                extension_id: remote_extension_id,
                payload: encode_metadata_request(piece),
            })
            .await
            .map_err(|error| ActorError::Metadata(error.to_string()))?;
            requested += 1;
        }
    }
    let mut metadata = Vec::with_capacity(total_size.min(METADATA_BLOCK_BYTES));
    for block in pieces {
        metadata.extend_from_slice(
            block
                .as_deref()
                .ok_or_else(|| ActorError::Metadata("metadata piece is missing".to_owned()))?,
        );
    }
    Ok((metadata, peer, metadata_slot, connection))
}

async fn acquire_metadata_slot(
    services: &Services,
    cancellation: &CancellationToken,
) -> Result<OwnedSemaphorePermit, ActorError> {
    tokio::select! {
        () = cancellation.cancelled() => Err(ActorError::Cancelled),
        permit = services.metadata_slots.clone().acquire_owned() => {
            permit.map_err(|_| ActorError::Cancelled)
        }
    }
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
    requested: usize,
    pieces: &[Option<Bytes>],
    total_size: usize,
    metainfo_limit: usize,
) -> Result<(usize, Bytes), ActorError> {
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
                } => {
                    let piece =
                        usize::try_from(response_piece).map_err(|_| ActorError::Arithmetic)?;
                    let offset = piece
                        .checked_mul(METADATA_BLOCK_BYTES)
                        .ok_or(ActorError::Arithmetic)?;
                    let expected_length = total_size
                        .checked_sub(offset)
                        .map(|remaining| remaining.min(METADATA_BLOCK_BYTES));
                    if piece >= requested
                        || pieces.get(piece).is_none_or(Option::is_some)
                        || response_size != total_size
                        || expected_length != Some(block.len())
                    {
                        return Err(ActorError::Metadata(
                            "peer returned an unexpected metadata message".to_owned(),
                        ));
                    }
                    return Ok((piece, block));
                }
                MetadataMessage::Reject { piece } => {
                    let piece = usize::try_from(piece).map_err(|_| ActorError::Arithmetic)?;
                    if piece >= requested || pieces.get(piece).is_none_or(Option::is_some) {
                        return Err(ActorError::Metadata(
                            "peer returned an unexpected metadata message".to_owned(),
                        ));
                    }
                    return Err(ActorError::Metadata(
                        "peer rejected a metadata request".to_owned(),
                    ));
                }
                MetadataMessage::Request { .. } => {
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
    let state = completion_state(record, services).await?;
    update_record_state(record, state, services).await
}

async fn completion_state(
    record: &mut TorrentRecord,
    services: &Services,
) -> Result<TorrentState, ActorError> {
    record.stop_on_complete = services
        .store
        .get_torrent(record.id)
        .await?
        .ok_or(ActorError::Missing)?
        .stop_on_complete;
    if record.stop_on_complete {
        Ok(TorrentState::Stopped)
    } else {
        Ok(TorrentState::Seeding)
    }
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
    let discovery = start_peer_discovery(
        services,
        DiscoveryQuery {
            trackers: &metainfo.trackers,
            record,
            info_hash,
            left: metainfo.total_length.saturating_sub(record.downloaded),
            allow_dht: !metainfo.private,
            dht_announce: true,
            announce_event,
            cancellation: cancellation.child_token(),
        },
    )?;
    run_peer_swarm_with_discovery(
        Vec::new(),
        Some(discovery),
        info_hash,
        metainfo,
        record,
        services,
        cancellation,
    )
    .await
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
        record.downloaded = record
            .downloaded
            .checked_add(piece_content_length(metainfo, piece)?)
            .ok_or(ActorError::Arithmetic)?;
        persist_download_progress(services, record).await?;
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

fn public_web_seed_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => public_web_seed_ipv4(ip),
        IpAddr::V6(ip) => {
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

#[cfg(test)]
async fn run_peer_swarm(
    peers: Vec<SocketAddr>,
    info_hash: Sha1Hash,
    metainfo: &Metainfo,
    record: &mut TorrentRecord,
    services: &Services,
    cancellation: &CancellationToken,
) -> Result<(), ActorError> {
    run_peer_swarm_with_discovery(
        peers,
        None,
        info_hash,
        metainfo,
        record,
        services,
        cancellation,
    )
    .await
}

#[allow(clippy::too_many_lines)] // The select loop keeps all mutable swarm I/O queues in one task.
async fn run_peer_swarm_with_discovery(
    peers: Vec<SocketAddr>,
    mut discovery: Option<mpsc::Receiver<DiscoveryEvent>>,
    info_hash: Sha1Hash,
    metainfo: &Metainfo,
    record: &mut TorrentRecord,
    services: &Services,
    cancellation: &CancellationToken,
) -> Result<(), ActorError> {
    let (mut swarm, mut events) =
        initialize_swarm(peers, info_hash, metainfo, record, services, cancellation)?;
    let (incoming_sender, mut incoming_peers) =
        mpsc::channel(services.per_torrent_peer_limit.max(1));
    let _incoming_guard = register_incoming_swarm(record.id, incoming_sender, services);
    let mut discovery_error = None;
    let mut writes = FuturesUnordered::<PieceWriteFuture<'_>>::new();
    let mut verifications = FuturesUnordered::<PieceVerifyFuture<'_>>::new();
    let mut flush_tasks = FuturesUnordered::<PieceFlushFuture<'_>>::new();
    let mut pending_pieces = Vec::new();
    let mut pending_paths = HashSet::new();
    let mut flush_interval = tokio::time::interval(services.piece_flush_interval);
    flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    flush_interval.tick().await;
    let mut retention_interval = tokio::time::interval(PEER_RETENTION_INTERVAL);
    retention_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    retention_interval.tick().await;
    let mut last_discovery = Instant::now();
    let mut last_dht_announce = Instant::now();
    let mut tracker_next_due: HashMap<String, Instant> = HashMap::new();
    loop {
        if cancellation.is_cancelled() {
            shutdown_swarm(&swarm);
            return Err(ActorError::Cancelled);
        }
        let pending_io = !writes.is_empty()
            || !verifications.is_empty()
            || !flush_tasks.is_empty()
            || !pending_pieces.is_empty();
        if swarm.picker.remaining() == 0 && !pending_io {
            stop_workers(&swarm.workers);
            return Ok(());
        }
        fill_peer_slots(&mut swarm);
        if std::mem::take(&mut swarm.schedule_dirty) {
            schedule_pieces(&mut swarm, metainfo)?;
        }
        if swarm_is_exhausted(&swarm, discovery.is_none(), pending_io) {
            shutdown_swarm(&swarm);
            if let Some(error) = discovery_error {
                return Err(error);
            }
            return Err(swarm
                .last_error
                .map_or(ActorError::NoPeers, ActorError::Peer));
        }
        let event = tokio::select! {
            () = cancellation.cancelled() => {
                shutdown_swarm(&swarm);
                return Err(ActorError::Cancelled);
            },
            event = events.recv() => {
                SwarmLoopEvent::Worker(event.ok_or(ActorError::NoPeers)?)
            },
            incoming = incoming_peers.recv() => {
                SwarmLoopEvent::Incoming(incoming.ok_or(ActorError::Cancelled)?)
            },
            event = receive_discovery_event(&mut discovery), if discovery.is_some() => {
                SwarmLoopEvent::Discovery(event)
            },
            verified = verifications.next(), if !verifications.is_empty() => {
                SwarmLoopEvent::Verified(verified.ok_or(ActorError::Arithmetic)?)
            },
            stored = writes.next(), if !writes.is_empty() => {
                SwarmLoopEvent::Stored(stored.ok_or(ActorError::Arithmetic)?)
            },
            flushed = flush_tasks.next(), if !flush_tasks.is_empty() => {
                SwarmLoopEvent::Flushed(flushed.ok_or(ActorError::Arithmetic)?)
            },
            _ = flush_interval.tick() => SwarmLoopEvent::FlushTick,
            _ = retention_interval.tick() => SwarmLoopEvent::RetentionTick,
        };
        match event {
            SwarmLoopEvent::Incoming(incoming) => {
                spawn_incoming_swarm_worker(&mut swarm, incoming);
            }
            SwarmLoopEvent::Worker(PeerWorkerEvent::Complete {
                worker,
                piece,
                result,
            }) => {
                if let Some(data) = prepare_piece_result(&mut swarm, worker, piece, result, record)?
                {
                    queue_piece_verification(
                        &mut verifications,
                        worker,
                        piece,
                        data,
                        metainfo,
                        services,
                    )?;
                }
            }
            SwarmLoopEvent::Verified(verified) => {
                if let Some(data) = accept_verified_piece(&mut swarm, verified)? {
                    let (worker, piece) = (data.0, data.1);
                    queue_piece_write(&mut writes, worker, piece, data.2, metainfo, services);
                }
            }
            SwarmLoopEvent::Worker(event) => {
                handle_worker_event(&mut swarm, event, record)?;
            }
            SwarmLoopEvent::Discovery(Some(DiscoveryEvent::Peers(peers))) => {
                enqueue_peer_candidates(&mut swarm, peers, false);
            }
            SwarmLoopEvent::Discovery(Some(DiscoveryEvent::TrackerInterval { url, interval })) => {
                let interval = interval.clamp(TRACKER_INTERVAL_MIN, TRACKER_INTERVAL_MAX);
                tracker_next_due.insert(url, Instant::now() + interval);
            }
            SwarmLoopEvent::Discovery(Some(DiscoveryEvent::Finished(result))) => {
                if let Err(error) = result {
                    discovery_error = Some(error);
                }
                discovery = None;
            }
            SwarmLoopEvent::Discovery(None) => discovery = None,
            SwarmLoopEvent::Stored(stored) => {
                stage_stored_piece(&mut swarm, stored, &mut pending_pieces, &mut pending_paths)?;
            }
            SwarmLoopEvent::Flushed(flushed) => {
                commit_flushed_pieces(&mut swarm, flushed, metainfo, record, services).await?;
            }
            SwarmLoopEvent::FlushTick => {
                queue_piece_flush(
                    &mut flush_tasks,
                    &mut pending_pieces,
                    &mut pending_paths,
                    &services.storage,
                );
            }
            SwarmLoopEvent::RetentionTick => {
                retain_productive_peers(&mut swarm)?;
                if discovery.is_none() && last_discovery.elapsed() >= PEER_REANNOUNCE_INTERVAL {
                    let due_trackers = due_tracker_tiers(&metainfo.trackers, &tracker_next_due);
                    let dht_announce = last_dht_announce.elapsed() >= DHT_ANNOUNCE_INTERVAL;
                    if dht_announce {
                        last_dht_announce = Instant::now();
                    }
                    discovery = Some(restart_peer_discovery(
                        services,
                        &due_trackers,
                        metainfo,
                        record,
                        info_hash,
                        cancellation,
                        dht_announce,
                    )?);
                    last_discovery = Instant::now();
                }
            }
        }
    }
}

fn restart_peer_discovery(
    services: &Services,
    trackers: &[Vec<String>],
    metainfo: &Metainfo,
    record: &TorrentRecord,
    info_hash: Sha1Hash,
    cancellation: &CancellationToken,
    dht_announce: bool,
) -> Result<mpsc::Receiver<DiscoveryEvent>, ActorError> {
    start_peer_discovery(
        services,
        DiscoveryQuery {
            trackers,
            record,
            info_hash,
            left: metainfo.total_length.saturating_sub(record.downloaded),
            allow_dht: !metainfo.private,
            dht_announce,
            announce_event: AnnounceEvent::None,
            cancellation: cancellation.child_token(),
        },
    )
}

/// Tracker tiers reduced to the trackers whose requested interval has
/// elapsed, so re-discovery every minute re-uses DHT, LSD, and PEX without
/// hammering trackers that asked for a longer interval.
fn due_tracker_tiers(
    tiers: &[Vec<String>],
    next_due: &HashMap<String, Instant>,
) -> Vec<Vec<String>> {
    let now = Instant::now();
    tiers
        .iter()
        .map(|tier| {
            tier.iter()
                .filter(|tracker| {
                    Url::parse(tracker).map_or(true, |url| {
                        next_due.get(&url.to_string()).is_none_or(|due| *due <= now)
                    })
                })
                .cloned()
                .collect::<Vec<_>>()
        })
        .filter(|tier| !tier.is_empty())
        .collect()
}

fn register_incoming_swarm(
    torrent_id: TorrentId,
    sender: mpsc::Sender<IncomingDownloadPeer>,
    services: &Services,
) -> IncomingSwarmGuard {
    if let Ok(mut routes) = services.incoming_swarms.lock() {
        routes.insert(torrent_id, sender.clone());
    }
    IncomingSwarmGuard {
        routes: services.incoming_swarms.clone(),
        torrent_id,
        sender,
    }
}

fn queue_piece_verification<'a>(
    verifications: &mut FuturesUnordered<PieceVerifyFuture<'a>>,
    worker: usize,
    piece: usize,
    data: Bytes,
    metainfo: &Metainfo,
    services: &'a Services,
) -> Result<(), ActorError> {
    let verification = prepare_verification(metainfo, piece, data.len())?;
    verifications.push(Box::pin(async move {
        let result = verify_piece_offloaded(services, verification, data.clone()).await;
        PieceVerifyResult {
            worker,
            piece,
            data,
            result,
        }
    }));
    Ok(())
}

/// Applies a verification outcome: a corrupt piece penalises and drops the
/// peer, a duplicate of a piece already being written is discarded, and a
/// fresh valid piece is claimed for writing while duplicate requests on other
/// workers are cancelled.
fn accept_verified_piece(
    swarm: &mut SwarmState,
    verified: PieceVerifyResult,
) -> Result<Option<(usize, usize, Bytes)>, ActorError> {
    let PieceVerifyResult {
        worker,
        piece,
        data,
        result,
    } = verified;
    let reserved = data.len();
    swarm.schedule_dirty = true;
    if !result? {
        if let Some(peer) = swarm
            .workers
            .get(&worker)
            .and_then(|handle| handle.peer_key)
        {
            record_peer_failure(&swarm.services, swarm.torrent_id, peer);
        }
        swarm
            .picker
            .mark_request_failed(piece)
            .map_err(|error| ActorError::Peer(error.to_string()))?;
        swarm.last_error = Some(format!("piece {piece} failed its integrity check"));
        swarm.budget.release(reserved);
        remove_worker(swarm, worker)?;
        return Ok(None);
    }
    if !swarm.writing.insert(piece) {
        swarm
            .picker
            .mark_request_failed(piece)
            .map_err(|error| ActorError::Peer(error.to_string()))?;
        swarm.budget.release(reserved);
        return Ok(None);
    }
    swarm
        .picker
        .set_enabled(piece, piece.saturating_add(1), false)
        .map_err(|error| ActorError::Peer(error.to_string()))?;
    for (other, assignments) in &swarm.assignments {
        if assignments
            .iter()
            .any(|assignment| assignment.piece == piece)
            && let Some(handle) = swarm.workers.get(other)
        {
            let _result_ignored = handle
                .commands
                .try_send(PeerWorkerCommand::Cancel { piece });
        }
    }
    Ok(Some((worker, piece, data)))
}

fn queue_piece_write<'a>(
    writes: &mut FuturesUnordered<PieceWriteFuture<'a>>,
    worker: usize,
    piece: usize,
    data: Bytes,
    metainfo: &'a Metainfo,
    services: &'a Services,
) {
    let storage = &services.storage;
    let bytes = u64::try_from(data.len()).unwrap_or(u64::MAX);
    writes.push(Box::pin(async move {
        PieceWriteResult {
            worker,
            piece,
            bytes,
            result: write_piece_unflushed(metainfo, piece, data, storage).await,
        }
    }));
}

fn swarm_is_exhausted(swarm: &SwarmState, discovery_finished: bool, pending_io: bool) -> bool {
    discovery_finished
        && !pending_io
        && swarm.candidates.is_empty()
        && swarm.connecting == 0
        && swarm.assignments.is_empty()
        && swarm.workers.is_empty()
}

fn queue_piece_flush<'a>(
    flush_tasks: &mut FuturesUnordered<PieceFlushFuture<'a>>,
    pending_pieces: &mut Vec<usize>,
    pending_paths: &mut HashSet<TorrentPath>,
    storage: &'a StorageHandle,
) {
    if !flush_tasks.is_empty() || pending_pieces.is_empty() {
        return;
    }
    let batch_pieces = std::mem::take(pending_pieces);
    let batch_paths = std::mem::take(pending_paths);
    flush_tasks.push(Box::pin(async move {
        PieceFlushResult {
            pieces: batch_pieces,
            result: sync_paths(batch_paths, storage).await,
        }
    }));
}

fn initialize_swarm(
    peers: Vec<SocketAddr>,
    info_hash: Sha1Hash,
    metainfo: &Metainfo,
    record: &TorrentRecord,
    services: &Services,
    cancellation: &CancellationToken,
) -> Result<(SwarmState, mpsc::Receiver<PeerWorkerEvent>), ActorError> {
    let pieces = piece_count(metainfo)?;
    let event_capacity = services.per_torrent_peer_limit.saturating_mul(4).max(1);
    let (event_sender, events) = mpsc::channel(event_capacity);
    let mut picker = PiecePicker::new(pieces, 4);
    for index in 0..pieces {
        if bit_is_set(&record.completed_pieces, index) {
            picker
                .mark_complete(index)
                .map_err(|error| ActorError::Peer(error.to_string()))?;
        }
    }
    let mut swarm = SwarmState {
        workers: HashMap::with_capacity(services.per_torrent_peer_limit),
        assignments: HashMap::new(),
        picker,
        budget: BudgetAccount::new(services.download_budget.clone()),
        schedule_dirty: true,
        connecting: 0,
        last_error: None,
        writing: HashSet::new(),
        candidates: VecDeque::new(),
        known_candidates: HashMap::new(),
        next_worker: 0,
        event_sender,
        info_hash,
        piece_count: pieces,
        completed_pieces: record.completed_pieces.clone(),
        services: services.clone(),
        cancellation: cancellation.child_token(),
        allow_pex: !metainfo.private,
        torrent_id: record.id,
        peer_limit: services.per_torrent_peer_limit,
    };
    enqueue_peer_candidates(&mut swarm, peers, false);
    fill_peer_slots(&mut swarm);
    Ok((swarm, events))
}

async fn receive_discovery_event(
    discovery: &mut Option<mpsc::Receiver<DiscoveryEvent>>,
) -> Option<DiscoveryEvent> {
    match discovery {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

fn shutdown_swarm(swarm: &SwarmState) {
    swarm.cancellation.cancel();
    stop_workers(&swarm.workers);
}

fn enqueue_peer_candidates(
    swarm: &mut SwarmState,
    peers: impl IntoIterator<Item = SocketAddr>,
    force_utp: bool,
) {
    let now = Instant::now();
    for address in peers {
        if is_self_candidate(&swarm.services, address) {
            continue;
        }
        let candidate = PeerCandidate { address, force_utp };
        if swarm
            .known_candidates
            .get(&candidate)
            .is_some_and(|seen| now.duration_since(*seen) < CANDIDATE_RETRY_INTERVAL)
        {
            continue;
        }
        if swarm
            .workers
            .values()
            .any(|handle| handle.address == address)
        {
            // Already connected; refresh the timestamp so the address is not
            // dialled the moment it drops, but leave the queue alone.
            swarm.known_candidates.insert(candidate, now);
            continue;
        }
        if swarm.known_candidates.len() >= KNOWN_CANDIDATE_LIMIT {
            // Reset the deduplication window rather than grow without bound;
            // a returning address is simply retried later.
            swarm.known_candidates.clear();
        }
        swarm.known_candidates.insert(candidate, now);
        if swarm.candidates.len() >= QUEUED_CANDIDATE_LIMIT {
            swarm.candidates.pop_front();
        }
        swarm.candidates.push_back(candidate);
    }
}

fn fill_peer_slots(swarm: &mut SwarmState) {
    while swarm.workers.len() < swarm.peer_limit
        && swarm.connecting < PEER_CONNECT_CONCURRENCY.min(swarm.peer_limit)
    {
        let Some(candidate) = swarm.candidates.pop_front() else {
            break;
        };
        spawn_swarm_worker(swarm, candidate);
    }
}

fn spawn_swarm_worker(swarm: &mut SwarmState, candidate: PeerCandidate) {
    let worker = swarm.next_worker;
    swarm.next_worker = swarm.next_worker.saturating_add(1);
    let (commands, receiver) = mpsc::channel(PEER_COMMAND_CAPACITY);
    let cancellation = swarm.cancellation.child_token();
    swarm.workers.insert(
        worker,
        PeerWorkerHandle {
            commands,
            bitfield: None,
            idle: false,
            choked: true,
            address: candidate.address,
            peer_key: None,
            seed: false,
            useful_pieces: 0,
            verified_bytes: 0,
            connected_at: Instant::now(),
            last_verified: None,
            cancellation: cancellation.clone(),
            assigned_bytes: 0,
            target_bytes: ASSIGNMENT_TARGET_MIN,
            wants_more: false,
            skip_generation: None,
        },
    );
    swarm.connecting = swarm.connecting.saturating_add(1);
    swarm.services.tasks.spawn(peer_worker(
        PeerWorkerContext {
            worker,
            address: candidate.address,
            info_hash: swarm.info_hash,
            piece_count: swarm.piece_count,
            completed_pieces: swarm.completed_pieces.clone(),
            services: swarm.services.clone(),
            events: swarm.event_sender.clone(),
            cancellation,
            allow_pex: swarm.allow_pex,
            force_utp: candidate.force_utp,
            torrent_id: swarm.torrent_id,
        },
        receiver,
    ));
}

fn spawn_incoming_swarm_worker(swarm: &mut SwarmState, incoming: IncomingDownloadPeer) {
    let worker = swarm.next_worker;
    swarm.next_worker = swarm.next_worker.saturating_add(1);
    let (commands, receiver) = mpsc::channel(PEER_COMMAND_CAPACITY);
    let cancellation = swarm.cancellation.child_token();
    swarm.workers.insert(
        worker,
        PeerWorkerHandle {
            commands,
            bitfield: None,
            idle: false,
            choked: true,
            address: incoming.address,
            peer_key: None,
            seed: false,
            useful_pieces: 0,
            verified_bytes: 0,
            connected_at: Instant::now(),
            last_verified: None,
            cancellation: cancellation.clone(),
            assigned_bytes: 0,
            target_bytes: ASSIGNMENT_TARGET_MIN,
            wants_more: false,
            skip_generation: None,
        },
    );
    swarm.connecting = swarm.connecting.saturating_add(1);
    swarm.services.tasks.spawn(promoted_incoming_peer_worker(
        PeerWorkerContext {
            worker,
            address: incoming.address,
            info_hash: swarm.info_hash,
            piece_count: swarm.piece_count,
            completed_pieces: swarm.completed_pieces.clone(),
            services: swarm.services.clone(),
            events: swarm.event_sender.clone(),
            cancellation,
            allow_pex: swarm.allow_pex,
            force_utp: false,
            torrent_id: swarm.torrent_id,
        },
        receiver,
        incoming,
    ));
}

fn per_torrent_peer_limit(global_limit: usize) -> usize {
    global_limit.div_ceil(4).clamp(1, ACTIVE_PEER_LIMIT)
}

fn outbound_connection_limit(global_limit: usize) -> usize {
    if global_limit == 1 {
        return 1;
    }
    let inbound_reserve = global_limit.div_ceil(4).clamp(1, INCOMING_PEER_LIMIT);
    global_limit.saturating_sub(inbound_reserve).max(1)
}

fn useful_piece_count(remote: &[u8], local: &[u8], pieces: usize) -> usize {
    let bytes = pieces.div_ceil(8);
    let spare = bytes.saturating_mul(8).saturating_sub(pieces);
    let mut useful = 0_usize;
    for index in 0..bytes {
        let remote_byte = remote.get(index).copied().unwrap_or(0);
        let local_byte = local.get(index).copied().unwrap_or(0);
        let mut word = remote_byte & !local_byte;
        if index + 1 == bytes && spare > 0 {
            word &= 0xff_u8 << spare;
        }
        useful += word.count_ones() as usize;
    }
    useful
}

fn peer_retention_score(handle: &PeerWorkerHandle, reputation: Option<PeerReputation>) -> u128 {
    let elapsed_ms = u64::try_from(handle.connected_at.elapsed().as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    let bytes_per_second = handle
        .verified_bytes
        .saturating_mul(1_000)
        .checked_div(elapsed_ms)
        .unwrap_or(0);
    let seed_bonus = u128::from(handle.seed) << 120;
    let recent_bonus = u128::from(
        handle
            .last_verified
            .is_some_and(|verified| verified.elapsed() < Duration::from_secs(30)),
    ) << 112;
    let contribution = reputation.map_or(0, |score| {
        score.verified_from.saturating_sub(score.uploaded_to / 2)
    });
    let failure_penalty = reputation.map_or(0, |score| u128::from(score.failures) << 40);
    (seed_bonus
        | recent_bonus
        | (u128::from(bytes_per_second) << 48)
        | (u128::from(contribution) << 16)
        | u128::try_from(handle.useful_pieces).unwrap_or(u128::MAX))
    .saturating_sub(failure_penalty)
}

fn lowest_idle_worker(swarm: &SwarmState, include_seeds: bool) -> Option<usize> {
    let reputation = swarm.services.upload_policy.lock().ok();
    swarm
        .workers
        .iter()
        .filter(|(_, handle)| handle.idle && (include_seeds || !handle.seed))
        .min_by_key(|(_, handle)| {
            let score = handle.peer_key.and_then(|peer| {
                reputation
                    .as_ref()
                    .and_then(|policy| policy.reputation.get(&(swarm.torrent_id, peer)).copied())
            });
            (!handle.choked, peer_retention_score(handle, score))
        })
        .map(|(worker, _)| *worker)
}

fn enforce_peer_limit(swarm: &mut SwarmState) -> Result<(), ActorError> {
    while swarm.workers.len() > swarm.peer_limit {
        let Some(worker) = lowest_idle_worker(swarm, true) else {
            break;
        };
        remove_worker(swarm, worker)?;
    }
    Ok(())
}

fn retain_productive_peers(swarm: &mut SwarmState) -> Result<(), ActorError> {
    if swarm.candidates.is_empty() || swarm.workers.len() < swarm.peer_limit {
        return Ok(());
    }
    let seeds = swarm.workers.values().filter(|handle| handle.seed).count();
    let Some(worker) = lowest_idle_worker(swarm, false) else {
        return Ok(());
    };
    let audition_candidate = swarm.workers.get(&worker).is_some_and(|handle| {
        handle.choked
            || handle
                .last_verified
                .is_none_or(|verified| verified.elapsed() >= Duration::from_secs(60))
    });
    if seeds < PREFERRED_SEED_PEERS || audition_candidate {
        remove_worker(swarm, worker)?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)] // One match arm per worker event keeps the state transitions adjacent.
fn handle_worker_event(
    swarm: &mut SwarmState,
    event: PeerWorkerEvent,
    record: &mut TorrentRecord,
) -> Result<(), ActorError> {
    match event {
        PeerWorkerEvent::Ready {
            worker,
            peer_id,
            bitfield,
            peers,
        } => {
            swarm.connecting = swarm.connecting.saturating_sub(1);
            let seed = all_complete(&bitfield, swarm.piece_count);
            let useful_pieces =
                useful_piece_count(&bitfield, &record.completed_pieces, swarm.piece_count);
            swarm
                .picker
                .add_peer_bitfield(&bitfield)
                .map_err(|error| ActorError::Peer(error.to_string()))?;
            if let Some(handle) = swarm.workers.get_mut(&worker) {
                handle.bitfield = Some(bitfield);
                handle.peer_key = Some(PeerKey {
                    ip: handle.address.ip(),
                    peer_id,
                });
                handle.idle = true;
                handle.choked = false;
                handle.seed = seed;
                handle.useful_pieces = useful_pieces;
                handle.connected_at = Instant::now();
                handle.skip_generation = None;
                set_session_interesting(
                    &swarm.services,
                    swarm.torrent_id,
                    PeerKey {
                        ip: handle.address.ip(),
                        peer_id,
                    },
                    useful_pieces > 0,
                );
            }
            swarm.schedule_dirty = true;
            enqueue_peer_candidates(swarm, peers, false);
            enforce_peer_limit(swarm)?;
        }
        PeerWorkerEvent::Complete { .. } => {
            return Err(ActorError::Peer(
                "piece completion bypassed the storage pipeline".to_owned(),
            ));
        }
        PeerWorkerEvent::Gone { worker, error } => {
            swarm.connecting = swarm.connecting.saturating_sub(usize::from(
                swarm
                    .workers
                    .get(&worker)
                    .is_some_and(|handle| handle.bitfield.is_none()),
            ));
            remove_worker(swarm, worker)?;
            swarm.last_error = Some(error);
        }
        PeerWorkerEvent::Peers { worker, peers } => {
            if swarm.workers.contains_key(&worker) {
                enqueue_peer_candidates(swarm, peers, false);
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
                if !bit_is_set(&record.completed_pieces, piece) {
                    handle.useful_pieces = handle.useful_pieces.saturating_add(1);
                    if handle.useful_pieces == 1
                        && let Some(peer) = handle.peer_key
                    {
                        set_session_interesting(&swarm.services, swarm.torrent_id, peer, true);
                    }
                }
                handle.skip_generation = None;
                swarm.schedule_dirty = true;
            }
            if bit_is_set(&record.completed_pieces, piece) {
                let _result_ignored = handle.commands.try_send(PeerWorkerCommand::Have {
                    piece: u32::try_from(piece).map_err(|_| ActorError::Arithmetic)?,
                });
            }
        }
        PeerWorkerEvent::ChokeState { worker, choked } => {
            if let Some(handle) = swarm.workers.get_mut(&worker) {
                handle.choked = choked;
            }
            if !choked {
                swarm.schedule_dirty = true;
            }
        }
        PeerWorkerEvent::NeedPieces {
            worker,
            rate_bytes_per_second,
        } => {
            if let Some(handle) = swarm.workers.get_mut(&worker) {
                handle.wants_more = true;
                handle.target_bytes = assignment_target(rate_bytes_per_second);
            }
            swarm.schedule_dirty = true;
        }
        PeerWorkerEvent::HolePunch { worker, address } => {
            if swarm.workers.contains_key(&worker) {
                enqueue_peer_candidates(swarm, [address], true);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
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

#[cfg(test)]
async fn discover_with_dht(
    trackers: &[Vec<String>],
    record: &TorrentRecord,
    services: &Services,
    info_hash: Sha1Hash,
    left: u64,
    allow_dht: bool,
    announce_event: AnnounceEvent,
) -> Result<Vec<SocketAddr>, ActorError> {
    let receiver = start_peer_discovery(
        services,
        DiscoveryQuery {
            trackers,
            record,
            info_hash,
            left,
            allow_dht,
            dht_announce: false,
            announce_event,
            cancellation: services.shutdown.child_token(),
        },
    )?;
    collect_discovered_peers(receiver).await
}

fn start_peer_discovery(
    services: &Services,
    query: DiscoveryQuery<'_>,
) -> Result<mpsc::Receiver<DiscoveryEvent>, ActorError> {
    let http = HttpTrackerClient::new(services.tracker_response_limit)
        .map_err(|error| ActorError::Peer(error.to_string()))?;
    let udp = UdpTrackerClient::new(services.tracker_response_limit)
        .map_err(|error| ActorError::Peer(error.to_string()))?;
    let request = tracker_request(
        query.record,
        services,
        query.info_hash,
        query.left,
        query.announce_event,
    );
    let urls = tracker_urls(query.trackers);
    let dht = if query.allow_dht && !services.dht_bootstrap.is_empty() {
        Some(if let Some(client) = &services.dht {
            client.clone()
        } else {
            DhtClient::new(128, 65_507, Duration::from_secs(2))
                .map_err(|error| ActorError::Peer(error.to_string()))?
        })
    } else {
        None
    };
    let bootstrap = services.dht_bootstrap.clone();
    let lsd_services = services.clone();
    let tracker_attempted = !urls.is_empty();
    let info_hash = query.info_hash;
    let allow_dht = query.allow_dht;
    let dht_announce_port = query
        .dht_announce
        .then(|| services.advertised_peer_port.load(Ordering::Acquire));
    let cancellation = query.cancellation;
    let (sender, receiver) = mpsc::channel(DISCOVERY_EVENT_CAPACITY);
    let slots = Arc::new(Semaphore::new(TRACKER_ANNOUNCE_CONCURRENCY));
    let mut sources = tokio::task::JoinSet::new();
    for url in urls {
        let http = http.clone();
        let slots = slots.clone();
        sources.spawn(async move {
            let permit = slots.acquire_owned().await;
            let result = match permit {
                Ok(_permit) => announce_tracker(&http, udp, &url, request).await,
                Err(error) => Err(error.to_string()),
            };
            DiscoverySourceResult::Tracker { url, result }
        });
    }
    if let Some(dht) = dht {
        sources.spawn(async move {
            let lookup = async {
                match dht_announce_port {
                    Some(port) => {
                        dht.get_peers_and_announce(info_hash, &bootstrap, port)
                            .await
                    }
                    None => dht.get_peers(info_hash, &bootstrap).await,
                }
            };
            let result = match tokio::time::timeout(DHT_DISCOVERY_TIMEOUT, lookup).await {
                Ok(Ok(peers)) => Ok(peers),
                Ok(Err(error)) => Err(error.to_string()),
                Err(_) => Err("DHT lookup timed out".to_owned()),
            };
            DiscoverySourceResult::Dht(result)
        });
    }
    if allow_dht {
        sources.spawn(async move {
            DiscoverySourceResult::Lsd(discover_lsd_peer(info_hash, &lsd_services).await)
        });
    }
    services.tasks.spawn(forward_discovery_sources(
        sources,
        sender.clone(),
        cancellation,
        tracker_attempted,
        allow_dht,
    ));
    drop(sender);
    Ok(receiver)
}

async fn forward_discovery_sources(
    mut sources: tokio::task::JoinSet<DiscoverySourceResult>,
    sender: mpsc::Sender<DiscoveryEvent>,
    cancellation: CancellationToken,
    tracker_attempted: bool,
    allow_dht: bool,
) {
    let mut found = false;
    let mut last_error = None;
    loop {
        let joined = tokio::select! {
            () = cancellation.cancelled() => return,
            () = sender.closed() => return,
            joined = sources.join_next() => joined,
        };
        let Some(joined) = joined else {
            break;
        };
        let source = match joined {
            Ok(source) => source,
            Err(error) => {
                last_error = Some(error.to_string());
                continue;
            }
        };
        let peers = match source {
            DiscoverySourceResult::Tracker { url, result } => match result {
                Ok((peers, interval)) => {
                    debug!(tracker = %url, peers = peers.len(), ?interval, "tracker announce succeeded");
                    if sender
                        .send(DiscoveryEvent::TrackerInterval {
                            url: url.to_string(),
                            interval,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    peers
                }
                Err(error) => {
                    debug!(tracker = %url, %error, "tracker announce failed");
                    last_error = Some(error);
                    Vec::new()
                }
            },
            DiscoverySourceResult::Dht(result) => match result {
                Ok(peers) => {
                    debug!(peers = peers.len(), "DHT peer discovery succeeded");
                    peers
                }
                Err(error) => {
                    debug!(%error, "DHT peer discovery failed");
                    last_error = Some(error);
                    Vec::new()
                }
            },
            DiscoverySourceResult::Lsd(peers) => peers,
        };
        if !peers.is_empty() {
            found = true;
            if sender.send(DiscoveryEvent::Peers(peers)).await.is_err() {
                return;
            }
        }
    }
    let result = if found {
        Ok(())
    } else if !tracker_attempted && !allow_dht {
        Err(ActorError::NoTracker)
    } else {
        Err(last_error.map_or(ActorError::NoPeers, ActorError::Peer))
    };
    let _result_ignored = sender.send(DiscoveryEvent::Finished(result)).await;
}

#[cfg(test)]
async fn collect_discovered_peers(
    mut receiver: mpsc::Receiver<DiscoveryEvent>,
) -> Result<Vec<SocketAddr>, ActorError> {
    let mut peers = HashSet::new();
    while let Some(event) = receiver.recv().await {
        match event {
            DiscoveryEvent::Peers(discovered) => peers.extend(discovered),
            DiscoveryEvent::TrackerInterval { .. } => {}
            DiscoveryEvent::Finished(result) => {
                if peers.is_empty() {
                    result?;
                }
                return Ok(peers.into_iter().collect());
            }
        }
    }
    if peers.is_empty() {
        Err(ActorError::NoPeers)
    } else {
        Ok(peers.into_iter().collect())
    }
}

fn tracker_request(
    record: &TorrentRecord,
    services: &Services,
    info_hash: Sha1Hash,
    left: u64,
    announce_event: AnnounceEvent,
) -> TrackerRequest {
    let peer_id = services.peer_id.as_bytes();
    TrackerRequest {
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
    }
}

fn tracker_urls(trackers: &[Vec<String>]) -> Vec<Url> {
    let mut unique = HashSet::new();
    trackers
        .iter()
        .flatten()
        .filter_map(|tracker| Url::parse(tracker).ok())
        .filter(|url| matches!(url.scheme(), "http" | "https" | "udp"))
        .filter(|url| unique.insert(url.clone()))
        .collect()
}

/// Announces to one tracker and returns its peers with the re-announce
/// interval it asked for (the larger of `interval` and `min interval`).
async fn announce_tracker(
    http: &HttpTrackerClient,
    udp: UdpTrackerClient,
    url: &Url,
    request: TrackerRequest,
) -> Result<(Vec<SocketAddr>, Duration), String> {
    match url.scheme() {
        "http" | "https" => http
            .announce(url, request)
            .await
            .map(|announce| {
                let interval = announce
                    .interval
                    .max(announce.minimum_interval.unwrap_or(Duration::ZERO));
                (announce.peers, interval)
            })
            .map_err(|error| error.to_string()),
        "udp" => udp
            .announce(url, request)
            .await
            .map(|announce| {
                let interval = announce
                    .interval
                    .max(announce.minimum_interval.unwrap_or(Duration::ZERO));
                (announce.peers, interval)
            })
            .map_err(|error| error.to_string()),
        _ => Err("unsupported tracker scheme".to_owned()),
    }
}

#[cfg(test)]
async fn discover_tracker_peers(
    trackers: &[Vec<String>],
    record: &TorrentRecord,
    services: &Services,
    info_hash: Sha1Hash,
    left: u64,
    announce_event: AnnounceEvent,
) -> Result<Vec<SocketAddr>, ActorError> {
    let receiver = start_peer_discovery(
        services,
        DiscoveryQuery {
            trackers,
            record,
            info_hash,
            left,
            allow_dht: false,
            dht_announce: false,
            announce_event,
            cancellation: services.shutdown.child_token(),
        },
    )?;
    collect_discovered_peers(receiver).await
}

async fn peer_worker(context: PeerWorkerContext, commands: mpsc::Receiver<PeerWorkerCommand>) {
    let outbound_permit = tokio::select! {
        () = context.cancellation.cancelled() => return,
        permit = context.services.outbound_slots.clone().acquire_owned() => permit,
    };
    let Ok(_outbound_permit) = outbound_permit else {
        return;
    };
    let permit = tokio::select! {
        () = context.cancellation.cancelled() => return,
        permit = context.services.peer_slots.clone().acquire_owned() => permit,
    };
    let Ok(_permit) = permit else {
        return;
    };
    let Some((peer, seed, upload)) = establish_peer_worker(&context).await else {
        return;
    };
    let _connection = track_connection(
        &context.services,
        context.torrent_id,
        PeerDirection::Outbound,
        seed,
    );
    run_ready_peer_worker(&context, commands, peer, upload).await;
}

/// A piece queued on a worker: blocks are requested from `next_begin`, the
/// buffer is allocated lazily on the first block, and the piece is reported
/// complete when every byte has arrived.
struct QueuedPiece {
    piece: u32,
    length: usize,
    next_begin: usize,
    received: usize,
    buffer: Option<Vec<u8>>,
}

/// Per-peer request pipeline spanning piece boundaries.
struct DownloadPipeline {
    queue: VecDeque<QueuedPiece>,
    pending: HashMap<(u32, u32), u32>,
    limit_blocks: usize,
    /// Outstanding requests the remote advertised it will queue (`reqq`).
    remote_limit: usize,
    rate: RateEstimator,
    want_sent: bool,
    active: Option<ActiveDownloadGuard>,
}

impl DownloadPipeline {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            pending: HashMap::new(),
            limit_blocks: BLOCK_PIPELINE,
            remote_limit: usize::MAX,
            rate: RateEstimator::new(),
            want_sent: false,
            active: None,
        }
    }

    fn unrequested_bytes(&self) -> usize {
        self.queue
            .iter()
            .map(|queued| queued.length.saturating_sub(queued.next_begin))
            .sum()
    }
}

/// Exponentially weighted bytes-per-second estimate sampled every
/// `RATE_SAMPLE_INTERVAL`.
struct RateEstimator {
    window_start: Instant,
    window_bytes: u64,
    rate: u64,
}

impl RateEstimator {
    fn new() -> Self {
        Self {
            window_start: Instant::now(),
            window_bytes: 0,
            rate: 0,
        }
    }

    fn observe(&mut self, bytes: usize) {
        self.window_bytes = self
            .window_bytes
            .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
        let elapsed = self.window_start.elapsed();
        if elapsed >= RATE_SAMPLE_INTERVAL {
            let millis = u64::try_from(elapsed.as_millis())
                .unwrap_or(u64::MAX)
                .max(1);
            let sample = self.window_bytes.saturating_mul(1000) / millis;
            self.rate = if self.rate == 0 {
                sample
            } else {
                (self.rate.saturating_mul(3).saturating_add(sample)) / 4
            };
            self.window_bytes = 0;
            self.window_start = Instant::now();
        }
    }

    const fn bytes_per_second(&self) -> u64 {
        self.rate
    }
}

fn pipeline_limit(rate_bytes_per_second: u64) -> usize {
    let wanted = rate_bytes_per_second.saturating_mul(PIPELINE_TARGET_SECONDS)
        / u64::try_from(BLOCK_BYTES).unwrap_or(u64::MAX);
    usize::try_from(wanted)
        .unwrap_or(usize::MAX)
        .clamp(BLOCK_PIPELINE, BLOCK_PIPELINE_MAX)
}

#[allow(clippy::too_many_lines)] // The select loop owns every command and wire transition of a peer.
async fn run_ready_peer_worker(
    context: &PeerWorkerContext,
    mut commands: mpsc::Receiver<PeerWorkerCommand>,
    mut peer: PeerConnection,
    mut upload: Option<PeerUploadContext>,
) {
    let mut pipeline = DownloadPipeline::new();
    let forwarder = peer_event_forwarder(context);
    loop {
        if let Some(limit) = upload
            .as_ref()
            .and_then(|upload| upload.seed.remote_request_queue)
        {
            pipeline.remote_limit = limit.max(1);
            pipeline.limit_blocks = pipeline.limit_blocks.min(pipeline.remote_limit);
        }
        if let Err(error) = fill_request_pipeline(&peer, &mut pipeline, context).await {
            report_worker_failure(context, &mut pipeline, error).await;
            break;
        }
        let timeout = if pipeline.pending.is_empty() {
            context.services.peer_message_timeout.saturating_mul(4)
        } else {
            context.services.peer_message_timeout
        };
        let input = tokio::select! {
            () = context.cancellation.cancelled() => break,
            command = commands.recv() => match command {
                Some(command) => PeerWorkerInput::Command(command),
                None => break,
            },
            event = next_peer_event_with_timeout(&mut peer, timeout) => match event {
                Ok(event) => PeerWorkerInput::Event(event),
                Err(error) => {
                    report_worker_failure(context, &mut pipeline, error).await;
                    break;
                }
            },
        };
        match input {
            PeerWorkerInput::Command(PeerWorkerCommand::Download { piece, length }) => {
                let Ok(piece) = u32::try_from(piece) else {
                    report_worker_failure(context, &mut pipeline, ActorError::Arithmetic).await;
                    break;
                };
                pipeline.queue.push_back(QueuedPiece {
                    piece,
                    length,
                    next_begin: 0,
                    received: 0,
                    buffer: None,
                });
                pipeline.want_sent = false;
                if pipeline.active.is_none() {
                    pipeline.active =
                        Some(track_active_download(&context.services, context.torrent_id));
                }
            }
            PeerWorkerInput::Command(PeerWorkerCommand::Cancel { piece }) => {
                pipeline.want_sent = false;
                let Ok(wire_piece) = u32::try_from(piece) else {
                    continue;
                };
                if !cancel_queued_piece(&peer, &mut pipeline, wire_piece).await {
                    continue;
                }
                if pipeline.queue.is_empty() {
                    pipeline.active = None;
                }
                if context
                    .events
                    .send(PeerWorkerEvent::Complete {
                        worker: context.worker,
                        piece,
                        result: PieceResult::Cancelled,
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
            PeerWorkerInput::Command(PeerWorkerCommand::Have { piece }) => {
                debug!(address = %context.address, piece, "announcing completed piece to peer");
                if let Some(upload) = upload.as_mut()
                    && let Ok(piece) = usize::try_from(piece)
                {
                    set_bit(&mut upload.seed.completed_pieces, piece);
                }
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
            PeerWorkerInput::Event(PeerEvent::Message(PeerMessage::Piece {
                piece,
                begin,
                block,
            })) => match receive_block(&mut pipeline, piece, begin, &block, &forwarder) {
                Ok(Some((piece, data))) => {
                    if pipeline.queue.is_empty() {
                        pipeline.active = None;
                    }
                    if context
                        .events
                        .send(PeerWorkerEvent::Complete {
                            worker: context.worker,
                            piece,
                            result: PieceResult::Data(data),
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    report_worker_failure(context, &mut pipeline, error).await;
                    break;
                }
            },
            PeerWorkerInput::Event(PeerEvent::Message(PeerMessage::Choke)) => {
                // BEP 3: a choke discards every outstanding request. Return the
                // queued pieces to the picker and keep the connection so an
                // unchoke can resume without a reconnect.
                pipeline.pending.clear();
                pipeline.want_sent = false;
                pipeline.active = None;
                let mut lost = false;
                for queued in pipeline.queue.drain(..) {
                    if context
                        .events
                        .send(PeerWorkerEvent::Complete {
                            worker: context.worker,
                            piece: queued.piece as usize,
                            result: PieceResult::Cancelled,
                        })
                        .await
                        .is_err()
                    {
                        lost = true;
                        break;
                    }
                }
                if lost
                    || context
                        .events
                        .send(PeerWorkerEvent::ChokeState {
                            worker: context.worker,
                            choked: true,
                        })
                        .await
                        .is_err()
                {
                    break;
                }
            }
            PeerWorkerInput::Event(PeerEvent::Message(PeerMessage::Unchoke)) => {
                if context
                    .events
                    .send(PeerWorkerEvent::ChokeState {
                        worker: context.worker,
                        choked: false,
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
            PeerWorkerInput::Event(PeerEvent::Disconnected) => {
                let error = if pipeline.queue.is_empty() {
                    "peer disconnected".to_owned()
                } else {
                    "peer disconnected during piece transfer".to_owned()
                };
                report_worker_failure(context, &mut pipeline, ActorError::Peer(error)).await;
                break;
            }
            PeerWorkerInput::Event(PeerEvent::Failed(error)) => {
                let error = if pipeline.queue.is_empty() {
                    error
                } else {
                    format!("peer session failed during piece transfer: {error}")
                };
                report_worker_failure(context, &mut pipeline, ActorError::Peer(error)).await;
                break;
            }
            PeerWorkerInput::Event(event) => {
                if !forward_idle_peer_event(context, &peer, upload.as_mut(), event).await {
                    break;
                }
            }
        }
    }
    peer.shutdown();
}

/// Reports a fatal worker error: as a failed piece when pieces were queued (the
/// actor then drops the worker and returns the rest), otherwise as `Gone`.
async fn report_worker_failure(
    context: &PeerWorkerContext,
    pipeline: &mut DownloadPipeline,
    error: ActorError,
) {
    pipeline.pending.clear();
    pipeline.active = None;
    let event = if let Some(queued) = pipeline.queue.pop_front() {
        debug!(
            address = %context.address,
            piece = queued.piece,
            %error,
            "piece download failed"
        );
        PeerWorkerEvent::Complete {
            worker: context.worker,
            piece: queued.piece as usize,
            result: PieceResult::Failed(error.to_string()),
        }
    } else {
        PeerWorkerEvent::Gone {
            worker: context.worker,
            error: error.to_string(),
        }
    };
    let _result_ignored = context.events.send(event).await;
}

/// Drops a queued piece and cancels its outstanding requests on the wire.
/// Returns `false` when the piece was not queued (already completed).
async fn cancel_queued_piece(
    peer: &PeerConnection,
    pipeline: &mut DownloadPipeline,
    piece: u32,
) -> bool {
    let Some(position) = pipeline
        .queue
        .iter()
        .position(|queued| queued.piece == piece)
    else {
        return false;
    };
    pipeline.queue.remove(position);
    let mut cancelled = Vec::new();
    pipeline.pending.retain(|(pending_piece, begin), length| {
        if *pending_piece == piece {
            cancelled.push((*begin, *length));
            false
        } else {
            true
        }
    });
    cancelled.sort_unstable();
    for (begin, length) in cancelled {
        let _result_ignored = peer
            .send_unacked(PeerMessage::Cancel(BlockRequest {
                piece,
                begin,
                length,
            }))
            .await;
    }
    true
}

fn receive_block(
    pipeline: &mut DownloadPipeline,
    piece: u32,
    begin: u32,
    block: &Bytes,
    forwarder: &PeerEventForwarder,
) -> Result<Option<(usize, Bytes)>, ActorError> {
    let expected = pipeline
        .pending
        .remove(&(piece, begin))
        .ok_or_else(|| ActorError::Peer("peer returned an unsolicited block".to_owned()))?;
    if usize::try_from(expected).ok() != Some(block.len()) {
        return Err(ActorError::Peer(
            "peer returned a block with the wrong length".to_owned(),
        ));
    }
    let position = pipeline
        .queue
        .iter()
        .position(|queued| queued.piece == piece)
        .ok_or_else(|| ActorError::Peer("peer returned a block for another piece".to_owned()))?;
    let queued = pipeline
        .queue
        .get_mut(position)
        .ok_or(ActorError::Arithmetic)?;
    let start = usize::try_from(begin).map_err(|_| ActorError::Arithmetic)?;
    let end = start
        .checked_add(block.len())
        .filter(|end| *end <= queued.length)
        .ok_or(ActorError::Arithmetic)?;
    let length = queued.length;
    let buffer = queued.buffer.get_or_insert_with(|| vec![0_u8; length]);
    buffer
        .get_mut(start..end)
        .ok_or(ActorError::Arithmetic)?
        .copy_from_slice(block);
    queued.received = queued
        .received
        .checked_add(block.len())
        .ok_or(ActorError::Arithmetic)?;
    pipeline.rate.observe(block.len());
    pipeline.limit_blocks = pipeline_limit(pipeline.rate.bytes_per_second())
        .min(pipeline.remote_limit)
        .max(1);
    record_downloaded_block(forwarder, block.len());
    if queued.received < queued.length {
        return Ok(None);
    }
    let done = pipeline
        .queue
        .remove(position)
        .ok_or(ActorError::Arithmetic)?;
    let data = Bytes::from(done.buffer.unwrap_or_default());
    Ok(Some((done.piece as usize, data)))
}

async fn promoted_incoming_peer_worker(
    context: PeerWorkerContext,
    commands: mpsc::Receiver<PeerWorkerCommand>,
    incoming: IncomingDownloadPeer,
) {
    let _completion = IncomingCompletion(Some(incoming.completion));
    let mut peer = incoming.peer;
    let (upload_session, upload_guard) = register_upload_session(
        &context.services,
        context.torrent_id,
        PeerKey {
            ip: incoming.address.ip(),
            peer_id: incoming.remote.peer_id,
        },
        peer.sender(),
        incoming.seed.reciprocal,
    );
    let mut upload = PeerUploadContext {
        address: incoming.address,
        remote: incoming.remote,
        seed: incoming.seed,
        session: upload_session,
        _guard: upload_guard,
    };
    let ready = async {
        if context.allow_pex && peer.remote_supports_extensions() {
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
        await_unchoke_with_upload(
            &mut peer,
            context.piece_count,
            &mut upload,
            &context.services,
        )
        .await
    };
    let (available, peers) = match tokio::select! {
        () = context.cancellation.cancelled() => return,
        ready = ready => ready,
    } {
        Ok(ready) => ready,
        Err(error) => {
            let _result_ignored = context
                .events
                .send(PeerWorkerEvent::Gone {
                    worker: context.worker,
                    error: error.to_string(),
                })
                .await;
            return;
        }
    };
    let bitfield = available.unwrap_or_else(|| vec![0; context.piece_count.div_ceil(8)]);
    let _seed = all_complete(&bitfield, context.piece_count)
        .then(|| track_seed_peer(&context.services, context.torrent_id));
    if context
        .events
        .send(PeerWorkerEvent::Ready {
            worker: context.worker,
            peer_id: peer.remote_peer_id(),
            bitfield,
            peers,
        })
        .await
        .is_ok()
    {
        run_ready_peer_worker(&context, commands, peer, Some(upload)).await;
    }
}

fn peer_event_forwarder(context: &PeerWorkerContext) -> PeerEventForwarder {
    let downloaded_bytes = context
        .services
        .torrent_activity
        .lock()
        .map(|mut torrents| {
            torrents
                .entry(context.torrent_id)
                .or_default()
                .downloaded_bytes
                .clone()
        })
        .unwrap_or_default();
    PeerEventForwarder { downloaded_bytes }
}

async fn establish_peer_worker(
    context: &PeerWorkerContext,
) -> Option<(PeerConnection, bool, Option<PeerUploadContext>)> {
    let result = connect_peer_worker(context).await;
    let (peer, bitfield, peers, upload) = match result {
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
    let seed = all_complete(&bitfield, context.piece_count);
    if context
        .events
        .send(PeerWorkerEvent::Ready {
            worker: context.worker,
            peer_id: peer.remote_peer_id(),
            bitfield,
            peers,
        })
        .await
        .is_err()
    {
        return None;
    }
    Some((peer, seed, upload))
}

async fn forward_idle_peer_event(
    context: &PeerWorkerContext,
    peer: &PeerConnection,
    upload: Option<&mut PeerUploadContext>,
    event: PeerEvent,
) -> bool {
    let event = if let Some(upload) = upload {
        match handle_peer_upload_event(peer, upload, &context.services, event).await {
            Ok(Some(event)) => event,
            Ok(None) => return true,
            Err(error) => return report_peer_gone(context, error.to_string()).await,
        }
    } else {
        event
    };
    let worker_event = match event {
        PeerEvent::Message(PeerMessage::Have(piece)) => {
            debug!(address = %context.address, piece, "peer announced piece availability");
            Some(PeerWorkerEvent::Have {
                worker: context.worker,
                piece,
            })
        }
        PeerEvent::Message(PeerMessage::Choke) => Some(PeerWorkerEvent::ChokeState {
            worker: context.worker,
            choked: true,
        }),
        PeerEvent::Message(PeerMessage::Unchoke) => Some(PeerWorkerEvent::ChokeState {
            worker: context.worker,
            choked: false,
        }),
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
    context: &PeerWorkerContext,
) -> Result<
    (
        PeerConnection,
        Vec<u8>,
        Vec<SocketAddr>,
        Option<PeerUploadContext>,
    ),
    ActorError,
> {
    let address = context.address;
    let info_hash = context.info_hash;
    let piece_count = context.piece_count;
    let completed_pieces = &context.completed_pieces;
    let torrent_id = context.torrent_id;
    let services = &context.services;
    let mut reserved = [0_u8; 8];
    reserved[5] |= 0x10;
    reserved[7] |= 0x01;
    let handshake = Handshake {
        reserved,
        info_hash,
        peer_id: services.peer_id,
    };
    let mut peer = if context.force_utp {
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
    if peer.remote_peer_id() == services.peer_id {
        learn_self_address(services, address.ip());
        return Err(ActorError::Peer(
            "candidate address belongs to this daemon".to_owned(),
        ));
    }
    let mut upload = outgoing_peer_upload_context(
        services,
        torrent_id,
        address,
        info_hash,
        completed_pieces,
        &peer,
    )
    .await;
    peer.send(PeerMessage::Bitfield(Bytes::copy_from_slice(
        completed_pieces,
    )))
    .await
    .map_err(|error| ActorError::Peer(error.to_string()))?;
    if context.allow_pex && peer.remote_supports_extensions() {
        peer.send(PeerMessage::Extended {
            extension_id: 0,
            payload: encode_extension_handshake(
                upload.as_ref().map(|upload| upload.seed.content.info.len()),
            ),
        })
        .await
        .map_err(|error| ActorError::Peer(error.to_string()))?;
    }
    peer.send(PeerMessage::Interested)
        .await
        .map_err(|error| ActorError::Peer(error.to_string()))?;
    let (available, peers) = if let Some(upload) = upload.as_mut() {
        await_unchoke_with_upload(&mut peer, piece_count, upload, services).await?
    } else {
        await_unchoke(&mut peer, piece_count).await?
    };
    Ok((
        peer,
        available.unwrap_or_else(|| vec![0; piece_count.div_ceil(8)]),
        peers,
        upload,
    ))
}

async fn outgoing_peer_upload_context(
    services: &Services,
    torrent_id: TorrentId,
    address: SocketAddr,
    info_hash: Sha1Hash,
    completed_pieces: &[u8],
    peer: &PeerConnection,
) -> Option<PeerUploadContext> {
    let (record, content) = find_incoming_torrent(info_hash, services).await.ok()?;
    let remote = Handshake {
        reserved: peer.remote_reserved(),
        info_hash,
        peer_id: peer.remote_peer_id(),
    };
    let (session, guard) = register_upload_session(
        services,
        torrent_id,
        PeerKey {
            ip: address.ip(),
            peer_id: remote.peer_id,
        },
        peer.sender(),
        record.state != TorrentState::Seeding,
    );
    Some(PeerUploadContext {
        address,
        remote,
        seed: IncomingSeed {
            torrent_id,
            reciprocal: record.state != TorrentState::Seeding,
            completed_pieces: completed_pieces.to_vec(),
            content,
            interested: false,
            remote_metadata_id: None,
            remote_holepunch_id: None,
            remote_request_queue: None,
        },
        session,
        _guard: guard,
    })
}

/// Hands pieces to workers whose assigned bytes are below their target, one
/// extra piece to workers that reported an emptying pipeline, and skips
/// workers that had nothing selectable at the current picker generation.
fn schedule_pieces(swarm: &mut SwarmState, metainfo: &Metainfo) -> Result<usize, ActorError> {
    let generation = swarm.picker.generation();
    let mut eligible: Vec<_> = swarm
        .workers
        .iter()
        .filter(|(_, handle)| {
            handle.bitfield.is_some()
                && !handle.choked
                && handle.useful_pieces > 0
                && handle.skip_generation != Some(generation)
                && (handle.wants_more || handle.assigned_bytes < handle.target_bytes)
        })
        .map(|(worker, handle)| {
            (
                std::cmp::Reverse((
                    handle.seed,
                    handle.last_verified.is_some(),
                    handle.verified_bytes,
                )),
                *worker,
            )
        })
        .collect();
    eligible.sort_unstable();
    if eligible.is_empty() {
        return Ok(0);
    }
    let max_piece = piece_length(metainfo, 0)?;
    let mut scheduled = 0_usize;
    'workers: for (_, worker) in eligible {
        let mut granted = 0_usize;
        let mut channel_full = false;
        loop {
            let Some(handle) = swarm.workers.get_mut(&worker) else {
                break;
            };
            let need =
                (handle.wants_more && granted == 0) || handle.assigned_bytes < handle.target_bytes;
            if !need {
                break;
            }
            let Some(bitfield) = handle.bitfield.as_deref() else {
                break;
            };
            if !swarm.budget.reserve(max_piece) {
                break 'workers;
            }
            let held = swarm.assignments.get(&worker);
            let selected = swarm
                .picker
                .select_where(bitfield, SelectionMode::RarestFirst, |piece| {
                    held.is_none_or(|assignments| {
                        !assignments
                            .iter()
                            .any(|assignment| assignment.piece == piece)
                    })
                })
                .map_err(|error| ActorError::Peer(error.to_string()))?;
            let Some(piece) = selected else {
                swarm.budget.release(max_piece);
                handle.skip_generation = Some(generation);
                break;
            };
            let length = piece_length(metainfo, piece)?;
            swarm.budget.release(max_piece.saturating_sub(length));
            if handle
                .commands
                .try_send(PeerWorkerCommand::Download { piece, length })
                .is_err()
            {
                swarm
                    .picker
                    .mark_request_failed(piece)
                    .map_err(|error| ActorError::Peer(error.to_string()))?;
                swarm.budget.release(length);
                channel_full = true;
                break;
            }
            swarm
                .assignments
                .entry(worker)
                .or_default()
                .push(PieceAssignment { piece, length });
            handle.assigned_bytes = handle.assigned_bytes.saturating_add(length);
            handle.idle = false;
            granted += 1;
            scheduled += 1;
        }
        if channel_full {
            remove_worker(swarm, worker)?;
            continue;
        }
        if granted > 0
            && let Some(handle) = swarm.workers.get_mut(&worker)
        {
            handle.wants_more = false;
        }
    }
    Ok(scheduled)
}

fn assignment_target(rate_bytes_per_second: u64) -> usize {
    let target = rate_bytes_per_second.saturating_mul(ASSIGNMENT_TARGET_SECONDS);
    usize::try_from(target)
        .unwrap_or(usize::MAX)
        .clamp(ASSIGNMENT_TARGET_MIN, ASSIGNMENT_TARGET_MAX)
}

fn take_assignment(swarm: &mut SwarmState, worker: usize, piece: usize) -> Option<PieceAssignment> {
    let (taken, drained) = {
        let assignments = swarm.assignments.get_mut(&worker)?;
        let position = assignments
            .iter()
            .position(|assignment| assignment.piece == piece)?;
        let taken = assignments.remove(position);
        (taken, assignments.is_empty())
    };
    if drained {
        swarm.assignments.remove(&worker);
    }
    if let Some(handle) = swarm.workers.get_mut(&worker) {
        handle.assigned_bytes = handle.assigned_bytes.saturating_sub(taken.length);
    }
    sync_worker_idle(swarm, worker);
    Some(taken)
}

fn release_worker_assignments(swarm: &mut SwarmState, worker: usize) -> Result<(), ActorError> {
    if let Some(assignments) = swarm.assignments.remove(&worker) {
        for assignment in assignments {
            swarm
                .picker
                .mark_request_failed(assignment.piece)
                .map_err(|error| ActorError::Peer(error.to_string()))?;
            swarm.budget.release(assignment.length);
        }
    }
    if let Some(handle) = swarm.workers.get_mut(&worker) {
        handle.assigned_bytes = 0;
    }
    sync_worker_idle(swarm, worker);
    swarm.schedule_dirty = true;
    Ok(())
}

fn sync_worker_idle(swarm: &mut SwarmState, worker: usize) {
    let busy = swarm.assignments.contains_key(&worker);
    if let Some(handle) = swarm.workers.get_mut(&worker) {
        handle.idle = handle.bitfield.is_some() && !busy;
    }
}

/// Bookkeeping for a worker's piece outcome. Valid-looking data is returned
/// for verification; every other outcome releases the budget immediately.
fn prepare_piece_result(
    swarm: &mut SwarmState,
    worker: usize,
    piece: usize,
    result: PieceResult,
    record: &TorrentRecord,
) -> Result<Option<Bytes>, ActorError> {
    let reserved = take_assignment(swarm, worker, piece).map_or(0, |assignment| assignment.length);
    swarm.schedule_dirty = true;
    match result {
        PieceResult::Data(_data) if bit_is_set(&record.completed_pieces, piece) => {
            swarm
                .picker
                .mark_complete(piece)
                .map_err(|error| ActorError::Peer(error.to_string()))?;
            swarm.budget.release(reserved);
        }
        PieceResult::Data(data) => {
            if swarm.writing.contains(&piece) {
                swarm
                    .picker
                    .mark_request_failed(piece)
                    .map_err(|error| ActorError::Peer(error.to_string()))?;
                swarm.budget.release(reserved);
                return Ok(None);
            }
            return Ok(Some(data));
        }
        PieceResult::Cancelled => {
            swarm
                .picker
                .mark_request_failed(piece)
                .map_err(|error| ActorError::Peer(error.to_string()))?;
            swarm.budget.release(reserved);
        }
        PieceResult::Failed(error) => {
            if let Some(peer) = swarm
                .workers
                .get(&worker)
                .and_then(|handle| handle.peer_key)
            {
                record_peer_failure(&swarm.services, swarm.torrent_id, peer);
            }
            swarm
                .picker
                .mark_request_failed(piece)
                .map_err(|failure| ActorError::Peer(failure.to_string()))?;
            swarm.last_error = Some(error);
            swarm.budget.release(reserved);
            remove_worker(swarm, worker)?;
        }
    }
    Ok(None)
}

fn stage_stored_piece(
    swarm: &mut SwarmState,
    stored: PieceWriteResult,
    pending_pieces: &mut Vec<usize>,
    pending_paths: &mut HashSet<TorrentPath>,
) -> Result<(), ActorError> {
    pending_paths.extend(stored.result?);
    swarm
        .picker
        .mark_complete(stored.piece)
        .map_err(|error| ActorError::Peer(error.to_string()))?;
    pending_pieces.push(stored.piece);
    let contributing_peer = if let Some(handle) = swarm.workers.get_mut(&stored.worker) {
        handle.verified_bytes = handle.verified_bytes.saturating_add(stored.bytes);
        handle.last_verified = Some(Instant::now());
        handle.peer_key
    } else {
        None
    };
    if let Some(peer) = contributing_peer {
        record_verified_download(&swarm.services, swarm.torrent_id, peer, stored.bytes);
    }
    swarm
        .budget
        .release(usize::try_from(stored.bytes).unwrap_or(usize::MAX));
    swarm.schedule_dirty = true;
    let wire_piece = u32::try_from(stored.piece).map_err(|_| ActorError::Arithmetic)?;
    for handle in swarm.workers.values() {
        let _result_ignored = handle
            .commands
            .try_send(PeerWorkerCommand::Have { piece: wire_piece });
    }
    Ok(())
}

async fn commit_flushed_pieces(
    swarm: &mut SwarmState,
    flushed: PieceFlushResult,
    metainfo: &Metainfo,
    record: &mut TorrentRecord,
    services: &Services,
) -> Result<(), ActorError> {
    flushed.result?;
    for piece in flushed.pieces {
        swarm.writing.remove(&piece);
        set_bit(&mut record.completed_pieces, piece);
        set_bit(&mut swarm.completed_pieces, piece);
        for handle in swarm.workers.values_mut() {
            if handle
                .bitfield
                .as_deref()
                .is_some_and(|bitfield| bit_is_set(bitfield, piece))
            {
                handle.useful_pieces = handle.useful_pieces.saturating_sub(1);
                if handle.useful_pieces == 0
                    && let Some(peer) = handle.peer_key
                {
                    set_session_interesting(&swarm.services, swarm.torrent_id, peer, false);
                }
            }
        }
        record.downloaded = record
            .downloaded
            .checked_add(piece_content_length(metainfo, piece)?)
            .ok_or(ActorError::Arithmetic)?;
    }
    persist_download_progress(services, record).await?;
    Ok(())
}

fn remove_worker(swarm: &mut SwarmState, worker: usize) -> Result<(), ActorError> {
    release_worker_assignments(swarm, worker)?;
    if let Some(handle) = swarm.workers.remove(&worker) {
        handle.cancellation.cancel();
        let _result_ignored = handle.commands.try_send(PeerWorkerCommand::Shutdown);
        if let Some(bitfield) = handle.bitfield {
            swarm
                .picker
                .remove_peer_bitfield(&bitfield)
                .map_err(|error| ActorError::Peer(error.to_string()))?;
        }
    }
    swarm.schedule_dirty = true;
    Ok(())
}

fn stop_workers(workers: &HashMap<usize, PeerWorkerHandle>) {
    for handle in workers.values() {
        handle.cancellation.cancel();
        let _result_ignored = handle.commands.try_send(PeerWorkerCommand::Shutdown);
    }
}

fn record_downloaded_block(forwarder: &PeerEventForwarder, bytes: usize) {
    if let Ok(bytes) = u64::try_from(bytes) {
        forwarder
            .downloaded_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Keeps up to `limit_blocks` requests outstanding across the queued pieces
/// and asks the actor for more pieces once fewer than one pipeline's worth of
/// blocks remain unrequested.
async fn fill_request_pipeline(
    peer: &PeerConnection,
    pipeline: &mut DownloadPipeline,
    context: &PeerWorkerContext,
) -> Result<(), ActorError> {
    let DownloadPipeline {
        queue,
        pending,
        limit_blocks,
        ..
    } = pipeline;
    for queued in queue.iter_mut() {
        while pending.len() < *limit_blocks && queued.next_begin < queued.length {
            let block_length = BLOCK_BYTES.min(queued.length - queued.next_begin);
            let begin = u32::try_from(queued.next_begin).map_err(|_| ActorError::Arithmetic)?;
            let wire_length = u32::try_from(block_length).map_err(|_| ActorError::Arithmetic)?;
            peer.send_unacked(PeerMessage::Request(BlockRequest {
                piece: queued.piece,
                begin,
                length: wire_length,
            }))
            .await
            .map_err(|error| ActorError::Peer(error.to_string()))?;
            pending.insert((queued.piece, begin), wire_length);
            queued.next_begin = queued
                .next_begin
                .checked_add(block_length)
                .ok_or(ActorError::Arithmetic)?;
        }
        if pending.len() >= *limit_blocks {
            break;
        }
    }
    let low_water = pipeline.limit_blocks.saturating_mul(BLOCK_BYTES);
    if !pipeline.want_sent && pipeline.unrequested_bytes() < low_water {
        context
            .events
            .send(PeerWorkerEvent::NeedPieces {
                worker: context.worker,
                rate_bytes_per_second: pipeline.rate.bytes_per_second(),
            })
            .await
            .map_err(|_| ActorError::Cancelled)?;
        pipeline.want_sent = true;
    }
    Ok(())
}

async fn connect_outgoing_peer(
    address: SocketAddr,
    handshake: Handshake,
    limits: PeerCodecLimits,
    services: &Services,
) -> Result<PeerConnection, dendrite_net::peer::PeerSessionError> {
    let plaintext_first = matches!(
        services.encryption,
        EncryptionPolicy::Disabled | EncryptionPolicy::PlaintextPreferred
    );
    if !plaintext_first {
        match PeerConnection::connect_encrypted(address, handshake, limits).await {
            Ok(peer) => return Ok(peer),
            Err(error) if services.encryption == EncryptionPolicy::Required => return Err(error),
            Err(error) => {
                debug!(%address, %error, "encrypted peer connection failed; trying plaintext");
            }
        }
    }
    let plain = match PeerConnection::connect(address, handshake, limits).await {
        Ok(peer) => return Ok(peer),
        Err(error) => error,
    };
    if services.encryption == EncryptionPolicy::PlaintextPreferred
        && let Ok(peer) = PeerConnection::connect_encrypted(address, handshake, limits).await
    {
        return Ok(peer);
    }
    let Some(utp) = &services.utp else {
        return Err(plain);
    };
    utp.connect_peer(address, handshake, limits)
        .await
        .map_err(|_| plain)
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

async fn await_unchoke_with_upload(
    peer: &mut PeerConnection,
    piece_count: usize,
    upload: &mut PeerUploadContext,
    services: &Services,
) -> Result<(Option<Vec<u8>>, Vec<SocketAddr>), ActorError> {
    let expected_bitfield = piece_count.div_ceil(8);
    let mut available = None;
    let mut peers = Vec::new();
    loop {
        let event = next_peer_event(peer).await?;
        let Some(event) = handle_peer_upload_event(peer, upload, services, event).await? else {
            continue;
        };
        match event {
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
            }
            PeerEvent::Message(PeerMessage::Extended {
                extension_id: LOCAL_PEX_EXTENSION_ID,
                payload,
            }) => peers.extend(pex_addresses(&payload)?),
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
    record.downloaded = 0;
    let pieces = piece_count(metainfo)?;
    for index in 0..pieces {
        cancelled(cancellation)?;
        if let Some(piece) = read_piece(metainfo, index, &services.storage).await?
            && verify_piece(metainfo, index, &piece)?
        {
            set_bit(&mut record.completed_pieces, index);
            record.downloaded = record
                .downloaded
                .checked_add(piece_content_length(metainfo, index)?)
                .ok_or(ActorError::Arithmetic)?;
        }
        if index % 64 == 63 {
            persist_download_progress(services, record).await?;
        }
    }
    let state = if all_complete(&record.completed_pieces, pieces) {
        completion_state(record, services).await?
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
    let touched = write_piece_unflushed(metainfo, index, piece, storage).await?;
    sync_paths(touched, storage).await
}

async fn write_piece_unflushed(
    metainfo: &Metainfo,
    index: usize,
    piece: Bytes,
    storage: &StorageHandle,
) -> Result<HashSet<TorrentPath>, ActorError> {
    if metainfo.v1_piece_hashes.is_empty() {
        let (file, _, offset) = v2_piece_location(metainfo, index)?;
        if !file.padding {
            storage
                .write(file.path.clone(), offset, piece, file.length)
                .await?;
            return Ok(HashSet::from([file.path.clone()]));
        }
        return Ok(HashSet::new());
    }
    let start = piece_start(metainfo, index)?;
    let segments = file_segments(wire_files(metainfo), start, piece.len())?;
    let mut consumed = 0_usize;
    let mut touched = HashSet::new();
    let mut writes = Vec::new();
    for segment in segments {
        let end = consumed
            .checked_add(segment.length)
            .ok_or(ActorError::Arithmetic)?;
        if !segment.file.padding {
            touched.insert(segment.file.path.clone());
            writes.push((
                segment.file.path.clone(),
                segment.file_offset,
                piece.slice(consumed..end),
                segment.file.length,
            ));
        }
        consumed = end;
    }
    if consumed != piece.len() {
        return Err(ActorError::Arithmetic);
    }
    stream::iter(writes)
        .map(|(path, offset, data, file_length)| async move {
            storage.write(path, offset, data, file_length).await
        })
        .buffer_unordered(STORAGE_IO_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(touched)
}

async fn sync_paths(
    paths: HashSet<TorrentPath>,
    storage: &StorageHandle,
) -> Result<(), ActorError> {
    stream::iter(paths)
        .map(|path| async move { storage.sync(path).await })
        .buffer_unordered(STORAGE_IO_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
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
    let mut segments = Vec::new();
    let first = files.partition_point(|file| {
        file.wire_offset
            .checked_add(file.length)
            .is_some_and(|file_end| file_end <= start)
    });
    for file in &files[first..] {
        let file_end = file
            .wire_offset
            .checked_add(file.length)
            .ok_or(ActorError::Arithmetic)?;
        let overlap_start = start.max(file.wire_offset);
        let overlap_end = end.min(file_end);
        if overlap_start < overlap_end {
            segments.push(FileSegment {
                file,
                file_offset: overlap_start - file.wire_offset,
                length: usize::try_from(overlap_end - overlap_start)
                    .map_err(|_| ActorError::Arithmetic)?,
            });
        }
        if file_end >= end {
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
    Ok(prepare_verification(metainfo, index, piece.len())?.verify(piece))
}

/// Everything needed to check one piece without the metainfo, so the hashing
/// itself can run on a blocking thread.
struct PieceVerification {
    v1: Option<Sha1Hash>,
    v2: Option<V2Check>,
}

struct V2Check {
    expected: dendrite_core::Sha256Hash,
    range: std::ops::Range<usize>,
    piece_length: u32,
}

impl PieceVerification {
    fn verify(&self, piece: &[u8]) -> bool {
        if let Some(expected) = self.v1 {
            // OpenSSL's assembly SHA-1 is about twice as fast as the pure
            // Rust implementation on CPUs without SHA extensions; piece
            // verification is the daemon's largest CPU cost.
            if Sha1Hash::from_bytes(openssl::sha::sha1(piece)) != expected {
                return false;
            }
        }
        if let Some(check) = &self.v2 {
            let Some(data) = piece.get(check.range.clone()) else {
                return false;
            };
            if v2_piece_root(data, check.piece_length) != check.expected {
                return false;
            }
        }
        true
    }
}

fn prepare_verification(
    metainfo: &Metainfo,
    index: usize,
    piece_len: usize,
) -> Result<PieceVerification, ActorError> {
    let v1 = metainfo.v1_piece_hashes.get(index).copied();
    let v2 = if metainfo.v2_info_hash.is_some() {
        let placeholder = vec![0_u8; piece_len];
        match v2_verification_target(metainfo, index, &placeholder)? {
            Some(target) => {
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
                        .ok_or_else(|| {
                            ActorError::Metainfo("v2 piece layer is incomplete".to_owned())
                        })?
                } else {
                    root
                };
                let start = target.data.as_ptr() as usize - placeholder.as_ptr() as usize;
                Some(V2Check {
                    expected,
                    range: start..start + target.data.len(),
                    piece_length: metainfo.piece_length.get(),
                })
            }
            None => None,
        }
    } else {
        None
    };
    Ok(PieceVerification { v1, v2 })
}

/// Runs a prepared verification on the blocking pool, bounded by the hash
/// concurrency limit so verification never occupies runtime workers.
async fn verify_piece_offloaded(
    services: &Services,
    verification: PieceVerification,
    data: Bytes,
) -> Result<bool, ActorError> {
    let _permit = services
        .hash_slots
        .acquire()
        .await
        .map_err(|_| ActorError::Cancelled)?;
    tokio::task::spawn_blocking(move || verification.verify(&data))
        .await
        .map_err(|error| ActorError::Peer(format!("piece verification task failed: {error}")))
}

fn hash_concurrency() -> usize {
    std::thread::available_parallelism().map_or(2, |cores| (cores.get() / 2).max(1))
}

/// The source addresses the kernel would use for public IPv4 and IPv6
/// traffic. Connecting a UDP socket sends nothing; it only resolves a route.
fn local_outbound_addresses() -> HashSet<IpAddr> {
    let mut addresses = HashSet::new();
    for (bind, probe) in [("0.0.0.0:0", "192.0.2.1:9"), ("[::]:0", "[2001:db8::1]:9")] {
        if let Ok(socket) = std::net::UdpSocket::bind(bind)
            && socket.connect(probe).is_ok()
            && let Ok(local) = socket.local_addr()
            && !local.ip().is_unspecified()
        {
            addresses.insert(local.ip());
        }
    }
    addresses
}

fn learn_self_address(services: &Services, address: IpAddr) {
    if address.is_unspecified() {
        return;
    }
    if let Ok(mut addresses) = services.self_addresses.write() {
        addresses.insert(address);
    }
}

/// A candidate that would reach this daemon's own peer port at one of its
/// own addresses.
fn is_self_candidate(services: &Services, address: SocketAddr) -> bool {
    let own_port = address.port() == services.peer_port
        || address.port() == services.advertised_peer_port.load(Ordering::Relaxed);
    own_port
        && services
            .self_addresses
            .read()
            .is_ok_and(|addresses| addresses.contains(&address.ip()))
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
        .map(|block| dendrite_core::Sha256Hash::from_bytes(openssl::sha::sha256(block)))
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

#[cfg(test)]
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
    persist_download_progress(services, record).await?;
    if !matches!(state, TorrentState::Downloading | TorrentState::Seeding) {
        services.incoming_content.lock().await.remove(&record.id);
    }
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
    persist_download_progress(services, &record).await?;
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

async fn persist_download_progress(
    services: &Services,
    record: &TorrentRecord,
) -> Result<(), ActorError> {
    if services
        .store
        .update_download_progress(
            record.id,
            record.state,
            record.completed_pieces.clone(),
            record.downloaded,
        )
        .await?
    {
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
    fn downloading_upload_slots_prefer_contributors_and_rotate_optimistic_peers() {
        let transfer = TransferPolicy {
            upload_slots: 8,
            optimistic_upload_slots: 2,
            ..TransferPolicy::default()
        };
        let mut candidates = (0_u64..12)
            .map(|session| UploadCandidate {
                session,
                contribution_score: 12 - session,
                verified_rate: 12 - session,
                recent_upload: session,
                eligible: true,
                interesting: true,
            })
            .collect::<Vec<_>>();
        let first =
            select_upload_sessions(TorrentState::Downloading, &mut candidates, 0, &transfer);
        for contributor in 0..8_u64 {
            assert!(first.contains(&contributor));
        }
        assert!(first.contains(&8));
        assert!(first.contains(&9));

        let second =
            select_upload_sessions(TorrentState::Downloading, &mut candidates, 2, &transfer);
        assert!(second.contains(&10));
        assert!(second.contains(&11));
        assert!(!second.contains(&8));
        assert!(!second.contains(&9));
    }

    #[test]
    fn downloading_upload_slots_skip_peers_that_cannot_repay() {
        let transfer = TransferPolicy {
            upload_slots: 4,
            optimistic_upload_slots: 2,
            ..TransferPolicy::default()
        };
        let mut candidates = vec![
            UploadCandidate {
                session: 1,
                contribution_score: 1 << 30,
                verified_rate: 1 << 20,
                recent_upload: 0,
                eligible: true,
                interesting: false,
            },
            UploadCandidate {
                session: 2,
                contribution_score: 0,
                verified_rate: 4096,
                recent_upload: 0,
                eligible: true,
                interesting: true,
            },
            UploadCandidate {
                session: 3,
                contribution_score: 0,
                verified_rate: 0,
                recent_upload: 0,
                eligible: true,
                interesting: true,
            },
        ];
        let selected =
            select_upload_sessions(TorrentState::Downloading, &mut candidates, 0, &transfer);
        assert!(
            !selected.contains(&1),
            "a peer with nothing we need never earns a slot while downloading"
        );
        assert!(
            selected.contains(&2),
            "a delivering peer holds a regular slot"
        );
        assert!(
            selected.contains(&3),
            "an interesting newcomer gets an optimistic slot"
        );
        let seeding = select_upload_sessions(TorrentState::Seeding, &mut candidates, 0, &transfer);
        assert!(seeding.contains(&1), "seeding ignores interest");
    }

    #[test]
    fn seeding_upload_slots_prefer_recent_rate_and_keep_optimistic_slots() {
        let transfer = TransferPolicy {
            upload_slots: 8,
            optimistic_upload_slots: 2,
            ..TransferPolicy::default()
        };
        let mut candidates = (0_u64..12)
            .map(|session| UploadCandidate {
                session,
                contribution_score: 0,
                verified_rate: 0,
                recent_upload: session * 1024,
                eligible: true,
                interesting: false,
            })
            .collect::<Vec<_>>();
        let selected = select_upload_sessions(TorrentState::Seeding, &mut candidates, 0, &transfer);
        assert_eq!(selected.len(), 10);
        for fast in 4_u64..12 {
            assert!(selected.contains(&fast));
        }
    }

    #[test]
    fn downloading_upload_credit_is_strictly_reciprocal_after_bootstrap() {
        let transfer = TransferPolicy::default();
        let balanced = PeerReputation {
            verified_from: 32 * 1024 * 1024,
            uploaded_to: 32 * 1024 * 1024,
            ..PeerReputation::default()
        };
        assert!(has_reciprocal_upload_credit(&transfer, balanced));
        assert_eq!(reciprocal_contribution(balanced), 0);

        let exhausted = PeerReputation {
            uploaded_to: balanced
                .verified_from
                .saturating_add(RECIPROCAL_BOOTSTRAP_BYTES),
            ..balanced
        };
        assert!(!has_reciprocal_upload_credit(&transfer, exhausted));
        let half = TransferPolicy {
            reciprocal_ratio: 0.5,
            ..transfer
        };
        assert!(
            !has_reciprocal_upload_credit(&half, balanced),
            "half ratio halves the credit"
        );
        let unlimited = TransferPolicy {
            reciprocal_ratio: 0.0,
            ..transfer
        };
        assert!(has_reciprocal_upload_credit(&unlimited, exhausted));

        let contributor = PeerReputation {
            verified_from: 64 * 1024 * 1024,
            uploaded_to: 16 * 1024 * 1024,
            ..PeerReputation::default()
        };
        assert_eq!(reciprocal_contribution(contributor), 48 * 1024 * 1024);
    }

    #[test]
    fn upload_limiter_delays_once_the_bucket_is_empty() {
        let mut limiter = UploadLimiter::default();
        assert_eq!(limiter.charge(0, 1 << 30), Duration::ZERO, "unlimited");
        assert_eq!(limiter.charge(1_000_000, 500_000), Duration::ZERO);
        assert_eq!(limiter.charge(1_000_000, 500_000), Duration::ZERO);
        let wait = limiter.charge(1_000_000, 250_000);
        assert!(
            wait >= Duration::from_millis(200) && wait <= Duration::from_millis(260),
            "{wait:?}"
        );
    }

    #[test]
    fn exhausted_peer_cannot_consume_regular_or_optimistic_upload_slot() {
        let mut candidates = [
            UploadCandidate {
                session: 1,
                contribution_score: u64::MAX,
                recent_upload: u64::MAX,
                verified_rate: 0,
                eligible: false,
                interesting: true,
            },
            UploadCandidate {
                session: 2,
                contribution_score: 1024,
                recent_upload: 0,
                verified_rate: 0,
                eligible: true,
                interesting: true,
            },
            UploadCandidate {
                session: 3,
                contribution_score: 0,
                recent_upload: 0,
                verified_rate: 0,
                eligible: true,
                interesting: true,
            },
        ];
        let transfer = TransferPolicy {
            upload_slots: 8,
            optimistic_upload_slots: 2,
            ..TransferPolicy::default()
        };
        let selected =
            select_upload_sessions(TorrentState::Downloading, &mut candidates, 0, &transfer);
        assert!(!selected.contains(&1));
        assert!(selected.contains(&2));
        assert!(selected.contains(&3));
    }

    #[test]
    fn file_segments_cross_boundaries_without_escape() -> Result<(), Box<dyn std::error::Error>> {
        let files = vec![
            FileEntry {
                path: TorrentPath::new(["root".to_owned(), "a".to_owned()])?,
                length: 3,
                pieces_root: None,
                padding: false,
                wire_offset: 0,
            },
            FileEntry {
                path: TorrentPath::new(["root".to_owned(), "b".to_owned()])?,
                length: 5,
                pieces_root: None,
                padding: false,
                wire_offset: 3,
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
        assert_eq!(outbound_connection_limit(1), 1);
        assert_eq!(outbound_connection_limit(2), 1);
        assert_eq!(outbound_connection_limit(1_000), 750);
        assert_eq!(outbound_connection_limit(10_000), 9_744);
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
            outbound_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            incoming_handshake_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            metadata_slots: Arc::new(Semaphore::new(METADATA_GLOBAL_CONCURRENCY)),
            per_torrent_peer_limit: per_torrent_peer_limit(INCOMING_PEER_LIMIT),
            lsd_cookie: "pex-test".to_owned(),
            encryption: EncryptionPolicy::Disabled,
            rendezvous: Arc::new(Mutex::new(HashMap::new())),
            incoming_content: Arc::new(Mutex::new(HashMap::new())),
            incoming_swarms: Arc::new(std::sync::Mutex::new(HashMap::new())),
            upload_policy: Arc::new(std::sync::Mutex::new(UploadPolicy::default())),
            connected_peers: Arc::new(AtomicUsize::new(0)),
            torrent_activity: Arc::new(std::sync::Mutex::new(HashMap::new())),
            payload_claims: Arc::new(std::sync::Mutex::new(HashMap::new())),
            download_budget: Arc::new(DownloadBudget::new(DEFAULT_DOWNLOAD_BUFFER_BYTES)),
            piece_cache_budget: Arc::new(CacheBudget::new(DEFAULT_PIECE_CACHE_BYTES)),
            hash_slots: Arc::new(Semaphore::new(hash_concurrency())),
            transfer: TransferPolicy::default(),
            piece_flush_interval: PIECE_FLUSH_INTERVAL,
            self_addresses: Arc::new(std::sync::RwLock::new(HashSet::new())),
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
    #[allow(clippy::too_many_lines)]
    async fn incoming_seed_is_promoted_into_the_active_download_swarm()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"an inbound seed can drive the entire verified download");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let stalled_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let stalled_address = stalled_listener.local_addr()?;
        let tracker_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let tracker_url = format!("http://{}/announce", tracker_listener.local_addr()?);
        let raw = single_file_metainfo(&tracker_url, "inbound.bin", &payload, digest);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let tracker = tokio::spawn(fake_tracker(tracker_listener, stalled_address));

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
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let incoming_address = listener.local_addr()?;
        engine.serve_incoming(listener);
        let mut events = engine.subscribe();
        engine.resume(id).await?;

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let registered = engine
                    .services
                    .incoming_swarms
                    .lock()
                    .is_ok_and(|routes| routes.contains_key(&id));
                if registered {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "incoming swarm route was not registered")?;

        let (request_seen, request_received) = oneshot::channel();
        let (release_piece, piece_released) = oneshot::channel();
        let inbound_payload = payload.clone();
        let inbound = tokio::spawn(async move {
            let mut request_seen = Some(request_seen);
            let mut piece_released = Some(piece_released);
            let mut peer = PeerConnection::connect(
                incoming_address,
                Handshake {
                    reserved: [0; 8],
                    info_hash,
                    peer_id: PeerId::from_bytes([0x71; 20]),
                },
                PeerCodecLimits::default(),
            )
            .await?;
            peer.send(PeerMessage::Bitfield(Bytes::from_static(&[0x80])))
                .await?;
            peer.send(PeerMessage::Unchoke).await?;
            loop {
                match peer.next_event().await {
                    Some(PeerEvent::Message(PeerMessage::Request(request))) => {
                        if let Some(request_seen) = request_seen.take() {
                            let _result_ignored = request_seen.send(());
                        }
                        if let Some(piece_released) = piece_released.take() {
                            piece_released.await?;
                        }
                        let begin = usize::try_from(request.begin)?;
                        let length = usize::try_from(request.length)?;
                        let block = inbound_payload.slice(begin..begin + length);
                        peer.send(PeerMessage::Piece {
                            piece: request.piece,
                            begin: request.begin,
                            block,
                        })
                        .await?;
                    }
                    Some(PeerEvent::Message(PeerMessage::Have(0))) => break,
                    Some(PeerEvent::Failed(error)) => return Err(error.into()),
                    Some(PeerEvent::Disconnected) | None => {
                        return Err("promoted inbound peer disconnected".into());
                    }
                    _ => {}
                }
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });

        tokio::time::timeout(Duration::from_secs(5), request_received)
            .await
            .map_err(|_| "promoted peer did not receive a piece request")??;
        assert_eq!(
            engine.torrent_peer_stats(id),
            TorrentPeerStats {
                total: 1,
                inbound: 1,
                outbound: 0,
                seeds: 1,
                active_downloaders: 1,
            }
        );
        let _result_ignored = release_piece.send(());

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
        .map_err(|_| "inbound seed download timed out")??;

        assert_eq!(
            tokio::fs::read(downloads.join("inbound.bin")).await?,
            payload
        );
        let updated = store.get_torrent(id).await?.ok_or("record disappeared")?;
        assert_eq!(updated.downloaded, updated.total_length);
        inbound.await.map_err(|error| error.to_string())??;
        tracker.await.map_err(|error| error.to_string())??;
        engine.shutdown().await?;
        drop(stalled_listener);
        Ok(())
    }

    #[tokio::test]
    async fn incoming_admission_drains_stalled_flood_and_recovers_without_restart()
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
                download_buffer_bytes: DEFAULT_DOWNLOAD_BUFFER_BYTES,
                piece_cache_bytes: DEFAULT_PIECE_CACHE_BYTES,
                transfer: TransferPolicy::default(),
                piece_flush_interval: PIECE_FLUSH_INTERVAL,
            },
        );
        engine.serve_incoming(listener);
        saturate_and_release_incoming_handshakes(&engine, address, info_hash).await?;

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
                download_buffer_bytes: DEFAULT_DOWNLOAD_BUFFER_BYTES,
                piece_cache_bytes: DEFAULT_PIECE_CACHE_BYTES,
                transfer: TransferPolicy::default(),
                piece_flush_interval: PIECE_FLUSH_INTERVAL,
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
            outbound_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            incoming_handshake_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            metadata_slots: Arc::new(Semaphore::new(METADATA_GLOBAL_CONCURRENCY)),
            per_torrent_peer_limit: per_torrent_peer_limit(INCOMING_PEER_LIMIT),
            lsd_cookie: "swarm-test".to_owned(),
            encryption: EncryptionPolicy::Disabled,
            rendezvous: Arc::new(Mutex::new(HashMap::new())),
            incoming_content: Arc::new(Mutex::new(HashMap::new())),
            incoming_swarms: Arc::new(std::sync::Mutex::new(HashMap::new())),
            upload_policy: Arc::new(std::sync::Mutex::new(UploadPolicy::default())),
            connected_peers: Arc::new(AtomicUsize::new(0)),
            torrent_activity: Arc::new(std::sync::Mutex::new(HashMap::new())),
            payload_claims: Arc::new(std::sync::Mutex::new(HashMap::new())),
            download_budget: Arc::new(DownloadBudget::new(DEFAULT_DOWNLOAD_BUFFER_BYTES)),
            piece_cache_budget: Arc::new(CacheBudget::new(DEFAULT_PIECE_CACHE_BYTES)),
            hash_slots: Arc::new(Semaphore::new(hash_concurrency())),
            transfer: TransferPolicy::default(),
            piece_flush_interval: PIECE_FLUSH_INTERVAL,
            self_addresses: Arc::new(std::sync::RwLock::new(HashSet::new())),
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
    async fn healthy_tracker_streams_peers_without_waiting_for_stalled_tracker()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let stalled_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let healthy_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let stalled_url = format!("http://{}/announce", stalled_listener.local_addr()?);
        let healthy_url = format!("http://{}/announce", healthy_listener.local_addr()?);
        let expected_peer = SocketAddr::from(([127, 0, 0, 1], 61_003));
        let stalled_task = tokio::spawn(fake_stalled_tracker(stalled_listener));
        let healthy_task = tokio::spawn(fake_tracker(healthy_listener, expected_peer));
        let payload = Bytes::from_static(b"stream the first healthy tracker result");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw = single_file_metainfo(&healthy_url, "stream.bin", &payload, digest);
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
            "tracker-stream-test",
        );
        let mut receiver = start_peer_discovery(
            &services,
            DiscoveryQuery {
                trackers: &[vec![stalled_url], vec![healthy_url]],
                record: &record,
                info_hash,
                left: metainfo.total_length,
                allow_dht: false,
                dht_announce: false,
                announce_event: AnnounceEvent::Started,
                cancellation: CancellationToken::new(),
            },
        )?;

        let event = loop {
            let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
                .await?
                .ok_or("discovery channel closed before the healthy result")?;
            if !matches!(event, DiscoveryEvent::TrackerInterval { .. }) {
                break event;
            }
        };
        assert!(matches!(
            event,
            DiscoveryEvent::Peers(peers) if peers == vec![expected_peer]
        ));
        drop(receiver);
        stalled_task.abort();
        healthy_task.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn failed_worker_is_replaced_from_retained_candidate_queue()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"retained candidate completed the transfer");
        let digest: [u8; 20] = Sha1::digest(&payload).into();
        let raw = single_file_metainfo("http://127.0.0.1/announce", "queue.bin", &payload, digest);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let unavailable_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let unavailable_address = unavailable_listener.local_addr()?;
        drop(unavailable_listener);
        let healthy_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let healthy_address = healthy_listener.local_addr()?;
        let healthy_peer = tokio::spawn(fake_peer(healthy_listener, info_hash, payload.clone()));
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 8)?;
        let mut services = test_services(
            store.clone(),
            StorageHandle::start_portable(&downloads, 8)?,
            "candidate-queue-test",
        );
        services.per_torrent_peer_limit = 1;
        let mut record = test_record(&metainfo, raw);
        normalize_completion(&mut record, 1);
        store.put_torrent(record.clone()).await?;

        run_peer_swarm(
            vec![unavailable_address, healthy_address],
            info_hash,
            &metainfo,
            &mut record,
            &services,
            &CancellationToken::new(),
        )
        .await?;

        assert_eq!(tokio::fs::read(downloads.join("queue.bin")).await?, payload);
        healthy_peer.await.map_err(|error| error.to_string())??;
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
            outbound_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            incoming_handshake_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            metadata_slots: Arc::new(Semaphore::new(METADATA_GLOBAL_CONCURRENCY)),
            per_torrent_peer_limit: per_torrent_peer_limit(INCOMING_PEER_LIMIT),
            lsd_cookie: "lsd-downloader-test".to_owned(),
            encryption: EncryptionPolicy::Disabled,
            rendezvous: Arc::new(Mutex::new(HashMap::new())),
            incoming_content: Arc::new(Mutex::new(HashMap::new())),
            incoming_swarms: Arc::new(std::sync::Mutex::new(HashMap::new())),
            upload_policy: Arc::new(std::sync::Mutex::new(UploadPolicy::default())),
            connected_peers: Arc::new(AtomicUsize::new(0)),
            torrent_activity: Arc::new(std::sync::Mutex::new(HashMap::new())),
            payload_claims: Arc::new(std::sync::Mutex::new(HashMap::new())),
            download_budget: Arc::new(DownloadBudget::new(DEFAULT_DOWNLOAD_BUFFER_BYTES)),
            piece_cache_budget: Arc::new(CacheBudget::new(DEFAULT_PIECE_CACHE_BYTES)),
            hash_slots: Arc::new(Semaphore::new(hash_concurrency())),
            transfer: TransferPolicy::default(),
            piece_flush_interval: PIECE_FLUSH_INTERVAL,
            self_addresses: Arc::new(std::sync::RwLock::new(HashSet::new())),
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
            outbound_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            incoming_handshake_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            metadata_slots: Arc::new(Semaphore::new(METADATA_GLOBAL_CONCURRENCY)),
            per_torrent_peer_limit: per_torrent_peer_limit(INCOMING_PEER_LIMIT),
            lsd_cookie: "mse-out-test".to_owned(),
            encryption: EncryptionPolicy::Required,
            rendezvous: Arc::new(Mutex::new(HashMap::new())),
            incoming_content: Arc::new(Mutex::new(HashMap::new())),
            incoming_swarms: Arc::new(std::sync::Mutex::new(HashMap::new())),
            upload_policy: Arc::new(std::sync::Mutex::new(UploadPolicy::default())),
            connected_peers: Arc::new(AtomicUsize::new(0)),
            torrent_activity: Arc::new(std::sync::Mutex::new(HashMap::new())),
            payload_claims: Arc::new(std::sync::Mutex::new(HashMap::new())),
            download_budget: Arc::new(DownloadBudget::new(DEFAULT_DOWNLOAD_BUFFER_BYTES)),
            piece_cache_budget: Arc::new(CacheBudget::new(DEFAULT_PIECE_CACHE_BYTES)),
            hash_slots: Arc::new(Semaphore::new(hash_concurrency())),
            transfer: TransferPolicy::default(),
            piece_flush_interval: PIECE_FLUSH_INTERVAL,
            self_addresses: Arc::new(std::sync::RwLock::new(HashSet::new())),
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
                download_buffer_bytes: DEFAULT_DOWNLOAD_BUFFER_BYTES,
                piece_cache_bytes: DEFAULT_PIECE_CACHE_BYTES,
                transfer: TransferPolicy::default(),
                piece_flush_interval: PIECE_FLUSH_INTERVAL,
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
    async fn worker_requests_across_piece_boundaries_without_draining()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        const PIECES: usize = 6;
        const BLOCKS_PER_PIECE: usize = 4;
        let pieces: Vec<Bytes> = (0..PIECES)
            .map(|index| {
                Bytes::from(vec![
                    0x40 + u8::try_from(index).unwrap_or(0);
                    BLOCK_BYTES * BLOCKS_PER_PIECE
                ])
            })
            .collect();
        let raw = multi_piece_v1_metainfo_with_piece_length(
            "spanning.bin",
            &pieces,
            BLOCK_BYTES * BLOCKS_PER_PIECE,
        );
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let payload: Vec<Bytes> = pieces.clone();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut peer = test_peer_connection(stream, info_hash).await?;
            wait_for_interest(&mut peer).await?;
            peer.send(PeerMessage::Bitfield(Bytes::from_static(&[0b1111_1100])))
                .await?;
            peer.send(PeerMessage::Unchoke).await?;
            // Every block of every piece must be requested before a single
            // block is served: the pipeline spans piece boundaries.
            let mut requests = Vec::new();
            while requests.len() < PIECES * BLOCKS_PER_PIECE {
                match peer.next_event().await {
                    Some(PeerEvent::Message(PeerMessage::Request(request))) => {
                        requests.push(request);
                    }
                    Some(PeerEvent::Failed(error)) => return Err(error.into()),
                    Some(PeerEvent::Disconnected) | None => {
                        return Err::<(), Box<dyn std::error::Error + Send + Sync>>(
                            "client disconnected before filling the pipeline".into(),
                        );
                    }
                    _ => {}
                }
            }
            let distinct: HashSet<u32> = requests
                .iter()
                .take(BLOCKS_PER_PIECE + 1)
                .map(|request| request.piece)
                .collect();
            assert!(
                distinct.len() >= 2,
                "requests did not cross a piece boundary: {distinct:?}"
            );
            for request in requests {
                let piece = usize::try_from(request.piece)?;
                let start = usize::try_from(request.begin)?;
                let end = start + usize::try_from(request.length)?;
                let block = payload
                    .get(piece)
                    .and_then(|data| data.get(start..end))
                    .ok_or("invalid block request")?;
                peer.send(PeerMessage::Piece {
                    piece: request.piece,
                    begin: request.begin,
                    block: Bytes::copy_from_slice(block),
                })
                .await?;
            }
            Ok(())
        });
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start(&downloads, 32)?;
        let services = test_services(store.clone(), storage, "spanning-test");
        let mut record = test_record(&metainfo, raw);
        normalize_completion(&mut record, piece_count(&metainfo)?);
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
        let written = tokio::fs::read(downloads.join("spanning.bin")).await?;
        let expected: Vec<u8> = pieces.iter().flat_map(|piece| piece.to_vec()).collect();
        assert_eq!(written, expected);
        peer.await.map_err(|error| error.to_string())??;
        assert_eq!(services.download_budget.used(), 0, "budget leaked");
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // Two scheduler scenarios share one swarm fixture.
    async fn schedule_pieces_honours_budget_targets_and_generation_skips()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        const PIECES: usize = 8;
        let pieces: Vec<Bytes> = (0..PIECES)
            .map(|index| Bytes::from(vec![u8::try_from(index).unwrap_or(0); BLOCK_BYTES]))
            .collect();
        let raw = multi_piece_v1_metainfo_with_piece_length("budget.bin", &pieces, BLOCK_BYTES);
        let metainfo = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = wire_info_hash(&metainfo)?;
        let directory = tempfile::tempdir()?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start(directory.path(), 32)?;
        let mut record = test_record(&metainfo, raw);
        normalize_completion(&mut record, PIECES);
        let cancellation = CancellationToken::new();
        let add_worker = |swarm: &mut SwarmState,
                          worker: usize,
                          bitfield: Vec<u8>,
                          target_bytes: usize|
         -> Result<
            mpsc::Receiver<PeerWorkerCommand>,
            dendrite_core::BitfieldError,
        > {
            let (commands, receiver) = mpsc::channel(PEER_COMMAND_CAPACITY);
            swarm.workers.insert(
                worker,
                PeerWorkerHandle {
                    commands,
                    bitfield: Some(bitfield.clone()),
                    idle: true,
                    choked: false,
                    address: SocketAddr::from((Ipv4Addr::LOCALHOST, 6881)),
                    peer_key: None,
                    seed: false,
                    useful_pieces: useful_piece_count(&bitfield, &record.completed_pieces, PIECES),
                    verified_bytes: 0,
                    connected_at: Instant::now(),
                    last_verified: None,
                    cancellation: cancellation.child_token(),
                    assigned_bytes: 0,
                    target_bytes,
                    wants_more: false,
                    skip_generation: None,
                },
            );
            swarm.picker.add_peer_bitfield(&bitfield)?;
            Ok(receiver)
        };
        let drain = |receiver: &mut mpsc::Receiver<PeerWorkerCommand>| {
            let mut granted = Vec::new();
            while let Ok(PeerWorkerCommand::Download { piece, .. }) = receiver.try_recv() {
                granted.push(piece);
            }
            granted
        };

        // Global budget of three pieces: one worker holding everything is cut
        // off after three assignments and the budget is returned on release.
        let mut services = test_services(store.clone(), storage.clone(), "budget-test");
        services.download_budget = Arc::new(DownloadBudget::new(3 * BLOCK_BYTES));
        let (mut swarm, _events) = initialize_swarm(
            Vec::new(),
            info_hash,
            &metainfo,
            &record,
            &services,
            &cancellation,
        )?;
        let mut all = add_worker(&mut swarm, 0, vec![0b1111_1111], ASSIGNMENT_TARGET_MIN)?;
        assert_eq!(schedule_pieces(&mut swarm, &metainfo)?, 3);
        assert_eq!(drain(&mut all), vec![0, 1, 2]);
        assert_eq!(services.download_budget.used(), 3 * BLOCK_BYTES as u64);
        assert_eq!(swarm.workers[&0].assigned_bytes, 3 * BLOCK_BYTES);
        assert!(!swarm.workers[&0].idle);
        assert_eq!(
            schedule_pieces(&mut swarm, &metainfo)?,
            0,
            "budget exhausted"
        );
        release_worker_assignments(&mut swarm, 0)?;
        assert_eq!(services.download_budget.used(), 0);
        assert!(swarm.workers[&0].idle);
        assert!(swarm.schedule_dirty);
        drop(swarm);

        // Two workers sharing a single piece: the second has nothing
        // selectable and is skipped until the generation changes, which
        // happens when the first worker leaves and returns the piece.
        let services = test_services(store, storage, "skip-test");
        let (mut swarm, _events) = initialize_swarm(
            Vec::new(),
            info_hash,
            &metainfo,
            &record,
            &services,
            &cancellation,
        )?;
        let mut first = add_worker(&mut swarm, 0, vec![0b1000_0000], ASSIGNMENT_TARGET_MIN)?;
        let mut second = add_worker(&mut swarm, 1, vec![0b1000_0000], ASSIGNMENT_TARGET_MIN)?;
        assert_eq!(schedule_pieces(&mut swarm, &metainfo)?, 1);
        assert_eq!(drain(&mut first), vec![0]);
        assert!(drain(&mut second).is_empty());
        let generation = swarm.picker.generation();
        assert_eq!(swarm.workers[&1].skip_generation, Some(generation));
        assert_eq!(schedule_pieces(&mut swarm, &metainfo)?, 0);
        assert!(drain(&mut second).is_empty());
        remove_worker(&mut swarm, 0)?;
        assert_ne!(swarm.picker.generation(), generation);
        assert_eq!(schedule_pieces(&mut swarm, &metainfo)?, 1);
        assert_eq!(drain(&mut second), vec![0]);
        assert_eq!(services.download_budget.used(), BLOCK_BYTES as u64);
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
            outbound_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            incoming_handshake_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            metadata_slots: Arc::new(Semaphore::new(METADATA_GLOBAL_CONCURRENCY)),
            per_torrent_peer_limit: per_torrent_peer_limit(INCOMING_PEER_LIMIT),
            lsd_cookie: "endgame-test".to_owned(),
            encryption: EncryptionPolicy::Disabled,
            rendezvous: Arc::new(Mutex::new(HashMap::new())),
            incoming_content: Arc::new(Mutex::new(HashMap::new())),
            incoming_swarms: Arc::new(std::sync::Mutex::new(HashMap::new())),
            upload_policy: Arc::new(std::sync::Mutex::new(UploadPolicy::default())),
            connected_peers: Arc::new(AtomicUsize::new(0)),
            torrent_activity: Arc::new(std::sync::Mutex::new(HashMap::new())),
            payload_claims: Arc::new(std::sync::Mutex::new(HashMap::new())),
            download_budget: Arc::new(DownloadBudget::new(DEFAULT_DOWNLOAD_BUFFER_BYTES)),
            piece_cache_budget: Arc::new(CacheBudget::new(DEFAULT_PIECE_CACHE_BYTES)),
            hash_slots: Arc::new(Semaphore::new(hash_concurrency())),
            transfer: TransferPolicy::default(),
            piece_flush_interval: PIECE_FLUSH_INTERVAL,
            self_addresses: Arc::new(std::sync::RwLock::new(HashSet::new())),
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
            outbound_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            incoming_handshake_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            metadata_slots: Arc::new(Semaphore::new(METADATA_GLOBAL_CONCURRENCY)),
            per_torrent_peer_limit: per_torrent_peer_limit(INCOMING_PEER_LIMIT),
            lsd_cookie: "scheduler-test".to_owned(),
            encryption: EncryptionPolicy::Disabled,
            rendezvous: Arc::new(Mutex::new(HashMap::new())),
            incoming_content: Arc::new(Mutex::new(HashMap::new())),
            incoming_swarms: Arc::new(std::sync::Mutex::new(HashMap::new())),
            upload_policy: Arc::new(std::sync::Mutex::new(UploadPolicy::default())),
            connected_peers: Arc::new(AtomicUsize::new(0)),
            torrent_activity: Arc::new(std::sync::Mutex::new(HashMap::new())),
            payload_claims: Arc::new(std::sync::Mutex::new(HashMap::new())),
            download_budget: Arc::new(DownloadBudget::new(DEFAULT_DOWNLOAD_BUFFER_BYTES)),
            piece_cache_budget: Arc::new(CacheBudget::new(DEFAULT_PIECE_CACHE_BYTES)),
            hash_slots: Arc::new(Semaphore::new(hash_concurrency())),
            transfer: TransferPolicy::default(),
            piece_flush_interval: PIECE_FLUSH_INTERVAL,
            self_addresses: Arc::new(std::sync::RwLock::new(HashSet::new())),
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
    async fn supervised_actor_honors_stop_on_complete_enabled_during_download()
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
        let peer_task = tokio::spawn(fake_superseed_peer_with_delay(
            peer_listener,
            info_hash,
            payload.clone(),
            Duration::from_millis(250),
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
                stop_on_complete: false,
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
                if event.state == TorrentState::Downloading {
                    return Ok(());
                }
            }
        })
        .await
        .map_err(|_| "torrent actor did not start downloading")??;
        let mut live_record = store.get_torrent(id).await?.ok_or("record disappeared")?;
        live_record.stop_on_complete = true;
        assert!(store.replace_torrent(live_record).await?);

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = events.recv().await.map_err(|error| error.to_string())?;
                if event.state == TorrentState::Error {
                    return Err(event
                        .detail
                        .unwrap_or_else(|| "unknown actor error".to_owned()));
                }
                if event.state == TorrentState::Stopped {
                    return Ok(());
                }
            }
        })
        .await
        .map_err(|_| "torrent actor timed out")??;

        let written = tokio::fs::read(directory.path().join("downloads/payload.bin")).await?;
        assert_eq!(written, payload);
        let record = store.get_torrent(id).await?.ok_or("record disappeared")?;
        assert_eq!(record.state, TorrentState::Stopped);
        assert!(record.stop_on_complete);
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
            first.clone(),
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
        services.peer_message_timeout = Duration::from_millis(100);

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
            [(first_address, true), (second_address, false)],
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
                stop_on_complete: false,
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
            if let Ok(partial) = tokio::fs::read(downloads.join("root/a")).await {
                assert_eq!(partial, first);
            }
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
                stop_on_complete: false,
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
    async fn magnet_metadata_reannounces_after_exhausting_a_bad_peer_round()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = Bytes::from_static(b"metadata retry eventually downloads this payload");
        let piece_digest: [u8; 20] = Sha1::digest(&payload).into();
        let bad_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let bad_address = bad_listener.local_addr()?;
        let healthy_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let healthy_address = healthy_listener.local_addr()?;
        let tracker_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let tracker_url = format!("http://{}/announce", tracker_listener.local_addr()?);
        let full_metainfo = single_file_metainfo(&tracker_url, "retry.bin", &payload, piece_digest);
        let metainfo = Metainfo::parse(&full_metainfo, BencodeLimits::default())?;
        let info_hash = metainfo.v1_info_hash.ok_or("missing v1 info hash")?;
        let info = Bytes::copy_from_slice(metainfo_info_bytes(&full_metainfo)?);
        let mut magnet = Url::parse("magnet:?")?;
        magnet
            .query_pairs_mut()
            .append_pair("xt", &format!("urn:btih:{info_hash}"))
            .append_pair("dn", "retry.bin")
            .append_pair("tr", &tracker_url);

        let tracker = tokio::spawn(fake_tracker_sequence(
            tracker_listener,
            [
                (bad_address, true),
                (healthy_address, false),
                (healthy_address, true),
            ],
        ));
        let bad_peer = tokio::spawn(fake_metadata_handshake_disconnect_peer(
            bad_listener,
            info_hash,
        ));
        let healthy_peer = tokio::spawn(fake_metadata_then_payload_peer(
            healthy_listener,
            info_hash,
            info,
            payload.clone(),
        ));
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start_portable(&downloads, 32)?;
        let metainfo_limit = 64 * 1024;
        let id = TorrentId::new();
        store
            .put_torrent(TorrentRecord {
                record_version: TorrentRecord::RECORD_VERSION,
                id,
                name: "retry.bin".to_owned(),
                state: TorrentState::Starting,
                v1_info_hash: Some(info_hash),
                v2_info_hash: None,
                total_length: 0,
                raw_metainfo: Vec::new(),
                magnet_uri: Some(magnet.to_string()),
                stop_on_complete: false,
                completed_pieces: Vec::new(),
                downloaded: 0,
                uploaded: 0,
                added_at_unix_ms: 0,
            })
            .await?;
        let engine = EngineHandle::start(
            store.clone(),
            storage,
            metainfo_limit,
            metainfo_limit,
            Vec::new(),
            None,
            6881,
        );
        let mut events = engine.subscribe();
        engine.resume(id).await?;
        tokio::time::timeout(Duration::from_secs(10), wait_for_seeding(&mut events))
            .await
            .map_err(|_| "metadata retry did not reach seeding")??;
        assert_eq!(tokio::fs::read(downloads.join("retry.bin")).await?, payload);
        tracker.await.map_err(|error| error.to_string())??;
        bad_peer.await.map_err(|error| error.to_string())??;
        healthy_peer.await.map_err(|error| error.to_string())??;
        engine.shutdown().await?;
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
                fetch_metadata(
                    TorrentId::new(),
                    address,
                    info_hash,
                    &services,
                    &CancellationToken::new(),
                ),
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
    async fn metadata_fetches_share_a_bounded_global_admission_pool()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let directory = tempfile::tempdir()?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 4)?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let storage = StorageHandle::start_portable(&downloads, 4)?;
        let services = test_services(store, storage, "metadata-admission");
        let occupied = services
            .metadata_slots
            .clone()
            .acquire_many_owned(u32::try_from(METADATA_GLOBAL_CONCURRENCY)?)
            .await?;
        let cancellation = CancellationToken::new();
        assert!(
            tokio::time::timeout(
                Duration::from_millis(25),
                acquire_metadata_slot(&services, &cancellation),
            )
            .await
            .is_err()
        );
        drop(occupied);
        let _permit = tokio::time::timeout(
            Duration::from_secs(1),
            acquire_metadata_slot(&services, &cancellation),
        )
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn metadata_fetch_pipeline_accepts_out_of_order_blocks()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let info_hash = Sha1Hash::from_bytes([42; 20]);
        let metadata = Bytes::from(
            (0..METADATA_BLOCK_BYTES * 2 + 7)
                .map(|index| u8::try_from(index % 251))
                .collect::<Result<Vec<_>, _>>()?,
        );
        let peer = tokio::spawn(fake_pipelined_metadata_peer(
            listener,
            info_hash,
            metadata.clone(),
        ));
        let directory = tempfile::tempdir()?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 4)?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let storage = StorageHandle::start_portable(&downloads, 4)?;
        let mut services = test_services(store, storage, "metadata-pipeline");
        services.metainfo_limit = metadata.len();
        let (received, connection, _slot, _guard) = tokio::time::timeout(
            Duration::from_secs(2),
            fetch_metadata(
                TorrentId::new(),
                address,
                info_hash,
                &services,
                &CancellationToken::new(),
            ),
        )
        .await??;
        connection.shutdown();
        assert_eq!(received, metadata);
        peer.await.map_err(|error| error.to_string())??;
        Ok(())
    }

    #[tokio::test]
    async fn large_v1_magnet_does_not_require_v2_piece_layers()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload_pieces = [
            Bytes::from(vec![1; BLOCK_BYTES * BLOCK_PIPELINE]),
            Bytes::from(vec![2; BLOCK_BYTES * BLOCK_PIPELINE]),
        ];
        let raw = multi_piece_v1_metainfo("large-v1.bin", &payload_pieces);
        let parsed = Metainfo::parse(&raw, BencodeLimits::default())?;
        let info_hash = parsed.v1_info_hash.ok_or("missing v1 info hash")?;
        let info = Bytes::copy_from_slice(metainfo_info_bytes(&raw)?);
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let peer = tokio::spawn(fake_metadata_only_peer(listener, info_hash, info));
        let directory = tempfile::tempdir()?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 4)?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let storage = StorageHandle::start_portable(&downloads, 4)?;
        let services = test_services(store, storage, "large-v1-magnet");
        let magnet = Magnet {
            v1_info_hash: Some(info_hash),
            v2_info_hash: None,
            display_name: Some("large-v1.bin".to_owned()),
            trackers: Vec::new(),
            web_seeds: Vec::new(),
        };
        let (_, acquired) = tokio::time::timeout(
            Duration::from_secs(2),
            fetch_and_validate_metadata(
                TorrentId::new(),
                address,
                info_hash,
                &magnet,
                &services,
                &CancellationToken::new(),
            ),
        )
        .await??;
        assert_eq!(acquired.v1_info_hash, Some(info_hash));
        assert_eq!(acquired.v2_info_hash, None);
        assert!(acquired.total_length > u64::from(acquired.piece_length.get()));
        peer.await.map_err(|error| error.to_string())??;
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
            stop_on_complete: false,
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
                stop_on_complete: false,
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
        multi_piece_v1_metainfo_with_piece_length(name, pieces, BLOCK_BYTES * BLOCK_PIPELINE)
    }

    fn multi_piece_v1_metainfo_with_piece_length(
        name: &str,
        pieces: &[Bytes],
        piece_length: usize,
    ) -> Vec<u8> {
        let length: usize = pieces.iter().map(Bytes::len).sum();
        let mut info = format!(
            "d6:lengthi{length}e4:name{}:{name}12:piece lengthi{}e6:pieces{}:",
            name.len(),
            piece_length,
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
            stop_on_complete: false,
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
            outbound_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            incoming_handshake_slots: Arc::new(Semaphore::new(INCOMING_PEER_LIMIT)),
            metadata_slots: Arc::new(Semaphore::new(METADATA_GLOBAL_CONCURRENCY)),
            per_torrent_peer_limit: per_torrent_peer_limit(INCOMING_PEER_LIMIT),
            lsd_cookie: cookie.to_owned(),
            encryption: EncryptionPolicy::Disabled,
            rendezvous: Arc::new(Mutex::new(HashMap::new())),
            incoming_content: Arc::new(Mutex::new(HashMap::new())),
            incoming_swarms: Arc::new(std::sync::Mutex::new(HashMap::new())),
            upload_policy: Arc::new(std::sync::Mutex::new(UploadPolicy::default())),
            connected_peers: Arc::new(AtomicUsize::new(0)),
            torrent_activity: Arc::new(std::sync::Mutex::new(HashMap::new())),
            payload_claims: Arc::new(std::sync::Mutex::new(HashMap::new())),
            download_budget: Arc::new(DownloadBudget::new(DEFAULT_DOWNLOAD_BUFFER_BYTES)),
            piece_cache_budget: Arc::new(CacheBudget::new(DEFAULT_PIECE_CACHE_BYTES)),
            hash_slots: Arc::new(Semaphore::new(hash_concurrency())),
            transfer: TransferPolicy::default(),
            piece_flush_interval: PIECE_FLUSH_INTERVAL,
            self_addresses: Arc::new(std::sync::RwLock::new(HashSet::new())),
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

    #[test]
    fn download_budget_bounds_reservations_but_never_starves_an_empty_account() {
        let shared = Arc::new(DownloadBudget::new(100));
        let mut first = BudgetAccount::new(shared.clone());
        let mut second = BudgetAccount::new(shared.clone());
        assert!(first.reserve(80));
        assert!(!first.reserve(30), "second reservation exceeds the limit");
        assert!(
            second.reserve(30),
            "an empty account may always take one piece"
        );
        assert_eq!(shared.used(), 110);
        assert!(!second.reserve(1));
        first.release(80);
        assert_eq!(shared.used(), 30);
        assert!(second.reserve(60));
        assert_eq!(shared.used(), 90);
        drop(second);
        assert_eq!(shared.used(), 0, "dropping an account returns what it held");
        drop(first);
        assert_eq!(shared.used(), 0);
    }

    #[test]
    fn assignment_target_and_pipeline_scale_with_rate() {
        assert_eq!(assignment_target(0), ASSIGNMENT_TARGET_MIN);
        assert_eq!(assignment_target(8 * 1024 * 1024), 8 * 1024 * 1024);
        assert_eq!(assignment_target(u64::MAX), ASSIGNMENT_TARGET_MAX);
        assert_eq!(pipeline_limit(0), BLOCK_PIPELINE);
        assert_eq!(
            pipeline_limit(2 * 1024 * 1024),
            2 * 1024 * 1024 * 3 / BLOCK_BYTES
        );
        assert_eq!(pipeline_limit(u64::MAX), BLOCK_PIPELINE_MAX);
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

    async fn fake_stalled_tracker(
        listener: TcpListener,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (_stream, _) = listener.accept().await?;
        std::future::pending().await
    }

    async fn fake_tracker_sequence<const N: usize>(
        listener: TcpListener,
        peers: [(SocketAddr, bool); N],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (peer, expected_started) in peers {
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
            if request.contains("event=started") != expected_started {
                return Err("tracker announce carried the wrong lifecycle event".into());
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

    async fn fake_metadata_handshake_disconnect_peer(
        listener: TcpListener,
        info_hash: Sha1Hash,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream, _) = listener.accept().await?;
        let mut peer = test_peer_connection(stream, info_hash).await?;
        loop {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Extended {
                    extension_id: 0, ..
                })) => return Ok(()),
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
    }

    async fn fake_pipelined_metadata_peer(
        listener: TcpListener,
        info_hash: Sha1Hash,
        metadata: Bytes,
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
            payload: encode_extension_handshake(Some(metadata.len())),
        })
        .await?;
        let piece_count = metadata.len().div_ceil(METADATA_BLOCK_BYTES);
        let mut requested = Vec::with_capacity(piece_count);
        while requested.len() < piece_count {
            match peer.next_event().await {
                Some(PeerEvent::Message(PeerMessage::Extended {
                    extension_id: LOCAL_METADATA_EXTENSION_ID,
                    payload,
                })) => {
                    let MetadataMessage::Request { piece } =
                        decode_metadata_message(&payload, metadata.len())?
                    else {
                        return Err("client sent unexpected metadata message".into());
                    };
                    requested.push(usize::try_from(piece)?);
                }
                Some(PeerEvent::Failed(error)) => return Err(error.into()),
                Some(PeerEvent::Disconnected) | None => return Err("peer disconnected".into()),
                _ => {}
            }
        }
        requested.sort_unstable();
        if requested != (0..piece_count).collect::<Vec<_>>() {
            return Err("client did not pipeline each metadata request once".into());
        }
        for piece in requested.into_iter().rev() {
            let start = piece * METADATA_BLOCK_BYTES;
            let end = metadata.len().min(start + METADATA_BLOCK_BYTES);
            peer.send(PeerMessage::Extended {
                extension_id: LOCAL_METADATA_EXTENSION_ID,
                payload: encode_metadata_data(
                    u32::try_from(piece)?,
                    metadata.len(),
                    &metadata[start..end],
                ),
            })
            .await?;
        }
        Ok(())
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

    async fn saturate_and_release_incoming_handshakes(
        engine: &EngineHandle,
        address: SocketAddr,
        info_hash: Sha1Hash,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let stalled = vec![
            tokio::net::TcpStream::connect(address).await?,
            tokio::net::TcpStream::connect(address).await?,
        ];
        tokio::time::timeout(Duration::from_secs(2), async {
            while engine.services.incoming_handshake_slots.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "stalled handshakes did not fill bounded admission")?;
        let rejected = tokio::time::timeout(
            Duration::from_secs(2),
            PeerConnection::connect(
                address,
                Handshake {
                    reserved: [0; 8],
                    info_hash,
                    peer_id: PeerId::from_bytes([u8::MAX; 20]),
                },
                PeerCodecLimits::default(),
            ),
        )
        .await
        .map_err(|_| "overflow connection was left queued instead of drained")?;
        assert!(rejected.is_err());
        drop(stalled);
        tokio::time::timeout(Duration::from_secs(2), async {
            while engine.services.incoming_handshake_slots.available_permits() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "handshake admission did not recover after stalled peers left")?;
        Ok(())
    }

    async fn fake_superseed_peer_with_delay(
        listener: TcpListener,
        info_hash: Sha1Hash,
        payload: Bytes,
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
        tokio::time::sleep(delay).await;
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
        known_piece: Bytes,
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
        peer.send(PeerMessage::Interested).await?;
        wait_for_unchoke(&mut peer).await?;
        peer.send(PeerMessage::Request(BlockRequest {
            piece: 0,
            begin: 0,
            length: u32::try_from(BLOCK_BYTES)?,
        }))
        .await?;
        if wait_for_piece(&mut peer).await? != known_piece.slice(..BLOCK_BYTES) {
            return Err("outgoing download session did not reciprocate verified data".into());
        }
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
