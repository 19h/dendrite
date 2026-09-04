use std::{
    collections::HashMap,
    future::Future,
    io::Write as _,
    path::{Path, PathBuf},
    str::FromStr as _,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    body::Body,
    extract::{
        DefaultBodyLimit, Multipart, Path as AxumPath, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use dendrite_api_types::{
    API_VERSION, AddTorrentOptions, AddTorrentRequest, BrowserSessionResponse, EventEnvelope,
    ListResponse, Problem, StatusResponse, TokenRotationResponse, TorrentAction,
    TorrentActionRequest, TorrentSettingsUpdate, TorrentSummary,
};
use dendrite_config::{PeerEncryption, Settings};
use dendrite_core::{TorrentId, TorrentState};
use dendrite_engine::{EngineHandle, EngineOptions, TransferPolicy};
use dendrite_metainfo::{BencodeLimits, Magnet, Metainfo};
use dendrite_net::{
    dht::DhtClient,
    nat::{MappingProtocol, NatPmpClient},
    peer::EncryptionPolicy,
    utp::UtpEndpoint,
};
use dendrite_persistence::{StateStoreHandle, StoreError, TorrentRecord};
use dendrite_storage::{BackendKind, StorageHandle};
use futures_util::SinkExt as _;
use subtle::ConstantTimeEq as _;
use thiserror::Error;
use tokio::{
    net::TcpListener,
    sync::{Mutex, RwLock, Semaphore, broadcast},
};
use tower_http::{catch_panic::CatchPanicLayer, cors::CorsLayer, trace::TraceLayer};
use tracing::{info, warn};

#[derive(Clone)]
struct AppState {
    store: StateStoreHandle,
    storage: StorageHandle,
    engine: EngineHandle,
    token: Arc<RwLock<[u8; 32]>>,
    token_path: Arc<PathBuf>,
    sessions: Arc<Mutex<HashMap<String, BrowserSession>>>,
    max_sessions: usize,
    secure_cookie: bool,
    api_slots: Arc<Semaphore>,
    rate: Arc<Mutex<RateWindow>>,
    api_requests_per_second: usize,
    metrics: Arc<ApiMetrics>,
    rate_samples: Arc<std::sync::Mutex<HashMap<TorrentId, RateSample>>>,
    mutation_slot: Arc<Semaphore>,
    events: broadcast::Sender<EventEnvelope>,
    next_sequence: Arc<AtomicU64>,
    started: Instant,
    metainfo_limit: usize,
    websocket_limit: usize,
    loaded_limit: usize,
    active_limit: usize,
    list_page_size: usize,
}

#[derive(Clone)]
struct BrowserSession {
    csrf_token: String,
    expires: Instant,
}

struct RateWindow {
    started: Instant,
    requests: usize,
}

#[derive(Default)]
struct ApiMetrics {
    requests: AtomicU64,
    authentication_failures: AtomicU64,
    rejected_requests: AtomicU64,
    token_rotations: AtomicU64,
    sessions_created: AtomicU64,
}

struct RateSample {
    sampled: Instant,
    downloaded: u64,
    uploaded: u64,
    download_rate: u64,
    upload_rate: u64,
}

const ENGINE_SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("failed to initialize state: {0}")]
    Store(#[from] StoreError),
    #[error("failed to initialize payload storage: {0}")]
    Storage(#[from] dendrite_storage::StorageError),
    #[error("administrator token error: {0}")]
    Token(#[from] std::io::Error),
    #[error("administrator token file is invalid")]
    InvalidToken,
    #[error("server error: {0}")]
    Server(String),
}

#[derive(Debug)]
enum ApiError {
    Unauthorized,
    BadRequest(String),
    NotFound,
    Conflict(String),
    Limit(String),
    Internal(String),
}

pub async fn run(settings: Settings) -> Result<(), ServerError> {
    let state = initialize_state(&settings).await?;
    if let Some(gateway) = settings.listen.nat_pmp_gateway {
        tokio::spawn(maintain_nat_mappings(
            gateway,
            settings.listen.peer.port(),
            state.engine.clone(),
        ));
    }

    let app = build_router(&settings, &state)?;
    let engine = state.engine.clone();
    let result = serve(settings, state, app).await;
    match enforce_shutdown_grace(ENGINE_SHUTDOWN_GRACE, engine.shutdown()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, "torrent engine did not acknowledge shutdown"),
        Err(()) => warn!(
            grace_seconds = ENGINE_SHUTDOWN_GRACE.as_secs(),
            "torrent engine exceeded the shutdown grace period"
        ),
    }
    result
}

async fn enforce_shutdown_grace<F, T>(grace: Duration, future: F) -> Result<T, ()>
where
    F: Future<Output = T>,
{
    tokio::time::timeout(grace, future).await.map_err(|_| ())
}

fn build_router(settings: &Settings, state: &AppState) -> Result<Router, ServerError> {
    let protected = Router::new()
        .route("/status", get(status))
        .route("/torrents", get(list_torrents).post(add_torrent))
        .route("/torrents/magnet", post(add_magnet))
        .route(
            "/torrents/{id}",
            get(get_torrent)
                .patch(update_torrent_settings)
                .delete(remove_torrent),
        )
        .route("/torrents/{id}/actions", post(torrent_action))
        .route("/events", get(events))
        .route("/auth/session", post(create_browser_session))
        .route("/auth/session/logout", post(logout_browser_session))
        .route("/auth/token/rotate", post(rotate_token))
        .route("/metrics", get(metrics))
        .layer(DefaultBodyLimit::max(settings.limits.metainfo_bytes))
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate));
    let mut app = Router::new()
        .route("/healthz", get(health))
        .route("/api/v2/openapi.json", get(openapi))
        .nest("/api/v2", protected)
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());
    if !settings.listen.allowed_origins.is_empty() {
        let origins: Result<Vec<HeaderValue>, _> = settings
            .listen
            .allowed_origins
            .iter()
            .map(|value| HeaderValue::from_str(value))
            .collect();
        let origins = origins
            .map_err(|error| ServerError::Server(format!("invalid allowed origin: {error}")))?;
        app = app.layer(
            CorsLayer::new()
                .allow_origin(origins)
                .allow_methods([Method::GET, Method::POST, Method::PATCH, Method::DELETE])
                .allow_headers([
                    header::AUTHORIZATION,
                    header::CONTENT_TYPE,
                    header::HeaderName::from_static("x-csrf-token"),
                ])
                .allow_credentials(true),
        );
    }

    Ok(app)
}

async fn maintain_nat_mappings(
    gateway: std::net::SocketAddr,
    peer_port: u16,
    engine: EngineHandle,
) {
    let client = NatPmpClient::new(gateway, Duration::from_secs(3));
    let mut renewal = tokio::time::interval(Duration::from_mins(30));
    loop {
        renewal.tick().await;
        let tcp = client
            .map_port(
                MappingProtocol::Tcp,
                peer_port,
                peer_port,
                Duration::from_hours(1),
            )
            .await;
        let requested_udp_port = tcp
            .as_ref()
            .map_or(peer_port, |mapping| mapping.external_port);
        let udp = client
            .map_port(
                MappingProtocol::Udp,
                peer_port,
                requested_udp_port,
                Duration::from_hours(1),
            )
            .await;
        match (tcp, udp) {
            (Ok(tcp), Ok(udp)) if tcp.external_port == udp.external_port => {
                engine.set_advertised_peer_port(tcp.external_port);
                info!(
                    external_port = tcp.external_port,
                    "renewed NAT-PMP TCP and UDP mappings"
                );
            }
            (Ok(tcp), Ok(udp)) => warn!(
                tcp_port = tcp.external_port,
                udp_port = udp.external_port,
                "NAT-PMP gateway assigned inconsistent public ports; retaining prior advertisement"
            ),
            (Err(error), _) => {
                warn!(protocol = ?MappingProtocol::Tcp, %error, "failed to renew NAT-PMP mapping");
            }
            (_, Err(error)) => {
                warn!(protocol = ?MappingProtocol::Udp, %error, "failed to renew NAT-PMP mapping");
            }
        }
    }
}

async fn initialize_state(settings: &Settings) -> Result<AppState, ServerError> {
    std::fs::create_dir_all(&settings.data_dir)?;
    std::fs::create_dir_all(&settings.download_dir)?;
    let token_path = settings.data_dir.join("admin.token");
    let token = Arc::new(RwLock::new(load_or_create_token(&token_path)?));
    let store = StateStoreHandle::start(&settings.data_dir.join("state.redb"), 256)?;
    let storage = StorageHandle::start(&settings.download_dir, 1024)?;
    let peer_listener = bind_peer_listener(settings.listen.peer)
        .map_err(|error| ServerError::Server(error.to_string()))?;
    let utp = UtpEndpoint::bind(settings.listen.peer)
        .await
        .map_err(|error| ServerError::Server(error.to_string()))?;
    let dht = DhtClient::bind(settings.listen.dht, 512, 65_507, Duration::from_secs(2))
        .await
        .map_err(|error| ServerError::Server(error.to_string()))?;
    let engine = EngineHandle::start_configured(
        store.clone(),
        storage.clone(),
        EngineOptions {
            tracker_response_limit: settings.limits.tracker_response_bytes,
            metainfo_limit: settings.limits.metainfo_bytes,
            dht_bootstrap: settings.listen.dht_bootstrap.clone(),
            dht: Some(dht),
            utp: Some(utp),
            peer_port: settings.listen.peer.port(),
            encryption: match settings.listen.peer_encryption {
                PeerEncryption::Disabled => EncryptionPolicy::Disabled,
                PeerEncryption::Preferred => EncryptionPolicy::Preferred,
                PeerEncryption::PlaintextPreferred => EncryptionPolicy::PlaintextPreferred,
                PeerEncryption::Required => EncryptionPolicy::Required,
            },
            peer_connection_limit: settings.limits.peer_connections,
            allow_private_web_seeds: false,
            download_buffer_bytes: settings.limits.download_buffer_bytes,
            piece_cache_bytes: settings.limits.piece_cache_bytes,
            piece_flush_interval: Duration::from_secs(settings.storage.flush_interval_seconds),
            transfer: TransferPolicy {
                upload_slots: settings.transfer.upload_slots,
                optimistic_upload_slots: settings.transfer.optimistic_upload_slots,
                reciprocal_ratio: settings.transfer.reciprocal_ratio,
                reciprocal_bootstrap_bytes: settings.transfer.reciprocal_bootstrap_bytes,
                upload_rate_limit_bytes: settings.transfer.upload_rate_limit_bytes,
                torrent_max_upload_ratio: settings.transfer.torrent_max_upload_ratio,
            },
        },
    );
    engine.serve_incoming(peer_listener);
    let (event_sender, _) = broadcast::channel(4096);
    let state = AppState {
        store,
        storage,
        engine,
        token,
        token_path: Arc::new(token_path),
        sessions: Arc::new(Mutex::new(HashMap::new())),
        max_sessions: settings.limits.browser_sessions,
        secure_cookie: !settings.listen.api.ip().is_loopback(),
        api_slots: Arc::new(Semaphore::new(settings.limits.api_concurrency)),
        rate: Arc::new(Mutex::new(RateWindow {
            started: Instant::now(),
            requests: 0,
        })),
        api_requests_per_second: settings.limits.api_requests_per_second,
        metrics: Arc::new(ApiMetrics::default()),
        rate_samples: Arc::new(std::sync::Mutex::new(HashMap::new())),
        mutation_slot: Arc::new(Semaphore::new(1)),
        events: event_sender,
        next_sequence: Arc::new(AtomicU64::new(1)),
        started: Instant::now(),
        metainfo_limit: settings.limits.metainfo_bytes,
        websocket_limit: settings.limits.websocket_message_bytes,
        loaded_limit: settings.limits.loaded_torrents,
        active_limit: settings.limits.active_torrents,
        list_page_size: settings.limits.list_page_size,
    };
    bridge_engine_events(&state);
    restore_active_torrents(&state);
    Ok(state)
}

async fn serve(settings: Settings, state: AppState, app: Router) -> Result<(), ServerError> {
    let address = settings.listen.api;
    info!(%address, backend = ?state.storage.backend(), "starting Dendrite API");
    if address.ip().is_loopback() {
        let listener = TcpListener::bind(address)
            .await
            .map_err(|error| ServerError::Server(error.to_string()))?;
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|error| ServerError::Server(error.to_string()))?;
    } else {
        let certificate =
            settings.listen.tls_certificate.as_ref().ok_or_else(|| {
                ServerError::Server("remote API certificate is missing".to_owned())
            })?;
        let private_key =
            settings.listen.tls_private_key.as_ref().ok_or_else(|| {
                ServerError::Server("remote API private key is missing".to_owned())
            })?;
        let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(certificate, private_key)
            .await
            .map_err(|error| ServerError::Server(error.to_string()))?;
        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            shutdown_handle.graceful_shutdown(Some(Duration::from_secs(30)));
        });
        axum_server::bind_rustls(address, tls)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .map_err(|error| ServerError::Server(error.to_string()))?;
    }
    info!("Dendrite stopped cleanly");
    Ok(())
}

/// Peer listener with a deep accept queue: bursts of inbound connections
/// after an announce must not overflow the kernel backlog while handshakes are
/// being admitted.
fn bind_peer_listener(address: std::net::SocketAddr) -> std::io::Result<TcpListener> {
    const PEER_LISTEN_BACKLOG: i32 = 4096;
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(address),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.bind(&address.into())?;
    socket.listen(PEER_LISTEN_BACKLOG)?;
    socket.set_nonblocking(true)?;
    TcpListener::from_std(socket.into())
}

async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn status(State(state): State<AppState>) -> Json<StatusResponse> {
    let records = state.store.list_summaries().await.unwrap_or_default();
    let quarantined_records = state
        .store
        .quarantined_record_count()
        .await
        .unwrap_or_default();
    let active_torrents = records
        .iter()
        .filter(|record| !matches!(record.state, TorrentState::Stopped | TorrentState::Error))
        .count();
    Json(StatusResponse {
        api_version: API_VERSION.to_owned(),
        daemon_version: env!("CARGO_PKG_VERSION").to_owned(),
        uptime_seconds: state.started.elapsed().as_secs(),
        loaded_torrents: records.len(),
        active_torrents,
        connected_peers: state.engine.connected_peers(),
        quarantined_records,
        storage_backend: match state.storage.backend() {
            BackendKind::Portable => "portable",
            #[cfg(target_os = "linux")]
            BackendKind::IoUring => "io_uring",
        }
        .to_owned(),
    })
}

async fn list_torrents(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<ListResponse<TorrentSummary>>, ApiError> {
    let cursor = query.cursor.as_deref().map(parse_id).transpose()?;
    let limit = query.limit.unwrap_or(state.list_page_size);
    if limit == 0 || limit > state.list_page_size {
        return Err(ApiError::BadRequest(format!(
            "limit must be in 1..={}",
            state.list_page_size
        )));
    }
    let mut records: Vec<_> = state
        .store
        .list_summaries()
        .await
        .map_err(ApiError::from)?
        .into_iter()
        .filter(|record| cursor.is_none_or(|cursor| record.id > cursor))
        .take(limit.saturating_add(1))
        .collect();
    let has_more = records.len() > limit;
    records.truncate(limit);
    let next_cursor = has_more
        .then(|| records.last().map(|record| record.id.to_string()))
        .flatten();
    Ok(Json(ListResponse {
        items: records
            .iter()
            .map(|record| summary(&state, record))
            .collect(),
        next_cursor,
    }))
}

#[derive(serde::Deserialize)]
struct ListQuery {
    cursor: Option<String>,
    limit: Option<usize>,
}

async fn get_torrent(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<TorrentSummary>, ApiError> {
    let id = parse_id(&id)?;
    state
        .store
        .get_summary(id)
        .await
        .map_err(ApiError::from)?
        .map(|record| Json(summary(&state, &record)))
        .ok_or(ApiError::NotFound)
}

async fn add_torrent(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<TorrentSummary>), ApiError> {
    let _mutation = state
        .mutation_slot
        .acquire()
        .await
        .map_err(|_| ApiError::Internal("mutation coordinator stopped".to_owned()))?;
    enforce_capacity(&state, false).await?;
    let mut metainfo_bytes = None;
    let mut options = AddTorrentOptions::default();
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| ApiError::BadRequest(error.to_string()))?
    {
        if field.name() == Some("metainfo") {
            let bytes = field
                .bytes()
                .await
                .map_err(|error| ApiError::BadRequest(error.to_string()))?;
            if bytes.len() > state.metainfo_limit {
                return Err(ApiError::BadRequest(
                    "metainfo exceeds configured limit".to_owned(),
                ));
            }
            metainfo_bytes = Some(bytes);
        } else if field.name() == Some("options") {
            let encoded = field
                .text()
                .await
                .map_err(|error| ApiError::BadRequest(error.to_string()))?;
            options = serde_json::from_str(&encoded)
                .map_err(|error| ApiError::BadRequest(error.to_string()))?;
        }
    }
    let bytes =
        metainfo_bytes.ok_or_else(|| ApiError::BadRequest("missing metainfo field".to_owned()))?;
    validate_add_options(&options)?;
    let metainfo = Metainfo::parse(
        &bytes,
        BencodeLimits {
            input_bytes: state.metainfo_limit,
            byte_string_bytes: state.metainfo_limit,
            ..BencodeLimits::default()
        },
    )
    .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let record = TorrentRecord {
        record_version: TorrentRecord::RECORD_VERSION,
        id: TorrentId::new(),
        name: metainfo.name,
        state: if options.start {
            TorrentState::Starting
        } else {
            TorrentState::Stopped
        },
        v1_info_hash: metainfo.v1_info_hash,
        v2_info_hash: metainfo.v2_info_hash,
        total_length: metainfo.total_length,
        raw_metainfo: metainfo.raw,
        magnet_uri: None,
        stop_on_complete: options.stop_on_complete,
        completed_pieces: Vec::new(),
        downloaded: 0,
        uploaded: 0,
        added_at_unix_ms: unix_milliseconds(),
    };
    if options.start {
        enforce_capacity(&state, true).await?;
    }
    state
        .store
        .put_torrent(record.clone())
        .await
        .map_err(ApiError::from)?;
    publish(
        &state,
        Some(record.id.to_string()),
        "torrent_added",
        serde_json::json!({
            "torrent": summary(&state, &record),
        }),
    );
    if options.start {
        state
            .engine
            .resume(record.id)
            .await
            .map_err(|error| ApiError::Internal(error.to_string()))?;
    }
    Ok((StatusCode::CREATED, Json(summary(&state, &record))))
}

async fn add_magnet(
    State(state): State<AppState>,
    Json(request): Json<AddTorrentRequest>,
) -> Result<(StatusCode, Json<TorrentSummary>), ApiError> {
    let _mutation = state
        .mutation_slot
        .acquire()
        .await
        .map_err(|_| ApiError::Internal("mutation coordinator stopped".to_owned()))?;
    enforce_capacity(&state, false).await?;
    let AddTorrentRequest::Magnet { uri, options } = request;
    validate_add_options(&options)?;
    let magnet = Magnet::parse(&uri).map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let name = magnet
        .display_name
        .clone()
        .or_else(|| magnet.v1_info_hash.map(|hash| hash.to_string()))
        .or_else(|| magnet.v2_info_hash.map(|hash| hash.to_string()))
        .ok_or_else(|| ApiError::BadRequest("magnet has no usable identity".to_owned()))?;
    let record = TorrentRecord {
        record_version: TorrentRecord::RECORD_VERSION,
        id: TorrentId::new(),
        name,
        state: if options.start {
            TorrentState::Starting
        } else {
            TorrentState::Stopped
        },
        v1_info_hash: magnet.v1_info_hash,
        v2_info_hash: magnet.v2_info_hash,
        total_length: 0,
        raw_metainfo: Vec::new(),
        magnet_uri: Some(uri),
        stop_on_complete: options.stop_on_complete,
        completed_pieces: Vec::new(),
        downloaded: 0,
        uploaded: 0,
        added_at_unix_ms: unix_milliseconds(),
    };
    if options.start {
        enforce_capacity(&state, true).await?;
    }
    state
        .store
        .put_torrent(record.clone())
        .await
        .map_err(ApiError::from)?;
    publish(
        &state,
        Some(record.id.to_string()),
        "torrent_added",
        serde_json::json!({ "torrent": summary(&state, &record) }),
    );
    if options.start {
        state
            .engine
            .resume(record.id)
            .await
            .map_err(|error| ApiError::Internal(error.to_string()))?;
    }
    Ok((StatusCode::CREATED, Json(summary(&state, &record))))
}

async fn torrent_action(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<TorrentActionRequest>,
) -> Result<Json<TorrentSummary>, ApiError> {
    let _mutation = state
        .mutation_slot
        .acquire()
        .await
        .map_err(|_| ApiError::Internal("mutation coordinator stopped".to_owned()))?;
    let id = parse_id(&id)?;
    let mut record = state
        .store
        .get_torrent(id)
        .await
        .map_err(ApiError::from)?
        .ok_or(ApiError::NotFound)?;
    if matches!(
        request.action,
        TorrentAction::Resume | TorrentAction::Announce
    ) && matches!(record.state, TorrentState::Stopped | TorrentState::Error)
    {
        enforce_capacity(&state, true).await?;
    }
    record.state = match request.action {
        TorrentAction::Pause => TorrentState::Stopped,
        TorrentAction::Resume => TorrentState::Starting,
        TorrentAction::Recheck => TorrentState::Checking,
        TorrentAction::Announce => record.state,
    };
    state
        .store
        .put_torrent(record.clone())
        .await
        .map_err(ApiError::from)?;
    match request.action {
        TorrentAction::Pause => state.engine.pause(id).await,
        TorrentAction::Resume | TorrentAction::Announce => state.engine.resume(id).await,
        TorrentAction::Recheck => state.engine.recheck(id).await,
    }
    .map_err(|error| ApiError::Internal(error.to_string()))?;
    publish(
        &state,
        Some(record.id.to_string()),
        "torrent_state_changed",
        serde_json::json!({ "state": record.state }),
    );
    Ok(Json(summary(&state, &record)))
}

async fn update_torrent_settings(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<TorrentSettingsUpdate>,
) -> Result<Json<TorrentSummary>, ApiError> {
    let _mutation = state
        .mutation_slot
        .acquire()
        .await
        .map_err(|_| ApiError::Internal("mutation coordinator stopped".to_owned()))?;
    let id = parse_id(&id)?;
    let mut record = state
        .store
        .get_torrent(id)
        .await
        .map_err(ApiError::from)?
        .ok_or(ApiError::NotFound)?;
    record.stop_on_complete = request.stop_on_complete;
    if !state
        .store
        .replace_torrent(record.clone())
        .await
        .map_err(ApiError::from)?
    {
        return Err(ApiError::NotFound);
    }
    let complete_payload = record.total_length > 0 && record.downloaded >= record.total_length;
    if request.stop_on_complete
        && (record.state == TorrentState::Seeding || complete_payload)
        && record.state != TorrentState::Stopped
    {
        state
            .engine
            .pause(id)
            .await
            .map_err(|error| ApiError::Internal(error.to_string()))?;
        record = state
            .store
            .get_torrent(id)
            .await
            .map_err(ApiError::from)?
            .ok_or(ApiError::NotFound)?;
    }
    publish(
        &state,
        Some(record.id.to_string()),
        "torrent_settings_changed",
        serde_json::json!({ "stop_on_complete": record.stop_on_complete }),
    );
    Ok(Json(summary(&state, &record)))
}

async fn remove_torrent(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<StatusCode, ApiError> {
    let _mutation = state
        .mutation_slot
        .acquire()
        .await
        .map_err(|_| ApiError::Internal("mutation coordinator stopped".to_owned()))?;
    let id = parse_id(&id)?;
    state
        .engine
        .forget(id)
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;
    if !state
        .store
        .remove_torrent(id)
        .await
        .map_err(ApiError::from)?
    {
        return Err(ApiError::NotFound);
    }
    publish(
        &state,
        Some(id.to_string()),
        "torrent_removed",
        serde_json::Value::Null,
    );
    Ok(StatusCode::NO_CONTENT)
}

async fn events(State(state): State<AppState>, websocket: WebSocketUpgrade) -> impl IntoResponse {
    websocket
        .max_message_size(state.websocket_limit)
        .max_frame_size(state.websocket_limit)
        .on_upgrade(move |socket| event_session(socket, state))
}

async fn event_session(mut socket: WebSocket, state: AppState) {
    const SEND_TIMEOUT: Duration = Duration::from_secs(10);
    let mut receiver = state.events.subscribe();
    loop {
        match receiver.recv().await {
            Ok(event) => match serde_json::to_string(&event) {
                Ok(encoded) => {
                    if !matches!(
                        tokio::time::timeout(
                            SEND_TIMEOUT,
                            socket.send(Message::Text(encoded.into()))
                        )
                        .await,
                        Ok(Ok(()))
                    ) {
                        break;
                    }
                }
                Err(error) => warn!(%error, "failed to encode event"),
            },
            Err(broadcast::error::RecvError::Lagged(_)) => {
                let event = EventEnvelope {
                    schema_version: 1,
                    sequence: state.next_sequence.fetch_add(1, Ordering::Relaxed),
                    timestamp_unix_ms: unix_milliseconds(),
                    resource_id: None,
                    kind: "resync_required".to_owned(),
                    payload: serde_json::Value::Null,
                };
                if let Ok(encoded) = serde_json::to_string(&event) {
                    let _result_ignored = tokio::time::timeout(
                        SEND_TIMEOUT,
                        socket.send(Message::Text(encoded.into())),
                    )
                    .await;
                }
                let _result_ignored = tokio::time::timeout(SEND_TIMEOUT, socket.close()).await;
                break;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

const SESSION_SECONDS: u64 = 12 * 60 * 60;

async fn create_browser_session(
    State(state): State<AppState>,
) -> Result<(HeaderMap, Json<BrowserSessionResponse>), ApiError> {
    let session = URL_SAFE_NO_PAD.encode(rand::random::<[u8; 32]>());
    let csrf_token = URL_SAFE_NO_PAD.encode(rand::random::<[u8; 32]>());
    let mut sessions = state.sessions.lock().await;
    let now = Instant::now();
    sessions.retain(|_, value| value.expires > now);
    if sessions.len() >= state.max_sessions {
        return Err(ApiError::Limit(
            "browser session limit has been reached".to_owned(),
        ));
    }
    sessions.insert(
        session.clone(),
        BrowserSession {
            csrf_token: csrf_token.clone(),
            expires: now + Duration::from_secs(SESSION_SECONDS),
        },
    );
    drop(sessions);
    state
        .metrics
        .sessions_created
        .fetch_add(1, Ordering::Relaxed);
    let mut headers = HeaderMap::new();
    headers.insert(
        header::SET_COOKIE,
        session_cookie(&session, SESSION_SECONDS, state.secure_cookie)?,
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok((
        headers,
        Json(BrowserSessionResponse {
            csrf_token,
            expires_in_seconds: SESSION_SECONDS,
        }),
    ))
}

async fn logout_browser_session(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<(HeaderMap, StatusCode), ApiError> {
    if let Some(session) = cookie_value(&headers, "dendrite_session") {
        state.sessions.lock().await.remove(session);
    }
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        header::SET_COOKIE,
        session_cookie("", 0, state.secure_cookie)?,
    );
    Ok((response_headers, StatusCode::NO_CONTENT))
}

async fn rotate_token(
    State(state): State<AppState>,
) -> Result<(HeaderMap, Json<TokenRotationResponse>), ApiError> {
    let token: [u8; 32] = rand::random();
    persist_token_atomically(&state.token_path, token)
        .map_err(|error| ApiError::Internal(error.to_string()))?;
    *state.token.write().await = token;
    state.sessions.lock().await.clear();
    state
        .metrics
        .token_rotations
        .fetch_add(1, Ordering::Relaxed);
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok((
        headers,
        Json(TokenRotationResponse {
            token: URL_SAFE_NO_PAD.encode(token),
        }),
    ))
}

async fn metrics(State(state): State<AppState>) -> Response {
    let records = state.store.list_summaries().await.unwrap_or_default();
    let active = records
        .iter()
        .filter(|record| is_active(record.state))
        .count();
    let body = format!(
        concat!(
            "# TYPE dendrite_api_requests_total counter\n",
            "dendrite_api_requests_total {}\n",
            "# TYPE dendrite_api_authentication_failures_total counter\n",
            "dendrite_api_authentication_failures_total {}\n",
            "# TYPE dendrite_api_rejected_requests_total counter\n",
            "dendrite_api_rejected_requests_total {}\n",
            "# TYPE dendrite_token_rotations_total counter\n",
            "dendrite_token_rotations_total {}\n",
            "# TYPE dendrite_browser_sessions_created_total counter\n",
            "dendrite_browser_sessions_created_total {}\n",
            "# TYPE dendrite_torrents gauge\n",
            "dendrite_torrents {}\n",
            "# TYPE dendrite_active_torrents gauge\n",
            "dendrite_active_torrents {}\n",
            "# TYPE dendrite_state_commits_total counter\n",
            "dendrite_state_commits_total {}\n",
            "# TYPE dendrite_state_queue_depth gauge\n",
            "dendrite_state_queue_depth {}\n",
            "# TYPE dendrite_downloaded_bytes_total counter\n",
            "dendrite_downloaded_bytes_total {}\n",
            "# TYPE dendrite_uploaded_bytes_total counter\n",
            "dendrite_uploaded_bytes_total {}\n"
        ),
        state.metrics.requests.load(Ordering::Relaxed),
        state
            .metrics
            .authentication_failures
            .load(Ordering::Relaxed),
        state.metrics.rejected_requests.load(Ordering::Relaxed),
        state.metrics.token_rotations.load(Ordering::Relaxed),
        state.metrics.sessions_created.load(Ordering::Relaxed),
        records.len(),
        active,
        state.store.commit_count(),
        state.store.queue_depth(),
        records.iter().fold(0_u64, |total, record| total
            .saturating_add(record.downloaded)),
        records
            .iter()
            .fold(0_u64, |total, record| total.saturating_add(record.uploaded)),
    );
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

async fn openapi() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "openapi": "3.1.0",
        "info": { "title": "Dendrite API", "version": API_VERSION },
        "servers": [{ "url": "/api/v2" }],
        "components": {
            "securitySchemes": {
                "bearerAuth": { "type": "http", "scheme": "bearer", "bearerFormat": "base64url" },
                "browserSession": { "type": "apiKey", "in": "cookie", "name": "dendrite_session" }
            },
            "schemas": {
                "Problem": {
                    "type": "object",
                    "required": ["type", "title", "status", "code", "detail"]
                },
                "TorrentSummary": { "type": "object", "required": ["id", "name", "state", "total_length"] },
                "StatusResponse": { "type": "object", "required": ["api_version", "daemon_version", "uptime_seconds"] }
            }
        },
        "security": [{ "bearerAuth": [] }, { "browserSession": [] }],
        "paths": {
            "/status": { "get": { "responses": { "200": { "description": "Daemon status" } } } },
            "/torrents": {
                "get": { "parameters": [
                    { "name": "cursor", "in": "query", "schema": { "type": "string" } },
                    { "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1 } }
                ], "responses": { "200": { "description": "Torrent page" } } },
                "post": { "requestBody": { "required": true }, "responses": { "201": { "description": "Torrent added" } } }
            },
            "/torrents/magnet": { "post": { "responses": { "201": { "description": "Magnet added" } } } },
            "/torrents/{id}": {
                "get": { "responses": { "200": { "description": "Torrent" } } },
                "patch": { "responses": { "200": { "description": "Settings updated" } } },
                "delete": { "responses": { "204": { "description": "Removed" } } }
            },
            "/torrents/{id}/actions": { "post": { "responses": { "200": { "description": "Action accepted" } } } },
            "/events": { "get": { "responses": { "101": { "description": "WebSocket event stream" } } } },
            "/auth/session": { "post": { "responses": { "200": { "description": "Browser session created" } } } },
            "/auth/session/logout": { "post": { "responses": { "204": { "description": "Browser session destroyed" } } } },
            "/auth/token/rotate": { "post": { "responses": { "200": { "description": "Administrator token rotated" } } } },
            "/metrics": { "get": { "responses": { "200": { "description": "Prometheus metrics" } } } }
        }
    }))
}

async fn authenticate(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, ApiError> {
    state.metrics.requests.fetch_add(1, Ordering::Relaxed);
    let _slot = state.api_slots.clone().try_acquire_owned().map_err(|_| {
        state
            .metrics
            .rejected_requests
            .fetch_add(1, Ordering::Relaxed);
        ApiError::Limit("API concurrency limit has been reached".to_owned())
    })?;
    {
        let mut rate = state.rate.lock().await;
        if !consume_rate_slot(&mut rate, state.api_requests_per_second, Instant::now()) {
            state
                .metrics
                .rejected_requests
                .fetch_add(1, Ordering::Relaxed);
            return Err(ApiError::Limit(
                "API request rate limit has been reached".to_owned(),
            ));
        }
    }
    let bearer = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let bearer_valid = if let Some(value) = bearer {
        if let Ok(decoded) = URL_SAFE_NO_PAD.decode(value)
            && let Ok(candidate) = <[u8; 32]>::try_from(decoded)
        {
            bool::from(candidate.ct_eq(&*state.token.read().await))
        } else {
            false
        }
    } else {
        false
    };
    if !bearer_valid {
        let session_id = cookie_value(request.headers(), "dendrite_session").map(str::to_owned);
        let csrf = request
            .headers()
            .get("x-csrf-token")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let safe_method = matches!(
            *request.method(),
            Method::GET | Method::HEAD | Method::OPTIONS
        );
        let authorized_session = authorize_session(&state, session_id, csrf, safe_method).await;
        if !authorized_session {
            state
                .metrics
                .authentication_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(ApiError::Unauthorized);
        }
    }
    Ok(next.run(request).await)
}

async fn authorize_session(
    state: &AppState,
    session_id: Option<String>,
    csrf: Option<String>,
    safe_method: bool,
) -> bool {
    authorize_session_at(state, session_id, csrf, safe_method, Instant::now()).await
}

async fn authorize_session_at(
    state: &AppState,
    session_id: Option<String>,
    csrf: Option<String>,
    safe_method: bool,
    now: Instant,
) -> bool {
    let Some(session_id) = session_id else {
        return false;
    };
    let mut sessions = state.sessions.lock().await;
    let Some(session) = sessions.get(&session_id).cloned() else {
        return false;
    };
    if session.expires <= now {
        sessions.remove(&session_id);
        return false;
    }
    if safe_method {
        return true;
    }
    csrf.as_deref().is_some_and(|candidate| {
        candidate.len() == session.csrf_token.len()
            && bool::from(candidate.as_bytes().ct_eq(session.csrf_token.as_bytes()))
    })
}

fn consume_rate_slot(rate: &mut RateWindow, limit: usize, now: Instant) -> bool {
    if now.duration_since(rate.started) >= Duration::from_secs(1) {
        rate.started = now;
        rate.requests = 0;
    }
    if rate.requests >= limit {
        return false;
    }
    rate.requests += 1;
    true
}

fn cookie_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .filter_map(|part| part.trim().split_once('='))
        .find_map(|(candidate, value)| (candidate == name).then_some(value))
}

fn session_cookie(value: &str, max_age: u64, secure: bool) -> Result<HeaderValue, ApiError> {
    let secure = if secure { "; Secure" } else { "" };
    HeaderValue::from_str(&format!(
        "dendrite_session={value}; Path=/api/v2; Max-Age={max_age}; HttpOnly; SameSite=Strict{secure}"
    ))
    .map_err(|_| ApiError::Internal("failed to encode session cookie".to_owned()))
}

fn summary(state: &AppState, record: &TorrentRecord) -> TorrentSummary {
    let (download_rate, upload_rate) = sample_rates(state, record);
    let peers = state.engine.torrent_peer_stats(record.id);
    TorrentSummary {
        id: record.id,
        name: record.name.clone(),
        state: record.state,
        v1_info_hash: record.v1_info_hash,
        v2_info_hash: record.v2_info_hash,
        total_length: record.total_length,
        stop_on_complete: record.stop_on_complete,
        downloaded: record.downloaded,
        uploaded: record.uploaded,
        download_rate,
        upload_rate,
        peers: u32::try_from(peers.total).unwrap_or(u32::MAX),
        inbound_peers: u32::try_from(peers.inbound).unwrap_or(u32::MAX),
        outbound_peers: u32::try_from(peers.outbound).unwrap_or(u32::MAX),
        seed_peers: u32::try_from(peers.seeds).unwrap_or(u32::MAX),
        active_downloaders: u32::try_from(peers.active_downloaders).unwrap_or(u32::MAX),
    }
}

fn sample_rates(state: &AppState, record: &TorrentRecord) -> (u64, u64) {
    let Ok(mut samples) = state.rate_samples.lock() else {
        return (0, 0);
    };
    let now = Instant::now();
    let downloaded = state.engine.torrent_downloaded_bytes(record.id);
    let uploaded = state.engine.torrent_uploaded_bytes(record.id);
    let sample = samples.entry(record.id).or_insert(RateSample {
        sampled: now,
        downloaded,
        uploaded,
        download_rate: 0,
        upload_rate: 0,
    });
    let elapsed = now.duration_since(sample.sampled);
    if elapsed >= Duration::from_millis(250) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX).max(1);
        sample.download_rate = downloaded
            .saturating_sub(sample.downloaded)
            .saturating_mul(1_000_000_000)
            / nanos;
        sample.upload_rate = uploaded
            .saturating_sub(sample.uploaded)
            .saturating_mul(1_000_000_000)
            / nanos;
        sample.sampled = now;
        sample.downloaded = downloaded;
        sample.uploaded = uploaded;
    }
    (sample.download_rate, sample.upload_rate)
}

async fn enforce_capacity(state: &AppState, active: bool) -> Result<(), ApiError> {
    let records = state.store.list_summaries().await.map_err(ApiError::from)?;
    if !active && records.len() >= state.loaded_limit {
        return Err(ApiError::Limit(format!(
            "loaded torrent limit {} has been reached",
            state.loaded_limit
        )));
    }
    if active
        && records
            .iter()
            .filter(|record| is_active(record.state))
            .count()
            >= state.active_limit
    {
        return Err(ApiError::Limit(format!(
            "active torrent limit {} has been reached",
            state.active_limit
        )));
    }
    Ok(())
}

const fn is_active(state: TorrentState) -> bool {
    !matches!(state, TorrentState::Stopped | TorrentState::Error)
}

fn parse_id(value: &str) -> Result<TorrentId, ApiError> {
    TorrentId::from_str(value).map_err(|_| ApiError::BadRequest("invalid torrent id".to_owned()))
}

fn validate_add_options(options: &AddTorrentOptions) -> Result<(), ApiError> {
    if options.destination.is_some() {
        return Err(ApiError::BadRequest(
            "per-torrent destinations are not part of API v2.0".to_owned(),
        ));
    }
    if options.sequential {
        return Err(ApiError::BadRequest(
            "sequential mode is not part of API v2.0".to_owned(),
        ));
    }
    Ok(())
}

fn bridge_engine_events(state: &AppState) {
    let mut receiver = state.engine.subscribe();
    let state = state.clone();
    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(event) => publish(
                    &state,
                    Some(event.torrent_id.to_string()),
                    "torrent_state_changed",
                    serde_json::json!({
                        "state": event.state,
                        "detail": event.detail,
                    }),
                ),
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped, "daemon event bridge lagged behind torrent engine");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

fn restore_active_torrents(state: &AppState) {
    let store = state.store.clone();
    let engine = state.engine.clone();
    tokio::spawn(async move {
        let records = match store.list_summaries().await {
            Ok(records) => records,
            Err(error) => {
                warn!(%error, "failed to restore active torrents");
                return;
            }
        };
        for record in records {
            let result = match record.state {
                TorrentState::Starting | TorrentState::Downloading => {
                    engine.resume(record.id).await
                }
                TorrentState::Checking => engine.recheck(record.id).await,
                _ => continue,
            };
            if let Err(error) = result {
                warn!(id = %record.id, %error, "failed to restore torrent actor");
            }
        }
    });
}

fn publish(state: &AppState, resource_id: Option<String>, kind: &str, payload: serde_json::Value) {
    let event = EventEnvelope {
        schema_version: 1,
        sequence: state.next_sequence.fetch_add(1, Ordering::Relaxed),
        timestamp_unix_ms: unix_milliseconds(),
        resource_id,
        kind: kind.to_owned(),
        payload,
    };
    let _subscriber_count = state.events.send(event);
}

fn unix_milliseconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

pub(crate) fn load_or_create_token(path: &Path) -> Result<[u8; 32], ServerError> {
    match std::fs::read_to_string(path) {
        Ok(encoded) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                if std::fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
                    return Err(ServerError::InvalidToken);
                }
            }
            let decoded = URL_SAFE_NO_PAD
                .decode(encoded.trim())
                .map_err(|_| ServerError::InvalidToken)?;
            decoded.try_into().map_err(|_| ServerError::InvalidToken)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let token: [u8; 32] = rand::random();
            let encoded = URL_SAFE_NO_PAD.encode(token);
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                options.mode(0o600);
            }
            let mut file = options.open(path)?;
            file.write_all(encoded.as_bytes())?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            Ok(token)
        }
        Err(error) => Err(ServerError::Token(error)),
    }
}

fn persist_token_atomically(path: &Path, token: [u8; 32]) -> Result<(), std::io::Error> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "token has no parent")
    })?;
    let temporary = parent.join(format!(".admin.token.{:016x}.tmp", rand::random::<u64>()));
    let result = (|| {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(URL_SAFE_NO_PAD.encode(token).as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        std::fs::File::open(parent)?.sync_all()
    })();
    if result.is_err() {
        let _result_ignored = std::fs::remove_file(&temporary);
    }
    result
}

async fn shutdown_signal() {
    let control_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "failed to install Ctrl-C handler");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                warn!(%error, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = control_c => {},
        () = terminate => {},
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::DuplicateHash => Self::Conflict(error.to_string()),
            _ => Self::Internal(error.to_string()),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, title, detail) = match self {
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "Authentication required",
                "A valid administrator bearer token is required".to_owned(),
            ),
            Self::BadRequest(detail) => (
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Invalid request",
                detail,
            ),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "Resource not found",
                "The requested resource does not exist".to_owned(),
            ),
            Self::Conflict(detail) => (
                StatusCode::CONFLICT,
                "conflict",
                "Request conflicts with current state",
                detail,
            ),
            Self::Limit(detail) => (
                StatusCode::TOO_MANY_REQUESTS,
                "limit_reached",
                "Configured limit reached",
                detail,
            ),
            Self::Internal(detail) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "Internal server error",
                detail,
            ),
        };
        let problem = Problem {
            problem_type: format!("https://dendrite-bt.org/problems/{code}"),
            title: title.to_owned(),
            status: status.as_u16(),
            code: code.to_owned(),
            detail,
            instance: None,
        };
        (
            status,
            [(header::CONTENT_TYPE, "application/problem+json")],
            Json(problem),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt as _;

    use super::*;

    fn test_state(
        requests_per_second: usize,
    ) -> Result<(tempfile::TempDir, AppState, String), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let downloads = directory.path().join("downloads");
        std::fs::create_dir(&downloads)?;
        let token_path = directory.path().join("admin.token");
        let token = load_or_create_token(&token_path)?;
        let store = StateStoreHandle::start(&directory.path().join("state.redb"), 32)?;
        let storage = StorageHandle::start_portable(&downloads, 32)?;
        let engine = EngineHandle::start(
            store.clone(),
            storage.clone(),
            64 * 1024,
            64 * 1024,
            Vec::new(),
            None,
            48_181,
        );
        let (events, _) = broadcast::channel(32);
        let state = AppState {
            store,
            storage,
            engine,
            token: Arc::new(RwLock::new(token)),
            token_path: Arc::new(token_path),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            max_sessions: 4,
            secure_cookie: false,
            api_slots: Arc::new(Semaphore::new(8)),
            rate: Arc::new(Mutex::new(RateWindow {
                started: Instant::now(),
                requests: 0,
            })),
            api_requests_per_second: requests_per_second,
            metrics: Arc::new(ApiMetrics::default()),
            rate_samples: Arc::new(std::sync::Mutex::new(HashMap::new())),
            mutation_slot: Arc::new(Semaphore::new(1)),
            events,
            next_sequence: Arc::new(AtomicU64::new(1)),
            started: Instant::now(),
            metainfo_limit: 64 * 1024,
            websocket_limit: 64 * 1024,
            loaded_limit: 2,
            active_limit: 1,
            list_page_size: 1,
        };
        Ok((directory, state, URL_SAFE_NO_PAD.encode(token)))
    }

    fn test_settings() -> Settings {
        let mut settings = Settings::default();
        settings.limits.metainfo_bytes = 64 * 1024;
        settings
    }

    fn authorized(path: &str, token: &str) -> Result<Request<Body>, http::Error> {
        Request::builder()
            .uri(path)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
    }

    #[tokio::test]
    async fn openapi_is_public_but_operational_routes_require_authentication()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_directory, state, _) = test_state(100)?;
        let app = build_router(&test_settings(), &state)?;
        let openapi_response = app
            .clone()
            .oneshot(Request::get("/api/v2/openapi.json").body(Body::empty())?)
            .await?;
        assert_eq!(openapi_response.status(), StatusCode::OK);
        let document: serde_json::Value =
            serde_json::from_slice(&to_bytes(openapi_response.into_body(), 256 * 1024).await?)?;
        assert_eq!(document["openapi"], "3.1.0");
        assert!(document["paths"]["/auth/token/rotate"].is_object());
        let denied = app
            .oneshot(Request::get("/api/v2/status").body(Body::empty())?)
            .await?;
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        Ok(())
    }

    #[tokio::test]
    async fn magnet_add_persists_stop_on_complete_mode() -> Result<(), Box<dyn std::error::Error>> {
        let (_directory, state, token) = test_state(100)?;
        let app = build_router(&test_settings(), &state)?;
        let request = Request::post("/api/v2/torrents/magnet")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::json!({
                    "source": "magnet",
                    "uri": "magnet:?xt=urn:btih:0303030303030303030303030303030303030303&dn=stop-me",
                    "options": {
                        "start": false,
                        "stop_on_complete": true
                    }
                })
                .to_string(),
            ))?;
        let response = app.oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::CREATED);
        let summary: TorrentSummary =
            serde_json::from_slice(&to_bytes(response.into_body(), 64 * 1024).await?)?;
        assert!(summary.stop_on_complete);
        let record = state
            .store
            .get_torrent(summary.id)
            .await?
            .ok_or("added torrent disappeared")?;
        assert!(record.stop_on_complete);
        state.engine.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn settings_update_persists_mode_and_stops_existing_seed()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_directory, state, token) = test_state(100)?;
        let id = TorrentId::new();
        state
            .store
            .put_torrent(TorrentRecord {
                record_version: TorrentRecord::RECORD_VERSION,
                id,
                name: "finished-seed".to_owned(),
                state: TorrentState::Seeding,
                v1_info_hash: Some(dendrite_core::Sha1Hash::from_bytes([7; 20])),
                v2_info_hash: None,
                total_length: 1,
                raw_metainfo: Vec::new(),
                magnet_uri: None,
                stop_on_complete: false,
                completed_pieces: vec![0x80],
                downloaded: 1,
                uploaded: 0,
                added_at_unix_ms: 0,
            })
            .await?;
        let app = build_router(&test_settings(), &state)?;
        let enable = Request::patch(format!("/api/v2/torrents/{id}"))
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"stop_on_complete":true}"#))?;
        let response = app.clone().oneshot(enable).await?;
        assert_eq!(response.status(), StatusCode::OK);
        let summary: TorrentSummary =
            serde_json::from_slice(&to_bytes(response.into_body(), 64 * 1024).await?)?;
        assert!(summary.stop_on_complete);
        assert_eq!(summary.state, TorrentState::Stopped);

        let disable = Request::patch(format!("/api/v2/torrents/{id}"))
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"stop_on_complete":false}"#))?;
        let response = app.oneshot(disable).await?;
        assert_eq!(response.status(), StatusCode::OK);
        let summary: TorrentSummary =
            serde_json::from_slice(&to_bytes(response.into_body(), 64 * 1024).await?)?;
        assert!(!summary.stop_on_complete);
        assert_eq!(summary.state, TorrentState::Stopped);
        let record = state
            .store
            .get_torrent(id)
            .await?
            .ok_or("updated torrent disappeared")?;
        assert!(!record.stop_on_complete);
        state.engine.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn browser_cookie_requires_csrf_for_mutations() -> Result<(), Box<dyn std::error::Error>>
    {
        let (_directory, state, token) = test_state(100)?;
        let app = build_router(&test_settings(), &state)?;
        let create = Request::post("/api/v2/auth/session")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())?;
        let response = app.clone().oneshot(create).await?;
        assert_eq!(response.status(), StatusCode::OK);
        let cookie = response
            .headers()
            .get(header::SET_COOKIE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .ok_or("missing session cookie")?
            .to_owned();
        assert!(
            response
                .headers()
                .get(header::SET_COOKIE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(
                    |value| value.contains("HttpOnly") && value.contains("SameSite=Strict")
                )
        );
        let session: BrowserSessionResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
        let get = Request::get("/api/v2/status")
            .header(header::COOKIE, &cookie)
            .body(Body::empty())?;
        assert_eq!(app.clone().oneshot(get).await?.status(), StatusCode::OK);
        let missing_csrf = Request::post("/api/v2/auth/session/logout")
            .header(header::COOKIE, &cookie)
            .body(Body::empty())?;
        assert_eq!(
            app.clone().oneshot(missing_csrf).await?.status(),
            StatusCode::UNAUTHORIZED
        );
        let logout = Request::post("/api/v2/auth/session/logout")
            .header(header::COOKIE, cookie)
            .header("x-csrf-token", session.csrf_token)
            .body(Body::empty())?;
        assert_eq!(app.oneshot(logout).await?.status(), StatusCode::NO_CONTENT);
        Ok(())
    }

    #[tokio::test]
    async fn token_rotation_is_atomic_and_immediately_revokes_old_credentials()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_directory, state, old_token) = test_state(100)?;
        let token_path = state.token_path.clone();
        let app = build_router(&test_settings(), &state)?;
        let rotate = Request::post("/api/v2/auth/token/rotate")
            .header(header::AUTHORIZATION, format!("Bearer {old_token}"))
            .body(Body::empty())?;
        let response = app.clone().oneshot(rotate).await?;
        assert_eq!(response.status(), StatusCode::OK);
        let rotated: TokenRotationResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
        assert_ne!(rotated.token, old_token);
        assert_eq!(std::fs::read_to_string(&*token_path)?.trim(), rotated.token);
        assert_eq!(
            app.clone()
                .oneshot(authorized("/api/v2/status", &old_token)?)
                .await?
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            app.oneshot(authorized("/api/v2/status", &rotated.token)?)
                .await?
                .status(),
            StatusCode::OK
        );
        Ok(())
    }

    #[tokio::test]
    async fn pagination_metrics_and_rate_limits_are_enforced()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_directory, state, token) = test_state(100)?;
        for byte in [1_u8, 2] {
            state
                .store
                .put_torrent(TorrentRecord {
                    record_version: TorrentRecord::RECORD_VERSION,
                    id: TorrentId::new(),
                    name: format!("torrent-{byte}"),
                    state: TorrentState::Stopped,
                    v1_info_hash: Some(dendrite_core::Sha1Hash::from_bytes([byte; 20])),
                    v2_info_hash: None,
                    total_length: 1,
                    raw_metainfo: Vec::new(),
                    magnet_uri: None,
                    stop_on_complete: false,
                    completed_pieces: Vec::new(),
                    downloaded: 0,
                    uploaded: 0,
                    added_at_unix_ms: 0,
                })
                .await?;
        }
        let app = build_router(&test_settings(), &state)?;
        let first = app
            .clone()
            .oneshot(authorized("/api/v2/torrents?limit=1", &token)?)
            .await?;
        let first: ListResponse<TorrentSummary> =
            serde_json::from_slice(&to_bytes(first.into_body(), 64 * 1024).await?)?;
        assert_eq!(first.items.len(), 1);
        let cursor = first.next_cursor.ok_or("missing pagination cursor")?;
        let second = app
            .clone()
            .oneshot(authorized(
                &format!("/api/v2/torrents?limit=1&cursor={cursor}"),
                &token,
            )?)
            .await?;
        let second: ListResponse<TorrentSummary> =
            serde_json::from_slice(&to_bytes(second.into_body(), 64 * 1024).await?)?;
        assert_eq!(second.items.len(), 1);
        assert_ne!(first.items[0].id, second.items[0].id);
        let metrics = app.oneshot(authorized("/api/v2/metrics", &token)?).await?;
        let body = String::from_utf8(to_bytes(metrics.into_body(), 64 * 1024).await?.to_vec())?;
        assert!(body.contains("dendrite_torrents 2"));

        let (_directory, state, token) = test_state(1)?;
        let limited = build_router(&test_settings(), &state)?;
        assert_eq!(
            limited
                .clone()
                .oneshot(authorized("/api/v2/status", &token)?)
                .await?
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            limited
                .oneshot(authorized("/api/v2/status", &token)?)
                .await?
                .status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        Ok(())
    }

    #[tokio::test]
    async fn sessions_and_rate_limits_use_monotonic_deadlines()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_directory, state, _) = test_state(1)?;
        let base = Instant::now();
        state.sessions.lock().await.insert(
            "session".to_owned(),
            BrowserSession {
                csrf_token: "csrf".to_owned(),
                expires: base + Duration::from_secs(10),
            },
        );
        assert!(
            authorize_session_at(
                &state,
                Some("session".to_owned()),
                None,
                true,
                base + Duration::from_secs(9),
            )
            .await
        );
        assert!(
            !authorize_session_at(
                &state,
                Some("session".to_owned()),
                None,
                true,
                base + Duration::from_secs(11),
            )
            .await
        );
        assert!(!state.sessions.lock().await.contains_key("session"));

        let mut rate = RateWindow {
            started: base,
            requests: 0,
        };
        assert!(consume_rate_slot(&mut rate, 1, base));
        assert!(!consume_rate_slot(
            &mut rate,
            1,
            base + Duration::from_millis(999)
        ));
        assert!(consume_rate_slot(
            &mut rate,
            1,
            base + Duration::from_secs(1)
        ));
        state.engine.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn body_concurrency_bruteforce_and_forwarded_auth_pressure_is_bounded()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_directory, state, token) = test_state(3)?;
        let app = build_router(&test_settings(), &state)?;

        let oversized = Request::post("/api/v2/torrents/magnet")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(vec![b'x'; 64 * 1024 + 1]))?;
        assert_eq!(
            app.clone().oneshot(oversized).await?.status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );

        let forwarded = Request::get("/api/v2/status")
            .header("x-forwarded-authorization", format!("Bearer {token}"))
            .body(Body::empty())?;
        assert_eq!(
            app.clone().oneshot(forwarded).await?.status(),
            StatusCode::UNAUTHORIZED
        );

        let permits = state.api_slots.clone().acquire_many_owned(8).await?;
        assert_eq!(
            app.clone()
                .oneshot(authorized("/api/v2/status", &token)?)
                .await?
                .status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        drop(permits);

        for expected in [
            StatusCode::UNAUTHORIZED,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            let request = Request::get("/api/v2/status")
                .header(header::AUTHORIZATION, "Bearer definitely-invalid")
                .body(Body::empty())?;
            assert_eq!(app.clone().oneshot(request).await?.status(), expected);
        }
        assert!(
            state
                .metrics
                .authentication_failures
                .load(Ordering::Relaxed)
                >= 2
        );
        assert!(state.metrics.rejected_requests.load(Ordering::Relaxed) >= 3);
        state.engine.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_escalates_after_the_grace_period() -> Result<(), Box<dyn std::error::Error>> {
        let cleanup = tokio::spawn(enforce_shutdown_grace(
            ENGINE_SHUTDOWN_GRACE,
            std::future::pending::<()>(),
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(ENGINE_SHUTDOWN_GRACE).await;
        assert!(cleanup.await?.is_err());
        Ok(())
    }
}
