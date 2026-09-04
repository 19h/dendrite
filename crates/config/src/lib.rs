//! Configuration loading and validation.

use std::{net::SocketAddr, path::PathBuf};

use config::{Config, Environment, File};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    pub data_dir: PathBuf,
    pub download_dir: PathBuf,
    pub listen: ListenSettings,
    pub limits: Limits,
    pub transfer: Transfer,
    pub storage: Storage,
    pub logging: Logging,
}

/// Payload durability cadence.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Storage {
    /// Seconds between the group fsync barriers that commit completed
    /// pieces. Longer intervals batch more pieces per fsync and, on ZFS with
    /// a separate log device, keep most payload out of the log; the window of
    /// verified pieces that a crash can force to re-download grows with it.
    pub flush_interval_seconds: u64,
}

impl Default for Storage {
    fn default() -> Self {
        Self {
            flush_interval_seconds: 1,
        }
    }
}

/// Upload economics: how many peers are served, how much upload a peer earns
/// per byte it delivers, and hard caps on egress.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Transfer {
    /// Regular upload slots per torrent, filled by the peers currently
    /// delivering the most verified data to us.
    pub upload_slots: usize,
    /// Rotating slots that audition otherwise unproven peers.
    pub optimistic_upload_slots: usize,
    /// Bytes a downloading torrent may upload to a peer per verified byte
    /// received from it, on top of the bootstrap allowance. `0` disables the
    /// reciprocal cap entirely.
    pub reciprocal_ratio: f64,
    /// Allowance granted to every peer per hour of connection so it can start
    /// reciprocating before it has delivered anything.
    pub reciprocal_bootstrap_bytes: u64,
    /// Upload ceiling per torrent in bytes per second; `0` is unlimited.
    pub upload_rate_limit_bytes: u64,
    /// Per-torrent uploaded/downloaded ratio at which every peer is choked;
    /// `0` is unlimited.
    pub torrent_max_upload_ratio: f64,
}

impl Default for Transfer {
    fn default() -> Self {
        Self {
            upload_slots: 16,
            optimistic_upload_slots: 4,
            reciprocal_ratio: 1.0,
            reciprocal_bootstrap_bytes: 8 * 1024 * 1024,
            upload_rate_limit_bytes: 0,
            torrent_max_upload_ratio: 0.0,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ListenSettings {
    pub api: SocketAddr,
    pub peer: SocketAddr,
    pub dht: SocketAddr,
    pub dht_bootstrap: Vec<SocketAddr>,
    pub nat_pmp_gateway: Option<SocketAddr>,
    pub peer_encryption: PeerEncryption,
    pub tls_certificate: Option<PathBuf>,
    pub tls_private_key: Option<PathBuf>,
    pub allowed_origins: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerEncryption {
    Disabled,
    #[default]
    Preferred,
    /// Dial plaintext first and fall back to encryption; accept both inbound.
    PlaintextPreferred,
    Required,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub loaded_torrents: usize,
    pub active_torrents: usize,
    pub peer_connections: usize,
    pub metainfo_bytes: usize,
    pub tracker_response_bytes: usize,
    pub websocket_message_bytes: usize,
    pub api_concurrency: usize,
    pub api_requests_per_second: usize,
    pub browser_sessions: usize,
    pub list_page_size: usize,
    pub download_buffer_bytes: usize,
    pub piece_cache_bytes: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Logging {
    pub filter: String,
    pub json: bool,
}

#[derive(Debug, Error)]
pub enum SettingsError {
    #[error("failed to load configuration: {0}")]
    Load(#[from] config::ConfigError),
    #[error("non-loopback API binding requires a TLS certificate and private key")]
    RemoteApiWithoutTls,
    #[error("non-loopback API binding requires at least one allowed browser origin")]
    RemoteApiWithoutAllowedOrigins,
    #[error("NAT-PMP gateway must be a nonzero IPv4 socket address")]
    InvalidNatPmpGateway,
    #[error("limit {name} must be in {minimum}..={maximum}, got {actual}")]
    InvalidLimit {
        name: &'static str,
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    #[error("transfer ratio {name} must be a finite non-negative number, got {actual}")]
    InvalidRatio { name: &'static str, actual: f64 },
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./dendrite-data"),
            download_dir: PathBuf::from("./downloads"),
            listen: ListenSettings::default(),
            limits: Limits::default(),
            transfer: Transfer::default(),
            storage: Storage::default(),
            logging: Logging::default(),
        }
    }
}

impl Default for ListenSettings {
    fn default() -> Self {
        Self {
            api: SocketAddr::from(([127, 0, 0, 1], 8412)),
            peer: SocketAddr::from(([0, 0, 0, 0], 16_493)),
            dht: SocketAddr::from(([0, 0, 0, 0], 16_309)),
            dht_bootstrap: Vec::new(),
            nat_pmp_gateway: None,
            peer_encryption: PeerEncryption::Preferred,
            tls_certificate: None,
            tls_private_key: None,
            allowed_origins: Vec::new(),
        }
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            loaded_torrents: 10_000,
            active_torrents: 1_000,
            peer_connections: 10_000,
            metainfo_bytes: 64 * 1024 * 1024,
            tracker_response_bytes: 8 * 1024 * 1024,
            websocket_message_bytes: 8 * 1024 * 1024,
            api_concurrency: 256,
            api_requests_per_second: 1_000,
            browser_sessions: 1_024,
            list_page_size: 200,
            download_buffer_bytes: 2 * 1024 * 1024 * 1024,
            piece_cache_bytes: 512 * 1024 * 1024,
        }
    }
}

impl Default for Logging {
    fn default() -> Self {
        Self {
            filter: "dendrite=info".to_owned(),
            json: false,
        }
    }
}

impl Settings {
    pub fn load(path: Option<&std::path::Path>) -> Result<Self, SettingsError> {
        let mut builder = Config::builder();
        if let Some(path) = path {
            builder = builder.add_source(File::from(path).required(true));
        }
        let settings: Self = builder
            .add_source(Environment::with_prefix("DENDRITE").separator("__"))
            .build()?
            .try_deserialize()?;
        settings.validate()?;
        Ok(settings)
    }

    pub fn validate(&self) -> Result<(), SettingsError> {
        if !self.listen.api.ip().is_loopback()
            && (self.listen.tls_certificate.is_none() || self.listen.tls_private_key.is_none())
        {
            return Err(SettingsError::RemoteApiWithoutTls);
        }
        if !self.listen.api.ip().is_loopback() && self.listen.allowed_origins.is_empty() {
            return Err(SettingsError::RemoteApiWithoutAllowedOrigins);
        }
        if self
            .listen
            .nat_pmp_gateway
            .is_some_and(|gateway| !gateway.is_ipv4() || gateway.port() == 0)
        {
            return Err(SettingsError::InvalidNatPmpGateway);
        }
        check_limit("loaded_torrents", self.limits.loaded_torrents, 1, 100_000)?;
        check_limit("active_torrents", self.limits.active_torrents, 1, 10_000)?;
        check_limit("peer_connections", self.limits.peer_connections, 1, 100_000)?;
        check_limit(
            "metainfo_bytes",
            self.limits.metainfo_bytes,
            1024,
            64 * 1024 * 1024,
        )?;
        check_limit(
            "tracker_response_bytes",
            self.limits.tracker_response_bytes,
            1024,
            8 * 1024 * 1024,
        )?;
        check_limit(
            "websocket_message_bytes",
            self.limits.websocket_message_bytes,
            1024,
            8 * 1024 * 1024,
        )?;
        check_limit("api_concurrency", self.limits.api_concurrency, 1, 10_000)?;
        check_limit(
            "api_requests_per_second",
            self.limits.api_requests_per_second,
            1,
            1_000_000,
        )?;
        check_limit("browser_sessions", self.limits.browser_sessions, 1, 100_000)?;
        check_limit("list_page_size", self.limits.list_page_size, 1, 10_000)?;
        check_limit(
            "download_buffer_bytes",
            self.limits.download_buffer_bytes,
            16 * 1024 * 1024,
            64 * 1024 * 1024 * 1024,
        )?;
        check_limit(
            "piece_cache_bytes",
            self.limits.piece_cache_bytes,
            16 * 1024 * 1024,
            64 * 1024 * 1024 * 1024,
        )?;
        check_limit(
            "flush_interval_seconds",
            usize::try_from(self.storage.flush_interval_seconds).unwrap_or(usize::MAX),
            1,
            300,
        )?;
        check_limit("upload_slots", self.transfer.upload_slots, 1, 1_000)?;
        check_limit(
            "optimistic_upload_slots",
            self.transfer.optimistic_upload_slots,
            0,
            100,
        )?;
        for (name, actual) in [
            ("reciprocal_ratio", self.transfer.reciprocal_ratio),
            (
                "torrent_max_upload_ratio",
                self.transfer.torrent_max_upload_ratio,
            ),
        ] {
            if !actual.is_finite() || actual < 0.0 {
                return Err(SettingsError::InvalidRatio { name, actual });
            }
        }
        if self.limits.active_torrents > self.limits.loaded_torrents {
            return Err(SettingsError::InvalidLimit {
                name: "active_torrents",
                minimum: 1,
                maximum: self.limits.loaded_torrents,
                actual: self.limits.active_torrents,
            });
        }
        Ok(())
    }
}

fn check_limit(
    name: &'static str,
    actual: usize,
    minimum: usize,
    maximum: usize,
) -> Result<(), SettingsError> {
    if (minimum..=maximum).contains(&actual) {
        Ok(())
    } else {
        Err(SettingsError::InvalidLimit {
            name,
            minimum,
            maximum,
            actual,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_and_local() {
        let settings = Settings::default();
        assert!(settings.listen.api.ip().is_loopback());
        assert!(matches!(
            settings.listen.peer_encryption,
            PeerEncryption::Preferred
        ));
        assert!(settings.validate().is_ok());
    }

    #[test]
    fn transfer_ratios_must_be_finite() {
        let mut settings = Settings::default();
        settings.transfer.reciprocal_ratio = f64::NAN;
        assert!(matches!(
            settings.validate(),
            Err(SettingsError::InvalidRatio {
                name: "reciprocal_ratio",
                ..
            })
        ));
        settings.transfer.reciprocal_ratio = 0.5;
        settings.transfer.upload_slots = 0;
        assert!(matches!(
            settings.validate(),
            Err(SettingsError::InvalidLimit {
                name: "upload_slots",
                ..
            })
        ));
    }

    #[test]
    fn remote_api_requires_tls() {
        let mut settings = Settings::default();
        settings.listen.api = SocketAddr::from(([0, 0, 0, 0], 8412));
        assert!(matches!(
            settings.validate(),
            Err(SettingsError::RemoteApiWithoutTls)
        ));
    }
}
