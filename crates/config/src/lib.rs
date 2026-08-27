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
    pub logging: Logging,
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
    #[default]
    Disabled,
    Preferred,
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
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./dendrite-data"),
            download_dir: PathBuf::from("./downloads"),
            listen: ListenSettings::default(),
            limits: Limits::default(),
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
            peer_encryption: PeerEncryption::Disabled,
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
        assert!(settings.validate().is_ok());
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
