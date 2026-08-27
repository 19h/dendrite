use std::{io::Write as _, path::Path};

use dendrite_config::Settings;
use dendrite_net::utp::UtpEndpoint;
use dendrite_persistence::StateStore;
use dendrite_storage::StorageHandle;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct DoctorReport {
    pub healthy: bool,
    pub checks: Vec<DoctorCheck>,
}

#[derive(Debug, Serialize)]
pub struct DoctorCheck {
    pub name: &'static str,
    pub ok: bool,
    pub detail: String,
}

pub async fn run(settings: &Settings) -> DoctorReport {
    let mut checks = Vec::new();
    check(
        &mut checks,
        "configuration",
        settings.validate().map(|()| "valid".to_owned()),
    );
    check(
        &mut checks,
        "data_directory",
        writable_directory(&settings.data_dir),
    );
    check(
        &mut checks,
        "download_directory",
        writable_directory(&settings.download_dir),
    );
    check(
        &mut checks,
        "administrator_token",
        super::server::load_or_create_token(&settings.data_dir.join("admin.token"))
            .map(|_| "valid 256-bit token with private permissions".to_owned()),
    );
    check(
        &mut checks,
        "state_database",
        StateStore::open(&settings.data_dir.join("state.redb"))
            .map(|_| "schema is readable and writable".to_owned()),
    );
    check(
        &mut checks,
        "payload_storage",
        StorageHandle::start(&settings.download_dir, 8)
            .map(|storage| format!("{:?} backend initialized", storage.backend())),
    );
    check(
        &mut checks,
        "api_listener",
        tokio::net::TcpListener::bind(settings.listen.api)
            .await
            .map(|listener| {
                format!(
                    "{} available",
                    listener.local_addr().unwrap_or(settings.listen.api)
                )
            }),
    );
    check(
        &mut checks,
        "peer_tcp_listener",
        tokio::net::TcpListener::bind(settings.listen.peer)
            .await
            .map(|listener| {
                format!(
                    "{} available",
                    listener.local_addr().unwrap_or(settings.listen.peer)
                )
            }),
    );
    check(
        &mut checks,
        "peer_utp_listener",
        UtpEndpoint::bind(settings.listen.peer)
            .await
            .map(|endpoint| format!("{} available", endpoint.local_addr())),
    );
    check(
        &mut checks,
        "dht_udp_listener",
        tokio::net::UdpSocket::bind(settings.listen.dht)
            .await
            .map(|socket| {
                format!(
                    "{} available",
                    socket.local_addr().unwrap_or(settings.listen.dht)
                )
            }),
    );
    DoctorReport {
        healthy: checks.iter().all(|check| check.ok),
        checks,
    }
}

fn writable_directory(path: &Path) -> Result<String, std::io::Error> {
    std::fs::create_dir_all(path)?;
    let probe = path.join(format!(".dendrite-doctor-{:016x}", rand::random::<u64>()));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe)?;
        file.write_all(b"dendrite doctor\n")?;
        file.sync_all()?;
        std::fs::remove_file(&probe)?;
        Ok(format!("{} is writable", path.display()))
    })();
    if result.is_err() {
        let _result_ignored = std::fs::remove_file(probe);
    }
    result
}

fn check<T, E>(checks: &mut Vec<DoctorCheck>, name: &'static str, result: Result<T, E>)
where
    T: Into<String>,
    E: std::fmt::Display,
{
    match result {
        Ok(detail) => checks.push(DoctorCheck {
            name,
            ok: true,
            detail: detail.into(),
        }),
        Err(error) => checks.push(DoctorCheck {
            name,
            ok: false,
            detail: error.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use super::*;

    #[tokio::test]
    async fn healthy_isolated_environment_passes_every_probe()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let settings = Settings {
            data_dir: directory.path().join("data"),
            download_dir: directory.path().join("downloads"),
            listen: dendrite_config::ListenSettings {
                api: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                peer: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                dht: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                ..dendrite_config::ListenSettings::default()
            },
            ..Settings::default()
        };
        let report = run(&settings).await;
        assert!(report.healthy, "{report:?}");
        assert_eq!(report.checks.len(), 10);
        assert!(report.checks.iter().all(|check| check.ok));
        Ok(())
    }
}
