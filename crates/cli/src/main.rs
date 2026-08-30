use std::{
    fmt::Write as _,
    io::{IsTerminal as _, Write as _},
    path::PathBuf,
    process::ExitCode,
    time::Duration,
};

use clap::{Parser, Subcommand};
use dendrite_api_types::{
    AddTorrentOptions, AddTorrentRequest, ListResponse, StatusResponse, TokenRotationResponse,
    TorrentAction, TorrentActionRequest, TorrentSummary,
};
use dendrite_core::TorrentState;
use reqwest::header::{AUTHORIZATION, HeaderValue};
use thiserror::Error;

#[derive(Debug, Parser)]
#[command(name = "dendritectl", version, about)]
struct Arguments {
    #[arg(
        long,
        env = "DENDRITE_API",
        default_value = "http://127.0.0.1:8412/api/v2"
    )]
    api: String,
    #[arg(
        long,
        env = "DENDRITE_TOKEN_FILE",
        default_value = "./dendrite-data/admin.token"
    )]
    token_file: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Status,
    List,
    /// Continuously display human-readable torrent progress.
    Watch {
        /// Torrent ID. Omit it to watch every torrent.
        id: Option<String>,
        /// Refresh interval in seconds.
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..))]
        interval: u64,
        /// Append snapshots instead of redrawing the terminal.
        #[arg(long)]
        no_clear: bool,
    },
    Add {
        source: String,
        #[arg(long)]
        start: bool,
    },
    Pause {
        id: String,
    },
    Resume {
        id: String,
    },
    Recheck {
        id: String,
    },
    Remove {
        id: String,
    },
    RotateToken,
}

#[derive(Debug, Error)]
enum CliError {
    #[error("failed to read administrator token: {0}")]
    Token(#[source] std::io::Error),
    #[error("failed to read torrent input: {0}")]
    Input(#[source] std::io::Error),
    #[error("administrator token is not valid as an HTTP header")]
    TokenHeader,
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("daemon returned HTTP {0}: {1}")]
    Response(reqwest::StatusCode, String),
    #[error("failed to encode output: {0}")]
    Output(#[from] serde_json::Error),
    #[error("terminal I/O failed: {0}")]
    Terminal(#[source] std::io::Error),
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Arguments::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(arguments: Arguments) -> Result<(), CliError> {
    let token = std::fs::read_to_string(&arguments.token_file).map_err(CliError::Token)?;
    let authorization = HeaderValue::from_str(&format!("Bearer {}", token.trim()))
        .map_err(|_| CliError::TokenHeader)?;
    let client = reqwest::Client::new();
    match arguments.command {
        Command::Status => {
            let status: StatusResponse =
                get(&client, &arguments.api, "/status", &authorization).await?;
            println!("{}", serde_json::to_string_pretty(&status)?);
        }
        Command::List => {
            let torrents: ListResponse<TorrentSummary> =
                get(&client, &arguments.api, "/torrents", &authorization).await?;
            println!("{}", serde_json::to_string_pretty(&torrents)?);
        }
        Command::Watch {
            id,
            interval,
            no_clear,
        } => {
            watch(
                &client,
                &arguments.api,
                &authorization,
                id.as_deref(),
                Duration::from_secs(interval),
                no_clear,
            )
            .await?;
        }
        Command::Add { source, start } => {
            let torrent = add(&client, &arguments.api, &authorization, source, start).await?;
            println!("{}", serde_json::to_string_pretty(&torrent)?);
        }
        Command::Pause { id } => {
            print_action(
                &client,
                &arguments.api,
                &authorization,
                &id,
                TorrentAction::Pause,
            )
            .await?;
        }
        Command::Resume { id } => {
            print_action(
                &client,
                &arguments.api,
                &authorization,
                &id,
                TorrentAction::Resume,
            )
            .await?;
        }
        Command::Recheck { id } => {
            print_action(
                &client,
                &arguments.api,
                &authorization,
                &id,
                TorrentAction::Recheck,
            )
            .await?;
        }
        Command::Remove { id } => {
            checked(
                client
                    .delete(format!(
                        "{}/torrents/{id}",
                        arguments.api.trim_end_matches('/')
                    ))
                    .header(AUTHORIZATION, &authorization)
                    .send()
                    .await?,
            )
            .await?;
        }
        Command::RotateToken => {
            let response = checked(
                client
                    .post(format!(
                        "{}/auth/token/rotate",
                        arguments.api.trim_end_matches('/')
                    ))
                    .header(AUTHORIZATION, &authorization)
                    .send()
                    .await?,
            )
            .await?
            .json::<TokenRotationResponse>()
            .await?;
            persist_token(&arguments.token_file, &response.token).map_err(CliError::Token)?;
            println!("administrator token rotated");
        }
    }
    Ok(())
}

async fn watch(
    client: &reqwest::Client,
    base: &str,
    authorization: &HeaderValue,
    id: Option<&str>,
    interval: Duration,
    no_clear: bool,
) -> Result<(), CliError> {
    let redraw = std::io::stdout().is_terminal() && !no_clear;
    let interrupted = tokio::signal::ctrl_c();
    tokio::pin!(interrupted);
    let mut first_frame = true;
    loop {
        let fetch = fetch_watch_torrents(client, base, authorization, id);
        let torrents = tokio::select! {
            signal = &mut interrupted => {
                signal.map_err(CliError::Terminal)?;
                finish_watch(redraw)?;
                return Ok(());
            }
            result = fetch => result?,
        };
        let frame = if id.is_some() {
            render_torrent_detail(&torrents[0])
        } else {
            render_torrent_table(&torrents)
        };
        write_watch_frame(&frame, redraw, first_frame)?;
        first_frame = false;
        tokio::select! {
            signal = &mut interrupted => {
                signal.map_err(CliError::Terminal)?;
                finish_watch(redraw)?;
                return Ok(());
            }
            () = tokio::time::sleep(interval) => {}
        }
    }
}

async fn fetch_watch_torrents(
    client: &reqwest::Client,
    base: &str,
    authorization: &HeaderValue,
    id: Option<&str>,
) -> Result<Vec<TorrentSummary>, CliError> {
    if let Some(id) = id {
        let torrent = get(client, base, &format!("/torrents/{id}"), authorization).await?;
        return Ok(vec![torrent]);
    }
    let mut torrents = Vec::new();
    let mut path = "/torrents".to_owned();
    loop {
        let mut page: ListResponse<TorrentSummary> =
            get(client, base, &path, authorization).await?;
        torrents.append(&mut page.items);
        let Some(cursor) = page.next_cursor else {
            return Ok(torrents);
        };
        path = format!("/torrents?cursor={cursor}");
    }
}

fn write_watch_frame(frame: &str, redraw: bool, first: bool) -> Result<(), CliError> {
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    if redraw {
        output
            .write_all(if first {
                b"\x1b[2J\x1b[H"
            } else {
                b"\x1b[H\x1b[J"
            })
            .map_err(CliError::Terminal)?;
    } else if !first {
        output.write_all(b"\n").map_err(CliError::Terminal)?;
    }
    output
        .write_all(frame.as_bytes())
        .and_then(|()| output.flush())
        .map_err(CliError::Terminal)
}

fn finish_watch(redraw: bool) -> Result<(), CliError> {
    if redraw {
        std::io::stdout()
            .write_all(b"\n")
            .map_err(CliError::Terminal)?;
    }
    Ok(())
}

fn render_torrent_detail(torrent: &TorrentSummary) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "Dendrite torrent watch — Ctrl-C to stop\n");
    let _ = writeln!(output, "{}", torrent.name);
    let _ = writeln!(output, "ID       {}", torrent.id);
    let _ = writeln!(output, "State    {}", state_label(torrent.state));
    let _ = writeln!(
        output,
        "Progress {} {}",
        progress_bar(torrent.downloaded, torrent.total_length, 40),
        format_progress(torrent.downloaded, torrent.total_length)
    );
    let _ = writeln!(
        output,
        "Data     {} / {}",
        format_bytes(torrent.downloaded.min(torrent.total_length)),
        format_bytes(torrent.total_length)
    );
    let _ = writeln!(
        output,
        "Transfer ↓ {}   ↑ {}",
        format_rate(torrent.download_rate),
        format_rate(torrent.upload_rate)
    );
    let _ = writeln!(
        output,
        "Peers    {}   ETA {}",
        torrent.peers,
        format_eta(torrent)
    );
    output
}

fn render_torrent_table(torrents: &[TorrentSummary]) -> String {
    let download_rate = torrents.iter().fold(0_u64, |total, torrent| {
        total.saturating_add(torrent.download_rate)
    });
    let upload_rate = torrents.iter().fold(0_u64, |total, torrent| {
        total.saturating_add(torrent.upload_rate)
    });
    let peers = torrents.iter().fold(0_u64, |total, torrent| {
        total.saturating_add(u64::from(torrent.peers))
    });
    let mut output = String::new();
    let noun = if torrents.len() == 1 {
        "torrent"
    } else {
        "torrents"
    };
    let _ = writeln!(
        output,
        "Dendrite torrent watch — {} {noun} — Ctrl-C to stop",
        torrents.len()
    );
    let _ = writeln!(
        output,
        "Total: ↓ {}   ↑ {}   peers {}\n",
        format_rate(download_rate),
        format_rate(upload_rate),
        peers
    );
    if torrents.is_empty() {
        let _ = writeln!(output, "No torrents.");
        return output;
    }
    let _ = writeln!(
        output,
        "{:<20} {:<11} {:<19} {:>21} {:>12} {:>12} {:>5} {:>9}",
        "NAME", "STATE", "PROGRESS", "DONE / TOTAL", "DOWN", "UP", "PEERS", "ETA"
    );
    for torrent in torrents {
        let size = format!(
            "{} / {}",
            format_bytes(torrent.downloaded.min(torrent.total_length)),
            format_bytes(torrent.total_length)
        );
        let _ = writeln!(
            output,
            "{:<20} {:<11} {:<19} {:>21} {:>12} {:>12} {:>5} {:>9}",
            truncate(&torrent.name, 20),
            state_label(torrent.state),
            format!(
                "{} {}",
                progress_bar(torrent.downloaded, torrent.total_length, 8),
                format_progress(torrent.downloaded, torrent.total_length)
            ),
            size,
            format_rate(torrent.download_rate),
            format_rate(torrent.upload_rate),
            torrent.peers,
            format_eta(torrent)
        );
    }
    output
}

const fn state_label(state: TorrentState) -> &'static str {
    match state {
        TorrentState::Stopped => "stopped",
        TorrentState::Starting => "starting",
        TorrentState::Downloading => "downloading",
        TorrentState::Seeding => "seeding",
        TorrentState::Checking => "checking",
        TorrentState::Error => "error",
        TorrentState::Stopping => "stopping",
    }
}

fn progress_bar(downloaded: u64, total: u64, width: usize) -> String {
    let filled = if total == 0 {
        0
    } else {
        usize::try_from(
            u128::from(downloaded.min(total)) * u128::try_from(width).unwrap_or(u128::MAX)
                / u128::from(total),
        )
        .unwrap_or(width)
        .min(width)
    };
    format!("[{}{}]", "█".repeat(filled), "░".repeat(width - filled))
}

fn format_progress(downloaded: u64, total: u64) -> String {
    if total == 0 {
        return "   --  ".to_owned();
    }
    let hundredths = u128::from(downloaded.min(total)) * 10_000 / u128::from(total);
    format!("{:>3}.{:02}%", hundredths / 100, hundredths % 100)
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 7] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let bytes = u128::from(bytes);
    let mut divisor = 1_024_u128;
    let mut unit = 0_usize;
    while bytes >= divisor * 1_024 && unit < UNITS.len() - 2 {
        divisor *= 1_024;
        unit += 1;
    }
    unit += 1;
    if bytes >= divisor * 100 {
        let rounded = (bytes + divisor / 2) / divisor;
        format!("{rounded} {}", UNITS[unit])
    } else if bytes >= divisor * 10 {
        let tenths = (bytes * 10 + divisor / 2) / divisor;
        format!("{}.{:01} {}", tenths / 10, tenths % 10, UNITS[unit])
    } else {
        let hundredths = (bytes * 100 + divisor / 2) / divisor;
        format!(
            "{}.{:02} {}",
            hundredths / 100,
            hundredths % 100,
            UNITS[unit]
        )
    }
}

fn format_rate(bytes_per_second: u64) -> String {
    format!("{}/s", format_bytes(bytes_per_second))
}

fn format_eta(torrent: &TorrentSummary) -> String {
    if torrent.total_length > 0 && torrent.downloaded >= torrent.total_length {
        return "done".to_owned();
    }
    if torrent.state != TorrentState::Downloading || torrent.download_rate == 0 {
        return "--".to_owned();
    }
    let remaining = torrent.total_length.saturating_sub(torrent.downloaded);
    let seconds = remaining.saturating_add(torrent.download_rate - 1) / torrent.download_rate;
    format_duration(seconds)
}

fn format_duration(seconds: u64) -> String {
    let minutes = seconds / 60;
    let hours = minutes / 60;
    let days = hours / 24;
    if days > 0 {
        format!("{days}d {}h", hours % 24)
    } else if hours > 0 {
        format!("{hours}h {}m", minutes % 60)
    } else if minutes > 0 {
        format!("{minutes}m {}s", seconds % 60)
    } else {
        format!("{seconds}s")
    }
}

fn truncate(value: &str, width: usize) -> String {
    if value.chars().count() <= width {
        return value.to_owned();
    }
    value
        .chars()
        .take(width.saturating_sub(1))
        .chain(std::iter::once('…'))
        .collect()
}

fn persist_token(path: &std::path::Path, token: &str) -> Result<(), std::io::Error> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "token has no parent")
    })?;
    let temporary = parent.join(format!(".dendrite-token-{}.tmp", std::process::id()));
    let result = (|| {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(token.as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        std::fs::File::open(parent)?.sync_all()
    })();
    if result.is_err() {
        let _result_ignored = std::fs::remove_file(temporary);
    }
    result
}

async fn add(
    client: &reqwest::Client,
    base: &str,
    authorization: &HeaderValue,
    source: String,
    start: bool,
) -> Result<TorrentSummary, CliError> {
    let options = AddTorrentOptions {
        start,
        ..AddTorrentOptions::default()
    };
    if source.starts_with("magnet:") {
        return post_json(
            client,
            base,
            "/torrents/magnet",
            authorization,
            &AddTorrentRequest::Magnet {
                uri: source,
                options,
            },
        )
        .await;
    }

    let bytes = std::fs::read(&source).map_err(CliError::Input)?;
    let part = reqwest::multipart::Part::bytes(bytes).file_name("upload.torrent");
    let encoded_options = serde_json::to_string(&options)?;
    let form = reqwest::multipart::Form::new()
        .part("metainfo", part)
        .text("options", encoded_options);
    checked(
        client
            .post(format!("{}/torrents", base.trim_end_matches('/')))
            .header(AUTHORIZATION, authorization)
            .multipart(form)
            .send()
            .await?,
    )
    .await?
    .json()
    .await
    .map_err(CliError::from)
}

async fn print_action(
    client: &reqwest::Client,
    base: &str,
    authorization: &HeaderValue,
    id: &str,
    action: TorrentAction,
) -> Result<(), CliError> {
    let torrent: TorrentSummary = post_json(
        client,
        base,
        &format!("/torrents/{id}/actions"),
        authorization,
        &TorrentActionRequest { action },
    )
    .await?;
    println!("{}", serde_json::to_string_pretty(&torrent)?);
    Ok(())
}

async fn post_json<T: serde::Serialize, R: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    authorization: &HeaderValue,
    body: &T,
) -> Result<R, CliError> {
    checked(
        client
            .post(format!("{}{}", base.trim_end_matches('/'), path))
            .header(AUTHORIZATION, authorization)
            .json(body)
            .send()
            .await?,
    )
    .await?
    .json()
    .await
    .map_err(CliError::from)
}

async fn checked(response: reqwest::Response) -> Result<reqwest::Response, CliError> {
    let status = response.status();
    if !status.is_success() {
        return Err(CliError::Response(status, response.text().await?));
    }
    Ok(response)
}

async fn get<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    authorization: &HeaderValue,
) -> Result<T, CliError> {
    let response = client
        .get(format!("{}{}", base.trim_end_matches('/'), path))
        .header(AUTHORIZATION, authorization)
        .send()
        .await?;
    Ok(checked(response).await?.json().await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dendrite_core::TorrentId;

    #[test]
    fn parses_watch_all_and_individual_commands() {
        let all = Arguments::try_parse_from(["dendritectl", "watch"]);
        assert!(matches!(
            all.map(|arguments| arguments.command),
            Ok(Command::Watch {
                id: None,
                interval: 1,
                no_clear: false
            })
        ));

        let individual = Arguments::try_parse_from([
            "dendritectl",
            "watch",
            "01a05054-eb7e-7da3-9978-a673389fad22",
            "--interval",
            "5",
            "--no-clear",
        ]);
        assert!(matches!(
            individual.map(|arguments| arguments.command),
            Ok(Command::Watch {
                id: Some(id),
                interval: 5,
                no_clear: true
            }) if id == "01a05054-eb7e-7da3-9978-a673389fad22"
        ));
        assert!(Arguments::try_parse_from(["dendritectl", "watch", "--interval", "0"]).is_err());
    }

    #[test]
    fn formats_human_units_progress_and_eta() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(6_278_292), "5.99 MiB");
        assert_eq!(format_rate(20 * 1024 * 1024), "20.0 MiB/s");
        assert_eq!(progress_bar(50, 100, 8), "[████░░░░]");
        assert_eq!(format_progress(1, 4), " 25.00%");
        assert_eq!(format_duration(90_061), "1d 1h");
        assert_eq!(truncate("abcdefghijklmnopqrstuvwxyz", 8), "abcdefg…");
    }

    #[test]
    fn renders_detail_and_aggregate_views() {
        let torrent = fixture_torrent();
        let detail = render_torrent_detail(&torrent);
        assert!(detail.contains("steam2"));
        assert!(detail.contains("25.00%"));
        assert!(detail.contains("↓ 1.00 KiB/s"));
        assert!(detail.contains("ETA 3s"));

        let table = render_torrent_table(&[torrent]);
        assert!(table.contains("1 torrent"));
        assert!(table.contains("Total: ↓ 1.00 KiB/s"));
        assert!(table.contains("DOWN"));
        assert!(table.contains("steam2"));
        assert!(render_torrent_table(&[]).contains("No torrents."));
    }

    fn fixture_torrent() -> TorrentSummary {
        TorrentSummary {
            id: TorrentId::new(),
            name: "steam2".to_owned(),
            state: TorrentState::Downloading,
            v1_info_hash: None,
            v2_info_hash: None,
            total_length: 4_096,
            downloaded: 1_024,
            uploaded: 0,
            download_rate: 1_024,
            upload_rate: 0,
            peers: 51,
        }
    }
}
