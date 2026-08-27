use std::{io::Write as _, path::PathBuf, process::ExitCode};

use clap::{Parser, Subcommand};
use dendrite_api_types::{
    AddTorrentOptions, AddTorrentRequest, ListResponse, StatusResponse, TokenRotationResponse,
    TorrentAction, TorrentActionRequest, TorrentSummary,
};
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
