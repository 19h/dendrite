use std::{path::PathBuf, process::ExitCode};

use clap::{Parser, Subcommand};
use dendrite_config::Settings;
use tracing::error;

mod doctor;
mod server;

#[derive(Debug, Parser)]
#[command(name = "dendrite", version, about)]
struct Arguments {
    #[arg(long, env = "DENDRITE_CONFIG")]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run,
    Doctor,
}

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = Arguments::parse();
    let settings = match Settings::load(arguments.config.as_deref()) {
        Ok(settings) => settings,
        Err(error) => {
            eprintln!("configuration error: {error}");
            return ExitCode::FAILURE;
        }
    };
    match arguments.command.unwrap_or(Command::Run) {
        Command::Run => {
            initialize_tracing(&settings);
            if let Err(error) = server::run(settings).await {
                error!(%error, "daemon terminated");
                return ExitCode::FAILURE;
            }
        }
        Command::Doctor => {
            let report = doctor::run(&settings).await;
            match serde_json::to_string_pretty(&report) {
                Ok(encoded) => println!("{encoded}"),
                Err(error) => {
                    eprintln!("failed to encode diagnostic report: {error}");
                    return ExitCode::FAILURE;
                }
            }
            if !report.healthy {
                return ExitCode::FAILURE;
            }
        }
    }
    ExitCode::SUCCESS
}

fn initialize_tracing(settings: &Settings) {
    let filter = tracing_subscriber::EnvFilter::try_new(&settings.logging.filter)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("dendrite=info"));
    if settings.logging.json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}
