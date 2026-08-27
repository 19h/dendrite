use std::process::ExitCode;

use clap::Parser;
use dendrite_simulator::{SimulationConfig, run};

#[derive(Debug, Parser)]
#[command(name = "dendrite-sim", version, about)]
struct Arguments {
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value_t = 1_024)]
    pieces: usize,
    #[arg(long, default_value_t = 32)]
    peers: usize,
    #[arg(long, default_value_t = 1_000_000)]
    maximum_steps: usize,
    #[arg(long, default_value_t = 10)]
    corruption_per_mille: u16,
    #[arg(long, default_value_t = 5)]
    churn_per_mille: u16,
}

fn main() -> ExitCode {
    let arguments = Arguments::parse();
    match run(SimulationConfig {
        seed: arguments.seed,
        pieces: arguments.pieces,
        peers: arguments.peers,
        maximum_steps: arguments.maximum_steps,
        corruption_per_mille: arguments.corruption_per_mille,
        churn_per_mille: arguments.churn_per_mille,
    }) {
        Ok(report) => match serde_json::to_string_pretty(&report) {
            Ok(encoded) => {
                println!("{encoded}");
                if report.complete {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::FAILURE
                }
            }
            Err(error) => {
                eprintln!("failed to encode simulation report: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("simulation failed: {error}");
            ExitCode::FAILURE
        }
    }
}
