use std::path::PathBuf;

use check::check_cutoff;
use chrono::Utc;
use clap::{Parser, Subcommand};
use import::import;
use insert::insert_to_qstash;

pub mod progress;
pub mod tables;

mod check;
mod import;
mod insert;

#[derive(clap::Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// imports video details from bigquery new line separated table dump
    Import { file: PathBuf },
    /// inserts from local index to stdb
    Insert {
        /// rfc 3339 preferably, or rfc 2822. Non-inclusive
        #[arg(long, short)]
        cutoff: chrono::DateTime<Utc>,
        /// access token to stdb
        #[arg(long, short)]
        token: String,
    },
    /// check the cutoff for correctness
    ///
    /// prints the videos that will be inserted
    Check {
        /// rfc 3339 preferably, or rfc 2822. Non-inclusive
        #[arg(long, short)]
        cutoff: chrono::DateTime<Utc>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.command {
        Command::Import { file } => import(file).await,
        Command::Insert { cutoff, token } => insert_to_qstash(cutoff, token).await,
        Command::Check { cutoff } => check_cutoff(cutoff).await,
    }
}
