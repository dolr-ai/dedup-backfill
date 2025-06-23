use std::path::PathBuf;

use chrono::Utc;
use clap::{Parser, Subcommand};
use import::import;
use insert::insert_to_stdb;

pub mod progress;
pub mod tables;

mod import;
mod insert;

#[derive(clap::Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Import {
        file: PathBuf,
    },
    Insert {
        /// rfc 3339 preferably, or rfc 2822. Non-inclusive
        cutoff: chrono::DateTime<Utc>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.command {
        Command::Import { file } => import(file).await,
        Command::Insert { cutoff } => insert_to_stdb(cutoff).await,
    }
}
