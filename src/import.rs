use std::path::Path;

use anyhow::Context;
use chrono::Utc;
use fmmap::tokio::{AsyncMmapFile, AsyncMmapFileExt};
use indicatif::ProgressBar;
use kv::Json;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;

use crate::tables::{VideoHashFromBQ, get_hash_bucket, open_store};

#[derive(Debug, Serialize, Deserialize)]
struct BqDumpFormat {
    video_id: String,
    videohash: String,
    created_at: chrono::DateTime<Utc>,
}

pub async fn import(file: impl AsRef<Path>) -> anyhow::Result<()> {
    let file = AsyncMmapFile::open(file)
        .await
        .context("Couldn't open file")?;

    let store = open_store()?;
    let hash_bucket = get_hash_bucket(&store)?;
    let reader = file
        .reader(0)
        .context("Couldn't create a reader to the file")?;

    let bar = ProgressBar::new_spinner();

    let mut lines = reader.lines();
    let mut counter = 0;
    while let Some(line) = lines.next_line().await.context("Couldn't read line")? {
        let BqDumpFormat {
            video_id,
            videohash,
            created_at,
        }: BqDumpFormat = serde_json::from_str(&line).context("Coulnd't parse line")?;

        let prev_timestamp: Option<_> = hash_bucket.get(&video_id)?.map(|Json(v)| v.timestamp);
        let min_time = prev_timestamp
            .map(|t| t.min(created_at.into()))
            .unwrap_or(created_at.into());
        hash_bucket
            .set(
                &video_id,
                &Json(VideoHashFromBQ {
                    video_hash: videohash,
                    timestamp: min_time,
                }),
            )
            .context("Couldn't insert video id")?;

        counter += 1;

        bar.set_message(format!("imported {counter} ids"));
        bar.tick();
    }

    drop(bar);

    println!("Processed {counter} items");
    println!("Imported {} ids", hash_bucket.len());

    if hash_bucket.len() != counter {
        println!("Counts do not match because bq has duplicate entries for video_id");
    }

    Ok(())
}
