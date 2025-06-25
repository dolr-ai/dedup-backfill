use std::{collections::BTreeMap, time::UNIX_EPOCH};

use anyhow::Context;
use chrono::Utc;
use kv::Json;

use crate::tables::{VideoId, get_hash_bucket, open_store};

pub async fn check_cutoff(cutoff: chrono::DateTime<Utc>) -> anyhow::Result<()> {
    let store = open_store()?;
    let video_hashes = get_hash_bucket(&store)?;

    let mut work_items = BTreeMap::new();
    let mut skipped = 0;
    let mut total = 0;
    for hash in video_hashes.iter() {
        total += 1;
        let hash = hash.context("Couldn't read from kv")?;

        let key: VideoId = hash.key()?;

        let Json(value) = hash.value().expect("this doesn't really fail");

        // every video that _after_ and _including_ cutoff is to be excluded
        if value.timestamp >= cutoff.into() {
            skipped += 1;
            continue;
        }

        work_items.insert(key, value);
    }

    println!("total videos from bq: {total}");
    println!("number of videos that didn't make the cut: {skipped}");
    println!(
        "number of videos that will be inserted: {}",
        work_items.len()
    );

    let mut top_timestamp: Vec<_> = work_items.into_values().map(|v| v.timestamp).collect();
    top_timestamp.sort();

    let top_timestamp: Vec<_> = top_timestamp
        .into_iter()
        .rev()
        .take(10)
        .map(|t| {
            chrono::DateTime::from_timestamp_nanos(
                t.duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64
            )
            .to_rfc3339()
        })
        .collect();

    println!("top 10 timestamp (desc order): {top_timestamp:#?}");

    Ok(())
}
