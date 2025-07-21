use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime},
};

use anyhow::Context;
use chrono::Utc;
use futures::{StreamExt, TryStreamExt, stream};
use kv::Json;
use reqwest_middleware::ClientBuilder;
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use serde::Serialize;

use crate::{
    progress::styled_bar,
    tables::{
        InsertTaskState, VideoHashFromBQ, VideoId, get_hash_bucket, get_task_bucket, open_store,
    },
};

#[derive(Serialize)]
pub struct DedupRequestArgs {
    pub video_id: String,
    pub video_hash: String,
    pub created_at: SystemTime,
}

pub async fn insert_to_qstash(cutoff: chrono::DateTime<Utc>, token: String) -> anyhow::Result<()> {
    let store = open_store()?;
    let video_hashes = get_hash_bucket(&store)?;
    let results = get_task_bucket(&store)?;

    let mut work_items = BTreeMap::new();
    let mut skipped = 0;
    let mut time_cutoff = 0;
    let mut total = 0;
    for hash in video_hashes.iter() {
        total += 1;
        let hash = hash.context("Couldn't read from kv")?;

        let key: VideoId = hash.key()?;
        let value = match results.get(&key)? {
            Some(Json(value)) => value,
            None => Default::default(),
        };

        if matches!(value, InsertTaskState::Inserted) {
            skipped += 1;
            continue;
        }

        let Json(value) = hash.value().expect("this doesn't really fail");

        // every video that _after_ and _including_ cutoff is to be excluded
        if value.timestamp >= cutoff.into() {
            time_cutoff += 1;
            continue;
        }

        work_items.insert(key, value);
    }

    println!("total id = {total}");
    if skipped > 0 {
        println!("but, {skipped} were skipped as being previously inserted.");
    }

    if time_cutoff > 0 {
        println!("and, {time_cutoff} didn't make the cut.");
    }

    if time_cutoff > 0 || skipped > 0 {
        println!("leaving {} to be processed", work_items.len());
    }

    let bar = styled_bar(work_items.len() as u64);
    bar.enable_steady_tick(Duration::from_millis(100));

    // open connectiont to stdb
    // on each callback, map reducer status to insert task state and save
    // when counter hits expected len, regardless of status, send signal over oneshot
    // iterate and call reducer. fire and forget
    // wait on signal

    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(5);
    let client = ClientBuilder::new(reqwest::Client::new())
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();

    stream::iter(work_items)
        .map(anyhow::Ok)
        .try_for_each_concurrent(
            20,
            |(
                video_id,
                VideoHashFromBQ {
                    video_hash,
                    timestamp,
                },
            )| {
                let bar = bar.clone();
                let client = client.clone();
                let results = results.clone();
                let token = token.clone();
                async move {
                    let dest = "https://pr-238-dolr-ai-off-chain-agent.fly.dev/add_to_dedup_index";
                    let id = video_id.clone();
                    results.set(&video_id, &Json(InsertTaskState::Inserting))?;
                    client
                        .post(format!(
                            "https://qstash.upstash.io/v2/enqueue/dedup-index-backfill/{dest}"
                        ))
                        .bearer_auth(token)
                        .header("Upstash-Retry", "5")
                        .header("Upstash-Method", "POST")
                        .header("Upstash-Deduplication-Id", id.as_str())
                        .json(&DedupRequestArgs {
                            video_id: video_id.clone(),
                            video_hash,
                            created_at: timestamp,
                        })
                        .send()
                        .await?
                        .error_for_status()
                        .context("qstash returned an error status")?;

                    results
                        .set(&video_id, &Json(InsertTaskState::Inserted))
                        .expect("yeah this never fails tbh");
                    bar.inc(1);

                    anyhow::Ok(())
                }
            },
        )
        .await
        .context("Couldn't push all items to queue")?;

    bar.finish();
    Ok(())
}
