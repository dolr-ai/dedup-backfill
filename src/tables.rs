use std::time::SystemTime;

use anyhow::Context;
use kv::{Bucket, Json};
use serde::{Serialize, de::DeserializeOwned};

pub type VideoId = String;

const VIDEO_HASH_FROM_BQ: &str = "video_hash_from_bq";
const INSERT_TASK_STATE: &str = "insert_task_state";

pub fn open_store() -> anyhow::Result<kv::Store> {
    let config = kv::Config::new("./store");

    kv::Store::new(config).context("Couldn't open the store")
}

#[inline]
fn get_bucket_inner<'store, T: DeserializeOwned + Serialize>(
    store: &'store kv::Store,
    name: &str,
) -> anyhow::Result<Bucket<'store, VideoId, Json<T>>> {
    store.bucket(Some(name)).context("Couldn't get the bucket")
}

macro_rules! get_bucket_fn {
    ($fn_name:ident, $value:ty, $bucket_name:expr) => {
        pub fn $fn_name(store: &kv::Store) -> anyhow::Result<Bucket<'_, VideoId, Json<$value>>> {
            get_bucket_inner(store, $bucket_name)
        }
    };
}

get_bucket_fn!(get_hash_bucket, VideoHashFromBQ, VIDEO_HASH_FROM_BQ);
get_bucket_fn!(get_task_bucket, InsertTaskState, INSERT_TASK_STATE);

#[derive(serde::Deserialize, serde::Serialize)]
pub struct VideoHashFromBQ {
    pub video_hash: String,
    pub timestamp: SystemTime,
}

#[derive(serde::Deserialize, serde::Serialize, Default, Clone, Copy, Debug)]
pub enum InsertTaskState {
    #[default]
    ToBeInserted,
    Inserting,
    Inserted,
}
