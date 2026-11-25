use std::time::Duration;

use anyhow::anyhow;
use async_nats::jetstream::context::GetStreamErrorKind;
use async_nats::jetstream::kv;
use async_nats::jetstream::stream::{self, RetentionPolicy, Stream};
use async_nats::jetstream::{self, ErrorCode};

use super::NatsSettings;
use crate::lane::LaneId;

const TASK_STREAM: &str = "engine_tasks";
const TASK_SUBJECTS: &str = "orbit.engine.task.>";
const STATE_BUCKET: &str = "engine_state";

pub(super) struct OpenResources {
    pub(super) stream: Stream,
    pub(super) bucket: kv::Store,
}

pub(super) async fn open_resources(
    jetstream: &jetstream::Context,
    settings: &NatsSettings,
) -> anyhow::Result<OpenResources> {
    let stream = jetstream
        .get_or_create_stream(stream::Config {
            name: TASK_STREAM.to_owned(),
            subjects: vec![TASK_SUBJECTS.to_owned()],
            retention: RetentionPolicy::WorkQueue,
            num_replicas: settings.replicas,
            ..Default::default()
        })
        .await?;
    let bucket = jetstream
        .create_key_value(kv::Config {
            bucket: STATE_BUCKET.to_owned(),
            history: 1,
            num_replicas: settings.replicas,
            ..Default::default()
        })
        .await?;
    Ok(OpenResources { stream, bucket })
}

pub(super) fn lane_subject(lane: &LaneId) -> String {
    format!("orbit.engine.task.{}.{}", lane.worker(), lane.lane())
}

pub(super) fn consumer_name(lane: &LaneId) -> String {
    format!("{}_{}", lane.worker(), lane.lane()).replace('.', "_")
}

pub(super) async fn wait_until_stream_is_gone(
    jetstream: &jetstream::Context,
    every: Duration,
) -> anyhow::Error {
    loop {
        tokio::time::sleep(every).await;
        let Err(error) = jetstream.get_stream(TASK_STREAM).await else {
            continue;
        };
        if let GetStreamErrorKind::JetStream(error) = error.kind()
            && error.error_code() == ErrorCode::STREAM_NOT_FOUND
        {
            return anyhow!(
                "the task stream of NATS is gone, so this process stops and a restart creates it again"
            );
        }
    }
}
