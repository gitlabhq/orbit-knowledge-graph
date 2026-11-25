use std::collections::HashMap;
use std::time::Duration;

use anyhow::anyhow;
use async_nats::jetstream::consumer::{AckPolicy, PullConsumer, pull};
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::stream::Stream;
use async_nats::jetstream::{self, AckKind, Message};
use async_trait::async_trait;
use futures::StreamExt;
use parking_lot::Mutex;

use super::NatsSettings;
use super::resources::{consumer_name, lane_subject, wait_until_stream_is_gone};
use crate::lane::LaneId;
use crate::task::{Delivery, InFlight, TaskMessage, TaskQueue};

pub(super) struct NatsTaskQueue {
    jetstream: jetstream::Context,
    stream: Stream,
    ack_wait: Duration,
    idle_poll: Duration,
    stream_check: Duration,
    consumers: Mutex<HashMap<LaneId, PullConsumer>>,
}

struct NatsInFlight {
    message: Message,
}

impl NatsTaskQueue {
    pub(super) fn new(
        jetstream: jetstream::Context,
        stream: Stream,
        settings: &NatsSettings,
    ) -> Self {
        Self {
            jetstream,
            stream,
            ack_wait: settings.ack_wait,
            idle_poll: settings.idle_poll,
            stream_check: settings.stream_check,
            consumers: Mutex::new(HashMap::new()),
        }
    }

    async fn find_or_create_consumer(&self, lane: &LaneId) -> anyhow::Result<PullConsumer> {
        if let Some(consumer) = self.consumers.lock().get(lane) {
            return Ok(consumer.clone());
        }
        let durable = consumer_name(lane);
        let config = pull::Config {
            durable_name: Some(durable.clone()),
            filter_subject: lane_subject(lane),
            ack_policy: AckPolicy::Explicit,
            ack_wait: self.ack_wait,
            ..Default::default()
        };
        let consumer = self.stream.get_or_create_consumer(&durable, config).await?;
        self.consumers.lock().insert(lane.clone(), consumer.clone());
        Ok(consumer)
    }
}

#[async_trait]
impl TaskQueue for NatsTaskQueue {
    async fn publish(&self, message: TaskMessage) -> anyhow::Result<()> {
        let lane = LaneId::new(message.worker.clone(), message.lane.clone());
        let payload = serde_json::to_vec(&message)?;
        let publish = PublishMessage::build()
            .payload(payload.into())
            .message_id(message.task.as_uuid().to_string());
        self.jetstream
            .send_publish(lane_subject(&lane), publish)
            .await?
            .await?;
        Ok(())
    }

    async fn pull(&self, lane: &LaneId, max: usize) -> anyhow::Result<Vec<Delivery>> {
        let consumer = self.find_or_create_consumer(lane).await?;
        let mut batch = consumer
            .fetch()
            .max_messages(max)
            .messages()
            .await
            .map_err(|error| anyhow!(error))?;
        let mut deliveries = Vec::new();
        while let Some(message) = batch.next().await {
            let message = message.map_err(|error| anyhow!(error))?;
            let task = match serde_json::from_slice::<TaskMessage>(&message.payload) {
                Ok(task) => task,
                Err(error) => {
                    message
                        .ack_with(AckKind::Term)
                        .await
                        .map_err(|error| anyhow!(error))?;
                    return Err(anyhow!(
                        "the lane {lane} held a message that is no task: {error}"
                    ));
                }
            };
            deliveries.push(Delivery {
                message: task,
                in_flight: Box::new(NatsInFlight { message }),
            });
        }
        Ok(deliveries)
    }

    async fn wait_for_ready(&self, _lane: &LaneId) {
        tokio::time::sleep(self.idle_poll).await;
    }

    async fn wait_until_closed(&self) -> anyhow::Error {
        wait_until_stream_is_gone(&self.jetstream, self.stream_check).await
    }
}

#[async_trait]
impl InFlight for NatsInFlight {
    async fn ack(&self) -> anyhow::Result<()> {
        self.message
            .double_ack()
            .await
            .map_err(|error| anyhow!(error))
    }

    async fn ack_progress(&self) -> anyhow::Result<()> {
        self.message
            .ack_with(AckKind::Progress)
            .await
            .map_err(|error| anyhow!(error))
    }

    async fn nack(&self, delay: Duration) -> anyhow::Result<()> {
        self.message
            .ack_with(AckKind::Nak(Some(delay)))
            .await
            .map_err(|error| anyhow!(error))
    }
}
