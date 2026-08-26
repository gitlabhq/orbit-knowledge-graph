//! A `NatsServices` that counts publishes instead of sending them.
//!
//! The testkit mock keeps every published envelope, which at a million
//! dispatches would dominate the very measurement being taken.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use indexer::nats::{NatsMessage, NatsServices};
use indexer::types::{Envelope, Subscription};
use nats_client::{KvEntry, KvPutOptions, KvPutResult, NatsError};

/// Reverses the seeder's project-to-namespace mapping, so a published subject
/// can be attributed without the request payload being parsed.
pub struct Layout {
    pub first_project_id: i64,
    pub namespaces: u64,
    pub big_namespace_projects: u64,
}

impl Layout {
    fn namespace_of(&self, project_id: i64) -> usize {
        let index = (project_id - self.first_project_id).max(0) as u64;
        if self.big_namespace_projects == 0 {
            return (index % self.namespaces.max(1)) as usize;
        }
        if index < self.big_namespace_projects {
            return 0;
        }
        let spread = (self.namespaces - 1).max(1);
        (1 + ((index - self.big_namespace_projects) % spread)) as usize
    }
}

/// Publish order per namespace: where each namespace first appears in the queue
/// and how long a single namespace holds it uninterrupted. These are the two
/// numbers the fleet-wide shuffle was introduced to move.
#[derive(serde::Serialize)]
pub struct PublishOrder {
    pub max_same_namespace_run: u64,
    pub first_publish_index: Vec<i64>,
    pub published_per_namespace: Vec<u64>,
}

struct OrderProbe {
    layout: Layout,
    first_publish_index: Vec<i64>,
    published_per_namespace: Vec<u64>,
    last_namespace: Option<usize>,
    run: u64,
    max_run: u64,
}

impl OrderProbe {
    fn record(&mut self, subject: &str, index: u64) {
        let Some(project_id) = subject
            .rsplit('.')
            .nth(1)
            .and_then(|id| id.parse::<i64>().ok())
        else {
            return;
        };
        let namespace = self.layout.namespace_of(project_id);
        if namespace >= self.published_per_namespace.len() {
            return;
        }
        if self.first_publish_index[namespace] < 0 {
            self.first_publish_index[namespace] = index as i64;
        }
        self.published_per_namespace[namespace] += 1;
        match self.last_namespace {
            Some(previous) if previous == namespace => self.run += 1,
            _ => self.run = 1,
        }
        self.last_namespace = Some(namespace);
        self.max_run = self.max_run.max(self.run);
    }
}

pub struct CountingNats {
    published: AtomicU64,
    published_bytes: AtomicU64,
    publish_delay: Option<std::time::Duration>,
    order: Option<Mutex<OrderProbe>>,
}

impl CountingNats {
    pub fn new(publish_delay: Option<std::time::Duration>, layout: Option<Layout>) -> Self {
        let namespaces = layout.as_ref().map_or(0, |l| l.namespaces as usize);
        Self {
            published: AtomicU64::new(0),
            published_bytes: AtomicU64::new(0),
            publish_delay,
            order: layout.map(|layout| {
                Mutex::new(OrderProbe {
                    layout,
                    first_publish_index: vec![-1; namespaces],
                    published_per_namespace: vec![0; namespaces],
                    last_namespace: None,
                    run: 0,
                    max_run: 0,
                })
            }),
        }
    }

    pub fn publish_order(&self) -> Option<PublishOrder> {
        let probe = self.order.as_ref()?.lock().ok()?;
        Some(PublishOrder {
            max_same_namespace_run: probe.max_run,
            first_publish_index: probe.first_publish_index.clone(),
            published_per_namespace: probe.published_per_namespace.clone(),
        })
    }

    pub fn published(&self) -> u64 {
        self.published.load(Ordering::Relaxed)
    }

    pub fn published_bytes(&self) -> u64 {
        self.published_bytes.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl NatsServices for CountingNats {
    async fn publish(
        &self,
        subscription: &Subscription,
        envelope: &Envelope,
    ) -> Result<(), NatsError> {
        let index = self.published.fetch_add(1, Ordering::Relaxed);
        if let Some(order) = &self.order
            && let Ok(mut probe) = order.lock()
        {
            probe.record(&subscription.subject, index);
        }
        self.published_bytes
            .fetch_add(envelope.payload.len() as u64, Ordering::Relaxed);
        if let Some(delay) = self.publish_delay {
            tokio::time::sleep(delay).await;
        }
        Ok(())
    }

    async fn kv_get(&self, _bucket: &str, _key: &str) -> Result<Option<KvEntry>, NatsError> {
        Ok(None)
    }

    async fn kv_put(
        &self,
        _bucket: &str,
        _key: &str,
        _value: Bytes,
        _options: KvPutOptions,
    ) -> Result<KvPutResult, NatsError> {
        Ok(KvPutResult::Success(1))
    }

    async fn kv_delete(&self, _bucket: &str, _key: &str) -> Result<(), NatsError> {
        Ok(())
    }

    async fn kv_keys(&self, _bucket: &str) -> Result<Vec<String>, NatsError> {
        Ok(Vec::new())
    }

    async fn consume_pending(
        &self,
        _subscription: &Subscription,
        _batch_size: usize,
    ) -> Result<Vec<NatsMessage>, NatsError> {
        Ok(Vec::new())
    }
}
