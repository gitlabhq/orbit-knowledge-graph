use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::{Future, IntoFuture};
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::anyhow;
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::PoolConfig;
use crate::worker::TaskError;

pub struct PoolRequest<'a> {
    pools: &'a Pools,
    name: String,
    key: Option<String>,
}

pub struct Permit {
    _held: OwnedSemaphorePermit,
}

pub(crate) struct Pools {
    registered: BTreeSet<&'static str>,
    sizes: BTreeMap<String, PoolConfig>,
    semaphores: Mutex<HashMap<PoolSlot, Arc<Semaphore>>>,
}

#[derive(PartialEq, Eq, Hash)]
struct PoolSlot {
    pool: String,
    key: Option<String>,
}

impl<'a> PoolRequest<'a> {
    pub(crate) fn new(pools: &'a Pools, name: &str) -> Self {
        Self {
            pools,
            name: name.to_owned(),
            key: None,
        }
    }

    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }
}

impl<'a> IntoFuture for PoolRequest<'a> {
    type Output = Result<Permit, TaskError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.pools.acquire(self.name, self.key).await })
    }
}

impl Pools {
    pub(crate) fn new(
        registered: BTreeSet<&'static str>,
        sizes: BTreeMap<String, PoolConfig>,
    ) -> Self {
        Self {
            registered,
            sizes,
            semaphores: Mutex::new(HashMap::new()),
        }
    }

    async fn acquire(&self, name: String, key: Option<String>) -> Result<Permit, TaskError> {
        if !self.registered.contains(name.as_str()) {
            return Err(TaskError::permanent(anyhow!(
                "no module registered the pool {name}"
            )));
        }
        let Some(size) = self.sizes.get(&name) else {
            return Err(TaskError::permanent(anyhow!(
                "the config of this pod gives no size to the pool {name}"
            )));
        };
        match size {
            PoolConfig::Pod(limit) => {
                let semaphore = self.find_or_create_semaphore(name, None, *limit);
                let held = semaphore
                    .acquire_owned()
                    .await
                    .map_err(TaskError::permanent)?;
                Ok(Permit { _held: held })
            }
            PoolConfig::Cluster(cluster) => {
                let limit = if key.is_some() {
                    cluster.per_key
                } else {
                    cluster.unknown_key
                };
                let semaphore = self.find_or_create_semaphore(name, key, limit);
                match tokio::time::timeout(cluster.max_wait, semaphore.acquire_owned()).await {
                    Ok(held) => Ok(Permit {
                        _held: held.map_err(TaskError::permanent)?,
                    }),
                    Err(_) => Err(TaskError::defer(cluster.max_wait, "pool_wait")),
                }
            }
        }
    }

    fn find_or_create_semaphore(
        &self,
        name: String,
        key: Option<String>,
        limit: NonZeroU32,
    ) -> Arc<Semaphore> {
        let mut semaphores = self.semaphores.lock();
        let semaphore = semaphores
            .entry(PoolSlot { pool: name, key })
            .or_insert_with(|| Arc::new(Semaphore::new(limit.get() as usize)));
        Arc::clone(semaphore)
    }
}
