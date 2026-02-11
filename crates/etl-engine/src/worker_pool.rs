//! Semaphore-based concurrency control. Acquire a [`WorkerPermit`] before processing;
//! it releases automatically when dropped.
//!
//! # Why two semaphores?
//!
//! The worker pool uses two levels of semaphores: one global, one per-module.
//!
//! The global semaphore caps total concurrency across the entire engine. This
//! protects shared resources like CPU, memory, and database connections.
//!
//! Per-module semaphores let you run multiple indexers in a single pod without
//! one starving the others. For example, if you run both a SDLC indexer and
//! a Code Indexer together, you can give each a concurrency limit of 4 while
//! keeping the global limit at 6. That way neither indexer can monopolize all
//! workers, but both can burst up to their limit when the other is idle.
//!
//! If you only need a global limit, don't configure any per-module limits and
//! the engine will skip the module semaphore entirely.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use opentelemetry::KeyValue;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::configuration::EngineConfiguration;
use crate::metrics::EngineMetrics;

/// A permit that reserves worker capacity.
///
/// Holding a permit allows processing a message. The permit is automatically
/// released when dropped, freeing capacity for other handlers.
///
/// The permit may include both a global permit (always) and a module-specific
/// permit (when the module has a configured concurrency limit).
pub struct WorkerPermit {
    _global_permit: OwnedSemaphorePermit,
    _module_permit: Option<OwnedSemaphorePermit>,
    metrics: Arc<EngineMetrics>,
    attributes: Vec<KeyValue>,
}

impl Drop for WorkerPermit {
    fn drop(&mut self) {
        for attr in &self.attributes {
            self.metrics
                .active_permits
                .add(-1, std::slice::from_ref(attr));
        }
    }
}

/// A pool that controls concurrent message processing.
///
/// The worker pool uses semaphores to enforce concurrency limits. It maintains
/// a global semaphore for overall capacity and optional per-module semaphores
/// for finer-grained control.
pub struct WorkerPool {
    global_semaphore: Arc<Semaphore>,
    module_semaphores: HashMap<String, Arc<Semaphore>>,
    metrics: Arc<EngineMetrics>,
}

impl WorkerPool {
    /// Creates a new worker pool from the engine configuration.
    ///
    /// The global semaphore is sized according to `max_concurrent_workers`.
    /// Module semaphores are created only for modules that have
    /// `max_concurrency` configured.
    pub fn new(configuration: &EngineConfiguration, metrics: Arc<EngineMetrics>) -> Self {
        let global_semaphore = Arc::new(Semaphore::new(configuration.max_concurrent_workers));

        let module_semaphores = configuration
            .modules
            .iter()
            .filter_map(|(name, config)| {
                config
                    .max_concurrency
                    .map(|max| (name.clone(), Arc::new(Semaphore::new(max))))
            })
            .collect();

        WorkerPool {
            global_semaphore,
            module_semaphores,
            metrics,
        }
    }

    /// Acquires a permit for processing a message from the given module.
    ///
    /// This method blocks until capacity is available. It acquires the global
    /// permit first, then the module-specific permit if configured.
    ///
    /// Returns `None` if the semaphore is closed (which should not happen
    /// during normal operation).
    ///
    /// # Arguments
    ///
    /// * `module_name` - The name of the module requesting capacity
    pub async fn acquire(&self, module_name: &str) -> Option<WorkerPermit> {
        let global_start = Instant::now();
        let global_permit = self.global_semaphore.clone().acquire_owned().await.ok()?;
        self.metrics.permit_wait_duration.record(
            global_start.elapsed().as_secs_f64(),
            &[
                KeyValue::new("scope", "global"),
                KeyValue::new("module", module_name.to_owned()),
            ],
        );

        let mut attributes = vec![KeyValue::new("scope", "global")];
        self.metrics
            .active_permits
            .add(1, &[KeyValue::new("scope", "global")]);

        let module_permit = if let Some(semaphore) = self.module_semaphores.get(module_name) {
            let module_start = Instant::now();
            let permit = semaphore.clone().acquire_owned().await.ok()?;
            self.metrics.permit_wait_duration.record(
                module_start.elapsed().as_secs_f64(),
                &[
                    KeyValue::new("scope", "module"),
                    KeyValue::new("module", module_name.to_owned()),
                ],
            );
            self.metrics
                .active_permits
                .add(1, &[KeyValue::new("scope", module_name.to_owned())]);
            attributes.push(KeyValue::new("scope", module_name.to_owned()));
            Some(permit)
        } else {
            None
        };

        Some(WorkerPermit {
            _global_permit: global_permit,
            _module_permit: module_permit,
            metrics: self.metrics.clone(),
            attributes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::ModuleConfiguration;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn test_metrics() -> Arc<EngineMetrics> {
        Arc::new(EngineMetrics::new())
    }

    async fn measure_max_concurrency(
        pool: Arc<WorkerPool>,
        module: &'static str,
        tasks: usize,
    ) -> usize {
        let active = Arc::new(AtomicUsize::new(0));
        let max_observed = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..tasks)
            .map(|_| {
                let (pool, active, max_obs) = (pool.clone(), active.clone(), max_observed.clone());
                tokio::spawn(async move {
                    let _permit = pool.acquire(module).await;
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_obs.fetch_max(current, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                })
            })
            .collect();

        futures::future::join_all(handles).await;
        max_observed.load(Ordering::SeqCst)
    }

    #[tokio::test]
    async fn test_global_semaphore_limits_concurrent_workers() {
        let config = EngineConfiguration {
            max_concurrent_workers: 2,
            ..Default::default()
        };

        let max = measure_max_concurrency(
            Arc::new(WorkerPool::new(&config, test_metrics())),
            "any",
            10,
        )
        .await;

        assert!(
            max <= 2,
            "Should not exceed global limit of 2, observed: {}",
            max
        );
    }

    #[tokio::test]
    async fn test_module_semaphore_limits_concurrency() {
        let mut config = EngineConfiguration {
            max_concurrent_workers: 10,
            ..Default::default()
        };
        config.modules.insert(
            "limited".into(),
            ModuleConfiguration {
                max_concurrency: Some(2),
            },
        );

        let max = measure_max_concurrency(
            Arc::new(WorkerPool::new(&config, test_metrics())),
            "limited",
            10,
        )
        .await;

        assert!(
            max <= 2,
            "Module limit of 2 should be respected, observed: {}",
            max
        );
    }

    #[tokio::test]
    async fn test_permit_drop_releases_capacity() {
        let pool = Arc::new(WorkerPool::new(
            &EngineConfiguration {
                max_concurrent_workers: 1,
                ..Default::default()
            },
            test_metrics(),
        ));

        {
            let _permit = pool.acquire("test").await;
        }

        assert!(
            tokio::time::timeout(Duration::from_millis(100), pool.acquire("test"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_both_limits_enforced() {
        // Setup: global limit of 3, module-a limit of 2, module-b has no limit
        let mut config = EngineConfiguration {
            max_concurrent_workers: 3,
            ..Default::default()
        };
        config.modules.insert(
            "module-a".into(),
            ModuleConfiguration {
                max_concurrency: Some(2),
            },
        );
        let pool = Arc::new(WorkerPool::new(&config, test_metrics()));

        let module_a_active = Arc::new(AtomicUsize::new(0));
        let module_a_max = Arc::new(AtomicUsize::new(0));
        let global_active = Arc::new(AtomicUsize::new(0));
        let global_max = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();

        // Spawn 5 tasks for module-a (has module limit of 2)
        for _ in 0..5 {
            let pool = pool.clone();
            let module_active = module_a_active.clone();
            let module_max = module_a_max.clone();
            let global_active = global_active.clone();
            let global_max = global_max.clone();

            handles.push(tokio::spawn(async move {
                let _permit = pool.acquire("module-a").await;

                let current = module_active.fetch_add(1, Ordering::SeqCst) + 1;
                module_max.fetch_max(current, Ordering::SeqCst);
                let current = global_active.fetch_add(1, Ordering::SeqCst) + 1;
                global_max.fetch_max(current, Ordering::SeqCst);

                tokio::time::sleep(Duration::from_millis(50)).await;

                module_active.fetch_sub(1, Ordering::SeqCst);
                global_active.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        // Spawn 5 tasks for module-b (no module limit, only global)
        for _ in 0..5 {
            let pool = pool.clone();
            let global_active = global_active.clone();
            let global_max = global_max.clone();

            handles.push(tokio::spawn(async move {
                let _permit = pool.acquire("module-b").await;

                let current = global_active.fetch_add(1, Ordering::SeqCst) + 1;
                global_max.fetch_max(current, Ordering::SeqCst);

                tokio::time::sleep(Duration::from_millis(50)).await;

                global_active.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        futures::future::join_all(handles).await;

        assert!(
            module_a_max.load(Ordering::SeqCst) <= 2,
            "Module-a should respect its limit of 2"
        );
        assert!(
            global_max.load(Ordering::SeqCst) <= 3,
            "Global limit should be respected"
        );
    }

    #[tokio::test]
    async fn test_unknown_module_uses_global_only() {
        let mut config = EngineConfiguration {
            max_concurrent_workers: 2,
            ..Default::default()
        };
        config.modules.insert(
            "known".into(),
            ModuleConfiguration {
                max_concurrency: Some(1),
            },
        );

        let max = measure_max_concurrency(
            Arc::new(WorkerPool::new(&config, test_metrics())),
            "unknown",
            5,
        )
        .await;

        assert!(
            max <= 2,
            "Unknown module should use global limit of 2, observed: {}",
            max
        );
    }

    #[tokio::test]
    async fn test_permit_released_when_worker_panics() {
        let pool = Arc::new(WorkerPool::new(
            &EngineConfiguration {
                max_concurrent_workers: 1,
                ..Default::default()
            },
            test_metrics(),
        ));

        let pool_clone = pool.clone();
        let panicking_task = tokio::spawn(async move {
            let _permit = pool_clone.acquire("test").await;
            panic!("simulated worker failure");
        });

        // Wait for the panic (ignore the JoinError)
        let _ = panicking_task.await;

        // The permit should be released despite the panic, so we can acquire again
        let result = tokio::time::timeout(Duration::from_millis(100), pool.acquire("test")).await;

        assert!(
            result.is_ok(),
            "Should acquire permit after panicked task released it"
        );
    }
}
