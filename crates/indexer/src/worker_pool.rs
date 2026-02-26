//! Semaphore-based concurrency control. Acquire a [`HandlerSlot`] before processing;
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
use tracing::{debug, info};

use crate::configuration::EngineConfiguration;
use crate::metrics::EngineMetrics;

/// A permit that reserves capacity for one handler execution.
///
/// Holding a permit allows processing one handler execution. The permit is automatically
/// released when dropped, freeing capacity for other handlers.
///
/// The permit may include both a global permit (always) and a module-specific
/// permit (when the module has a configured concurrency limit).
pub struct HandlerSlot {
    _global_permit: OwnedSemaphorePermit,
    _module_permit: Option<OwnedSemaphorePermit>,
    metrics: Arc<EngineMetrics>,
    attributes: Vec<KeyValue>,
}

impl Drop for HandlerSlot {
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

        let module_semaphores: HashMap<String, Arc<Semaphore>> = configuration
            .modules
            .iter()
            .filter_map(|(name, config)| {
                config
                    .max_concurrency
                    .map(|max| (name.clone(), Arc::new(Semaphore::new(max))))
            })
            .collect();

        info!(
            global_limit = configuration.max_concurrent_workers,
            module_limits = ?module_semaphores.keys().collect::<Vec<_>>(),
            "worker pool created"
        );

        WorkerPool {
            global_semaphore,
            module_semaphores,
            metrics,
        }
    }

    /// Acquires capacity for one handler execution in the given module.
    ///
    /// This method blocks until capacity is available. It acquires the
    /// module-specific permit first (when configured), then the global permit.
    ///
    /// Returns `None` if the semaphore is closed (which should not happen
    /// during normal operation).
    ///
    /// # Arguments
    ///
    /// * `module_name` - The name of the module requesting capacity
    pub async fn acquire_handler_slot(&self, module_name: &str) -> Option<HandlerSlot> {
        let mut module_permit = None;
        let module_label = KeyValue::new("module", module_name.to_owned());
        let mut attributes = vec![KeyValue::new("permit_kind", "global")];

        if let Some(semaphore) = self.module_semaphores.get(module_name) {
            let module_start = Instant::now();
            module_permit = Some(semaphore.clone().acquire_owned().await.ok()?);
            let wait_duration = module_start.elapsed();
            self.metrics.permit_wait_duration.record(
                wait_duration.as_secs_f64(),
                &[KeyValue::new("permit_kind", "module"), module_label.clone()],
            );
            debug!(
                module = module_name,
                wait_ms = wait_duration.as_millis() as u64,
                "module permit acquired"
            );
            attributes.push(module_label.clone());
        }

        let global_start = Instant::now();
        let global_permit = self.global_semaphore.clone().acquire_owned().await.ok()?;
        self.metrics.permit_wait_duration.record(
            global_start.elapsed().as_secs_f64(),
            &[KeyValue::new("permit_kind", "global"), module_label],
        );
        self.metrics
            .active_permits
            .add(1, &[KeyValue::new("permit_kind", "global")]);
        if module_permit.is_some() {
            self.metrics
                .active_permits
                .add(1, &[KeyValue::new("permit_kind", module_name.to_owned())]);
        }

        Some(HandlerSlot {
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
                    let _permit = pool.acquire_handler_slot(module).await;
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
                ..Default::default()
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
            let _permit = pool.acquire_handler_slot("test").await;
        }

        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                pool.acquire_handler_slot("test")
            )
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
                ..Default::default()
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
                let _permit = pool.acquire_handler_slot("module-a").await;

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
                let _permit = pool.acquire_handler_slot("module-b").await;

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
    async fn test_module_waiters_do_not_reserve_global_capacity() {
        let mut config = EngineConfiguration {
            max_concurrent_workers: 2,
            ..Default::default()
        };
        config.modules.insert(
            "module-a".into(),
            ModuleConfiguration {
                max_concurrency: Some(1),
                ..Default::default()
            },
        );
        let pool = Arc::new(WorkerPool::new(&config, test_metrics()));

        let first_module_a = pool
            .acquire_handler_slot("module-a")
            .await
            .expect("first module-a slot should acquire");

        let pool_for_waiter = pool.clone();
        let module_a_waiter = tokio::spawn(async move {
            let _second_module_a = pool_for_waiter
                .acquire_handler_slot("module-a")
                .await
                .expect("second module-a slot should eventually acquire");
            tokio::time::sleep(Duration::from_millis(50)).await;
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let module_b_slot = tokio::time::timeout(
            Duration::from_millis(100),
            pool.acquire_handler_slot("module-b"),
        )
        .await
        .expect("module-b should not be blocked by module-a waiter")
        .expect("module-b should receive a global slot");

        drop(module_b_slot);
        drop(first_module_a);
        module_a_waiter.await.expect("waiter task should complete");
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
                ..Default::default()
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
            let _permit = pool_clone.acquire_handler_slot("test").await;
            panic!("simulated worker failure");
        });

        // Wait for the panic (ignore the JoinError)
        let _ = panicking_task.await;

        // The permit should be released despite the panic, so we can acquire again
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            pool.acquire_handler_slot("test"),
        )
        .await;

        assert!(
            result.is_ok(),
            "Should acquire permit after panicked task released it"
        );
    }
}
