//! Semaphore-based concurrency control. Acquire a [`HandlerSlot`] before processing;
//! it releases automatically when dropped.
//!
//! # Why two semaphores?
//!
//! The worker pool uses two levels of semaphores: one global, one per concurrency group.
//!
//! The global semaphore caps total concurrency across the entire engine. This
//! protects shared resources like CPU, memory, and database connections.
//!
//! Per-group semaphores let you run multiple handler types in a single pod without
//! one starving the others. For example, if you run both SDLC and Code handlers
//! together, you can give each group a concurrency limit of 4 while keeping the
//! global limit at 6. That way neither group can monopolize all workers.
//!
//! If you only need a global limit, don't configure any concurrency groups and
//! the engine will skip the group semaphore entirely.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use opentelemetry::KeyValue;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info};

use super::metrics::EngineMetrics;
use gkg_server_config::EngineConfiguration;

/// A permit that reserves capacity for one handler execution.
///
/// Holding a permit allows processing one handler execution. The permit is automatically
/// released when dropped, freeing capacity for other handlers.
///
/// The permit may include both a global permit (always) and a group-specific
/// permit (when the handler belongs to a configured concurrency group).
pub struct HandlerSlot {
    _global_permit: OwnedSemaphorePermit,
    _group_permit: Option<OwnedSemaphorePermit>,
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
/// a global semaphore for overall capacity and optional per-group semaphores
/// for finer-grained control.
pub struct WorkerPool {
    global_semaphore: Arc<Semaphore>,
    group_semaphores: HashMap<String, Arc<Semaphore>>,
    metrics: Arc<EngineMetrics>,
}

impl WorkerPool {
    /// Creates a new worker pool from the engine configuration.
    ///
    /// The global semaphore is sized according to `max_concurrent_workers`.
    /// Group semaphores are created for each entry in `concurrency_groups`.
    pub fn new(configuration: &EngineConfiguration, metrics: Arc<EngineMetrics>) -> Self {
        let global_semaphore = Arc::new(Semaphore::new(configuration.max_concurrent_workers));

        let group_semaphores: HashMap<String, Arc<Semaphore>> = configuration
            .concurrency_groups
            .iter()
            .map(|(name, &limit)| (name.clone(), Arc::new(Semaphore::new(limit))))
            .collect();

        info!(
            global_limit = configuration.max_concurrent_workers,
            group_limits = ?group_semaphores.keys().collect::<Vec<_>>(),
            "worker pool created"
        );

        WorkerPool {
            global_semaphore,
            group_semaphores,
            metrics,
        }
    }

    /// Acquires capacity for one handler execution.
    ///
    /// If the handler belongs to a concurrency group, acquires the group permit
    /// first, then the global permit. This prevents a group waiter from reserving
    /// global capacity.
    ///
    /// Returns `None` if the semaphore is closed (which should not happen
    /// during normal operation).
    pub async fn acquire_handler_slot(
        &self,
        concurrency_group: Option<&str>,
    ) -> Option<HandlerSlot> {
        let mut group_permit = None;
        let mut attributes = vec![KeyValue::new("permit_kind", "global")];

        if let Some(group_name) = concurrency_group
            && let Some(semaphore) = self.group_semaphores.get(group_name)
        {
            let group_label = KeyValue::new("group", group_name.to_owned());
            let group_start = Instant::now();
            group_permit = Some(semaphore.clone().acquire_owned().await.ok()?);
            let wait_duration = group_start.elapsed();
            self.metrics.permit_wait_duration.record(
                wait_duration.as_secs_f64(),
                &[KeyValue::new("permit_kind", "group"), group_label.clone()],
            );
            debug!(
                group = group_name,
                wait_ms = wait_duration.as_millis() as u64,
                "group permit acquired"
            );
            attributes.push(KeyValue::new("permit_kind", group_name.to_owned()));
        }

        let global_start = Instant::now();
        let global_permit = self.global_semaphore.clone().acquire_owned().await.ok()?;
        let group_label = concurrency_group
            .map(|g| KeyValue::new("group", g.to_owned()))
            .unwrap_or_else(|| KeyValue::new("group", "none"));
        self.metrics.permit_wait_duration.record(
            global_start.elapsed().as_secs_f64(),
            &[KeyValue::new("permit_kind", "global"), group_label],
        );
        self.metrics
            .active_permits
            .add(1, &[KeyValue::new("permit_kind", "global")]);
        if let Some(group_name) = concurrency_group
            && group_permit.is_some()
        {
            self.metrics
                .active_permits
                .add(1, &[KeyValue::new("permit_kind", group_name.to_owned())]);
        }

        Some(HandlerSlot {
            _global_permit: global_permit,
            _group_permit: group_permit,
            metrics: self.metrics.clone(),
            attributes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn test_metrics() -> Arc<EngineMetrics> {
        Arc::new(EngineMetrics::with_meter(&crate::testkit::test_meter()))
    }

    async fn measure_max_concurrency(
        pool: Arc<WorkerPool>,
        group: Option<&'static str>,
        tasks: usize,
    ) -> usize {
        let active = Arc::new(AtomicUsize::new(0));
        let max_observed = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..tasks)
            .map(|_| {
                let (pool, active, max_obs) = (pool.clone(), active.clone(), max_observed.clone());
                tokio::spawn(async move {
                    let _permit = pool.acquire_handler_slot(group).await;
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

        let max =
            measure_max_concurrency(Arc::new(WorkerPool::new(&config, test_metrics())), None, 10)
                .await;

        assert!(
            max <= 2,
            "Should not exceed global limit of 2, observed: {}",
            max
        );
    }

    #[tokio::test]
    async fn test_group_semaphore_limits_concurrency() {
        let config = EngineConfiguration {
            max_concurrent_workers: 10,
            concurrency_groups: HashMap::from([("limited".into(), 2)]),
            ..Default::default()
        };

        let max = measure_max_concurrency(
            Arc::new(WorkerPool::new(&config, test_metrics())),
            Some("limited"),
            10,
        )
        .await;

        assert!(
            max <= 2,
            "Group limit of 2 should be respected, observed: {}",
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
            let _permit = pool.acquire_handler_slot(None).await;
        }

        assert!(
            tokio::time::timeout(Duration::from_millis(100), pool.acquire_handler_slot(None))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_both_limits_enforced() {
        let config = EngineConfiguration {
            max_concurrent_workers: 3,
            concurrency_groups: HashMap::from([("group-a".into(), 2)]),
            ..Default::default()
        };
        let pool = Arc::new(WorkerPool::new(&config, test_metrics()));

        let group_a_active = Arc::new(AtomicUsize::new(0));
        let group_a_max = Arc::new(AtomicUsize::new(0));
        let global_active = Arc::new(AtomicUsize::new(0));
        let global_max = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();

        // Spawn 5 tasks for group-a (has group limit of 2)
        for _ in 0..5 {
            let pool = pool.clone();
            let group_active = group_a_active.clone();
            let group_max = group_a_max.clone();
            let global_active = global_active.clone();
            let global_max = global_max.clone();

            handles.push(tokio::spawn(async move {
                let _permit = pool.acquire_handler_slot(Some("group-a")).await;

                let current = group_active.fetch_add(1, Ordering::SeqCst) + 1;
                group_max.fetch_max(current, Ordering::SeqCst);
                let current = global_active.fetch_add(1, Ordering::SeqCst) + 1;
                global_max.fetch_max(current, Ordering::SeqCst);

                tokio::time::sleep(Duration::from_millis(50)).await;

                group_active.fetch_sub(1, Ordering::SeqCst);
                global_active.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        // Spawn 5 tasks with no group (only global)
        for _ in 0..5 {
            let pool = pool.clone();
            let global_active = global_active.clone();
            let global_max = global_max.clone();

            handles.push(tokio::spawn(async move {
                let _permit = pool.acquire_handler_slot(None).await;

                let current = global_active.fetch_add(1, Ordering::SeqCst) + 1;
                global_max.fetch_max(current, Ordering::SeqCst);

                tokio::time::sleep(Duration::from_millis(50)).await;

                global_active.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        futures::future::join_all(handles).await;

        assert!(
            group_a_max.load(Ordering::SeqCst) <= 2,
            "Group-a should respect its limit of 2"
        );
        assert!(
            global_max.load(Ordering::SeqCst) <= 3,
            "Global limit should be respected"
        );
    }

    #[tokio::test]
    async fn test_group_waiters_do_not_reserve_global_capacity() {
        let config = EngineConfiguration {
            max_concurrent_workers: 2,
            concurrency_groups: HashMap::from([("group-a".into(), 1)]),
            ..Default::default()
        };
        let pool = Arc::new(WorkerPool::new(&config, test_metrics()));

        let first_group_a = pool
            .acquire_handler_slot(Some("group-a"))
            .await
            .expect("first group-a slot should acquire");

        let pool_for_waiter = pool.clone();
        let group_a_waiter = tokio::spawn(async move {
            let _second_group_a = pool_for_waiter
                .acquire_handler_slot(Some("group-a"))
                .await
                .expect("second group-a slot should eventually acquire");
            tokio::time::sleep(Duration::from_millis(50)).await;
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let no_group_slot =
            tokio::time::timeout(Duration::from_millis(100), pool.acquire_handler_slot(None))
                .await
                .expect("ungrouped handler should not be blocked by group-a waiter")
                .expect("ungrouped handler should receive a global slot");

        drop(no_group_slot);
        drop(first_group_a);
        group_a_waiter.await.expect("waiter task should complete");
    }

    #[tokio::test]
    async fn test_unknown_group_uses_global_only() {
        let config = EngineConfiguration {
            max_concurrent_workers: 2,
            concurrency_groups: HashMap::from([("known".into(), 1)]),
            ..Default::default()
        };

        let max = measure_max_concurrency(
            Arc::new(WorkerPool::new(&config, test_metrics())),
            Some("unknown"),
            5,
        )
        .await;

        assert!(
            max <= 2,
            "Unknown group should use global limit of 2, observed: {}",
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
            let _permit = pool_clone.acquire_handler_slot(None).await;
            panic!("simulated worker failure");
        });

        // Wait for the panic (ignore the JoinError)
        let _ = panicking_task.await;

        // The permit should be released despite the panic, so we can acquire again
        let result =
            tokio::time::timeout(Duration::from_millis(100), pool.acquire_handler_slot(None)).await;

        assert!(
            result.is_ok(),
            "Should acquire permit after panicked task released it"
        );
    }
}
