use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

use crate::config::CircuitConfig;
use crate::error::CircuitBreakerError;
use crate::observer::{CircuitBreakerObserver, NoopObserver};
use crate::service::ServiceName;
use crate::state::{AcquireResult, Circuit};

/// Creates [`CircuitBreaker`] handles for each service.
///
/// ```ignore
/// let registry = CircuitBreakerRegistry::new(configs, observer);
/// let db = registry.circuit_breaker(ExternalService::Database);
/// let nats = registry.circuit_breaker(ExternalService::Nats);
/// let handler = Handler::new(db, nats);
/// ```
pub struct CircuitBreakerRegistry {
    circuits: RwLock<HashMap<&'static str, Arc<Circuit>>>,
    observer: Arc<dyn CircuitBreakerObserver>,
}

impl CircuitBreakerRegistry {
    pub fn new(
        configs: HashMap<&'static str, CircuitConfig>,
        observer: Arc<dyn CircuitBreakerObserver>,
    ) -> Self {
        let circuits = configs
            .into_iter()
            .map(|(name, config)| (name, Arc::new(Circuit::new(config))))
            .collect();

        Self {
            circuits: RwLock::new(circuits),
            observer,
        }
    }

    pub fn with_defaults(observer: Arc<dyn CircuitBreakerObserver>) -> Self {
        Self::new(HashMap::new(), observer)
    }

    pub fn without_observer(configs: HashMap<&'static str, CircuitConfig>) -> Self {
        Self::new(configs, Arc::new(NoopObserver))
    }

    /// Returns a [`CircuitBreaker`] handle for the given service.
    /// All handles for the same service share state.
    pub fn circuit_breaker<S: ServiceName>(&self, service: S) -> CircuitBreaker {
        let name = service.as_str();
        CircuitBreaker {
            name,
            circuit: self.get_or_create(name),
            observer: self.observer.clone(),
        }
    }

    /// Returns `true` if the service's circuit is closed, or if no
    /// circuit exists yet.
    pub fn is_available<S: ServiceName>(&self, service: S) -> bool {
        let name = service.as_str();
        let circuits = self.circuits.read();
        circuits
            .get(name)
            .is_none_or(|circuit| circuit.is_available())
    }

    /// Names of all services with non-closed circuits.
    pub fn unavailable_services(&self) -> Vec<&'static str> {
        let circuits = self.circuits.read();
        circuits
            .iter()
            .filter(|(_, circuit)| !circuit.is_available())
            .map(|(name, _)| *name)
            .collect()
    }

    fn get_or_create(&self, name: &'static str) -> Arc<Circuit> {
        {
            let circuits = self.circuits.read();
            if let Some(circuit) = circuits.get(name) {
                return circuit.clone();
            }
        }
        let mut circuits = self.circuits.write();
        circuits
            .entry(name)
            .or_insert_with(|| Arc::new(Circuit::new(CircuitConfig::default())))
            .clone()
    }
}

/// Circuit breaker for a single service. Clone is cheap (`Arc`).
///
/// ```ignore
/// self.db.call(|| async { client.execute(query).await }).await?;
/// ```
#[derive(Clone)]
pub struct CircuitBreaker {
    name: &'static str,
    circuit: Arc<Circuit>,
    observer: Arc<dyn CircuitBreakerObserver>,
}

impl CircuitBreaker {
    /// Run `f` if the circuit is closed. Returns [`CircuitBreakerError::Open`]
    /// without calling `f` if open.
    pub async fn call<F, Fut, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        self.call_with_filter(f, |_| true).await
    }

    /// Like [`call`](Self::call), but `classifier` decides which errors
    /// count as failures. Returns `false` → error passes through without
    /// counting toward the threshold.
    pub async fn call_with_filter<F, Fut, T, E, C>(
        &self,
        f: F,
        classifier: C,
    ) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        C: FnOnce(&E) -> bool,
    {
        if !self.circuit.is_available() {
            match self.circuit.try_acquire(Instant::now()) {
                AcquireResult::Allowed | AcquireResult::Probe => {}
                AcquireResult::Rejected => {
                    self.observer.on_call_rejected(self.name);
                    return Err(CircuitBreakerError::Open { service: self.name });
                }
            }
        }

        let result = f().await;

        match &result {
            Ok(_) => {
                if let Some(transition) = self.circuit.record_success() {
                    self.observer.on_state_change(self.name, transition);
                    self.observer
                        .on_state_snapshot(self.name, self.circuit.state_label());
                }
                self.observer.on_call_success(self.name);
            }
            Err(error) => {
                if classifier(error) {
                    if let Some(transition) = self.circuit.record_failure(Instant::now()) {
                        self.observer.on_state_change(self.name, transition);
                        self.observer
                            .on_state_snapshot(self.name, self.circuit.state_label());
                    }
                    self.observer.on_call_failure(self.name);
                } else {
                    self.observer.on_call_success(self.name);
                }
            }
        }

        result.map_err(CircuitBreakerError::Inner)
    }

    pub fn is_available(&self) -> bool {
        self.circuit.is_available()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    #[derive(Clone, Copy)]
    enum TestService {
        Db,
        Api,
    }

    impl ServiceName for TestService {
        fn as_str(&self) -> &'static str {
            match self {
                Self::Db => "db",
                Self::Api => "api",
            }
        }
    }

    const COOLDOWN: Duration = Duration::from_millis(50);

    fn test_registry() -> CircuitBreakerRegistry {
        let mut configs = HashMap::new();
        configs.insert(
            "db",
            CircuitConfig {
                failure_threshold: 2,
                window: Duration::from_secs(10),
                cooldown: COOLDOWN,
            },
        );
        configs.insert(
            "api",
            CircuitConfig {
                failure_threshold: 3,
                window: Duration::from_secs(10),
                cooldown: COOLDOWN,
            },
        );
        CircuitBreakerRegistry::without_observer(configs)
    }

    fn db(registry: &CircuitBreakerRegistry) -> CircuitBreaker {
        registry.circuit_breaker(TestService::Db)
    }

    async fn trip(breaker: &CircuitBreaker) {
        for _ in 0..2 {
            let _ = breaker.call(|| async { Err::<(), _>("fail") }).await;
        }
    }

    async fn wait_for_cooldown() {
        tokio::time::sleep(COOLDOWN + Duration::from_millis(10)).await;
    }

    #[tokio::test]
    async fn successful_calls_flow_through() {
        let db = db(&test_registry());
        let result = db.call(|| async { Ok::<_, &str>(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn errors_flow_through_as_inner() {
        let db = db(&test_registry());
        let result = db.call(|| async { Err::<i32, _>("boom") }).await;
        assert_eq!(result.unwrap_err().into_inner().unwrap(), "boom");
    }

    #[tokio::test]
    async fn circuit_opens_after_threshold() {
        let db = db(&test_registry());
        trip(&db).await;

        let result = db.call(|| async { Ok::<_, &str>(1) }).await;
        assert!(result.unwrap_err().is_open());
    }

    #[tokio::test]
    async fn open_circuit_does_not_execute_function() {
        let db = db(&test_registry());
        let executed = AtomicU32::new(0);
        trip(&db).await;

        let _ = db
            .call(|| async {
                executed.fetch_add(1, Ordering::Relaxed);
                Ok::<_, &str>(1)
            })
            .await;

        assert_eq!(executed.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn circuits_are_independent() {
        let registry = test_registry();
        let db = registry.circuit_breaker(TestService::Db);
        let api = registry.circuit_breaker(TestService::Api);

        trip(&db).await;

        let result = api.call(|| async { Ok::<_, &str>(1) }).await;
        assert_eq!(result.unwrap(), 1);
    }

    #[tokio::test]
    async fn clones_share_state() {
        let registry = test_registry();
        let db_a = registry.circuit_breaker(TestService::Db);
        let db_b = registry.circuit_breaker(TestService::Db);

        trip(&db_a).await;

        assert!(!db_b.is_available());
        let result = db_b.call(|| async { Ok::<_, &str>(1) }).await;
        assert!(result.unwrap_err().is_open());
    }

    #[tokio::test]
    async fn unconfigured_service_uses_defaults() {
        let registry = test_registry();

        struct Other;
        impl ServiceName for Other {
            fn as_str(&self) -> &'static str {
                "other"
            }
        }

        let other = registry.circuit_breaker(Other);
        let result = other.call(|| async { Ok::<_, &str>(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn classifier_controls_failure_counting() {
        let db = db(&test_registry());

        for _ in 0..5 {
            let result = db
                .call_with_filter(|| async { Err::<i32, _>(404) }, |e| *e >= 500)
                .await;
            assert_eq!(result.unwrap_err().into_inner().unwrap(), 404);
        }

        let result = db.call(|| async { Ok::<_, i32>(1) }).await;
        assert_eq!(result.unwrap(), 1);
    }

    #[tokio::test]
    async fn recovery_after_cooldown() {
        let db = db(&test_registry());
        trip(&db).await;
        assert!(!db.is_available());

        wait_for_cooldown().await;

        let result = db.call(|| async { Ok::<_, &str>(42) }).await;
        assert_eq!(result.unwrap(), 42);
        assert!(db.is_available());
    }

    #[tokio::test]
    async fn unavailable_services_reports_open_circuits() {
        let registry = test_registry();
        let db = registry.circuit_breaker(TestService::Db);
        assert!(registry.unavailable_services().is_empty());

        trip(&db).await;

        assert_eq!(registry.unavailable_services(), vec!["db"]);
        assert!(registry.is_available(TestService::Api));
    }

    #[tokio::test]
    async fn failed_probe_reopens_circuit() {
        let db = db(&test_registry());
        trip(&db).await;
        wait_for_cooldown().await;

        let result = db.call(|| async { Err::<i32, _>("still down") }).await;
        assert_eq!(result.unwrap_err().into_inner().unwrap(), "still down");
        assert!(!db.is_available());

        let executed = AtomicU32::new(0);
        let _ = db
            .call(|| async {
                executed.fetch_add(1, Ordering::Relaxed);
                Ok::<_, &str>(1)
            })
            .await;
        assert_eq!(executed.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn observer_receives_all_callbacks() {
        struct CountingObserver {
            successes: AtomicU32,
            failures: AtomicU32,
            rejections: AtomicU32,
            transitions: AtomicU32,
        }

        impl CircuitBreakerObserver for CountingObserver {
            fn on_call_success(&self, _: &str) {
                self.successes.fetch_add(1, Ordering::Relaxed);
            }
            fn on_call_failure(&self, _: &str) {
                self.failures.fetch_add(1, Ordering::Relaxed);
            }
            fn on_call_rejected(&self, _: &str) {
                self.rejections.fetch_add(1, Ordering::Relaxed);
            }
            fn on_state_change(&self, _: &str, _: crate::state::Transition) {
                self.transitions.fetch_add(1, Ordering::Relaxed);
            }
        }

        let observer = Arc::new(CountingObserver {
            successes: AtomicU32::new(0),
            failures: AtomicU32::new(0),
            rejections: AtomicU32::new(0),
            transitions: AtomicU32::new(0),
        });

        let mut configs = HashMap::new();
        configs.insert(
            "db",
            CircuitConfig {
                failure_threshold: 2,
                window: Duration::from_secs(10),
                cooldown: Duration::from_secs(60),
            },
        );
        let registry = CircuitBreakerRegistry::new(configs, observer.clone());
        let db = registry.circuit_breaker(TestService::Db);

        let _ = db.call(|| async { Ok::<_, &str>(1) }).await;
        assert_eq!(observer.successes.load(Ordering::Relaxed), 1);

        for _ in 0..2 {
            let _ = db.call(|| async { Err::<i32, _>("fail") }).await;
        }
        assert_eq!(observer.failures.load(Ordering::Relaxed), 2);
        assert_eq!(observer.transitions.load(Ordering::Relaxed), 1);

        let _ = db.call(|| async { Ok::<_, &str>(1) }).await;
        assert_eq!(observer.rejections.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn is_available_returns_true_for_unknown_service() {
        let registry = test_registry();

        struct Unknown;
        impl ServiceName for Unknown {
            fn as_str(&self) -> &'static str {
                "unknown"
            }
        }

        assert!(registry.is_available(Unknown));
    }
}
