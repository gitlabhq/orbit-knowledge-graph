use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use crate::classifiable::CircuitBreakableError;
use crate::config::CircuitConfig;
use crate::error::CircuitBreakerError;
use crate::observer::CircuitBreakerObserver;
use crate::state::{AcquireResult, Circuit};

/// Circuit breaker for a single service. Clones share one circuit (`Arc`).
///
/// ```text
/// self.db.call(|| async { client.execute(query).await }).await?;
/// ```
#[derive(Clone)]
pub struct CircuitBreaker {
    name: &'static str,
    circuit: Arc<Circuit>,
    observer: Arc<dyn CircuitBreakerObserver>,
}

impl CircuitBreaker {
    pub fn new(
        name: &'static str,
        config: CircuitConfig,
        observer: Arc<dyn CircuitBreakerObserver>,
    ) -> Self {
        Self {
            name,
            circuit: Arc::new(Circuit::new(config)),
            observer,
        }
    }

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
    /// counting toward the threshold, and closes a half-open circuit
    /// because the service answered.
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

        if result.as_ref().err().is_some_and(classifier) {
            if let Some(transition) = self.circuit.record_failure(Instant::now()) {
                self.observer.on_state_change(self.name, transition);
                self.observer
                    .on_state_snapshot(self.name, self.circuit.state_label());
            }
            self.observer.on_call_failure(self.name);
        } else {
            if let Some(transition) = self.circuit.record_success() {
                self.observer.on_state_change(self.name, transition);
                self.observer
                    .on_state_snapshot(self.name, self.circuit.state_label());
            }
            self.observer.on_call_success(self.name);
        }

        result.map_err(CircuitBreakerError::Inner)
    }

    pub async fn call_transient<F, Fut, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: CircuitBreakableError,
    {
        self.call_with_filter(f, E::is_transient)
            .await
            .map_err(E::from_circuit_breaker)
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

    const COOLDOWN: Duration = Duration::from_millis(50);

    fn breaker(name: &'static str, failure_threshold: u32) -> CircuitBreaker {
        let config = CircuitConfig {
            failure_threshold,
            window: Duration::from_secs(10),
            cooldown: COOLDOWN,
        };
        CircuitBreaker::new(name, config, Arc::new(crate::NoopObserver))
    }

    fn db() -> CircuitBreaker {
        breaker("db", 2)
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
        let db = db();
        let result = db.call(|| async { Ok::<_, &str>(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn errors_flow_through_as_inner() {
        let db = db();
        let result = db.call(|| async { Err::<i32, _>("boom") }).await;
        assert_eq!(result.unwrap_err().into_inner().unwrap(), "boom");
    }

    #[tokio::test]
    async fn circuit_opens_after_threshold() {
        let db = db();
        trip(&db).await;

        let result = db.call(|| async { Ok::<_, &str>(1) }).await;
        assert!(result.unwrap_err().is_open());
    }

    #[tokio::test]
    async fn open_circuit_does_not_execute_function() {
        let db = db();
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
        let db = db();
        let api = breaker("api", 3);

        trip(&db).await;

        let result = api.call(|| async { Ok::<_, &str>(1) }).await;
        assert_eq!(result.unwrap(), 1);
    }

    #[tokio::test]
    async fn clones_share_state() {
        let db_a = db();
        let db_b = db_a.clone();

        trip(&db_a).await;

        assert!(!db_b.is_available());
        let result = db_b.call(|| async { Ok::<_, &str>(1) }).await;
        assert!(result.unwrap_err().is_open());
    }

    #[tokio::test]
    async fn classifier_controls_failure_counting() {
        let db = db();

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
        let db = db();
        trip(&db).await;
        assert!(!db.is_available());

        wait_for_cooldown().await;

        let result = db.call(|| async { Ok::<_, &str>(42) }).await;
        assert_eq!(result.unwrap(), 42);
        assert!(db.is_available());
    }

    #[tokio::test]
    async fn failed_probe_reopens_circuit() {
        let db = db();
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
    async fn dropped_probe_lets_a_later_call_probe_again() {
        let db = db();
        trip(&db).await;
        wait_for_cooldown().await;

        let dropped_probe = tokio::time::timeout(
            Duration::from_millis(1),
            db.call(std::future::pending::<Result<i32, &str>>),
        )
        .await;
        assert!(dropped_probe.is_err());

        let result = db.call(|| async { Ok::<_, &str>(1) }).await;
        assert!(result.unwrap_err().is_open());

        wait_for_cooldown().await;

        let result = db.call(|| async { Ok::<_, &str>(42) }).await;
        assert_eq!(result.unwrap(), 42);
        assert!(db.is_available());
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

        let config = CircuitConfig {
            failure_threshold: 2,
            window: Duration::from_secs(10),
            cooldown: Duration::from_secs(60),
        };
        let db = CircuitBreaker::new("db", config, observer.clone());

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

    #[derive(Debug, PartialEq)]
    enum ClassifiedError {
        Transient,
        Permanent,
        CircuitOpen { service: &'static str },
    }

    impl CircuitBreakableError for ClassifiedError {
        fn is_transient(&self) -> bool {
            matches!(self, Self::Transient)
        }

        fn circuit_open(service: &'static str) -> Self {
            Self::CircuitOpen { service }
        }
    }

    async fn trip_transient(breaker: &CircuitBreaker) {
        for _ in 0..2 {
            let _ = breaker
                .call_transient(|| async { Err::<(), _>(ClassifiedError::Transient) })
                .await;
        }
    }

    #[tokio::test]
    async fn call_transient_returns_concrete_error_type() {
        let db = db();
        let result: Result<i32, ClassifiedError> = db
            .call_transient(|| async { Err(ClassifiedError::Transient) })
            .await;
        assert_eq!(result.unwrap_err(), ClassifiedError::Transient);
    }

    #[tokio::test]
    async fn call_transient_trips_on_transient_errors() {
        let db = db();
        trip_transient(&db).await;

        let result: Result<i32, ClassifiedError> = db.call_transient(|| async { Ok(1) }).await;
        assert_eq!(
            result.unwrap_err(),
            ClassifiedError::CircuitOpen { service: "db" }
        );
    }

    #[tokio::test]
    async fn call_transient_ignores_permanent_errors() {
        let db = db();

        for _ in 0..5 {
            let result: Result<i32, ClassifiedError> = db
                .call_transient(|| async { Err(ClassifiedError::Permanent) })
                .await;
            assert_eq!(result.unwrap_err(), ClassifiedError::Permanent);
        }

        let result: Result<i32, ClassifiedError> = db.call_transient(|| async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn permanent_error_from_probe_closes_circuit() {
        let db = db();
        trip_transient(&db).await;
        wait_for_cooldown().await;

        let result: Result<i32, ClassifiedError> = db
            .call_transient(|| async { Err(ClassifiedError::Permanent) })
            .await;
        assert_eq!(result.unwrap_err(), ClassifiedError::Permanent);
        assert!(db.is_available());

        let result: Result<i32, ClassifiedError> = db.call_transient(|| async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }
}
