use std::time::Duration;

/// Per-service circuit breaker configuration.
#[derive(Debug, Clone)]
pub struct CircuitConfig {
    /// Number of failures within `window` before the circuit opens.
    pub failure_threshold: u32,
    /// Sliding time window for counting failures.
    pub window: Duration,
    /// How long the circuit stays open before allowing a probe.
    pub cooldown: Duration,
}

impl Default for CircuitConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            window: Duration::from_secs(30),
            cooldown: Duration::from_secs(60),
        }
    }
}
