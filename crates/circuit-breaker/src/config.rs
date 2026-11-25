use std::time::Duration;

#[derive(Debug, Clone)]
pub struct CircuitConfig {
    pub failure_threshold: u32,
    pub window: Duration,
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
