/// Identifies an external service protected by a circuit breaker.
///
/// Implement this on an enum in your crate to get type-safe service
/// names without the circuit-breaker crate knowing your services:
///
/// ```
/// use circuit_breaker::ServiceName;
///
/// enum MyService {
///     Database,
///     Api,
/// }
///
/// impl ServiceName for MyService {
///     fn as_str(&self) -> &'static str {
///         match self {
///             Self::Database => "database",
///             Self::Api => "api",
///         }
///     }
/// }
/// ```
///
/// The returned string must match a key in the config passed to
/// [`CircuitBreaker::new`](crate::CircuitBreaker::new).
pub trait ServiceName {
    fn as_str(&self) -> &'static str;
}
