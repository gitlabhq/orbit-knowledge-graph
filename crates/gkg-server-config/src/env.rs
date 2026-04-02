//! Environment variable parsing utilities.

use std::str::FromStr;

/// Parses an environment variable or returns the default value.
///
/// Returns `default` if the variable is unset or fails to parse.
pub fn env_var_or<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Parses an optional environment variable.
///
/// Returns `None` if the variable is unset or fails to parse.
pub fn env_var_opt<T: FromStr>(key: &str) -> Option<T> {
    std::env::var(key).ok().and_then(|v| v.parse().ok())
}
