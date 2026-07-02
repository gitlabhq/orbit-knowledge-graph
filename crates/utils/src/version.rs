use std::sync::OnceLock;

/// Version string compiled in by `build.rs` from `git describe`.
/// Falls back to `0.0.0-dev` when git metadata is unavailable.
const BUILD_VERSION: &str = env!("GKG_BUILD_VERSION");

/// Returns the resolved server version.
///
/// Priority (highest → lowest):
/// 1. `GKG_VERSION` env var (set at runtime in container deployments)
/// 2. `GKG_BUILD_VERSION` (injected at compile time by `build.rs`)
pub fn get() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION
        .get_or_init(|| resolve(std::env::var("GKG_VERSION").ok()))
        .as_str()
}

fn resolve(runtime_override: Option<String>) -> String {
    runtime_override
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| BUILD_VERSION.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_version_is_not_workspace_placeholder() {
        assert_ne!(BUILD_VERSION, "0.1.0");
    }

    #[test]
    fn build_version_is_not_empty() {
        assert!(!BUILD_VERSION.is_empty());
    }

    #[test]
    fn runtime_override_wins() {
        assert_eq!(resolve(Some("99.0.0".to_string())), "99.0.0");
    }

    #[test]
    fn empty_override_falls_back_to_build_version() {
        assert_eq!(resolve(Some(String::new())), BUILD_VERSION);
    }

    #[test]
    fn none_override_falls_back_to_build_version() {
        assert_eq!(resolve(None), BUILD_VERSION);
    }
}
