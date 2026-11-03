use std::sync::OnceLock;

const UNKNOWN_BUILD_VERSION: &str = "0.0.0-dev";

static VERSION: OnceLock<String> = OnceLock::new();

/// Records the version the binary was built from. Call it before the first [`get`].
pub fn set_build_version(build_version: &str) {
    let _ = VERSION.set(resolve(std::env::var("GKG_VERSION").ok(), build_version));
}

pub fn get() -> &'static str {
    VERSION
        .get_or_init(|| resolve(std::env::var("GKG_VERSION").ok(), UNKNOWN_BUILD_VERSION))
        .as_str()
}

fn resolve(runtime_override: Option<String>, build_version: &str) -> String {
    runtime_override
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| build_version.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_override_wins() {
        assert_eq!(resolve(Some("99.0.0".to_string()), "1.2.3"), "99.0.0");
    }

    #[test]
    fn empty_override_falls_back_to_build_version() {
        assert_eq!(resolve(Some(String::new()), "1.2.3"), "1.2.3");
    }

    #[test]
    fn none_override_falls_back_to_build_version() {
        assert_eq!(resolve(None, "1.2.3"), "1.2.3");
    }
}
