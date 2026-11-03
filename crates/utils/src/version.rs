use std::sync::OnceLock;

const UNKNOWN_BUILD_VERSION: &str = "0.0.0-dev";

static BUILD_VERSION: OnceLock<&'static str> = OnceLock::new();

/// Records the version the binary was built from. Call it before the first [`get`].
pub fn set_build_version(version: &'static str) {
    let _ = BUILD_VERSION.set(version);
}

pub fn get() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION
        .get_or_init(|| {
            resolve(
                std::env::var("GKG_VERSION").ok(),
                BUILD_VERSION.get().copied(),
            )
        })
        .as_str()
}

fn resolve(runtime_override: Option<String>, build_version: Option<&str>) -> String {
    runtime_override
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| build_version.unwrap_or(UNKNOWN_BUILD_VERSION).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_override_wins() {
        assert_eq!(resolve(Some("99.0.0".to_string()), Some("1.2.3")), "99.0.0");
    }

    #[test]
    fn empty_override_falls_back_to_build_version() {
        assert_eq!(resolve(Some(String::new()), Some("1.2.3")), "1.2.3");
    }

    #[test]
    fn none_override_falls_back_to_build_version() {
        assert_eq!(resolve(None, Some("1.2.3")), "1.2.3");
    }

    #[test]
    fn unset_build_version_falls_back_to_unknown() {
        assert_eq!(resolve(None, None), UNKNOWN_BUILD_VERSION);
    }
}
