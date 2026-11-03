use std::process::Command;
use std::sync::OnceLock;

const UNKNOWN_BUILD_VERSION: &str = "0.0.0-dev";

pub fn get() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION
        .get_or_init(|| {
            resolve(std::env::var("GKG_VERSION").ok(), || {
                describe_checkout().unwrap_or_else(|| UNKNOWN_BUILD_VERSION.to_string())
            })
        })
        .as_str()
}

/// `git describe` of the source checkout this binary was built from, without the leading `v`.
pub fn describe_checkout() -> Option<String> {
    let output = Command::new("git")
        .args(["describe", "--tags", "--match", "v[0-9]*", "--always"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()
        .filter(|output| output.status.success())?;
    let described = String::from_utf8(output.stdout).ok()?;
    let described = described.trim();
    (!described.is_empty()).then(|| described.strip_prefix('v').unwrap_or(described).to_string())
}

fn resolve(runtime_override: Option<String>, fallback: impl FnOnce() -> String) -> String {
    runtime_override
        .filter(|v| !v.is_empty())
        .unwrap_or_else(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_override_wins() {
        assert_eq!(
            resolve(Some("99.0.0".to_string()), || unreachable!()),
            "99.0.0"
        );
    }

    #[test]
    fn empty_override_falls_back() {
        assert_eq!(
            resolve(Some(String::new()), || "1.2.3".to_string()),
            "1.2.3"
        );
    }

    #[test]
    fn none_override_falls_back() {
        assert_eq!(resolve(None, || "1.2.3".to_string()), "1.2.3");
    }

    #[test]
    fn describe_checkout_reads_this_repository() {
        let described = describe_checkout().expect("tests run inside the git checkout");
        assert!(!described.is_empty() && !described.starts_with('v'));
    }
}
