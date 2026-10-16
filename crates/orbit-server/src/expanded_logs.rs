use std::collections::HashSet;
use std::sync::LazyLock;

use labkit::verbose_logs::{ExpandedLogsPolicy, SpanFacts, VerboseLogsConfig};
use orbit_log_fields::{expanded, safe};
use orbit_server_config::ExpandedLogsConfig;

/// When the Rails feature flag `gkg_verbose_logs` is on, Rails sends the
/// `verbose_gkg_logs` token in this header on every GitLab.com request.
pub static VERBOSE_LOGS: LazyLock<VerboseLogsConfig> = LazyLock::new(|| {
    VerboseLogsConfig::default()
        .with_feature_flags_header("x-gitlab-enabled-feature-flags")
        .with_target_feature_flag("verbose_gkg_logs")
});

/// rust-analyzer crates put crate names and paths of the parsed repository in
/// their messages, and the policy cannot filter messages.
const MESSAGE_LEAKING_TARGETS: &str = "ra_ap=off";

pub fn log_directive(configured: Option<&str>) -> String {
    let base = configured
        .filter(|directive| !directive.is_empty())
        .map(str::to_owned)
        .or_else(|| std::env::var("RUST_LOG").ok())
        .unwrap_or_else(|| "info".to_owned());
    format!("{base},{MESSAGE_LEAKING_TARGETS}")
}

/// The default hook prints the panic message to stderr, and a parser panic
/// message can hold source text of the parsed file.
pub fn install_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        let location = info.location().map(ToString::to_string);
        tracing::error!(
            { safe::PANIC_LOCATION } = location.as_deref(),
            { expanded::ERROR_MESSAGE } = info.payload_as_str(),
            "panic"
        );
    }));
}

pub fn policy(config: &ExpandedLogsConfig) -> ExpandedLogsPolicy {
    let internal: HashSet<u64> = config.root_namespace_ids.iter().copied().collect();
    ExpandedLogsPolicy::default()
        .with_safe_fields(safe::ALL)
        .with_global_override(config.global_override)
        .with_predicate(move |facts| is_requested(facts) || is_internal_tenant(facts, &internal))
}

fn is_requested(facts: &SpanFacts<'_>) -> bool {
    facts.get_bool(safe::EXPANDED_LOGS_REQUESTED) == Some(true)
}

/// Spans can name the tenant twice, by ID and by traversal path. Both must be
/// internal, so a customer path inside an internal span is not expanded.
fn is_internal_tenant(facts: &SpanFacts<'_>, internal: &HashSet<u64>) -> bool {
    let by_id = facts.get_u64(safe::GL_ROOT_NAMESPACE_ID);
    let by_path = facts
        .get_str(safe::TRAVERSAL_PATH)
        .map(root_namespace_id_from_traversal_path);
    let roots: Vec<Option<u64>> = by_id.map(Some).into_iter().chain(by_path).collect();
    !roots.is_empty()
        && roots
            .iter()
            .all(|root| root.is_some_and(|id| internal.contains(&id)))
}

/// A traversal path is `<org_id>/<root_namespace_id>/...`.
fn root_namespace_id_from_traversal_path(traversal_path: &str) -> Option<u64> {
    traversal_path.split('/').nth(1)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::{Arc, Mutex};

    use labkit::verbose_logs::ExpandedLogsLayer;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    const FILE_PATH: &str = "app/models/user.rb";

    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn config(global_override: bool, root_namespace_ids: Vec<u64>) -> ExpandedLogsConfig {
        ExpandedLogsConfig {
            global_override,
            root_namespace_ids,
        }
    }

    fn capture(config: &ExpandedLogsConfig, emit: impl FnOnce()) -> String {
        capture_with_filter(config, tracing_subscriber::EnvFilter::new("trace"), emit)
    }

    fn capture_with_filter(
        config: &ExpandedLogsConfig,
        filter: tracing_subscriber::EnvFilter,
        emit: impl FnOnce(),
    ) -> String {
        let buffer = Buffer::default();
        let writer = buffer.clone();
        let layer = ExpandedLogsLayer::new(policy(config))
            .with_writer(move || writer.clone())
            .with_level_filter(filter);
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, emit);
        let bytes = buffer.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    fn log_parsed_file() {
        tracing::info!(
            { safe::GL_PROJECT_ID } = 7,
            { expanded::FILE_PATH } = FILE_PATH,
            "parsed file"
        );
    }

    #[test]
    fn internal_root_namespace_lines_are_expanded() {
        let logs = capture(&config(false, vec![9970]), || {
            let span = tracing::info_span!("work", { safe::GL_ROOT_NAMESPACE_ID } = 9970_u64);
            span.in_scope(log_parsed_file);
        });

        assert!(logs.contains(FILE_PATH), "{logs}");
    }

    #[test]
    fn other_root_namespace_lines_keep_only_safe_fields() {
        let logs = capture(&config(false, vec![9970]), || {
            let span = tracing::info_span!("work", { safe::GL_ROOT_NAMESPACE_ID } = 42_u64);
            span.in_scope(log_parsed_file);
        });

        assert!(!logs.contains(FILE_PATH), "{logs}");
        assert!(logs.contains(r#""gl_project_id":7"#), "{logs}");
    }

    #[test]
    fn traversal_path_names_the_root_namespace() {
        let logs = capture(&config(false, vec![9970]), || {
            let span = tracing::info_span!("work", { safe::TRAVERSAL_PATH } = "1/9970/123/");
            span.in_scope(log_parsed_file);
        });

        assert!(logs.contains(FILE_PATH), "{logs}");
    }

    #[test]
    fn customer_path_inside_an_internal_span_is_not_expanded() {
        let logs = capture(&config(false, vec![9970]), || {
            let outer = tracing::info_span!("work", { safe::GL_ROOT_NAMESPACE_ID } = 9970_u64);
            let inner = outer.in_scope(|| {
                tracing::info_span!("project", { safe::TRAVERSAL_PATH } = "1/42/123/")
            });
            inner.in_scope(log_parsed_file);
        });

        assert!(!logs.contains(FILE_PATH), "{logs}");
    }

    #[test]
    fn requested_lines_are_expanded() {
        let logs = capture(&config(false, vec![]), || {
            let span = tracing::info_span!(
                "request",
                { safe::GL_ROOT_NAMESPACE_ID } = 42_u64,
                { safe::EXPANDED_LOGS_REQUESTED } = true
            );
            span.in_scope(log_parsed_file);
        });

        assert!(logs.contains(FILE_PATH), "{logs}");
    }

    #[test]
    fn lines_outside_a_span_are_not_expanded() {
        let logs = capture(&config(false, vec![9970]), log_parsed_file);

        assert!(!logs.contains(FILE_PATH), "{logs}");
    }

    #[test]
    fn rust_analyzer_lines_are_dropped() {
        let filter = tracing_subscriber::EnvFilter::new(log_directive(Some("info")));
        let logs = capture_with_filter(&config(true, vec![]), filter, || {
            tracing::error!(target: "ra_ap_hir_def::attrs", "unknown crate acme_secret");
            tracing::error!(target: "orbit_server", "kept");
        });

        assert!(!logs.contains("acme_secret"), "{logs}");
        assert!(logs.contains("kept"), "{logs}");
    }

    #[test]
    fn global_override_expands_every_line() {
        let logs = capture(&config(true, vec![]), log_parsed_file);

        assert!(logs.contains(FILE_PATH), "{logs}");
    }
}
