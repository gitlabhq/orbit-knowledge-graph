use std::collections::HashSet;
use std::sync::LazyLock;

use labkit::verbose_logs::{ExpandedLogsPolicy, SpanFacts, VerboseLogsConfig};
use orbit_log_fields::safe;
use orbit_server_config::ExpandedLogsConfig;
use orbit_utils::traversal_path::TraversalPath;
use tracing_subscriber::EnvFilter;

/// Rails sends this token when its `gkg_verbose_logs` feature flag is on.
pub static VERBOSE_LOGS: LazyLock<VerboseLogsConfig> = LazyLock::new(|| {
    VerboseLogsConfig::default()
        .with_feature_flags_header("x-gitlab-enabled-feature-flags")
        .with_target_feature_flag("verbose_gkg_logs")
});

const MESSAGE_LEAKING_TARGET: &str = "ra_ap";

/// rust-analyzer writes names from the parsed repository into its messages. EnvFilter
/// prefers the most specific directive, so configured `ra_ap*` directives are dropped.
pub fn log_directive(configured: Option<&str>) -> String {
    let base = configured
        .filter(|directive| !directive.is_empty())
        .map(str::to_owned)
        .or_else(valid_rust_log)
        .unwrap_or_else(|| "info".to_owned());
    let kept: Vec<&str> = base
        .split(',')
        .filter(|directive| !directive.trim_start().starts_with(MESSAGE_LEAKING_TARGET))
        .collect();
    format!("{},{MESSAGE_LEAKING_TARGET}=off", kept.join(","))
}

fn valid_rust_log() -> Option<String> {
    std::env::var(EnvFilter::DEFAULT_ENV)
        .ok()
        .filter(|directive| EnvFilter::try_new(directive).is_ok())
}

/// The variant of a typed error inside an `anyhow` error, else `fatal`: many webserver errors carry no kind.
pub fn anyhow_error_type(error: &anyhow::Error) -> &'static str {
    if let Some(error) = error.downcast_ref::<indexer::config::IndexerError>() {
        return error.into();
    }
    if let Some(error) = error.downcast_ref::<indexer::config::DispatcherError>() {
        return error.into();
    }
    if let Some(error) = error.downcast_ref::<orbit_migrations::version::SchemaVersionError>() {
        return error.into();
    }
    if let Some(error) = error.downcast_ref::<orbit_migrations::catalog::CatalogError>() {
        return error.into();
    }
    "fatal"
}

/// The default hook prints the panic text, which can hold parsed source code, to stderr.
pub fn install_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        let location = info.location().map(ToString::to_string);
        orbit_log_fields::error!(
            safe(PANIC_LOCATION) = location.as_deref(),
            safe(ERROR_TYPE) = "panic",
            expanded(ERROR_MESSAGE) = info.payload_as_str(),
            "panic"
        );
    }));
}

pub fn policy(config: &ExpandedLogsConfig) -> ExpandedLogsPolicy {
    let internal: HashSet<u64> = config.root_namespace_ids.iter().copied().collect();
    ExpandedLogsPolicy::default()
        .with_safe_fields(safe::ALL)
        .with_decision_fields([
            safe::EXPANDED_LOGS_REQUESTED,
            safe::GL_ROOT_NAMESPACE_ID,
            safe::TRAVERSAL_PATH,
        ])
        .with_global_override(config.global_override)
        .with_predicate(move |facts| is_requested(facts) || is_internal_tenant(facts, &internal))
}

fn is_requested(facts: &SpanFacts<'_>) -> bool {
    facts.get_bool(safe::EXPANDED_LOGS_REQUESTED) == Some(true)
}

/// A customer path inside an internal span must not be expanded.
fn is_internal_tenant(facts: &SpanFacts<'_>, internal: &HashSet<u64>) -> bool {
    let by_id = facts.get_u64(safe::GL_ROOT_NAMESPACE_ID);
    let by_path = facts.get_str(safe::TRAVERSAL_PATH);
    if by_id.is_none() && by_path.is_none() {
        return false;
    }
    let is_internal = |id: u64| internal.contains(&id);
    let id_is_internal = by_id.is_none_or(is_internal);
    let path_is_internal =
        by_path.is_none_or(|path| root_namespace_id(path).is_some_and(is_internal));
    id_is_internal && path_is_internal
}

fn root_namespace_id(traversal_path: &str) -> Option<u64> {
    let id = TraversalPath::new_unchecked(traversal_path).top_level_namespace_id()?;
    u64::try_from(id).ok()
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::{Arc, Mutex};

    use labkit::verbose_logs::ExpandedLogsLayer;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    const FILE_PATH: &str = "app/models/user.rb";

    #[test]
    fn anyhow_error_type_names_the_variant_of_a_typed_error() {
        use indexer::config::{DispatcherError, IndexerError};
        use indexer::orchestrator::scheduled::SchedulerError;

        let indexer = IndexerError::Analytics(labkit_events::Error::MissingField("schema"));
        let dispatcher = DispatcherError::Scheduler(SchedulerError::TaskPanicked { count: 1 });

        assert_eq!(anyhow_error_type(&indexer.into()), "analytics");
        assert_eq!(anyhow_error_type(&dispatcher.into()), "scheduler");
        let schema_version =
            orbit_migrations::version::SchemaVersionError::UnexpectedResult("x".into());
        assert_eq!(
            anyhow_error_type(&schema_version.into()),
            "unexpected_result"
        );
        let catalog = || orbit_migrations::catalog::CatalogError::Missing(7);
        assert_eq!(anyhow_error_type(&catalog().into()), "missing");
        let from_result = anyhow::Context::context(Err::<(), _>(catalog()), "reading the catalog");
        assert_eq!(anyhow_error_type(&from_result.unwrap_err()), "missing");
        let nested = anyhow::Error::from(catalog())
            .context("inner")
            .context("outer");
        assert_eq!(anyhow_error_type(&nested), "missing");
        assert_eq!(
            anyhow_error_type(&anyhow::anyhow!("customer text")),
            "fatal"
        );
    }

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
        orbit_log_fields::info!(
            safe(GL_PROJECT_ID) = 7,
            expanded(FILE_PATH) = FILE_PATH,
            "parsed file"
        );
    }

    #[test]
    fn internal_root_namespace_lines_are_expanded() {
        let logs = capture(&config(false, vec![9970]), || {
            let span = orbit_log_fields::info_span!("work", safe(GL_ROOT_NAMESPACE_ID) = 9970_u64);
            span.in_scope(log_parsed_file);
        });

        assert!(logs.contains(FILE_PATH), "{logs}");
    }

    #[test]
    fn other_root_namespace_lines_keep_only_safe_fields() {
        let logs = capture(&config(false, vec![9970]), || {
            let span = orbit_log_fields::info_span!("work", safe(GL_ROOT_NAMESPACE_ID) = 42_u64);
            span.in_scope(log_parsed_file);
        });

        assert!(!logs.contains(FILE_PATH), "{logs}");
        assert!(logs.contains(r#""gl_project_id":7"#), "{logs}");
    }

    #[test]
    fn traversal_path_names_the_root_namespace() {
        let logs = capture(&config(false, vec![9970]), || {
            let span = orbit_log_fields::info_span!("work", safe(TRAVERSAL_PATH) = "1/9970/123/");
            span.in_scope(log_parsed_file);
        });

        assert!(logs.contains(FILE_PATH), "{logs}");
    }

    #[test]
    fn customer_path_inside_an_internal_span_is_not_expanded() {
        let logs = capture(&config(false, vec![9970]), || {
            let outer = orbit_log_fields::info_span!("work", safe(GL_ROOT_NAMESPACE_ID) = 9970_u64);
            let inner = outer.in_scope(|| {
                orbit_log_fields::info_span!("project", safe(TRAVERSAL_PATH) = "1/42/123/")
            });
            inner.in_scope(log_parsed_file);
        });

        assert!(!logs.contains(FILE_PATH), "{logs}");
    }

    #[test]
    fn requested_lines_are_expanded() {
        let logs = capture(&config(false, vec![]), || {
            let span = orbit_log_fields::info_span!(
                "request",
                safe(GL_ROOT_NAMESPACE_ID) = 42_u64,
                safe(EXPANDED_LOGS_REQUESTED) = true
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
            orbit_log_fields::error!(target: "ra_ap_hir_def::attrs", "unknown crate acme_secret");
            orbit_log_fields::error!(target: "orbit_server", "kept");
        });

        assert!(!logs.contains("acme_secret"), "{logs}");
        assert!(logs.contains("kept"), "{logs}");
    }

    #[test]
    fn configured_rust_analyzer_directive_does_not_reenable_its_lines() {
        let filter =
            tracing_subscriber::EnvFilter::new(log_directive(Some("info,ra_ap_hir_ty=warn")));
        let logs = capture_with_filter(&config(true, vec![]), filter, || {
            tracing::warn!(target: "ra_ap_hir_ty::x", "unknown crate acme_secret");
            tracing::info!(target: "orbit_server", "kept");
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
