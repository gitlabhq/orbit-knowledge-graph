use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::Value;
use thiserror::Error;
use tracing::{debug, warn};

use crate::config::AnalyticsConfig;
use crate::context::{AnalyticsContext, OrbitCommon};
use crate::event::AnalyticsEvent;

#[derive(Debug, Error)]
pub enum InstallError {
    #[error("analytics disabled or collector_url empty")]
    NotConfigured,
}

/// Product-analytics handle. Cheaply cloneable.
///
/// Noop by default. `install` validates config and returns a live handle
/// once the labkit-events transport lands; today it's a logging stub that
/// still delegates to Noop.
#[derive(Clone)]
pub struct Analytics(Arc<Inner>);

enum Inner {
    Noop,
    Recording(Arc<Mutex<Vec<Recorded>>>),
}

#[derive(Clone, Debug)]
pub struct Recorded {
    pub event_name: &'static str,
    pub schema_uri: &'static str,
    pub props: Value,
    /// `(schema_uri, serialized data)` for each context captured at emit time.
    pub contexts: Vec<(&'static str, Value)>,
}

impl Analytics {
    pub fn noop() -> Self {
        Self(Arc::new(Inner::Noop))
    }

    /// Validate config and return a handle. Today the live transport is
    /// stubbed — a successful install logs a warning and hands back
    /// [`Analytics::noop`]. Real transport lands in a follow-up.
    pub fn install(cfg: &AnalyticsConfig) -> Result<Self, InstallError> {
        if !cfg.enabled || cfg.collector_url.is_empty() {
            return Err(InstallError::NotConfigured);
        }
        warn!(
            collector = %cfg.collector_url,
            "analytics: live transport not yet wired — emitting as noop"
        );
        Ok(Self::noop())
    }

    pub fn recording() -> (Self, crate::testkit::RecordingHandle) {
        let sink = Arc::new(Mutex::new(Vec::new()));
        (
            Self(Arc::new(Inner::Recording(sink.clone()))),
            crate::testkit::RecordingHandle::new(sink),
        )
    }

    /// Emit an event. Fire-and-forget. Serialization failures are logged.
    ///
    /// Attaches whichever of [`OrbitCommon`] and `E::PathContext` are in
    /// scope at call time. Missing contexts are simply omitted.
    pub fn track<E: AnalyticsEvent>(&self, event: E) {
        match &*self.0 {
            Inner::Noop => debug!(name = E::EVENT_NAME, "analytics: noop"),
            Inner::Recording(sink) => {
                let props = match serde_json::to_value(&event) {
                    Ok(v) => v,
                    Err(err) => {
                        warn!(%err, name = E::EVENT_NAME, "analytics: serialize failed");
                        return;
                    }
                };
                let mut contexts = Vec::with_capacity(2);
                push_context::<OrbitCommon>(&mut contexts);
                push_context::<E::PathContext>(&mut contexts);
                sink.lock().push(Recorded {
                    event_name: E::EVENT_NAME,
                    schema_uri: E::SCHEMA_URI,
                    props,
                    contexts,
                });
            }
        }
    }

    /// Flush buffered events. No-op in the current stub.
    pub async fn shutdown(&self) {}
}

fn push_context<C: AnalyticsContext>(out: &mut Vec<(&'static str, Value)>) {
    let Some(ctx) = C::current() else { return };
    match serde_json::to_value(&ctx) {
        Ok(v) => out.push((C::SCHEMA_URI, v)),
        Err(err) => warn!(%err, schema = C::SCHEMA_URI, "analytics: context serialize failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{DeploymentType, QueryContext, SourceType};
    use crate::event::declare_event;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Demo {
        value: u32,
    }
    declare_event!(
        Demo
            => "demo"
            @  "iglu:com.gitlab/demo/jsonschema/1-0-0"
            with QueryContext
    );

    #[tokio::test]
    async fn noop_drops_silently() {
        Analytics::noop().track(Demo { value: 1 });
    }

    #[tokio::test]
    async fn recording_captures_event_and_both_contexts() {
        let (a, h) = Analytics::recording();
        let common = OrbitCommon::builder()
            .deployment_type(DeploymentType::Com)
            .build();
        let query = QueryContext::builder()
            .source_type(SourceType::Mcp)
            .namespace_id("7")
            .build();
        common
            .scope(query.scope(async {
                a.track(Demo { value: 7 });
            }))
            .await;

        let events = h.events();
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.event_name, "demo");
        assert_eq!(e.schema_uri, "iglu:com.gitlab/demo/jsonschema/1-0-0");
        assert_eq!(e.props["value"], 7);
        assert_eq!(e.contexts.len(), 2);
        assert_eq!(
            e.contexts[0].0,
            "iglu:com.gitlab/orbit_common/jsonschema/1-0-0"
        );
        assert_eq!(e.contexts[0].1["deployment_type"], "com");
        assert_eq!(
            e.contexts[1].0,
            "iglu:com.gitlab/orbit_query/jsonschema/1-0-0"
        );
        assert_eq!(e.contexts[1].1["source_type"], "mcp");
        assert_eq!(e.contexts[1].1["namespace_id"], "7");
    }

    #[tokio::test]
    async fn recording_omits_contexts_when_not_in_scope() {
        let (a, h) = Analytics::recording();
        a.track(Demo { value: 1 });
        assert!(h.events()[0].contexts.is_empty());
    }

    #[tokio::test]
    async fn install_rejects_disabled_config() {
        assert!(matches!(
            Analytics::install(&AnalyticsConfig::default()),
            Err(InstallError::NotConfigured)
        ));
    }
}
