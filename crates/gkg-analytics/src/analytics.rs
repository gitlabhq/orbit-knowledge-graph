use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::Value;
use thiserror::Error;
use tracing::{debug, warn};

use crate::config::AnalyticsConfig;
use crate::context::{OrbitContext, current};
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
    pub context: Option<OrbitContext>,
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
    pub fn track<E: AnalyticsEvent>(&self, event: E) {
        match &*self.0 {
            Inner::Noop => debug!(name = E::EVENT_NAME, "analytics: noop"),
            Inner::Recording(sink) => match serde_json::to_value(&event) {
                Ok(props) => sink.lock().push(Recorded {
                    event_name: E::EVENT_NAME,
                    schema_uri: E::SCHEMA_URI,
                    props,
                    context: current(),
                }),
                Err(err) => warn!(%err, name = E::EVENT_NAME, "analytics: serialize failed"),
            },
        }
    }

    /// Flush buffered events. No-op in the current stub.
    pub async fn shutdown(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{DeploymentType, SourceType, with_context};
    use crate::event::declare_event;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Demo {
        value: u32,
    }
    declare_event!(Demo => "demo" @ "iglu:com.gitlab/demo/jsonschema/1-0-0");

    #[tokio::test]
    async fn noop_drops_silently() {
        let a = Analytics::noop();
        a.track(Demo { value: 1 });
        a.shutdown().await;
    }

    #[tokio::test]
    async fn recording_captures_event_and_context() {
        let (a, h) = Analytics::recording();
        let ctx = OrbitContext::builder()
            .source_type(SourceType::Mcp)
            .deployment_type(DeploymentType::Com)
            .build();
        with_context(ctx, async {
            a.track(Demo { value: 7 });
        })
        .await;
        let events = h.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_name, "demo");
        assert_eq!(
            events[0].schema_uri,
            "iglu:com.gitlab/demo/jsonschema/1-0-0"
        );
        assert_eq!(events[0].props["value"], 7);
        assert_eq!(
            events[0].context.as_ref().map(|c| c.source_type),
            Some(SourceType::Mcp)
        );
    }

    #[tokio::test]
    async fn install_rejects_disabled_config() {
        assert!(matches!(
            Analytics::install(&AnalyticsConfig::default()),
            Err(InstallError::NotConfigured)
        ));
    }
}
