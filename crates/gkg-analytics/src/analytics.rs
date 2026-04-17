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
#[derive(Clone)]
pub struct Analytics(Arc<Inner>);

enum Inner {
    Noop,
    /// Stub: the real tracker (labkit-events) lands in a follow-up MR. For
    /// now, Live just logs each event at debug level so we can exercise the
    /// config path and prove wiring end-to-end.
    Live {
        app_id: String,
        collector_url: String,
    },
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

    pub fn install(cfg: &AnalyticsConfig) -> Result<Self, InstallError> {
        if !cfg.enabled || cfg.collector_url.is_empty() {
            return Err(InstallError::NotConfigured);
        }
        let app_id = cfg.app_id.clone().unwrap_or_else(|| "gkg".to_owned());
        Ok(Self(Arc::new(Inner::Live {
            app_id,
            collector_url: cfg.collector_url.clone(),
        })))
    }

    pub fn recording() -> (Self, crate::testkit::RecordingHandle) {
        let sink = Arc::new(Mutex::new(Vec::new()));
        (
            Self(Arc::new(Inner::Recording(sink.clone()))),
            crate::testkit::RecordingHandle::new(sink),
        )
    }

    /// Emit an event. Fire-and-forget — serialization failures are logged.
    pub fn track<E: AnalyticsEvent>(&self, event: E) {
        let props = match serde_json::to_value(&event) {
            Ok(v) => v,
            Err(err) => {
                warn!(%err, name = E::event_name(), "analytics: serialize failed");
                return;
            }
        };
        match &*self.0 {
            Inner::Noop => debug!(name = E::event_name(), "analytics: noop"),
            Inner::Live {
                app_id,
                collector_url,
            } => debug!(
                name = E::event_name(),
                schema = E::schema_uri(),
                %app_id,
                %collector_url,
                ?props,
                "analytics: live stub (no transport yet)"
            ),
            Inner::Recording(sink) => sink.lock().push(Recorded {
                event_name: E::event_name(),
                schema_uri: E::schema_uri(),
                props,
                context: current(),
            }),
        }
    }

    /// Flush any buffered events. No-op in the current stub.
    pub async fn shutdown(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{DeploymentType, SourceType, with_context};
    use crate::event::sealed::Sealed;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Demo {
        value: u32,
    }
    impl Sealed for Demo {}
    impl AnalyticsEvent for Demo {
        fn schema_uri() -> &'static str {
            "iglu:com.gitlab/demo/jsonschema/1-0-0"
        }
        fn event_name() -> &'static str {
            "demo"
        }
    }

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
