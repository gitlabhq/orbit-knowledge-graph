//! Tests for the logging module.

use super::config::{Format, LogConfig};
use super::layer::{CorrelationIdJsonFormatter, CorrelationIdTextFormatter};
use crate::correlation::{CorrelationId, context};
use std::io::Write;
use std::sync::{Arc, Mutex};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Layer};

#[test]
fn format_from_str_json() {
    assert_eq!("json".parse::<Format>().unwrap(), Format::Json);
    assert_eq!("JSON".parse::<Format>().unwrap(), Format::Json);
    assert_eq!("Json".parse::<Format>().unwrap(), Format::Json);
}

#[test]
fn format_from_str_text() {
    assert_eq!("text".parse::<Format>().unwrap(), Format::Text);
    assert_eq!("TEXT".parse::<Format>().unwrap(), Format::Text);
    assert_eq!("anything".parse::<Format>().unwrap(), Format::Text);
    assert_eq!("".parse::<Format>().unwrap(), Format::Text);
}

#[test]
fn log_config_builders() {
    let config = LogConfig::json();
    assert_eq!(config.format, Format::Json);
    assert!(config.level.is_none());

    let config = LogConfig::text();
    assert_eq!(config.format, Format::Text);

    let config = LogConfig::new()
        .with_level("debug")
        .with_format(Format::Json);
    assert_eq!(config.format, Format::Json);
    assert_eq!(config.level, Some("debug".to_string()));
}

#[test]
fn log_config_default() {
    // Note: This test may be affected by LOG_FORMAT env var if set
    let config = LogConfig::default();
    assert!(config.level.is_none());
}

/// A writer that captures output to a buffer for testing
#[derive(Clone)]
struct TestWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl TestWriter {
    fn new() -> Self {
        Self {
            buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn contents(&self) -> String {
        let buf = self.buffer.lock().unwrap();
        String::from_utf8_lossy(&buf).to_string()
    }
}

impl Write for TestWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for TestWriter {
    type Writer = TestWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test]
async fn json_format_includes_correlation_id() {
    let test_writer = TestWriter::new();
    let test_writer_clone = test_writer.clone();

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(test_writer_clone)
        .event_format(CorrelationIdJsonFormatter)
        .with_filter(EnvFilter::new("info"));

    let subscriber = tracing_subscriber::registry().with(fmt_layer);

    // Use tracing::subscriber::with_default to set the subscriber for this scope only
    tracing::subscriber::with_default(subscriber, || {
        let correlation_id = CorrelationId::from_string("test-correlation-id-123");

        // Run within a correlation context
        context::sync_scope(correlation_id.clone(), || {
            tracing::info!("Test message with correlation");
        });
    });

    let output = test_writer.contents();

    // Verify JSON output contains the correlation_id field
    assert!(
        output.contains("\"correlation_id\":\"test-correlation-id-123\""),
        "Expected correlation_id in JSON output. Got: {}",
        output
    );
    assert!(
        output.contains("\"message\":"),
        "Expected message field in JSON output. Got: {}",
        output
    );
    assert!(
        output.contains("\"level\":\"INFO\""),
        "Expected level field in JSON output. Got: {}",
        output
    );
}

#[tokio::test]
async fn text_format_includes_correlation_id() {
    let test_writer = TestWriter::new();
    let test_writer_clone = test_writer.clone();

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(test_writer_clone)
        .event_format(CorrelationIdTextFormatter)
        .with_filter(EnvFilter::new("info"));

    let subscriber = tracing_subscriber::registry().with(fmt_layer);

    tracing::subscriber::with_default(subscriber, || {
        let correlation_id = CorrelationId::from_string("test-text-correlation-456");

        context::sync_scope(correlation_id.clone(), || {
            tracing::info!("Test text message");
        });
    });

    let output = test_writer.contents();

    // Verify text output contains the correlation_id
    assert!(
        output.contains("correlation_id=test-text-correlation-456"),
        "Expected correlation_id in text output. Got: {}",
        output
    );
    assert!(
        output.contains("INFO"),
        "Expected INFO level in text output. Got: {}",
        output
    );
}

#[tokio::test]
async fn no_correlation_id_when_outside_context() {
    let test_writer = TestWriter::new();
    let test_writer_clone = test_writer.clone();

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(test_writer_clone)
        .event_format(CorrelationIdJsonFormatter)
        .with_filter(EnvFilter::new("info"));

    let subscriber = tracing_subscriber::registry().with(fmt_layer);

    tracing::subscriber::with_default(subscriber, || {
        // Log without a correlation context
        tracing::info!("Message without correlation");
    });

    let output = test_writer.contents();

    // Should not contain correlation_id field
    assert!(
        !output.contains("correlation_id"),
        "Should not have correlation_id when outside context. Got: {}",
        output
    );
    // But should still have the message
    assert!(
        output.contains("\"message\":"),
        "Expected message field. Got: {}",
        output
    );
}

#[tokio::test]
async fn correlation_id_propagates_through_async_scope() {
    let test_writer = TestWriter::new();
    let test_writer_clone = test_writer.clone();

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(test_writer_clone)
        .event_format(CorrelationIdJsonFormatter)
        .with_filter(EnvFilter::new("info"));

    let subscriber = tracing_subscriber::registry().with(fmt_layer);

    let _guard = tracing::subscriber::set_default(subscriber);

    let correlation_id = CorrelationId::from_string("async-correlation-789");

    context::scope(correlation_id.clone(), async {
        tracing::info!("Async message 1");

        // Simulate some async work
        tokio::task::yield_now().await;

        tracing::info!("Async message 2");
    })
    .await;

    let output = test_writer.contents();

    // Both messages should have the correlation_id
    let matches: Vec<_> = output.match_indices("async-correlation-789").collect();
    assert_eq!(
        matches.len(),
        2,
        "Expected correlation_id in both async messages. Got: {}",
        output
    );
}
