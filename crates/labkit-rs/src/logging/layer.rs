//! Custom tracing formatters for correlation ID injection.

use crate::correlation::context;
use crate::correlation::id::LOG_FIELD_CORRELATION_ID;
use chrono::Utc;
use serde_json::{Map, Value};
use std::fmt;
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

/// A JSON formatter that includes correlation ID from task-local context.
///
/// This formatter produces properly-escaped JSON output and injects the
/// `correlation_id` field into every log event when present in task-local context.
pub struct CorrelationIdJsonFormatter;

impl<S, N> FormatEvent<S, N> for CorrelationIdJsonFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let mut obj = Map::new();

        obj.insert(
            "timestamp".to_string(),
            Value::String(Utc::now().to_rfc3339()),
        );
        obj.insert("level".to_string(), Value::String(meta.level().to_string()));
        obj.insert(
            "target".to_string(),
            Value::String(meta.target().to_string()),
        );

        if let Some(correlation_id) = context::current() {
            obj.insert(
                LOG_FIELD_CORRELATION_ID.to_string(),
                Value::String(correlation_id.to_string()),
            );
        }

        let mut visitor = JsonVisitor::new(&mut obj);
        event.record(&mut visitor);

        let json = serde_json::to_string(&obj).map_err(|_| fmt::Error)?;
        writeln!(writer, "{}", json)
    }
}

/// A text formatter that includes correlation ID from task-local context.
///
/// This formatter produces human-readable output with the correlation ID
/// included as a field.
pub struct CorrelationIdTextFormatter;

impl<S, N> FormatEvent<S, N> for CorrelationIdTextFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let now = Utc::now();

        write!(writer, "{} ", now.to_rfc3339())?;
        write!(writer, "{:5} ", meta.level())?;
        write!(writer, "{}: ", meta.target())?;

        if let Some(correlation_id) = context::current() {
            write!(
                writer,
                "{}={} ",
                LOG_FIELD_CORRELATION_ID,
                correlation_id.as_str()
            )?;
        }

        if let Some(scope) = ctx.event_scope() {
            let mut seen = false;
            for span in scope.from_root() {
                seen = true;
                write!(writer, "{}:", span.name())?;
            }
            if seen {
                write!(writer, " ")?;
            }
        }

        let mut visitor = TextVisitor::new(&mut writer);
        event.record(&mut visitor);

        writeln!(writer)?;
        Ok(())
    }
}

struct JsonVisitor<'a> {
    obj: &'a mut Map<String, Value>,
}

impl<'a> JsonVisitor<'a> {
    fn new(obj: &'a mut Map<String, Value>) -> Self {
        Self { obj }
    }
}

impl tracing::field::Visit for JsonVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.obj.insert(
            field.name().to_string(),
            Value::String(format!("{:?}", value)),
        );
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.obj
            .insert(field.name().to_string(), Value::String(value.to_string()));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.obj
            .insert(field.name().to_string(), Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.obj
            .insert(field.name().to_string(), Value::Number(value.into()));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.obj
            .insert(field.name().to_string(), Value::Bool(value));
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if let Some(n) = serde_json::Number::from_f64(value) {
            self.obj.insert(field.name().to_string(), Value::Number(n));
        } else {
            // NaN/Infinity - store as string
            self.obj
                .insert(field.name().to_string(), Value::String(value.to_string()));
        }
    }
}

struct TextVisitor<'a, W: fmt::Write> {
    writer: &'a mut W,
    first: bool,
}

impl<'a, W: fmt::Write> TextVisitor<'a, W> {
    fn new(writer: &'a mut W) -> Self {
        Self {
            writer,
            first: true,
        }
    }
}

impl<W: fmt::Write> tracing::field::Visit for TextVisitor<'_, W> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        let name = field.name();
        if name == "message" {
            let _ = write!(self.writer, "{:?}", value);
        } else {
            if !self.first {
                let _ = write!(self.writer, " ");
            }
            let _ = write!(self.writer, "{}={:?}", name, value);
        }
        self.first = false;
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        let name = field.name();
        if name == "message" {
            let _ = write!(self.writer, "{}", value);
        } else {
            if !self.first {
                let _ = write!(self.writer, " ");
            }
            let _ = write!(self.writer, "{}=\"{}\"", name, value);
        }
        self.first = false;
    }
}
