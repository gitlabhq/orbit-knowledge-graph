use std::io::Write;
use std::sync::Mutex;
use std::time::Instant;

use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

use crate::memory::{alloc_stats, process_memory, set_phase_quiet};

/// Writes one JSONL record per tracing event, stamped with the allocator and
/// process memory readings taken at that instant. Joining this against the
/// sampler timeline is what turns "peak was 6 GB" into "peak was 6 GB while
/// the java family held its parse results and the graph".
pub struct EventLayer {
    start: Instant,
    out: Mutex<std::io::BufWriter<std::fs::File>>,
}

impl EventLayer {
    pub fn new(path: &std::path::Path, start: Instant) -> anyhow::Result<Self> {
        Ok(Self {
            start,
            out: Mutex::new(std::io::BufWriter::new(std::fs::File::create(path)?)),
        })
    }
}

#[derive(Default)]
struct FieldCollector(Map<String, Value>);

impl Visit for FieldCollector {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(
            field.name().to_string(),
            Value::String(format!("{value:?}")),
        );
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0
            .insert(field.name().to_string(), Value::String(value.to_string()));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().to_string(), Value::from(value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_string(), Value::from(value));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(field.name().to_string(), Value::from(value));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().to_string(), Value::from(value));
    }
}

impl<S: tracing::Subscriber> Layer<S> for EventLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = FieldCollector::default();
        event.record(&mut fields);

        // The archive download and extraction leave no marker the profiler can
        // see from outside; the first inventory probe is where parsing starts.
        if fields.0.get("stage").and_then(Value::as_str) == Some("inventory") {
            set_phase_quiet("parse");
        }

        let pm = process_memory();
        let a = alloc_stats();
        let mut record = Map::new();
        record.insert(
            "t_ms".into(),
            Value::from(self.start.elapsed().as_millis() as u64),
        );
        record.insert("target".into(), Value::from(event.metadata().target()));
        record.insert(
            "level".into(),
            Value::from(event.metadata().level().as_str()),
        );
        record.insert("rss".into(), Value::from(pm.resident_bytes));
        record.insert("footprint".into(), Value::from(pm.footprint_bytes));
        record.insert("alloc_live".into(), Value::from(a.live_bytes));
        record.insert("total_alloc_bytes".into(), Value::from(a.total_alloc_bytes));
        record.insert("total_allocs".into(), Value::from(a.total_allocs));
        record.insert("fields".into(), Value::Object(fields.0));

        if let Ok(mut out) = self.out.lock()
            && let Ok(line) = serde_json::to_string(&Value::Object(record))
        {
            let _ = writeln!(out, "{line}");
            let _ = out.flush();
        }
    }
}
