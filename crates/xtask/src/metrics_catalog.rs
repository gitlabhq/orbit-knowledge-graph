//! Generates `orbit-dashboards/gkg-metrics.json` from the static
//! `gkg_observability::catalog()`.
//!
//! With `--check`, compares the regenerated output against the committed file
//! and fails with a diff if they differ. This is the CI gate that keeps
//! dashboards and emitted metrics from drifting apart.

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use gkg_observability::{MetricSpec, catalog};
use serde::Serialize;

/// Default output path relative to the workspace root.
pub const DEFAULT_OUTPUT: &str = "crates/gkg-observability/orbit-dashboards/gkg-metrics.json";

/// Hand-authored note prepended to the generated file via a `$generated_by` key.
const GENERATED_BY: &str = "cargo xtask metrics-catalog - do not edit";

/// Minimum number of metrics that must appear in the catalog. Acts as a
/// sanity floor so an accidental truncation (e.g. a missing `v.extend(...)`
/// line in `gkg_observability::catalog`) fails the `--check` pass rather than
/// quietly shipping an incomplete dashboard inventory.
const MIN_CATALOG_SIZE: usize = 55;

pub fn run(output: Option<PathBuf>, check: bool) -> Result<()> {
    let specs = catalog();
    validate(&specs)?;
    let rendered = render(&specs)?;

    let path = output.unwrap_or_else(|| PathBuf::from(DEFAULT_OUTPUT));

    if check {
        let current = fs::read_to_string(&path)
            .with_context(|| format!("reading existing catalog at {}", path.display()))?;
        if normalise(&current) == normalise(&rendered) {
            println!("metrics catalog is up to date ({} entries)", specs.len());
            return Ok(());
        }
        eprintln!(
            "metrics catalog at {} is stale. Run `mise run metrics:catalog` and commit.",
            path.display()
        );
        print_diff(&current, &rendered);
        return Err(anyhow!("metrics catalog stale"));
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating directory {}", parent.display()))?;
    }
    fs::write(&path, &rendered)
        .with_context(|| format!("writing metrics catalog to {}", path.display()))?;
    println!("wrote {} metric entries to {}", specs.len(), path.display());
    Ok(())
}

fn validate(specs: &[&'static MetricSpec]) -> Result<()> {
    if specs.len() < MIN_CATALOG_SIZE {
        return Err(anyhow!(
            "catalog has only {} entries (minimum {}); did a module fail to register?",
            specs.len(),
            MIN_CATALOG_SIZE
        ));
    }
    let mut otel_seen = std::collections::HashSet::new();
    let mut prom_seen = std::collections::HashSet::new();
    for spec in specs {
        if !otel_seen.insert(spec.otel_name) {
            return Err(anyhow!("duplicate otel_name: {}", spec.otel_name));
        }
        let prom = spec.prom_name();
        if !prom_seen.insert(prom.clone()) {
            return Err(anyhow!(
                "duplicate prom_name {} (otel: {})",
                prom,
                spec.otel_name
            ));
        }
        if spec.kind.is_histogram() && spec.buckets.is_none() {
            return Err(anyhow!(
                "histogram {} has no buckets; assign one from gkg_observability::buckets",
                spec.otel_name
            ));
        }
    }
    Ok(())
}

/// Serialisable wrapper that carries every catalog field plus the computed
/// Prometheus name so jsonnet consumers can use either.
#[derive(Serialize)]
struct Entry {
    otel_name: &'static str,
    prom_name: String,
    description: &'static str,
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    unit: Option<&'static str>,
    labels: Vec<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    buckets: Option<Vec<f64>>,
    stability: &'static str,
    domain: &'static str,
}

impl From<&MetricSpec> for Entry {
    fn from(spec: &MetricSpec) -> Self {
        Self {
            otel_name: spec.otel_name,
            prom_name: spec.prom_name(),
            description: spec.description,
            kind: kind_str(spec.kind),
            unit: spec.unit,
            labels: spec.labels.to_vec(),
            buckets: spec.buckets.map(<[f64]>::to_vec),
            stability: stability_str(spec.stability),
            domain: spec.domain,
        }
    }
}

fn kind_str(kind: gkg_observability::MetricKind) -> &'static str {
    use gkg_observability::MetricKind::*;
    match kind {
        Counter => "counter",
        UpDownCounter => "up_down_counter",
        Gauge => "gauge",
        ObservableGauge => "observable_gauge",
        HistogramF64 => "histogram_f64",
        HistogramU64 => "histogram_u64",
    }
}

fn stability_str(stability: gkg_observability::Stability) -> &'static str {
    match stability {
        gkg_observability::Stability::Stable => "stable",
        gkg_observability::Stability::Experimental => "experimental",
    }
}

#[derive(Serialize)]
struct Output {
    #[serde(rename = "$generated_by")]
    generated_by: &'static str,
    metrics: Vec<Entry>,
}

fn render(specs: &[&'static MetricSpec]) -> Result<String> {
    let mut sorted: Vec<&&MetricSpec> = specs.iter().collect();
    sorted.sort_by_key(|s| s.otel_name);
    let metrics: Vec<Entry> = sorted.iter().map(|s| Entry::from(**s)).collect();
    let output = Output {
        generated_by: GENERATED_BY,
        metrics,
    };
    let mut json = serde_json::to_string_pretty(&output).context("serialising catalog to JSON")?;
    json.push('\n');
    Ok(json)
}

/// Canonicalise JSON so whitespace-only differences don't count as drift.
fn normalise(raw: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(value) => serde_json::to_string_pretty(&value).unwrap_or_else(|_| raw.to_string()),
        Err(_) => raw.to_string(),
    }
}

fn print_diff(before: &str, after: &str) {
    let before_lines: Vec<&str> = before.lines().collect();
    let after_lines: Vec<&str> = after.lines().collect();
    let shown_lines = before_lines.len().max(after_lines.len()).min(50);
    for i in 0..shown_lines {
        let b = before_lines.get(i).copied().unwrap_or("");
        let a = after_lines.get(i).copied().unwrap_or("");
        if b != a {
            eprintln!("- {b}");
            eprintln!("+ {a}");
        }
    }
}
