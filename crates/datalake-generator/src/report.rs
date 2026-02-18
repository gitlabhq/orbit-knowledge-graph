use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::json;
use sysinfo::System;
use tracing::info;

pub struct MetricsCollector {
    start_time: Instant,
    phase_timings: HashMap<String, Duration>,
    row_counts: HashMap<String, usize>,
    system_sampler: SystemSampler,
    enabled: bool,
}

impl MetricsCollector {
    pub fn new(enabled: bool) -> Self {
        Self {
            start_time: Instant::now(),
            phase_timings: HashMap::new(),
            row_counts: HashMap::new(),
            system_sampler: SystemSampler::new(),
            enabled,
        }
    }

    pub fn record_phase(&mut self, phase: &str, duration: Duration) {
        if !self.enabled {
            return;
        }
        self.phase_timings.insert(phase.to_string(), duration);
    }

    pub fn record_rows(&mut self, table: &str, count: usize) {
        if !self.enabled {
            return;
        }
        *self.row_counts.entry(table.to_string()).or_default() += count;
    }

    pub fn start_system_sampling(&mut self) {
        if self.enabled {
            self.system_sampler.start();
        }
    }

    pub fn stop_system_sampling(&mut self) {
        if self.enabled {
            self.system_sampler.stop();
        }
    }

    pub fn total_elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn phase_timings(&self) -> &HashMap<String, Duration> {
        &self.phase_timings
    }

    pub fn row_counts(&self) -> &HashMap<String, usize> {
        &self.row_counts
    }

    pub fn system_summary(&self) -> SystemSummary {
        self.system_sampler.summary()
    }
}

struct SystemSampler {
    samples: Vec<SystemSample>,
    sampling: bool,
}

struct SystemSample {
    memory_rss_bytes: u64,
}

#[derive(Default)]
pub struct SystemSummary {
    pub peak_memory_mb: f64,
    pub avg_memory_mb: f64,
}

impl SystemSampler {
    fn new() -> Self {
        Self {
            samples: Vec::new(),
            sampling: false,
        }
    }

    fn start(&mut self) {
        self.sampling = true;
        self.take_sample();
    }

    fn stop(&mut self) {
        self.take_sample();
        self.sampling = false;
    }

    fn take_sample(&mut self) {
        if !self.sampling {
            return;
        }
        let mut sys = System::new();
        sys.refresh_memory();

        let pid = sysinfo::get_current_pid().ok();
        let memory = if let Some(pid) = pid {
            sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
            if let Some(process) = sys.process(pid) {
                process.memory()
            } else {
                sys.used_memory()
            }
        } else {
            sys.used_memory()
        };

        self.samples.push(SystemSample {
            memory_rss_bytes: memory,
        });
    }

    fn summary(&self) -> SystemSummary {
        if self.samples.is_empty() {
            return SystemSummary::default();
        }

        let peak_memory = self
            .samples
            .iter()
            .map(|sample| sample.memory_rss_bytes)
            .max()
            .unwrap_or(0);
        let avg_memory = self
            .samples
            .iter()
            .map(|sample| sample.memory_rss_bytes)
            .sum::<u64>()
            / self.samples.len() as u64;

        SystemSummary {
            peak_memory_mb: peak_memory as f64 / (1024.0 * 1024.0),
            avg_memory_mb: avg_memory as f64 / (1024.0 * 1024.0),
        }
    }
}

pub fn write_report_json(collector: &MetricsCollector, output_path: &str) -> Result<()> {
    let system_summary = collector.system_summary();

    let report = json!({
        "run_id": chrono::Utc::now().to_rfc3339(),
        "total_duration_secs": collector.total_elapsed().as_secs_f64(),
        "generation_debug": {
            "tables_seeded": collector.row_counts(),
            "phase_timings": collector.phase_timings()
                .iter()
                .map(|(k, v)| (k.clone(), v.as_secs_f64()))
                .collect::<HashMap<_, _>>(),
        },
        "resource_usage": {
            "memory": {
                "peak_rss_mb": system_summary.peak_memory_mb,
                "avg_rss_mb": system_summary.avg_memory_mb,
            },
        },
    });

    let json_str = serde_json::to_string_pretty(&report)?;
    std::fs::write(output_path, &json_str)?;
    info!(path = output_path, "wrote report");
    Ok(())
}

pub fn print_summary(collector: &MetricsCollector) {
    let system_summary = collector.system_summary();

    println!("=== Datalake Generator Report ===\n");
    println!("Duration: {:.1}s", collector.total_elapsed().as_secs_f64());

    println!("\nTables Seeded:");
    for (table, count) in collector.row_counts() {
        println!("  {table}: {count}");
    }

    println!("\nResource Usage:");
    println!(
        "  Memory: {:.0} MB peak, {:.0} MB avg",
        system_summary.peak_memory_mb, system_summary.avg_memory_mb
    );
}
