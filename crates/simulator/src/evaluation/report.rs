//! Report generation for evaluation results.

use super::ExecutionResult;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Output format for reports.
#[derive(Debug, Clone, Copy, Default)]
pub enum ReportFormat {
    #[default]
    Text,
    Json,
    Markdown,
}

impl std::str::FromStr for ReportFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "text" | "txt" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            "markdown" | "md" => Ok(Self::Markdown),
            _ => Err(format!("Unknown format: {}", s)),
        }
    }
}

/// Summary statistics for a set of query results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Summary {
    pub total_queries: usize,
    pub successful: usize,
    pub failed: usize,
    pub total_rows: u64,
    pub total_time: Duration,
    pub avg_time: Duration,
    pub min_time: Duration,
    pub max_time: Duration,
}

impl Summary {
    pub fn from_results(results: &[ExecutionResult]) -> Self {
        let total_queries = results.len();
        let successful = results.iter().filter(|r| r.success).count();
        let failed = total_queries - successful;

        let total_rows: u64 = results.iter().filter_map(|r| r.row_count).sum();

        let times: Vec<Duration> = results.iter().map(|r| r.execution_time).collect();
        let total_time: Duration = times.iter().sum();
        let avg_time = if !times.is_empty() {
            total_time / times.len() as u32
        } else {
            Duration::ZERO
        };
        let min_time = times.iter().min().copied().unwrap_or(Duration::ZERO);
        let max_time = times.iter().max().copied().unwrap_or(Duration::ZERO);

        Self {
            total_queries,
            successful,
            failed,
            total_rows,
            total_time,
            avg_time,
            min_time,
            max_time,
        }
    }

    pub fn success_rate(&self) -> f64 {
        if self.total_queries == 0 {
            0.0
        } else {
            self.successful as f64 / self.total_queries as f64 * 100.0
        }
    }
}

/// Report containing results and summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    pub summary: Summary,
    pub results: Vec<ExecutionResult>,
}

impl Report {
    pub fn new(results: Vec<ExecutionResult>) -> Self {
        let summary = Summary::from_results(&results);
        Self { summary, results }
    }

    /// Format the report as text.
    pub fn to_text(&self) -> String {
        let mut output = String::new();

        output.push_str("═══════════════════════════════════════════════════════════════════\n");
        output.push_str("                    QUERY EVALUATION REPORT\n");
        output.push_str("═══════════════════════════════════════════════════════════════════\n\n");

        // Summary
        output.push_str("SUMMARY\n");
        output.push_str("───────────────────────────────────────────────────────────────────\n");
        output.push_str(&format!(
            "Total Queries:   {:>6}\n",
            self.summary.total_queries
        ));
        output.push_str(&format!(
            "Successful:      {:>6} ({:.1}%)\n",
            self.summary.successful,
            self.summary.success_rate()
        ));
        output.push_str(&format!("Failed:          {:>6}\n", self.summary.failed));
        output.push_str(&format!(
            "Total Rows:      {:>6}\n",
            self.summary.total_rows
        ));
        output.push_str(&format!(
            "Total Time:      {:>6.2}s\n",
            self.summary.total_time.as_secs_f64()
        ));
        output.push_str(&format!(
            "Avg Time:        {:>6.2}ms\n",
            self.summary.avg_time.as_secs_f64() * 1000.0
        ));
        output.push_str(&format!(
            "Min Time:        {:>6.2}ms\n",
            self.summary.min_time.as_secs_f64() * 1000.0
        ));
        output.push_str(&format!(
            "Max Time:        {:>6.2}ms\n",
            self.summary.max_time.as_secs_f64() * 1000.0
        ));

        output.push_str("\n");

        // Individual results
        output.push_str("QUERY RESULTS\n");
        output.push_str("───────────────────────────────────────────────────────────────────\n");

        // Successful queries
        let mut successful: Vec<_> = self.results.iter().filter(|r| r.success).collect();
        successful.sort_by(|a, b| b.execution_time.cmp(&a.execution_time));

        if !successful.is_empty() {
            output.push_str("\n✓ Successful Queries (sorted by execution time):\n\n");
            for result in &successful {
                output.push_str(&format!(
                    "  {:50} {:>6} rows  {:>8.2}ms\n",
                    truncate(&result.query_name, 50),
                    result.row_count.unwrap_or(0),
                    result.execution_time.as_secs_f64() * 1000.0
                ));
            }
        }

        // Failed queries
        let failed: Vec<_> = self.results.iter().filter(|r| !r.success).collect();
        if !failed.is_empty() {
            output.push_str("\n✗ Failed Queries:\n\n");
            for result in &failed {
                output.push_str(&format!("  {}\n", result.query_name));
                if let Some(error) = &result.error {
                    output.push_str(&format!("    Error: {}\n", error));
                }
            }
        }

        output.push_str("\n═══════════════════════════════════════════════════════════════════\n");

        output
    }

    /// Format the report as JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Format the report as Markdown.
    pub fn to_markdown(&self) -> String {
        let mut output = String::new();

        output.push_str("# Query Evaluation Report\n\n");

        // Summary table
        output.push_str("## Summary\n\n");
        output.push_str("| Metric | Value |\n");
        output.push_str("|--------|-------|\n");
        output.push_str(&format!(
            "| Total Queries | {} |\n",
            self.summary.total_queries
        ));
        output.push_str(&format!(
            "| Successful | {} ({:.1}%) |\n",
            self.summary.successful,
            self.summary.success_rate()
        ));
        output.push_str(&format!("| Failed | {} |\n", self.summary.failed));
        output.push_str(&format!("| Total Rows | {} |\n", self.summary.total_rows));
        output.push_str(&format!(
            "| Total Time | {:.2}s |\n",
            self.summary.total_time.as_secs_f64()
        ));
        output.push_str(&format!(
            "| Avg Time | {:.2}ms |\n",
            self.summary.avg_time.as_secs_f64() * 1000.0
        ));
        output.push_str(&format!(
            "| Min Time | {:.2}ms |\n",
            self.summary.min_time.as_secs_f64() * 1000.0
        ));
        output.push_str(&format!(
            "| Max Time | {:.2}ms |\n",
            self.summary.max_time.as_secs_f64() * 1000.0
        ));

        output.push_str("\n## Results\n\n");

        // Results table
        output.push_str("| Query | Status | Rows | Time (ms) |\n");
        output.push_str("|-------|--------|------|----------|\n");

        let mut sorted_results = self.results.clone();
        sorted_results.sort_by(|a, b| b.execution_time.cmp(&a.execution_time));

        for result in &sorted_results {
            let status = if result.success { "✓" } else { "✗" };
            let rows = result
                .row_count
                .map(|r| r.to_string())
                .unwrap_or_else(|| "-".to_string());
            output.push_str(&format!(
                "| {} | {} | {} | {:.2} |\n",
                result.query_name,
                status,
                rows,
                result.execution_time.as_secs_f64() * 1000.0
            ));
        }

        // Failed queries details
        let failed: Vec<_> = self.results.iter().filter(|r| !r.success).collect();
        if !failed.is_empty() {
            output.push_str("\n## Failures\n\n");
            for result in &failed {
                output.push_str(&format!("### {}\n\n", result.query_name));
                if let Some(error) = &result.error {
                    output.push_str(&format!("```\n{}\n```\n\n", error));
                }
            }
        }

        output
    }

    /// Format the report in the specified format.
    pub fn format(&self, format: ReportFormat) -> String {
        match format {
            ReportFormat::Text => self.to_text(),
            ReportFormat::Json => self.to_json(),
            ReportFormat::Markdown => self.to_markdown(),
        }
    }
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        format!("{:width$}", s, width = max_len)
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_summary_from_results() {
        let results = vec![
            ExecutionResult::success(
                "q1".to_string(),
                10,
                Duration::from_millis(100),
                "SELECT 1".to_string(),
                serde_json::json!({}),
            ),
            ExecutionResult::success(
                "q2".to_string(),
                20,
                Duration::from_millis(200),
                "SELECT 2".to_string(),
                serde_json::json!({}),
            ),
            ExecutionResult::failure(
                "q3".to_string(),
                "error".to_string(),
                Duration::from_millis(50),
            ),
        ];

        let summary = Summary::from_results(&results);
        assert_eq!(summary.total_queries, 3);
        assert_eq!(summary.successful, 2);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.total_rows, 30);
    }

    #[test]
    fn test_report_format() {
        let results = vec![ExecutionResult::success(
            "test".to_string(),
            10,
            Duration::from_millis(100),
            "SELECT 1".to_string(),
            serde_json::json!({}),
        )];

        let report = Report::new(results);

        let text = report.to_text();
        assert!(text.contains("QUERY EVALUATION REPORT"));
        assert!(text.contains("test"));

        let json = report.to_json();
        assert!(json.contains("\"query_name\""));

        let md = report.to_markdown();
        assert!(md.contains("# Query Evaluation Report"));
    }
}
