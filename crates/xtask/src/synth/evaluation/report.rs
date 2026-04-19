//! Report generation for evaluation results.

use super::ExecutionResult;
use serde::{Deserialize, Serialize};
use sqlformat::{FormatOptions, QueryParams};
use std::time::Duration;
use tabled::settings::Style;
use tabled::{Table, Tabled};

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
    pub empty_results: usize,
    pub total_rows: u64,
    pub total_time: Duration,
    pub avg_time: Duration,
    pub min_time: Duration,
    pub max_time: Duration,
    pub errors_by_category: std::collections::HashMap<String, usize>,
}

/// Wrapper for Duration to display as seconds.
struct DurationSecs(Duration);

impl std::fmt::Display for DurationSecs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.2}s", self.0.as_secs_f64())
    }
}

/// Wrapper for Duration to display as milliseconds.
struct DurationMs(Duration);

impl std::fmt::Display for DurationMs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.2}ms", self.0.as_secs_f64() * 1000.0)
    }
}

/// Display row for the summary table.
#[derive(Tabled)]
struct SummaryRow {
    total_queries: usize,
    successful: usize,
    failed: usize,
    empty_results: usize,
    total_rows: u64,
    total_time: DurationSecs,
    avg_time: DurationMs,
    min_time: DurationMs,
    max_time: DurationMs,
}

impl SummaryRow {
    fn from_summary(summary: &Summary) -> Self {
        Self {
            total_queries: summary.total_queries,
            successful: summary.successful,
            failed: summary.failed,
            empty_results: summary.empty_results,
            total_rows: summary.total_rows,
            total_time: DurationSecs(summary.total_time),
            avg_time: DurationMs(summary.avg_time),
            min_time: DurationMs(summary.min_time),
            max_time: DurationMs(summary.max_time),
        }
    }
}

impl Summary {
    pub fn from_results(results: &[ExecutionResult]) -> Self {
        let total_queries = results.len();
        let successful = results.iter().filter(|r| r.success).count();
        let failed = total_queries - successful;
        let empty_results = results
            .iter()
            .filter(|r| r.success && r.row_count == Some(0))
            .count();

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

        // Count errors by category
        let mut errors_by_category = std::collections::HashMap::new();
        for result in results.iter().filter(|r| !r.success) {
            let category = result
                .error_category()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "UNKNOWN".to_string());
            *errors_by_category.entry(category).or_insert(0) += 1;
        }

        Self {
            total_queries,
            successful,
            failed,
            empty_results,
            total_rows,
            total_time,
            avg_time,
            min_time,
            max_time,
            errors_by_category,
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

        output.push_str("QUERY EVALUATION REPORT\n\n");

        // Summary table
        let summary_row = SummaryRow::from_summary(&self.summary);
        let mut table = Table::new(vec![summary_row]);
        table.with(Style::rounded());
        output.push_str(&table.to_string());
        output.push_str(&format!(
            "\nSuccess Rate: {:.1}%\n",
            self.summary.success_rate()
        ));

        // Error breakdown if any failures
        if !self.summary.errors_by_category.is_empty() {
            output.push_str("\nErrors by Category:\n");
            let mut categories: Vec<_> = self.summary.errors_by_category.iter().collect();
            categories.sort_by(|a, b| b.1.cmp(a.1));
            for (category, count) in categories {
                output.push_str(&format!("  {} x{}\n", category, count));
            }
        }

        // Show empty results (potential issues)
        let empty: Vec<_> = self
            .results
            .iter()
            .filter(|r| r.success && r.row_count == Some(0))
            .collect();
        if !empty.is_empty() {
            output.push_str(&format!("\n⚠ {} queries returned 0 rows:\n", empty.len()));
            for result in &empty {
                output.push_str(&format!("  • {}\n", result.query_name));
            }
        }

        // Show failed queries with details
        let failed: Vec<_> = self.results.iter().filter(|r| !r.success).collect();
        if !failed.is_empty() {
            output.push_str(&format!("\n✗ {} failed queries:\n", failed.len()));

            for result in &failed {
                output.push_str(&format!("\n  {}\n", result.query_name));
                if let Some(parsed) = &result.parsed_error {
                    output.push_str(&format!("  Error: {}\n", parsed.summary));
                }
                if let Some(sql) = &result.sql {
                    output.push_str(&format_sql(sql));
                }
            }
        }

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
        sorted_results.sort_by_key(|r| std::cmp::Reverse(r.execution_time));

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

/// Format SQL for pretty display.
fn format_sql(sql: &str) -> String {
    let formatted = sqlformat::format(sql, &QueryParams::None, &FormatOptions::default());

    // Indent each line for display
    formatted
        .lines()
        .map(|line| format!("  {}", line))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
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
                vec![],
                vec![],
                Duration::from_millis(100),
                "SELECT 1".to_string(),
                serde_json::json!({}),
                None,
            ),
            ExecutionResult::success(
                "q2".to_string(),
                20,
                vec![],
                vec![],
                Duration::from_millis(200),
                "SELECT 2".to_string(),
                serde_json::json!({}),
                None,
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
        let results = vec![
            ExecutionResult::success(
                "test_success".to_string(),
                10,
                vec![vec!["val".to_string()]],
                vec!["col".to_string()],
                Duration::from_millis(100),
                "SELECT 1".to_string(),
                serde_json::json!({}),
                None,
            ),
            ExecutionResult::failure(
                "test_failure".to_string(),
                "Something went wrong".to_string(),
                Duration::from_millis(50),
            ),
        ];

        let report = Report::new(results);

        let text = report.to_text();
        assert!(text.contains("QUERY EVALUATION REPORT"));
        assert!(text.contains("Success Rate"));
        assert!(text.contains("test_failure"));

        let json = report.to_json();
        assert!(json.contains("\"query_name\""));
        assert!(json.contains("test_success"));
        assert!(json.contains("test_failure"));

        let md = report.to_markdown();
        assert!(md.contains("# Query Evaluation Report"));
        assert!(md.contains("test_success"));
    }
}
