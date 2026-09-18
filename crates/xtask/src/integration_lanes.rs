use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::process::Command;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

const CI_CONFIG: &str = ".gitlab-ci.yml";
const LANES: [&str; 3] = [
    "integration-test",
    "integration-test-data-correctness",
    "corpus-smoke-test",
];

#[derive(Deserialize)]
struct LaneJob {
    variables: LaneVariables,
}

#[derive(Deserialize)]
struct LaneVariables {
    #[serde(rename = "NEXTEST_FILTER")]
    nextest_filter: String,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct PartitionIssues {
    missing: BTreeSet<String>,
    duplicates: BTreeMap<String, Vec<String>>,
}

pub fn run(check: bool) -> Result<()> {
    if !check {
        bail!("pass --check to verify the integration test lane partition");
    }

    let config = fs::read_to_string(CI_CONFIG).context("reading .gitlab-ci.yml")?;
    let config: BTreeMap<String, serde_json::Value> =
        orbit_utils::yaml::from_str(&config).context("parsing .gitlab-ci.yml")?;
    let all_tests = list_tests(None)?;
    if all_tests.is_empty() {
        bail!("cargo nextest listed no container tests");
    }
    let mut lane_tests = BTreeMap::new();

    for lane in LANES {
        let job = config
            .get(lane)
            .with_context(|| format!("missing integration lane job {lane} in .gitlab-ci.yml"))?;
        let job: LaneJob = serde_json::from_value(job.clone())
            .with_context(|| format!("parsing integration lane job {lane}"))?;
        let filter = job.variables.nextest_filter;
        if filter.trim().is_empty() {
            bail!("{lane}.variables.NEXTEST_FILTER must not be empty");
        }
        let tests = list_tests(Some(&filter))?;
        println!("{lane}: {} tests", tests.len());
        lane_tests.insert(lane.to_string(), tests);
    }

    let issues = partition_issues(&all_tests, &lane_tests);
    if issues == PartitionIssues::default() {
        println!(
            "All {} container tests are assigned to exactly one integration lane.",
            all_tests.len()
        );
        return Ok(());
    }

    report_issues(&issues);
    bail!("integration lane filters do not cover every container test exactly once")
}

fn list_tests(filter: Option<&str>) -> Result<BTreeSet<String>> {
    let mut command = Command::new("cargo");
    command.args([
        "nextest",
        "list",
        "--all-features",
        "--test",
        "containers",
        "-p",
        "integration-tests",
        "--message-format",
        "oneline",
    ]);
    if let Some(filter) = filter {
        command.args(["-E", filter]);
    }

    let output = command
        .output()
        .context("running cargo nextest list for container tests")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("cargo nextest list failed:\n{}", stderr.trim());
    }

    let stdout = String::from_utf8(output.stdout).context("decoding cargo nextest output")?;
    parse_test_list(&stdout)
}

fn parse_test_list(stdout: &str) -> Result<BTreeSet<String>> {
    stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.split_once(char::is_whitespace)
                .map(|(_, test)| test.trim().to_string())
                .filter(|test| !test.is_empty())
                .with_context(|| format!("unexpected cargo nextest list output: {line}"))
        })
        .collect()
}

fn partition_issues(
    all_tests: &BTreeSet<String>,
    lane_tests: &BTreeMap<String, BTreeSet<String>>,
) -> PartitionIssues {
    let mut assignments: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (lane, tests) in lane_tests {
        for test in tests {
            assignments.entry(test).or_default().push(lane);
        }
    }

    let missing = all_tests
        .iter()
        .filter(|test| !assignments.contains_key(test.as_str()))
        .cloned()
        .collect();
    let duplicates = assignments
        .into_iter()
        .filter(|(test, lanes)| all_tests.contains(*test) && lanes.len() > 1)
        .map(|(test, lanes)| {
            (
                test.to_string(),
                lanes.into_iter().map(str::to_string).collect(),
            )
        })
        .collect();

    PartitionIssues {
        missing,
        duplicates,
    }
}

fn report_issues(issues: &PartitionIssues) {
    if !issues.missing.is_empty() {
        eprintln!("Container tests missing from every integration lane:");
        for test in &issues.missing {
            eprintln!("  {test}");
        }
    }
    if !issues.duplicates.is_empty() {
        eprintln!("Container tests assigned to more than one integration lane:");
        for (test, lanes) in &issues.duplicates {
            eprintln!("  {test}: {}", lanes.join(", "));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn parses_nextest_oneline_output() {
        let output = "integration-tests::containers first::test\n\nintegration-tests::containers second::test\n";

        assert_eq!(
            parse_test_list(output).unwrap(),
            set(&["first::test", "second::test"])
        );
    }

    #[test]
    fn rejects_malformed_nextest_oneline_output() {
        assert!(parse_test_list("missing-separator").is_err());
        assert!(parse_test_list("integration-tests::containers  ").is_err());
    }

    #[test]
    fn complete_partition_has_no_issues() {
        let all = set(&["one", "two", "three"]);
        let lanes = BTreeMap::from([
            ("normal".to_string(), set(&["one", "two"])),
            ("corpus".to_string(), set(&["three"])),
        ]);

        assert_eq!(partition_issues(&all, &lanes), PartitionIssues::default());
    }

    #[test]
    fn reports_tests_missing_from_every_lane() {
        let all = set(&["one", "two", "three"]);
        let lanes = BTreeMap::from([
            ("normal".to_string(), set(&["one"])),
            ("corpus".to_string(), set(&["three"])),
        ]);

        let issues = partition_issues(&all, &lanes);

        assert_eq!(issues.missing, set(&["two"]));
        assert!(issues.duplicates.is_empty());
    }

    #[test]
    fn reports_tests_assigned_to_multiple_lanes() {
        let all = set(&["one", "two", "three"]);
        let lanes = BTreeMap::from([
            ("normal".to_string(), set(&["one", "two"])),
            ("corpus".to_string(), set(&["two", "three"])),
        ]);

        let issues = partition_issues(&all, &lanes);

        assert!(issues.missing.is_empty());
        assert_eq!(
            issues.duplicates,
            BTreeMap::from([(
                "two".to_string(),
                vec!["corpus".to_string(), "normal".to_string()]
            )])
        );
    }
}
