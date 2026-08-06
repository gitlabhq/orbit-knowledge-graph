//! Extraction is CPU-bound gzip and tar running on the blocking pool, so a wide parse on the
//! same pod steals its cores and stretches it. Compares medians across repetitions rather
//! than racing a deadline, so it does not depend on timing margins.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use gkg_server_config::CodeIndexingPipelineConfig;
use indexer::handler::Handler;
use indexer::topic::CodeIndexingTaskRequest;
use indexer::types::Envelope;

use super::helpers::{CodeIndexingDeps, MockGitlabServer, handler_context};

const VICTIM_ID: i64 = 5100;
const LOAD_ID: i64 = 5200;
const VICTIM_FILES: usize = 12_000;
const LOAD_FILES: usize = 3_000;
const LOAD_METHODS: usize = 80;
const REPS: usize = 5;
const NARROW_DIVISOR: usize = 4;
const MIN_SLOWDOWN: f64 = 1.3;

fn envelope(project_id: i64) -> Envelope {
    Envelope::new(&CodeIndexingTaskRequest {
        task_id: 0,
        project_id,
        branch: Some("main".to_string()),
        commit_sha: None,
        traversal_path: format!("1/{project_id}/"),
        dispatch_id: uuid::Uuid::new_v4(),
        campaign_id: None,
    })
    .expect("envelope")
}

/// Many tiny files: extraction dominates and the parse is negligible, so the measured wall
/// time tracks extraction.
fn victim_files() -> Vec<(String, String)> {
    (0..VICTIM_FILES)
        .map(|i| (format!("d{}/n{}.txt", i % 60, i), format!("line {i}\n")))
        .collect()
}

/// Few files, expensive to parse: saturates the rayon pool without adding extraction work.
fn load_files() -> Vec<(String, String)> {
    (0..LOAD_FILES)
        .map(|i| {
            let mut body = format!("package p{};\n\npublic class C{} {{\n", i % 30, i);
            for m in 0..LOAD_METHODS {
                body.push_str(&format!(
                    "  public int m{m}(int a, int b) {{ int c = a + b; for (int i = 0; i < b; i++) {{ c += i * a; }} return c; }}\n"
                ));
            }
            body.push_str("}\n");
            (format!("src/p{}/C{}.java", i % 30, i), body)
        })
        .collect()
}

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

async fn slowdown_at_parse_width(worker_threads: usize) -> f64 {
    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let victim = victim_files();
    let load = load_files();
    fn as_refs(v: &[(String, String)]) -> Vec<(&str, &str)> {
        v.iter().map(|(p, c)| (p.as_str(), c.as_str())).collect()
    }
    let mock = MockGitlabServer::start().await;
    mock.add_project(VICTIM_ID, "main", &as_refs(&victim));
    mock.add_project(LOAD_ID, "main", &as_refs(&load));

    let deps = CodeIndexingDeps::new_with_pipeline_config(
        &mock,
        &clickhouse,
        CodeIndexingPipelineConfig {
            job_timeout_secs: 0,
            worker_threads,
            ..Default::default()
        },
    );
    let handler = Arc::new(deps.code_indexing_task_handler());

    let mut idle = Vec::new();
    for _ in 0..REPS {
        let started = Instant::now();
        let _ = handler.handle(handler_context(), envelope(VICTIM_ID)).await;
        idle.push(started.elapsed());
    }

    let stop = Arc::new(AtomicBool::new(false));
    let loader = {
        let (handler, stop) = (handler.clone(), stop.clone());
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let _ = handler.handle(handler_context(), envelope(LOAD_ID)).await;
            }
        })
    };
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut loaded = Vec::new();
    for _ in 0..REPS {
        let started = Instant::now();
        let _ = handler.handle(handler_context(), envelope(VICTIM_ID)).await;
        loaded.push(started.elapsed());
    }
    stop.store(true, Ordering::Relaxed);
    loader.abort();

    let idle_median = median(idle).as_secs_f64();
    let loaded_median = median(loaded).as_secs_f64();
    let slowdown = loaded_median / idle_median.max(1e-9);
    println!(
        "worker_threads={worker_threads}: idle={idle_median:.2}s loaded={loaded_median:.2}s slowdown={slowdown:.2}x"
    );
    slowdown
}

#[tokio::test]
async fn a_concurrent_parse_stretches_extraction_through_the_handler() {
    let slowdown = slowdown_at_parse_width(0).await;
    assert!(
        slowdown > MIN_SLOWDOWN,
        "a parse using every core must measurably stretch extraction, got {slowdown:.2}x"
    );
}

#[tokio::test]
async fn narrowing_the_parse_relieves_extraction() {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    // Below this there is no width to give back, so the comparison would be vacuous.
    if cores < NARROW_DIVISOR * 2 {
        println!("skipping: {cores} cores leaves nothing to narrow");
        return;
    }
    let wide = slowdown_at_parse_width(0).await;
    let narrow = slowdown_at_parse_width(cores / NARROW_DIVISOR).await;
    assert!(
        narrow < wide,
        "narrowing the parse must leave extraction less starved; wide={wide:.2}x narrow={narrow:.2}x"
    );
}
