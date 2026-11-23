use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use code_graph_incremental::language::SupportLang;
use code_graph_incremental::{Env, Killed, Limits, Sentinel, driver};
mod common;

const MAIN: &str = "\
from utils import helper

class Greeter:
    def greet(self, name):
        return helper(name)
";

const UTILS: &str = "\
def helper(x):
    return x

class Store:
    pass
";

const BUDGET_MS: u64 = 20;

fn sleeps_always() -> Sentinel {
    Sentinel::new("run", "", u64::MAX).with_cancellation(Arc::new(|| {
        std::thread::sleep(Duration::from_millis(BUDGET_MS / 4));
        false
    }))
}

#[test]
fn per_file_budgets_count_thread_cpu_time_not_time_asleep() {
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), &[("main.py", MAIN), ("utils.py", UTILS)]);
    let env = Env::builder(SupportLang::Python)
        .limits(Limits {
            file_parse_ms: BUDGET_MS,
            file_rewrite_ms: BUDGET_MS,
            file_link_ms: BUDGET_MS,
            ..Limits::UNLIMITED
        })
        .build()
        .unwrap();

    let (_, report) = driver::index(&env, &common::walk(repo.path()), &sleeps_always()).unwrap();

    assert_eq!(skipped(&report.skipped), Vec::<(&str, &str)>::new());
}

fn write_all(root: &Path, files: &[(&str, &str)]) {
    for (path, content) in files {
        std::fs::write(root.join(path), content).unwrap();
    }
}

fn skipped(killed: &[Killed]) -> Vec<(&str, &str)> {
    let mut found: Vec<_> = killed
        .iter()
        .map(|k| (k.label.as_ref(), k.path.as_str()))
        .collect();
    found.sort();
    found
}
