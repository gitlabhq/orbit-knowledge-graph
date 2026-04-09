//! CLI integration tests.
//!
//! Invokes shell test scripts via the harness in `integration-testkit::cli`,
//! parses their JSON output, and asserts all sub-tests passed.
//!
//! Run with: `cargo nextest run --test cli`

use gitalisk_core::repository::testing::local::LocalGitRepository;
use integration_testkit::cli::{assert_all_passed, run_script};

#[test]
fn cli_concurrency() {
    let mut repo = LocalGitRepository::new(None);
    repo.fs
        .create_file("src/main.py", Some("def hello():\n    pass\n"));
    repo.fs
        .create_file("src/utils.py", Some("def read():\n    pass\n"));
    repo.add_all().commit("initial");
    let result = run_script("cli-test-concurrency.sh", &[repo.path.to_str().unwrap()]);
    assert_all_passed(&result);
}

#[test]
fn cli_worktree() {
    let result = run_script("cli-test-worktree.sh", &[]);
    assert_all_passed(&result);
}
