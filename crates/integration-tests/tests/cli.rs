//! CLI integration tests.
//!
//! Invokes shell test scripts via the harness in `integration-testkit::cli`,
//! parses their JSON output, and asserts all sub-tests passed.
//!
//! Repo setup uses gitalisk's `LocalGitRepository`. Shell scripts
//! only handle indexing, querying, and assertions.
//!
//! Run with: `cargo nextest run --test cli`

use std::process::Command;

use gitalisk_core::repository::testing::local::LocalGitRepository;
use integration_testkit::cli::{assert_all_passed, run_script};

fn create_test_repo() -> LocalGitRepository {
    let mut repo = LocalGitRepository::new(None);
    repo.fs.create_file(
        "src/main.py",
        Some(
            "def hello():\n    print('hello')\n\nclass App:\n    def run(self):\n        hello()\n",
        ),
    );
    repo.fs.create_file(
        "src/utils.py",
        Some("import os\n\ndef read_file(path):\n    return open(path).read()\n"),
    );
    repo.add_all().commit("initial");
    repo
}

fn git(dir: &std::path::Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[test]
fn cli_concurrency() {
    let repo = create_test_repo();
    let result = run_script("cli-test-concurrency.sh", &[repo.path.to_str().unwrap()]);
    assert_all_passed(&result);
}

#[test]
fn cli_worktree() {
    let repo = create_test_repo();
    let main_sha = git(&repo.path, &["rev-parse", "HEAD"]);
    let main_branch = git(&repo.path, &["symbolic-ref", "--short", "HEAD"]);

    // Feature worktree: adds tests.py
    let wt_feat = repo.workspace_path.join("wt-feat");
    git(
        &repo.path,
        &[
            "worktree",
            "add",
            "-b",
            "feature/tests",
            wt_feat.to_str().unwrap(),
        ],
    );
    std::fs::write(wt_feat.join("src/tests.py"), "def test_hello(): pass\n").unwrap();
    git(&wt_feat, &["add", "-A"]);
    git(&wt_feat, &["commit", "-m", "add tests"]);
    let feat_sha = git(&wt_feat, &["rev-parse", "HEAD"]);

    // Fix worktree: modifies utils.py (from initial commit)
    let wt_fix = repo.workspace_path.join("wt-fix");
    git(
        &repo.path,
        &[
            "worktree",
            "add",
            "-b",
            "fix/utils",
            wt_fix.to_str().unwrap(),
            &main_sha,
        ],
    );
    std::fs::write(wt_fix.join("src/utils.py"), "def patched(): return True\n").unwrap();
    git(&wt_fix, &["add", "-A"]);
    git(&wt_fix, &["commit", "-m", "patch utils"]);
    let fix_sha = git(&wt_fix, &["rev-parse", "HEAD"]);

    let result = run_script(
        "cli-test-worktree.sh",
        &[
            repo.path.to_str().unwrap(),
            wt_feat.to_str().unwrap(),
            wt_fix.to_str().unwrap(),
            &main_branch,
            &main_sha,
            &feat_sha,
            &fix_sha,
        ],
    );
    assert_all_passed(&result);
}
