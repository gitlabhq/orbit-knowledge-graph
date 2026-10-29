use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

pub fn orbit_bin() -> PathBuf {
    let mut p = std::env::current_exe().unwrap();
    p.pop(); // deps
    p.pop(); // debug
    p.push("orbit");
    assert!(p.exists(), "orbit not found at {}", p.display());
    p
}

pub fn orbit_cmd() -> Command {
    let bin = orbit_bin();
    let mut lib = bin.clone();
    lib.pop();
    lib.push("deps");

    let mut cmd = Command::new(&bin);
    cmd.env("DYLD_LIBRARY_PATH", &lib);
    cmd.env("LD_LIBRARY_PATH", &lib);
    // Keep a scratch folder from resolving to a checkout that encloses TMPDIR.
    cmd.env("GIT_CEILING_DIRECTORIES", std::env::temp_dir());
    cmd.current_dir(std::env::temp_dir());
    cmd
}

pub fn orbit_index(repo: &Path, data_dir: &Path) -> bool {
    orbit_cmd()
        .args(["workspace", "index", repo.to_str().unwrap()])
        .env("ORBIT_DATA_DIR", data_dir)
        .output()
        .unwrap()
        .status
        .success()
}

pub fn orbit_sql(sql: &str, data_dir: &Path) -> Value {
    let out = orbit_cmd()
        .args(["workspace", "sql", "-F", "json", sql])
        .current_dir(data_dir)
        .env("ORBIT_DATA_DIR", data_dir)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "sql failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("invalid JSON")
}

pub fn rows(v: &Value) -> Vec<&Value> {
    v.as_array().map(|a| a.iter().collect()).unwrap_or_default()
}

pub fn rows_where<'a>(v: &'a Value, field: &str, val: &str) -> Vec<&'a Value> {
    rows(v)
        .into_iter()
        .filter(|n| n[field].as_str() == Some(val))
        .collect()
}

pub fn sorted_ids(v: &Value) -> Vec<i64> {
    let mut ids: Vec<i64> = rows(v).iter().map(|n| n["id"].as_i64().unwrap()).collect();
    ids.sort();
    ids
}

pub fn git(dir: &Path, args: &[&str]) -> String {
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

/// Use this when the repo must be at a controlled location (e.g.
/// nested inside another repo). For standalone repos, prefer
/// [`create_test_repo`] which uses gitalisk's `LocalGitRepository`.
pub fn init_repo_at(path: &Path, files: &[(&str, &str)]) {
    std::fs::create_dir_all(path).unwrap();
    for (name, content) in files {
        let file_path = path.join(name);
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&file_path, content).unwrap();
    }
    git(path, &["init"]);
    git(path, &["config", "user.email", "test@test.com"]);
    git(path, &["config", "user.name", "Test"]);
    git(path, &["add", "-A"]);
    git(path, &["commit", "-m", "initial"]);
}

pub fn create_test_repo() -> gitalisk_core::repository::testing::local::LocalGitRepository {
    let mut repo = gitalisk_core::repository::testing::local::LocalGitRepository::new(None);
    repo.fs.create_file(
        "src/main.py",
        Some(
            "from utils import read_file\n\n\
             class Base:\n    def setup(self):\n        pass\n\n\
             def hello():\n    print('hello')\n\n\
             class App(Base):\n    def run(self):\n        hello()\n        read_file('x')\n",
        ),
    );
    repo.fs.create_file(
        "src/utils.py",
        Some("import os\n\ndef read_file(path):\n    return open(path).read()\n"),
    );
    repo.add_all().commit("initial");
    repo
}
