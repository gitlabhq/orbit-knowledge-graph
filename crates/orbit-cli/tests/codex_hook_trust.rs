#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

use serde_json::{Value, json};

const OURS: &str = "orbit hook-guard instructions";

/// A `codex` that answers `codex app-server` from `hooks.json` (the hooks it lists) and records
/// every `config/batchWrite` request it gets in `writes.jsonl`.
fn fake_codex(bin: &Path, home: &Path, listed: impl Fn(&Path) -> Value) {
    let hooks = listed(&home.join(".codex/hooks.json"));
    std::fs::write(
        bin.join("hooks.json"),
        json!({ "data": [{ "hooks": hooks }] }).to_string(),
    )
    .unwrap();
    let script = format!(
        r#"#!/bin/sh
dir='{dir}'
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{{"jsonrpc":"2.0","id":%s,"result":{{}}}}\n' "$id" ;;
    *'"method":"hooks/list"'*) printf '{{"jsonrpc":"2.0","id":%s,"result":%s}}\n' "$id" "$(cat "$dir/hooks.json")" ;;
    *'"method":"config/batchWrite"'*)
      printf '%s\n' "$line" >> "$dir/writes.jsonl"
      printf '{{"jsonrpc":"2.0","id":%s,"result":{{"status":"ok"}}}}\n' "$id" ;;
  esac
done
"#,
        dir = bin.display()
    );
    let path = bin.join("codex");
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

fn hook(key: &str, path: &Path, event: &str, command: &str, trust: &str) -> Value {
    json!({
        "key": key,
        "sourcePath": path,
        "eventName": event,
        "command": command,
        "currentHash": format!("sha256:{key}"),
        "trustStatus": trust,
    })
}

fn setup_codex(listed: impl Fn(&Path) -> Value) -> (Vec<Value>, String) {
    let home = tempfile::tempdir().unwrap();
    let bin = home.path().join("bin");
    std::fs::create_dir_all(home.path().join(".codex")).unwrap();
    std::fs::create_dir_all(&bin).unwrap();
    fake_codex(&bin, home.path(), listed);
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let output = Command::new(env!("CARGO_BIN_EXE_orbit"))
        .args(["setup", "codex", "--yes", "--no-index"])
        .current_dir(home.path())
        .env("HOME", home.path())
        .env("PATH", path)
        .env("GIT_CEILING_DIRECTORIES", home.path().parent().unwrap())
        .env_remove("CODEX_HOME")
        .env_remove("GITLAB_ORBIT_DISTRIBUTION")
        .output()
        .unwrap();
    let printed = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "{printed}");
    let writes = std::fs::read_to_string(bin.join("writes.jsonl"))
        .unwrap_or_default()
        .lines()
        .flat_map(|line| {
            let request: Value = serde_json::from_str(line).unwrap();
            request["params"]["edits"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        })
        .collect();
    (writes, printed)
}

#[test]
fn setup_trusts_only_the_session_start_hook_it_wrote() {
    let (writes, printed) = setup_codex(|file| {
        json!([
            hook("ours", file, "sessionStart", OURS, "untrusted"),
            hook(
                "lookalike",
                file,
                "sessionStart",
                "scripts/hook-guard-lint.sh",
                "untrusted"
            ),
            hook("pre_tool", file, "preToolUse", OURS, "untrusted"),
        ])
    });

    assert_eq!(
        writes,
        [json!({
            "keyPath": "hooks.state.\"ours\".trusted_hash",
            "value": "sha256:ours",
            "mergeStrategy": "upsert",
        })],
        "{printed}"
    );
    assert!(!printed.contains("WARNING"), "{printed}");
}

#[test]
fn an_already_trusted_hook_writes_nothing() {
    let (writes, printed) =
        setup_codex(|file| json!([hook("ours", file, "sessionStart", OURS, "trusted")]));

    assert!(writes.is_empty(), "{writes:?}");
    assert!(!printed.contains("WARNING"), "{printed}");
}

#[test]
fn a_hook_codex_does_not_list_warns_and_writes_nothing() {
    let (writes, printed) = setup_codex(|_| json!([]));

    assert!(writes.is_empty(), "{writes:?}");
    assert!(
        printed.contains("Codex skips the Orbit hook; trust this folder in Codex"),
        "{printed}"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn a_folder_that_is_not_utf8_warns_instead_of_panicking() {
    use std::os::unix::ffi::OsStrExt;

    let home = tempfile::tempdir().unwrap();
    let bin = home.path().join("bin");
    std::fs::create_dir_all(home.path().join(".codex")).unwrap();
    std::fs::create_dir_all(&bin).unwrap();
    fake_codex(&bin, home.path(), |_| json!([]));
    let project = home.path().join(std::ffi::OsStr::from_bytes(b"repo-\xff"));
    std::fs::create_dir_all(&project).unwrap();
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let output = Command::new(env!("CARGO_BIN_EXE_orbit"))
        .args([
            "setup",
            "codex",
            "--yes",
            "--no-index",
            "--project",
            "--dir",
        ])
        .arg(&project)
        .current_dir(home.path())
        .env("HOME", home.path())
        .env("PATH", path)
        .env("GIT_CEILING_DIRECTORIES", home.path().parent().unwrap())
        .env_remove("CODEX_HOME")
        .env_remove("GITLAB_ORBIT_DISTRIBUTION")
        .output()
        .unwrap();
    let printed = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(output.status.success(), "{printed}");
    assert!(!printed.contains("panicked"), "{printed}");
    assert!(
        printed.contains("Codex skips the Orbit hook; Codex can't read the folder"),
        "{printed}"
    );
    assert!(!bin.join("writes.jsonl").exists());
}
