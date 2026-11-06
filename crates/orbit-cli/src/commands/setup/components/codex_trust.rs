use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use serde_json::{Value, json};

const TIMEOUT: Duration = Duration::from_secs(20);

/// Marks the `SessionStart` hooks setup wrote to `hooks_file` trusted, as Codex's `/hooks`
/// review does, so Codex runs them. Returns how many it trusted. `Ok(None)` means Codex is not
/// on PATH, so there is nothing to ask.
pub(super) fn trust_hooks(
    cwd: &Path,
    hooks_file: &Path,
    commands: &[String],
) -> Result<Option<usize>, String> {
    if !on_path("codex") {
        return Ok(None);
    }
    let folder = cwd.to_str().ok_or_else(|| {
        format!(
            "Codex can't read the folder {}, which is not UTF-8",
            cwd.display()
        )
    })?;
    let mut server = AppServer::start(cwd)?;
    server.request(
        "initialize",
        json!({ "clientInfo": { "name": "orbit-setup", "version": crate::version::get() } }),
    )?;
    server.notify("initialized")?;
    let listed = server.request("hooks/list", json!({ "cwds": [folder] }))?;
    let hooks = ours(&listed, hooks_file, commands);
    if hooks.is_empty() {
        return Err("trust this folder in Codex, then run setup again".to_string());
    }
    let edits: Vec<Value> = hooks
        .iter()
        .filter(|hook| hook["trustStatus"] != "trusted")
        .filter_map(|hook| {
            let key = hook["key"].as_str().filter(|key| !key.is_empty())?;
            Some(json!({
                "keyPath": format!("hooks.state.{}.trusted_hash", quoted_key(key)),
                "value": hook["currentHash"],
                "mergeStrategy": "upsert",
            }))
        })
        .collect();
    if !edits.is_empty() {
        server.request("config/batchWrite", json!({ "edits": edits }))?;
    }
    Ok(Some(edits.len()))
}

fn on_path(program: &str) -> bool {
    std::env::var_os("PATH")
        .iter()
        .flat_map(std::env::split_paths)
        .any(|dir| dir.join(program).is_file() || dir.join(format!("{program}.exe")).is_file())
}

/// The `SessionStart` hooks in `hooks_file` that run exactly a command setup wrote there.
fn ours<'a>(listed: &'a Value, hooks_file: &Path, commands: &[String]) -> Vec<&'a Value> {
    let wanted = dunce::canonicalize(hooks_file).unwrap_or_else(|_| hooks_file.to_path_buf());
    listed["data"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|entry| entry["hooks"].as_array().into_iter().flatten())
        .filter(|hook| {
            let source = hook["sourcePath"].as_str().map(Path::new);
            let same_file = source.is_some_and(|source| {
                dunce::canonicalize(source).unwrap_or_else(|_| source.to_path_buf()) == wanted
            });
            same_file
                && hook["eventName"] == "sessionStart"
                && hook["command"]
                    .as_str()
                    .is_some_and(|command| commands.iter().any(|ours| ours == command))
        })
        .collect()
}

/// A TOML basic-string key, so a hook key holding a Windows path or a quote stays one segment.
fn quoted_key(key: &str) -> String {
    let mut quoted = String::with_capacity(key.len() + 2);
    quoted.push('"');
    for character in key.chars() {
        match character {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            other => quoted.push(other),
        }
    }
    quoted.push('"');
    quoted
}

struct AppServer {
    child: Child,
    stdin: ChildStdin,
    lines: mpsc::Receiver<String>,
    next_id: u64,
}

impl AppServer {
    fn start(cwd: &Path) -> Result<AppServer, String> {
        let mut child = Command::new("codex")
            .arg("app-server")
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("could not start codex app-server: {error}"))?;
        let stdin = child.stdin.take().ok_or("codex app-server has no stdin")?;
        let stdout = child
            .stdout
            .take()
            .ok_or("codex app-server has no stdout")?;
        let (sender, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        Ok(AppServer {
            child,
            stdin,
            lines,
            next_id: 1,
        })
    }

    fn send(&mut self, message: &Value) -> Result<(), String> {
        writeln!(self.stdin, "{message}")
            .and_then(|()| self.stdin.flush())
            .map_err(|error| format!("codex app-server closed: {error}"))
    }

    fn notify(&mut self, method: &str) -> Result<(), String> {
        self.send(&json!({ "jsonrpc": "2.0", "method": method }))
    }

    fn request(&mut self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.next_id;
        self.next_id += 1;
        self.send(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))?;
        loop {
            let line = self
                .lines
                .recv_timeout(TIMEOUT)
                .map_err(|_| format!("codex app-server did not answer {method}"))?;
            let Ok(message) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            if message["id"] != id {
                continue;
            }
            if let Some(error) = message.get("error") {
                return Err(format!(
                    "codex app-server refused {method}: {}",
                    error["message"].as_str().unwrap_or("unknown error")
                ));
            }
            return Ok(message["result"].clone());
        }
    }
}

impl Drop for AppServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_session_start_hook_setup_wrote_is_trusted() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("hooks.json");
        std::fs::write(&file, "{}").unwrap();
        let other = dir.path().join("other.json");
        let ours_command = "orbit hook-guard instructions";
        let hook = |key: &str, path: &Path, event: &str, command: &str| json!({ "key": key, "sourcePath": path, "eventName": event, "command": command });
        let listed = json!({ "data": [{ "hooks": [
            hook("ours", &file, "sessionStart", ours_command),
            hook("lookalike", &file, "sessionStart", "scripts/hook-guard-lint.sh"),
            hook("pre_tool", &file, "preToolUse", ours_command),
            hook("other_file", &other, "sessionStart", ours_command),
        ] }] });

        let keys: Vec<&str> = ours(&listed, &file, &[ours_command.to_string()])
            .iter()
            .map(|hook| hook["key"].as_str().unwrap())
            .collect();

        assert_eq!(keys, ["ours"]);
    }

    #[test]
    fn keys_with_quotes_or_backslashes_stay_one_segment() {
        assert_eq!(quoted_key("a"), r#""a""#);
        assert_eq!(
            quoted_key(r#"C:\h\hooks.json:session_start:0:0"#),
            r#""C:\\h\\hooks.json:session_start:0:0""#
        );
        assert_eq!(quoted_key(r#"x"y"#), r#""x\"y""#);
    }
}
