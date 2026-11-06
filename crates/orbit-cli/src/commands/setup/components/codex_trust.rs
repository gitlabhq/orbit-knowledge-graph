use std::path::Path;

use serde_json::Value;

#[cfg(test)]
pub(super) fn trust_hooks(_cwd: &Path, _hooks_file: &Path, _marker: &str) -> Result<usize, String> {
    Ok(0)
}

#[cfg(not(test))]
pub(super) use app_server::trust_hooks;

fn ours<'a>(listed: &'a Value, hooks_file: &Path, marker: &str) -> Vec<&'a Value> {
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
                && hook["command"]
                    .as_str()
                    .is_some_and(|command| command.contains(marker))
        })
        .collect()
}

#[cfg(not(test))]
mod app_server {
    use std::io::{BufRead, BufReader, Write};
    use std::path::Path;
    use std::process::{Child, ChildStdin, Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;

    use serde_json::{Value, json};

    use super::ours;

    const TIMEOUT: Duration = Duration::from_secs(20);

    pub(in crate::commands::setup) fn trust_hooks(
        cwd: &Path,
        hooks_file: &Path,
        marker: &str,
    ) -> Result<usize, String> {
        let mut server = AppServer::start(cwd)?;
        server.request(
            "initialize",
            json!({ "clientInfo": { "name": "orbit-setup", "version": crate::version::get() } }),
        )?;
        server.notify("initialized")?;
        let listed = server.request("hooks/list", json!({ "cwds": [cwd] }))?;
        let hooks = ours(&listed, hooks_file, marker);
        let edits: Vec<Value> = hooks
            .iter()
            .filter(|hook| hook["trustStatus"] != "trusted")
            .map(|hook| {
                json!({
                    "keyPath": format!("hooks.state.\"{}\".trusted_hash", hook["key"].as_str().unwrap_or_default()),
                    "value": hook["currentHash"],
                    "mergeStrategy": "upsert",
                })
            })
            .collect();
        if hooks.is_empty() {
            return Err("trust this folder in Codex, then run setup again".to_string());
        }
        if !edits.is_empty() {
            server.request("config/batchWrite", json!({ "edits": edits }))?;
        }
        Ok(edits.len())
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn only_the_orbit_hooks_in_the_written_file_are_trusted() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("hooks.json");
        std::fs::write(&file, "{}").unwrap();
        let other = dir.path().join("other.json");
        let listed = json!({ "data": [{ "hooks": [
            { "key": "a", "sourcePath": file, "command": "orbit hook-guard instructions" },
            { "key": "b", "sourcePath": file, "command": "their-hook" },
            { "key": "c", "sourcePath": other, "command": "orbit hook-guard instructions" },
        ] }] });

        let keys: Vec<&str> = ours(&listed, &file, "hook-guard")
            .iter()
            .map(|hook| hook["key"].as_str().unwrap())
            .collect();

        assert_eq!(keys, ["a"]);
    }
}
