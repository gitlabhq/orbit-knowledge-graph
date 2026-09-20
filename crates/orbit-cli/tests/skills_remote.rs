use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::process::{Command, Output};
use std::thread;
use std::time::Duration;

use sha2::{Digest, Sha256};

const ACCEPT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct Reply {
    status: u16,
    reason: &'static str,
    etag: Option<String>,
    body: String,
}

#[derive(Debug)]
struct Request {
    line: String,
    headers: BTreeMap<String, String>,
}

fn mock_server(replies: Vec<Reply>) -> (String, thread::JoinHandle<Vec<Request>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let base_url = format!("http://{address}");
    let handle = thread::spawn(move || {
        let mut requests = Vec::new();
        listener.set_nonblocking(true).unwrap();
        for reply in replies {
            let deadline = std::time::Instant::now() + ACCEPT_TIMEOUT;
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "mock server timed out"
                        );
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("mock accept failed: {error}"),
                }
            };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let request_line = line.trim_end().to_string();
            let mut headers = BTreeMap::new();
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                let trimmed = line.trim_end();
                if trimmed.is_empty() {
                    break;
                }
                if let Some((name, value)) = trimmed.split_once(':') {
                    headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
                }
            }
            requests.push(Request {
                line: request_line,
                headers,
            });
            let etag = reply
                .etag
                .as_ref()
                .map(|value| format!("ETag: {value}\r\n"))
                .unwrap_or_default();
            let response = format!(
                "HTTP/1.1 {} {}\r\n{etag}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                reply.status,
                reply.reason,
                reply.body.len(),
                reply.body
            );
            stream.write_all(response.as_bytes()).unwrap();
            stream.flush().unwrap();
        }
        requests
    });
    (base_url, handle)
}

fn run_orbit(base_url: Option<&str>, cache: &tempfile::TempDir, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_orbit"));
    command.args(args).env("XDG_CACHE_HOME", cache.path());
    for key in [
        "ORBIT_API_BASE_URL",
        "ORBIT_AUTH_HEADER_NAME",
        "ORBIT_AUTH_HEADER_VALUE",
        "GITLAB_TOKEN",
    ] {
        command.env_remove(key);
    }
    if let Some(base_url) = base_url {
        command
            .env("ORBIT_API_BASE_URL", base_url)
            .env("ORBIT_AUTH_HEADER_NAME", "Private-Token")
            .env("ORBIT_AUTH_HEADER_VALUE", "glpat-test");
    }
    command.output().unwrap()
}

fn tree_reply(version: &str, body_text: &str) -> Reply {
    let manifest = format!(
        "---\nname: orbit\nversion: {version}\ndescription: Remote Orbit skill\n---\n# {body_text}\n"
    );
    let reference = format!("{body_text}\n");
    let files = BTreeMap::from([
        ("SKILL.md", manifest.as_str()),
        ("references/remote.md", reference.as_str()),
    ]);
    let tree_hash = tree_hash(&files);
    let body = serde_json::json!({
        "name": "orbit",
        "version": version,
        "tree_sha256": tree_hash,
        "files": files.iter().map(|(path, content)| serde_json::json!({
            "path": path,
            "sha256": hex(Sha256::digest(content.as_bytes())),
            "content": content,
        })).collect::<Vec<_>>()
    })
    .to_string();
    Reply {
        status: 200,
        reason: "OK",
        etag: Some(format!("\"{version}:{tree_hash}\"")),
        body,
    }
}

fn tree_hash(files: &BTreeMap<&str, &str>) -> String {
    let mut hasher = Sha256::new();
    for (path, content) in files {
        hasher.update(path.as_bytes());
        hasher.update([0]);
        hasher.update((content.len() as u64).to_be_bytes());
        hasher.update(content.as_bytes());
    }
    hex(hasher.finalize())
}

fn hex(digest: impl AsRef<[u8]>) -> String {
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn cache_miss_then_304_downloads_content_exactly_once() {
    let cache = tempfile::tempdir().unwrap();
    let first = tree_reply("1.0.0", "Remote v1");
    let etag = first.etag.clone().unwrap();
    let not_modified = Reply {
        status: 304,
        reason: "Not Modified",
        etag: Some(etag.clone()),
        body: String::new(),
    };
    let (url, server) = mock_server(vec![first, not_modified]);

    let miss = run_orbit(Some(&url), &cache, &["skills", "get", "orbit"]);
    let hit = run_orbit(Some(&url), &cache, &["skills", "get", "orbit"]);
    assert!(miss.status.success(), "{}", stderr(&miss));
    assert!(hit.status.success(), "{}", stderr(&hit));
    assert_eq!(miss.stdout, hit.stdout);
    assert!(String::from_utf8_lossy(&hit.stdout).contains("Remote v1"));

    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].line, "GET /api/v4/orbit/skills/orbit HTTP/1.1");
    assert!(!requests[0].headers.contains_key("if-none-match"));
    assert_eq!(requests[1].headers.get("if-none-match"), Some(&etag));
}

#[test]
fn remote_listing_uses_collection_and_authentication() {
    let cache = tempfile::tempdir().unwrap();
    let reply = Reply {
        status: 200,
        reason: "OK",
        etag: None,
        body: r#"{"skills":[{"name":"orbit","version":"1.0.0","tree_sha256":"abc","description":"Remote description"}]}"#.to_string(),
    };
    let (url, server) = mock_server(vec![reply]);
    let output = run_orbit(Some(&url), &cache, &["skills"]);
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(output.stdout, b"orbit \xE2\x80\x94 Remote description\n");
    let requests = server.join().unwrap();
    assert_eq!(requests[0].line, "GET /api/v4/orbit/skills HTTP/1.1");
    assert_eq!(
        requests[0].headers.get("private-token").map(String::as_str),
        Some("glpat-test")
    );
}

#[test]
fn no_credentials_is_a_silent_local_only_mode() {
    let cache = tempfile::tempdir().unwrap();
    let output = run_orbit(None, &cache, &["skills", "get", "orbit"]);
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(output.stderr.is_empty());
    assert!(String::from_utf8_lossy(&output.stdout).contains("name: orbit-cli"));

    let partial = Command::new(env!("CARGO_BIN_EXE_orbit"))
        .args(["skills"])
        .env("XDG_CACHE_HOME", cache.path())
        .env("ORBIT_API_BASE_URL", "http://127.0.0.1:1")
        .env_remove("ORBIT_AUTH_HEADER_NAME")
        .env_remove("ORBIT_AUTH_HEADER_VALUE")
        .output()
        .unwrap();
    assert!(partial.status.success(), "{}", stderr(&partial));
    assert!(partial.stderr.is_empty());
}

#[test]
fn endpoint_404_warns_and_falls_back_to_local() {
    let cache = tempfile::tempdir().unwrap();
    let (url, server) = mock_server(vec![Reply {
        status: 404,
        reason: "Not Found",
        etag: None,
        body: r#"{"message":"unknown endpoint"}"#.to_string(),
    }]);
    let output = run_orbit(Some(&url), &cache, &["skills", "get", "orbit"]);
    server.join().unwrap();
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(stderr(&output).contains("does not serve Orbit skills"));
    assert!(String::from_utf8_lossy(&output.stdout).contains("name: orbit-cli"));
}

#[test]
fn offline_uses_last_validated_host_tree() {
    let cache = tempfile::tempdir().unwrap();
    let (url, server) = mock_server(vec![tree_reply("1.0.0", "Cached remote")]);
    let first = run_orbit(Some(&url), &cache, &["skills", "get", "orbit"]);
    server.join().unwrap();
    assert!(first.status.success(), "{}", stderr(&first));

    let offline = run_orbit(Some(&url), &cache, &["skills", "get", "orbit"]);
    assert!(offline.status.success(), "{}", stderr(&offline));
    assert!(stderr(&offline).contains("last validated"));
    assert!(String::from_utf8_lossy(&offline.stdout).contains("Cached remote"));
}

#[test]
fn auth_errors_never_fall_back() {
    for (status, expected_exit) in [(401, 3), (403, 4)] {
        let cache = tempfile::tempdir().unwrap();
        let (url, server) = mock_server(vec![Reply {
            status,
            reason: "Denied",
            etag: None,
            body: "denied".to_string(),
        }]);
        let output = run_orbit(Some(&url), &cache, &["skills", "get", "orbit"]);
        server.join().unwrap();
        assert_eq!(output.status.code(), Some(expected_exit));
        assert!(output.stdout.is_empty());
    }
}

#[test]
fn transient_server_error_uses_last_validated_tree() {
    let cache = tempfile::tempdir().unwrap();
    let (url, server) = mock_server(vec![
        tree_reply("1.0.0", "Cached after 5xx"),
        Reply {
            status: 503,
            reason: "Unavailable",
            etag: None,
            body: "down".to_string(),
        },
    ]);
    let first = run_orbit(Some(&url), &cache, &["skills", "get", "orbit"]);
    let fallback = run_orbit(Some(&url), &cache, &["skills", "get", "orbit"]);
    server.join().unwrap();
    assert!(first.status.success(), "{}", stderr(&first));
    assert!(fallback.status.success(), "{}", stderr(&fallback));
    assert!(stderr(&fallback).contains("last validated"));
    assert!(String::from_utf8_lossy(&fallback.stdout).contains("Cached after 5xx"));
}

#[test]
fn server_error_without_cache_is_returned() {
    let cache = tempfile::tempdir().unwrap();
    let (url, server) = mock_server(vec![Reply {
        status: 503,
        reason: "Unavailable",
        etag: None,
        body: "down".to_string(),
    }]);
    let output = run_orbit(Some(&url), &cache, &["skills", "get", "orbit"]);
    server.join().unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(stderr(&output).contains("service unavailable"));
}

#[test]
fn same_version_different_hash_warns_and_replaces() {
    let cache = tempfile::tempdir().unwrap();
    let (url, server) = mock_server(vec![
        tree_reply("1.0.0", "First tree"),
        tree_reply("1.0.0", "Replacement tree"),
    ]);
    let first = run_orbit(Some(&url), &cache, &["skills", "get", "orbit"]);
    let second = run_orbit(Some(&url), &cache, &["skills", "get", "orbit"]);
    server.join().unwrap();
    assert!(first.status.success(), "{}", stderr(&first));
    assert!(second.status.success(), "{}", stderr(&second));
    assert!(stderr(&second).contains("changed tree hash"));
    assert!(String::from_utf8_lossy(&second.stdout).contains("Replacement tree"));
}

#[test]
fn unsafe_remote_path_fails_without_populating_cache() {
    let cache = tempfile::tempdir().unwrap();
    let content = "bad";
    let body = serde_json::json!({
        "name": "orbit",
        "version": "1.0.0",
        "tree_sha256": "0".repeat(64),
        "files": [{
            "path": "../secret",
            "sha256": hex(Sha256::digest(content.as_bytes())),
            "content": content
        }]
    })
    .to_string();
    let (url, server) = mock_server(vec![Reply {
        status: 200,
        reason: "OK",
        etag: Some("\"unsafe\"".to_string()),
        body,
    }]);
    let output = run_orbit(Some(&url), &cache, &["skills", "get", "orbit"]);
    server.join().unwrap();
    assert!(!output.status.success());
    assert!(stderr(&output).contains("unsafe") || stderr(&output).contains("not normalized"));
    let files = walk_files(cache.path());
    assert!(
        files.is_empty(),
        "unsafe response wrote cache files: {files:?}"
    );
}

#[test]
fn cache_is_isolated_by_instance_origin() {
    let cache = tempfile::tempdir().unwrap();
    let (first_url, first_server) = mock_server(vec![tree_reply("1.0.0", "Host one")]);
    let (second_url, second_server) = mock_server(vec![tree_reply("1.0.0", "Host two")]);
    let first = run_orbit(Some(&first_url), &cache, &["skills", "get", "orbit"]);
    let second = run_orbit(Some(&second_url), &cache, &["skills", "get", "orbit"]);
    first_server.join().unwrap();
    second_server.join().unwrap();
    assert!(first.status.success(), "{}", stderr(&first));
    assert!(second.status.success(), "{}", stderr(&second));
    assert!(String::from_utf8_lossy(&first.stdout).contains("Host one"));
    assert!(String::from_utf8_lossy(&second.stdout).contains("Host two"));

    let manifests = walk_files(cache.path())
        .into_iter()
        .filter(|path| path.ends_with(".orbit-cache.json"))
        .collect::<Vec<_>>();
    assert_eq!(manifests.len(), 2, "{manifests:?}");
}

fn walk_files(root: &std::path::Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut files = Vec::new();
    for entry in entries.flatten() {
        if entry.path().is_dir() {
            files.extend(walk_files(&entry.path()));
        } else {
            files.push(entry.path().display().to_string());
        }
    }
    files
}
