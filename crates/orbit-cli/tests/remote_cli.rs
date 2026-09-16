use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::thread;
use std::time::Duration;

const ACCEPT_TIMEOUT: Duration = Duration::from_secs(10);

struct CapturedRequest {
    request_line: String,
    auth_header: Option<String>,
    body: String,
}

fn serve_once(
    response_body: &'static str,
    content_type: &'static str,
) -> (String, thread::JoinHandle<CapturedRequest>) {
    serve_response("200 OK", response_body, content_type)
}

fn serve_response(
    status: &'static str,
    response_body: &'static str,
    content_type: &'static str,
) -> (String, thread::JoinHandle<CapturedRequest>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
    let addr = listener.local_addr().expect("mock addr");
    let base_url = format!("http://{addr}");

    let handle = thread::spawn(move || {
        let mut stream = accept_within(&listener, ACCEPT_TIMEOUT);
        let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));

        let mut request_line = String::new();
        reader
            .read_line(&mut request_line)
            .expect("read request line");

        let mut auth_header = None;
        let mut content_length = 0usize;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).expect("read header line");
            let trimmed = line.trim_end();
            if trimmed.is_empty() {
                break;
            }
            if let Some((name, value)) = trimmed.split_once(':') {
                let name = name.trim().to_ascii_lowercase();
                let value = value.trim().to_string();
                if name == "private-token" {
                    auth_header = Some(value.clone());
                } else if name == "content-length" {
                    content_length = value.parse().expect("parse content-length");
                }
            }
        }

        let mut body = vec![0u8; content_length];
        if content_length > 0 {
            reader.read_exact(&mut body).expect("read request body");
        }

        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
            response_body.len()
        );
        stream
            .write_all(response.as_bytes())
            .expect("write response");
        stream.flush().expect("flush response");

        CapturedRequest {
            request_line: request_line.trim_end().to_string(),
            auth_header,
            body: String::from_utf8(body).expect("utf8 body"),
        }
    });

    (base_url, handle)
}

fn accept_within(listener: &TcpListener, timeout: Duration) -> std::net::TcpStream {
    listener
        .set_nonblocking(true)
        .expect("set non-blocking accept");
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream
                    .set_nonblocking(false)
                    .expect("restore blocking stream");
                return stream;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "orbit never connected to the mock server within {timeout:?}; \
                     the CLI most likely rejected the argv before sending a request"
                );
                thread::sleep(Duration::from_millis(10));
            }
            Err(e) => panic!("accept mock connection: {e}"),
        }
    }
}

fn orbit_command(base_url: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_orbit"));
    command
        .env("ORBIT_API_BASE_URL", base_url)
        .env("ORBIT_AUTH_HEADER_NAME", "Private-Token")
        .env("ORBIT_AUTH_HEADER_VALUE", "glpat-test")
        .env("ORBIT_TELEMETRY_ENABLED", "false")
        .env_remove("GITLAB_TOKEN");
    command
}

fn run_orbit(base_url: &str, args: &[&str]) -> std::process::Output {
    orbit_command(base_url)
        .args(args)
        .output()
        .expect("run orbit binary")
}

#[test]
fn ontology_sends_get_with_expand_and_auth_header() {
    let (base_url, handle) = serve_once(r#"{"schema_version":"1"}"#, "application/json");
    let output = run_orbit(&base_url, &["ontology", "User", "Project"]);
    let request = handle.join().expect("join mock");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        request.request_line,
        "GET /api/v4/orbit/schema?expand=User%2CProject HTTP/1.1"
    );
    assert_eq!(request.auth_header.as_deref(), Some("glpat-test"));

    let stdout: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json stdout");
    assert_eq!(stdout["schema_version"], "1");
}

#[test]
fn status_endpoint_is_get_orbit_status() {
    let (base_url, handle) = serve_once(r#"{"status":"healthy"}"#, "application/json");
    let output = run_orbit(&base_url, &["status"]);
    let request = handle.join().expect("join mock");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(request.request_line, "GET /api/v4/orbit/status HTTP/1.1");
}

#[test]
fn dsl_endpoint_is_get_orbit_schema_dsl() {
    let (base_url, handle) = serve_once(r#"{"$schema":"draft"}"#, "application/json");
    let output = run_orbit(&base_url, &["dsl"]);
    let request = handle.join().expect("join mock");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        request.request_line,
        "GET /api/v4/orbit/schema/dsl HTTP/1.1"
    );
}

#[test]
fn graph_status_sends_full_path_query() {
    let (base_url, handle) = serve_once(r#"{"projects":{"indexed":1}}"#, "application/json");
    let output = run_orbit(
        &base_url,
        &["graph-status", "--full-path", "gitlab-org/gitlab"],
    );
    let request = handle.join().expect("join mock");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        request.request_line,
        "GET /api/v4/orbit/graph_status?full_path=gitlab-org%2Fgitlab HTTP/1.1"
    );
}

#[test]
fn query_posts_envelope_with_resolved_response_format() {
    let (base_url, handle) = serve_once("@ok", "text/plain");
    let output = {
        use std::process::Stdio;
        let mut child = Command::new(env!("CARGO_BIN_EXE_orbit"))
            .args(["query", "--response-format", "raw", "-"])
            .env("ORBIT_API_BASE_URL", &base_url)
            .env("ORBIT_AUTH_HEADER_NAME", "Private-Token")
            .env("ORBIT_AUTH_HEADER_VALUE", "glpat-test")
            .env_remove("GITLAB_TOKEN")
            .env("ORBIT_TELEMETRY_ENABLED", "false")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn orbit query");
        child
            .stdin
            .take()
            .unwrap()
            .write_all(br#"{"query":{"query_type":"traversal"}}"#)
            .expect("write stdin");
        child.wait_with_output().expect("wait orbit query")
    };
    let request = handle.join().expect("join mock");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(request.request_line, "POST /api/v4/orbit/query HTTP/1.1");

    let sent: serde_json::Value = serde_json::from_str(&request.body).expect("json body");
    assert_eq!(sent["response_format"], "raw");
    assert_eq!(sent["query"]["query_type"], "traversal");

    assert_eq!(output.stdout, b"@ok");
}

#[test]
fn http_403_exits_with_code_four() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let handle = thread::spawn(move || {
        let mut stream = accept_within(&listener, ACCEPT_TIMEOUT);
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        while reader.read_line(&mut line).unwrap() > 0 {
            if line == "\r\n" {
                break;
            }
            line.clear();
        }
        let body = "No Orbit enabled namespaces";
        let response = format!(
            "HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).unwrap();
    });

    let output = run_orbit(&base_url, &["status"]);
    handle.join().unwrap();

    assert_eq!(output.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&output.stderr).contains("access denied"));
}

#[test]
fn context_normalized_batch_preserves_bytes_and_404_does_not_fall_back() {
    let response = "{\"entities\": [], \"future\": \"Café\"}\r\n";
    for status in ["200 OK", "404 Not Found"] {
        let (base_url, handle) = serve_response(status, response, "application/json");
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let output = orbit_command(&base_url)
            .current_dir(dir.path())
            .env("ORBIT_DATA_DIR", &data)
            .args([
                "context",
                "MergeRequest:007",
                "Issue[9]",
                "Project:42",
                "--response-format",
                "json",
            ])
            .output()
            .unwrap();
        let request = handle.join().unwrap();
        assert_eq!(
            request.request_line,
            "GET /api/v4/orbit/context?refs%5B%5D=MergeRequest%5B7%5D&refs%5B%5D=WorkItem%5B9%5D&refs%5B%5D=Project%5B42%5D&response_format=json HTTP/1.1"
        );
        assert_eq!(request.auth_header.as_deref(), Some("glpat-test"));
        assert!(request.body.is_empty());
        assert!(!data.exists());
        let stderr = String::from_utf8_lossy(&output.stderr);
        if status == "200 OK" {
            assert!(output.status.success(), "{stderr}");
            assert_eq!(output.stdout, response.as_bytes());
        } else {
            assert_eq!(output.status.code(), Some(2), "{stderr}");
            assert!(
                stderr.contains("context endpoint returned HTTP 404"),
                "{stderr}"
            );
            assert!(!stderr.contains("feature flag"), "{stderr}");
            assert!(output.stdout.is_empty());
        }
    }
}

#[cfg(unix)]
#[test]
fn context_preflight_rejects_bad_batches_before_storage_http_or_credentials() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let helper = dir.path().join("glab");
    let marker = dir.path().join("called");
    std::fs::write(&helper, "#!/bin/sh\nprintf called > \"$HELPER_MARKER\"\n").unwrap();
    std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::write(dir.path().join("Issue[9]"), "local collision").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    for (targets, message) in [
        (
            ["Definition:2", "Issue[9]", "Project[bad]"],
            "invalid context reference",
        ),
        (
            ["Definition:2", "Issue[9]", "Issue[1/2]"],
            "invalid context reference",
        ),
        (["Issue[9]", "--repo", "."], "local-only"),
    ] {
        for credential in ["glpat-test", ""] {
            let output = orbit_command(&base_url)
                .current_dir(dir.path())
                .env("ORBIT_AUTH_HEADER_VALUE", credential)
                .env("PATH", dir.path())
                .env("HELPER_MARKER", &marker)
                .env("ORBIT_DATA_DIR", dir.path().join("data"))
                .env("ORBIT_TELEMETRY_ENABLED", "true")
                .env("ORBIT_TELEMETRY_COLLECTOR_URL", &base_url)
                .arg("context")
                .args(targets)
                .output()
                .unwrap();
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(!output.status.success());
            assert!(stderr.contains(message), "{targets:?}: {stderr}");
            assert!(output.stdout.is_empty());
            assert!(!marker.exists());
            assert!(!dir.path().join("data").exists());
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
        }
    }
}
