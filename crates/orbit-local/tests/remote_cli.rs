use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::thread;

struct CapturedRequest {
    request_line: String,
    auth_header: Option<String>,
    body: String,
}

fn serve_once(
    response_body: &'static str,
    content_type: &'static str,
) -> (String, thread::JoinHandle<CapturedRequest>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
    let addr = listener.local_addr().expect("mock addr");
    let base_url = format!("http://{addr}");

    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept mock connection");
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
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
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

fn run_orbit(base_url: &str, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_orbit"))
        .args(args)
        .env("ORBIT_API_BASE_URL", base_url)
        .env("ORBIT_AUTH_HEADER_NAME", "Private-Token")
        .env("ORBIT_AUTH_HEADER_VALUE", "glpat-test")
        .env_remove("GITLAB_TOKEN")
        .output()
        .expect("run orbit binary")
}

#[test]
fn schema_sends_get_with_expand_and_auth_header() {
    let (base_url, handle) = serve_once(r#"{"schema_version":"1"}"#, "application/json");
    let output = run_orbit(&base_url, &["remote", "schema", "User", "Project"]);
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
    let output = run_orbit(&base_url, &["remote", "status"]);
    let request = handle.join().expect("join mock");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(request.request_line, "GET /api/v4/orbit/status HTTP/1.1");
}

#[test]
fn graph_status_sends_full_path_query() {
    let (base_url, handle) = serve_once(r#"{"projects":{"indexed":1}}"#, "application/json");
    let output = run_orbit(
        &base_url,
        &["remote", "graph-status", "--full-path", "gitlab-org/gitlab"],
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
            .args(["remote", "query", "--response-format", "raw", "-"])
            .env("ORBIT_API_BASE_URL", &base_url)
            .env("ORBIT_AUTH_HEADER_NAME", "Private-Token")
            .env("ORBIT_AUTH_HEADER_VALUE", "glpat-test")
            .env_remove("GITLAB_TOKEN")
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
        let (mut stream, _) = listener.accept().unwrap();
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

    let output = run_orbit(&base_url, &["remote", "status"]);
    handle.join().unwrap();

    assert_eq!(output.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&output.stderr).contains("access denied"));
}
