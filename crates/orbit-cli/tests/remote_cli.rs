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

fn orbit_command(base_url: &str, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_orbit"));
    command
        .args(args)
        .env("ORBIT_API_BASE_URL", base_url)
        .env("ORBIT_AUTH_HEADER_NAME", "Private-Token")
        .env("ORBIT_AUTH_HEADER_VALUE", "glpat-test")
        .env_remove("GITLAB_TOKEN");
    command
}

fn run_orbit(base_url: &str, args: &[&str]) -> std::process::Output {
    orbit_command(base_url, args)
        .output()
        .expect("run orbit binary")
}

fn run_orbit_with_stdin(base_url: &str, args: &[&str], input: &[u8]) -> std::process::Output {
    use std::process::Stdio;

    let mut child = orbit_command(base_url, args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn orbit query");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input)
        .expect("write stdin");
    child.wait_with_output().expect("wait orbit query")
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
    let output = run_orbit_with_stdin(
        &base_url,
        &["query", "--response-format", "raw", "-"],
        br#"{"query":{"query_type":"traversal"}}"#,
    );
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
fn gql_file_and_stdin_preserve_query_text_and_response_bytes() {
    let text = "  MATCH (u:User {username: 'a\\\\b\\\"λ'})\r\nRETURN u LIMIT 1\n";
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("query.cypher");
    std::fs::write(&path, text).unwrap();
    let path = path.to_str().unwrap();

    for (format, response, source) in [
        ("llm", "@query\nλ \\\"quoted\\\"\n", path),
        ("raw", "{ \"result\": {\"nodes\":[]} }\n", "-"),
    ] {
        let (base_url, handle) = serve_once(response, "text/plain");
        let args = [
            "query",
            "--language",
            "gql",
            "--response-format",
            format,
            source,
        ];
        let output = if source == path {
            run_orbit(&base_url, &args)
        } else {
            run_orbit_with_stdin(&base_url, &args, text.as_bytes())
        };
        let request = handle.join().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let sent: serde_json::Value = serde_json::from_str(&request.body).unwrap();
        assert_eq!(sent["query"].as_str().unwrap().as_bytes(), text.as_bytes());
        assert_eq!(sent["language"], "gql");
        assert_eq!(sent["response_format"], format);
        assert_eq!(output.stdout, response.as_bytes());
    }
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
