use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::Path;
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
    serve_with(move |listener| answer_request(&listener, response_body, content_type))
}

fn serve_in_order(
    response_bodies: &'static [&'static str],
) -> (String, thread::JoinHandle<Vec<CapturedRequest>>) {
    serve_with(move |listener| {
        response_bodies
            .iter()
            .map(|body| answer_request(&listener, body, "application/json"))
            .collect()
    })
}

fn serve_with<T: Send + 'static>(
    answer: impl FnOnce(TcpListener) -> T + Send + 'static,
) -> (String, thread::JoinHandle<T>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
    let base_url = format!("http://{}", listener.local_addr().expect("mock addr"));
    (base_url, thread::spawn(move || answer(listener)))
}

fn answer_request(
    listener: &TcpListener,
    response_body: &str,
    content_type: &str,
) -> CapturedRequest {
    let mut stream = accept_within(listener, ACCEPT_TIMEOUT);
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

fn run_orbit(base_url: &str, args: &[&str]) -> std::process::Output {
    run_orbit_in(Path::new("."), base_url, args)
}

fn run_orbit_in(dir: &Path, base_url: &str, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_orbit"))
        .current_dir(dir)
        .args(args)
        .env("ORBIT_API_BASE_URL", base_url)
        .env("ORBIT_AUTH_HEADER_NAME", "Private-Token")
        .env("ORBIT_AUTH_HEADER_VALUE", "glpat-test")
        .env_remove("GITLAB_TOKEN")
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

const GRAPH_STATUS_RESPONSE: &str =
    r#"{"data":{"group":null,"project":{"orbit":{"indexing":{"state":"READY"}}}}}"#;

fn graphql_variables(request: &CapturedRequest) -> serde_json::Value {
    let body: serde_json::Value = serde_json::from_str(&request.body).expect("json body");
    body["variables"].clone()
}

#[test]
fn graph_status_asks_graphql_for_the_full_path() {
    let (base_url, handle) = serve_once(GRAPH_STATUS_RESPONSE, "application/json");
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
    assert_eq!(request.request_line, "POST /api/graphql HTTP/1.1");
    assert_eq!(
        graphql_variables(&request),
        serde_json::json!({"fullPath": "gitlab-org/gitlab"})
    );
    let stdout: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json stdout");
    assert_eq!(stdout, serde_json::json!({"indexing": {"state": "READY"}}));
}

#[test]
fn graph_status_without_scope_inspects_the_origin_remote_project() {
    for remote in [
        "git@localhost:my-group/my-project.git",
        "localhost:my-group/my-project.git",
        "ssh://git@localhost:2222/my-group/my-project.git",
        "https://localhost/my-group/my-project",
    ] {
        let (base_url, handle) = serve_once(GRAPH_STATUS_RESPONSE, "application/json");
        let base_url = base_url.replace("127.0.0.1", "localhost");
        let clone = tempfile::tempdir().expect("tempdir");
        for args in [
            ["init", "--quiet"].as_slice(),
            &["remote", "add", "origin", remote],
        ] {
            let status = Command::new("git")
                .args(args)
                .current_dir(clone.path())
                .status()
                .expect("run git");
            assert!(status.success(), "git {args:?} failed");
        }

        let output = run_orbit_in(clone.path(), &base_url, &["graph-status"]);
        let request = handle.join().expect("join mock");

        assert!(
            output.status.success(),
            "{remote}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            graphql_variables(&request),
            serde_json::json!({"fullPath": "my-group/my-project"}),
            "{remote}"
        );
    }
}

#[test]
fn graph_status_llm_format_prints_the_namespace_status_as_toon() {
    let (base_url, handle) =
        serve_in_order(&[r#"{"full_path":"gsd-alpha"}"#, GRAPH_STATUS_RESPONSE]);
    let output = run_orbit(
        &base_url,
        &[
            "graph-status",
            "--namespace-id",
            "9970",
            "--response-format",
            "llm",
        ],
    );
    let requests = handle.join().expect("join mock");

    assert!(output.status.success());
    assert_eq!(
        requests[0].request_line,
        "GET /api/v4/groups/9970?with_projects=false HTTP/1.1"
    );
    assert_eq!(
        graphql_variables(&requests[1]),
        serde_json::json!({"fullPath": "gsd-alpha"})
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "indexing:\n  state: READY\n"
    );
}

#[test]
fn graph_status_resolves_a_project_id_to_its_full_path() {
    let (base_url, handle) = serve_in_order(&[
        r#"{"path_with_namespace":"gsd-alpha/app"}"#,
        r#"{"data":{"group":null,"project":null}}"#,
    ]);
    let output = run_orbit(&base_url, &["graph-status", "--project-id", "2"]);
    let requests = handle.join().expect("join mock");

    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("Group or project not found"));
    assert_eq!(requests[0].request_line, "GET /api/v4/projects/2 HTTP/1.1");
    assert_eq!(
        graphql_variables(&requests[1]),
        serde_json::json!({"fullPath": "gsd-alpha/app"})
    );
}

const STATUS_WITHOUT_ORBIT: &str =
    r#"{"data":{"group":{"orbit":{"indexing":null,"itemCounts":null}},"project":null}}"#;

fn run_graph_status_without_orbit(bodies: &'static [&'static str]) -> std::process::Output {
    let (base_url, handle) = serve_in_order(bodies);
    let output = run_orbit(&base_url, &["graph-status", "--full-path", "gsd-alpha"]);
    let requests = handle.join().expect("join mock");
    assert_eq!(
        requests[1].request_line,
        "GET /api/v4/orbit/status HTTP/1.1"
    );
    output
}

#[test]
fn graph_status_without_orbit_for_the_scope_exits_forbidden() {
    let output = run_graph_status_without_orbit(&[
        STATUS_WITHOUT_ORBIT,
        r#"{"user":{"available":true},"system":{"status":"healthy"}}"#,
    ]);

    assert_eq!(output.status.code(), Some(4));
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("Orbit is not turned on for gsd-alpha, or your role is below Reporter.")
    );
    assert!(output.stdout.is_empty());
}

#[test]
fn graph_status_without_orbit_for_the_user_exits_unavailable() {
    let output = run_graph_status_without_orbit(&[
        STATUS_WITHOUT_ORBIT,
        r#"{"user":{"available":false},"system":null}"#,
    ]);

    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("Orbit is not available for your user")
    );
}

#[test]
fn graph_status_with_a_token_that_cannot_read_orbit_exits_forbidden() {
    let (base_url, handle) = serve_once(
        r#"{"data":{"group":{"orbit":null},"project":null}}"#,
        "application/json",
    );
    let output = run_orbit(&base_url, &["graph-status", "--full-path", "gsd-alpha"]);
    handle.join().expect("join mock");

    assert_eq!(output.status.code(), Some(4));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("This token cannot read Orbit status")
    );
}

#[test]
fn graph_status_prints_the_data_and_fails_on_a_field_error() {
    let (base_url, handle) = serve_once(
        r#"{"data":{"group":{"orbit":{"indexing":{"state":"READY"},"itemCounts":null}},"project":null},
            "errors":[{"message":"requested too many times","path":["group","orbit","itemCounts"]}]}"#,
        "application/json",
    );
    let output = run_orbit(&base_url, &["graph-status", "--full-path", "gsd-alpha"]);
    handle.join().expect("join mock");

    let stdout: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json stdout");
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(stdout["indexing"]["state"], "READY");
    assert!(String::from_utf8_lossy(&output.stderr).contains("requested too many times"));
}

#[test]
fn graph_status_prints_nothing_on_a_query_error() {
    let (base_url, handle) = serve_once(
        r#"{"errors":[{"message":"Field 'unknownField' doesn't exist on type 'OrbitIndexing'"}]}"#,
        "application/json",
    );
    let output = run_orbit(&base_url, &["graph-status", "--full-path", "gsd-alpha"]);
    handle.join().expect("join mock");

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unknownField"));
}

#[test]
fn query_posts_envelope_with_resolved_response_format() {
    let (base_url, handle) = serve_once("@ok", "text/plain");
    let output = {
        use std::process::Stdio;
        let mut child = Command::new(env!("CARGO_BIN_EXE_orbit"))
            .args(["query", "--response-format", "raw", "--file", "-"])
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
fn query_posts_positional_text_as_query_string() {
    let (base_url, handle) = serve_once("@ok", "text/plain");
    let output = run_orbit(&base_url, &["query", "CALL db.schema()"]);
    let request = handle.join().expect("join mock");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let sent: serde_json::Value = serde_json::from_str(&request.body).expect("json body");
    assert_eq!(sent["query"], "CALL db.schema()");
    assert_eq!(sent["response_format"], "llm");
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
fn scheme_less_gitlab_url_fails_with_a_clear_error() {
    let output = Command::new(env!("CARGO_BIN_EXE_orbit"))
        .args(["status"])
        .env_remove("ORBIT_API_BASE_URL")
        .env("GITLAB_TOKEN", "glpat-env")
        .env("GITLAB_URL", "gitlab.com")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        stderr.contains("GITLAB_URL must be an absolute https URL"),
        "{stderr}"
    );
    assert!(!stderr.contains("plaintext"), "{stderr}");
}
