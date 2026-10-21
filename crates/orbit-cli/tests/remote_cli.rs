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
    accept_encoding: Option<String>,
    body: String,
}

fn serve_once(
    response_body: &'static str,
    content_type: &'static str,
) -> (String, thread::JoinHandle<CapturedRequest>) {
    serve_status_once("200 OK", "", response_body, content_type)
}

fn serve_once_encoded(
    response_body: Vec<u8>,
    content_type: &'static str,
    content_encoding: Option<&'static str>,
) -> (String, thread::JoinHandle<CapturedRequest>) {
    serve_response("200 OK", "", response_body, content_type, content_encoding)
}

fn serve_status_once(
    status: &'static str,
    extra_headers: &'static str,
    response_body: &'static str,
    content_type: &'static str,
) -> (String, thread::JoinHandle<CapturedRequest>) {
    serve_response(
        status,
        extra_headers,
        response_body.as_bytes().to_vec(),
        content_type,
        None,
    )
}

fn serve_response(
    status: &'static str,
    extra_headers: &'static str,
    response_body: Vec<u8>,
    content_type: &'static str,
    content_encoding: Option<&'static str>,
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
        let mut accept_encoding = None;
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
                } else if name == "accept-encoding" {
                    accept_encoding = Some(value);
                } else if name == "content-length" {
                    content_length = value.parse().expect("parse content-length");
                }
            }
        }

        let mut body = vec![0u8; content_length];
        if content_length > 0 {
            reader.read_exact(&mut body).expect("read request body");
        }

        let encoding_header = content_encoding
            .map(|encoding| format!("Content-Encoding: {encoding}\r\n"))
            .unwrap_or_default();
        let head = format!(
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\n{extra_headers}{encoding_header}Content-Length: {}\r\nConnection: close\r\n\r\n",
            response_body.len()
        );
        stream.write_all(head.as_bytes()).expect("write head");
        stream.write_all(&response_body).expect("write response");
        stream.flush().expect("flush response");

        CapturedRequest {
            request_line: request_line.trim_end().to_string(),
            auth_header,
            accept_encoding,
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
fn graph_status_without_scope_inspects_the_origin_remote_project() {
    for remote in [
        "git@localhost:my-group/my-project.git",
        "localhost:my-group/my-project.git",
        "ssh://git@localhost:2222/my-group/my-project.git",
        "https://localhost/my-group/my-project",
    ] {
        let (base_url, handle) = serve_once(r#"{"projects":{"indexed":1}}"#, "application/json");
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
            request.request_line,
            "GET /api/v4/orbit/graph_status?full_path=my-group%2Fmy-project HTTP/1.1",
            "{remote}"
        );
    }
}

#[test]
fn graph_status_llm_format_prints_the_formatted_text() {
    let (base_url, handle) = serve_once(
        r#"{"formatted_text":"projects:\n  indexed: 1"}"#,
        "application/json",
    );
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
    let request = handle.join().expect("join mock");

    assert!(output.status.success());
    assert_eq!(
        request.request_line,
        "GET /api/v4/orbit/graph_status?namespace_id=9970&response_format=llm HTTP/1.1"
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "projects:\n  indexed: 1\n"
    );
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

#[test]
fn ontology_advertises_compression_and_decodes_gzip_response() {
    use flate2::Compression;
    use flate2::write::GzEncoder;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(br#"{"schema_version":"gzipped"}"#)
        .expect("gzip body");
    let compressed = encoder.finish().expect("finish gzip");

    let (base_url, handle) = serve_once_encoded(compressed, "application/json", Some("gzip"));
    let output = run_orbit(&base_url, &["ontology"]);
    let request = handle.join().expect("join mock");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let accept_encoding = request.accept_encoding.expect("Accept-Encoding header");
    assert!(accept_encoding.contains("gzip"), "{accept_encoding}");
    assert!(accept_encoding.contains("br"), "{accept_encoding}");
    assert!(accept_encoding.contains("zstd"), "{accept_encoding}");

    let stdout: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json stdout");
    assert_eq!(stdout["schema_version"], "gzipped");
}

fn git_clone_with_origin(origin: &str) -> tempfile::TempDir {
    let clone = tempfile::tempdir().expect("tempdir");
    for args in [
        ["init", "--quiet"].as_slice(),
        &["remote", "add", "origin", origin],
    ] {
        let status = Command::new("git")
            .args(args)
            .current_dir(clone.path())
            .status()
            .expect("run git");
        assert!(status.success(), "git {args:?} failed");
    }
    clone
}

fn run_orbit_with_data(
    dir: &Path,
    data: &Path,
    base_url: &str,
    args: &[&str],
) -> std::process::Output {
    orbit_command(dir, data, base_url, args)
        .output()
        .expect("run orbit binary")
}

fn orbit_command(dir: &Path, data: &Path, base_url: &str, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_orbit"));
    command
        .current_dir(dir)
        .args(args)
        .env("ORBIT_API_BASE_URL", base_url)
        .env("ORBIT_AUTH_HEADER_NAME", "Private-Token")
        .env("ORBIT_AUTH_HEADER_VALUE", "glpat-test")
        .env("ORBIT_DATA_DIR", data)
        .env_remove("GITLAB_TOKEN");
    command
}

const GREP_RESPONSE: &str = r#"{"version":"1.0.0","target":{"type":"project","id":1,"full_path":"my-group/my-project"},"scopes":[{"name":"merge_requests","status":"ok","backend":"basic","result_count":1}],"results":[{"scope":"merge_requests","type":"MergeRequest","id":42,"token":"MergeRequest[42]","reference":{"id":42},"text":"Add runner"}],"result_count":1,"truncated":false}"#;

#[test]
fn grep_for_a_hosted_kind_searches_the_origin_group() {
    let (base_url, handle) = serve_once(GREP_RESPONSE, "application/json");
    let base_url = base_url.replace("127.0.0.1", "localhost");
    let clone = git_clone_with_origin("https://localhost/my-group/my-project");
    let output = run_orbit_with_data(
        clone.path(),
        clone.path(),
        &base_url,
        &["grep", "mr:runner"],
    );
    let request = handle.join().expect("join mock");
    assert_eq!(
        request.request_line,
        "GET /api/v4/orbit/grep?search=runner&full_path=my-group&scopes=merge_requests HTTP/1.1"
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("MergeRequest[42]  Add runner"));
}

fn serve_routes(
    routes: Vec<(&'static str, &'static str)>,
) -> (String, thread::JoinHandle<Vec<CapturedRequest>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
    let base_url = format!("http://{}", listener.local_addr().expect("mock addr"));
    let handle = thread::spawn(move || {
        (0..routes.len())
            .map(|_| {
                let mut stream = accept_within(&listener, ACCEPT_TIMEOUT);
                let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
                let mut request_line = String::new();
                reader.read_line(&mut request_line).expect("read request line");
                let mut content_length = 0usize;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).expect("read header line");
                    let trimmed = line.trim_end();
                    if trimmed.is_empty() {
                        break;
                    }
                    if let Some((name, value)) = trimmed.split_once(':')
                        && name.trim().eq_ignore_ascii_case("content-length")
                    {
                        content_length = value.trim().parse().expect("parse content-length");
                    }
                }
                let mut body = vec![0u8; content_length];
                reader.read_exact(&mut body).expect("read request body");
                let response_body = routes
                    .iter()
                    .find(|(pattern, _)| request_line.contains(pattern))
                    .map_or("[]", |(_, body)| body);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                    response_body.len()
                );
                stream.write_all(response.as_bytes()).expect("write response");
                stream.flush().expect("flush response");
                CapturedRequest {
                    request_line: request_line.trim_end().to_string(),
                    auth_header: None,
                    accept_encoding: None,
                    body: String::from_utf8(body).expect("utf8 body"),
                }
            })
            .collect()
    });
    (base_url, handle)
}

#[test]
fn context_branch_target_reads_its_merge_request_and_last_failed_pipeline() {
    let (base_url, handle) = serve_routes(vec![
        ("merge_requests?", r#"[{"id":42,"iid":4,"state":"opened"}]"#),
        (
            "pipelines?ref=fix-ci",
            r#"[{"id":7,"status":"failed","ref":"fix-ci","sha":"0123456789abcdef","web_url":"https://localhost/p/-/pipelines/7"}]"#,
        ),
        ("pipelines/7/jobs", "[]"),
        ("pipelines?ref=refs", "[]"),
        ("orbit/context", "- found=true\n"),
    ]);
    let clone = git_clone_with_origin("https://localhost/my-group/my-project");
    let base_url = base_url.replace("127.0.0.1", "localhost");
    let output = run_orbit_with_data(
        clone.path(),
        clone.path(),
        &base_url,
        &["context", "branch:fix-ci"],
    );
    let requests = handle.join().expect("join mock");
    assert!(
        requests[4].body.contains("my-group/my-project!4"),
        "{}",
        requests[4].body
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "branch:fix-ci mr:4 opened\n\
         pipeline:7 failed ref fix-ci commit:0123456789abcdef https://localhost/p/-/pipelines/7\n\
         - found=true\n"
    );
}

#[test]
fn grep_refreshes_an_edited_file_inline_or_detached() {
    let repo = tempfile::tempdir().expect("tempdir");
    let write = |name: &str, text: &str| std::fs::write(repo.path().join(name), text).unwrap();
    write("util.py", "def helper():\n    return 1\n");
    for args in [
        &["init", "-q"][..],
        &["add", "."],
        &[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "commit",
            "-qm",
            "init",
        ],
    ] {
        assert!(
            Command::new("git")
                .args(args)
                .current_dir(repo.path())
                .status()
                .unwrap()
                .success()
        );
    }
    let (edited, fresh) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let orbit = |data: &Path, args: &[&str], wait: &str| {
        let output = orbit_command(repo.path(), data, "http://localhost:1", args)
            .env("ORBIT_REFRESH_WAIT_MS", wait)
            .output()
            .expect("run orbit binary");
        String::from_utf8_lossy(&output.stdout).into_owned()
    };
    let pause = || std::thread::sleep(Duration::from_millis(1100));
    orbit(edited.path(), &["index", "."], "2000");
    pause();
    write(
        "util.py",
        "def helper():\n    return 1\n\ndef later():\n    return 2\n",
    );
    assert!(orbit(edited.path(), &["grep", "later"], "0").contains("edited since index"));
    let landed = (0..60).any(|_| {
        std::thread::sleep(Duration::from_millis(500));
        orbit(edited.path(), &["grep", "later"], "0").contains("Function later")
    });
    assert!(landed, "a detached refresh lands the edit");
    orbit(fresh.path(), &["index", "."], "2000");
    let graph = "SELECT (SELECT list(fqn ORDER BY fqn) FROM gl_definition) AS defs, (SELECT count(*) FROM gl_edge) AS edges";
    assert_eq!(
        orbit(edited.path(), &["sql", "--format", "json", graph], "2000"),
        orbit(fresh.path(), &["sql", "--format", "json", graph], "2000")
    );
}
