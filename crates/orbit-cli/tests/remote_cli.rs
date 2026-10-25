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
    serve_once_encoded(response_body.as_bytes().to_vec(), content_type, None)
}

fn serve_once_encoded(
    response_body: Vec<u8>,
    content_type: &'static str,
    content_encoding: Option<&'static str>,
) -> (String, thread::JoinHandle<CapturedRequest>) {
    serve_with(move |listener| {
        answer_request(&listener, &response_body, content_type, content_encoding)
    })
}

fn serve_in_order(
    response_bodies: &'static [&'static str],
) -> (String, thread::JoinHandle<Vec<CapturedRequest>>) {
    serve_with(move |listener| {
        response_bodies
            .iter()
            .map(|body| answer_request(&listener, body.as_bytes(), "application/json", None))
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
    response_body: &[u8],
    content_type: &str,
    content_encoding: Option<&str>,
) -> CapturedRequest {
    let mut stream = accept_within(listener, ACCEPT_TIMEOUT);
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
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\n{encoding_header}Content-Length: {}\r\nConnection: close\r\n\r\n",
        response_body.len()
    );
    stream.write_all(head.as_bytes()).expect("write head");
    stream.write_all(response_body).expect("write response");
    stream.flush().expect("flush response");

    CapturedRequest {
        request_line: request_line.trim_end().to_string(),
        auth_header,
        accept_encoding,
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

fn graphql_variables(request: &CapturedRequest) -> serde_json::Value {
    let body: serde_json::Value = serde_json::from_str(&request.body).expect("json body");
    body["variables"].clone()
}

const ONTOLOGY_RESPONSE: &str = r#"{"data":{"orbit":{"schema":{
    "nodes":{"pageInfo":{"hasNextPage":false,"endCursor":"Mg"},"nodes":[
        {"name":"User","description":"A user","domain":{"name":"core"},"primaryKey":"id",
         "labelProperty":"username","properties":{"nodes":[
            {"name":"id","dataType":"Int","nullable":false,"enumValues":[]}]}},
        {"name":"Project","description":null,"domain":{"name":"core"},"primaryKey":"id",
         "labelProperty":"name","properties":{"nodes":[
            {"name":"id","dataType":"Int","nullable":false,"enumValues":[]}]}}]},
    "edges":{"pageInfo":{"hasNextPage":false,"endCursor":"MQ"},"nodes":[
        {"name":"MEMBER_OF","description":"Membership","endpoints":{"nodes":[
            {"source":{"name":"User"},"target":{"name":"Project"}}]}}]}}}}}"#;

#[test]
fn schema_asks_graphql_for_the_properties_of_the_named_nodes() {
    for verb in ["schema", "ontology"] {
        let (base_url, handle) = serve_once(ONTOLOGY_RESPONSE, "application/json");
        let output = run_orbit(&base_url, &[verb, "User"]);
        let request = handle.join().expect("join mock");

        assert!(
            output.status.success(),
            "{verb} stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(request.request_line, "POST /api/graphql HTTP/1.1");
        assert_eq!(request.auth_header.as_deref(), Some("glpat-test"));
        assert_eq!(
            graphql_variables(&request),
            serde_json::json!({
                "nodesAfter": null, "edgesAfter": null,
                "readNodes": true, "readEdges": true, "withProperties": true
            })
        );
        let stdout: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("json stdout");
        assert_eq!(
            stdout,
            serde_json::json!({
                "nodes": [
                    {"name": "User", "description": "A user", "domain": {"name": "core"},
                     "primaryKey": "id", "labelProperty": "username", "properties": {"nodes": [
                        {"name": "id", "dataType": "Int", "nullable": false, "enumValues": []}]}},
                    {"name": "Project", "description": null, "domain": {"name": "core"},
                     "primaryKey": "id", "labelProperty": "name"}
                ],
                "edges": [
                    {"name": "MEMBER_OF", "description": "Membership", "endpoints": {"nodes": [
                        {"source": {"name": "User"}, "target": {"name": "Project"}}]}}
                ]
            })
        );
    }
}

#[test]
fn ontology_star_keeps_the_properties_of_every_node() {
    let (base_url, handle) = serve_once(ONTOLOGY_RESPONSE, "application/json");
    let output = run_orbit(&base_url, &["ontology", "*"]);
    let request = handle.join().expect("join mock");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(graphql_variables(&request)["withProperties"], true);
    let stdout: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json stdout");
    let nodes = stdout["nodes"].as_array().expect("nodes");
    assert_eq!(nodes.len(), 2);
    assert!(
        nodes
            .iter()
            .all(|node| node["properties"]["nodes"].is_array())
    );
}

#[test]
fn ontology_reads_each_connection_until_its_last_page() {
    let (base_url, handle) = serve_in_order(&[
        r#"{"data":{"orbit":{"schema":{
            "nodes":{"pageInfo":{"hasNextPage":true,"endCursor":"MQ"},"nodes":[
                {"name":"User","description":null,"domain":{"name":"core"},
                 "primaryKey":"id","labelProperty":"username"}]},
            "edges":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]}}}}}"#,
        r#"{"data":{"orbit":{"schema":{
            "nodes":{"pageInfo":{"hasNextPage":false,"endCursor":"Mg"},"nodes":[
                {"name":"Project","description":null,"domain":{"name":"core"},
                 "primaryKey":"id","labelProperty":"name"}]}}}}}"#,
    ]);
    let output = run_orbit(&base_url, &["schema"]);
    let requests = handle.join().expect("join mock");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        graphql_variables(&requests[1]),
        serde_json::json!({
            "nodesAfter": "MQ", "edgesAfter": null,
            "readNodes": true, "readEdges": false, "withProperties": false
        })
    );
    let stdout: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json stdout");
    let names: Vec<&str> = stdout["nodes"]
        .as_array()
        .expect("nodes")
        .iter()
        .map(|node| node["name"].as_str().expect("name"))
        .collect();
    assert_eq!(names, ["User", "Project"]);
    assert_eq!(stdout["edges"], serde_json::json!([]));
}

#[test]
fn ontology_without_the_orbit_graphql_api_exits_unavailable() {
    let (base_url, handle) = serve_once(r#"{"data":{"orbit":null}}"#, "application/json");
    let output = run_orbit(&base_url, &["schema"]);
    handle.join().expect("join mock");

    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("Orbit schema not available"));
}

#[test]
fn ontology_fails_on_a_graphql_error() {
    let (base_url, handle) = serve_once(
        r#"{"errors":[{"message":"Internal server error"}]}"#,
        "application/json",
    );
    let output = run_orbit(&base_url, &["schema"]);
    handle.join().expect("join mock");

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("Orbit schema query failed: Internal server error")
    );
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

const GRAPH_STATUS_RESPONSE: &str = r#"{"data":{"group":null,"project":{"orbit":{
    "indexing":{"state":"READY","areas":null,"projects":null},"itemCounts":null}}}}"#;

#[test]
fn graph_status_asks_graphql_for_the_full_path() {
    for verb in ["status", "graph-status"] {
        let (base_url, handle) = serve_once(GRAPH_STATUS_RESPONSE, "application/json");
        let output = run_orbit(&base_url, &[verb, "--full-path", "gitlab-org/gitlab"]);
        let request = handle.join().expect("join mock");

        assert!(
            output.status.success(),
            "{verb} stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(request.request_line, "POST /api/graphql HTTP/1.1");
        assert_eq!(
            graphql_variables(&request),
            serde_json::json!({"fullPath": "gitlab-org/gitlab"})
        );
        let stdout: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("json stdout");
        assert_eq!(
            stdout,
            serde_json::json!({
                "indexing": {"state": "READY", "areas": null, "projects": null},
                "itemCounts": null
            })
        );
    }
}

#[test]
fn status_rejects_a_response_format_without_a_scope() {
    let output = run_orbit(
        "http://127.0.0.1:9",
        &["status", "--response-format", "raw"],
    );
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--response-format needs --full-path"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
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
            "status",
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
        "indexing:\n  state: READY\n  areas: null\n  projects: null\nitemCounts: null\n"
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
        r#"{"data":{"group":{"orbit":{"indexing":{"state":"READY","areas":null,"projects":null},"itemCounts":null}},"project":null},
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
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("Orbit isn't enabled for any of your groups")
    );
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
        .write_all(ONTOLOGY_RESPONSE.as_bytes())
        .expect("gzip body");
    let compressed = encoder.finish().expect("finish gzip");

    let (base_url, handle) = serve_once_encoded(compressed, "application/json", Some("gzip"));
    let output = run_orbit(&base_url, &["schema"]);
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
    assert_eq!(stdout["edges"][0]["name"], "MEMBER_OF");
}

fn committed_repo(origin: &str, files: &[(&str, &str)]) -> tempfile::TempDir {
    let repo = tempfile::tempdir().expect("tempdir");
    for (path, text) in files {
        let path = repo.path().join(path);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(path, text).expect("write file");
    }
    let commit = [
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@t",
        "commit",
        "-qm",
        "init",
    ];
    let remote = ["remote", "add", "origin", origin];
    let mut steps = vec![&["init", "-q"][..], &remote];
    if !files.is_empty() {
        steps.extend([&["add", "-A"][..], &commit]);
    }
    for args in steps {
        let status = Command::new("git")
            .args(args)
            .current_dir(repo.path())
            .status();
        assert!(status.expect("run git").success(), "git {args:?} failed");
    }
    repo
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
    let clone = committed_repo("https://localhost/my-group/my-project", &[]);
    let output = orbit_command(
        clone.path(),
        clone.path(),
        &base_url,
        &["grep", "mr:runner"],
    )
    .output()
    .expect("run orbit binary");
    let request = handle.join().expect("join mock");
    assert_eq!(
        request.request_line,
        "GET /api/v4/orbit/grep?search=runner&full_path=my-group&scopes=MergeRequest HTTP/1.1"
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("MergeRequest[42]  Add runner"));
}

fn serve_routes(
    routes: Vec<(&'static str, &'static str)>,
) -> (String, thread::JoinHandle<Vec<CapturedRequest>>) {
    serve_routes_with_status(
        routes
            .into_iter()
            .map(|(pattern, body)| (pattern, 200, body))
            .collect(),
    )
}

fn serve_routes_with_status(
    routes: Vec<(&'static str, u16, &'static str)>,
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
                let text = String::from_utf8_lossy(&body);
                let (status, response_body) = routes
                    .iter()
                    .find(|(pattern, _, _)| {
                        request_line.contains(pattern) || text.contains(pattern)
                    })
                    .map_or((200, "[]"), |(_, status, body)| (*status, *body));
                let response = format!(
                    "HTTP/1.1 {status} Mock\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
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
fn a_query_with_an_unknown_relationship_prints_the_schema() {
    let (base_url, handle) = serve_routes_with_status(vec![
        (
            "WROTE",
            400,
            r#"{"code":"compile_error","message":"allowlist rejected: unknown relationship type \"WROTE\""}"#,
        ),
        (
            "CALL db.schema()",
            200,
            "domains[1]:\n  - name: core\nedges[1]: (User)-[:AUTHORED]->(MergeRequest)\n",
        ),
    ]);
    let output = run_orbit(
        &base_url,
        &[
            "query",
            "MATCH (u:User)-[:WROTE]->(m:MergeRequest) RETURN m",
        ],
    );
    let requests = handle.join().expect("join mock");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success());
    assert!(stderr.contains("unknown relationship type"), "{stderr}");
    assert!(
        stderr.contains("(User)-[:AUTHORED]->(MergeRequest)"),
        "{stderr}"
    );
    let sent: serde_json::Value = serde_json::from_str(&requests[1].body).expect("json body");
    assert_eq!(sent["query"], "CALL db.schema()");
    assert_eq!(sent["response_format"], "llm");
}

#[test]
fn a_query_with_an_unknown_field_prints_only_the_error() {
    let (base_url, handle) = serve_routes_with_status(vec![(
        "body",
        400,
        r#"{"code":"compile_error","message":"allowlist rejected: field \"body\" does not exist on node type \"Note\". Valid fields: id, note"}"#,
    )]);
    let output = run_orbit(&base_url, &["query", "MATCH (n:Note) RETURN n.body"]);
    let requests = handle.join().expect("join mock");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success());
    assert!(stderr.contains("Valid fields: id, note"), "{stderr}");
    assert_eq!(requests.len(), 1);
}

#[test]
fn context_branch_target_reads_its_merge_request() {
    let (base_url, handle) = serve_routes(vec![
        (
            r#""response_format":"raw""#,
            r#"{"version":"1.3.0","entities":[{"token":"my-group/my-project@fix-ci","type":"Branch","id":"fix-ci","found":true,"summary":{"merge_request":{"type":"MergeRequest","id":"42","token":"MergeRequest[42]","iid":4,"project_path":"my-group/my-project","title":"Fix CI"}}}]}"#,
        ),
        (r#""response_format":"llm""#, "- found=true\n"),
    ]);
    let clone = committed_repo("https://localhost/my-group/my-project", &[]);
    let base_url = base_url.replace("127.0.0.1", "localhost");
    let output = orbit_command(
        clone.path(),
        clone.path(),
        &base_url,
        &["context", "branch:fix-ci"],
    )
    .output()
    .expect("run orbit binary");
    let requests = handle.join().expect("join mock");
    assert!(
        requests[0].body.contains("my-group/my-project@fix-ci"),
        "{}",
        requests[0].body
    );
    assert!(
        requests[1].body.contains("my-group/my-project!4"),
        "{}",
        requests[1].body
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "branch:fix-ci mr:4 Fix CI\n- found=true\n"
    );
}

#[test]
fn context_appends_the_local_code_a_merge_request_changes() {
    let clone = committed_repo(
        "https://localhost/my-group/my-project",
        &[(
            "src/app.py",
            "def total(items):\n    count = len(items)\n    return count\n",
        )],
    );
    let data = tempfile::tempdir().expect("tempdir");
    let indexed = orbit_command(
        clone.path(),
        data.path(),
        "http://localhost:1",
        &["workspace", "index", "."],
    )
    .output()
    .expect("run orbit binary");
    assert!(
        indexed.status.success(),
        "{}",
        String::from_utf8_lossy(&indexed.stderr)
    );
    let (base_url, handle) = serve_routes(vec![(
        "orbit/context",
        "- mr:my-group/my-project!4 found=true\n  changed_files[1]:\n    M src/app.py @ 2-3\n",
    )]);
    let base_url = base_url.replace("127.0.0.1", "localhost");
    let output = orbit_command(
        clone.path(),
        data.path(),
        &base_url,
        &["context", "my-group/my-project!4"],
    )
    .output()
    .expect("run orbit binary");
    handle.join().expect("join mock");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("src/app.py:1-3  Function  src.app.total  (at 2-3)\n1|def total(items):\n2|    count = len(items)\n3|    return count\n"),
        "{stdout}"
    );
}

#[test]
fn grep_without_a_line_holding_every_word_shows_the_best_few_files() {
    let files: Vec<(String, String)> = (1..=7)
        .map(|n| {
            (
                format!("src/f{n}.py"),
                format!("alpha = {n}\n\nbeta = {n}\n"),
            )
        })
        .collect();
    let files: Vec<(&str, &str)> = files
        .iter()
        .map(|(path, body)| (path.as_str(), body.as_str()))
        .collect();
    let repo = committed_repo("https://localhost/my-group/my-project", &files);
    let data = tempfile::tempdir().expect("tempdir");
    let orbit = |args: &[&str]| {
        orbit_command(repo.path(), data.path(), "http://localhost:1", args)
            .output()
            .expect("run orbit binary")
    };
    let indexed = orbit(&["workspace", "index", "."]);
    assert!(
        indexed.status.success(),
        "{}",
        String::from_utf8_lossy(&indexed.stderr)
    );
    let stdout = String::from_utf8_lossy(&orbit(&["grep", "alpha beta"]).stdout).into_owned();
    assert!(stdout.contains("in 5 files"), "{stdout}");
    assert!(
        stdout.contains("0 lines contain \"alpha beta\"; showing the files with 2 of its 2 words"),
        "{stdout}"
    );
    assert!(stdout.contains("; 2 more files have them too"), "{stdout}");
}

#[test]
fn context_prints_each_anchored_line_once() {
    let clone = committed_repo(
        "https://localhost/my-group/my-project",
        &[(
            "src/app.py",
            "import os\n\n\ndef total(items):\n    count = len(items)\n    return count\n",
        )],
    );
    let data = tempfile::tempdir().expect("tempdir");
    let indexed = orbit_command(
        clone.path(),
        data.path(),
        "http://localhost:1",
        &["workspace", "index", "."],
    )
    .output()
    .expect("run orbit binary");
    assert!(
        indexed.status.success(),
        "{}",
        String::from_utf8_lossy(&indexed.stderr)
    );
    let (base_url, handle) = serve_routes(vec![(
        "orbit/context",
        "- mr:my-group/my-project!4 found=true\n  changed_files[1]:\n    M src/app.py @ 2-6\n  discussions[1]:\n    @a [unresolved] src/app.py:5:\n      why count?\n",
    )]);
    let base_url = base_url.replace("127.0.0.1", "localhost");
    let output = orbit_command(
        clone.path(),
        data.path(),
        &base_url,
        &["context", "my-group/my-project!4"],
    )
    .output()
    .expect("run orbit binary");
    handle.join().expect("join mock");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        stdout.matches("5|    count = len(items)").count(),
        1,
        "{stdout}"
    );
    assert_eq!(stdout.matches("6|    return count").count(), 1, "{stdout}");
}

#[test]
fn context_reads_a_definition_named_after_its_path() {
    let clone = committed_repo(
        "https://localhost/my-group/my-project",
        &[
            (
                "src/app.py",
                "def total(items):\n    return len(items)\n\n\nclass Cart:\n    def total(self):\n        return 0\n",
            ),
            ("src/other.py", "def total():\n    return 1\n"),
        ],
    );
    let data = tempfile::tempdir().expect("tempdir");
    let orbit = |args: &[&str]| {
        orbit_command(clone.path(), data.path(), "http://localhost:1", args)
            .output()
            .expect("run orbit binary")
    };
    let indexed = orbit(&["workspace", "index", "."]);
    assert!(
        indexed.status.success(),
        "{}",
        String::from_utf8_lossy(&indexed.stderr)
    );
    let output = orbit(&["context", "src/app.py:Cart.total"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("6|    def total(self):"), "{stdout}");
    assert!(!stdout.contains("src/other.py"), "{stdout}");
    assert!(!stdout.contains("return len(items)"), "{stdout}");
}

#[test]
fn context_stops_a_whole_file_and_says_how_to_read_the_rest() {
    let body: String = (1..=250).map(|n| format!("X_{n} = {n}\n")).collect();
    let clone = committed_repo(
        "https://localhost/my-group/my-project",
        &[("src/big.py", body.as_str())],
    );
    let data = tempfile::tempdir().expect("tempdir");
    let orbit = |args: &[&str]| {
        orbit_command(clone.path(), data.path(), "http://localhost:1", args)
            .output()
            .expect("run orbit binary")
    };
    let indexed = orbit(&["workspace", "index", "."]);
    assert!(
        indexed.status.success(),
        "{}",
        String::from_utf8_lossy(&indexed.stderr)
    );
    let stdout = String::from_utf8_lossy(&orbit(&["context", "src/big.py"]).stdout).into_owned();
    assert!(stdout.contains("200|X_200 = 200\n"), "{stdout}");
    assert!(!stdout.contains("201|"), "{stdout}");
    assert!(
        stdout.contains(
            "» src/big.py has 250 lines; `orbit context src/big.py:201-250` reads the rest"
        ),
        "{stdout}"
    );
    let rest =
        String::from_utf8_lossy(&orbit(&["context", "src/big.py:201-250"]).stdout).into_owned();
    assert!(rest.contains("250|X_250 = 250"), "{rest}");
}

#[test]
fn grep_refreshes_an_edited_file_inline_or_detached() {
    let repo = committed_repo(
        "https://localhost/my-group/my-project",
        &[("util.py", "def helper():\n    return 1\n")],
    );
    let write = |name: &str, text: &str| std::fs::write(repo.path().join(name), text).unwrap();
    let (edited, fresh) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let orbit = |data: &Path, args: &[&str], wait: &str| {
        let output = orbit_command(repo.path(), data, "http://localhost:1", args)
            .env("ORBIT_REFRESH_WAIT_MS", wait)
            .output()
            .expect("run orbit binary");
        String::from_utf8_lossy(&output.stdout).into_owned()
    };
    let pause = || std::thread::sleep(Duration::from_millis(1100));
    orbit(edited.path(), &["workspace", "index", "."], "2000");
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
    orbit(fresh.path(), &["workspace", "index", "."], "2000");
    let graph = "SELECT (SELECT list(fqn ORDER BY fqn) FROM gl_definition) AS defs, (SELECT count(*) FROM gl_edge) AS edges";
    assert_eq!(
        orbit(edited.path(), &["sql", "--format", "json", graph], "2000"),
        orbit(fresh.path(), &["sql", "--format", "json", graph], "2000")
    );
}

fn duckdb_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(duckdb_files(&path));
        } else if path.extension().is_some_and(|ext| ext == "duckdb") {
            found.push(path);
        }
    }
    found
}

#[test]
fn a_refresh_that_cannot_write_leaves_the_file_marked_as_edited() {
    let repo = committed_repo(
        "https://localhost/my-group/my-project",
        &[("util.py", "def helper():\n    return 1\n")],
    );
    let data = tempfile::tempdir().unwrap();
    let orbit = |args: &[&str]| {
        orbit_command(repo.path(), data.path(), "http://localhost:1", args)
            .env("ORBIT_REFRESH_WAIT_MS", "0")
            .output()
            .expect("run orbit binary")
    };
    orbit(&["workspace", "index", "."]);
    std::thread::sleep(Duration::from_millis(1100));
    std::fs::write(
        repo.path().join("util.py"),
        "def renamed():\n    return 1\n",
    )
    .unwrap();
    let db = duckdb_files(data.path()).pop().expect("an indexed graph");
    let mut lock_path = db.as_os_str().to_owned();
    lock_path.push(".lock");
    let reader = std::fs::File::open(&lock_path).expect("open the graph lock");
    reader.lock_shared().expect("hold a reader lock");
    let repo_path = repo.path().to_string_lossy().into_owned();
    let refresh = orbit(&["refresh", &repo_path, "--", "util.py"]);
    drop(reader);

    assert!(
        !refresh.status.success(),
        "the writer could not take the lock"
    );
    let sql = orbit(&[
        "sql",
        "--format",
        "json",
        "SELECT list(name) AS names FROM gl_definition",
    ]);
    assert!(String::from_utf8_lossy(&sql.stdout).contains("helper"));
    let grep = String::from_utf8_lossy(&orbit(&["grep", "renamed"]).stdout).into_owned();
    assert!(grep.contains("edited since index"), "{grep}");
}

#[test]
fn a_slow_gitlab_does_not_hold_back_local_grep_results() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
    let base_url = format!("http://localhost:{}", listener.local_addr().unwrap().port());
    thread::spawn(move || {
        let held: Vec<_> = listener.incoming().take(4).filter_map(Result::ok).collect();
        thread::sleep(Duration::from_secs(20));
        drop(held);
    });
    let repo = committed_repo(
        "https://localhost/my-group/my-project",
        &[("util.py", "def alpha_marker():\n    return 1\n")],
    );
    let data = tempfile::tempdir().unwrap();
    let started = std::time::Instant::now();
    let output = orbit_command(
        repo.path(),
        data.path(),
        &base_url,
        &["grep", "alpha_marker"],
    )
    .output()
    .expect("run orbit binary");
    let elapsed = started.elapsed();

    assert!(String::from_utf8_lossy(&output.stdout).contains("alpha_marker"));
    assert!(elapsed < Duration::from_secs(8), "grep took {elapsed:?}");
}
