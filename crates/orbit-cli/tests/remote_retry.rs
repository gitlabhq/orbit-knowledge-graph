use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Command, Output};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

struct MockServer {
    base_url: String,
    requests: Arc<AtomicUsize>,
}

fn unreachable_url() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    format!("http://{}", listener.local_addr().expect("addr"))
}

fn serve(replies: &'static [(u16, Option<&'static str>)]) -> MockServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
    let base_url = format!("http://{}", listener.local_addr().expect("mock addr"));
    let requests = Arc::new(AtomicUsize::new(0));
    let served = Arc::clone(&requests);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.expect("accept");
            let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
            let mut content_length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).expect("read header");
                let line = line.trim_end().to_ascii_lowercase();
                if line.is_empty() {
                    break;
                }
                if let Some(value) = line.strip_prefix("content-length:") {
                    content_length = value.trim().parse().expect("content-length");
                }
            }
            reader
                .read_exact(&mut vec![0; content_length])
                .expect("read body");
            let index = served.fetch_add(1, Ordering::SeqCst).min(replies.len() - 1);
            let (status, retry_after) = replies[index];
            let retry_after = retry_after
                .map(|value| format!("Retry-After: {value}\r\n"))
                .unwrap_or_default();
            let body = if status == 200 { "@ok" } else { "no" };
            let _ = write!(
                stream,
                "HTTP/1.1 {status} X\r\nContent-Type: text/plain\r\n{retry_after}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    MockServer { base_url, requests }
}

fn run_query(server: &MockServer, extra_args: &[&str]) -> Output {
    run_query_at(&server.base_url, extra_args)
}

fn run_query_at(base_url: &str, extra_args: &[&str]) -> Output {
    run_orbit(
        base_url,
        &[&["query", "--response-format", "raw"], extra_args].concat(),
        &[r#"{"query":{"query_type":"traversal"}}"#],
    )
}

fn run_orbit(base_url: &str, args: &[&str], trailing: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_orbit"))
        .args(args)
        .args(trailing)
        .env("ORBIT_API_BASE_URL", base_url)
        .env("ORBIT_AUTH_HEADER_NAME", "Private-Token")
        .env("ORBIT_AUTH_HEADER_VALUE", "glpat-test")
        .env_remove("GITLAB_TOKEN")
        .env_remove("ORBIT_RETRIES")
        .output()
        .expect("run orbit binary")
}

#[test]
fn a_rate_limited_query_is_retried_until_it_succeeds() {
    let server = serve(&[(429, Some("0")), (429, Some("0")), (200, None)]);
    let output = run_query(&server, &[]);

    assert!(output.status.success());
    assert_eq!(server.requests.load(Ordering::SeqCst), 3);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("retry 1/3"), "stderr: {stderr}");
    assert!(stderr.contains("retry 2/3"), "stderr: {stderr}");
}

#[test]
fn retry_after_is_a_lower_bound_on_the_wait() {
    let server = serve(&[(429, Some("2")), (200, None)]);
    let started = Instant::now();
    let output = run_query(&server, &[]);

    assert!(output.status.success());
    assert!(started.elapsed() >= Duration::from_secs(2));
}

#[test]
fn retries_stop_after_the_configured_count() {
    let server = serve(&[(429, Some("0"))]);
    let output = run_query(&server, &["--retries", "1"]);

    assert_eq!(output.status.code(), Some(5));
    assert_eq!(server.requests.load(Ordering::SeqCst), 2);
}

#[test]
fn zero_retries_disables_retrying() {
    let server = serve(&[(429, Some("0")), (200, None)]);
    let output = run_query(&server, &["--retries", "0"]);

    assert_eq!(output.status.code(), Some(5));
    assert_eq!(server.requests.load(Ordering::SeqCst), 1);
}

#[test]
fn a_retry_after_beyond_the_budget_fails_fast() {
    let server = serve(&[(429, Some("3600")), (200, None)]);
    let output = run_query(&server, &[]);

    assert_eq!(output.status.code(), Some(5));
    assert_eq!(server.requests.load(Ordering::SeqCst), 1);
}

#[test]
fn server_errors_on_a_query_are_not_retried() {
    for status in [500, 502, 503, 504] {
        let replies: &'static [(u16, Option<&'static str>)] =
            Box::leak(Box::new([(status, None), (200, None)]));
        let server = serve(replies);
        let output = run_query(&server, &[]);

        assert_eq!(output.status.code(), Some(1), "status {status}");
        assert_eq!(server.requests.load(Ordering::SeqCst), 1, "status {status}");
    }
}

#[test]
fn a_failed_connection_is_retried_with_backoff() {
    let output = run_query_at(&unreachable_url(), &["--retries", "1"]);

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("connection failed; retrying"),
        "stderr: {stderr}"
    );
    assert!(stderr.contains("retry 1/1"), "stderr: {stderr}");
}

#[test]
fn zero_retries_also_disables_connect_retries() {
    let output = run_query_at(&unreachable_url(), &["--retries", "0"]);

    assert_eq!(output.status.code(), Some(1));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("retrying"));
}

#[test]
fn retry_after_may_be_an_http_date() {
    let reply_at = httpdate::fmt_http_date(std::time::SystemTime::now() + Duration::from_secs(3));
    let replies: &'static [(u16, Option<&'static str>)] =
        Box::leak(Box::new([(429, Some(&*reply_at.leak())), (200, None)]));
    let server = serve(replies);
    let started = Instant::now();
    let output = run_query(&server, &[]);

    assert!(output.status.success());
    assert!(started.elapsed() >= Duration::from_secs(2));
}

#[test]
fn the_rate_limit_error_reports_the_wait_the_server_asked_for() {
    let server = serve(&[(429, Some("3600"))]);
    let output = run_query(&server, &[]);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(5));
    assert!(
        stderr.contains("asked to wait 3600 seconds"),
        "stderr: {stderr}"
    );
}

#[test]
fn schema_retries_a_503() {
    let server = serve(&[(503, None), (200, None)]);
    let output = run_orbit(&server.base_url, &["schema"], &[]);

    assert_eq!(server.requests.load(Ordering::SeqCst), 2);
    assert!(String::from_utf8_lossy(&output.stderr).contains("HTTP 503; retrying"));
}

fn run_remote_grep(server: &MockServer) -> Output {
    run_orbit(
        &server.base_url,
        &["grep", "mr:runner", "my-group/my-project"],
        &[],
    )
}

#[test]
fn remote_grep_retries_a_503() {
    let server = serve(&[(503, None), (200, None)]);
    let output = run_remote_grep(&server);

    assert_eq!(server.requests.load(Ordering::SeqCst), 2);
    assert!(String::from_utf8_lossy(&output.stderr).contains("HTTP 503; retrying"));
}

#[test]
fn remote_grep_gives_up_instead_of_waiting_out_a_long_retry_after() {
    let server = serve(&[(429, Some("60")), (200, None)]);
    let started = Instant::now();
    let output = run_remote_grep(&server);

    assert_eq!(server.requests.load(Ordering::SeqCst), 1);
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(output.status.code(), Some(1));
}
