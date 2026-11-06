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

/// Answers each connection with the next `(status, retry_after)` reply, then keeps answering with
/// the last one. Runs on a detached thread that ends with the test process.
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
    Command::new(env!("CARGO_BIN_EXE_orbit"))
        .args(["query", "--response-format", "raw"])
        .args(extra_args)
        .arg(r#"{"query":{"query_type":"traversal"}}"#)
        .env("ORBIT_API_BASE_URL", &server.base_url)
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
