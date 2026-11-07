use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const STATUS: &str = r#"{"data":{"orbit":{"serviceStatus":{
    "state":"HEALTHY","version":"1.1.0","components":[]}}}}"#;
const DOMAINS: &str = r#"{"result":{"domains":[
    {"name":"core","nodes":["Project",{"name":"User","props":"id, username"}]},
    {"name":"source_code","nodes":["File","Definition"]},
    {"name":"future","nodes":["Widget"]}],"edges":[]},"query_type":"","row_count":0}"#;
const NEWER: &str = r#"{"result":{"domains":[
    {"name":"core","nodes":["Project"]},{"name":"plan","nodes":["Gadget"]}],"edges":[]}}"#;
const NOT_FOUND: &str = r#"{"error":"404 Not Found"}"#;

#[derive(Clone, Copy)]
struct Answers {
    schema: Option<&'static str>,
    delay: Duration,
}

const FULL: Answers = Answers {
    schema: Some(DOMAINS),
    delay: Duration::ZERO,
};

const NOTHING: Answers = Answers {
    schema: None,
    delay: Duration::ZERO,
};

struct Server {
    base_url: String,
    paths: Arc<Mutex<Vec<String>>>,
    answers: Arc<Mutex<Answers>>,
}

impl Server {
    fn start(answers: Answers) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let paths = Arc::new(Mutex::new(Vec::new()));
        let answers = Arc::new(Mutex::new(answers));
        let (seen, current) = (Arc::clone(&paths), Arc::clone(&answers));
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let seen = Arc::clone(&seen);
                let answers = *current.lock().unwrap();
                thread::spawn(move || answer(stream, answers, &seen));
            }
        });
        Self {
            base_url,
            paths,
            answers,
        }
    }

    fn answer_with(&self, answers: Answers) {
        *self.answers.lock().unwrap() = answers;
    }

    fn schema_reads(&self) -> usize {
        self.paths
            .lock()
            .unwrap()
            .iter()
            .filter(|path| *path == "/api/v4/orbit/query")
            .count()
    }

    fn paths(&self) -> Vec<String> {
        self.paths.lock().unwrap().clone()
    }
}

fn cache_file(data: &Path) -> std::path::PathBuf {
    std::fs::read_dir(data.join("var"))
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("server-schema-"))
        })
        .expect("a schema cache")
}

fn expire(cache: &Path) {
    let mut cached: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(cache).unwrap()).unwrap();
    cached["fetched_at"] = 0.into();
    std::fs::write(cache, cached.to_string()).unwrap();
}

fn answer(mut stream: TcpStream, answers: Answers, seen: &Mutex<Vec<String>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() {
        return;
    }
    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .split('?')
        .next()
        .unwrap_or_default()
        .to_string();
    let mut length = 0;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() || line.trim_end().is_empty() {
            break;
        }
        if let Some((name, value)) = line.trim_end().split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            length = value.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0; length];
    let _ = reader.read_exact(&mut body);
    seen.lock().unwrap().push(path.clone());
    let (status, body) = match path.as_str() {
        "/api/graphql" => (200, STATUS.to_string()),
        "/api/v4/orbit/query" => {
            thread::sleep(answers.delay);
            match answers.schema {
                Some(schema) => (200, schema.to_string()),
                None => (404, NOT_FOUND.to_string()),
            }
        }
        _ => (404, NOT_FOUND.to_string()),
    };
    let head = format!(
        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body.as_bytes());
}

fn instructions(base_url: &str, data: &Path) -> (String, Duration) {
    let work = tempfile::tempdir().unwrap();
    let started = Instant::now();
    let output: Output = Command::new(env!("CARGO_BIN_EXE_orbit"))
        .current_dir(work.path())
        .args(["hook-guard", "instructions"])
        .env("ORBIT_API_BASE_URL", base_url)
        .env("ORBIT_AUTH_HEADER_NAME", "Private-Token")
        .env("ORBIT_AUTH_HEADER_VALUE", "glpat-test")
        .env("ORBIT_DATA_DIR", data)
        .env("ORBIT_TELEMETRY", "0")
        .env_remove("GITLAB_TOKEN")
        .env_remove("GITLAB_URL")
        .env_remove("GLAB_ORBIT_VERSION")
        .output()
        .unwrap();
    let elapsed = started.elapsed();
    assert!(output.status.success(), "{output:?}");
    (String::from_utf8(output.stdout).unwrap(), elapsed)
}

fn query_line(text: &str) -> &str {
    text.lines()
        .find(|line| line.contains("`orbit query` for counts"))
        .unwrap_or_else(|| panic!("no query line in {text}"))
}

fn grep_line(text: &str) -> &str {
    text.lines()
        .find(|line| line.contains("Prefix a GitLab kind"))
        .unwrap_or_else(|| panic!("no grep line in {text}"))
}

const PROVIDERS: &str = "Connected providers and their entities, for `orbit query`:";

fn providers(text: &str) -> Option<&str> {
    text.split_once(PROVIDERS).map(|(_, list)| list.trim())
}

const BUNDLED: &str = "across GitLab. Read-only openCypher";

#[test]
fn the_servers_entities_are_listed_under_gitlab() {
    let server = Server::start(FULL);
    let data = tempfile::tempdir().unwrap();
    let (text, _) = instructions(&server.base_url, data.path());

    assert_eq!(text.matches("MANDATORY").count(), 1, "{text}");
    assert_eq!(
        providers(&text),
        Some("- gitlab: Project, User, File, Definition, Widget"),
        "{text}"
    );
    assert!(query_line(&text).contains(BUNDLED), "{text}");
    assert!(!grep_line(&text).contains("Widget"), "{text}");
    assert!(grep_line(&text).contains("`issue:`"), "{text}");
    assert_eq!(server.schema_reads(), 1);
    assert!(
        !server.paths().iter().any(|path| path.contains("/mcp")),
        "{:?}",
        server.paths()
    );
}

#[test]
fn a_warm_cache_answers_without_asking_the_server_again() {
    let server = Server::start(FULL);
    let data = tempfile::tempdir().unwrap();
    let (cold, _) = instructions(&server.base_url, data.path());

    server.answer_with(NOTHING);
    let (warm, _) = instructions(&server.base_url, data.path());
    assert_eq!(warm, cold);
    assert_eq!(server.schema_reads(), 1, "the warm start read the schema");

    let other = Server::start(NOTHING);
    let (elsewhere, _) = instructions(&other.base_url, data.path());
    assert_eq!(other.schema_reads(), 1, "each GitLab has its own cache");
    assert!(!elsewhere.contains("Widget"), "{elsewhere}");
}

fn fetched_at(cache: &Path) -> u64 {
    std::fs::read_to_string(cache)
        .ok()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
        .and_then(|cached| cached["fetched_at"].as_u64())
        .unwrap_or_default()
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn an_expired_cache_answers_at_once_and_refreshes_in_the_background() {
    let server = Server::start(FULL);
    let data = tempfile::tempdir().unwrap();
    let (cold, _) = instructions(&server.base_url, data.path());
    server.answer_with(Answers {
        schema: Some(NEWER),
        delay: Duration::from_secs(2),
    });
    let cache = cache_file(data.path());
    expire(&cache);

    let (stale, elapsed) = instructions(&server.base_url, data.path());
    assert_eq!(stale, cold, "the expired list is served while it refreshes");
    assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");

    instructions(&server.base_url, data.path());
    wait_for("the background refresh", || {
        std::fs::read_to_string(&cache).is_ok_and(|text| text.contains("Gadget"))
    });
    assert_eq!(
        server.schema_reads(),
        2,
        "a second start did not refresh again"
    );

    let (refreshed, _) = instructions(&server.base_url, data.path());
    assert_eq!(
        providers(&refreshed),
        Some("- gitlab: Project, Gadget"),
        "{refreshed}"
    );
}

#[test]
fn a_failed_schema_read_gets_the_bundled_query_line() {
    let server = Server::start(NOTHING);
    let data = tempfile::tempdir().unwrap();
    let (text, _) = instructions(&server.base_url, data.path());

    assert_eq!(providers(&text), None, "{text}");
    assert!(query_line(&text).contains(BUNDLED), "{text}");
    assert_eq!(server.schema_reads(), 1);
}

#[test]
fn a_failed_refresh_keeps_the_last_answer_and_waits_before_retrying() {
    let server = Server::start(FULL);
    let data = tempfile::tempdir().unwrap();
    let (fresh, _) = instructions(&server.base_url, data.path());
    let cache = cache_file(data.path());
    expire(&cache);

    server.answer_with(NOTHING);
    let (stale, _) = instructions(&server.base_url, data.path());
    assert_eq!(stale, fresh);
    wait_for("the failed refresh to be recorded", || {
        fetched_at(&cache) > 0
    });
    assert_eq!(server.schema_reads(), 2);

    let (again, _) = instructions(&server.base_url, data.path());
    assert_eq!(again, fresh);
    thread::sleep(Duration::from_millis(500));
    assert_eq!(server.schema_reads(), 2, "retried before the retry window");
}

#[test]
fn parallel_cold_starts_read_the_schema_once() {
    let server = Server::start(Answers {
        delay: Duration::from_millis(800),
        ..FULL
    });
    let data = tempfile::tempdir().unwrap();
    let starts: Vec<_> = (0..6)
        .map(|_| {
            let (url, data) = (server.base_url.clone(), data.path().to_path_buf());
            thread::spawn(move || instructions(&url, &data).0)
        })
        .collect();
    for start in starts {
        start.join().unwrap();
    }
    assert_eq!(server.schema_reads(), 1);
}

#[test]
fn a_failed_first_read_does_not_hide_the_list_from_the_next_start() {
    let server = Server::start(NOTHING);
    let data = tempfile::tempdir().unwrap();
    let (first, _) = instructions(&server.base_url, data.path());
    assert_eq!(providers(&first), None, "{first}");

    server.answer_with(FULL);
    let (next, _) = instructions(&server.base_url, data.path());
    assert_eq!(
        providers(&next),
        Some("- gitlab: Project, User, File, Definition, Widget"),
        "{next}"
    );
}

#[test]
fn offline_gets_the_bundled_local_instructions_without_waiting() {
    let closed = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let data = tempfile::tempdir().unwrap();
    let (text, elapsed) = instructions(&base_url, data.path());
    assert!(text.starts_with("## Orbit"), "{text}");
    assert!(!text.contains("GitLab"), "{text}");
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
}

#[test]
fn a_slow_schema_read_costs_at_most_the_timeout() {
    let server = Server::start(Answers {
        delay: Duration::from_secs(10),
        ..FULL
    });
    let data = tempfile::tempdir().unwrap();
    let (text, elapsed) = instructions(&server.base_url, data.path());
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
    assert!(query_line(&text).contains(BUNDLED), "{text}");
}
