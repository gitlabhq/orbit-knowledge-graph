use std::net::SocketAddr;
use std::path::Path;

use orbit_demo::config::{Checked, Config};
use orbit_demo::lockdown::system_resolve;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Response {
    pub status: u16,
    pub head: String,
    pub body: String,
}

impl Response {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.head.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }
}

/// One raw HTTP/1.1 exchange, so tests control every header the server sees.
pub async fn exchange(address: SocketAddr, request: &str) -> Response {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw).to_string();
    let (head, body) = text.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    let head = head.to_string();
    let chunked = head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked");
    let body = if chunked {
        dechunk(body)
    } else {
        body.to_string()
    };
    Response { status, head, body }
}

fn dechunk(mut body: &str) -> String {
    let mut out = String::new();
    while let Some((size, rest)) = body.split_once("\r\n") {
        let size = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
        if size == 0 {
            break;
        }
        out.push_str(&rest[..size]);
        body = rest[size..].trim_start_matches("\r\n");
    }
    out
}

pub fn get(path: &str, host: &str, extra: &str) -> String {
    format!("GET {path} HTTP/1.1\r\nHost: {host}\r\n{extra}Connection: close\r\n\r\n")
}

pub fn post(path: &str, host: &str, content_type: &str, extra: &str, body: &str) -> String {
    format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: {content_type}\r\n{extra}\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// The shipped config on free ports and the test database.
pub fn config() -> Config {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("demo.yaml");
    let mut config = Config::read(&path).unwrap();
    config.page_port = 0;
    config.providers.orbit_next = "http://127.0.0.1:0".into();
    config.providers.slack = "http://127.0.0.1:0".into();
    config.clickhouse.database = "orbit_demo_test".into();
    config
}

pub fn checked() -> Checked {
    config().check(&system_resolve).unwrap()
}
