//! The only HTTP client in the demo: one JSON POST over a TCP connection to a loopback address.
//! It takes no URL and resolves no name, so it cannot reach past the machine.

use std::net::SocketAddr;

use anyhow::{Context, bail};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn post<B: Serialize, R: DeserializeOwned>(
    address: SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
    body: &B,
) -> anyhow::Result<R> {
    if !address.ip().is_loopback() {
        bail!("the demo client connects to loopback only, not {address}");
    }
    let body = serde_json::to_vec(body)?;
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    let mut stream = TcpStream::connect(address)
        .await
        .with_context(|| format!("connecting to {address}"))?;
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(&body).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let (status, body) = split(&response)?;
    if status != 200 {
        bail!(
            "{address}{path} answered {status}: {}",
            String::from_utf8_lossy(body)
        );
    }
    serde_json::from_slice(body).with_context(|| format!("decoding {address}{path}"))
}

fn split(response: &[u8]) -> anyhow::Result<(u16, &[u8])> {
    let end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .context("the response has no header end")?;
    let head = std::str::from_utf8(&response[..end])?;
    if head
        .lines()
        .any(|line| line.to_ascii_lowercase().starts_with("transfer-encoding:"))
    {
        bail!("the fakes answer with a Content-Length body");
    }
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .context("the response has no status")?;
    Ok((status, &response[end + 4..]))
}
