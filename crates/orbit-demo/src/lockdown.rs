//! The demo's network lockdown: every server binds loopback, every endpoint is checked to be
//! loopback before startup, and every request passes the Host, Origin and content-type checks.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};

use anyhow::{Context, bail};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

pub const BIND: Ipv4Addr = Ipv4Addr::LOCALHOST;

pub const CSP: &str = "default-src 'self'; connect-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'";

pub async fn listen(port: u16) -> std::io::Result<tokio::net::TcpListener> {
    tokio::net::TcpListener::bind(SocketAddr::from((BIND, port))).await
}

/// Resolves a host the way the system does, `/etc/hosts` included.
pub type Resolve = dyn Fn(&str, u16) -> std::io::Result<Vec<SocketAddr>>;

pub fn system_resolve(host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
    Ok((host, port).to_socket_addrs()?.collect())
}

/// The one address an endpoint (`http://host:port` or `host:port`) connects to, if it is an IP
/// literal or `localhost` and every address it resolves to is 127.0.0.0/8 or ::1.
pub fn loopback(endpoint: &str, resolve: &Resolve) -> anyhow::Result<SocketAddr> {
    let authority = endpoint.strip_prefix("http://").unwrap_or(endpoint);
    let authority = authority.trim_end_matches('/');
    if authority.contains('/') || authority.contains('@') {
        bail!("{endpoint} is not a plain http://host:port endpoint");
    }
    let (host, port) = authority
        .rsplit_once(':')
        .with_context(|| format!("{endpoint} names no port"))?;
    let port: u16 = port
        .parse()
        .with_context(|| format!("{endpoint} has a bad port"))?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let addresses = match host.parse::<IpAddr>() {
        Ok(ip) => vec![SocketAddr::new(ip, port)],
        Err(_) if host.eq_ignore_ascii_case("localhost") => {
            resolve(host, port).with_context(|| format!("resolving {endpoint}"))?
        }
        Err(_) => bail!("{endpoint} names {host}; only IP literals and localhost are allowed"),
    };
    let Some(first) = addresses.first() else {
        bail!("{endpoint} resolves to nothing");
    };
    for address in &addresses {
        let ip = address.ip();
        if ip.is_unspecified() || !ip.is_loopback() {
            bail!("{endpoint} resolves to {ip}, which is not loopback");
        }
    }
    Ok(*first)
}

/// Rejects requests that did not come from our own page on loopback, then hardens the response.
pub async fn guard(State(port): State<u16>, request: Request, next: Next) -> Response {
    let response = match admit(port, request.method(), request.headers()) {
        Ok(()) => next.run(request).await,
        Err(reason) => (StatusCode::FORBIDDEN, reason).into_response(),
    };
    harden(response)
}

fn admit(port: u16, method: &Method, headers: &HeaderMap) -> Result<(), &'static str> {
    let own = [format!("localhost:{port}"), format!("127.0.0.1:{port}")];
    let text = |name: HeaderName| headers.get(name).and_then(|value| value.to_str().ok());
    if !text(header::HOST).is_some_and(|host| own.iter().any(|own| own == host)) {
        return Err("the Host header is not this server on loopback");
    }
    if let Some(origin) = headers.get(header::ORIGIN) {
        let ours = origin
            .to_str()
            .ok()
            .and_then(|origin| origin.strip_prefix("http://"))
            .is_some_and(|origin| own.iter().any(|own| own == origin));
        if !ours {
            return Err("the Origin header is not this server");
        }
    }
    if text(HeaderName::from_static("sec-fetch-site")) == Some("cross-site") {
        return Err("cross-site requests are not served");
    }
    if !matches!(*method, Method::GET | Method::HEAD) {
        let json = text(header::CONTENT_TYPE).is_some_and(|value| {
            let media = value.split(';').next().unwrap_or_default().trim();
            media.eq_ignore_ascii_case("application/json")
        });
        if !json {
            return Err("writes take Content-Type: application/json");
        }
    }
    Ok(())
}

fn harden(mut response: Response) -> Response {
    let headers = response.headers_mut();
    let cors: Vec<HeaderName> = headers
        .keys()
        .filter(|name| name.as_str().starts_with("access-control-"))
        .cloned()
        .collect();
    for name in cors {
        headers.remove(name);
    }
    let set = [
        (header::CONTENT_SECURITY_POLICY, CSP),
        (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        (header::X_FRAME_OPTIONS, "DENY"),
        (header::REFERRER_POLICY, "no-referrer"),
        (
            HeaderName::from_static("cross-origin-resource-policy"),
            "same-origin",
        ),
        (
            HeaderName::from_static("cross-origin-opener-policy"),
            "same-origin",
        ),
    ];
    for (name, value) in set {
        headers.insert(name, HeaderValue::from_static(value));
    }
    response
}
