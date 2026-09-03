use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use gitaly_protos::proto::Repository;
use rustls_platform_verifier::ConfigVerifierExt;
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::client::ClientRequestBuilder;
use tokio_tungstenite::tungstenite::http::{self, HeaderMap, StatusCode, Uri};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::{Connector, MaybeTlsStream};
use tracing::debug;

use super::error::GitalyProxyError;
use super::websocket_io::WebSocketIo;
use crate::client::{AUTH_HEADER, sign_jwt};

pub const HEADER_PROFILE: &str = "Gitlab-Gitaly-Proxy-Profile";
pub const HEADER_REPOSITORY: &str = "Gitlab-Gitaly-Proxy-Repository";
pub const HEADER_EXPIRES_IN: &str = "Gitlab-Gitaly-Proxy-Expires-In";

/// The only profile this client implements. Compared byte-for-byte; a newer
/// server announcing another profile must be refused (wire contract §3, §8).
pub const PROFILE_READONLY_REPOSITORY: &str = "readonly_repository";

/// The server closes the connection with 1009 above this; mirroring it on the
/// client turns an oversized inbound message into an error instead of a stall.
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;

/// Session parameters captured from the 101 response; the only place the
/// client learns them.
#[derive(Debug, Clone)]
pub struct Session {
    pub repository: Repository,
    pub profile: String,
    pub expires_in: Duration,
    pub connected_at: Instant,
}

impl Session {
    pub fn expires_at(&self) -> Instant {
        self.connected_at + self.expires_in
    }
}

#[derive(Deserialize)]
struct RepositoryCoordinates {
    storage_name: String,
    relative_path: String,
    #[serde(default)]
    gl_repository: String,
    #[serde(default)]
    gl_project_path: String,
}

/// Everything needed to open a proxy session; shared by every channel the
/// client dials so TLS configuration is built once.
#[derive(Debug)]
pub struct Dialer {
    ws_base: Uri,
    connect_addr: Option<IpAddr>,
    signing_key: SigningKey,
    secure: bool,
    tls: OnceLock<Arc<rustls::ClientConfig>>,
}

struct SigningKey(Vec<u8>);

impl std::fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SigningKey(<redacted>)")
    }
}

impl Dialer {
    pub fn new(
        base_url: &str,
        resolve_host: Option<&str>,
        signing_key: Vec<u8>,
    ) -> Result<Self, GitalyProxyError> {
        let parsed = reqwest::Url::parse(base_url)
            .map_err(|e| GitalyProxyError::InvalidBaseUrl(e.to_string()))?;
        let ws_scheme = match parsed.scheme() {
            "http" => "ws",
            "https" => "wss",
            other => {
                return Err(GitalyProxyError::InvalidBaseUrl(format!(
                    "unsupported scheme {other}"
                )));
            }
        };
        let host = parsed
            .host_str()
            .ok_or_else(|| GitalyProxyError::InvalidBaseUrl("no host".into()))?;
        let authority = match parsed.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_owned(),
        };
        let ws_base: Uri = format!("{ws_scheme}://{authority}")
            .parse()
            .map_err(|e| GitalyProxyError::InvalidBaseUrl(format!("{e}")))?;

        let connect_addr = resolve_host
            .map(|resolve_host| {
                let port = parsed.port_or_known_default().unwrap_or(0);
                std::net::ToSocketAddrs::to_socket_addrs(&(resolve_host, port))
                    .map_err(|e| {
                        GitalyProxyError::InvalidBaseUrl(format!(
                            "failed to resolve {resolve_host}: {e}"
                        ))
                    })?
                    .next()
                    .map(|addr| addr.ip())
                    .ok_or_else(|| {
                        GitalyProxyError::InvalidBaseUrl(format!(
                            "no addresses found for {resolve_host}"
                        ))
                    })
            })
            .transpose()?;

        Ok(Self {
            ws_base,
            connect_addr,
            signing_key: SigningKey(signing_key),
            secure: ws_scheme == "wss",
            tls: OnceLock::new(),
        })
    }

    /// Replaces the platform trust store with `config` for every `wss://`
    /// dial. Production always verifies against the platform roots; this is
    /// the seam for tests that terminate TLS in-process with their own CA.
    pub fn with_tls_config(self, config: Arc<rustls::ClientConfig>) -> Self {
        Self {
            tls: OnceLock::from(config),
            ..self
        }
    }

    /// Built on first use so a host without a CA bundle fails at dial time,
    /// like the reqwest client does, not at construction.
    fn tls_config(&self) -> Result<Arc<rustls::ClientConfig>, GitalyProxyError> {
        if let Some(config) = self.tls.get() {
            return Ok(Arc::clone(config));
        }
        // Idempotent; the Err case means a provider is already installed.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        // Same trust store as the reqwest client, so a certificate that works
        // for the HTTP path works for the tunnel.
        let config = rustls::ClientConfig::with_platform_verifier()
            .map_err(|e| GitalyProxyError::Handshake(format!("TLS config: {e}")))?;
        Ok(Arc::clone(self.tls.get_or_init(|| Arc::new(config))))
    }

    pub fn session_uri(&self, project_id: i64) -> Uri {
        format!(
            "{}/api/v4/internal/gitaly_proxy/project/{project_id}/ws",
            self.ws_base.to_string().trim_end_matches('/')
        )
        .parse()
        .expect("base URI and numeric project id form a valid URI")
    }

    /// Performs one upgrade: fresh KG-JWT, TCP (+TLS) connect, RFC 6455
    /// handshake, 101-header validation.
    pub async fn handshake(
        &self,
        project_id: i64,
    ) -> Result<(WebSocketIo<MaybeTlsStream<TcpStream>>, Session), GitalyProxyError> {
        let uri = self.session_uri(project_id);
        let token = sign_jwt(&self.signing_key.0).map_err(GitalyProxyError::JwtSigning)?;
        let request = ClientRequestBuilder::new(uri.clone()).with_header(AUTH_HEADER, token);

        let host = uri.host().unwrap_or_default();
        let port = uri.port_u16().unwrap_or(if self.secure { 443 } else { 80 });
        let tcp = match self.connect_addr {
            Some(ip) => TcpStream::connect(SocketAddr::new(ip, port)).await,
            None => TcpStream::connect((host, port)).await,
        }
        .map_err(|e| GitalyProxyError::Handshake(format!("connect {host}:{port}: {e}")))?;

        let connector = if self.secure {
            Connector::Rustls(self.tls_config()?)
        } else {
            Connector::Plain
        };
        let config = WebSocketConfig::default()
            .max_message_size(Some(MAX_MESSAGE_BYTES))
            .max_frame_size(Some(MAX_MESSAGE_BYTES));

        debug!(project_id, uri = %uri, "opening gitaly proxy session");
        let (mut socket, response) = tokio_tungstenite::client_async_tls_with_config(
            request,
            tcp,
            Some(config),
            Some(connector),
        )
        .await
        .map_err(|e| map_handshake_error(e, project_id))?;

        let session = match parse_session(response.headers()) {
            Ok(session) => session,
            Err(error) => {
                let reason = match &error {
                    GitalyProxyError::UnsupportedProfile { .. } => "unsupported_profile",
                    _ => "malformed_session",
                };
                let _ = socket
                    .close(Some(CloseFrame {
                        code: CloseCode::Normal,
                        reason: reason.into(),
                    }))
                    .await;
                return Err(error);
            }
        };

        debug!(
            project_id,
            storage = %session.repository.storage_name,
            relative_path = %session.repository.relative_path,
            expires_in_secs = session.expires_in.as_secs(),
            "gitaly proxy session open"
        );
        Ok((WebSocketIo::new(socket), session))
    }
}

fn map_handshake_error(error: WsError, project_id: i64) -> GitalyProxyError {
    match error {
        WsError::Http(response) => {
            let status = response.status();
            match status {
                StatusCode::UNAUTHORIZED => GitalyProxyError::Unauthorized,
                StatusCode::FORBIDDEN => GitalyProxyError::Forbidden { project_id },
                StatusCode::NOT_FOUND => GitalyProxyError::NotAvailable { project_id },
                StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE => {
                    GitalyProxyError::Busy {
                        status: status.as_u16(),
                        retry_after: retry_after(response.headers()),
                    }
                }
                StatusCode::BAD_GATEWAY => GitalyProxyError::BadGateway,
                _ => GitalyProxyError::UnexpectedStatus {
                    status: status.as_u16(),
                },
            }
        }
        other => GitalyProxyError::Handshake(other.to_string()),
    }
}

fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    headers
        .get(http::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

pub fn parse_session(headers: &HeaderMap) -> Result<Session, GitalyProxyError> {
    let header_str = |name: &'static str| -> Result<&str, GitalyProxyError> {
        headers
            .get(name)
            .ok_or(GitalyProxyError::MalformedSessionHeader {
                header: name,
                reason: "missing".into(),
            })?
            .to_str()
            .map_err(|e| GitalyProxyError::MalformedSessionHeader {
                header: name,
                reason: e.to_string(),
            })
    };

    let profile = header_str(HEADER_PROFILE)?;
    if profile != PROFILE_READONLY_REPOSITORY {
        return Err(GitalyProxyError::UnsupportedProfile {
            profile: profile.to_owned(),
        });
    }

    let expires_in = header_str(HEADER_EXPIRES_IN)?
        .trim()
        .parse::<u64>()
        .map_err(|e| GitalyProxyError::MalformedSessionHeader {
            header: HEADER_EXPIRES_IN,
            reason: e.to_string(),
        })?;

    let encoded = header_str(HEADER_REPOSITORY)?;
    let json =
        URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|e| GitalyProxyError::MalformedSessionHeader {
                header: HEADER_REPOSITORY,
                reason: e.to_string(),
            })?;
    let coordinates: RepositoryCoordinates =
        serde_json::from_slice(&json).map_err(|e| GitalyProxyError::MalformedSessionHeader {
            header: HEADER_REPOSITORY,
            reason: e.to_string(),
        })?;
    if coordinates.storage_name.is_empty() || coordinates.relative_path.is_empty() {
        return Err(GitalyProxyError::MalformedSessionHeader {
            header: HEADER_REPOSITORY,
            reason: "storage_name and relative_path are required".into(),
        });
    }

    Ok(Session {
        repository: Repository {
            storage_name: coordinates.storage_name,
            relative_path: coordinates.relative_path,
            gl_repository: coordinates.gl_repository,
            gl_project_path: coordinates.gl_project_path,
            ..Default::default()
        },
        profile: profile.to_owned(),
        expires_in: Duration::from_secs(expires_in),
        connected_at: Instant::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::http::HeaderValue;

    fn headers(profile: &str, expires_in: &str, repository_json: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_PROFILE, HeaderValue::from_str(profile).unwrap());
        headers.insert(
            HEADER_EXPIRES_IN,
            HeaderValue::from_str(expires_in).unwrap(),
        );
        headers.insert(
            HEADER_REPOSITORY,
            HeaderValue::from_str(&URL_SAFE_NO_PAD.encode(repository_json)).unwrap(),
        );
        headers
    }

    const REPO_JSON: &str = r#"{"storage_name":"default","relative_path":"@hashed/6b/86/6b86.git","gl_repository":"project-42","gl_project_path":"group/proj"}"#;

    #[test]
    fn parses_the_three_session_headers() {
        let session = parse_session(&headers("readonly_repository", "600", REPO_JSON)).unwrap();
        assert_eq!(session.repository.storage_name, "default");
        assert_eq!(session.repository.relative_path, "@hashed/6b/86/6b86.git");
        assert_eq!(session.repository.gl_repository, "project-42");
        assert_eq!(session.repository.gl_project_path, "group/proj");
        assert_eq!(session.expires_in, Duration::from_secs(600));
        assert_eq!(session.profile, PROFILE_READONLY_REPOSITORY);
    }

    #[test]
    fn profile_is_compared_byte_for_byte() {
        for profile in [
            "Readonly_Repository",
            "readonly_repository_v2",
            "",
            "multi_repository",
        ] {
            let err = parse_session(&headers(profile, "600", REPO_JSON)).unwrap_err();
            assert!(
                matches!(&err, GitalyProxyError::UnsupportedProfile { profile: p } if p == profile),
                "{profile:?} -> {err}"
            );
        }
    }

    #[test]
    fn rejects_missing_or_malformed_headers() {
        let mut missing = headers("readonly_repository", "600", REPO_JSON);
        missing.remove(HEADER_REPOSITORY);
        assert!(matches!(
            parse_session(&missing).unwrap_err(),
            GitalyProxyError::MalformedSessionHeader {
                header: HEADER_REPOSITORY,
                ..
            }
        ));

        assert!(matches!(
            parse_session(&headers("readonly_repository", "soon", REPO_JSON)).unwrap_err(),
            GitalyProxyError::MalformedSessionHeader {
                header: HEADER_EXPIRES_IN,
                ..
            }
        ));

        assert!(matches!(
            parse_session(&headers(
                "readonly_repository",
                "600",
                r#"{"storage_name":"default"}"#
            ))
            .unwrap_err(),
            GitalyProxyError::MalformedSessionHeader {
                header: HEADER_REPOSITORY,
                ..
            }
        ));

        let mut padded = headers("readonly_repository", "600", REPO_JSON);
        padded.insert(
            HEADER_REPOSITORY,
            HeaderValue::from_str(&base64::engine::general_purpose::STANDARD.encode(REPO_JSON))
                .unwrap(),
        );
        assert!(matches!(
            parse_session(&padded).unwrap_err(),
            GitalyProxyError::MalformedSessionHeader {
                header: HEADER_REPOSITORY,
                ..
            }
        ));
    }

    #[test]
    fn builds_the_session_uri_from_base_url() {
        let dialer = Dialer::new("https://gitlab.example.com", None, vec![1]).unwrap();
        assert_eq!(
            dialer.session_uri(42).to_string(),
            "wss://gitlab.example.com/api/v4/internal/gitaly_proxy/project/42/ws"
        );

        let dialer = Dialer::new("http://127.0.0.1:3000/", None, vec![1]).unwrap();
        assert_eq!(
            dialer.session_uri(7).to_string(),
            "ws://127.0.0.1:3000/api/v4/internal/gitaly_proxy/project/7/ws"
        );

        assert!(matches!(
            Dialer::new("ftp://gitlab.example.com", None, vec![1]).unwrap_err(),
            GitalyProxyError::InvalidBaseUrl(_)
        ));
    }

    #[test]
    fn resolve_host_overrides_the_connect_address() {
        let dialer = Dialer::new("https://gitlab.example.com", Some("localhost"), vec![1]).unwrap();
        assert!(dialer.connect_addr.unwrap().is_loopback());

        assert!(matches!(
            Dialer::new(
                "https://gitlab.example.com",
                Some("no-such-host.invalid"),
                vec![1]
            )
            .unwrap_err(),
            GitalyProxyError::InvalidBaseUrl(_)
        ));
    }
}
