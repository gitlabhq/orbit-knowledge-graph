//! Exercises the transport against an in-process fake Workhorse: a
//! tokio-tungstenite upgrade server that answers the preauth the way the real
//! route does (101 + session headers, or an HTTP rejection) and serves a stub
//! `gitaly.BlobService` over the tunnel through a tonic server per connection.

#![cfg_attr(
    feature = "testkit",
    allow(
        dead_code,
        unused_imports,
        reason = "the exported test harness shares this module with its own transport tests"
    )
)]

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::{STANDARD as BASE64, URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use gitaly_protos::proto::blob_service_client::BlobServiceClient;
use gitaly_protos::proto::{
    GetArchiveRequest, GetArchiveResponse, ListBlobsRequest, ListBlobsResponse, Repository,
    list_blobs_response,
};
use orbit_server_config::GitlabClientConfiguration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::LazyConfigAcceptor;
use tokio_rustls::rustls::server::Acceptor;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::handshake::server::{
    Callback, ErrorResponse, Request, Response,
};
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Message, protocol::WebSocketConfig};
use tonic::codegen::http;
use tonic::server::NamedService;
use tonic::transport::server::Connected;
use tonic::{Code, Status};
use tower::{Service, service_fn};

use crate::GitlabClient;
use crate::client::{AUTH_HEADER, JWT_AUDIENCE, JWT_ISSUER, JWT_SUBJECT};
use crate::{GitalyProxyChannels, GitalyProxyDialer};
use crate::{
    GitalyProxyError, HEADER_EXPIRES_IN, HEADER_PROFILE, HEADER_REPOSITORY,
    PROFILE_READONLY_REPOSITORY, WebSocketIo,
};

const SECRET: &[u8] = b"test-secret-that-is-long-enough!";
const REPO_JSON: &str = r#"{"storage_name":"default","relative_path":"@hashed/6b/86/6b86.git","gl_repository":"project-42","gl_project_path":"group/proj"}"#;
const PROJECT_ID: i64 = 42;

/// What the fake answers to the upgrade request.
#[derive(Clone, Debug)]
pub enum Preauth {
    Upgrade {
        profile: &'static str,
        expires_in: &'static str,
        repository: Option<&'static str>,
    },
    Reject {
        status: StatusCode,
        retry_after: Option<&'static str>,
    },
}

impl Preauth {
    pub fn ok(expires_in: &'static str) -> Self {
        Self::Upgrade {
            profile: PROFILE_READONLY_REPOSITORY,
            expires_in,
            repository: Some(REPO_JSON),
        }
    }

    pub fn reject(status: StatusCode) -> Self {
        Self::Reject {
            status,
            retry_after: None,
        }
    }
}

/// Records the upgrade request, then answers it the way `preauth` says.
struct Answer {
    preauth: Preauth,
    observed: Arc<Observed>,
}

impl Callback for Answer {
    fn on_request(
        self,
        request: &Request,
        mut response: Response,
    ) -> Result<Response, ErrorResponse> {
        self.observed.requests.lock().unwrap().push(request.clone());
        match self.preauth {
            Preauth::Upgrade {
                profile,
                expires_in,
                repository,
            } => {
                let headers = response.headers_mut();
                headers.insert(HEADER_PROFILE, HeaderValue::from_static(profile));
                headers.insert(HEADER_EXPIRES_IN, HeaderValue::from_static(expires_in));
                if let Some(repository) = repository {
                    headers.insert(
                        HEADER_REPOSITORY,
                        HeaderValue::from_str(&URL_SAFE_NO_PAD.encode(repository)).unwrap(),
                    );
                }
                Ok(response)
            }
            Preauth::Reject {
                status,
                retry_after,
            } => {
                let mut rejection = ErrorResponse::new(Some("gitaly proxy: nope".into()));
                *rejection.status_mut() = status;
                if let Some(retry_after) = retry_after {
                    rejection
                        .headers_mut()
                        .insert("Retry-After", HeaderValue::from_static(retry_after));
                }
                Err(rejection)
            }
        }
    }
}

/// How the stub answers `ListBlobs` on connection number `conn`.
#[derive(Clone, Debug)]
pub enum StreamPlan {
    /// `count` responses, `interval` apart, each tagged with the connection.
    Serve {
        count: usize,
        interval: Duration,
    },
    Reject(Code, &'static str),
    Cut(Code),
}

pub type Director = Arc<dyn Fn(usize) -> StreamPlan + Send + Sync>;
type Expectation = fn(&GitalyProxyError) -> bool;

#[derive(Default)]
struct Observed {
    upgrades: AtomicUsize,
    rpcs: AtomicUsize,
    requests: Mutex<Vec<Request>>,
    client_closes: Mutex<Vec<Option<CloseFrame>>>,
    /// SNI presented on each TLS connection, in accept order.
    sni: Mutex<Vec<Option<String>>>,
    /// How the server end of each tunneled connection ended, by connection index.
    conn_ends: Mutex<Vec<(usize, ConnEnd)>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ConnEnd {
    CleanClose,
    Error(std::io::ErrorKind),
}

pub struct FakeWorkhorse {
    addr: SocketAddr,
    observed: Arc<Observed>,
}

impl FakeWorkhorse {
    pub async fn start(preauth: Preauth, director: Director) -> Self {
        Self::start_with(preauth, director, None).await
    }

    async fn start_tls(
        preauth: Preauth,
        director: Director,
        server_config: Arc<rustls::ServerConfig>,
    ) -> Self {
        Self::start_with(preauth, director, Some(server_config)).await
    }

    async fn start_with(
        preauth: Preauth,
        director: Director,
        tls: Option<Arc<rustls::ServerConfig>>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let observed = Arc::new(Observed::default());
        let accept_observed = Arc::clone(&observed);
        tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    return;
                };
                let preauth = preauth.clone();
                let director = Arc::clone(&director);
                let observed = Arc::clone(&accept_observed);
                match tls.clone() {
                    None => {
                        tokio::spawn(serve_connection(tcp, preauth, director, observed));
                    }
                    Some(config) => {
                        tokio::spawn(async move {
                            let acceptor = LazyConfigAcceptor::new(Acceptor::default(), tcp);
                            let Ok(start) = acceptor.await else {
                                return;
                            };
                            let sni = start.client_hello().server_name().map(str::to_owned);
                            observed.sni.lock().unwrap().push(sni);
                            let Ok(stream) = start.into_stream(config).await else {
                                return;
                            };
                            serve_connection(stream, preauth, director, observed).await;
                        });
                    }
                }
            }
        });
        Self { addr, observed }
    }

    fn conn_ends(&self) -> Vec<(usize, ConnEnd)> {
        self.observed.conn_ends.lock().unwrap().clone()
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn client(&self) -> GitlabClient {
        GitlabClient::new(GitlabClientConfiguration {
            base_url: self.base_url(),
            signing_key: BASE64.encode(SECRET),
            resolve_host: None,
        })
        .unwrap()
    }

    pub fn upgrades(&self) -> usize {
        self.observed.upgrades.load(Ordering::SeqCst)
    }

    pub fn rpcs(&self) -> usize {
        self.observed.rpcs.load(Ordering::SeqCst)
    }
}

fn preauth_is_acceptable(preauth: &Preauth) -> bool {
    matches!(
        preauth,
        Preauth::Upgrade {
            profile: PROFILE_READONLY_REPOSITORY,
            repository: Some(_),
            ..
        }
    )
}

async fn serve_connection<S>(tcp: S, preauth: Preauth, director: Director, observed: Arc<Observed>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let callback = Answer {
        preauth: preauth.clone(),
        observed: Arc::clone(&observed),
    };
    let Ok(mut socket) = tokio_tungstenite::accept_hdr_async(tcp, callback).await else {
        return;
    };
    let conn = observed.upgrades.fetch_add(1, Ordering::SeqCst);

    // The client must refuse these 101s before any gRPC flows, so the fake
    // only records how the client shut the socket instead of serving on it.
    if !preauth_is_acceptable(&preauth) {
        while let Some(Ok(message)) = socket.next().await {
            if let Message::Close(frame) = message {
                observed.client_closes.lock().unwrap().push(frame);
                return;
            }
        }
        return;
    }

    let stub = StubBlobService {
        conn,
        director,
        observed: Arc::clone(&observed),
    };
    let io = ServerIo {
        inner: WebSocketIo::new(socket),
        conn,
        observed,
        ended: false,
    };
    let _ = tonic::transport::Server::builder()
        .add_service(stub.clone())
        .add_service(StubRepositoryService(stub))
        .serve_with_incoming(futures::stream::once(async { Ok::<_, Infallible>(io) }))
        .await;
}

/// Server end of the tunnel. Reusing the client adapter on an accepted socket
/// is fine here because the fake never writes outside tonic's connection task.
struct ServerIo<S> {
    inner: WebSocketIo<S>,
    conn: usize,
    observed: Arc<Observed>,
    ended: bool,
}

impl<S> tokio::io::AsyncRead for ServerIo<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        let end = match &result {
            Poll::Ready(Ok(())) if buf.filled().len() == before => Some(ConnEnd::CleanClose),
            Poll::Ready(Err(error)) => Some(ConnEnd::Error(error.kind())),
            _ => None,
        };
        if let Some(end) = end
            && !self.ended
        {
            self.ended = true;
            self.observed
                .conn_ends
                .lock()
                .unwrap()
                .push((self.conn, end));
        }
        result
    }
}

impl<S> tokio::io::AsyncWrite for ServerIo<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl<S> Connected for ServerIo<S> {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

#[derive(Clone)]
struct StubBlobService {
    conn: usize,
    director: Director,
    observed: Arc<Observed>,
}

impl NamedService for StubBlobService {
    const NAME: &'static str = "gitaly.BlobService";
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

impl Service<http::Request<tonic::body::Body>> for StubBlobService {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        if request.uri().path() != "/gitaly.BlobService/ListBlobs" {
            return Box::pin(async move {
                let mut response = http::Response::new(tonic::body::Body::default());
                response
                    .headers_mut()
                    .insert(Status::GRPC_STATUS, (Code::Unimplemented as i32).into());
                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    tonic::metadata::GRPC_CONTENT_TYPE,
                );
                Ok(response)
            });
        }

        let conn = self.conn;
        let plan = (self.director)(conn);
        let observed = Arc::clone(&self.observed);
        let handler = service_fn(move |_request: tonic::Request<ListBlobsRequest>| {
            observed.rpcs.fetch_add(1, Ordering::SeqCst);
            let plan = plan.clone();
            async move {
                match plan {
                    StreamPlan::Reject(code, message) => Err(Status::new(code, message)),
                    StreamPlan::Cut(code) => {
                        let (tx, rx) = tokio::sync::mpsc::channel(2);
                        tokio::spawn(async move {
                            let _ = tx.send(Ok(blob_response(conn))).await;
                            let _ = tx.send(Err(Status::new(code, "stream cut"))).await;
                        });
                        Ok(tonic::Response::new(ReceiverStream::new(rx)))
                    }
                    StreamPlan::Serve { count, interval } => {
                        let (tx, rx) = tokio::sync::mpsc::channel(1);
                        tokio::spawn(async move {
                            for _ in 0..count {
                                tokio::time::sleep(interval).await;
                                if tx.send(Ok(blob_response(conn))).await.is_err() {
                                    return;
                                }
                            }
                        });
                        Ok(tonic::Response::new(ReceiverStream::new(rx)))
                    }
                }
            }
        });
        Box::pin(async move {
            let codec = tonic_prost::ProstCodec::<ListBlobsResponse, ListBlobsRequest>::default();
            Ok(tonic::server::Grpc::new(codec)
                .server_streaming(handler, request)
                .await)
        })
    }
}

#[derive(Clone)]
struct StubRepositoryService(StubBlobService);

impl NamedService for StubRepositoryService {
    const NAME: &'static str = "gitaly.RepositoryService";
}

impl Service<http::Request<tonic::body::Body>> for StubRepositoryService {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let conn = self.0.conn;
        let plan = (self.0.director)(conn);
        let observed = Arc::clone(&self.0.observed);
        let handler = service_fn(move |_request: tonic::Request<GetArchiveRequest>| {
            observed.rpcs.fetch_add(1, Ordering::SeqCst);
            let plan = plan.clone();
            async move {
                match plan {
                    StreamPlan::Reject(code, message) => Err(Status::new(code, message)),
                    StreamPlan::Cut(code) => {
                        let (tx, rx) = tokio::sync::mpsc::channel(2);
                        tokio::spawn(async move {
                            let _ = tx
                                .send(Ok(GetArchiveResponse {
                                    data: b"partial".to_vec(),
                                }))
                                .await;
                            let _ = tx.send(Err(Status::new(code, "stream cut"))).await;
                        });
                        Ok(tonic::Response::new(ReceiverStream::new(rx)))
                    }
                    StreamPlan::Serve { count, interval } => {
                        let (tx, rx) = tokio::sync::mpsc::channel(1);
                        tokio::spawn(async move {
                            for _ in 0..count {
                                tokio::time::sleep(interval).await;
                                if tx
                                    .send(Ok(GetArchiveResponse {
                                        data: b"archive".to_vec(),
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        });
                        Ok(tonic::Response::new(ReceiverStream::new(rx)))
                    }
                }
            }
        });
        Box::pin(async move {
            let codec = tonic_prost::ProstCodec::<GetArchiveResponse, GetArchiveRequest>::default();
            Ok(tonic::server::Grpc::new(codec)
                .server_streaming(handler, request)
                .await)
        })
    }
}

fn blob_response(conn: usize) -> ListBlobsResponse {
    ListBlobsResponse {
        blobs: vec![list_blobs_response::Blob {
            path: format!("conn-{conn}").into_bytes(),
            ..Default::default()
        }],
    }
}

pub fn serve(count: usize, interval_ms: u64) -> Director {
    Arc::new(move |_| StreamPlan::Serve {
        count,
        interval: Duration::from_millis(interval_ms),
    })
}

pub fn direct(plans: &'static [StreamPlan]) -> Director {
    Arc::new(move |conn| {
        plans
            .get(conn)
            .cloned()
            .unwrap_or(plans[plans.len() - 1].clone())
    })
}

async fn list_blobs(
    channel: Arc<crate::GitalyProxyChannel>,
) -> Result<tonic::Streaming<ListBlobsResponse>, Status> {
    let mut client = BlobServiceClient::new(channel.channel());
    let request = ListBlobsRequest {
        repository: Some(channel.repository()),
        revisions: vec!["HEAD".into()],
        with_paths: true,
        ..Default::default()
    };
    client
        .list_blobs(request)
        .await
        .map(tonic::Response::into_inner)
}

async fn blob_paths(mut stream: tonic::Streaming<ListBlobsResponse>) -> Vec<String> {
    let mut paths = Vec::new();
    while let Some(response) = stream.message().await.unwrap() {
        for blob in response.blobs {
            paths.push(String::from_utf8(blob.path).unwrap());
        }
    }
    paths
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < deadline, "condition not met within 5 s");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Raw WebSocket pair for the adapter tests: `server` gets the accepted socket,
/// the returned adapter wraps the client side with the production frame limits.
async fn raw_pair<F, Fut>(server: F) -> WebSocketIo<tokio_tungstenite::MaybeTlsStream<TcpStream>>
where
    F: FnOnce(WebSocketStream<TcpStream>) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
        server(socket).await;
    });
    let config = WebSocketConfig::default()
        .max_message_size(Some(1024 * 1024))
        .max_frame_size(Some(1024 * 1024));
    let (socket, _) =
        tokio_tungstenite::connect_async_with_config(format!("ws://{addr}"), Some(config), false)
            .await
            .unwrap();
    WebSocketIo::new(socket)
}

mod websocket_io {
    use super::*;

    async fn close_with(frame: Option<CloseFrame>) -> std::io::Result<usize> {
        let mut io = raw_pair(|mut socket| async move {
            socket.send(Message::Close(frame)).await.unwrap();
            while socket.next().await.is_some() {}
        })
        .await;
        io.read(&mut [0u8; 8]).await
    }

    #[tokio::test]
    async fn drains_one_message_across_short_reads() {
        let mut io = raw_pair(|mut socket| async move {
            socket
                .send(Message::Binary(Bytes::from_static(b"hello")))
                .await
                .unwrap();
            socket
                .send(Message::Binary(Bytes::from_static(b"world")))
                .await
                .unwrap();
            while socket.next().await.is_some() {}
        })
        .await;

        let mut chunks = Vec::new();
        let mut buf = [0u8; 3];
        for _ in 0..4 {
            let n = io.read(&mut buf).await.unwrap();
            chunks.push(String::from_utf8_lossy(&buf[..n]).into_owned());
        }
        assert_eq!(chunks, ["hel", "lo", "wor", "ld"]);
    }

    #[tokio::test]
    async fn only_normal_and_away_closes_are_eof() {
        for code in [CloseCode::Normal, CloseCode::Away] {
            let read = close_with(Some(CloseFrame {
                code,
                reason: "bye".into(),
            }))
            .await;
            assert_eq!(read.unwrap(), 0, "{code:?}");
        }

        for code in [CloseCode::Error, CloseCode::Protocol, CloseCode::Size] {
            let read = close_with(Some(CloseFrame {
                code,
                reason: "".into(),
            }))
            .await;
            assert_eq!(
                read.unwrap_err().kind(),
                std::io::ErrorKind::ConnectionReset,
                "{code:?}"
            );
        }

        assert_eq!(
            close_with(None).await.unwrap_err().kind(),
            std::io::ErrorKind::ConnectionReset
        );
    }

    #[tokio::test]
    async fn socket_ending_without_close_is_an_error() {
        let mut io = raw_pair(|socket| async move {
            drop(socket.into_inner());
        })
        .await;
        assert!(io.read(&mut [0u8; 8]).await.is_err());
    }

    #[tokio::test]
    async fn pong_reaches_the_server_while_the_client_only_reads() {
        let (pong_tx, pong_rx) = tokio::sync::oneshot::channel();
        let mut io = raw_pair(|mut socket| async move {
            socket
                .send(Message::Ping(Bytes::from_static(b"keepalive")))
                .await
                .unwrap();
            let pong = loop {
                match socket.next().await {
                    Some(Ok(Message::Pong(payload))) => break payload,
                    Some(Ok(_)) => continue,
                    other => panic!("expected a pong, got {other:?}"),
                }
            };
            pong_tx.send(pong).unwrap();
            socket
                .send(Message::Binary(Bytes::from_static(b"done")))
                .await
                .unwrap();
            while socket.next().await.is_some() {}
        })
        .await;

        let mut buf = [0u8; 8];
        let n = io.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"done");
        assert_eq!(pong_rx.await.unwrap(), Bytes::from_static(b"keepalive"));
    }

    #[tokio::test]
    async fn oversized_inbound_message_is_an_error_not_a_stall() {
        let mut io = raw_pair(|mut socket| async move {
            let oversized = Bytes::from(vec![0u8; 1024 * 1024 + 1]);
            let _ = socket.send(Message::Binary(oversized)).await;
            while socket.next().await.is_some() {}
        })
        .await;
        let error = tokio::time::timeout(Duration::from_secs(5), io.read(&mut [0u8; 8]))
            .await
            .expect("read must not stall")
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn each_write_is_one_binary_message() {
        let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
        let mut io = raw_pair(|mut socket| async move {
            let mut seen = Vec::new();
            while seen.len() < 2 {
                match socket.next().await {
                    Some(Ok(Message::Binary(data))) => seen.push(data),
                    Some(Ok(_)) => continue,
                    other => panic!("expected binary, got {other:?}"),
                }
            }
            seen_tx.send(seen).unwrap();
            while socket.next().await.is_some() {}
        })
        .await;

        io.write_all(b"first").await.unwrap();
        io.write_all(b"second").await.unwrap();
        io.flush().await.unwrap();
        assert_eq!(
            seen_rx.await.unwrap(),
            [Bytes::from_static(b"first"), Bytes::from_static(b"second")]
        );
    }

    #[tokio::test]
    async fn shutdown_sends_a_normal_close_frame() {
        let (close_tx, close_rx) = tokio::sync::oneshot::channel();
        let mut io = raw_pair(|mut socket| async move {
            let frame = loop {
                match socket.next().await {
                    Some(Ok(Message::Close(frame))) => break frame,
                    Some(Ok(_)) => continue,
                    other => panic!("expected a close frame, got {other:?}"),
                }
            };
            close_tx.send(frame).unwrap();
            while socket.next().await.is_some() {}
        })
        .await;

        io.shutdown().await.unwrap();
        let frame = close_rx
            .await
            .unwrap()
            .expect("close frame with a status code");
        assert_eq!(frame.code, CloseCode::Normal);
    }
}

mod handshake {
    use super::*;

    fn reject(status: StatusCode, retry_after: Option<&'static str>) -> Preauth {
        Preauth::Reject {
            status,
            retry_after,
        }
    }

    fn first_request(fake: &FakeWorkhorse) -> Request {
        fake.observed.requests.lock().unwrap()[0].clone()
    }

    #[tokio::test]
    async fn upgrade_carries_a_fresh_kg_jwt_on_the_session_path() {
        let fake = FakeWorkhorse::start(Preauth::ok("600"), serve(1, 0)).await;
        fake.client().gitaly_channel(PROJECT_ID).await.unwrap();

        let request = first_request(&fake);
        assert_eq!(
            request.uri().path(),
            "/api/v4/internal/gitaly_proxy/project/42/ws"
        );
        let token = request.headers()[AUTH_HEADER].to_str().unwrap();
        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
        validation.set_issuer(&[JWT_ISSUER]);
        validation.set_audience(&[JWT_AUDIENCE]);
        let claims = jsonwebtoken::decode::<serde_json::Value>(
            token,
            &jsonwebtoken::DecodingKey::from_secret(SECRET),
            &validation,
        )
        .unwrap()
        .claims;
        assert_eq!(claims["sub"], JWT_SUBJECT);
    }

    #[tokio::test]
    async fn session_parameters_come_from_the_101_headers() {
        let fake = FakeWorkhorse::start(Preauth::ok("600"), serve(1, 0)).await;
        let before = Instant::now();
        let channel = fake.client().gitaly_channel(PROJECT_ID).await.unwrap();
        let after = Instant::now();

        let expected: Repository = Repository {
            storage_name: "default".into(),
            relative_path: "@hashed/6b/86/6b86.git".into(),
            gl_repository: "project-42".into(),
            gl_project_path: "group/proj".into(),
            ..Default::default()
        };
        assert_eq!(channel.repository(), expected);
        assert_eq!(channel.project_id(), PROJECT_ID);
        let session_age = Duration::from_secs(600);
        assert!(channel.expires_at() >= before + session_age);
        assert!(channel.expires_at() <= after + session_age);
        assert_eq!(
            channel.rotate_at(),
            channel.expires_at() - Duration::from_secs(60)
        );
        assert!(channel.accepts_new_streams());
        assert_eq!(fake.upgrades(), 1);
    }

    #[tokio::test]
    async fn unknown_profile_is_refused_with_a_normal_close() {
        let fake = FakeWorkhorse::start(
            Preauth::Upgrade {
                profile: "multi_repository",
                expires_in: "600",
                repository: Some(REPO_JSON),
            },
            serve(1, 0),
        )
        .await;

        let error = fake.client().gitaly_channel(PROJECT_ID).await.unwrap_err();
        assert!(
            matches!(&error, GitalyProxyError::UnsupportedProfile { profile } if profile == "multi_repository"),
            "{error}"
        );

        wait_until(|| !fake.observed.client_closes.lock().unwrap().is_empty()).await;
        let close = fake.observed.client_closes.lock().unwrap()[0]
            .clone()
            .expect("close frame with a status code");
        assert_eq!(close.code, CloseCode::Normal);
        assert_eq!(close.reason, "unsupported_profile");
    }

    #[tokio::test]
    async fn missing_repository_header_is_refused_with_a_normal_close() {
        let fake = FakeWorkhorse::start(
            Preauth::Upgrade {
                profile: PROFILE_READONLY_REPOSITORY,
                expires_in: "600",
                repository: None,
            },
            serve(1, 0),
        )
        .await;

        let error = fake.client().gitaly_channel(PROJECT_ID).await.unwrap_err();
        assert!(
            matches!(
                error,
                GitalyProxyError::MalformedSessionHeader {
                    header: HEADER_REPOSITORY,
                    ..
                }
            ),
            "{error}"
        );

        wait_until(|| !fake.observed.client_closes.lock().unwrap().is_empty()).await;
        let close = fake.observed.client_closes.lock().unwrap()[0]
            .clone()
            .unwrap();
        assert_eq!(close.code, CloseCode::Normal);
        assert_eq!(close.reason, "malformed_session");
    }

    #[tokio::test]
    async fn http_rejections_map_to_typed_errors() {
        let cases: Vec<(Preauth, Expectation)> = vec![
            (reject(StatusCode::UNAUTHORIZED, None), |e| {
                matches!(e, GitalyProxyError::Unauthorized)
            }),
            (reject(StatusCode::FORBIDDEN, None), |e| {
                matches!(e, GitalyProxyError::Forbidden { project_id: 42 })
            }),
            (reject(StatusCode::NOT_FOUND, None), |e| {
                matches!(e, GitalyProxyError::NotAvailable { project_id: 42 })
            }),
            (reject(StatusCode::TOO_MANY_REQUESTS, Some("7")), |e| {
                matches!(
                    e,
                    GitalyProxyError::Busy {
                        status: 429,
                        retry_after: Some(d)
                    } if *d == Duration::from_secs(7)
                )
            }),
            (reject(StatusCode::SERVICE_UNAVAILABLE, None), |e| {
                matches!(
                    e,
                    GitalyProxyError::Busy {
                        status: 503,
                        retry_after: None
                    }
                )
            }),
            (reject(StatusCode::BAD_GATEWAY, None), |e| {
                matches!(e, GitalyProxyError::BadGateway)
            }),
            (reject(StatusCode::INTERNAL_SERVER_ERROR, None), |e| {
                matches!(e, GitalyProxyError::UnexpectedStatus { status: 500 })
            }),
        ];

        for (preauth, matches) in cases {
            let fake = FakeWorkhorse::start(preauth.clone(), serve(1, 0)).await;
            let error = fake.client().gitaly_channel(PROJECT_ID).await.unwrap_err();
            assert!(matches(&error), "{preauth:?} -> {error}");
            assert_eq!(fake.upgrades(), 0);
        }
    }

    #[tokio::test]
    async fn resolve_host_redirects_the_tcp_connect_but_keeps_the_host_header() {
        let fake = FakeWorkhorse::start(Preauth::ok("600"), serve(1, 0)).await;
        let base_url = format!("http://gitlab.example.test:{}", fake.addr.port());
        let client = GitlabClient::new(GitlabClientConfiguration {
            base_url: base_url.clone(),
            signing_key: BASE64.encode(SECRET),
            resolve_host: Some("127.0.0.1".into()),
        })
        .unwrap();

        client.gitaly_channel(PROJECT_ID).await.unwrap();

        let request = first_request(&fake);
        assert_eq!(
            request.headers()[http::header::HOST].to_str().unwrap(),
            format!("gitlab.example.test:{}", fake.addr.port())
        );
    }
}

mod rotation {
    use super::*;

    #[tokio::test]
    async fn new_streams_move_to_a_fresh_session_while_old_streams_finish() {
        // A 2 s session sits below the 5 s rotation floor, so the channel is
        // single-use and every new stream after the first must re-upgrade.
        let fake = FakeWorkhorse::start(Preauth::ok("2"), serve(6, 100)).await;
        let client = fake.client();

        let first_channel = client.gitaly_channel(PROJECT_ID).await.unwrap();
        let first = list_blobs(Arc::clone(&first_channel)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let second_channel = client.gitaly_channel(PROJECT_ID).await.unwrap();
        assert!(!Arc::ptr_eq(&first_channel, &second_channel));
        let second = list_blobs(Arc::clone(&second_channel)).await.unwrap();

        let (first_paths, second_paths) = tokio::join!(blob_paths(first), blob_paths(second));
        assert_eq!(first_paths, vec!["conn-0"; 6]);
        assert_eq!(second_paths, vec!["conn-1"; 6]);
        assert_eq!(fake.upgrades(), 2);
        assert_eq!(fake.rpcs(), 2);

        // The slot already let go of conn 0 when it rotated; the test's own
        // holder is the only thing keeping the old socket open.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(fake.conn_ends().is_empty(), "{:?}", fake.conn_ends());

        drop(first_channel);
        wait_until(|| !fake.conn_ends().is_empty()).await;
        assert_eq!(fake.conn_ends(), vec![(0, ConnEnd::CleanClose)]);
        drop(second_channel);
    }

    #[tokio::test]
    async fn a_fresh_session_is_shared_by_consecutive_streams() {
        let fake = FakeWorkhorse::start(Preauth::ok("600"), serve(2, 0)).await;
        let client = fake.client();

        for _ in 0..3 {
            let channel = client.gitaly_channel(PROJECT_ID).await.unwrap();
            let paths = blob_paths(list_blobs(channel).await.unwrap()).await;
            assert_eq!(paths, vec!["conn-0"; 2]);
        }
        assert_eq!(fake.upgrades(), 1);
        assert_eq!(fake.rpcs(), 3);
    }

    #[tokio::test]
    async fn projects_get_separate_sessions() {
        let fake = FakeWorkhorse::start(Preauth::ok("600"), serve(1, 0)).await;
        let client = fake.client();

        let a = client.gitaly_channel(1).await.unwrap();
        let b = client.gitaly_channel(2).await.unwrap();
        let a_again = client.gitaly_channel(1).await.unwrap();

        assert!(Arc::ptr_eq(&a, &a_again));
        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(fake.upgrades(), 2);
        let paths: Vec<String> = fake
            .observed
            .requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| request.uri().path().to_owned())
            .collect();
        assert_eq!(
            paths,
            [
                "/api/v4/internal/gitaly_proxy/project/1/ws",
                "/api/v4/internal/gitaly_proxy/project/2/ws"
            ]
        );
    }
}

mod retry_once {
    use super::*;

    const SERVE_ONE: StreamPlan = StreamPlan::Serve {
        count: 1,
        interval: Duration::ZERO,
    };

    #[tokio::test]
    async fn stale_session_is_rotated_and_retried_exactly_once() {
        for stale in [
            StreamPlan::Reject(Code::Unauthenticated, "proxy: session_expired"),
            StreamPlan::Reject(Code::Unavailable, "proxy: shutting_down"),
        ] {
            let plans: &'static [StreamPlan] = Box::leak(Box::new([stale.clone(), SERVE_ONE]));
            let fake = FakeWorkhorse::start(Preauth::ok("600"), direct(plans)).await;

            let stream = fake
                .client()
                .with_gitaly_channel(PROJECT_ID, list_blobs)
                .await
                .unwrap();
            assert_eq!(blob_paths(stream).await, vec!["conn-1"]);
            assert_eq!(fake.upgrades(), 2);
            assert_eq!(fake.rpcs(), 2);
        }
    }

    #[tokio::test]
    async fn a_second_stale_rejection_is_not_retried_again() {
        let fake = FakeWorkhorse::start(
            Preauth::ok("600"),
            Arc::new(|_| StreamPlan::Reject(Code::Unauthenticated, "proxy: session_expired")),
        )
        .await;

        let error = fake
            .client()
            .with_gitaly_channel(PROJECT_ID, list_blobs)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, GitalyProxyError::StaleAfterRetry(status) if status.code() == Code::Unauthenticated),
            "{error}"
        );
        assert_eq!(fake.upgrades(), 2);
        assert_eq!(fake.rpcs(), 2);
    }

    #[tokio::test]
    async fn non_stale_rejections_are_not_retried() {
        let cases: Vec<(StreamPlan, Expectation)> = vec![
            (
                StreamPlan::Reject(Code::DeadlineExceeded, "proxy: stream_deadline"),
                |e| matches!(e, GitalyProxyError::StreamDeadline),
            ),
            (
                StreamPlan::Reject(Code::PermissionDenied, "proxy: method_not_allowed"),
                |e| matches!(e, GitalyProxyError::PolicyDenied { reason } if reason == "method_not_allowed"),
            ),
            (
                StreamPlan::Reject(Code::NotFound, "repository not found"),
                |e| matches!(e, GitalyProxyError::Rpc(status) if status.code() == Code::NotFound),
            ),
            (
                StreamPlan::Reject(Code::Unavailable, "connection reset by gitaly"),
                |e| matches!(e, GitalyProxyError::Rpc(status) if status.code() == Code::Unavailable),
            ),
        ];

        for (plan, matches) in cases {
            let rejected = plan.clone();
            let fake =
                FakeWorkhorse::start(Preauth::ok("600"), Arc::new(move |_| rejected.clone())).await;

            let error = fake
                .client()
                .with_gitaly_channel(PROJECT_ID, list_blobs)
                .await
                .unwrap_err();
            assert!(matches(&error), "{plan:?} -> {error}");
            assert_eq!(fake.upgrades(), 1);
            assert_eq!(fake.rpcs(), 1);
        }
    }

    #[tokio::test]
    async fn a_successful_first_attempt_is_not_retried() {
        let fake = FakeWorkhorse::start(Preauth::ok("600"), serve(3, 0)).await;

        let stream = fake
            .client()
            .with_gitaly_channel(PROJECT_ID, list_blobs)
            .await
            .unwrap();
        assert_eq!(blob_paths(stream).await, vec!["conn-0"; 3]);
        assert_eq!(fake.upgrades(), 1);
        assert_eq!(fake.rpcs(), 1);
    }
}

mod tls {
    use super::*;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    const DOMAIN: &str = "gitlab.example.test";

    /// Self-signed certificate for `san` plus a client config that trusts only
    /// it, so verification is real but hermetic.
    fn test_pki(san: &str) -> (Arc<rustls::ServerConfig>, Arc<rustls::ClientConfig>) {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec![san.to_owned()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let cert_der: CertificateDer<'static> = cert.der().clone();
        let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der()));

        let server = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert_der).unwrap();
        let client = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        (Arc::new(server), Arc::new(client))
    }

    /// `.test` never resolves, so a completed handshake proves `resolve_host`
    /// carried the TCP connect to the fake while the URL kept the domain.
    fn channels(fake: &FakeWorkhorse, client: Arc<rustls::ClientConfig>) -> GitalyProxyChannels {
        let base_url = format!("https://{DOMAIN}:{}", fake.addr.port());
        let dialer = GitalyProxyDialer::new(&base_url, Some("127.0.0.1"), SECRET.to_vec())
            .unwrap()
            .with_tls_config(client);
        GitalyProxyChannels::new(dialer)
    }

    #[tokio::test]
    async fn tunnel_completes_over_tls_with_the_configured_domain_as_sni_and_host() {
        let (server, client) = test_pki(DOMAIN);
        let fake = FakeWorkhorse::start_tls(Preauth::ok("600"), serve(2, 0), server).await;

        let channel = channels(&fake, client).get(PROJECT_ID).await.unwrap();
        let paths = blob_paths(list_blobs(channel).await.unwrap()).await;

        assert_eq!(paths, vec!["conn-0"; 2]);
        assert_eq!(fake.upgrades(), 1);
        assert_eq!(
            *fake.observed.sni.lock().unwrap(),
            vec![Some(DOMAIN.to_owned())]
        );
        let request = fake.observed.requests.lock().unwrap()[0].clone();
        assert_eq!(
            request.headers()[http::header::HOST].to_str().unwrap(),
            format!("{DOMAIN}:{}", fake.addr.port())
        );
    }

    #[tokio::test]
    async fn certificate_for_another_name_fails_the_handshake() {
        let (server, client) = test_pki("other.example.test");
        let fake = FakeWorkhorse::start_tls(Preauth::ok("600"), serve(1, 0), server).await;

        let error = channels(&fake, client).get(PROJECT_ID).await.unwrap_err();

        assert!(matches!(error, GitalyProxyError::Handshake(_)), "{error}");
        assert_eq!(fake.upgrades(), 0);
        assert!(fake.observed.requests.lock().unwrap().is_empty());
    }
}
