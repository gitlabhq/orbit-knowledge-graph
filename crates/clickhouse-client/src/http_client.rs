use std::time::Duration;

use hyper_timeout::TimeoutConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;

// Matches the default client of the clickhouse crate, so only the read timeout differs.
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

/// TCP keepalive does not find a dead replica behind a proxy that keeps the connection open,
/// so a read timeout is the only limit on how long a request waits for a reply.
pub(crate) fn clickhouse_client(read_timeout: Option<Duration>) -> clickhouse::Client {
    let mut http = HttpConnector::new();
    http.set_keepalive(Some(TCP_KEEPALIVE));
    http.enforce_http(false);

    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_provider_and_native_roots(rustls::crypto::aws_lc_rs::default_provider())
        .expect("the system has native root certificates")
        .https_or_http()
        .enable_http1()
        .wrap_connector(http);

    let mut connector = TimeoutConnector::new(https);
    connector.set_read_timeout(read_timeout);

    let http_client = Client::builder(TokioExecutor::new())
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .build(connector);
    clickhouse::Client::with_http_client(http_client)
}
