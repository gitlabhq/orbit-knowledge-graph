//! The lockdown without ClickHouse: the request guard every server runs, and the config check
//! that refuses to start on any endpoint that is not loopback.

mod support;

use std::net::SocketAddr;

use axum::Router;
use axum::routing::{get, post};
use orbit_demo::lockdown::{CSP, system_resolve};
use support::{config, exchange, get as get_request, post as post_request};

async fn guarded() -> SocketAddr {
    let router = Router::new()
        .route("/", get(|| async { "ok" }))
        .route("/write", post(|| async { "ok" }));
    orbit_demo::fakes::serve(router, 0).await.unwrap()
}

#[tokio::test]
async fn a_host_header_that_is_not_loopback_is_rejected() {
    let address = guarded().await;
    let own = format!("127.0.0.1:{}", address.port());
    let local = format!("localhost:{}", address.port());
    for host in [own.as_str(), local.as_str()] {
        let response = exchange(address, &get_request("/", host, "")).await;
        assert_eq!((response.status, response.body.as_str()), (200, "ok"));
    }
    let rebound = format!("attacker.example:{}", address.port());
    let wrong_port = format!("127.0.0.1:{}", address.port() + 1);
    for host in [
        rebound.as_str(),
        wrong_port.as_str(),
        "127.0.0.1",
        "192.168.1.20:8484",
    ] {
        let response = exchange(address, &get_request("/", host, "")).await;
        assert_eq!(response.status, 403, "{host}");
        assert_eq!(response.header("content-security-policy"), Some(CSP));
    }
}

#[tokio::test]
async fn a_foreign_origin_and_a_cross_site_fetch_are_rejected() {
    let address = guarded().await;
    let host = format!("localhost:{}", address.port());
    let ours = format!("Origin: http://{host}\r\n");
    assert_eq!(
        exchange(address, &get_request("/", &host, &ours))
            .await
            .status,
        200
    );
    for extra in [
        "Origin: http://attacker.example\r\n",
        "Origin: null\r\n",
        "Sec-Fetch-Site: cross-site\r\n",
    ] {
        assert_eq!(
            exchange(address, &get_request("/", &host, extra))
                .await
                .status,
            403,
            "{extra}"
        );
    }
}

#[tokio::test]
async fn writes_take_json_and_no_response_allows_cross_origin_reads() {
    let address = guarded().await;
    let host = format!("127.0.0.1:{}", address.port());
    let json = exchange(
        address,
        &post_request("/write", &host, "application/json", "", "{}"),
    )
    .await;
    assert_eq!(json.status, 200);
    for content_type in [
        "text/plain",
        "application/x-www-form-urlencoded",
        "multipart/form-data",
    ] {
        let response = exchange(
            address,
            &post_request("/write", &host, content_type, "", "{}"),
        )
        .await;
        assert_eq!(response.status, 403, "{content_type}");
    }
    let preflight = format!(
        "OPTIONS /write HTTP/1.1\r\nHost: {host}\r\nOrigin: http://attacker.example\r\n\
         Access-Control-Request-Method: POST\r\nConnection: close\r\n\r\n"
    );
    for request in [preflight, get_request("/", &host, "")] {
        let response = exchange(address, &request).await;
        assert!(
            !response
                .head
                .to_ascii_lowercase()
                .contains("access-control-"),
            "{}",
            response.head
        );
        assert_eq!(response.header("content-security-policy"), Some(CSP));
    }
}

fn refusal(
    edit: impl FnOnce(&mut orbit_demo::config::Config),
    resolve: &orbit_demo::lockdown::Resolve,
) -> String {
    let mut config = config();
    edit(&mut config);
    match config.check(resolve) {
        Ok(_) => panic!("the config passed"),
        Err(error) => error.to_string(),
    }
}

#[test]
fn every_endpoint_must_be_loopback() {
    support::checked();
    assert!(
        refusal(
            |c| c.clickhouse.url = "http://10.1.2.3:8123".into(),
            &system_resolve
        )
        .contains("10.1.2.3, which is not loopback")
    );
    assert!(
        refusal(
            |c| c.clickhouse.url = "http://0.0.0.0:18123".into(),
            &system_resolve
        )
        .contains("0.0.0.0")
    );
    assert!(
        refusal(
            |c| c.providers.slack = "http://[::]:18202".into(),
            &system_resolve
        )
        .contains("::")
    );
    assert!(
        refusal(
            |c| c.providers.orbit_next = "http://code.example:18201".into(),
            &system_resolve
        )
        .contains("only IP literals and localhost")
    );
    assert!(
        refusal(
            |c| c.providers.orbit_next = "http://[::1]:18201".into(),
            &system_resolve
        )
        .contains("127.0.0.1 only")
    );
}

#[test]
fn a_hosts_file_that_points_localhost_elsewhere_is_caught() {
    let overridden = |_: &str, port: u16| {
        Ok(vec![
            SocketAddr::from(([127, 0, 0, 1], port)),
            SocketAddr::from(([192, 168, 1, 20], port)),
        ])
    };
    let error = refusal(
        |c| c.clickhouse.url = "http://localhost:18123".into(),
        &overridden,
    );
    assert!(
        error.contains("192.168.1.20, which is not loopback"),
        "{error}"
    );
}

#[test]
fn the_binary_refuses_to_start_on_a_remote_clickhouse() {
    let dir = std::env::temp_dir().join(format!("orbit-demo-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let shipped =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/demo.yaml")).unwrap();
    let path = dir.join("remote.yaml");
    std::fs::write(
        &path,
        shipped.replace("http://127.0.0.1:18123", "http://10.20.30.40:8123"),
    )
    .unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_orbit-demo"))
        .arg(&path)
        .output()
        .unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("the demo refuses to start") && stderr.contains("10.20.30.40"),
        "{stderr}"
    );
}
