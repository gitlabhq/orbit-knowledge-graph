use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::active_schema::ActiveSchema;
use crate::cli::Mode;

pub type Check = Box<dyn Fn() -> Result<(), String> + Send + Sync + 'static>;

pub fn schema_installed(active: &ActiveSchema) -> Result<(), String> {
    active
        .snapshot()
        .map(|_| ())
        .map_err(|_| "no active schema snapshot installed".to_string())
}

pub fn gate_cleared(serving: &AtomicBool) -> Result<(), String> {
    if serving.load(Ordering::Relaxed) {
        Ok(())
    } else {
        Err("startup gate not cleared".to_string())
    }
}

pub fn readiness_checks(
    mode: Mode,
    active: &Arc<ActiveSchema>,
    serving: &Arc<AtomicBool>,
) -> Vec<(&'static str, Check)> {
    match mode {
        Mode::Webserver => {
            let active = active.clone();
            vec![("schema", Box::new(move || schema_installed(&active)))]
        }
        Mode::Indexer | Mode::DispatchIndexing => {
            let serving = serving.clone();
            vec![("schema_gate", Box::new(move || gate_cleared(&serving)))]
        }
        Mode::HealthCheck => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::net::{SocketAddr, TcpListener};

    use labkit::health::ReadinessCheck;

    use super::*;

    fn pinned_schema() -> Arc<ActiveSchema> {
        let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
        ActiveSchema::pinned(ontology)
    }

    fn free_addr() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    }

    async fn probe_server(checks: Vec<(&'static str, Check)>) -> String {
        let addr = free_addr();
        let checks = checks
            .into_iter()
            .map(|(name, check)| ReadinessCheck::new(name, check))
            .collect();
        labkit::health::spawn_server(addr, checks, None).unwrap();
        format!("http://{addr}")
    }

    async fn get(url: &str) -> (reqwest::StatusCode, String) {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let response = reqwest::get(url).await.unwrap();
        let status = response.status();
        (status, response.text().await.unwrap())
    }

    #[test]
    fn schema_check_fails_until_a_snapshot_is_installed() {
        assert!(schema_installed(&ActiveSchema::default()).is_err());
        assert!(schema_installed(&pinned_schema()).is_ok());
    }

    #[test]
    fn gate_check_follows_the_serving_flag() {
        let serving = AtomicBool::new(false);
        assert!(gate_cleared(&serving).is_err());
        serving.store(true, Ordering::Relaxed);
        assert!(gate_cleared(&serving).is_ok());
    }

    #[test]
    fn each_mode_registers_its_own_gate() {
        let active = Arc::new(ActiveSchema::default());
        let serving = Arc::new(AtomicBool::new(false));

        let names = |mode| -> Vec<&'static str> {
            readiness_checks(mode, &active, &serving)
                .into_iter()
                .map(|(name, _)| name)
                .collect()
        };

        assert_eq!(names(Mode::Webserver), ["schema"]);
        assert_eq!(names(Mode::Indexer), ["schema_gate"]);
        assert_eq!(names(Mode::DispatchIndexing), ["schema_gate"]);
        assert!(names(Mode::HealthCheck).is_empty());
    }

    #[tokio::test]
    async fn probe_server_readiness_follows_the_indexer_gate() {
        let active = Arc::new(ActiveSchema::default());
        let serving = Arc::new(AtomicBool::new(false));
        let base = probe_server(readiness_checks(Mode::Indexer, &active, &serving)).await;

        let (status, body) = get(&format!("{base}/-/readiness")).await;
        assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE);
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["status"], "error");
        assert_eq!(json["checks"]["schema_gate"], "startup gate not cleared");

        serving.store(true, Ordering::Relaxed);

        let (status, body) = get(&format!("{base}/-/readiness")).await;
        assert_eq!(status, reqwest::StatusCode::OK);
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["status"], "ok");
        assert_eq!(json["checks"]["schema_gate"], "ok");

        let (status, _) = get(&format!("{base}/-/liveness")).await;
        assert_eq!(status, reqwest::StatusCode::OK);
    }

    #[tokio::test]
    async fn probe_server_readiness_follows_the_webserver_schema() {
        let serving = Arc::new(AtomicBool::new(false));

        let pending = probe_server(readiness_checks(
            Mode::Webserver,
            &Arc::new(ActiveSchema::default()),
            &serving,
        ))
        .await;
        let (status, body) = get(&format!("{pending}/-/readiness")).await;
        assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("\"schema\":\"no active schema snapshot installed\""));

        let ready = probe_server(readiness_checks(
            Mode::Webserver,
            &pinned_schema(),
            &serving,
        ))
        .await;
        let (status, _) = get(&format!("{ready}/-/readiness")).await;
        assert_eq!(status, reqwest::StatusCode::OK);
    }
}
