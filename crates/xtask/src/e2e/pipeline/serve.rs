//! `cargo xtask e2e serve` — port-forward services to localhost.
//!
//! Starts port-forwards and prints access info.
//! Runs in the foreground. Ctrl+C to stop.

use std::process::Stdio;

use anyhow::{Context, Result};
use tokio::process::Command;
use tokio::signal;

use crate::e2e::{config::Config, ui};

struct PortForward {
    label: &'static str,
    namespace: String,
    service: String,
    local_port: String,
    service_port: String,
}

pub async fn run(cfg: &Config) -> Result<()> {
    ui::banner("E2E Serve")?;

    let forwards = vec![
        PortForward {
            label: "GitLab UI",
            namespace: cfg.namespaces.gitlab.clone(),
            service: cfg.gitlab_ui.service.clone(),
            local_port: cfg.gitlab_ui.local_port.clone(),
            service_port: cfg.gitlab_ui.service_port.clone(),
        },
        PortForward {
            label: "GKG webserver",
            namespace: cfg.namespaces.default.clone(),
            service: cfg.gitlab_ui.gkg_service.clone(),
            local_port: cfg.gitlab_ui.gkg_local_port.clone(),
            service_port: cfg.gitlab_ui.gkg_service_port.clone(),
        },
    ];

    let mut children: Vec<tokio::process::Child> = Vec::new();
    for fwd in &forwards {
        match spawn_port_forward(fwd) {
            Ok(child) => children.push(child),
            Err(e) => {
                for child in &mut children {
                    let _ = child.kill().await;
                }
                return Err(e)
                    .with_context(|| format!("failed to start port-forward for {}", fwd.label));
            }
        }
    }

    for fwd in &forwards {
        ui::info(&format!(
            "{}: http://localhost:{}",
            fwd.label, fwd.local_port
        ))?;
    }
    ui::info(&format!("Login: root / {}", cfg.gitlab_ui.root_password))?;
    ui::info("Press Ctrl+C to stop")?;

    signal::ctrl_c().await?;

    ui::info("Shutting down port-forwards...")?;
    for child in &mut children {
        let _ = child.kill().await;
    }

    ui::outro("Serve stopped")?;
    Ok(())
}

fn spawn_port_forward(fwd: &PortForward) -> Result<tokio::process::Child> {
    Command::new("kubectl")
        .args([
            "port-forward",
            &format!("svc/{}", fwd.service),
            &format!("{}:{}", fwd.local_port, fwd.service_port),
            "-n",
            &fwd.namespace,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("kubectl port-forward")
}
