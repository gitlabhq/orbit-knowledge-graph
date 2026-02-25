//! E2E environment teardown.
//!
//! Full teardown (inverse of CNG deploy + CNG setup + GKG stack):
//!   1. Tear down GKG stack (uninstall Helm chart, delete ClickHouse, secrets)
//!   2. Uninstall GitLab Helm release, delete PVCs and namespace
//!   3. Remove CNG setup artifacts (postgres-credentials secret)
//!   4. Uninstall Traefik Helm release
//!   5. Clean up local artifacts (.dev/ logs)
//!   6. Stop and delete the Colima VM (unless --keep-colima)

use std::fs;

use anyhow::Result;
use xshell::Shell;

use crate::e2e::{
    config::Config,
    constants as c,
    infra::{
        colima, docker, helm,
        kube::{self, DeleteTarget},
    },
    ui,
};

/// Run the E2E teardown.
///
/// - `gkg_only`: only tear down GKG resources (step 1), keeping GitLab
///   and Colima running.
/// - `keep_colima`: remove everything *except* the Colima VM.
///
/// When `gkg_only` is set, `keep_colima` is ignored (Colima is always kept).
pub async fn run(sh: &Shell, cfg: &Config, keep_colima: bool, gkg_only: bool) -> Result<()> {
    ui::banner("E2E Teardown")?;
    ui::detail("GKG only", &gkg_only.to_string())?;
    ui::detail("Keep Colima", &keep_colima.to_string())?;

    // Step 1 is always executed.
    teardown_gkg_stack(sh, cfg).await?;

    if gkg_only {
        // Only clean GKG-related local artifacts, then exit early.
        cleanup_local_artifacts_gkg(cfg)?;

        ui::info("GKG-only teardown complete. GitLab and Colima still running.")?;
        ui::info("Re-run GKG stack: cargo xtask e2e setup --gkg-only")?;
        ui::outro("Teardown complete")?;
        return Ok(());
    }

    let docker_host = cfg.docker_host();

    teardown_gitlab(sh, cfg, &docker_host).await?;
    teardown_cngsetup_artifacts(cfg).await?;
    teardown_traefik(sh, cfg, &docker_host)?;
    cleanup_local_artifacts(cfg)?;

    if keep_colima {
        ui::step(6, "Keeping Colima VM (--keep-colima)")?;
        ui::info(&format!(
            "Colima profile '{}' still running",
            cfg.colima.profile
        ))?;

        // Show what images are still present
        let prefix_glob = format!("{}/*", cfg.cng.local_prefix);
        if let Ok(lines) = docker::list_images(&cfg.colima.profile, &prefix_glob).await {
            for line in &lines {
                let _ = ui::info(line);
            }
        }
    } else {
        teardown_colima(sh, cfg)?;
    }

    if keep_colima {
        ui::info(&format!(
            "Colima VM still running. To fully remove:\n  \
             colima stop --profile {profile}\n  \
             colima delete --profile {profile} --force",
            profile = cfg.colima.profile
        ))?;
    }

    ui::info("To start fresh: cargo xtask e2e setup")?;
    ui::outro("Teardown complete")?;

    Ok(())
}

// -- Step 1: Tear down GKG stack (Helm chart + ClickHouse) --------------------

/// Uninstall the GKG Helm chart and remove all GKG resources from the default
/// namespace (ClickHouse, NATS, siphon, dispatch-indexing, secrets, PVCs).
async fn teardown_gkg_stack(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(1, "Tearing down GKG stack")?;

    let ns = &cfg.namespaces.default;
    let ch_svc = &cfg.clickhouse.service_name;

    // Uninstall the GKG Helm release first (removes NATS, siphon, GKG pods).
    let gkg_release = &cfg.helm.gkg.release;
    let docker_host = cfg.docker_host();
    ui::info("Uninstalling GKG Helm release...")?;
    helm::uninstall(sh, gkg_release, ns, &docker_host);

    let ch_label = cfg.ch_label();
    let dispatch_job = &cfg.gkg.dispatch_job;
    let init_cm = &cfg.clickhouse.init_configmap;
    let ss_names: [&str; 1] = [ch_svc];
    let job_names: [&str; 1] = [dispatch_job];
    let svc_names: [&str; 1] = [ch_svc];
    let cm_names: [&str; 1] = [init_cm];
    let secret_names: [&str; 3] = [
        &cfg.postgres.bridge_secret_name,
        &cfg.clickhouse.credentials_secret,
        &cfg.gkg.server_credentials_secret,
    ];

    let _ = tokio::join!(
        kube::delete(ns, "apps/v1", "StatefulSet", DeleteTarget::Names(&ss_names)),
        kube::delete(ns, "batch/v1", "Job", DeleteTarget::Names(&job_names)),
        kube::delete(ns, "v1", "Service", DeleteTarget::Names(&svc_names)),
        kube::delete(ns, "v1", "ConfigMap", DeleteTarget::Names(&cm_names)),
        kube::delete(ns, "v1", "Secret", DeleteTarget::Names(&secret_names)),
        kube::delete(
            ns,
            "v1",
            "PersistentVolumeClaim",
            DeleteTarget::Label(&ch_label)
        ),
    );

    ui::info("GKG stack resources removed")?;
    Ok(())
}

// -- Step 2: Tear down GitLab -------------------------------------------------

async fn teardown_gitlab(sh: &Shell, cfg: &Config, docker_host: &str) -> Result<()> {
    ui::step(2, "Tearing down GitLab")?;

    let ns = &cfg.namespaces.gitlab;
    let release = &cfg.helm.gitlab.release;

    if helm::release_exists(sh, release, ns, docker_host) {
        ui::info("Uninstalling GitLab Helm release")?;
        helm::uninstall_with_timeout(sh, release, ns, &cfg.helm.uninstall_timeout, docker_host);
        ui::info("GitLab Helm release removed")?;
    } else {
        ui::info("No GitLab Helm release found")?;
    }

    ui::info("Removing GitLab PVCs")?;
    let _ = kube::delete(ns, "v1", "PersistentVolumeClaim", DeleteTarget::Label("")).await;

    ui::info(&format!("Removing {ns} namespace"))?;
    let _ = kube::delete_namespace(ns).await;

    Ok(())
}

// -- Step 3: Remove CNG setup artifacts ---------------------------------------

async fn teardown_cngsetup_artifacts(cfg: &Config) -> Result<()> {
    ui::step(3, "Removing CNG setup artifacts")?;

    let ns = &cfg.namespaces.default;
    let bridge_secret = &cfg.postgres.bridge_secret_name;

    let _ = kube::delete(ns, "v1", "Secret", DeleteTarget::Names(&[bridge_secret])).await;
    ui::info(&format!("Removed {bridge_secret} secret"))?;

    Ok(())
}

// -- Step 4: Tear down Traefik ------------------------------------------------

fn teardown_traefik(sh: &Shell, cfg: &Config, docker_host: &str) -> Result<()> {
    ui::step(4, "Tearing down Traefik")?;

    let release = &cfg.helm.traefik.release;
    let kube_ns = &cfg.namespaces.kube_system;

    if helm::release_exists(sh, release, kube_ns, docker_host) {
        ui::info("Uninstalling Traefik")?;
        helm::uninstall(sh, release, kube_ns, docker_host);
        ui::info("Traefik removed")?;
    } else {
        ui::info("No Traefik release found")?;
    }

    Ok(())
}

// -- Step 5: Clean up local artifacts -----------------------------------------

fn cleanup_local_artifacts(cfg: &Config) -> Result<()> {
    ui::step(5, "Cleaning up local artifacts")?;
    do_cleanup_local_artifacts(cfg, c::TEARDOWN_LOG_FILES)
}

/// GKG-only variant — removes only GKG-phase logs, leaving CNG-phase logs
/// (colima-start, create-test-data, manifest) intact.
fn cleanup_local_artifacts_gkg(cfg: &Config) -> Result<()> {
    ui::step(2, "Cleaning up GKG local artifacts")?;
    do_cleanup_local_artifacts(cfg, c::GKG_TEARDOWN_LOG_FILES)
}

fn do_cleanup_local_artifacts(cfg: &Config, log_files: &[&str]) -> Result<()> {
    if cfg.log_dir.is_dir() {
        for name in log_files {
            let path = cfg.log_dir.join(name);
            if path.exists() {
                fs::remove_file(&path)?;
            }
        }
        ui::info("Cleaned .dev/")?;
    }

    Ok(())
}

// -- Step 6: Stop and delete Colima -------------------------------------------

fn teardown_colima(sh: &Shell, cfg: &Config) -> Result<()> {
    let profile = &cfg.colima.profile;
    ui::step(6, &format!("Stopping Colima (profile: {profile})"))?;

    if colima::is_running(sh, profile) {
        ui::info("Stopping and deleting Colima VM")?;
        colima::stop_and_delete(sh, profile);
        ui::info("Colima VM deleted")?;
    } else {
        ui::info(&format!("Colima ({profile}) not running"))?;
    }

    ui::info("CNG images are gone with the Colima VM")?;
    Ok(())
}
