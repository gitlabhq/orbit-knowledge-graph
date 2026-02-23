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
use xshell::{Shell, cmd};

use super::cmd as cmd_helpers;
use super::config::Config;
use super::constants as c;
use super::kubectl;
use super::ui;

/// Run the E2E teardown.
///
/// - `gkg_only`: only tear down GKG resources (step 1), keeping GitLab
///   and Colima running.
/// - `keep_colima`: remove everything *except* the Colima VM.
///
/// When `gkg_only` is set, `keep_colima` is ignored (Colima is always kept).
pub fn run(sh: &Shell, cfg: &Config, keep_colima: bool, gkg_only: bool) -> Result<()> {
    ui::banner("E2E Teardown")?;
    ui::detail("GKG only", &gkg_only.to_string())?;
    ui::detail("Keep Colima", &keep_colima.to_string())?;

    // Step 1 is always executed.
    teardown_gkg_stack(sh, cfg)?;

    if gkg_only {
        // Only clean GKG-related local artifacts, then exit early.
        cleanup_local_artifacts_gkg(cfg)?;

        ui::info("GKG-only teardown complete. GitLab and Colima still running.")?;
        ui::info("Re-run GKG stack: cargo xtask e2e setup --gkg-only")?;
        ui::outro("Teardown complete")?;
        return Ok(());
    }

    let docker_host = cfg.docker_host();

    teardown_gitlab(sh, cfg, &docker_host)?;
    teardown_cngsetup_artifacts(sh, cfg)?;
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
        let fmt = "  {{.Repository}}:{{.Tag}}  ({{.Size}})";
        let _ = cmd!(sh, "docker images {prefix_glob} --format {fmt}")
            .env("DOCKER_HOST", &docker_host)
            .quiet()
            .ignore_status()
            .run();
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
fn teardown_gkg_stack(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(1, "Tearing down GKG stack")?;

    let default_ns = &cfg.namespaces.default;
    let ch_svc = &cfg.clickhouse.service_name;

    // Uninstall the GKG Helm release first (removes NATS, siphon, GKG pods).
    let gkg_release = &cfg.helm.gkg.release;
    ui::info("Uninstalling GKG Helm release...")?;
    let _ = cmd!(sh, "helm uninstall {gkg_release} -n {default_ns}")
        .quiet()
        .ignore_status()
        .run();

    // Delete ClickHouse StatefulSet + Service + init ConfigMap + PVCs.
    let _ = cmd!(
        sh,
        "kubectl delete statefulset -n {default_ns} {ch_svc} --ignore-not-found"
    )
    .quiet()
    .ignore_status()
    .run();

    let dispatch_job = &cfg.gkg.dispatch_job;
    let _ = cmd!(
        sh,
        "kubectl delete job -n {default_ns} {dispatch_job} --ignore-not-found"
    )
    .quiet()
    .ignore_status()
    .run();

    let _ = cmd!(
        sh,
        "kubectl delete service -n {default_ns} {ch_svc} --ignore-not-found"
    )
    .quiet()
    .ignore_status()
    .run();

    let ch_init_cm = &cfg.clickhouse.init_configmap;
    let _ = cmd!(
        sh,
        "kubectl delete configmap -n {default_ns} {ch_init_cm} --ignore-not-found"
    )
    .quiet()
    .ignore_status()
    .run();

    // Delete secrets created by the xtask setup.
    let bridge_secret = &cfg.postgres.bridge_secret_name;
    let ch_cred = &cfg.clickhouse.credentials_secret;
    let gkg_cred = &cfg.gkg.server_credentials_secret;
    let _ = cmd!(
        sh,
        "kubectl delete secret -n {default_ns} {bridge_secret} {ch_cred} {gkg_cred} --ignore-not-found"
    )
    .quiet()
    .ignore_status()
    .run();

    // Delete ClickHouse PVCs.
    let ch_label = cfg.ch_label();
    let _ = cmd!(
        sh,
        "kubectl delete pvc -n {default_ns} -l {ch_label} --ignore-not-found"
    )
    .quiet()
    .ignore_status()
    .run();

    ui::info("GKG stack resources removed")?;
    Ok(())
}

// -- Step 2: Tear down GitLab -------------------------------------------------

fn teardown_gitlab(sh: &Shell, cfg: &Config, docker_host: &str) -> Result<()> {
    ui::step(2, "Tearing down GitLab")?;

    let ns = &cfg.namespaces.gitlab;
    let release = &cfg.helm.gitlab.release;

    if kubectl::helm_release_exists(sh, release, ns, docker_host) {
        ui::info("Uninstalling GitLab Helm release")?;
        let timeout = &cfg.helm.uninstall_timeout;
        let _ = cmd!(sh, "helm uninstall {release} -n {ns} --timeout {timeout}")
            .env("DOCKER_HOST", docker_host)
            .ignore_status()
            .run();
        ui::info("GitLab Helm release removed")?;
    } else {
        ui::info("No GitLab Helm release found")?;
    }

    ui::info("Removing GitLab PVCs")?;
    let _ = cmd!(sh, "kubectl delete pvc -n {ns} --all --ignore-not-found")
        .quiet()
        .ignore_status()
        .run();

    ui::info(&format!("Removing {ns} namespace"))?;
    let _ = cmd!(
        sh,
        "kubectl delete namespace {ns} --ignore-not-found --timeout=120s"
    )
    .quiet()
    .ignore_status()
    .run();

    Ok(())
}

// -- Step 3: Remove CNG setup artifacts ---------------------------------------

fn teardown_cngsetup_artifacts(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(3, "Removing CNG setup artifacts")?;

    let default_ns = &cfg.namespaces.default;
    let bridge_secret = &cfg.postgres.bridge_secret_name;

    let _ = cmd!(
        sh,
        "kubectl delete secret {bridge_secret} -n {default_ns} --ignore-not-found"
    )
    .quiet()
    .ignore_status()
    .run();
    ui::info(&format!("Removed {bridge_secret} secret"))?;

    Ok(())
}

// -- Step 4: Tear down Traefik ------------------------------------------------

fn teardown_traefik(sh: &Shell, cfg: &Config, docker_host: &str) -> Result<()> {
    ui::step(4, "Tearing down Traefik")?;

    let release = &cfg.helm.traefik.release;
    let kube_ns = &cfg.namespaces.kube_system;

    if kubectl::helm_release_exists(sh, release, kube_ns, docker_host) {
        ui::info("Uninstalling Traefik")?;
        let _ = cmd!(sh, "helm uninstall {release} -n {kube_ns}")
            .env("DOCKER_HOST", docker_host)
            .ignore_status()
            .run();
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

    if cmd_helpers::succeeds(sh, "colima", &["status", "--profile", profile]) {
        ui::info("Stopping Colima VM")?;
        let _ = cmd!(sh, "colima stop --profile {profile}")
            .ignore_status()
            .run();

        ui::info("Deleting Colima VM")?;
        let _ = cmd!(sh, "colima delete --profile {profile} --force")
            .ignore_status()
            .run();

        ui::info("Colima VM deleted")?;
    } else {
        ui::info(&format!("Colima ({profile}) not running"))?;
    }

    ui::info("CNG images are gone with the Colima VM")?;
    Ok(())
}
