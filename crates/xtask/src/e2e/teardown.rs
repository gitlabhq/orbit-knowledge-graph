//! E2E environment teardown.
//!
//! Full teardown (inverse of CNG deploy + CNG setup + GKG stack):
//!   1. Tear down Tilt / GKG stack (stop Tilt, delete ClickHouse, GKG resources)
//!   2. Uninstall GitLab Helm release, delete PVCs and namespace
//!   3. Remove CNG setup artifacts (postgres-credentials secret)
//!   4. Uninstall Traefik Helm release
//!   5. Clean up local artifacts (.dev/ logs, e2e/tilt/.secrets)
//!   6. Stop and delete the Colima VM (unless --keep-colima)

use std::fs;

use anyhow::Result;
use xshell::{Shell, cmd};

use super::cmd as cmd_helpers;
use super::config::Config;
use super::constants as c;
use super::kubectl;
use super::pipeline::gkg as gkg_pipeline;
use super::ui;

/// Run the E2E teardown.
///
/// - `gkg_only`: only tear down GKG/Tilt resources (step 1), keeping
///   GitLab and Colima running. Equivalent to `teardown.sh --tilt-only`.
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
    teardown_traefik(sh, &docker_host)?;
    cleanup_local_artifacts(cfg)?;

    if keep_colima {
        ui::step(6, "Keeping Colima VM (--keep-colima)")?;
        ui::info(&format!(
            "Colima profile '{}' still running",
            cfg.colima_profile
        ))?;

        // Show what images are still present
        let prefix_glob = format!("{}/*", cfg.local_prefix);
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
            profile = cfg.colima_profile
        ))?;
    }

    ui::info("To start fresh: cargo xtask e2e setup")?;
    ui::outro("Teardown complete")?;

    Ok(())
}

// -- Step 1: Tear down GKG stack (Tilt + ClickHouse) -------------------------

/// Stop Tilt, run `tilt down`, and remove all GKG resources from the default
/// namespace (ClickHouse, NATS, siphon, dispatch-indexing, secrets, PVCs).
fn teardown_gkg_stack(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(1, "Tearing down Tilt / GKG stack")?;

    // Kill Tilt process if running (via PID file).
    ui::info("Stopping Tilt process...")?;
    gkg_pipeline::kill_tilt(cfg);
    // Give it a moment to exit.
    std::thread::sleep(std::time::Duration::from_secs(2));

    // Run `tilt down` via mise to clean up Tilt-managed resources.
    if cmd_helpers::exists(sh, "mise") {
        ui::info("Running tilt down...")?;
        let tiltfile = cfg.gkg_root.join(c::TILTFILE_PATH);
        let tiltfile_str = tiltfile.to_string_lossy().to_string();
        let _ = cmd!(sh, "mise exec -- tilt down --file {tiltfile_str}")
            .quiet()
            .ignore_status()
            .run();
    }

    let default_ns = &cfg.default_ns;
    let ch_svc = &cfg.ch_service_name;

    // Delete Tilt-managed deployments.
    ui::info("Cleaning up default namespace resources...")?;
    let _ = cmd!(
        sh,
        "kubectl delete deployment -n {default_ns} -l app.kubernetes.io/managed-by=tilt --ignore-not-found"
    )
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

    let dispatch_job = &cfg.gkg_dispatch_job;
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

    let ch_init_cm = c::CH_INIT_CONFIGMAP;
    let _ = cmd!(
        sh,
        "kubectl delete configmap -n {default_ns} {ch_init_cm} --ignore-not-found"
    )
    .quiet()
    .ignore_status()
    .run();

    // Delete secrets created by Tilt and the xtask setup.
    let bridge_secret = c::PG_BRIDGE_SECRET_NAME;
    let ch_cred = c::CH_CREDENTIALS_SECRET;
    let gkg_cred = c::GKG_SERVER_CREDENTIALS_SECRET;
    let _ = cmd!(
        sh,
        "kubectl delete secret -n {default_ns} {bridge_secret} {ch_cred} {gkg_cred} --ignore-not-found"
    )
    .quiet()
    .ignore_status()
    .run();

    // Delete ClickHouse PVCs.
    let ch_label = format!("app={ch_svc}");
    let _ = cmd!(
        sh,
        "kubectl delete pvc -n {default_ns} -l {ch_label} --ignore-not-found"
    )
    .quiet()
    .ignore_status()
    .run();

    // Uninstall the GKG Helm release if it exists.
    let gkg_release = c::GKG_HELM_RELEASE;
    let _ = cmd!(sh, "helm uninstall {gkg_release} -n {default_ns}")
        .quiet()
        .ignore_status()
        .run();

    ui::info("GKG stack resources removed")?;
    Ok(())
}

// -- Step 2: Tear down GitLab -------------------------------------------------

fn teardown_gitlab(sh: &Shell, cfg: &Config, docker_host: &str) -> Result<()> {
    ui::step(2, "Tearing down GitLab")?;

    let ns = &cfg.gitlab_ns;

    let release = c::GITLAB_HELM_RELEASE;

    if kubectl::helm_release_exists(sh, release, ns, docker_host) {
        ui::info("Uninstalling GitLab Helm release")?;
        let timeout = c::HELM_UNINSTALL_TIMEOUT;
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

    let default_ns = &cfg.default_ns;
    let bridge_secret = c::PG_BRIDGE_SECRET_NAME;

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

fn teardown_traefik(sh: &Shell, docker_host: &str) -> Result<()> {
    ui::step(4, "Tearing down Traefik")?;

    let release = c::TRAEFIK_HELM_RELEASE;
    let kube_ns = c::KUBE_SYSTEM_NS;

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
    do_cleanup_local_artifacts(cfg, c::TEARDOWN_LOG_FILES, true)
}

/// GKG-only variant — removes only Tilt/GKG logs, leaving `.secrets` and
/// CNG-phase logs (colima-start, create-test-data, manifest) intact.
/// `.secrets` is preserved because it contains long-lived credentials
/// (JWT, PG password) that don't change across GKG stack rebuilds.
fn cleanup_local_artifacts_gkg(cfg: &Config) -> Result<()> {
    ui::step(2, "Cleaning up GKG local artifacts")?;
    do_cleanup_local_artifacts(cfg, c::GKG_TEARDOWN_LOG_FILES, false)
}

fn do_cleanup_local_artifacts(
    cfg: &Config,
    log_files: &[&str],
    remove_secrets: bool,
) -> Result<()> {
    if remove_secrets {
        let secrets_file = cfg.tilt_dir.join(c::SECRETS_FILE);
        if secrets_file.exists() {
            fs::remove_file(&secrets_file)?;
            ui::info(&format!("Removed {}", secrets_file.display()))?;
        }
    }

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
    let profile = &cfg.colima_profile;
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
