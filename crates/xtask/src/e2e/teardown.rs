//! E2E environment teardown.
//!
//! Full teardown (inverse of CNG deploy + CNG setup):
//!   1. Uninstall GitLab Helm release, delete PVCs and namespace
//!   2. Remove CNG setup artifacts (postgres-credentials secret)
//!   3. Uninstall Traefik Helm release
//!   4. Clean up local artifacts (.dev/ logs, e2e/tilt/.secrets)
//!   5. Stop and delete the Colima VM (unless --keep-colima)

use std::fs;

use anyhow::Result;
use xshell::{Shell, cmd};

use super::cmd as cmd_helpers;
use super::config::Config;
use super::constants as c;
use super::kubectl;
use super::ui;

/// Run the full E2E teardown (CNG deploy + CNG setup artifacts).
pub fn run(sh: &Shell, cfg: &Config, keep_colima: bool) -> Result<()> {
    ui::banner("E2E Teardown")?;
    ui::detail("Keep Colima", &keep_colima.to_string())?;

    let docker_host = cfg.docker_host();

    teardown_gitlab(sh, cfg, &docker_host)?;
    teardown_cngsetup_artifacts(sh, cfg)?;
    teardown_traefik(sh, &docker_host)?;
    cleanup_local_artifacts(cfg)?;

    if keep_colima {
        ui::step(5, "Keeping Colima VM (--keep-colima)")?;
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

// -- Step 1: Tear down GitLab -------------------------------------------------

fn teardown_gitlab(sh: &Shell, cfg: &Config, docker_host: &str) -> Result<()> {
    ui::step(1, "Tearing down GitLab")?;

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

// -- Step 2: Remove CNG setup artifacts ---------------------------------------

fn teardown_cngsetup_artifacts(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(2, "Removing CNG setup artifacts")?;

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

// -- Step 3: Tear down Traefik ------------------------------------------------

fn teardown_traefik(sh: &Shell, docker_host: &str) -> Result<()> {
    ui::step(3, "Tearing down Traefik")?;

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

// -- Step 4: Clean up local artifacts -----------------------------------------

fn cleanup_local_artifacts(cfg: &Config) -> Result<()> {
    ui::step(4, "Cleaning up local artifacts")?;

    let secrets_file = cfg.tilt_dir.join(c::SECRETS_FILE);
    if secrets_file.exists() {
        fs::remove_file(&secrets_file)?;
        ui::info(&format!("Removed {}", secrets_file.display()))?;
    }

    if cfg.log_dir.is_dir() {
        for name in c::TEARDOWN_LOG_FILES {
            let path = cfg.log_dir.join(name);
            if path.exists() {
                fs::remove_file(&path)?;
            }
        }
        ui::info("Cleaned .dev/")?;
    }

    Ok(())
}

// -- Step 5: Stop and delete Colima -------------------------------------------

fn teardown_colima(sh: &Shell, cfg: &Config) -> Result<()> {
    let profile = &cfg.colima_profile;
    ui::step(5, &format!("Stopping Colima (profile: {profile})"))?;

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
