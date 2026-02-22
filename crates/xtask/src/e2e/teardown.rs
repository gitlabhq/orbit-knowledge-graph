//! E2E environment teardown.
//!
//! CNG teardown (inverse of CNG setup):
//!   1. Uninstall GitLab Helm release, delete PVCs and namespace
//!   2. Uninstall Traefik Helm release
//!   3. Clean up local artifacts (.dev/ logs, e2e/tilt/.secrets)
//!   4. Stop and delete the Colima VM (unless --keep-colima)

use std::fs;

use anyhow::Result;
use xshell::{Shell, cmd};

use super::cmd as cmd_helpers;
use super::config::Config;
use super::ui;

/// Run the CNG teardown.
pub fn run(sh: &Shell, cfg: &Config, keep_colima: bool) -> Result<()> {
    ui::banner("E2E Teardown")?;
    ui::detail("Keep Colima", &keep_colima.to_string())?;

    let docker_host = cfg.docker_host();

    teardown_gitlab(sh, cfg, &docker_host)?;
    teardown_traefik(sh, &docker_host)?;
    cleanup_local_artifacts(cfg)?;

    if keep_colima {
        ui::step(4, "Keeping Colima VM (--keep-colima)")?;
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

    let has_release = cmd!(sh, "helm status gitlab -n {ns}")
        .env("DOCKER_HOST", docker_host)
        .quiet()
        .ignore_status()
        .ignore_stdout()
        .ignore_stderr()
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    if has_release {
        ui::info("Uninstalling GitLab Helm release")?;
        let _ = cmd!(sh, "helm uninstall gitlab -n {ns} --timeout 5m")
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

// -- Step 2: Tear down Traefik ------------------------------------------------

fn teardown_traefik(sh: &Shell, docker_host: &str) -> Result<()> {
    ui::step(2, "Tearing down Traefik")?;

    let has_release = cmd!(sh, "helm status traefik -n kube-system")
        .env("DOCKER_HOST", docker_host)
        .quiet()
        .ignore_status()
        .ignore_stdout()
        .ignore_stderr()
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    if has_release {
        ui::info("Uninstalling Traefik")?;
        let _ = cmd!(sh, "helm uninstall traefik -n kube-system")
            .env("DOCKER_HOST", docker_host)
            .ignore_status()
            .run();
        ui::info("Traefik removed")?;
    } else {
        ui::info("No Traefik release found")?;
    }

    Ok(())
}

// -- Step 3: Clean up local artifacts -----------------------------------------

fn cleanup_local_artifacts(cfg: &Config) -> Result<()> {
    ui::step(3, "Cleaning up local artifacts")?;

    let secrets_file = cfg.gkg_root.join("e2e/tilt/.secrets");
    if secrets_file.exists() {
        fs::remove_file(&secrets_file)?;
        ui::info(&format!("Removed {}", secrets_file.display()))?;
    }

    let log_dir = cfg.gkg_root.join(".dev");
    if log_dir.is_dir() {
        let log_files = [
            "create-test-data.log",
            "manifest.json",
            "colima-start.log",
            "tilt-ci.log",
            "tilt-ci.pid",
            "clickhouse-migrate.log",
            "redaction-test.log",
            "tilt-e2e.log",
        ];
        for name in &log_files {
            let path = log_dir.join(name);
            if path.exists() {
                fs::remove_file(&path)?;
            }
        }
        ui::info("Cleaned .dev/")?;
    }

    Ok(())
}

// -- Step 4: Stop and delete Colima -------------------------------------------

fn teardown_colima(sh: &Shell, cfg: &Config) -> Result<()> {
    let profile = &cfg.colima_profile;
    ui::step(4, &format!("Stopping Colima (profile: {profile})"))?;

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
