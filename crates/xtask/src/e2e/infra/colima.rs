//! Colima VM management (shell-outs — no Rust Colima library exists).

use anyhow::Result;
use xshell::{Shell, cmd};

use crate::e2e::cmd as cmd_helpers;

/// Check whether a Colima profile is currently running.
pub fn is_running(sh: &Shell, profile: &str) -> bool {
    cmd_helpers::succeeds(sh, "colima", &["status", "--profile", profile])
}

/// Start a Colima VM with k3s, the given resource limits, and VZ virtualisation.
pub fn start(
    sh: &Shell,
    profile: &str,
    memory: &str,
    cpus: &str,
    disk: &str,
    k8s_version: &str,
) -> Result<()> {
    cmd!(
        sh,
        "colima start
            --profile {profile}
            --memory {memory}
            --cpu {cpus}
            --disk {disk}
            --vm-type vz
            --kubernetes
            --kubernetes-version {k8s_version}"
    )
    .run()?;
    Ok(())
}

/// Stop and delete a Colima VM. Ignores errors (best-effort teardown).
pub fn stop_and_delete(sh: &Shell, profile: &str) {
    let _ = cmd!(sh, "colima stop --profile {profile}")
        .ignore_status()
        .run();
    let _ = cmd!(sh, "colima delete --profile {profile} --force")
        .ignore_status()
        .run();
}
