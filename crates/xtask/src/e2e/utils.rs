//! Shared utility helpers used across pipeline phases.
//!
//! These operations are needed by both `cngsetup` and `gkg`, so they live
//! here to avoid duplication.

use anyhow::{Context, Result};
use xshell::{Shell, cmd};

use super::config::Config;
use super::constants as c;
use super::kubectl;
use super::ui;

// -- K8s secrets --------------------------------------------------------------

/// Create the three K8s secrets that the GKG Helm chart expects.
///
/// Reads the JWT secret from the toolbox pod and the PG password from the
/// GitLab secret, then creates `postgres-credentials`,
/// `clickhouse-credentials`, and `gkg-server-credentials` in the default
/// namespace using idempotent `--dry-run=client -o yaml | kubectl apply`.
pub fn create_k8s_secrets(sh: &Shell, cfg: &Config, toolbox_pod: &str) -> Result<()> {
    let jwt_path = &cfg.pod_paths.jwt_secret_path;
    let jwt_secret = kubectl::toolbox_exec(sh, cfg, toolbox_pod, &["cat", jwt_path])
        .context("failed to read JWT secret from toolbox pod")?;

    let pg_pass = kubectl::read_secret(
        sh,
        &cfg.namespaces.gitlab,
        &cfg.postgres.secret_name,
        &cfg.postgres.password_key,
    )?;

    let default_ns = &cfg.namespaces.default;

    // Each secret is created via --dry-run=client | apply for idempotency.
    for (name, key, value) in [
        (
            &cfg.postgres.bridge_secret_name,
            "password",
            pg_pass.as_str(),
        ),
        (
            &cfg.clickhouse.credentials_secret,
            &cfg.clickhouse.credentials_key,
            "",
        ),
        (
            &cfg.gkg.server_credentials_secret,
            "jwt-secret",
            jwt_secret.as_str(),
        ),
    ] {
        let yaml = cmd!(
            sh,
            "kubectl create secret generic {name}
                -n {default_ns}
                --from-literal={key}={value}
                --dry-run=client -o yaml"
        )
        .quiet()
        .read()?;

        cmd!(sh, "kubectl apply -f -").stdin(&yaml).quiet().run()?;
        ui::detail_item(&name.to_string())?;
    }

    Ok(())
}

// -- Test script copying ------------------------------------------------------

/// Copy all `.rb` test scripts from `e2e/tests/` into the toolbox pod.
pub fn copy_test_scripts(sh: &Shell, cfg: &Config, toolbox_pod: &str) -> Result<usize> {
    let local_dir = cfg.gkg_root.join(c::E2E_TESTS_DIR);
    kubectl::cp_files(
        sh,
        cfg,
        toolbox_pod,
        &cfg.namespaces.gitlab,
        &local_dir,
        &cfg.pod_paths.e2e_pod_dir,
        "rb",
    )
}
