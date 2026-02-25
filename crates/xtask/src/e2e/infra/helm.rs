//! Helm CLI helpers (shell-outs — no Rust Helm library exists).

use anyhow::{Context, Result};
use xshell::{Shell, cmd};

use crate::e2e::constants as c;

/// Check whether a Helm release exists in the given namespace.
pub fn release_exists(sh: &Shell, release: &str, namespace: &str, docker_host: &str) -> bool {
    cmd!(sh, "helm status {release} -n {namespace}")
        .env(c::DOCKER_HOST_ENV, docker_host)
        .quiet()
        .ignore_status()
        .ignore_stdout()
        .ignore_stderr()
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Add a Helm repo (idempotent) and update it.
pub fn repo_add_update(
    sh: &Shell,
    repo_name: &str,
    repo_url: &str,
    docker_host: &str,
) -> Result<()> {
    let _ = cmd!(sh, "helm repo add {repo_name} {repo_url}")
        .env(c::DOCKER_HOST_ENV, docker_host)
        .quiet()
        .ignore_status()
        .run();

    cmd!(sh, "helm repo update {repo_name}")
        .env(c::DOCKER_HOST_ENV, docker_host)
        .run()
        .with_context(|| format!("helm repo update {repo_name}"))?;

    Ok(())
}

/// Install a Helm chart with `--wait`. Pass `""` for `version` to skip pinning.
#[allow(clippy::too_many_arguments)]
pub fn install(
    sh: &Shell,
    release: &str,
    chart: &str,
    namespace: &str,
    values_file: &str,
    version: &str,
    timeout: &str,
    docker_host: &str,
) -> Result<()> {
    install_with_sets(
        sh,
        release,
        chart,
        namespace,
        values_file,
        &[],
        version,
        timeout,
        docker_host,
    )
}

/// Install a Helm chart with extra `--set` flags and `--wait`.
/// Pass `""` for `version` to skip pinning.
#[allow(clippy::too_many_arguments)]
pub fn install_with_sets(
    sh: &Shell,
    release: &str,
    chart: &str,
    namespace: &str,
    values_file: &str,
    sets: &[(&str, &str)],
    version: &str,
    timeout: &str,
    docker_host: &str,
) -> Result<()> {
    let set_args: Vec<String> = sets
        .iter()
        .flat_map(|(k, v)| ["--set".to_string(), format!("{k}={v}")])
        .collect();

    let version_args: Vec<String> = if version.is_empty() {
        vec![]
    } else {
        vec!["--version".to_string(), version.to_string()]
    };

    cmd!(
        sh,
        "helm install {release} {chart}
            -n {namespace}
            -f {values_file}
            {set_args...}
            {version_args...}
            --wait
            --timeout {timeout}"
    )
    .env(c::DOCKER_HOST_ENV, docker_host)
    .run()
    .with_context(|| format!("helm install {release}"))?;

    Ok(())
}

/// Upgrade an existing Helm release with optional `--set` overrides and version pin.
#[allow(clippy::too_many_arguments)]
pub fn upgrade(
    sh: &Shell,
    release: &str,
    chart: &str,
    namespace: &str,
    values_file: &str,
    sets: &[(&str, &str)],
    version: &str,
    timeout: &str,
    docker_host: &str,
) -> Result<()> {
    let set_args: Vec<String> = sets
        .iter()
        .flat_map(|(k, v)| ["--set".to_string(), format!("{k}={v}")])
        .collect();

    let version_args: Vec<String> = if version.is_empty() {
        vec![]
    } else {
        vec!["--version".to_string(), version.to_string()]
    };

    cmd!(
        sh,
        "helm upgrade {release} {chart}
            -n {namespace}
            -f {values_file}
            {set_args...}
            {version_args...}
            --timeout {timeout}"
    )
    .env(c::DOCKER_HOST_ENV, docker_host)
    .run()
    .with_context(|| format!("helm upgrade {release}"))?;

    Ok(())
}

/// Uninstall a Helm release. Best-effort (ignores errors).
pub fn uninstall(sh: &Shell, release: &str, namespace: &str, docker_host: &str) {
    let _ = cmd!(sh, "helm uninstall {release} -n {namespace}")
        .env(c::DOCKER_HOST_ENV, docker_host)
        .quiet()
        .ignore_status()
        .run();
}

/// Uninstall a Helm release with a timeout. Best-effort (ignores errors).
pub fn uninstall_with_timeout(
    sh: &Shell,
    release: &str,
    namespace: &str,
    timeout: &str,
    docker_host: &str,
) {
    let _ = cmd!(
        sh,
        "helm uninstall {release} -n {namespace} --timeout {timeout}"
    )
    .env(c::DOCKER_HOST_ENV, docker_host)
    .ignore_status()
    .run();
}

/// Run `helm dependency build` on a chart directory. Best-effort.
pub fn dependency_build(sh: &Shell, chart_path: &str, docker_host: &str) {
    let _ = cmd!(sh, "helm dependency build {chart_path}")
        .env(c::DOCKER_HOST_ENV, docker_host)
        .quiet()
        .ignore_status()
        .run();
}
