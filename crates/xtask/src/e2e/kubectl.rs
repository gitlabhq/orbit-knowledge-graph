//! Kubernetes / Helm helpers built on top of [`xshell`].
//!
//! Shared operations used across CNG deploy, CNG setup, teardown, and
//! (eventually) the GKG stack phase.

use anyhow::{Context, Result, bail};
use xshell::{Shell, cmd};

use super::cmd as cmd_helpers;
use super::config::Config;
use super::constants as c;
use super::ui;

// -- Helm ---------------------------------------------------------------------

/// Check whether a Helm release exists in the given namespace.
pub fn helm_release_exists(sh: &Shell, release: &str, namespace: &str, docker_host: &str) -> bool {
    cmd!(sh, "helm status {release} -n {namespace}")
        .env("DOCKER_HOST", docker_host)
        .quiet()
        .ignore_status()
        .ignore_stdout()
        .ignore_stderr()
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

// -- Pod readiness ------------------------------------------------------------

/// Block until a pod matching `label` in `namespace` is ready, or warn on timeout.
pub fn wait_for_pod(sh: &Shell, label: &str, namespace: &str, timeout: &str) -> Result<()> {
    ui::info(&format!(
        "Waiting for pod ({label}) in {namespace} (timeout {timeout})"
    ))?;
    let timeout_arg = format!("--timeout={timeout}");
    let ok = cmd!(
        sh,
        "kubectl wait --for=condition=ready pod
            -l {label}
            -n {namespace}
            {timeout_arg}"
    )
    .quiet()
    .ignore_status()
    .ignore_stdout()
    .ignore_stderr()
    .output()
    .map(|o| o.status.success())
    .unwrap_or(false);

    if !ok {
        ui::warn(&format!(
            "Pod {label} not ready after {timeout}. Continuing..."
        ))?;
    }
    Ok(())
}

// -- Toolbox pod --------------------------------------------------------------

/// Resolve the toolbox pod name in the gitlab namespace.
pub fn get_toolbox_pod(sh: &Shell, cfg: &Config) -> Result<String> {
    let ns = &cfg.gitlab_ns;
    let jsonpath = "{.items[0].metadata.name}";
    let pod = cmd_helpers::capture(
        sh,
        "kubectl",
        &[
            "get",
            "pod",
            "-n",
            ns,
            "-l",
            c::TOOLBOX_LABEL,
            "-o",
            &format!("jsonpath={jsonpath}"),
        ],
    );

    match pod {
        Some(name) if !name.is_empty() => Ok(name),
        _ => bail!(
            "No toolbox pod found in {ns} namespace.\n\
             Is GitLab deployed? Run `cargo xtask e2e setup` first."
        ),
    }
}

/// Run an arbitrary command inside the toolbox pod.
pub fn toolbox_exec(sh: &Shell, cfg: &Config, pod: &str, command: &[&str]) -> Result<String> {
    let ns = &cfg.gitlab_ns;
    let output = cmd!(sh, "kubectl exec -n {ns} {pod} --")
        .args(command)
        .quiet()
        .ignore_status()
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("toolbox exec failed: {stderr}");
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Run a one-liner Ruby command via `rails runner` in the toolbox pod.
///
/// `rails_root` and `ruby_cmd` are passed as positional parameters to
/// `bash -c` (`$0` and `$1`) so they are never interpreted as shell syntax.
pub fn toolbox_rails_eval(sh: &Shell, cfg: &Config, pod: &str, ruby_cmd: &str) -> Result<String> {
    let ns = &cfg.gitlab_ns;
    let rails_root = &cfg.rails_root;
    let script = r#"cd "$0" && bundle exec rails runner "$1" RAILS_ENV=production"#;

    let output = cmd!(
        sh,
        "kubectl exec -n {ns} {pod} -- bash -c {script} {rails_root} {ruby_cmd}"
    )
    .quiet()
    .ignore_status()
    .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("rails runner failed: {stderr}");
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

// -- Secrets ------------------------------------------------------------------

/// Read a k8s secret field (base64-decoded).
pub fn read_secret(sh: &Shell, namespace: &str, secret_name: &str, key: &str) -> Result<String> {
    let jsonpath = format!("{{.data.{key}}}");
    let encoded = cmd_helpers::capture(
        sh,
        "kubectl",
        &[
            "get",
            "secret",
            "-n",
            namespace,
            secret_name,
            "-o",
            &format!("jsonpath={jsonpath}"),
        ],
    )
    .with_context(|| format!("reading secret {secret_name}/{key} in {namespace}"))?;

    // base64 decode
    let decoded = cmd!(sh, "base64 -d")
        .stdin(&encoded)
        .quiet()
        .ignore_stderr()
        .read()
        .with_context(|| format!("base64-decoding secret {secret_name}/{key}"))?;

    Ok(decoded.trim().to_string())
}

// -- PostgreSQL ---------------------------------------------------------------

/// Run a psql command as superuser in the PG pod.
///
/// The password and SQL are passed as positional parameters to `bash -c`
/// (`$0` and `$1`) so they are never interpreted as shell syntax.
pub fn pg_superuser_exec(
    sh: &Shell,
    cfg: &Config,
    pg_superpass: &str,
    sql: &str,
) -> Result<String> {
    let ns = &cfg.gitlab_ns;
    let pod = &cfg.pg_pod;
    let db = &cfg.pg_database;
    let superuser = c::PG_SUPERUSER;
    let script = format!(r#"PGPASSWORD="$0" psql -U {superuser} -d {db} -c "$1""#);

    let output = cmd!(
        sh,
        "kubectl exec -n {ns} {pod} -- bash -c {script} {pg_superpass} {sql}"
    )
    .quiet()
    .ignore_status()
    .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("psql command failed: {stderr}");
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

// -- ClickHouse ---------------------------------------------------------------

/// Resolve the ClickHouse pod name in the default namespace.
pub fn get_ch_pod(sh: &Shell, cfg: &Config) -> Result<String> {
    let ns = &cfg.default_ns;
    let label = format!("app={}", cfg.ch_service_name);
    let jsonpath = "{.items[0].metadata.name}";
    let pod = cmd_helpers::capture(
        sh,
        "kubectl",
        &[
            "get",
            "pod",
            "-n",
            ns,
            "-l",
            &label,
            "-o",
            &format!("jsonpath={jsonpath}"),
        ],
    );

    match pod {
        Some(name) if !name.is_empty() => Ok(name),
        _ => bail!(
            "No ClickHouse pod found in {ns} namespace.\n\
             Has ClickHouse been deployed? (step 15)"
        ),
    }
}
