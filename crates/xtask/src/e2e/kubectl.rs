//! Kubernetes / Helm helpers built on top of [`xshell`].
//!
//! Shared operations used across CNG deploy, CNG setup, teardown, and
//! the GKG stack phase.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use xshell::{Shell, cmd};

use super::cmd as cmd_helpers;
use super::config::Config;
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
    let ns = &cfg.namespaces.gitlab;
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
            &cfg.labels.toolbox,
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
    let ns = &cfg.namespaces.gitlab;
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
    let ns = &cfg.namespaces.gitlab;
    let rails_root = &cfg.pod_paths.rails_root;
    let script = r#"cd "$0" && bundle exec rails runner "$1" RAILS_ENV=production"#.to_string();

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
    let ns = &cfg.namespaces.gitlab;
    let pod = &cfg.postgres.pod;
    let db = &cfg.postgres.database;
    let superuser = &cfg.postgres.superuser;
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

/// Run a psql query as superuser and return the scalar result (no headers).
///
/// Uses `-t` (tuples-only) so the output is just the value, no column
/// headers or row-count footers. Whitespace is stripped.
///
/// The password and SQL are passed as positional parameters to `bash -c`
/// (`$0` and `$1`) so they are never interpreted as shell syntax.
pub fn pg_superuser_query(
    sh: &Shell,
    cfg: &Config,
    pg_superpass: &str,
    sql: &str,
) -> Result<String> {
    let ns = &cfg.namespaces.gitlab;
    let pod = &cfg.postgres.pod;
    let db = &cfg.postgres.database;
    let superuser = &cfg.postgres.superuser;
    let script = format!(r#"PGPASSWORD="$0" psql -U {superuser} -d {db} -t -c "$1""#);

    let output = cmd!(
        sh,
        "kubectl exec -n {ns} {pod} -- bash -c {script} {pg_superpass} {sql}"
    )
    .quiet()
    .ignore_status()
    .ignore_stderr()
    .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("psql query failed: {stderr}");
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

// -- ClickHouse ---------------------------------------------------------------

/// Resolve the ClickHouse pod name in the default namespace.
pub fn get_ch_pod(sh: &Shell, cfg: &Config) -> Result<String> {
    let ns = &cfg.namespaces.default;
    let label = cfg.ch_label();
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

/// Run a clickhouse-client query and return the output.
pub fn ch_query(
    sh: &Shell,
    cfg: &Config,
    ch_pod: &str,
    database: &str,
    query: &str,
) -> Result<String> {
    let ns = &cfg.namespaces.default;
    let ch_user = &cfg.clickhouse.default_user;
    let output = cmd!(
        sh,
        "kubectl exec -n {ns} {ch_pod} --
            clickhouse-client --user {ch_user} --database {database} --query {query}"
    )
    .quiet()
    .ignore_status()
    .ignore_stderr()
    .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("clickhouse-client query failed: {stderr}");
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

// -- File copying -------------------------------------------------------------

/// Copy local files matching `extension` from `local_dir` into a pod.
///
/// Creates `pod_dir` inside the pod first (`mkdir -p`).  Returns the number
/// of files copied.  If `local_dir` does not exist, returns 0.
pub fn cp_files(
    sh: &Shell,
    cfg: &Config,
    pod: &str,
    namespace: &str,
    local_dir: &Path,
    pod_dir: &str,
    extension: &str,
) -> Result<usize> {
    toolbox_exec(sh, cfg, pod, &["mkdir", "-p", pod_dir])?;

    if !local_dir.exists() {
        return Ok(0);
    }

    let mut count = 0;
    for entry in fs::read_dir(local_dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == extension) {
            let filename = path.file_name().unwrap().to_string_lossy();
            let src = path.to_string_lossy().to_string();
            let dest = format!("{namespace}/{pod}:{pod_dir}/{filename}");
            cmd!(sh, "kubectl cp {src} {dest}").quiet().run()?;
            count += 1;
        }
    }

    Ok(count)
}
