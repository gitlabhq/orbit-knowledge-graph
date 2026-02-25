//! Shared helpers used across pipeline phases.
//!
//! Domain-specific wrappers (GitLab toolbox, PostgreSQL, ClickHouse) live
//! here so that `kube.rs` stays a pure k8s primitive library.

use anyhow::{Context, Result, anyhow};

use crate::e2e::{config::Config, constants as c, infra::kube, ui};

// =============================================================================
// GitLab toolbox pod
// =============================================================================

/// Resolve the toolbox pod name in the gitlab namespace.
pub async fn get_toolbox_pod(cfg: &Config) -> Result<String> {
    let ns = &cfg.namespaces.gitlab;
    let label = &cfg.labels.toolbox;

    kube::find_pod(ns, label).await?.ok_or_else(|| {
        anyhow!(
            "No toolbox pod found in {ns} namespace.\n\
             Is GitLab deployed? Run `cargo xtask e2e setup` first."
        )
    })
}

/// Run an arbitrary command inside the toolbox pod.
pub async fn toolbox_exec(cfg: &Config, pod: &str, command: &[&str]) -> Result<String> {
    kube::pod_exec(&cfg.namespaces.gitlab, pod, command, None).await
}

/// Run a one-liner Ruby command via `rails runner` in the toolbox pod.
///
/// `rails_root` and `ruby_cmd` are passed as positional parameters to
/// `bash -c` (`$0` and `$1`) so they are never interpreted as shell syntax.
pub async fn toolbox_rails_eval(cfg: &Config, pod: &str, ruby_cmd: &str) -> Result<String> {
    let script = r#"cd "$0" && bundle exec rails runner "$1" RAILS_ENV=production"#;
    kube::exec_bash_output(
        &cfg.namespaces.gitlab,
        pod,
        script,
        &[&cfg.pod_paths.rails_root, ruby_cmd],
    )
    .await?
    .strict("rails runner")
}

// =============================================================================
// PostgreSQL (exec in PG pod)
// =============================================================================

/// Run a psql command as superuser in the PG pod.
///
/// The password and SQL are passed as positional parameters to `bash -c`
/// (`$0` and `$1`) so they are never interpreted as shell syntax.
/// When `tuples_only` is true, uses `-t` for scalar results.
pub async fn pg_superuser(
    cfg: &Config,
    pg_superpass: &str,
    sql: &str,
    tuples_only: bool,
) -> Result<String> {
    let db = &cfg.postgres.database;
    let superuser = &cfg.postgres.superuser;
    let t_flag = if tuples_only { " -t" } else { "" };
    let script = format!(r#"PGPASSWORD="$0" psql -U {superuser} -d {db}{t_flag} -c "$1""#);

    kube::exec_bash_output(
        &cfg.namespaces.gitlab,
        &cfg.postgres.pod,
        &script,
        &[pg_superpass, sql],
    )
    .await?
    .strict("psql command failed")
}

// =============================================================================
// ClickHouse (exec in CH pod)
// =============================================================================

/// Resolve the ClickHouse pod name in the default namespace.
pub async fn get_ch_pod(cfg: &Config) -> Result<String> {
    let ns = &cfg.namespaces.default;
    let label = cfg.ch_label();

    kube::find_pod(ns, &label).await?.ok_or_else(|| {
        anyhow!(
            "No ClickHouse pod found in {ns} namespace.\n\
             Has ClickHouse been deployed? (step 15)"
        )
    })
}

/// Run a clickhouse-client query and return the output.
pub async fn ch_query(cfg: &Config, ch_pod: &str, database: &str, query: &str) -> Result<String> {
    kube::pod_exec(
        &cfg.namespaces.default,
        ch_pod,
        &[
            "clickhouse-client",
            "--user",
            &cfg.clickhouse.default_user,
            "--database",
            database,
            "--query",
            query,
        ],
        None,
    )
    .await
    .context("clickhouse-client query failed")
}

/// Run a clickhouse-client command with stdin data (e.g. multi-query SQL).
pub async fn ch_exec_stdin(
    cfg: &Config,
    ch_pod: &str,
    database: &str,
    stdin: &str,
) -> Result<String> {
    kube::pod_exec(
        &cfg.namespaces.default,
        ch_pod,
        &[
            "clickhouse-client",
            "--user",
            &cfg.clickhouse.default_user,
            "--database",
            database,
            "--multiquery",
        ],
        Some(stdin.as_bytes()),
    )
    .await
    .context("clickhouse-client exec failed")
}

/// Query row counts for multiple tables concurrently. Returns a vec of
/// `(table_name, count_string)` pairs preserving input order.
pub async fn ch_row_counts<'a>(
    cfg: &Config,
    ch_pod: &str,
    database: &str,
    tables: &[&'a str],
) -> Vec<(&'a str, String)> {
    let futs: Vec<_> = tables
        .iter()
        .map(|table| async move {
            let query = format!("SELECT count() FROM {table}");
            let count = ch_query(cfg, ch_pod, database, &query)
                .await
                .unwrap_or_else(|_| "0".into());
            (*table, count)
        })
        .collect();
    futures::future::join_all(futs).await
}

// =============================================================================
// K8s secrets (shared across cngsetup + gkg)
// =============================================================================

/// Create the three K8s secrets that the GKG Helm chart expects.
///
/// Reads the JWT secret from the toolbox pod and the PG password from the
/// GitLab secret, then creates `postgres-credentials`,
/// `clickhouse-credentials`, and `gkg-server-credentials` in the default
/// namespace using idempotent apply.
pub async fn create_k8s_secrets(cfg: &Config, toolbox_pod: &str) -> Result<()> {
    let jwt_path = &cfg.pod_paths.jwt_secret_path;
    let jwt_secret = toolbox_exec(cfg, toolbox_pod, &["cat", jwt_path])
        .await
        .context("failed to read JWT secret from toolbox pod")?;

    let pg_pass = kube::read_secret(
        &cfg.namespaces.gitlab,
        &cfg.postgres.secret_name,
        &cfg.postgres.password_key,
    )
    .await?;

    let default_ns = &cfg.namespaces.default;

    let secrets: Vec<(&str, &str, &str)> = vec![
        (
            &cfg.postgres.bridge_secret_name,
            &cfg.postgres.bridge_password_key,
            pg_pass.as_str(),
        ),
        (
            &cfg.clickhouse.credentials_secret,
            &cfg.clickhouse.credentials_key,
            "",
        ),
        (
            &cfg.gkg.server_credentials_secret,
            &cfg.gkg.server_credentials_jwt_key,
            jwt_secret.as_str(),
        ),
    ];

    let futs: Vec<_> = secrets
        .iter()
        .map(|(name, key, value)| kube::apply_secret(default_ns, name, key, value))
        .collect();
    futures::future::try_join_all(futs).await?;

    for (name, _, _) in &secrets {
        ui::detail_item(name)?;
    }

    Ok(())
}

// =============================================================================
// Test script copying (shared across cngsetup + gkg)
// =============================================================================

/// Copy all `.rb` test scripts from `e2e/tests/` into the toolbox pod.
///
/// Returns the number of files copied. Returns 0 if the directory does not exist.
pub async fn copy_test_scripts(cfg: &Config, toolbox_pod: &str) -> Result<usize> {
    let local_dir = cfg.gkg_root.join(c::E2E_TESTS_DIR);
    if !local_dir.exists() {
        return Ok(0);
    }

    let mut paths = Vec::new();
    for entry in
        std::fs::read_dir(&local_dir).with_context(|| format!("reading {}", local_dir.display()))?
    {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "rb") {
            paths.push(path);
        }
    }
    if paths.is_empty() {
        return Ok(0);
    }

    let count = paths.len();
    let refs: Vec<&std::path::Path> = paths.iter().map(|p| p.as_path()).collect();
    kube::cp_to_pod(
        &cfg.namespaces.gitlab,
        toolbox_pod,
        &refs,
        &cfg.pod_paths.e2e_pod_dir,
    )
    .await?;
    Ok(count)
}
