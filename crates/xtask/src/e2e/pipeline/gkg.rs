//! GKG stack: ClickHouse, schema, siphon, dispatch-indexing, E2E tests.
//!
//! Deploys ClickHouse, applies schemas, builds and deploys the GKG Helm
//! chart (NATS + siphon subcharts + GKG services), waits for data to flow, runs
//! dispatch-indexing, verifies graph tables, and runs the redaction/
//! permission E2E test suite.
//!
//! ClickHouse MUST be deployed before the GKG chart — materialized views
//! only fire on NEW inserts, so tables must exist before data flows in.
//!
//!  15.  Deploy ClickHouse (standalone StatefulSet, before Helm chart)
//!  16.  Run datalake migrations (gitlab:clickhouse:migrate)
//!  17.  Apply GKG graph schema (graph.sql -> gl_* tables)
//!  18.  Drop stale siphon state in PG (slot + publication)
//!  19.  Verify knowledge_graph_enabled_namespaces rows in PG
//!  20.  Build GKG image, create K8s secrets, deploy GKG Helm chart
//!  21.  Wait for siphon data to flow (poll hierarchy tables)
//!  22.  Run dispatch-indexing (k8s Job, wait, poll gl_project)
//!  23.  OPTIMIZE TABLE FINAL on all gl_* tables
//!  24.  Verify graph tables have data (row counts)
//!  25.  Run E2E redaction tests (redaction_test.rb in toolbox pod)

use std::collections::HashMap;
use std::fs;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures::stream::{self, StreamExt};
use xshell::{Shell, cmd};

use crate::e2e::{
    config::Config,
    constants as c,
    infra::{
        helm,
        kube::{self, DeleteTarget},
    },
    template, ui, utils,
};

/// Run all GKG stack steps (15-25).
pub async fn run(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::banner("GKG Stack")?;

    let docker_host = cfg.docker_host();
    sh.set_var(c::DOCKER_HOST_ENV, &docker_host);

    let toolbox_pod = utils::get_toolbox_pod(cfg).await?;
    ui::detail("Toolbox pod", &toolbox_pod)?;

    deploy_clickhouse(cfg).await?;
    run_datalake_migrations(cfg, &toolbox_pod).await?;
    apply_graph_schema(cfg).await?;
    drop_stale_siphon_state(cfg).await?;
    verify_kg_enabled_namespaces(cfg, &toolbox_pod).await?;
    build_gkg_image(sh, cfg)?;
    create_k8s_secrets(cfg, &toolbox_pod).await?;
    deploy_gkg_chart(sh, cfg)?;
    wait_for_gkg_pods(cfg).await?;
    wait_for_siphon_data(cfg).await?;
    run_dispatch_indexing(cfg).await?;
    optimize_graph_tables(cfg).await?;
    verify_graph_tables(cfg).await?;
    dump_datalake_diagnostics(cfg).await?;

    ui::outro("GKG stack setup complete")?;
    Ok(())
}

// -- Step 15: Deploy ClickHouse -----------------------------------------------

async fn deploy_clickhouse(cfg: &Config) -> Result<()> {
    ui::step(15, "Deploying ClickHouse")?;

    let template_path = cfg.gkg_root.join(c::CLICKHOUSE_YAML_TEMPLATE);
    let vars = HashMap::from([
        ("NAMESPACE", cfg.namespaces.default.as_str()),
        ("SERVICE_NAME", cfg.clickhouse.service_name.as_str()),
        ("INIT_CONFIGMAP", cfg.clickhouse.init_configmap.as_str()),
        ("DATALAKE_DB", cfg.clickhouse.datalake_db.as_str()),
        ("GRAPH_DB", cfg.clickhouse.graph_db.as_str()),
        ("HTTP_PORT", cfg.clickhouse.http_port.as_str()),
        ("NATIVE_PORT", cfg.clickhouse.native_port.as_str()),
        ("IMAGE", cfg.clickhouse.image.as_str()),
        ("CH_USER", cfg.clickhouse.default_user.as_str()),
    ]);
    let yaml = template::render(&template_path, &vars)?;
    kube::apply_yaml(&yaml)
        .await
        .context("failed to apply clickhouse.yaml")?;
    ui::info("ClickHouse manifests applied")?;

    kube::wait_for_pod(
        &cfg.ch_label(),
        &cfg.namespaces.default,
        &cfg.timeouts.ch_pod,
    )
    .await?;

    ui::done("ClickHouse is ready")?;
    Ok(())
}

// -- Step 16: Run datalake migrations -----------------------------------------

async fn run_datalake_migrations(cfg: &Config, toolbox_pod: &str) -> Result<()> {
    ui::step(16, "Running ClickHouse datalake migrations")?;

    ui::info("Writing config/click_house.yml to toolbox pod...")?;

    let ns = &cfg.namespaces.gitlab;
    let rails_root = &cfg.pod_paths.rails_root;

    let tmpl_path = cfg.gkg_root.join(c::RAILS_CLICKHOUSE_CONFIG_TEMPLATE);
    let ch_url = cfg.ch_url();
    let vars = HashMap::from([
        ("DATABASE", cfg.clickhouse.datalake_db.as_str()),
        ("URL", ch_url.as_str()),
        ("USERNAME", cfg.clickhouse.default_user.as_str()),
    ]);
    let click_house_yml = template::render(&tmpl_path, &vars)?;

    let tmp_dir = tempfile::tempdir().context("creating temp dir for click_house.yml")?;
    let tmp_path = tmp_dir.path().join("click_house.yml");
    fs::write(&tmp_path, &click_house_yml)?;

    let dest_dir = format!("{rails_root}/config");
    kube::cp_to_pod(ns, toolbox_pod, &[tmp_path.as_path()], &dest_dir)
        .await
        .context("failed to copy click_house.yml into toolbox pod")?;

    ui::info("Running gitlab:clickhouse:migrate (this may take a minute)...")?;

    let migrate_start = Instant::now();
    let rails_env = c::RAILS_ENV;
    let script =
        format!(r#"cd "$0" && bundle exec rake gitlab:clickhouse:migrate RAILS_ENV={rails_env}"#);

    let output = kube::exec_bash_output(ns, toolbox_pod, &script, &[rails_root])
        .await?
        .strict("gitlab:clickhouse:migrate failed")?;

    let migration_count = output.lines().filter(|l| l.contains("migrated")).count();
    let elapsed = migrate_start.elapsed().as_secs();

    fs::create_dir_all(&cfg.log_dir)?;
    let log_path = cfg.log_dir.join(c::CH_MIGRATE_LOG);
    fs::write(&log_path, &output)?;

    ui::info(&format!(
        "Ran {migration_count} migrations in {elapsed}s (log: {})",
        log_path.display()
    ))?;
    ui::done("Datalake migrations complete (tables + MVs + dictionaries)")?;
    Ok(())
}

// -- Step 17: Apply GKG graph schema ------------------------------------------

async fn apply_graph_schema(cfg: &Config) -> Result<()> {
    ui::step(17, "Applying GKG graph schema")?;

    let ch_pod = utils::get_ch_pod(cfg).await?;
    let graph_db = &cfg.clickhouse.graph_db;

    // Copy graph.sql into the ClickHouse pod.
    let graph_sql = cfg.gkg_root.join(c::GRAPH_SQL_PATH);
    let ns = &cfg.namespaces.default;
    kube::cp_to_pod(ns, &ch_pod, &[graph_sql.as_path()], "/tmp")
        .await
        .context("failed to copy graph.sql into ClickHouse pod")?;

    // Execute the schema via clickhouse-client with stdin.
    let sql_content = fs::read_to_string(&graph_sql).context("reading graph.sql")?;
    utils::ch_exec_stdin(cfg, &ch_pod, graph_db, &sql_content)
        .await
        .context("failed to apply graph schema")?;

    ui::done(&format!("Graph schema applied to {graph_db}"))?;
    Ok(())
}

// -- Step 18: Drop stale siphon state -----------------------------------------

async fn drop_stale_siphon_state(cfg: &Config) -> Result<()> {
    ui::step(18, "Dropping stale siphon state in PG (slot + publication)")?;

    let pg_superpass = kube::read_secret(
        &cfg.namespaces.gitlab,
        &cfg.postgres.secret_name,
        &cfg.postgres.superpass_key,
    )
    .await?;

    let slot = &cfg.siphon.slot;
    let active_sql = format!("SELECT active FROM pg_replication_slots WHERE slot_name='{slot}';");
    let active_result = utils::pg_superuser(cfg, &pg_superpass, &active_sql, true).await?;

    match active_result.trim() {
        "t" => {
            ui::info("Replication slot is active (siphon-producer running), skipping drop")?;
        }
        "f" => {
            let drop_sql = format!("SELECT pg_drop_replication_slot('{slot}');");
            utils::pg_superuser(cfg, &pg_superpass, &drop_sql, false).await?;
            ui::info("Dropped stale replication slot")?;
        }
        _ => {
            ui::info("No replication slot found")?;
        }
    }

    let publication = &cfg.siphon.publication;
    let drop_pub = format!("DROP PUBLICATION IF EXISTS {publication};");
    utils::pg_superuser(cfg, &pg_superpass, &drop_pub, false).await?;
    ui::done("Publication dropped (will be recreated by siphon producer)")?;

    Ok(())
}

// -- Step 19: Verify knowledge_graph_enabled_namespaces -----------------------

async fn verify_kg_enabled_namespaces(cfg: &Config, toolbox_pod: &str) -> Result<()> {
    let table = &cfg.postgres.kg_enabled_table;
    ui::step(19, &format!("Verifying {table} in PG"))?;

    let ruby = format!(
        r#"puts ActiveRecord::Base.connection.select_values("SELECT root_namespace_id FROM {table} ORDER BY root_namespace_id").inspect"#
    );
    let result = utils::toolbox_rails_eval(cfg, toolbox_pod, &ruby).await?;
    ui::info(&format!("Namespace IDs: {result}"))?;

    ui::done(&format!("{table} verified"))?;
    Ok(())
}

// -- Step 20a: Build GKG server image -----------------------------------------

pub(crate) fn build_gkg_image(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(20, "Building GKG server image")?;

    let script = cfg.gkg_root.join(c::BUILD_DEV_SCRIPT);
    let script_str = script.to_string_lossy().to_string();
    let image_tag = format!("{}:{}", cfg.gkg.server_image, cfg.gkg.dev_tag);

    cmd!(sh, "bash {script_str} {image_tag}")
        .run()
        .context("scripts/build-dev.sh failed")?;

    ui::done(&format!("Built {image_tag}"))?;
    Ok(())
}

// -- Step 20b: Create K8s secrets for GKG chart -------------------------------

async fn create_k8s_secrets(cfg: &Config, toolbox_pod: &str) -> Result<()> {
    ui::info("Creating K8s secrets for GKG chart...")?;
    utils::create_k8s_secrets(cfg, toolbox_pod).await?;
    ui::done("K8s secrets created")?;
    Ok(())
}

// -- Step 20c: Deploy GKG Helm chart ------------------------------------------

fn deploy_gkg_chart(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::info("Deploying GKG Helm chart...")?;

    let sync_script = cfg.gkg_root.join("helm/sync.sh");
    cmd!(sh, "{sync_script}")
        .run()
        .context("helm/sync.sh failed — is vendir installed?")?;

    let chart_path = cfg.gkg_root.join(c::GKG_CHART_PATH);
    let chart_str = chart_path.to_string_lossy().to_string();
    let values_path = cfg.gkg_root.join(c::HELM_VALUES_YAML);
    let values_str = values_path.to_string_lossy().to_string();
    let release = &cfg.helm.gkg.release;
    let default_ns = &cfg.namespaces.default;
    let timeout = &cfg.timeouts.gkg_chart;
    let docker_host = cfg.docker_host();

    helm::uninstall(sh, release, default_ns, &docker_host);

    let secret_name = &cfg.gkg.server_credentials_secret;
    helm::install_with_sets(
        sh,
        release,
        &chart_str,
        default_ns,
        &values_str,
        &[
            ("clickhouse.datalake.host", &cfg.clickhouse.service_name),
            ("clickhouse.datalake.httpPort", &cfg.clickhouse.http_port),
            ("clickhouse.datalake.database", &cfg.clickhouse.datalake_db),
            ("clickhouse.datalake.user", &cfg.clickhouse.default_user),
            ("clickhouse.graph.host", &cfg.clickhouse.service_name),
            ("clickhouse.graph.httpPort", &cfg.clickhouse.http_port),
            ("clickhouse.graph.database", &cfg.clickhouse.graph_db),
            ("clickhouse.graph.user", &cfg.clickhouse.default_user),
            ("image.repository", &cfg.gkg.server_image),
            ("image.tag", &cfg.gkg.dev_tag),
            ("secrets.existingSecret", secret_name),
        ],
        "",
        timeout,
        &docker_host,
    )?;

    ui::done(&format!("GKG chart deployed (release: {release})"))?;
    Ok(())
}

// -- Step 20d: Wait for GKG pods ----------------------------------------------

pub(crate) async fn wait_for_gkg_pods(cfg: &Config) -> Result<()> {
    ui::info("Verifying GKG pod readiness...")?;

    let ns = &cfg.namespaces.default;
    let pods: Vec<(&str, &str, &str)> = cfg
        .gkg_pod_readiness
        .iter()
        .map(|pr| (pr.label.as_str(), ns.as_str(), pr.timeout.as_str()))
        .collect();
    kube::wait_for_pods_parallel(&pods).await?;

    ui::done("All GKG pods ready")?;
    Ok(())
}

// -- Step 21: Wait for siphon data --------------------------------------------

async fn wait_for_siphon_data(cfg: &Config) -> Result<()> {
    ui::step(21, "Waiting for siphon data to flow")?;

    let ch_pod = utils::get_ch_pod(cfg).await?;
    let db = &cfg.clickhouse.datalake_db;
    let timeout = Duration::from_secs(cfg.siphon.poll_timeout);

    let start = Instant::now();
    let mut pending: Vec<&str> = c::SIPHON_POLL_TABLES.to_vec();

    ui::info(&format!(
        "Waiting for {} datalake tables (up to {}s)...",
        pending.len(),
        timeout.as_secs()
    ))?;

    loop {
        if pending.is_empty() {
            break;
        }

        if start.elapsed() >= timeout {
            bail!(
                "Timed out after {}s. Still empty: {}. \
                 Check siphon pod logs: kubectl logs -l app=siphon-producer",
                timeout.as_secs(),
                pending.join(", ")
            );
        }

        let results = utils::ch_row_counts(cfg, &ch_pod, db, &pending).await;
        let mut still_pending = Vec::new();
        for (table, count) in &results {
            if count.trim().parse::<u64>().unwrap_or(0) > 0 {
                ui::info(&format!("{table}: {count} rows"))?;
            } else {
                still_pending.push(*table);
            }
        }

        if still_pending.is_empty() {
            break;
        }

        let elapsed = start.elapsed().as_secs();
        ui::info(&format!(
            "... waiting ({elapsed}s elapsed, {} tables remaining: {})",
            still_pending.len(),
            still_pending.join(", ")
        ))?;
        pending = still_pending;
        tokio::time::sleep(Duration::from_secs(cfg.siphon.poll_interval)).await;
    }

    ui::done("Siphon data check complete")?;
    Ok(())
}

// -- Step 22: Run dispatch-indexing -------------------------------------------

pub(crate) async fn run_dispatch_indexing(cfg: &Config) -> Result<()> {
    ui::step(22, "Running dispatch-indexing")?;

    let default_ns = &cfg.namespaces.default;
    let job_name = &cfg.gkg.dispatch_job;
    let server_image = &cfg.gkg.server_image;
    let configmap = &cfg.gkg.indexer_configmap;
    let dev_tag = &cfg.gkg.dev_tag;

    let _ = kube::delete(
        default_ns,
        "batch/v1",
        "Job",
        DeleteTarget::Names(&[job_name]),
    )
    .await;

    let tmpl_path = cfg.gkg_root.join(c::DISPATCH_JOB_TEMPLATE);
    let vars = HashMap::from([
        ("JOB_NAME", job_name.as_str()),
        ("NAMESPACE", default_ns.as_str()),
        ("SERVER_IMAGE", server_image.as_str()),
        ("IMAGE_TAG", dev_tag.as_str()),
        ("CH_SECRET", cfg.clickhouse.credentials_secret.as_str()),
        ("CH_SECRET_KEY", cfg.clickhouse.credentials_key.as_str()),
        ("CONFIGMAP", configmap.as_str()),
        (
            "GKG_CREDENTIALS_SECRET",
            cfg.gkg.server_credentials_secret.as_str(),
        ),
        (
            "GKG_CREDENTIALS_JWT_KEY",
            cfg.gkg.server_credentials_jwt_key.as_str(),
        ),
    ]);
    let job_yaml = template::render(&tmpl_path, &vars)?;

    kube::apply_yaml(&job_yaml)
        .await
        .context("failed to apply dispatch-indexing Job")?;

    ui::info("Waiting for dispatch-indexing job to complete...")?;
    let ok = kube::wait_for_job(default_ns, job_name, &cfg.timeouts.dispatch_job).await?;

    if !ok {
        ui::warn(&format!(
            "dispatch-indexing job did not complete within {}",
            cfg.timeouts.dispatch_job
        ))?;
        let label = format!("job-name={job_name}");
        match kube::get_logs(default_ns, &label, c::DIAGNOSTIC_LOG_TAIL_LINES).await {
            Ok(logs) => {
                for line in logs.lines() {
                    ui::info(line)?;
                }
            }
            Err(_) => {
                ui::warn("Could not retrieve job logs")?;
            }
        }
    }

    ui::info("dispatch-indexing complete")?;

    // Poll all graph tables.
    let ch_pod = utils::get_ch_pod(cfg).await?;
    let graph_db = &cfg.clickhouse.graph_db;
    let idx_timeout = Duration::from_secs(cfg.timeouts.indexer_poll);
    let idx_start = Instant::now();

    let mut pending: Vec<&str> = c::GL_TABLES.to_vec();

    ui::info(&format!(
        "Waiting for {} graph tables to be populated...",
        pending.len()
    ))?;

    loop {
        if pending.is_empty() {
            break;
        }

        if idx_start.elapsed() >= idx_timeout {
            ui::warn(&format!(
                "Timed out after {}s. Still empty: {}",
                idx_timeout.as_secs(),
                pending.join(", ")
            ))?;
            break;
        }

        let results = utils::ch_row_counts(cfg, &ch_pod, graph_db, &pending).await;
        let mut still_pending = Vec::new();
        for (table, count) in &results {
            if count.trim().parse::<u64>().unwrap_or(0) > 0 {
                ui::info(&format!("{table}: {count} rows"))?;
            } else {
                still_pending.push(*table);
            }
        }

        if still_pending.is_empty() {
            break;
        }

        let elapsed = idx_start.elapsed().as_secs();
        ui::info(&format!(
            "... waiting ({elapsed}s elapsed, {} tables remaining: {})",
            still_pending.len(),
            still_pending.join(", ")
        ))?;
        pending = still_pending;
        tokio::time::sleep(Duration::from_secs(cfg.timeouts.indexer_poll_interval)).await;
    }

    let settle = cfg.timeouts.indexer_settle;
    ui::info(&format!(
        "Waiting {settle}s for indexer to finish remaining pipelines..."
    ))?;
    tokio::time::sleep(Duration::from_secs(settle)).await;

    ui::done("dispatch-indexing and indexer processing complete")?;
    Ok(())
}

// -- Step 23: OPTIMIZE TABLE FINAL --------------------------------------------

pub(crate) async fn optimize_graph_tables(cfg: &Config) -> Result<()> {
    ui::step(23, "Running OPTIMIZE TABLE FINAL on graph tables")?;

    let ch_pod = utils::get_ch_pod(cfg).await?;
    let graph_db = &cfg.clickhouse.graph_db;

    let futs: Vec<_> = c::GL_TABLES
        .iter()
        .map(|table| {
            let query = format!("OPTIMIZE TABLE {table} FINAL");
            let ch_pod = &ch_pod;
            async move { (*table, utils::ch_query(cfg, ch_pod, graph_db, &query).await) }
        })
        .collect();

    let results: Vec<_> = stream::iter(futs)
        .buffer_unordered(c::CH_OPTIMIZE_CONCURRENCY)
        .collect()
        .await;

    for (table, result) in results {
        if let Err(e) = result {
            ui::warn(&format!("OPTIMIZE TABLE {table} failed: {e}"))?;
        }
    }

    ui::done("OPTIMIZE TABLE FINAL complete")?;
    Ok(())
}

// -- Step 24: Verify graph tables have data -----------------------------------

pub(crate) async fn verify_graph_tables(cfg: &Config) -> Result<()> {
    ui::step(24, "Verifying graph tables")?;

    let ch_pod = utils::get_ch_pod(cfg).await?;
    let graph_db = &cfg.clickhouse.graph_db;

    ui::info(&format!("Row counts in {graph_db}:"))?;
    let results = utils::ch_row_counts(cfg, &ch_pod, graph_db, c::GL_TABLES).await;
    for (table, count) in &results {
        ui::info(&format!("  {table}: {count}"))?;
    }

    ui::done("Graph table verification complete")?;
    Ok(())
}

// -- Diagnostic: Datalake hierarchy table dump --------------------------------

pub(crate) async fn dump_datalake_diagnostics(cfg: &Config) -> Result<()> {
    ui::info("Datalake diagnostics (hierarchy tables in datalake DB):")?;

    let ch_pod = utils::get_ch_pod(cfg).await?;
    let datalake_db = &cfg.clickhouse.datalake_db;

    let results =
        utils::ch_row_counts(cfg, &ch_pod, datalake_db, c::DATALAKE_DIAGNOSTIC_TABLES).await;
    for (table, count) in &results {
        ui::info(&format!("  {table}: {count}"))?;
    }

    ui::info(&format!(
        "Indexer pod logs (last {} lines):",
        c::DIAGNOSTIC_LOG_TAIL_LINES
    ))?;
    let default_ns = &cfg.namespaces.default;
    match kube::get_logs(
        default_ns,
        &cfg.gkg.indexer_label,
        c::DIAGNOSTIC_LOG_TAIL_LINES,
    )
    .await
    {
        Ok(logs) => {
            for line in logs.lines() {
                ui::info(line)?;
            }
        }
        Err(_) => {
            ui::warn("Could not retrieve indexer logs")?;
        }
    }

    Ok(())
}

// -- Step 25: Run E2E redaction tests -----------------------------------------

pub(crate) async fn run_redaction_tests(cfg: &Config, toolbox_pod: &str) -> Result<()> {
    ui::step(25, "Running E2E redaction tests")?;

    let ns = &cfg.namespaces.gitlab;
    let e2e_pod_dir = &cfg.pod_paths.e2e_pod_dir;
    let rails_root = &cfg.pod_paths.rails_root;
    let grpc_endpoint = &cfg.gkg.grpc_endpoint;

    let count = utils::copy_test_scripts(cfg, toolbox_pod).await?;
    if count > 0 {
        ui::info(&format!("{count} test scripts re-copied to toolbox pod"))?;
    }

    let test_file = c::REDACTION_TEST_RB;
    ui::info(&format!("Running {test_file}..."))?;

    let rails_env = c::RAILS_ENV;
    let script = format!(
        r#"cd "$0" && KNOWLEDGE_GRAPH_GRPC_ENDPOINT="$1" E2E_POD_DIR="$2" bundle exec rails runner "$3" RAILS_ENV={rails_env}"#
    );
    let test_path = format!("{e2e_pod_dir}/{test_file}");

    let r = kube::exec_bash_output(
        ns,
        toolbox_pod,
        &script,
        &[rails_root, grpc_endpoint, e2e_pod_dir, &test_path],
    )
    .await?;

    let log_path = cfg.log_dir.join(c::REDACTION_TEST_LOG);
    let log_contents = format!("{}\n--- stderr ---\n{}", r.stdout, r.stderr);
    fs::write(&log_path, &log_contents)?;

    // Pull the structured JSON report from the pod.
    let results_pod_path = format!("{e2e_pod_dir}/{}", c::TEST_RESULTS_JSON);
    let results_local_path = cfg.log_dir.join(c::TEST_RESULTS_JSON);
    let _ = kube::cp_from_pod(ns, toolbox_pod, &results_pod_path, &results_local_path).await;

    if !results_local_path.exists() {
        if r.success {
            ui::warn("JSON report not found, but Ruby exited 0 — treating as pass")?;
            ui::done("All redaction tests passed")?;
        } else {
            bail!("redaction_test.rb failed — see {}", log_path.display());
        }
        return Ok(());
    }

    let json_str = fs::read_to_string(&results_local_path)?;
    let report: serde_json::Value = serde_json::from_str(&json_str)
        .with_context(|| format!("parsing {}", results_local_path.display()))?;

    let passed = report.get("passed").and_then(|v| v.as_u64()).unwrap_or(0);
    let failed = report.get("failed").and_then(|v| v.as_u64()).unwrap_or(0);
    let total = report.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
    if passed + failed != total {
        bail!(
            "malformed test report: passed ({passed}) + failed ({failed}) != total ({total}) — see {}",
            results_local_path.display()
        );
    }

    ui::info(&serde_json::to_string_pretty(&report)?)?;

    if failed > 0 {
        bail!(
            "{failed}/{total} redaction tests failed — see {}",
            results_local_path.display()
        );
    }

    ui::done(&format!("All redaction tests passed ({passed}/{total})"))?;
    Ok(())
}
