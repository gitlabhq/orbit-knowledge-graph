//! GKG stack: ClickHouse, schema, siphon, dispatch-indexing, E2E tests.
//!
//! Deploys ClickHouse, applies schemas, builds and deploys the GKG Helm
//! chart (NATS + siphon + GKG services), waits for data to flow, runs
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
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use xshell::{Shell, cmd};

use super::super::config::Config;
use super::super::constants as c;
use super::super::kubectl;
use super::super::template;
use super::super::ui;
use super::super::utils;

/// Run all GKG stack steps (15-25).
pub fn run(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::banner("GKG Stack")?;

    let docker_host = cfg.docker_host();
    sh.set_var("DOCKER_HOST", &docker_host);

    let toolbox_pod = kubectl::get_toolbox_pod(sh, cfg)?;
    ui::detail("Toolbox pod", &toolbox_pod)?;

    deploy_clickhouse(sh, cfg)?;
    run_datalake_migrations(sh, cfg, &toolbox_pod)?;
    apply_graph_schema(sh, cfg)?;
    drop_stale_siphon_state(sh, cfg)?;
    verify_kg_enabled_namespaces(sh, cfg, &toolbox_pod)?;
    build_gkg_image(sh, cfg)?;
    create_k8s_secrets(sh, cfg, &toolbox_pod)?;
    deploy_gkg_chart(sh, cfg)?;
    wait_for_gkg_pods(sh, cfg)?;
    wait_for_siphon_data(sh, cfg)?;
    run_dispatch_indexing(sh, cfg)?;
    optimize_graph_tables(sh, cfg)?;
    verify_graph_tables(sh, cfg)?;
    dump_datalake_diagnostics(sh, cfg)?;
    run_redaction_tests(sh, cfg, &toolbox_pod)?;

    ui::outro("GKG stack setup complete")?;
    Ok(())
}

// -- Step 15: Deploy ClickHouse -----------------------------------------------

fn deploy_clickhouse(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(15, "Deploying ClickHouse")?;

    let manifest = cfg.cng_dir.join(c::CLICKHOUSE_YAML);
    let manifest_str = manifest.to_string_lossy().to_string();

    cmd!(sh, "kubectl apply -f {manifest_str}")
        .run()
        .context("failed to apply clickhouse.yaml")?;
    ui::info("ClickHouse manifests applied")?;

    kubectl::wait_for_pod(
        sh,
        &cfg.ch_label(),
        &cfg.namespaces.default,
        &cfg.timeouts.ch_pod,
    )?;

    ui::done("ClickHouse is ready")?;
    Ok(())
}

// -- Step 16: Run datalake migrations -----------------------------------------

fn run_datalake_migrations(sh: &Shell, cfg: &Config, toolbox_pod: &str) -> Result<()> {
    ui::step(16, "Running ClickHouse datalake migrations")?;

    // Write config/click_house.yml into the toolbox pod so the rake task
    // knows where ClickHouse is. Same approach as GDK.
    //
    // We write to a local temp file and `kubectl cp` it in, avoiding any
    // shell interpolation of the URL / database values.
    ui::info("Writing config/click_house.yml to toolbox pod...")?;

    let ns = &cfg.namespaces.gitlab;
    let rails_root = &cfg.pod_paths.rails_root;

    let tmpl_path = cfg.gkg_root.join(c::CLICK_HOUSE_YML_TEMPLATE);
    let ch_url = cfg.ch_url();
    let vars = HashMap::from([
        ("DATABASE", cfg.clickhouse.datalake_db.as_str()),
        ("URL", ch_url.as_str()),
        ("USERNAME", cfg.clickhouse.default_user.as_str()),
    ]);
    let click_house_yml = template::render(&tmpl_path, &vars)?;

    let tmp = tempfile::NamedTempFile::new().context("creating temp file for click_house.yml")?;
    fs::write(tmp.path(), &click_house_yml)?;

    let src = tmp.path().to_string_lossy().to_string();
    let pod_dest = format!("{ns}/{toolbox_pod}:{rails_root}/config/click_house.yml");
    cmd!(sh, "kubectl cp {src} {pod_dest}")
        .quiet()
        .run()
        .context("failed to copy click_house.yml into toolbox pod")?;

    // Run the migration rake task. Output is captured and written to a log
    // file — only a summary is shown to the user.
    ui::info("Running gitlab:clickhouse:migrate (this may take a minute)...")?;

    let migrate_start = Instant::now();
    let script = r#"cd "$0" && bundle exec rake gitlab:clickhouse:migrate RAILS_ENV=production"#;

    let output = cmd!(
        sh,
        "kubectl exec -n {ns} {toolbox_pod} -- bash -c {script} {rails_root}"
    )
    .quiet()
    .ignore_stderr()
    .read()
    .context("gitlab:clickhouse:migrate failed")?;

    // Count migrations from the output.
    let migration_count = output.lines().filter(|l| l.contains("migrated")).count();
    let elapsed = migrate_start.elapsed().as_secs();

    // Write full output to log file for debugging.
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

fn apply_graph_schema(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(17, "Applying GKG graph schema")?;

    let ch_pod = kubectl::get_ch_pod(sh, cfg)?;
    let ns = &cfg.namespaces.default;
    let graph_db = &cfg.clickhouse.graph_db;

    // Copy graph.sql into the ClickHouse pod.
    let graph_sql = cfg.gkg_root.join(c::GRAPH_SQL_PATH);
    let graph_sql_str = graph_sql.to_string_lossy().to_string();
    let dest = format!("{ns}/{ch_pod}:/tmp/graph.sql");

    cmd!(sh, "kubectl cp {graph_sql_str} {dest}")
        .quiet()
        .run()
        .context("failed to copy graph.sql into ClickHouse pod")?;

    // Execute the schema via clickhouse-client.
    let ch_user = &cfg.clickhouse.default_user;
    cmd!(
        sh,
        "kubectl exec -n {ns} {ch_pod} -i --
            clickhouse-client --user {ch_user} --database {graph_db} --multiquery"
    )
    .stdin(fs::read_to_string(&graph_sql).context("reading graph.sql")?)
    .run()
    .context("failed to apply graph schema")?;

    ui::done(&format!("Graph schema applied to {graph_db}"))?;
    Ok(())
}

// -- Step 18: Drop stale siphon state -----------------------------------------

/// Drop the replication slot and publication so siphon takes a fresh snapshot.
fn drop_stale_siphon_state(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(18, "Dropping stale siphon state in PG (slot + publication)")?;

    let pg_superpass = kubectl::read_secret(
        sh,
        &cfg.namespaces.gitlab,
        &cfg.postgres.secret_name,
        &cfg.postgres.superpass_key,
    )?;

    let slot = &cfg.siphon.slot;
    let count_sql = format!("SELECT count(*) FROM pg_replication_slots WHERE slot_name='{slot}';");
    let slot_count = kubectl::pg_superuser_query(sh, cfg, &pg_superpass, &count_sql)?;

    if slot_count.trim() == "1" {
        let drop_sql = format!("SELECT pg_drop_replication_slot('{slot}');");
        kubectl::pg_superuser_exec(sh, cfg, &pg_superpass, &drop_sql)?;
        ui::info("Dropped stale replication slot")?;
    } else {
        ui::info("No stale replication slot found")?;
    }

    let publication = &cfg.siphon.publication;
    let drop_pub = format!("DROP PUBLICATION IF EXISTS {publication};");
    kubectl::pg_superuser_exec(sh, cfg, &pg_superpass, &drop_pub)?;
    ui::done("Publication dropped (will be recreated by siphon producer)")?;

    Ok(())
}

// -- Step 19: Verify knowledge_graph_enabled_namespaces -----------------------

fn verify_kg_enabled_namespaces(sh: &Shell, cfg: &Config, toolbox_pod: &str) -> Result<()> {
    let table = &cfg.postgres.kg_enabled_table;
    ui::step(19, &format!("Verifying {table} in PG"))?;

    let ruby = format!(
        r#"puts ActiveRecord::Base.connection.select_values("SELECT root_namespace_id FROM {table} ORDER BY root_namespace_id").inspect"#
    );
    let result = kubectl::toolbox_rails_eval(sh, cfg, toolbox_pod, &ruby)?;
    ui::info(&format!("Namespace IDs: {result}"))?;

    ui::done(&format!("{table} verified"))?;
    Ok(())
}

// -- Step 20a: Build GKG server image -----------------------------------------

/// Build the `gkg-server:dev` Docker image via `scripts/build-dev.sh`.
///
/// The script handles cross-compilation (cargo-zigbuild on macOS) and
/// produces a minimal UBI container with the debug binary.
fn build_gkg_image(sh: &Shell, cfg: &Config) -> Result<()> {
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

fn create_k8s_secrets(sh: &Shell, cfg: &Config, toolbox_pod: &str) -> Result<()> {
    ui::info("Creating K8s secrets for GKG chart...")?;

    utils::create_k8s_secrets(sh, cfg, toolbox_pod)?;

    ui::done("K8s secrets created")?;
    Ok(())
}

// -- Step 20c: Deploy GKG Helm chart ------------------------------------------

/// Deploy the GKG Helm chart (NATS + siphon + GKG services).
///
/// Uses `helm install --wait` which blocks until all pods are ready.
fn deploy_gkg_chart(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::info("Deploying GKG Helm chart...")?;

    let chart_path = cfg.gkg_root.join(c::GKG_CHART_PATH);
    let chart_str = chart_path.to_string_lossy().to_string();
    let values_path = cfg.gkg_root.join(c::HELM_VALUES_YAML);
    let values_str = values_path.to_string_lossy().to_string();
    let release = &cfg.helm.gkg.release;
    let default_ns = &cfg.namespaces.default;
    let timeout = &cfg.timeouts.gkg_chart;
    let docker_host = cfg.docker_host();

    // Ensure Helm dependencies are built (NATS sub-chart).
    let _ = cmd!(sh, "helm dependency build {chart_str}")
        .env("DOCKER_HOST", &docker_host)
        .quiet()
        .ignore_status()
        .run();

    // CNG mode: override Postgres connection to point at in-cluster PG.
    let pg_host = format!("postgresql.{}.svc.cluster.local", cfg.namespaces.gitlab);
    let pg_port = "5432";
    let pg_db = &cfg.postgres.database;
    let pg_user = &cfg.postgres.user;

    // Uninstall any previous release first (idempotent).
    let _ = cmd!(sh, "helm uninstall {release} -n {default_ns}")
        .env("DOCKER_HOST", &docker_host)
        .quiet()
        .ignore_status()
        .run();

    cmd!(
        sh,
        "helm install {release} {chart_str}
            -n {default_ns}
            -f {values_str}
            --set postgres.host={pg_host}
            --set postgres.port={pg_port}
            --set postgres.database={pg_db}
            --set postgres.user={pg_user}
            --wait
            --timeout {timeout}"
    )
    .env("DOCKER_HOST", &docker_host)
    .run()
    .context("helm install of GKG chart failed")?;

    ui::done(&format!("GKG chart deployed (release: {release})"))?;
    Ok(())
}

// -- Step 20d: Wait for GKG pods ----------------------------------------------

/// Wait for critical GKG pods to be ready.
///
/// `helm install --wait` already checks that pods reach Ready, but we
/// add explicit per-label waits for better diagnostics if something is
/// slow to converge.
fn wait_for_gkg_pods(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::info("Verifying GKG pod readiness...")?;

    let default_ns = &cfg.namespaces.default;
    let labels = [
        "app.kubernetes.io/name=nats",
        "app.kubernetes.io/component=siphon-producer",
        "app.kubernetes.io/component=siphon-consumer",
        "app.kubernetes.io/component=gkg-indexer",
        "app.kubernetes.io/component=gkg-webserver",
    ];

    for label in labels {
        kubectl::wait_for_pod(sh, label, default_ns, "120s")?;
    }

    ui::done("All GKG pods ready")?;
    Ok(())
}

// -- Step 21: Wait for siphon data --------------------------------------------

/// Poll ClickHouse until siphon data appears in datalake tables.
fn wait_for_siphon_data(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(21, "Waiting for siphon data to flow")?;

    let ch_pod = kubectl::get_ch_pod(sh, cfg)?;
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
                 Check siphon pod logs: kubectl logs -l app.kubernetes.io/component=siphon-producer",
                timeout.as_secs(),
                pending.join(", ")
            );
        }

        let mut still_pending = Vec::new();
        for table in &pending {
            let query = format!("SELECT count() FROM {table}");
            let count =
                kubectl::ch_query(sh, cfg, &ch_pod, db, &query).unwrap_or_else(|_| "0".into());
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
        thread::sleep(Duration::from_secs(cfg.siphon.poll_interval));
    }

    ui::done("Siphon data check complete")?;
    Ok(())
}

// -- Step 22: Run dispatch-indexing -------------------------------------------

/// Create a k8s Job that runs gkg-server in dispatch-indexing mode.
///
/// The image is already built as `gkg-server:dev` (step 20a). After the
/// job completes, poll graph tables to confirm the indexer processed data.
fn run_dispatch_indexing(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(22, "Running dispatch-indexing")?;

    let server_image = &cfg.gkg.server_image;
    let default_ns = &cfg.namespaces.default;
    let job_name = &cfg.gkg.dispatch_job;
    let configmap = &cfg.gkg.indexer_configmap;
    let dev_tag = &cfg.gkg.dev_tag;

    // Delete previous job if it exists (jobs are immutable).
    let _ = cmd!(
        sh,
        "kubectl delete job {job_name} -n {default_ns} --ignore-not-found"
    )
    .quiet()
    .ignore_status()
    .run();

    // Render the dispatch-indexing Job manifest from template.
    let tmpl_path = cfg.gkg_root.join(c::DISPATCH_JOB_TEMPLATE);
    let vars = HashMap::from([
        ("JOB_NAME", job_name.as_str()),
        ("NAMESPACE", default_ns.as_str()),
        ("SERVER_IMAGE", server_image.as_str()),
        ("IMAGE_TAG", dev_tag.as_str()),
        ("CH_SECRET", cfg.clickhouse.credentials_secret.as_str()),
        ("CH_SECRET_KEY", cfg.clickhouse.credentials_key.as_str()),
        ("CONFIGMAP", configmap.as_str()),
    ]);
    let job_yaml = template::render(&tmpl_path, &vars)?;

    cmd!(sh, "kubectl apply -f -")
        .stdin(&job_yaml)
        .run()
        .context("failed to apply dispatch-indexing Job")?;

    // Wait for the job to complete.
    ui::info("Waiting for dispatch-indexing job to complete...")?;
    let timeout_arg = format!("--timeout={}", cfg.timeouts.dispatch_job);
    let job_ref = format!("job/{job_name}");
    let ok = cmd!(
        sh,
        "kubectl wait --for=condition=complete {job_ref} -n {default_ns} {timeout_arg}"
    )
    .quiet()
    .ignore_status()
    .ignore_stderr()
    .output()
    .map(|o| o.status.success())
    .unwrap_or(false);

    if !ok {
        ui::warn(&format!(
            "dispatch-indexing job did not complete within {}",
            cfg.timeouts.dispatch_job
        ))?;
        let _ = cmd!(sh, "kubectl logs -n {default_ns} {job_ref} --tail=20")
            .quiet()
            .ignore_status()
            .run();
    }

    ui::info("dispatch-indexing complete")?;

    // Poll all graph tables — each cycle checks every table that hasn't
    // been seen yet.
    let ch_pod = kubectl::get_ch_pod(sh, cfg)?;
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

        let mut still_pending = Vec::new();
        for table in &pending {
            let query = format!("SELECT count() FROM {table}");
            let count = kubectl::ch_query(sh, cfg, &ch_pod, graph_db, &query)
                .unwrap_or_else(|_| "0".into());
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
        thread::sleep(Duration::from_secs(cfg.timeouts.indexer_poll_interval));
    }

    // Final settle — give the indexer time to flush any in-progress writes.
    let settle = cfg.timeouts.indexer_settle;
    ui::info(&format!(
        "Waiting {settle}s for indexer to finish remaining pipelines..."
    ))?;
    thread::sleep(Duration::from_secs(settle));

    ui::done("dispatch-indexing and indexer processing complete")?;
    Ok(())
}

// -- Step 23: OPTIMIZE TABLE FINAL --------------------------------------------

/// Force ReplacingMergeTree deduplication on all graph tables.
fn optimize_graph_tables(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(23, "Running OPTIMIZE TABLE FINAL on graph tables")?;

    let ch_pod = kubectl::get_ch_pod(sh, cfg)?;
    let graph_db = &cfg.clickhouse.graph_db;

    for table in c::GL_TABLES {
        let query = format!("OPTIMIZE TABLE {table} FINAL");
        if let Err(e) = kubectl::ch_query(sh, cfg, &ch_pod, graph_db, &query) {
            ui::warn(&format!("OPTIMIZE TABLE {table} failed: {e}"))?;
        }
    }

    ui::done("OPTIMIZE TABLE FINAL complete")?;
    Ok(())
}

// -- Step 24: Verify graph tables have data -----------------------------------

fn verify_graph_tables(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(24, "Verifying graph tables")?;

    let ch_pod = kubectl::get_ch_pod(sh, cfg)?;
    let graph_db = &cfg.clickhouse.graph_db;

    ui::info(&format!("Row counts in {graph_db}:"))?;
    for table in c::GL_TABLES {
        let query = format!("SELECT count() FROM {table} FINAL");
        let count =
            kubectl::ch_query(sh, cfg, &ch_pod, graph_db, &query).unwrap_or_else(|_| "?".into());
        ui::info(&format!("  {table}: {count}"))?;
    }

    ui::done("Graph table verification complete")?;
    Ok(())
}

// -- Diagnostic: Datalake hierarchy table dump --------------------------------

/// Dump row counts from datalake hierarchy tables so we can see whether the
/// issue is upstream (siphon/MV chain) or downstream (indexer).
fn dump_datalake_diagnostics(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::info("Datalake diagnostics (hierarchy tables in datalake DB):")?;

    let ch_pod = kubectl::get_ch_pod(sh, cfg)?;
    let datalake_db = &cfg.clickhouse.datalake_db;

    let tables = [
        "hierarchy_merge_requests",
        "hierarchy_work_items",
        "siphon_merge_requests",
        "siphon_issues",
        "siphon_namespace_details",
        "siphon_namespaces",
        "project_namespace_traversal_paths",
        "namespace_traversal_paths",
        "siphon_organizations",
    ];

    for table in tables {
        let query = format!("SELECT count() FROM {table}");
        let count = kubectl::ch_query(sh, cfg, &ch_pod, datalake_db, &query)
            .unwrap_or_else(|_| "? (table may not exist)".into());
        ui::info(&format!("  {table}: {count}"))?;
    }

    // Also dump indexer logs for any error signals.
    ui::info("Indexer pod logs (last 30 lines):")?;
    let default_ns = &cfg.namespaces.default;
    let _ = cmd!(
        sh,
        "kubectl logs -n {default_ns} -l app.kubernetes.io/component=gkg-indexer --tail=30"
    )
    .quiet()
    .ignore_status()
    .run();

    Ok(())
}

// -- Step 25: Run E2E redaction tests -----------------------------------------

/// Run `redaction_test.rb` in the toolbox pod via `rails runner`.
///
/// Re-copies all `.rb` test scripts first (in case they changed during
/// iteration), then executes the test with `KNOWLEDGE_GRAPH_GRPC_ENDPOINT`
/// pointing at the GKG webserver in the default namespace. Output is
/// captured to `.dev/redaction-test.log`.
fn run_redaction_tests(sh: &Shell, cfg: &Config, toolbox_pod: &str) -> Result<()> {
    ui::step(25, "Running E2E redaction tests")?;

    let ns = &cfg.namespaces.gitlab;
    let e2e_pod_dir = &cfg.pod_paths.e2e_pod_dir;
    let rails_root = &cfg.pod_paths.rails_root;
    let grpc_endpoint = &cfg.gkg.grpc_endpoint;

    // Re-copy test scripts in case they changed during iteration.
    let count = utils::copy_test_scripts(sh, cfg, toolbox_pod)?;
    if count > 0 {
        ui::info(&format!("{count} test scripts re-copied to toolbox pod"))?;
    }

    // Run redaction_test.rb via rails runner with the gRPC endpoint.
    let test_file = c::REDACTION_TEST_RB;
    ui::info(&format!("Running {test_file}..."))?;

    let script = r#"cd "$0" && KNOWLEDGE_GRAPH_GRPC_ENDPOINT="$1" bundle exec rails runner "$2" RAILS_ENV=production"#;
    let test_path = format!("{e2e_pod_dir}/{test_file}");

    let output = cmd!(
        sh,
        "kubectl exec -n {ns} {toolbox_pod} -- bash -c {script} {rails_root} {grpc_endpoint} {test_path}"
    )
    .ignore_status()
    .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Write log.
    let log_path = cfg.log_dir.join(c::REDACTION_TEST_LOG);
    let log_contents = format!("{stdout}\n--- stderr ---\n{stderr}");
    fs::write(&log_path, &log_contents)?;

    // Print test output (it contains PASS/FAIL lines).
    for line in stdout.lines() {
        ui::info(line)?;
    }

    if output.status.success() {
        ui::done("All redaction tests passed")?;
    } else {
        ui::warn(&format!(
            "Redaction tests failed (exit {}). Check: {}",
            output.status.code().unwrap_or(-1),
            log_path.display()
        ))?;
        bail!("redaction_test.rb failed — see {}", log_path.display());
    }

    Ok(())
}
