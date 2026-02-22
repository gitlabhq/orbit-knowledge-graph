//! GKG stack: ClickHouse + schema setup.
//!
//! Deploys ClickHouse and applies both the datalake and graph schemas.
//! This MUST run before Tilt starts siphon — materialized views only
//! fire on NEW inserts, so tables must exist before data flows in.
//!
//!  15.  Deploy ClickHouse (standalone StatefulSet, before Tilt)
//!  16.  Run datalake migrations (gitlab:clickhouse:migrate)
//!  17.  Apply GKG graph schema (graph.sql -> gl_* tables)

use std::fs;

use anyhow::{Context, Result};
use xshell::{Shell, cmd};

use super::super::config::Config;
use super::super::constants as c;
use super::super::kubectl;
use super::super::ui;

/// Run GKG stack steps 15-17 (ClickHouse deploy + schema).
pub fn run(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::banner("GKG Stack: ClickHouse + Schema")?;

    // Ensure prerequisites from Phase 2 are in place.
    let docker_host = cfg.docker_host();
    sh.set_var("DOCKER_HOST", &docker_host);

    let toolbox_pod = kubectl::get_toolbox_pod(sh, cfg)?;
    ui::detail("Toolbox pod", &toolbox_pod)?;

    deploy_clickhouse(sh, cfg)?;
    run_datalake_migrations(sh, cfg, &toolbox_pod)?;
    apply_graph_schema(sh, cfg)?;

    ui::outro("ClickHouse + schema setup complete")?;
    Ok(())
}

// -- Step 15: Deploy ClickHouse -----------------------------------------------

fn deploy_clickhouse(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(15, "Deploying ClickHouse")?;

    let manifest = cfg.cng_dir.join("clickhouse.yaml");
    let manifest_str = manifest.to_string_lossy().to_string();

    cmd!(sh, "kubectl apply -f {manifest_str}")
        .run()
        .context("failed to apply clickhouse.yaml")?;
    ui::info("ClickHouse manifests applied")?;

    let label = format!("app={}", cfg.ch_service_name);
    kubectl::wait_for_pod(sh, &label, &cfg.default_ns, "300s")?;

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

    let ch_url = &cfg.ch_url;
    let ch_db = &cfg.ch_datalake_db;
    let ns = &cfg.gitlab_ns;
    let rails_root = &cfg.rails_root;

    let ch_user = c::CH_DEFAULT_USER;
    let click_house_yml = format!(
        "\
production:
  main:
    database: {ch_db}
    url: '{ch_url}'
    username: {ch_user}
    password:
"
    );

    let tmp = tempfile::NamedTempFile::new().context("creating temp file for click_house.yml")?;
    fs::write(tmp.path(), &click_house_yml)?;

    let src = tmp.path().to_string_lossy().to_string();
    let pod_dest = format!("{ns}/{toolbox_pod}:{rails_root}/config/click_house.yml");
    cmd!(sh, "kubectl cp {src} {pod_dest}")
        .quiet()
        .run()
        .context("failed to copy click_house.yml into toolbox pod")?;

    // Run the migration rake task.
    ui::info("Running gitlab:clickhouse:migrate...")?;

    let script = r#"cd "$0" && bundle exec rake gitlab:clickhouse:migrate RAILS_ENV=production"#;

    cmd!(
        sh,
        "kubectl exec -n {ns} {toolbox_pod} -- bash -c {script} {rails_root}"
    )
    .run()
    .context("gitlab:clickhouse:migrate failed")?;

    ui::done("Datalake migrations complete (tables + MVs + dictionaries)")?;
    Ok(())
}

// -- Step 17: Apply GKG graph schema ------------------------------------------

fn apply_graph_schema(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(17, "Applying GKG graph schema")?;

    let ch_pod = kubectl::get_ch_pod(sh, cfg)?;
    let ns = &cfg.default_ns;
    let graph_db = &cfg.ch_graph_db;

    // Copy graph.sql into the ClickHouse pod.
    let graph_sql = cfg.gkg_root.join("fixtures/schema/graph.sql");
    let graph_sql_str = graph_sql.to_string_lossy().to_string();
    let dest = format!("{ns}/{ch_pod}:/tmp/graph.sql");

    cmd!(sh, "kubectl cp {graph_sql_str} {dest}")
        .quiet()
        .run()
        .context("failed to copy graph.sql into ClickHouse pod")?;

    // Execute the schema via clickhouse-client.
    // `graph_db` is passed as a direct argument (no shell), avoiding
    // single-quote breakout from `sh -c` interpolation.
    let ch_user = c::CH_DEFAULT_USER;
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
