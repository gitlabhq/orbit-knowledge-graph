//! CNG setup: post-deploy configuration.
//!
//! Runs after CNG deploy has stood up GitLab. Configures the cluster
//! for the GKG stack:
//!
//!   8.  Bridge PG credentials to default namespace (for Siphon)
//!   9.  Grant REPLICATION privilege to gitlab PG user (for Siphon WAL sender)
//!  10.  Run Rails db:migrate
//!  11.  Enable :knowledge_graph feature flag
//!  12.  Copy test scripts into toolbox pod
//!  13.  Create test data (users, groups, projects, MRs)

use std::fs;

use anyhow::Result;

use super::super::config::Config;
use super::super::constants as c;
use super::super::kube;
use super::super::ui;
use super::super::utils;

/// Run all CNG setup steps.
pub async fn run(cfg: &Config) -> Result<()> {
    ui::banner("CNG Setup: Post-deploy Configuration")?;

    fs::create_dir_all(&cfg.log_dir)?;

    let toolbox_pod = utils::get_toolbox_pod(cfg).await?;
    ui::detail("Toolbox pod", &toolbox_pod)?;

    bridge_pg_credentials(cfg).await?;
    grant_replication(cfg).await?;
    run_db_migrate(cfg, &toolbox_pod).await?;
    enable_feature_flag(cfg, &toolbox_pod).await?;
    copy_test_scripts(cfg, &toolbox_pod).await?;
    create_test_data(cfg, &toolbox_pod).await?;

    ui::outro("CNG setup complete")?;
    Ok(())
}

// -- Step 8: Bridge PG credentials --------------------------------------------

async fn bridge_pg_credentials(cfg: &Config) -> Result<()> {
    ui::step(8, "Bridging PostgreSQL credentials")?;

    let pg_pass = kube::read_secret(
        &cfg.namespaces.gitlab,
        &cfg.postgres.secret_name,
        &cfg.postgres.password_key,
    )
    .await?;
    let default_ns = &cfg.namespaces.default;
    let bridge_secret = &cfg.postgres.bridge_secret_name;

    kube::apply_secret(default_ns, bridge_secret, "password", &pg_pass).await?;

    ui::done(&format!("{bridge_secret} secret created in {default_ns}"))?;
    Ok(())
}

// -- Step 9: Grant REPLICATION privilege ---------------------------------------

async fn grant_replication(cfg: &Config) -> Result<()> {
    ui::step(
        9,
        &format!(
            "Granting REPLICATION privilege to {} PG user",
            cfg.postgres.user
        ),
    )?;

    let pg_superpass = kube::read_secret(
        &cfg.namespaces.gitlab,
        &cfg.postgres.secret_name,
        &cfg.postgres.superpass_key,
    )
    .await?;
    let sql = format!("ALTER USER {} REPLICATION;", cfg.postgres.user);

    utils::pg_superuser(cfg, &pg_superpass, &sql, false).await?;

    ui::done("REPLICATION privilege granted")?;
    Ok(())
}

// -- Step 10: Run Rails db:migrate --------------------------------------------

async fn run_db_migrate(cfg: &Config, toolbox_pod: &str) -> Result<()> {
    ui::step(10, "Running Rails db:migrate")?;

    let ns = &cfg.namespaces.gitlab;
    let rails_root = &cfg.pod_paths.rails_root;
    let script = r#"cd "$0" && bundle exec rails db:migrate RAILS_ENV=production"#;

    kube::exec_bash_output(ns, toolbox_pod, script, &[rails_root])
        .await?
        .strict("rails db:migrate failed")?;

    ui::done("Migrations complete")?;
    Ok(())
}

// -- Step 11: Enable feature flag ---------------------------------------------

async fn enable_feature_flag(cfg: &Config, toolbox_pod: &str) -> Result<()> {
    ui::step(11, "Enabling :knowledge_graph feature flag")?;

    utils::toolbox_rails_eval(cfg, toolbox_pod, "Feature.enable(:knowledge_graph)").await?;

    ui::done("Feature flag enabled")?;
    Ok(())
}

// -- Step 12: Copy test scripts -----------------------------------------------

async fn copy_test_scripts(cfg: &Config, toolbox_pod: &str) -> Result<()> {
    ui::step(12, "Copying test scripts to toolbox pod")?;

    let count = utils::copy_test_scripts(cfg, toolbox_pod).await?;
    if count == 0 {
        ui::warn("No test scripts found")?;
    } else {
        ui::done(&format!("{count} test scripts copied to toolbox pod"))?;
    }

    Ok(())
}

// -- Step 13: Create test data ------------------------------------------------

async fn create_test_data(cfg: &Config, toolbox_pod: &str) -> Result<()> {
    ui::step(13, "Creating test data")?;

    let ns = &cfg.namespaces.gitlab;
    let rails_root = &cfg.pod_paths.rails_root;
    let e2e_pod_dir = &cfg.pod_paths.e2e_pod_dir;
    let script =
        r#"cd "$0" && bundle exec rails runner "$1"/create_test_data.rb RAILS_ENV=production"#;

    let r = kube::exec_bash_output(ns, toolbox_pod, script, &[rails_root, e2e_pod_dir]).await?;

    // Write log
    let log_path = cfg.log_dir.join(c::CREATE_TEST_DATA_LOG);
    fs::write(&log_path, &r.stdout)?;

    if !r.success {
        ui::warn(&format!(
            "create_test_data.rb exited with error: {}",
            r.stderr
        ))?;
        ui::warn(&format!("Check {}", log_path.display()))?;
    }

    ui::done("Test data creation complete")?;

    // Check if manifest was written
    let manifest_pod_path = cfg.manifest_pod_path();
    let manifest_check =
        utils::toolbox_exec(cfg, toolbox_pod, &["test", "-f", &manifest_pod_path]).await;

    if manifest_check.is_ok() {
        ui::info("Manifest verified in toolbox pod")?;

        let local_path = cfg.log_dir.join(c::MANIFEST_JSON);
        let _ = kube::cp_from_pod(ns, toolbox_pod, &manifest_pod_path, &local_path).await;
        ui::info(&format!("Manifest copied to {}", local_path.display()))?;
    } else {
        ui::warn(&format!(
            "Manifest not found at {} -- create_test_data.rb may have failed",
            manifest_pod_path
        ))?;
        ui::warn(&format!("Check {}", log_path.display()))?;
    }

    Ok(())
}
