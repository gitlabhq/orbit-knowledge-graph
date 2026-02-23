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

use anyhow::{Context, Result};
use xshell::{Shell, cmd};

use super::super::config::Config;
use super::super::constants as c;
use super::super::kubectl;
use super::super::ui;
use super::super::utils;

/// Run all CNG setup steps.
pub fn run(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::banner("CNG Setup: Post-deploy Configuration")?;

    fs::create_dir_all(&cfg.log_dir)?;

    let toolbox_pod = kubectl::get_toolbox_pod(sh, cfg)?;
    ui::detail("Toolbox pod", &toolbox_pod)?;

    bridge_pg_credentials(sh, cfg)?;
    grant_replication(sh, cfg)?;
    run_db_migrate(sh, cfg, &toolbox_pod)?;
    enable_feature_flag(sh, cfg, &toolbox_pod)?;
    copy_test_scripts(sh, cfg, &toolbox_pod)?;
    create_test_data(sh, cfg, &toolbox_pod)?;

    ui::outro("CNG setup complete")?;
    Ok(())
}

// -- Step 8: Bridge PG credentials --------------------------------------------

fn bridge_pg_credentials(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(8, "Bridging PostgreSQL credentials")?;

    let pg_pass = kubectl::read_secret(
        sh,
        &cfg.namespaces.gitlab,
        &cfg.postgres.secret_name,
        &cfg.postgres.password_key,
    )?;
    let default_ns = &cfg.namespaces.default;
    let bridge_secret = &cfg.postgres.bridge_secret_name;

    // kubectl create --dry-run=client -o yaml | kubectl apply -f - is idempotent.
    let yaml = cmd!(
        sh,
        "kubectl create secret generic {bridge_secret}
            -n {default_ns}
            --from-literal=password={pg_pass}
            --dry-run=client -o yaml"
    )
    .quiet()
    .read()?;

    cmd!(sh, "kubectl apply -f -").stdin(&yaml).quiet().run()?;

    ui::done(&format!("{bridge_secret} secret created in {default_ns}"))?;
    Ok(())
}

// -- Step 9: Grant REPLICATION privilege ---------------------------------------

fn grant_replication(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(
        9,
        &format!(
            "Granting REPLICATION privilege to {} PG user",
            cfg.postgres.user
        ),
    )?;

    let pg_superpass = kubectl::read_secret(
        sh,
        &cfg.namespaces.gitlab,
        &cfg.postgres.secret_name,
        &cfg.postgres.superpass_key,
    )?;
    let sql = format!("ALTER USER {} REPLICATION;", cfg.postgres.user);

    kubectl::pg_superuser_exec(sh, cfg, &pg_superpass, &sql)?;

    ui::done("REPLICATION privilege granted")?;
    Ok(())
}

// -- Step 10: Run Rails db:migrate --------------------------------------------

fn run_db_migrate(sh: &Shell, cfg: &Config, toolbox_pod: &str) -> Result<()> {
    ui::step(10, "Running Rails db:migrate")?;

    let ns = &cfg.namespaces.gitlab;
    let rails_root = &cfg.pod_paths.rails_root;
    let script = r#"cd "$0" && bundle exec rails db:migrate RAILS_ENV=production"#.to_string();

    cmd!(
        sh,
        "kubectl exec -n {ns} {toolbox_pod} -- bash -c {script} {rails_root}"
    )
    .run()
    .context("rails db:migrate failed")?;

    ui::done("Migrations complete")?;
    Ok(())
}

// -- Step 11: Enable feature flag ---------------------------------------------

fn enable_feature_flag(sh: &Shell, cfg: &Config, toolbox_pod: &str) -> Result<()> {
    ui::step(11, "Enabling :knowledge_graph feature flag")?;

    kubectl::toolbox_rails_eval(sh, cfg, toolbox_pod, "Feature.enable(:knowledge_graph)")?;

    ui::done("Feature flag enabled")?;
    Ok(())
}

// -- Step 12: Copy test scripts -----------------------------------------------

fn copy_test_scripts(sh: &Shell, cfg: &Config, toolbox_pod: &str) -> Result<()> {
    ui::step(12, "Copying test scripts to toolbox pod")?;

    let count = utils::copy_test_scripts(sh, cfg, toolbox_pod)?;
    if count == 0 {
        ui::warn("No test scripts found")?;
    } else {
        ui::done(&format!("{count} test scripts copied to toolbox pod"))?;
    }

    Ok(())
}

// -- Step 13: Create test data ------------------------------------------------

fn create_test_data(sh: &Shell, cfg: &Config, toolbox_pod: &str) -> Result<()> {
    ui::step(13, "Creating test data")?;

    let ns = &cfg.namespaces.gitlab;
    let rails_root = &cfg.pod_paths.rails_root;
    let e2e_pod_dir = &cfg.pod_paths.e2e_pod_dir;
    let script =
        r#"cd "$0" && bundle exec rails runner "$1"/create_test_data.rb RAILS_ENV=production"#
            .to_string();

    let output = cmd!(
        sh,
        "kubectl exec -n {ns} {toolbox_pod} -- bash -c {script} {rails_root} {e2e_pod_dir}"
    )
    .ignore_status()
    .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Write log
    let log_path = cfg.log_dir.join(c::CREATE_TEST_DATA_LOG);
    fs::write(&log_path, stdout.as_ref())?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        ui::warn(&format!("create_test_data.rb exited with error: {stderr}"))?;
        ui::warn(&format!("Check {}", log_path.display()))?;
    }

    ui::done("Test data creation complete")?;

    // Check if manifest was written
    let manifest_pod_path = cfg.manifest_pod_path();
    let manifest_check =
        kubectl::toolbox_exec(sh, cfg, toolbox_pod, &["test", "-f", &manifest_pod_path]);

    if manifest_check.is_ok() {
        ui::info("Manifest verified in toolbox pod")?;

        let pod_path = format!("{ns}/{toolbox_pod}:{manifest_pod_path}");
        let local_path = cfg.log_dir.join(c::MANIFEST_JSON);
        let local_str = local_path.to_string_lossy().to_string();
        let _ = cmd!(sh, "kubectl cp {pod_path} {local_str}")
            .quiet()
            .ignore_status()
            .run();
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
