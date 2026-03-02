//! CNG setup: post-deploy configuration.
//!
//! Runs after CNG deploy has stood up GitLab. Configures the cluster
//! for the GKG stack:
//!
//!   8.  Bridge PG credentials to default namespace (for Siphon)
//!   9.  Grant REPLICATION privilege to gitlab PG user (for Siphon WAL sender)
//!  10.  Run Rails db:migrate
//!  11.  Enable :knowledge_graph feature flag
//!  12.  Configure Knowledge Graph in webservice ConfigMap
//!  13.  Copy test scripts into toolbox pod
//!  14.  Create test data (users, groups, projects, MRs)
//!  15.  Set root password

use std::fs;

use anyhow::Result;

use crate::e2e::{config::Config, constants as c, infra::kube, ui, utils};

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
    configure_knowledge_graph(cfg).await?;
    copy_test_scripts(cfg, &toolbox_pod).await?;
    create_test_data(cfg, &toolbox_pod).await?;
    set_root_password(cfg, &toolbox_pod).await?;

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

    kube::apply_secret(
        default_ns,
        bridge_secret,
        &cfg.postgres.bridge_password_key,
        &pg_pass,
    )
    .await?;

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
    let rails_env = c::RAILS_ENV;
    let script = format!(r#"cd "$0" && bundle exec rails db:migrate RAILS_ENV={rails_env}"#);

    kube::exec_bash_output(ns, toolbox_pod, &script, &[rails_root])
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

// -- Step 13: Copy test scripts -----------------------------------------------

async fn copy_test_scripts(cfg: &Config, toolbox_pod: &str) -> Result<()> {
    ui::step(13, "Copying test scripts to toolbox pod")?;

    let count = utils::copy_test_scripts(cfg, toolbox_pod).await?;
    if count == 0 {
        ui::warn("No test scripts found")?;
    } else {
        ui::done(&format!("{count} test scripts copied to toolbox pod"))?;
    }

    Ok(())
}

// -- Step 15: Set root password -----------------------------------------------

async fn set_root_password(cfg: &Config, toolbox_pod: &str) -> Result<()> {
    let password = &cfg.gitlab_ui.root_password;
    ui::step(15, "Setting root user password")?;

    let escaped_password = password.replace('\\', "\\\\").replace('\'', "\\'");
    let ruby = format!(
        r#"u = User.find_by(username: 'root'); u.password = '{escaped_password}'; u.password_confirmation = '{escaped_password}'; u.save!; puts 'ok'"#
    );
    utils::toolbox_rails_eval(cfg, toolbox_pod, &ruby).await?;

    ui::done(&format!("Root password set (root / {password})"))?;
    Ok(())
}

// -- Step 12: Configure Knowledge Graph in gitlab.yml.erb ---------------------

async fn configure_knowledge_graph(cfg: &Config) -> Result<()> {
    ui::step(12, "Configuring Knowledge Graph in webservice ConfigMap")?;

    let ns = &cfg.namespaces.gitlab;
    let cm_name = format!("{}-webservice", cfg.helm.gitlab.release);
    let key = "gitlab.yml.erb";

    let content = kube::read_configmap_field(ns, &cm_name, key).await?;

    if content.contains("knowledge_graph:") {
        ui::done("knowledge_graph already configured — skipping")?;
        return Ok(());
    }

    let grpc_endpoint = &cfg.gkg.grpc_endpoint;
    let kg_block = format!(
        "\n  knowledge_graph:\n    enabled: true\n    grpc_endpoint: \"dns:///{grpc_endpoint}\"\n"
    );

    // Insert after the `production:` line (first line matching `production:`)
    let patched = if let Some(pos) = content.find("production:") {
        let line_end = content[pos..]
            .find('\n')
            .map(|i| pos + i)
            .unwrap_or(content.len());
        let mut out = content[..line_end + 1].to_string();
        out.push_str(&kg_block);
        out.push_str(&content[line_end + 1..]);
        out
    } else {
        anyhow::bail!("could not find 'production:' in {key} — unexpected ConfigMap format");
    };

    kube::patch_configmap_field(ns, &cm_name, key, &patched).await?;

    ui::info("Restarting webservice deployment to pick up config change")?;
    let deploy_name = format!("{}-webservice-default", cfg.helm.gitlab.release);
    kube::rollout_restart(ns, &deploy_name).await?;

    kube::wait_for_pod("app=webservice", ns, "600s").await?;

    ui::done("Knowledge Graph configured in gitlab.yml.erb")?;
    Ok(())
}

// -- Step 14: Create test data ------------------------------------------------

async fn create_test_data(cfg: &Config, toolbox_pod: &str) -> Result<()> {
    ui::step(14, "Creating test data")?;

    let ns = &cfg.namespaces.gitlab;
    let rails_root = &cfg.pod_paths.rails_root;
    let e2e_pod_dir = &cfg.pod_paths.e2e_pod_dir;
    let rails_env = c::RAILS_ENV;
    let script = format!(
        r#"cd "$0" && E2E_POD_DIR="$1" bundle exec rails runner "$1"/create_test_data.rb RAILS_ENV={rails_env}"#
    );

    let r = kube::exec_bash_output(ns, toolbox_pod, &script, &[rails_root, e2e_pod_dir]).await?;

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
