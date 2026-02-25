//! Standalone E2E test runner.
//!
//! Copies test scripts to the toolbox pod and runs the redaction test suite.
//! Assumes the full E2E environment is already up (`cargo xtask e2e setup --gkg`).

use anyhow::Result;

use crate::e2e::{config::Config, ui, utils};

use super::gkg;

/// Run E2E tests against the running environment.
///
/// Resolves the toolbox pod, copies test scripts, dumps datalake
/// diagnostics for context, and runs the redaction test suite (~10s).
pub async fn run(cfg: &Config) -> Result<()> {
    ui::banner("E2E Test")?;

    let toolbox_pod = utils::get_toolbox_pod(cfg).await?;
    ui::detail("Toolbox pod", &toolbox_pod)?;

    let count = utils::copy_test_scripts(cfg, &toolbox_pod).await?;
    if count > 0 {
        ui::info(&format!("{count} test scripts copied to toolbox pod"))?;
    }

    gkg::dump_datalake_diagnostics(cfg).await?;
    gkg::run_redaction_tests(cfg, &toolbox_pod).await?;

    ui::outro("E2E tests complete")?;
    Ok(())
}
