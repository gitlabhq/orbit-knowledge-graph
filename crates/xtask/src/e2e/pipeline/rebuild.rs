//! GKG image rebuild + rollout restart.
//!
//! Rebuilds the GKG server image, triggers a rolling restart of all GKG
//! deployments, and waits for pods to become ready.
//!
//! With `--rails`, also rebuilds the CNG images from the current GITLAB_SRC
//! checkout and does a `helm upgrade` of GitLab before the GKG rebuild.
//! This handles Rails monolith changes without tearing down the environment.
//!
//! Migrations, test data, and siphon state persist across restarts — only the
//! binaries change. Typical cycle: ~2-3min (GKG only), ~5-8min (with Rails).

use anyhow::Result;
use xshell::Shell;

use crate::e2e::{config::Config, constants as c, infra::kube, ui};

use super::{cng, gkg};

/// Rebuild images, restart deployments, and wait for readiness.
///
/// When `rails` is true, rebuilds CNG images from GITLAB_SRC and does a
/// helm upgrade of GitLab.
/// When `gkg` is true, rebuilds the GKG server image and rollout restarts
/// all GKG deployments.
pub async fn run(sh: &Shell, cfg: &Config, gkg: bool, rails: bool) -> Result<()> {
    let label = match (rails, gkg) {
        (true, true) => "Rails + GKG Rebuild",
        (true, false) => "Rails Rebuild",
        (false, true) => "GKG Rebuild",
        (false, false) => unreachable!("caller validates at least one flag"),
    };
    ui::banner(label)?;

    if rails {
        cng::build_images(cfg).await?;
        cng::deploy_gitlab(sh, cfg).await?;
        cng::wait_for_pods(cfg).await?;
    }

    if gkg {
        gkg::build_gkg_image(sh, cfg)?;

        let ns = &cfg.namespaces.default;
        let futs: Vec<_> = c::GKG_DEPLOYMENTS
            .iter()
            .map(|d| kube::rollout_restart(ns, d))
            .collect();
        futures::future::try_join_all(futs).await?;

        gkg::wait_for_gkg_pods(cfg).await?;
    }

    ui::outro("Rebuild complete")?;
    Ok(())
}
