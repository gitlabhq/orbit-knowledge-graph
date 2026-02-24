//! CNG deploy: Cluster + Cloud Native GitLab.
//!
//! Steps:
//!   1. Start Colima (k3s cluster)
//!   2. Pre-pull workhorse image
//!   3. Build custom CNG images
//!   4. Deploy Traefik ingress controller
//!   5. Deploy GitLab via Helm chart
//!   6. Wait for all GitLab pods to be ready

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use xshell::{Shell, cmd};

use crate::e2e::{cmd as cmd_helpers, colima, config::Config, constants as c, docker, kube, ui};

/// Run all CNG deploy steps.
pub async fn run(sh: &Shell, cfg: &Config, skip_build: bool) -> Result<()> {
    ui::banner("CNG Deploy: Cluster + GitLab")?;
    ui::detail("GKG root    ", &cfg.gkg_root.display().to_string())?;
    ui::detail("GitLab src  ", &cfg.gitlab_src.display().to_string())?;
    ui::detail(
        "Colima      ",
        &format!(
            "profile={} mem={}GiB cpus={}",
            cfg.colima.profile, cfg.colima.memory, cfg.colima.cpus
        ),
    )?;
    ui::detail("Skip build  ", &skip_build.to_string())?;

    validate_prerequisites(sh, cfg, skip_build)?;
    start_colima(sh, cfg).await?;
    prepull_workhorse(cfg).await?;
    if skip_build {
        ui::step(3, "Skipping CNG image build (--skip-build)")?;
    } else {
        build_images(cfg).await?;
    }
    deploy_traefik(sh, cfg)?;
    deploy_gitlab(sh, cfg).await?;
    wait_for_pods(cfg).await?;

    ui::outro("CNG deploy complete")?;
    Ok(())
}

// -- Prerequisites ------------------------------------------------------------

fn validate_prerequisites(sh: &Shell, cfg: &Config, skip_build: bool) -> Result<()> {
    if !skip_build && !cfg.gitlab_src.join("Gemfile").exists() {
        bail!(
            "GitLab source not found at {}/Gemfile\n\
             Set GITLAB_SRC to the path of your GitLab Rails checkout.",
            cfg.gitlab_src.display()
        );
    }
    for tool in ["colima", "docker", "helm"] {
        if !cmd_helpers::exists(sh, tool) {
            bail!("{tool} not found on PATH");
        }
    }
    Ok(())
}

// -- Step 1: Start Colima -----------------------------------------------------

async fn start_colima(sh: &Shell, cfg: &Config) -> Result<()> {
    let profile = &cfg.colima.profile;
    ui::step(1, &format!("Starting Colima (profile: {profile})"))?;

    if colima::is_running(sh, profile) {
        ui::info(&format!("Colima ({profile}) already running"))?;
        return Ok(());
    }

    ui::info(&format!(
        "Starting Colima with k3s, {}GiB RAM, {} CPUs",
        cfg.colima.memory, cfg.colima.cpus
    ))?;

    colima::start(
        sh,
        profile,
        &cfg.colima.memory,
        &cfg.colima.cpus,
        &cfg.colima.disk,
        &cfg.colima.k8s_version,
    )?;

    if !docker::is_reachable(profile).await {
        bail!("docker not reachable via colima profile {profile}");
    }
    if !kube::cluster_reachable().await {
        bail!("cannot reach k8s cluster");
    }
    ui::info("Docker + k8s cluster connected")?;
    Ok(())
}

// -- Step 2: Pre-pull workhorse -----------------------------------------------

async fn prepull_workhorse(cfg: &Config) -> Result<()> {
    ui::step(2, "Pre-pulling workhorse image")?;

    let image = cfg.workhorse_image();
    let profile = &cfg.colima.profile;

    if docker::image_exists(profile, &image).await? {
        ui::info("Workhorse image already present")?;
        return Ok(());
    }

    ui::info(&format!("Pulling {image}"))?;
    docker::pull_image(profile, &image).await?;
    Ok(())
}

// -- Step 3: Build CNG images -------------------------------------------------

async fn build_images(cfg: &Config) -> Result<()> {
    ui::step(3, "Building custom CNG images")?;
    ui::info(&format!("Source: {}", cfg.gitlab_src.display()))?;
    ui::info(&format!("Base tag: {}", cfg.cng.base_tag))?;

    // Stage Rails code to a temp directory (avoids GitLab's restrictive .dockerignore).
    let staging_dir = tempfile::tempdir().context("creating staging directory")?;
    let staging = staging_dir.path();

    ui::info(&format!("Staging Rails code to {}", staging.display()))?;

    for dir in &cfg.cng.staging_dirs {
        let src = cfg.gitlab_src.join(dir);
        let dst = staging.join(dir);
        if src.exists() {
            ui::detail_item(&format!("Copying {dir}/"))?;
            copy_dir_recursive(&src, &dst).with_context(|| format!("copying {dir}/"))?;
        }
    }

    // vendor/gems -> vendor_gems (avoid the large vendor/bundle/)
    let vendor_gems_src = cfg.gitlab_src.join("vendor/gems");
    if vendor_gems_src.exists() {
        ui::detail_item("Copying vendor/gems/ -> vendor_gems/")?;
        copy_dir_recursive(&vendor_gems_src, &staging.join("vendor_gems"))?;
    }

    // Gemfile + Gemfile.lock
    ui::detail_item("Copying Gemfile, Gemfile.lock")?;
    fs::copy(cfg.gitlab_src.join("Gemfile"), staging.join("Gemfile"))?;
    fs::copy(
        cfg.gitlab_src.join("Gemfile.lock"),
        staging.join("Gemfile.lock"),
    )?;

    // Create a permissive .dockerignore
    fs::write(staging.join(".dockerignore"), ".git\n")?;

    let dockerfile = cfg.cng_dir.join(c::DOCKERFILE_RAILS);
    let profile = &cfg.colima.profile;

    for component in &cfg.cng.components {
        let tag = format!(
            "{}/{}:{}",
            cfg.cng.local_prefix, component, cfg.cng.local_tag
        );
        let base_image = format!("{}/{}", cfg.cng.registry, component);

        ui::info(&format!("Building {tag}"))?;
        ui::detail_item(&format!("Base: {base_image}:{}", cfg.cng.base_tag))?;

        let build_args = HashMap::from([
            ("BASE_IMAGE", base_image.as_str()),
            ("BASE_TAG", cfg.cng.base_tag.as_str()),
        ]);
        docker::build_image(profile, staging, &dockerfile, &tag, &build_args).await?;

        ui::detail_item(&format!("Done: {tag}"))?;
    }

    ui::done("All images built")?;
    Ok(())
}

// -- Step 4: Deploy Traefik ---------------------------------------------------

fn deploy_traefik(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(4, "Deploying Traefik ingress controller")?;

    let docker_host = cfg.docker_host();

    let release = &cfg.helm.traefik.release;
    let kube_ns = &cfg.namespaces.kube_system;

    if kube::helm_release_exists(sh, release, kube_ns, &docker_host) {
        ui::info("Traefik already deployed")?;
        return Ok(());
    }

    // Add/update repo
    let repo_name = &cfg.helm.traefik.repo_name;
    let repo_url = &cfg.helm.traefik.repo_url;
    let _ = cmd!(sh, "helm repo add {repo_name} {repo_url}")
        .env("DOCKER_HOST", &docker_host)
        .quiet()
        .ignore_status()
        .run();

    cmd!(sh, "helm repo update {repo_name}")
        .env("DOCKER_HOST", &docker_host)
        .run()?;

    let values_file = cfg.cng_dir.join(c::TRAEFIK_VALUES_YAML);
    let values_str = values_file.to_string_lossy().to_string();
    let chart = &cfg.helm.traefik.chart;
    let timeout = &cfg.helm.traefik.timeout;

    cmd!(
        sh,
        "helm install {release} {chart}
            -n {kube_ns}
            -f {values_str}
            --wait
            --timeout {timeout}"
    )
    .env("DOCKER_HOST", &docker_host)
    .run()?;

    ui::info("Traefik deployed")?;
    Ok(())
}

// -- Step 5: Deploy GitLab ----------------------------------------------------

async fn deploy_gitlab(sh: &Shell, cfg: &Config) -> Result<()> {
    ui::step(5, "Deploying GitLab via Helm chart")?;

    let docker_host = cfg.docker_host();
    let ns = &cfg.namespaces.gitlab;
    let release = &cfg.helm.gitlab.release;
    let chart = &cfg.helm.gitlab.chart;
    let timeout = &cfg.helm.gitlab.timeout;

    // Add/update repo
    let repo_name = &cfg.helm.gitlab.repo_name;
    let repo_url = &cfg.helm.gitlab.repo_url;
    let _ = cmd!(sh, "helm repo add {repo_name} {repo_url}")
        .env("DOCKER_HOST", &docker_host)
        .quiet()
        .ignore_status()
        .run();

    cmd!(sh, "helm repo update {repo_name}")
        .env("DOCKER_HOST", &docker_host)
        .run()?;

    let values_file = cfg.cng_dir.join(c::GITLAB_VALUES_YAML);
    let values_str = values_file.to_string_lossy().to_string();

    if kube::helm_release_exists(sh, release, ns, &docker_host) {
        ui::info("GitLab already deployed, upgrading")?;
        cmd!(
            sh,
            "helm upgrade {release} {chart}
                -n {ns}
                -f {values_str}
                --timeout {timeout}"
        )
        .env("DOCKER_HOST", &docker_host)
        .run()?;
    } else {
        kube::create_namespace(ns).await?;

        cmd!(
            sh,
            "helm install {release} {chart}
                -n {ns}
                -f {values_str}
                --timeout {timeout}"
        )
        .env("DOCKER_HOST", &docker_host)
        .run()?;
    }

    ui::info("GitLab deploy initiated")?;
    Ok(())
}

// -- Step 6: Wait for pods ----------------------------------------------------

async fn wait_for_pods(cfg: &Config) -> Result<()> {
    ui::step(6, "Waiting for GitLab pods to be ready")?;

    let ns = &cfg.namespaces.gitlab;

    for pr in &cfg.pod_readiness {
        kube::wait_for_pod(&pr.label, ns, &pr.timeout).await?;
    }

    // Print pod status
    ui::info("Pod status")?;
    kube::print_pod_status(ns).await?;

    Ok(())
}

// -- Helpers ------------------------------------------------------------------

/// Recursively copy a directory.
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}
