//! Docker helpers built on bollard.
//!
//! Replaces all `docker` CLI shell-outs except Helm's DOCKER_HOST usage.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use bollard::models::CreateImageInfo;
use bollard::query_parameters::{BuildImageOptionsBuilder, CreateImageOptions, ListImagesOptions};
use bollard::{API_DEFAULT_VERSION, Docker, body_try_stream};
use futures::{StreamExt, TryStreamExt};
use tokio_util::io::ReaderStream;

/// Connect to the Colima Docker socket for the given profile.
fn client(colima_profile: &str) -> Result<Docker> {
    let home = std::env::var("HOME").context("HOME not set")?;
    let socket = format!("{home}/.colima/{colima_profile}/docker.sock");
    Docker::connect_with_socket(&socket, 120, API_DEFAULT_VERSION)
        .with_context(|| format!("connecting to docker socket: {socket}"))
}

/// Check if the Docker daemon is reachable.
pub async fn is_reachable(colima_profile: &str) -> bool {
    let Ok(docker) = client(colima_profile) else {
        return false;
    };
    docker.ping().await.is_ok()
}

/// Return true if the image exists locally.
pub async fn image_exists(colima_profile: &str, image: &str) -> Result<bool> {
    let docker = client(colima_profile)?;
    match docker.inspect_image(image).await {
        Ok(_) => Ok(true),
        Err(bollard::errors::Error::DockerResponseServerError {
            status_code: 404, ..
        }) => Ok(false),
        Err(e) => Err(e).with_context(|| format!("inspecting image {image}")),
    }
}

/// Pull an image from a registry, streaming progress to the UI.
pub async fn pull_image(colima_profile: &str, image: &str) -> Result<()> {
    let docker = client(colima_profile)?;
    let (from_image, tag) = image.rsplit_once(':').unwrap_or((image, "latest"));
    let opts = CreateImageOptions {
        from_image: Some(from_image.to_string()),
        tag: Some(tag.to_string()),
        ..Default::default()
    };
    docker
        .create_image(Some(opts), None, None)
        .try_collect::<Vec<CreateImageInfo>>()
        .await
        .with_context(|| format!("pulling image {image}"))?;
    Ok(())
}

/// Build a Docker image from a build context directory.
///
/// The Dockerfile (which may live outside `context_dir`) is included in the
/// tar archive sent to the daemon. `build_args` maps ARG names to values.
pub async fn build_image(
    colima_profile: &str,
    context_dir: &Path,
    dockerfile: &Path,
    tag: &str,
    build_args: &HashMap<&str, &str>,
) -> Result<()> {
    let docker = client(colima_profile)?;

    // Write tar to a temp file so we can stream it without holding it all in RAM.
    let tar_file = tempfile::NamedTempFile::new().context("creating temp tar file")?;
    {
        let mut ar = tar::Builder::new(std::fs::File::create(tar_file.path())?);
        ar.append_dir_all(".", context_dir)
            .with_context(|| format!("tarring {}", context_dir.display()))?;
        // Include Dockerfile at a known path inside the archive.
        ar.append_path_with_name(dockerfile, "Dockerfile")
            .with_context(|| format!("adding Dockerfile {}", dockerfile.display()))?;
        ar.finish()?;
    }

    let file = tokio::fs::File::open(tar_file.path()).await?;
    let stream = ReaderStream::new(file);
    let body = body_try_stream(stream);

    let opts = BuildImageOptionsBuilder::default()
        .dockerfile("Dockerfile")
        .t(tag)
        .rm(true)
        .buildargs(build_args)
        .build();

    let mut stream = docker.build_image(opts, None, Some(body));
    while let Some(msg) = stream.next().await {
        let info = msg.with_context(|| format!("building image {tag}"))?;
        if let Some(detail) = &info.error_detail
            && let Some(err) = &detail.message
        {
            anyhow::bail!("docker build failed for {tag}: {err}");
        }
    }
    Ok(())
}

/// List local images matching a reference glob (e.g. `"myprefix/*"`).
/// Returns a list of `"repo:tag (size)"` strings for display.
pub async fn list_images(colima_profile: &str, reference: &str) -> Result<Vec<String>> {
    let docker = client(colima_profile)?;
    let opts = ListImagesOptions {
        filters: Some(HashMap::from([(
            "reference".to_string(),
            vec![reference.to_string()],
        )])),
        ..Default::default()
    };
    let images = docker
        .list_images(Some(opts))
        .await
        .context("listing docker images")?;

    let mut lines = Vec::new();
    for img in &images {
        for tag in &img.repo_tags {
            let size_mb = img.size / 1_000_000;
            lines.push(format!("  {tag}  ({size_mb}MB)"));
        }
    }
    Ok(lines)
}
