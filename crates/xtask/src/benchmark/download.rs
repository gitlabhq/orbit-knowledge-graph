use anyhow::{Context, Result};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use tokio::task::JoinSet;

const REPOS: &[(&str, &str)] = &[
    (
        "gitlab-shell",
        "https://gitlab.com/gitlab-org/gitlab-shell/-/archive/v14.47.0/gitlab-shell-v14.47.0.zip",
    ),
    (
        "gitlab",
        "https://gitlab.com/gitlab-org/gitlab/-/archive/v18.9.1-ee/gitlab-v18.9.1-ee.zip",
    ),
    (
        "gitlab-development-kit",
        "https://gitlab.com/gitlab-org/gitlab-development-kit/-/archive/v0.2.20/gitlab-development-kit-v0.2.20.zip",
    ),
];

pub async fn download_repos(output_dir: &Path) -> Result<()> {
    let temp_dir = output_dir.parent().unwrap_or(Path::new(".")).join("temp");

    std::fs::create_dir_all(output_dir).context("Failed to create output directory")?;
    std::fs::create_dir_all(&temp_dir).context("Failed to create temp directory")?;
    println!("Created output directories");

    let repos_to_download: Vec<_> = REPOS
        .iter()
        .filter(|(name, _)| {
            let should_skip = if *name == "gitlab-development-kit" {
                output_dir.exists()
                    && output_dir
                        .read_dir()
                        .map(|mut d| d.next().is_some())
                        .unwrap_or(false)
            } else {
                output_dir.join(name).exists()
            };

            if should_skip {
                println!("Repository {name} already exists, skipping");
            }

            !should_skip
        })
        .collect();

    if repos_to_download.is_empty() {
        println!("All repositories already exist, nothing to download");
        initialize_git_repos(output_dir)?;
        return Ok(());
    }

    let mut download_tasks = JoinSet::new();

    for (name, url) in repos_to_download.iter() {
        let name = name.to_string();
        let url = url.to_string();
        let zip_path = temp_dir.join(format!("{name}.zip"));

        download_tasks.spawn(async move {
            download_zip(&url, &zip_path)
                .await
                .context(format!("Failed to download {name} from {url}"))
                .map(|()| (name, zip_path))
        });
    }

    let mut downloaded = Vec::new();
    while let Some(result) = download_tasks.join_next().await {
        let (name, zip_path) = result??;
        println!("Successfully downloaded {name}");
        downloaded.push((name, zip_path));
    }

    for (name, zip_path) in downloaded {
        extract_zip(&zip_path, &temp_dir).context(format!("Failed to extract {name}"))?;

        let extracted_folder = std::fs::read_dir(&temp_dir)?
            .filter_map(|e| e.ok())
            .find(|e| {
                e.file_type().map(|ft| ft.is_dir()).unwrap_or(false)
                    && e.file_name().to_string_lossy().starts_with(&name)
            });

        let Some(extracted_folder) = extracted_folder else {
            anyhow::bail!("Could not find extracted folder for {name}");
        };

        let extracted_path = extracted_folder.path();

        if name == "gitlab-development-kit" {
            for entry in std::fs::read_dir(&extracted_path)? {
                let entry = entry?;
                let dest = output_dir.join(entry.file_name());
                std::fs::rename(entry.path(), &dest).context(format!(
                    "Failed to move GDK content {} to final location",
                    entry.file_name().to_string_lossy()
                ))?;
            }
            std::fs::remove_dir(&extracted_path)
                .context("Failed to remove empty GDK extracted folder")?;
        } else {
            let repo_path = output_dir.join(&name);
            std::fs::rename(&extracted_path, &repo_path)
                .context(format!("Failed to move {name} to final location"))?;
        }

        println!("Successfully extracted and moved {name}");
        std::fs::remove_file(&zip_path)
            .context(format!("Failed to remove ZIP file for {name}"))?;
    }

    std::fs::remove_dir_all(&temp_dir).context("Failed to remove temp directory")?;
    initialize_git_repos(output_dir)?;

    println!("All repositories downloaded, extracted, and initialized successfully");
    Ok(())
}

async fn download_zip(url: &str, dest_path: &Path) -> Result<()> {
    println!("Downloading {url} to {}", dest_path.display());

    let response = reqwest::get(url)
        .await
        .context("Failed to make HTTP request")?;

    if !response.status().is_success() {
        anyhow::bail!("HTTP request failed with status: {}", response.status());
    }

    let bytes = response
        .bytes()
        .await
        .context("Failed to read response bytes")?;

    let mut file = File::create(dest_path).context("Failed to create destination file")?;
    file.write_all(&bytes)
        .context("Failed to write to destination file")?;

    Ok(())
}

fn extract_zip(zip_path: &Path, extract_to: &Path) -> Result<()> {
    println!("Extracting {} to {}", zip_path.display(), extract_to.display());

    let file = File::open(zip_path).context("Failed to open ZIP file")?;
    let mut archive = zip::ZipArchive::new(file).context("Failed to read ZIP archive")?;

    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .context("Failed to get file from archive")?;

        let outpath = match entry.enclosed_name() {
            Some(path) => extract_to.join(path),
            None => continue,
        };

        if entry.name().ends_with('/') {
            std::fs::create_dir_all(&outpath).context("Failed to create directory")?;
        } else {
            if let Some(p) = outpath.parent()
                && !p.exists()
            {
                std::fs::create_dir_all(p).context("Failed to create parent directory")?;
            }
            let mut outfile =
                File::create(&outpath).context("Failed to create extracted file")?;
            std::io::copy(&mut entry, &mut outfile).context("Failed to copy file contents")?;
        }
    }

    Ok(())
}

fn initialize_git_repos(gdk_dir: &Path) -> Result<()> {
    println!("Initializing Git repositories for extracted repos");

    let sub_repos = ["gitlab", "gitlab-shell"]
        .iter()
        .map(|n| gdk_dir.join(n))
        .collect::<Vec<_>>();

    let all_dirs = std::iter::once(gdk_dir.to_path_buf())
        .chain(sub_repos)
        .filter(|p| p.exists() && p.read_dir().map(|mut d| d.next().is_some()).unwrap_or(false));

    for dir in all_dirs {
        init_minimal_git_repo(&dir);
    }

    Ok(())
}

fn init_minimal_git_repo(repo_path: &Path) {
    for (args, err_msg) in [
        (vec!["init"], "Failed to initialize git repository"),
        (
            vec!["config", "--local", "user.name", "test-gl-user"],
            "Failed to configure git user name",
        ),
        (
            vec!["config", "--local", "user.email", "test-gl-user@gitlab.com"],
            "Failed to configure git user email",
        ),
    ] {
        Command::new("git")
            .args(&args)
            .current_dir(repo_path)
            .output()
            .expect(err_msg);
    }
}
