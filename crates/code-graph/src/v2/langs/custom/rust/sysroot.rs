use std::fs;
use std::path::{Component, Path, PathBuf};

use rust_embed::RustEmbed;
use tempfile::TempDir;

use super::*;

pub(super) const EMBEDDED_RUST_SYSROOT_VERSION: &str = "1.95.0";

const EMBEDDED_RUST_PROJECT_JSON: &str = "rust-project.json";

#[derive(RustEmbed)]
#[folder = "assets/rust-sysroot-1.95.0"]
struct EmbeddedSysrootAssets;

pub(super) struct EmbeddedSysroot {
    _tempdir: TempDir,
    #[cfg(test)]
    root_path: PathBuf,
    root: AbsPathBuf,
    project_json: ProjectJson,
}

impl EmbeddedSysroot {
    pub(super) fn materialize() -> Result<Self> {
        let tempdir = tempfile::tempdir().context("failed to create embedded Rust sysroot dir")?;
        write_assets(tempdir.path())?;
        #[cfg(test)]
        let root_path = canonical_path(tempdir.path());
        let root = utf8_abs_path(tempdir.path())?;
        let project_json = load_project_json(&root)?;

        Ok(Self {
            _tempdir: tempdir,
            #[cfg(test)]
            root_path,
            root,
            project_json,
        })
    }

    pub(super) fn project_workspace_sysroot(&self) -> Result<Sysroot> {
        let mut sysroot = Sysroot::new(None, Some(self.root.clone()));
        let config = RustSourceWorkspaceConfig::Json(self.project_json.clone());
        if let Some(workspace) = sysroot.load_workspace(&config, false, &|_| ()) {
            sysroot.set_workspace(workspace);
        }
        if sysroot.is_rust_lib_src_empty() {
            bail!(
                "embedded Rust {} sysroot did not load any crates",
                EMBEDDED_RUST_SYSROOT_VERSION
            );
        }
        Ok(sysroot)
    }

    #[cfg(test)]
    pub(super) fn root_path(&self) -> &Path {
        &self.root_path
    }
}

fn write_assets(root: &Path) -> Result<()> {
    for asset_name in EmbeddedSysrootAssets::iter() {
        let relative_path = asset_name.as_ref();
        let asset = EmbeddedSysrootAssets::get(relative_path)
            .ok_or_else(|| anyhow!("missing embedded Rust sysroot asset `{relative_path}`"))?;
        let output_path = asset_output_path(root, relative_path)?;
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::write(&output_path, asset.data.as_ref())
            .with_context(|| format!("failed to write {}", output_path.display()))?;
    }
    Ok(())
}

fn asset_output_path(root: &Path, asset_path: &str) -> Result<PathBuf> {
    let mut output_path = root.to_path_buf();
    for component in Path::new(asset_path).components() {
        match component {
            Component::Normal(segment) => output_path.push(segment),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("embedded Rust sysroot asset path `{asset_path}` is invalid");
            }
        }
    }
    Ok(output_path)
}

fn load_project_json(root: &AbsPathBuf) -> Result<ProjectJson> {
    let project_json = EmbeddedSysrootAssets::get(EMBEDDED_RUST_PROJECT_JSON)
        .ok_or_else(|| anyhow!("missing embedded Rust sysroot project json"))?;
    let data: ProjectJsonData =
        serde_json::from_slice(project_json.data.as_ref()).with_context(|| {
            format!(
                "failed to parse embedded Rust {} sysroot project",
                EMBEDDED_RUST_SYSROOT_VERSION
            )
        })?;
    Ok(ProjectJson::new(None, root.as_ref(), data))
}

fn utf8_abs_path(path: &Path) -> Result<AbsPathBuf> {
    let canonical = canonical_path(path);
    let utf8 = Utf8PathBuf::from_path_buf(canonical.clone()).map_err(|_| {
        anyhow!(
            "embedded Rust sysroot path is not valid UTF-8: {}",
            canonical.display()
        )
    })?;
    Ok(AbsPathBuf::assert(utf8))
}

fn canonical_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}
