use std::io::Write;
use std::path::{Component, Path, PathBuf};

use orbit_utils::vfs::{
    Loading, Put, Source, SourceError, Tag,
    sources::{Archive, Changeset, Directory, Memory},
};

use super::schema::{Scenario, SourceKind};

pub(super) struct Input<'a> {
    pub scenario: &'a Scenario,
    pub kind: SourceKind,
    pub root: &'a Path,
}

impl Source for Input<'_> {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        let fixtures = &self.scenario.fixtures;
        match self.kind {
            SourceKind::Memory => Memory(
                fixtures
                    .iter()
                    .map(|file| (file.path.clone(), file.content.as_bytes().to_vec()))
                    .collect(),
            )
            .fill(into),
            SourceKind::Lazy => {
                for file in fixtures {
                    into.put(
                        &file.path,
                        Put::ReadAndStore {
                            size: file.content.len() as u64,
                            read: Box::new(|| Ok(file.content.as_bytes().to_vec())),
                        },
                    )?;
                }
                Ok(())
            }
            SourceKind::Directory | SourceKind::Changeset => {
                for file in fixtures {
                    if let Some(target) = &file.link {
                        let path = disk_path(self.root, &file.path);
                        std::fs::create_dir_all(path.parent().unwrap())?;
                        std::os::unix::fs::symlink(target, path)?;
                    } else {
                        write(self.root, &file.path, file.content.as_bytes());
                    }
                }
                if self.kind == SourceKind::Directory {
                    Directory(self.root).fill(into)
                } else {
                    Changeset {
                        root: self.root,
                        paths: self.scenario.changeset.clone().unwrap_or_else(|| {
                            fixtures.iter().map(|file| file.path.clone()).collect()
                        }),
                    }
                    .fill(into)
                }
            }
            SourceKind::Archive => {
                let mut builder = tar::Builder::new(Vec::new());
                for file in fixtures {
                    let mut header = tar::Header::new_gnu();
                    header.set_mode(0o644);
                    let path = format!("root/{}", file.path);
                    if let Some(target) = &file.link {
                        header.set_entry_type(tar::EntryType::Symlink);
                        header.set_size(0);
                        builder.append_link(&mut header, path, target)?;
                    } else {
                        header.set_size(file.content.len() as u64);
                        builder.append_data(&mut header, path, file.content.as_bytes())?;
                    }
                }
                let mut gzip =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
                gzip.write_all(&builder.into_inner()?)?;
                Archive(gzip.finish()?.as_slice()).fill(into)
            }
        }
    }
}

pub(super) fn disk_path(root: &Path, path: &str) -> PathBuf {
    assert!(
        !path.is_empty()
            && Path::new(path)
                .components()
                .all(|part| matches!(part, Component::Normal(_))),
        "unsafe fixture path: {path:?}"
    );
    let path = root.join(path);
    for component in path.ancestors().take_while(|parent| *parent != root) {
        if let Ok(metadata) = component.symlink_metadata() {
            assert!(
                !metadata.is_symlink(),
                "fixture path crosses a host symlink: {}",
                path.display()
            );
        }
    }
    path
}

pub(super) fn write(root: &Path, path: &str, bytes: &[u8]) {
    let path = disk_path(root, path);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}
