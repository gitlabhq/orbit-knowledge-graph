use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use orbit_utils::vfs::{
    Loading, Put, Source, SourceError, Tag,
    sources::{Archive, Changed, Checkout, Memory},
};

use super::schema::{Scenario, SourceKind};

pub(super) fn validate(scenario: &Scenario, kind: SourceKind) {
    assert!(
        scenario.changed.is_none() || kind == SourceKind::Changed,
        "changed requires Changed"
    );
    assert!(
        scenario.truncate_archive.is_none() || kind == SourceKind::Archive,
        "truncation requires Archive"
    );
    for entry in &scenario.entries {
        assert!(
            entry.link.is_none() || entry.hardlink.is_none(),
            "entry cannot be both link types"
        );
        assert!(
            entry.read_error.is_none() || kind == SourceKind::Lazy,
            "read_error requires Lazy"
        );
        assert!(
            entry.declared_size.is_none() || matches!(kind, SourceKind::Lazy | SourceKind::Archive),
            "declared_size requires Lazy or Archive"
        );
        assert!(
            kind == SourceKind::Archive
                || (entry.hardlink.is_none()
                    && entry.archive_path.is_none()
                    && entry.raw_type.is_none()
                    && entry.pax_size.is_none()),
            "archive fields require Archive"
        );
        assert!(
            entry.archive_path.is_none() || entry.raw_type.is_none(),
            "raw_type and archive_path cannot be combined"
        );
        assert!(
            (entry.link.is_none() && entry.hardlink.is_none())
                || (entry.data.bytes().is_empty()
                    && entry.declared_size.is_none()
                    && entry.pax_size.is_none()
                    && entry.read_error.is_none()),
            "links cannot carry content"
        );
    }
}

pub(super) struct Input<'a> {
    pub scenario: &'a Scenario,
    pub kind: SourceKind,
    pub root: &'a Path,
    pub reads: &'a AtomicUsize,
}

impl Source for Input<'_> {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        match self.kind {
            SourceKind::Memory => {
                assert!(
                    self.scenario
                        .entries
                        .iter()
                        .all(|entry| entry.link.is_none() && entry.hardlink.is_none()),
                    "Memory holds only regular files"
                );
                Memory(
                    self.scenario
                        .entries
                        .iter()
                        .map(|entry| (entry.path.clone(), entry.data.bytes()))
                        .collect(),
                )
                .fill(into)
            }
            SourceKind::Puts | SourceKind::Lazy => {
                for entry in &self.scenario.entries {
                    let input = if let Some(target) = &entry.link {
                        Put::Symlink(target.clone())
                    } else if self.kind == SourceKind::Lazy {
                        Put::Lazy {
                            size: entry
                                .declared_size
                                .unwrap_or_else(|| entry.data.bytes().len() as u64),
                            read: Box::new(|| {
                                self.reads.fetch_add(1, SeqCst);
                                if let Some(error) = &entry.read_error {
                                    return Err(io::Error::other(error.clone()));
                                }
                                Ok(entry.data.bytes())
                            }),
                        }
                    } else {
                        Put::Bytes(entry.data.bytes())
                    };
                    into.put(&entry.path, input)?;
                }
                Ok(())
            }
            SourceKind::Checkout | SourceKind::Changed => {
                for entry in &self.scenario.entries {
                    if let Some(target) = &entry.link {
                        let path = disk_path(self.root, &entry.path);
                        std::fs::create_dir_all(path.parent().unwrap())?;
                        std::os::unix::fs::symlink(target, path)?;
                    } else {
                        write(self.root, &entry.path, &entry.data.bytes());
                    }
                }
                if self.kind == SourceKind::Checkout {
                    Checkout(self.root).fill(into)
                } else {
                    Changed {
                        root: self.root,
                        paths: self.scenario.changed.clone().unwrap_or_else(|| {
                            self.scenario
                                .entries
                                .iter()
                                .map(|entry| entry.path.clone())
                                .collect()
                        }),
                    }
                    .fill(into)
                }
            }
            SourceKind::Archive => {
                let data = archive(self.scenario);
                Archive(data.as_slice()).fill(into)
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
    for parent in path
        .ancestors()
        .take_while(|parent| *parent != root)
        .skip(1)
    {
        if let Ok(metadata) = parent.symlink_metadata() {
            assert!(
                !metadata.is_symlink(),
                "fixture path crosses a host symlink: {}",
                path.display()
            );
        }
    }
    if let Ok(metadata) = path.symlink_metadata() {
        assert!(
            !metadata.is_symlink(),
            "fixture cannot overwrite a symlink: {}",
            path.display()
        );
    }
    path
}

pub(super) fn write(root: &Path, path: &str, bytes: &[u8]) {
    let path = disk_path(root, path);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

fn archive(scenario: &Scenario) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for entry in &scenario.entries {
        let path = entry
            .archive_path
            .clone()
            .unwrap_or_else(|| format!("root/{}", entry.path));
        if let Some(size) = entry.pax_size {
            builder
                .append_pax_extensions([("size", size.to_string().as_bytes())])
                .unwrap();
        }
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        let bytes = entry.data.bytes();
        header.set_size(entry.declared_size.unwrap_or(bytes.len() as u64));
        if let Some(target) = entry.link.as_ref().or(entry.hardlink.as_ref()) {
            header.set_entry_type(if entry.link.is_some() {
                tar::EntryType::Symlink
            } else {
                tar::EntryType::Link
            });
            header.set_size(0);
            builder.append_link(&mut header, &path, target).unwrap();
        } else if entry.archive_path.is_some() {
            assert!(path.len() < 100, "raw header path too long");
            header.as_mut_bytes()[..path.len()].copy_from_slice(path.as_bytes());
            header.set_cksum();
            builder.append(&header, bytes.as_slice()).unwrap();
        } else {
            if let Some(kind) = entry.raw_type {
                header.set_entry_type(tar::EntryType::new(kind));
            }
            builder
                .append_data(&mut header, &path, bytes.as_slice())
                .unwrap();
        }
    }
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(&builder.into_inner().unwrap()).unwrap();
    let mut data = encoder.finish().unwrap();
    if let Some(size) = scenario.truncate_archive {
        data.truncate(size);
    }
    data
}
