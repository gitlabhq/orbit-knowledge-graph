use orbit_utils::vfs::{Decision, File, Pass, Vfs, sources::Directory};
use std::{path::Path, sync::Arc};

struct FixturePolicy;

impl Pass for FixturePolicy {
    type Tag = ();
    fn metadata(&self, file: &File<'_, ()>) -> Decision<()> {
        if file.path.ends_with(".png") || file.path.ends_with(".min.js") {
            Decision::List("excluded_extension")
        } else {
            Decision::Pending
        }
    }
    fn content(&self, file: &File<'_, ()>) -> Decision<()> {
        if file.bytes().is_some_and(|bytes| bytes.contains(&0)) {
            Decision::List("binary")
        } else {
            Decision::Keep(())
        }
    }
}

pub fn load(source: impl orbit_utils::vfs::Source) -> Arc<Vfs<()>> {
    Arc::new(
        Vfs::load(
            source,
            FixturePolicy,
            Default::default(),
            Default::default(),
        )
        .unwrap(),
    )
}

pub fn walk(root: &Path) -> Arc<Vfs<()>> {
    load(Directory(root))
}
