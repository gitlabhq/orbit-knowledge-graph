use std::io::ErrorKind;
use std::path::Path;

use rayon::prelude::*;

use super::directory::{put, virtual_path};
use super::{Loading, Source, SourceError, Tag, is_safe_relative_path};

pub struct Diff<'a> {
    pub root: &'a Path,
    pub paths: Vec<String>,
}

impl Source for Diff<'_> {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        let root = dunce::canonicalize(self.root)?;
        self.paths.into_par_iter().try_for_each(|path| {
            if !is_safe_relative_path(Path::new(&path)) || path.is_empty() {
                return Err(
                    std::io::Error::new(ErrorKind::InvalidInput, "invalid diff path").into(),
                );
            }
            put(root.join(&path), &virtual_path(Path::new(&path))?, into)
        })
    }
}
