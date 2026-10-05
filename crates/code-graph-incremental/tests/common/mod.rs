use std::sync::Arc;

use code_graph::v2::config::{CodeFilter, detect_language_from_path};
use code_graph_incremental::pipeline::Sources;
use orbit_utils::vfs::{Limits, Source, Vfs};

pub fn repo(source: impl Source) -> Sources {
    Arc::new(
        Vfs::load(
            source,
            CodeFilter::new(detect_language_from_path),
            Limits {
                file_bytes: Some(5 * 1024 * 1024),
                ..Limits::default()
            },
            Default::default(),
        )
        .unwrap(),
    )
}
