//! The code-indexing pipeline's YAML: pipeline.yaml, the shared rule files
//! and the libraries they import, embedded at build time.

include!(concat!(env!("OUT_DIR"), "/rule_files.rs"));

pub const PIPELINE: &str = include_str!("../config/pipeline.yaml");
