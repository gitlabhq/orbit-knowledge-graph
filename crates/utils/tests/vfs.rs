#[path = "vfs/archive.rs"]
mod archive;
#[path = "vfs/native.rs"]
mod native;
#[path = "vfs/runner/mod.rs"]
mod runner;

include!(concat!(env!("OUT_DIR"), "/vfs_cases.rs"));
