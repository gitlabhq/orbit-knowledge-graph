// pub: consumed by gkg-server's content resolver for Workhorse wire format parsing.
pub mod blob_stream;
pub mod resolver;
pub(crate) mod service;

pub use resolver::{
    EmptyRepositoryReason, RepoDir, RepositoryResolver, ResolveError, cleanup_stale_temp_dirs,
};
pub use service::{
    ByteStream, CachingRepositoryService, RailsRepositoryService, RepositoryService,
    RepositoryServiceError,
};
