pub(crate) mod archive;
// pub: consumed by gkg-server's content resolver for Workhorse wire format parsing.
pub mod blob_stream;
pub mod cache;
pub mod resolver;
pub(crate) mod service;

pub use cache::{LocalRepositoryCache, RepositoryCache};
pub use resolver::{EmptyRepositoryReason, RepositoryResolver, ResolveError};
pub use service::{
    ByteStream, CachingRepositoryService, RailsRepositoryService, RepositoryService,
    RepositoryServiceError,
};
