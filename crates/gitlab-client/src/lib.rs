mod circuit_breaking;
mod client;
mod error;
mod gitaly_proxy;
mod types;

#[cfg(any(test, feature = "testkit"))]
pub mod test_support;

pub use circuit_breaking::CircuitBreakingGitlabClient;
pub use client::{ByteStream, GitlabClient, JWT_AUDIENCE, JWT_ISSUER, JWT_SUBJECT};
pub use error::GitlabClientError;
pub use gitaly_proxy::{
    GitalyProxyChannel, GitalyProxyChannels, GitalyProxyDialer, GitalyProxyError,
    GitalyProxySession, HEADER_EXPIRES_IN, HEADER_PROFILE, HEADER_REPOSITORY,
    PROFILE_READONLY_REPOSITORY, PROXY_STATUS_PREFIX, REASON_SESSION_EXPIRED, REASON_SHUTTING_DOWN,
    REASON_STREAM_DEADLINE, StatusClass, WebSocketIo, classify_status, proxy_reason,
};
pub use types::{MergeRequestDiffBatch, MergeRequestDiffFileEntry, ProjectInfo};
