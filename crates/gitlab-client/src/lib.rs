mod cc_token;
mod circuit_breaking;
mod client;
mod error;
mod types;

pub use cc_token::{CloudConnectorTokenCache, CloudConnectorTokenFetcher};
pub use circuit_breaking::CircuitBreakingGitlabClient;
pub use client::{ByteStream, GitlabClient, JWT_AUDIENCE, JWT_ISSUER, JWT_SUBJECT};
pub use error::GitlabClientError;
pub use types::{
    CloudConnectorToken, MergeRequestDiffBatch, MergeRequestDiffFileEntry, ProjectInfo,
};
