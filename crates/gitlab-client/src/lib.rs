mod cc_token;
mod client;
mod error;
#[cfg(test)]
mod testing;
mod types;

pub use cc_token::{CloudConnectorTokenCache, CloudConnectorTokenFetcher};
pub use client::{ByteStream, GitlabClient, JWT_AUDIENCE, JWT_ISSUER, JWT_SUBJECT};
pub use error::GitlabClientError;
pub use types::{
    CloudConnectorToken, MergeRequestDiffBatch, MergeRequestDiffFileEntry, ProjectInfo,
};
