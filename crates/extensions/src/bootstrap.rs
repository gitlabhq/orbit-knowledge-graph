//! Process-start bootstrap: fetch every provider's packages and merge them with
//! the bundled core model into the one ontology a process serves and indexes.
//!
//! This is the shared entry point for the webserver, the indexer, and the
//! dispatcher. With no providers configured it returns the core model unchanged,
//! so default behavior is exactly today's. A provider that is unreachable or
//! serves an invalid package is logged and skipped: Orbit still starts and serves
//! core, and the next refresh picks the provider up (a new digest then triggers
//! the graceful restart).

use ontology::Ontology;

use crate::loader::{LoaderError, load_merged_ontology};
use crate::package::Package;
use crate::provider::{ProviderClient, ProviderConfig};

/// The merged model plus what the scope dispatcher and the refresh loop need:
/// the fetched packages (with their digests) and each provider's scopes table.
pub struct ProviderModel {
    pub ontology: Ontology,
    pub packages: Vec<Package>,
    pub scopes_tables: Vec<String>,
}

/// Fetches and merges every provider's packages. Never fails for a provider
/// reason: unreachable providers and rejected packages are skipped. It fails
/// only if the resulting, possibly core-only, model cannot be built.
pub async fn load_provider_model(
    providers: &[ProviderConfig],
    schema_version: u32,
) -> Result<ProviderModel, LoaderError> {
    let mut packages = Vec::new();
    let mut scopes_tables = Vec::new();

    for provider in providers {
        match fetch_provider_packages(provider).await {
            Ok((provider_packages, scopes_table)) => {
                packages.extend(provider_packages);
                scopes_tables.push(scopes_table);
            }
            Err(error) => {
                orbit_log_fields::warn!(
                    safe(ERROR_TYPE) = "extension_provider_unavailable",
                    expanded(ERROR_MESSAGE) = %error,
                    "extension provider unavailable at start; serving core and retrying on refresh"
                );
            }
        }
    }

    let ontology = load_merged_ontology(schema_version, &packages)?;
    Ok(ProviderModel {
        ontology,
        packages,
        scopes_tables,
    })
}

async fn fetch_provider_packages(
    provider: &ProviderConfig,
) -> Result<(Vec<Package>, String), BootstrapError> {
    let mut client = ProviderClient::connect(provider).await?;
    let manifest = client.get_manifest().await?;
    let mut packages = Vec::with_capacity(manifest.packages.len());
    for reference in &manifest.packages {
        let package = client
            .get_package(
                &reference.name,
                &reference.version,
                reference.digest.clone(),
            )
            .await?;
        packages.push(package);
    }
    Ok((packages, manifest.scopes_table))
}

#[derive(Debug, thiserror::Error)]
enum BootstrapError {
    #[error(transparent)]
    Provider(#[from] crate::provider::ProviderError),
}
