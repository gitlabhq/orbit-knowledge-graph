//! Pipeline environment types.
//!
//! Each environment struct carries the configuration needed by a specific
//! pipeline variant. Passes access the environment via `ctx.env()`.
//!
//! Capability traits (`HasOntology`, `HasSecurityCtx`) let passes declare
//! exactly what they need from the env without coupling to a concrete type.

use std::sync::Arc;

use crate::passes::security::SecurityContext;
use crate::pipeline::PipelineEnv;
use ontology::Ontology;

/// Environment provides access to the ontology.
pub trait HasOntology {
    fn ontology(&self) -> &Ontology;
}

/// Environment provides access to the security context.
pub trait HasSecurityCtx {
    fn security_ctx(&self) -> &SecurityContext;
}

/// Environment for the standard ClickHouse compilation pipeline.
///
/// Carries the ontology (shared, immutable) and a per-request security
/// context for traversal path filtering.
pub struct ClickHouseEnv {
    pub ontology: Arc<Ontology>,
    pub security_ctx: SecurityContext,
}

impl PipelineEnv for ClickHouseEnv {}

impl ClickHouseEnv {
    pub fn new(ontology: Arc<Ontology>, security_ctx: SecurityContext) -> Self {
        Self {
            ontology,
            security_ctx,
        }
    }
}

impl HasOntology for ClickHouseEnv {
    fn ontology(&self) -> &Ontology {
        &self.ontology
    }
}

impl HasSecurityCtx for ClickHouseEnv {
    fn security_ctx(&self) -> &SecurityContext {
        &self.security_ctx
    }
}

/// Environment for the hydration pipeline.
///
/// Hydration queries are internal-only — they operate on pre-authorized IDs
/// and still need the security context for keyset pagination in optimize.
pub struct HydrationEnv {
    pub ontology: Arc<Ontology>,
    pub security_ctx: SecurityContext,
}

impl PipelineEnv for HydrationEnv {}

impl HydrationEnv {
    pub fn new(ontology: Arc<Ontology>, security_ctx: SecurityContext) -> Self {
        Self {
            ontology,
            security_ctx,
        }
    }
}

impl HasOntology for HydrationEnv {
    fn ontology(&self) -> &Ontology {
        &self.ontology
    }
}

impl HasSecurityCtx for HydrationEnv {
    fn security_ctx(&self) -> &SecurityContext {
        &self.security_ctx
    }
}
