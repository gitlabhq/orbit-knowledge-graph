//! Pipeline environments and capability traits.

use std::sync::Arc;

use ontology::Ontology;
use pipeline_macros::PipelineEnv;

use crate::pipeline::PipelineEnv;
use crate::types::SecurityContext;

crate::define_env_capabilities! {
    pub ontology: Arc<Ontology>,
    pub security_ctx: SecurityContext,
}

#[derive(PipelineEnv)]
pub struct SecureEnv {
    ontology: Arc<Ontology>,
    security_ctx: SecurityContext,
}

#[derive(PipelineEnv)]
pub struct LocalEnv {
    ontology: Arc<Ontology>,
}
