//! Security context injection pass.
//!
//! Walks the Substrait plan tree and injects traversal-path predicates
//! to enforce namespace-scoped access control. This is the defense-in-depth
//! layer — even if a frontend forgets security filtering, this pass adds it.
//!
//! TODO: implement once the security context types are defined.

use crate::ir::plan::Plan;

/// Security context for traversal-path-based access control.
#[derive(Debug, Clone)]
pub struct SecurityContext {
    pub traversal_paths: Vec<String>,
}

/// Injects security predicates into the plan.
pub struct SecurityPass {
    pub context: SecurityContext,
}

#[derive(Debug, thiserror::Error)]
pub enum SecurityError {
    #[error("security pass not yet implemented")]
    NotImplemented,
}

impl super::Pass for SecurityPass {
    type Error = SecurityError;

    fn transform(&self, plan: Plan) -> Result<Plan, Self::Error> {
        // TODO: walk the Substrait tree, find ReadRels, inject
        // `startsWith(traversal_path, ...)` predicates.
        let _ = &self.context;
        Ok(plan)
    }
}
