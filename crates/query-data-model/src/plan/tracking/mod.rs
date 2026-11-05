mod bodies;
mod construction;
mod contract;
mod error;
mod ids;
mod plan;
mod rewrite;

use std::fmt::Debug;

pub use bodies::Opening;
pub use construction::{Connected, Construction};
pub use contract::{Declare, Output, Outputs};
pub use error::Rejected;
pub use ids::{Body, BodyId, Declared, NodeId, OutputId, PlanId, Ref, Rows, ScopeId};
pub use plan::Plan;
pub use rewrite::rewrite;

pub trait Language: Sized {
    type Operation: Debug;
    type Value: Clone + Debug + PartialEq;

    fn read(body: Body) -> Self::Operation;

    fn references(operation: &mut Self::Operation, visit: &mut dyn FnMut(&mut Ref));
}
