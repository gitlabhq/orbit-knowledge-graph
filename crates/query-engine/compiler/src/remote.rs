//! The pieces of code and live stages the DAG runner reads: the served targets and the scope
//! lookups their reads need.

pub use crate::passes::plan::remote::{RemotePlan, Target};
pub use crate::passes::remote_security::{Admitted, Namespaces, admitted};

pub use crate::passes::scope_lookup::{KEY, Resolves, ScopeLookup, lookup, namespaces};
