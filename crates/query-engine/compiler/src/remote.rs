//! The pieces of code and live stages the DAG runner reads: the served targets and what their
//! reads' policies admit.

pub use crate::passes::plan::remote::{RemotePlan, Target};
pub use crate::passes::remote_security::{Admitted, admitted};
