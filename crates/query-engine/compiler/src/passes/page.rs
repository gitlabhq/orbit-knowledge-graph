//! Row window pass. Runs after lowering establishes the final ORDER BY:
//! overfetches one probe row for `has_more` and lowers `SKIP` into an
//! OFFSET over a total order. Compiler tie-breakers extend the sort whenever
//! SKIP is present, so consecutive windows of the same statement neither
//! overlap nor drop rows.

use crate::ast::Node;
use crate::input::Input;
use crate::passes::lower::LoweredMetadata;

pub fn apply(node: &mut Node, input: &Input, metadata: &LoweredMetadata) {
    let Node::Query(q) = node else {
        return;
    };
    q.limit = Some(input.fetch_limit());
    let Some(skip) = input.skip else {
        return;
    };
    let tie_breakers: Vec<_> = metadata
        .stable_order
        .iter()
        .filter(|key| !q.order_by.iter().any(|current| current.expr == key.expr))
        .cloned()
        .collect();
    q.order_by.extend(tie_breakers);
    q.offset = (skip > 0).then_some(skip);
}
