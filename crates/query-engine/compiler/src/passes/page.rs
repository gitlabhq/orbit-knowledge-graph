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
