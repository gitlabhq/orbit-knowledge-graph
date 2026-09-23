//! An edge chain `e1 -> e2 -> ... -> eN` over one edge table, each hop joined
//! on the previous hop's end id:
//!
//! ```sql
//! FROM gl_edge e1
//! JOIN gl_edge e2 ON e1.target_id = e2.source_id
//! JOIN gl_edge e3 ON e2.target_id = e3.source_id
//! ```
//!
//! Variable-length hops (`chain.rs`) and pathfinding frontiers
//! (`pathfinding.rs`) both project what they need from `e1` and `eN`.

use super::prelude::*;

type JoinConditions = Vec<(Col, Col)>;

/// A chain `e1 -> e2 -> ... -> eN` over one edge table, each hop joined on
/// the previous hop's end id. Shared by variable-length hops and pathfinding
/// frontiers; callers project what they need from `e1` and `eN`.
pub fn hop_chain(
    edge: &dyn Fn(&str) -> PhysOp,
    depth: u32,
    (start_col, end_col): (&str, &str),
    hop_preds: &dyn Fn(&str, bool) -> Vec<PExpr>,
    hop_on: &dyn Fn(&str, &str) -> JoinConditions,
) -> PhysOp {
    let mut chain = edge("e1").filter(hop_preds("e1", true));
    for i in 2..=depth {
        let (prev, curr) = (format!("e{}", i - 1), format!("e{i}"));
        let mut on = vec![(
            (prev.clone(), end_col.to_string()),
            (curr.clone(), start_col.to_string()),
        )];
        on.extend(hop_on(&prev, &curr));
        chain = chain.join(edge(&curr).filter(hop_preds(&curr, false)), on);
    }
    chain
}

/// `array(tuple(e_i.end, e_i.end_kind), ...)` for hops `range`.
pub fn path_nodes(range: impl Iterator<Item = u32>, end_col: &str) -> PExpr {
    let kind = kind_col(end_col);
    let tuples: Vec<String> = range
        .map(|i| format!("tuple(e{i}.{end_col}, e{i}.{kind})"))
        .collect();
    pe!("[{}]", tuples.join(", "))
}

/// Kind column that goes with an id column on the same edge side.
pub fn kind_col(id_col: &str) -> &'static str {
    if id_col == SOURCE_ID_COLUMN {
        SOURCE_KIND_COLUMN
    } else {
        TARGET_KIND_COLUMN
    }
}
