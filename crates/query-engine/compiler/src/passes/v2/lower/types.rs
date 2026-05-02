//! Core types for the v2 lowerer.

use crate::ast::*;
use crate::error::Result;
use crate::input::*;

// ─────────────────────────────────────────────────────────────────────────────
// Skeleton: edge chain + hydration result
// ─────────────────────────────────────────────────────────────────────────────

pub struct Skeleton {
    pub from: TableRef,
    pub edge_aliases: Vec<String>,
    pub where_parts: Vec<Expr>,
    pub select: Vec<SelectExpr>,
    pub ctes: Vec<Cte>,
}

impl Skeleton {
    /// Assemble into a final Query with additional SELECT, GROUP BY, ORDER BY.
    pub fn into_query(
        self,
        mut select: Vec<SelectExpr>,
        group_by: Vec<Expr>,
        order_by: Vec<OrderExpr>,
        limit: u32,
    ) -> Query {
        select.extend(self.select);
        Query {
            ctes: self.ctes,
            select,
            from: self.from,
            where_clause: Expr::conjoin(self.where_parts),
            group_by,
            order_by,
            limit: Some(limit),
            ..Default::default()
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Hydration strategy
// ─────────────────────────────────────────────────────────────────────────────

pub enum HydrationStrategy {
    /// Full JOIN — needed for GROUP BY, ORDER BY, agg property targets.
    Join,
    /// WHERE IN subquery — non-denormalized filters, no columns in SELECT.
    Subquery,
    /// No hydration — edge carries everything needed.
    Skip,
}

pub fn hydration_strategy(node: &InputNode, input: &Input) -> HydrationStrategy {
    let is_group_by = input
        .aggregations
        .iter()
        .any(|a| a.group_by.as_deref() == Some(&node.id));

    let is_agg_property_target = input.aggregations.iter().any(|a| {
        a.target.as_deref() == Some(&node.id)
            && a.property.is_some()
            && !matches!(a.function, AggFunction::Count)
    });

    let is_order_by_target = input.order_by.as_ref().is_some_and(|ob| ob.node == node.id);

    if is_group_by || is_agg_property_target || is_order_by_target {
        return HydrationStrategy::Join;
    }

    let has_non_denorm_filters = node.filters.iter().any(|(prop, _)| {
        let entity = node.entity.as_deref().unwrap_or("");
        let k1 = (entity.to_string(), prop.clone(), "source".to_string());
        let k2 = (entity.to_string(), prop.clone(), "target".to_string());
        !input.compiler.denormalized_columns.contains_key(&k1)
            && !input.compiler.denormalized_columns.contains_key(&k2)
    });

    if has_non_denorm_filters {
        return HydrationStrategy::Subquery;
    }

    HydrationStrategy::Skip
}
