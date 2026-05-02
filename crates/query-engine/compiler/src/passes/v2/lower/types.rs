//! Core types for the v2 lowerer.

use crate::ast::*;

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
