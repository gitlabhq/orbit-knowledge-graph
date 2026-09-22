//! The operators a plan is made of, and how to build and walk a tree of them.
//!
//! ```text
//!   Limit 11
//!     Project [e0.source_id AS e0_src, mr.title AS mr_title]
//!       Join Inner (e0.target_id = mr.id)
//!         Filter [e0.relationship_kind = 'AUTHORED', e0._deleted = false]
//!           Scan gl_edge e0
//!         Filter [mr.state = 'opened', mr._deleted = false]
//!           Scan gl_merge_request mr FINAL
//! ```
//!
//! Builders read bottom-up like that tree:
//! `scan(..).filter(..).join(..).project(..).limit(11)`.

use super::prelude::*;
use serde::Serialize;

// ── Operators ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "op")]
pub enum PhysOp {
    Scan {
        table: String,
        alias: String,
        dedup: Dedup,
    },
    Filter {
        input: Box<PhysOp>,
        predicates: Vec<PExpr>,
    },
    Project {
        input: Box<PhysOp>,
        columns: Vec<Named>,
    },
    Join {
        left: Box<PhysOp>,
        right: Box<PhysOp>,
        on: Vec<(Col, Col)>,
        kind: JoinKind,
    },
    Aggregate {
        input: Box<PhysOp>,
        group_by: Vec<Named>,
        metrics: Vec<Named>,
    },
    Union {
        arms: Vec<PhysOp>,
        alias: String,
    },
    Sort {
        input: Box<PhysOp>,
        keys: Vec<(PExpr, bool)>,
    },
    Limit {
        input: Box<PhysOp>,
        count: u32,
    },
    /// Named subqueries (CTEs) visible to `input`.
    With {
        ctes: Vec<(String, PhysOp)>,
        input: Box<PhysOp>,
    },
}

// ── Operators ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub enum Dedup {
    None,
    /// `FINAL`: ReplacingMergeTree merge-on-read.
    Final,
    /// `ORDER BY <sort_key>, _version DESC LIMIT 1 BY <sort_key>`; keeps
    /// column pruning eligible. `_deleted` must be filtered after the dedup.
    LimitBy,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub enum JoinKind {
    Inner,
    /// `left.col IN (SELECT right.col FROM right)`; uses `on[0]`.
    Semi,
}

/// `expr AS alias`.
pub type Named = (PExpr, String);

// ── Building ──────────────────────────────────────────────────────────────────

pub fn scan(table: &str, alias: &str, dedup: Dedup) -> PhysOp {
    PhysOp::Scan {
        table: table.to_string(),
        alias: alias.to_string(),
        dedup,
    }
}

pub fn named(expr: PExpr, alias: impl Into<String>) -> Named {
    (expr, alias.into())
}

impl PhysOp {
    /// The predicate list is a conjunction; top-level ANDs are flattened
    /// into it and stacked filters merge, so rules see one flat list.
    pub fn filter(self, predicates: Vec<PExpr>) -> PhysOp {
        let predicates: Vec<PExpr> = predicates
            .into_iter()
            .flat_map(|p| match p {
                PExpr::And(xs) => xs,
                p => vec![p],
            })
            .collect();
        if predicates.is_empty() {
            return self;
        }
        // A filter over a filter is one filter.
        if let PhysOp::Filter {
            input,
            predicates: mut existing,
        } = self
        {
            existing.extend(predicates);
            return PhysOp::Filter {
                input,
                predicates: existing,
            };
        }
        PhysOp::Filter {
            input: Box::new(self),
            predicates,
        }
    }

    pub fn project(self, columns: Vec<Named>) -> PhysOp {
        PhysOp::Project {
            input: Box::new(self),
            columns,
        }
    }

    pub fn join(self, right: PhysOp, on: Vec<(Col, Col)>) -> PhysOp {
        PhysOp::Join {
            left: Box::new(self),
            right: Box::new(right),
            on,
            kind: JoinKind::Inner,
        }
    }

    /// `self.col IN (SELECT right.col FROM right)`.
    pub fn semi(self, right: PhysOp, on: (Col, Col)) -> PhysOp {
        PhysOp::Join {
            left: Box::new(self),
            right: Box::new(right),
            on: vec![on],
            kind: JoinKind::Semi,
        }
    }

    pub fn sort(self, keys: Vec<(PExpr, bool)>) -> PhysOp {
        if keys.is_empty() {
            return self;
        }
        PhysOp::Sort {
            input: Box::new(self),
            keys,
        }
    }

    pub fn limit(self, count: u32) -> PhysOp {
        PhysOp::Limit {
            input: Box::new(self),
            count,
        }
    }

    /// `UNION ALL` of `arms` as a derived table named `alias`; a single arm
    /// is still wrapped so the alias is stable.
    pub fn union(arms: Vec<PhysOp>, alias: &str) -> PhysOp {
        PhysOp::Union {
            arms,
            alias: alias.to_string(),
        }
    }
}

// ── Walking ───────────────────────────────────────────────────────────────────

impl PhysOp {
    /// Alias a relation is visible under: a scan's or union's, through any
    /// filter or projection over it.
    pub fn alias(&self) -> Option<&str> {
        match self {
            PhysOp::Scan { alias, .. } | PhysOp::Union { alias, .. } => Some(alias),
            PhysOp::Filter { input, .. } | PhysOp::Project { input, .. } => input.alias(),
            _ => None,
        }
    }

    /// Aliases visible to the enclosing query: scans and unions reached
    /// without entering a semi-join's producer or a union arm.
    pub fn visible_aliases(&self) -> HashSet<String> {
        fn go(op: &PhysOp, out: &mut HashSet<String>) {
            match op {
                PhysOp::Scan { alias, .. } | PhysOp::Union { alias, .. } => {
                    out.insert(alias.clone());
                }
                PhysOp::Join {
                    left,
                    kind: JoinKind::Semi,
                    ..
                } => go(left, out),
                _ => op.children().into_iter().for_each(|c| go(c, out)),
            }
        }
        let mut out = HashSet::new();
        go(self, &mut out);
        out
    }

    /// Aliases read by projections, aggregates, and sort keys anywhere in
    /// this scope. Join conditions tie a relation in without reading it;
    /// union arms are their own scope.
    pub fn read_aliases(&self) -> HashSet<String> {
        fn go(op: &PhysOp, out: &mut HashSet<String>) {
            if let PhysOp::Union { .. } = op {
                return;
            }
            if !matches!(op, PhysOp::Filter { .. }) {
                op.exprs().into_iter().for_each(|e| e.aliases(out));
            }
            op.children().into_iter().for_each(|c| go(c, out));
        }
        let mut out = HashSet::new();
        go(self, &mut out);
        out
    }

    pub fn children(&self) -> Vec<&PhysOp> {
        match self {
            PhysOp::Scan { .. } => vec![],
            PhysOp::Filter { input, .. }
            | PhysOp::Project { input, .. }
            | PhysOp::Aggregate { input, .. }
            | PhysOp::Sort { input, .. }
            | PhysOp::Limit { input, .. } => vec![input],
            PhysOp::Join { left, right, .. } => vec![left, right],
            PhysOp::Union { arms, .. } => arms.iter().collect(),
            PhysOp::With { ctes, input } => ctes
                .iter()
                .map(|(_, c)| c)
                .chain([input.as_ref()])
                .collect(),
        }
    }

    pub fn map_children(self, f: &mut dyn FnMut(PhysOp) -> PhysOp) -> PhysOp {
        match self {
            PhysOp::Scan { .. } => self,
            PhysOp::Filter { input, predicates } => PhysOp::Filter {
                input: Box::new(f(*input)),
                predicates,
            },
            PhysOp::Project { input, columns } => PhysOp::Project {
                input: Box::new(f(*input)),
                columns,
            },
            PhysOp::Join {
                left,
                right,
                on,
                kind,
            } => PhysOp::Join {
                left: Box::new(f(*left)),
                right: Box::new(f(*right)),
                on,
                kind,
            },
            PhysOp::Aggregate {
                input,
                group_by,
                metrics,
            } => PhysOp::Aggregate {
                input: Box::new(f(*input)),
                group_by,
                metrics,
            },
            PhysOp::Union { arms, alias } => PhysOp::Union {
                arms: arms.into_iter().map(|a| f(a)).collect(),
                alias,
            },
            PhysOp::Sort { input, keys } => PhysOp::Sort {
                input: Box::new(f(*input)),
                keys,
            },
            PhysOp::Limit { input, count } => PhysOp::Limit {
                input: Box::new(f(*input)),
                count,
            },
            PhysOp::With { ctes, input } => PhysOp::With {
                ctes: ctes.into_iter().map(|(n, c)| (n, f(c))).collect(),
                input: Box::new(f(*input)),
            },
        }
    }

    /// Expressions this operator evaluates itself (not its children's).
    pub fn exprs(&self) -> Vec<&PExpr> {
        match self {
            PhysOp::Filter { predicates, .. } => predicates.iter().collect(),
            PhysOp::Project { columns, .. } => columns.iter().map(|(e, _)| e).collect(),
            PhysOp::Aggregate {
                group_by, metrics, ..
            } => group_by.iter().chain(metrics).map(|(e, _)| e).collect(),
            PhysOp::Sort { keys, .. } => keys.iter().map(|(e, _)| e).collect(),
            _ => vec![],
        }
    }

    pub fn map_exprs(self, f: &dyn Fn(&PExpr) -> PExpr) -> PhysOp {
        let name = |(e, a): Named| (f(&e), a);
        match self {
            PhysOp::Filter { input, predicates } => PhysOp::Filter {
                input,
                predicates: predicates.iter().map(f).collect(),
            },
            PhysOp::Project { input, columns } => PhysOp::Project {
                input,
                columns: columns.into_iter().map(name).collect(),
            },
            PhysOp::Aggregate {
                input,
                group_by,
                metrics,
            } => PhysOp::Aggregate {
                input,
                group_by: group_by.into_iter().map(name).collect(),
                metrics: metrics.into_iter().map(name).collect(),
            },
            PhysOp::Sort { input, keys } => PhysOp::Sort {
                input,
                keys: keys.into_iter().map(|(e, d)| (f(&e), d)).collect(),
            },
            other => other,
        }
    }
}
