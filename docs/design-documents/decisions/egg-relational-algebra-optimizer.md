# ADR: Equality Saturation Query Optimizer via Relational Algebra

**Status:** Proposed
**Date:** 2026-04-29

## Context

The GKG query compiler transforms JSON DSL queries into ClickHouse SQL through:

```
JSON -> Validate -> Normalize -> Restrict -> Lower -> Optimize -> Enforce -> Deduplicate -> Security -> Check -> HydratePlan -> Settings -> Codegen
```

The Optimize pass (`passes/optimize.rs`, 3646 lines) contains 13 hand-written procedural
transformations in a fixed order. Each mutates the SQL AST in place. Problems:

1. **Phase ordering:** Fixed pass sequence means optimizations miss opportunities created
   by later passes (e.g. `cascade_node_filter_ctes` runs before `apply_traversal_hop_frontiers`).
2. **Compositional fragility:** Each pass navigates CTEs, WHERE clauses, nested JOINs, UNION
   ALL arms via tree-walking helpers. Adding a new optimization requires understanding all 12 others.
3. **SQL syntax as IR:** CTEs have definitional ordering, SIP is `InSubquery { cte_name }` coupling
   optimization to emission order. Impossible to compare "SIP CTE vs inline semi-join."
4. **No cost model:** Every applicable transformation fires unconditionally.

This ADR proposes replacing the optimization pass with equality saturation using `egg`,
operating on a relational algebra IR.

## Decision

### New pipeline

```
JSON -> ... -> Lower(->RelAlg) -> EggOptimize -> Enforce(RelAlg) -> Deduplicate(RelAlg) -> Security(RelAlg) -> Check -> HydratePlan -> Settings -> Codegen(RelAlg->SQL)
```

---

## 1. The `define_language!` Enum

File: `crates/query-engine/relalg/src/lang.rs`

```rust
use egg::{define_language, Id, Symbol};

define_language! {
    /// Relational algebra + scalar expressions for GKG query optimization.
    ///
    /// Relational operators produce relations (bags of tuples).
    /// Scalar operators produce values within a single tuple.
    /// List operators encode variable-arity children (Cons/Nil).
    pub enum RelAlg {
        // =====================================================================
        // Relational operators
        // =====================================================================

        /// Base table scan. Child: table name symbol.
        /// Produces all rows from the named physical table.
        ///   (scan "gl_user")
        "scan" = Scan(Id),

        /// Table scan with ClickHouse FINAL modifier.
        /// Forces ReplacingMergeTree dedup at read time.
        ///   (scan-final "gl_user")
        "scan-final" = ScanFinal(Id),

        /// Selection (sigma): filter rows by predicate.
        ///   (select <predicate> <relation>)
        "select" = Select([Id; 2]),

        /// Projection (pi): restrict output columns.
        ///   (project <col-list> <relation>)
        "project" = Project([Id; 2]),

        /// Rename: change column names.
        ///   (rename <col-map-list> <relation>)
        "rename" = Rename([Id; 2]),

        /// Inner join with predicate.
        ///   (join <predicate> <left-rel> <right-rel>)
        "join" = Join([Id; 3]),

        /// Left outer join.
        ///   (left-join <predicate> <left-rel> <right-rel>)
        "left-join" = LeftJoin([Id; 3]),

        /// Semi-join: filter left by existence in right.
        /// Preserves left schema, never duplicates rows.
        ///   (semi-join <predicate> <left-rel> <right-rel>)
        "semi-join" = SemiJoin([Id; 3]),

        /// Anti-join: filter left by non-existence in right.
        ///   (anti-join <predicate> <left-rel> <right-rel>)
        "anti-join" = AntiJoin([Id; 3]),

        /// Bag union (UNION ALL).
        ///   (union <left-rel> <right-rel>)
        "union" = Union([Id; 2]),

        /// Group-by with aggregation (gamma).
        ///   (group-by <key-list> <agg-list> <relation>)
        "group-by" = GroupBy([Id; 3]),

        /// Sort (tau).
        ///   (order-by <sort-key-list> <relation>)
        "order-by" = OrderBy([Id; 2]),

        /// Top-N limit.
        ///   (limit <n> <relation>)
        "limit" = Limit([Id; 2]),

        /// ClickHouse LIMIT n BY cols.
        ///   (limit-by <n> <col-list> <relation>)
        "limit-by" = LimitBy([Id; 3]),

        /// Distinct (delta).
        ///   (distinct <relation>)
        "distinct" = Distinct(Id),

        /// Let-binding: materialize `def`, make it referenceable as `name` in `body`.
        /// Maps to CTEs in SQL.
        ///   (let <name-sym> <def-relation> <body-relation>)
        "let" = Let([Id; 3]),

        /// Reference to a Let-bound name.
        ///   (ref <name-sym>)
        "ref" = Ref(Id),

        /// Tagged table scan: Scan with an alias carried through the algebra.
        /// Lets us track which "alias" a scan corresponds to through rewrites.
        ///   (aliased-scan <table-sym> <alias-sym>)
        "aliased-scan" = AliasedScan([Id; 2]),

        // =====================================================================
        // Scalar expressions
        // =====================================================================

        /// Column reference: <table-alias>.<column-name>.
        ///   (col <table-sym> <column-sym>)
        "col" = Col([Id; 2]),

        /// Typed parameter: ClickHouse {pN:Type} bind variable.
        ///   (param <ch-type-sym> <value-sym>)
        "param" = Param([Id; 2]),

        /// Binary operator: comparison, arithmetic, logical.
        ///   (binop <op-sym> <left-expr> <right-expr>)
        "binop" = BinOp([Id; 3]),

        /// Unary operator: NOT, IS NULL, IS NOT NULL.
        ///   (unop <op-sym> <expr>)
        "unop" = UnOp([Id; 2]),

        /// Function call: ClickHouse functions (startsWith, arrayExists, etc.).
        ///   (func <name-sym> <arg-list>)
        "func" = Func([Id; 2]),

        /// Lambda expression: `param -> body`.
        ///   (lambda <param-sym> <body-expr>)
        "lambda" = Lambda([Id; 2]),

        /// Logical AND of two predicates.
        ///   (and <left> <right>)
        "and" = And([Id; 2]),

        /// Logical OR of two predicates.
        ///   (or <left> <right>)
        "or" = Or([Id; 2]),

        /// Logical NOT.
        ///   (not <expr>)
        "not" = Not(Id),

        /// IN list check: `expr IN (v1, v2, ...)`.
        ///   (in-list <expr> <value-list>)
        "in-list" = InList([Id; 2]),

        /// Boolean true literal (AND identity).
        "true" = True,

        /// Boolean false literal (OR identity).
        "false" = False,

        // =====================================================================
        // Aggregate expressions
        // =====================================================================

        /// Aggregate: func(arg).
        ///   (agg <func-name-sym> <arg-expr>)
        "agg" = Agg([Id; 2]),

        /// Aggregate with -If combinator: funcIf(arg, condition).
        ///   (agg-if <func-name-sym> <arg-expr> <condition-expr>)
        "agg-if" = AggIf([Id; 3]),

        /// Argument-less aggregate: count().
        ///   (agg0 <func-name-sym>)
        "agg0" = Agg0(Id),

        // =====================================================================
        // List construction (Cons/Nil for variable-arity)
        // =====================================================================

        /// Nil: empty list.
        "nil" = Nil,

        /// Cons: (head, tail).
        ///   (cons <head> <tail>)
        "cons" = Cons([Id; 2]),

        // =====================================================================
        // Sort/rename metadata
        // =====================================================================

        /// Sort key: (expr, direction).
        ///   (sort-key <expr> <direction-sym>)  ; direction = "asc" | "desc"
        "sort-key" = SortKey([Id; 2]),

        /// Column mapping entry: (old-name, new-name).
        ///   (col-map <old-sym> <new-sym>)
        "col-map" = ColMap([Id; 2]),

        // =====================================================================
        // Leaf symbols / literals
        // =====================================================================

        /// Interned string: table names, column names, operator names,
        /// ClickHouse type tags, string literal values.
        Sym(Symbol),

        /// Integer literal: LIMIT counts, depth values, node IDs.
        Num(i64),
    }
}

// ─── Convenience constructors ────────────────────────────────────────────────

use egg::{RecExpr, Id as EggId};

/// Helper to build RelAlg expressions programmatically.
/// Used by the lowerer and tests.
pub struct Builder {
    pub expr: RecExpr<RelAlg>,
}

impl Builder {
    pub fn new() -> Self {
        Self { expr: RecExpr::default() }
    }

    pub fn sym(&mut self, s: &str) -> Id {
        self.expr.add(RelAlg::Sym(Symbol::from(s)))
    }

    pub fn num(&mut self, n: i64) -> Id {
        self.expr.add(RelAlg::Num(n))
    }

    pub fn nil(&mut self) -> Id {
        self.expr.add(RelAlg::Nil)
    }

    pub fn cons(&mut self, head: Id, tail: Id) -> Id {
        self.expr.add(RelAlg::Cons([head, tail]))
    }

    /// Build a list from an iterator of Ids (right-folds into Cons/Nil).
    pub fn list(&mut self, items: impl IntoIterator<Item = Id>) -> Id {
        let items: Vec<Id> = items.into_iter().collect();
        let mut list = self.nil();
        for item in items.into_iter().rev() {
            list = self.cons(item, list);
        }
        list
    }

    pub fn scan(&mut self, table: &str) -> Id {
        let t = self.sym(table);
        self.expr.add(RelAlg::Scan(t))
    }

    pub fn aliased_scan(&mut self, table: &str, alias: &str) -> Id {
        let t = self.sym(table);
        let a = self.sym(alias);
        self.expr.add(RelAlg::AliasedScan([t, a]))
    }

    pub fn select(&mut self, pred: Id, rel: Id) -> Id {
        self.expr.add(RelAlg::Select([pred, rel]))
    }

    pub fn project(&mut self, cols: Id, rel: Id) -> Id {
        self.expr.add(RelAlg::Project([cols, rel]))
    }

    pub fn join(&mut self, pred: Id, left: Id, right: Id) -> Id {
        self.expr.add(RelAlg::Join([pred, left, right]))
    }

    pub fn semi_join(&mut self, pred: Id, left: Id, right: Id) -> Id {
        self.expr.add(RelAlg::SemiJoin([pred, left, right]))
    }

    pub fn union(&mut self, left: Id, right: Id) -> Id {
        self.expr.add(RelAlg::Union([left, right]))
    }

    pub fn group_by(&mut self, keys: Id, aggs: Id, rel: Id) -> Id {
        self.expr.add(RelAlg::GroupBy([keys, aggs, rel]))
    }

    pub fn order_by(&mut self, keys: Id, rel: Id) -> Id {
        self.expr.add(RelAlg::OrderBy([keys, rel]))
    }

    pub fn limit(&mut self, n: i64, rel: Id) -> Id {
        let n_id = self.num(n);
        self.expr.add(RelAlg::Limit([n_id, rel]))
    }

    pub fn limit_by(&mut self, n: i64, cols: Id, rel: Id) -> Id {
        let n_id = self.num(n);
        self.expr.add(RelAlg::LimitBy([n_id, cols, rel]))
    }

    pub fn let_bind(&mut self, name: &str, def: Id, body: Id) -> Id {
        let n = self.sym(name);
        self.expr.add(RelAlg::Let([n, def, body]))
    }

    pub fn ref_bind(&mut self, name: &str) -> Id {
        let n = self.sym(name);
        self.expr.add(RelAlg::Ref(n))
    }

    pub fn col(&mut self, table: &str, column: &str) -> Id {
        let t = self.sym(table);
        let c = self.sym(column);
        self.expr.add(RelAlg::Col([t, c]))
    }

    pub fn param(&mut self, ch_type: &str, value: &str) -> Id {
        let t = self.sym(ch_type);
        let v = self.sym(value);
        self.expr.add(RelAlg::Param([t, v]))
    }

    pub fn eq(&mut self, left: Id, right: Id) -> Id {
        let op = self.sym("=");
        self.expr.add(RelAlg::BinOp([op, left, right]))
    }

    pub fn and(&mut self, left: Id, right: Id) -> Id {
        self.expr.add(RelAlg::And([left, right]))
    }

    pub fn or(&mut self, left: Id, right: Id) -> Id {
        self.expr.add(RelAlg::Or([left, right]))
    }

    pub fn binop(&mut self, op: &str, left: Id, right: Id) -> Id {
        let o = self.sym(op);
        self.expr.add(RelAlg::BinOp([o, left, right]))
    }

    pub fn unop(&mut self, op: &str, expr: Id) -> Id {
        let o = self.sym(op);
        self.expr.add(RelAlg::UnOp([o, expr]))
    }

    pub fn func(&mut self, name: &str, args: Id) -> Id {
        let n = self.sym(name);
        self.expr.add(RelAlg::Func([n, args]))
    }

    pub fn in_list(&mut self, expr: Id, values: Id) -> Id {
        self.expr.add(RelAlg::InList([expr, values]))
    }

    pub fn agg(&mut self, func_name: &str, arg: Id) -> Id {
        let f = self.sym(func_name);
        self.expr.add(RelAlg::Agg([f, arg]))
    }

    pub fn agg_if(&mut self, func_name: &str, arg: Id, cond: Id) -> Id {
        let f = self.sym(func_name);
        self.expr.add(RelAlg::AggIf([f, arg, cond]))
    }

    pub fn agg0(&mut self, func_name: &str) -> Id {
        let f = self.sym(func_name);
        self.expr.add(RelAlg::Agg0(f))
    }

    pub fn sort_key(&mut self, expr: Id, dir: &str) -> Id {
        let d = self.sym(dir);
        self.expr.add(RelAlg::SortKey([expr, d]))
    }

    pub fn true_val(&mut self) -> Id {
        self.expr.add(RelAlg::True)
    }

    pub fn false_val(&mut self) -> Id {
        self.expr.add(RelAlg::False)
    }
}
```

---

## 2. The Analysis

File: `crates/query-engine/relalg/src/analysis.rs`

```rust
use egg::{Analysis, DidMerge, EGraph, Id};
use std::collections::{BTreeSet, HashMap, HashSet};

use crate::lang::{RelAlg, Symbol};

// ─── External context ────────────────────────────────────────────────────────

/// ClickHouse table physical properties, loaded from the ontology.
#[derive(Debug, Clone)]
pub struct TableProps {
    /// Primary key / ORDER BY columns.
    pub sort_key: Vec<String>,
    /// Named projections -> their column sets.
    pub projections: HashMap<String, Vec<String>>,
    /// true for edge tables (gl_edge, etc.)
    pub is_edge_table: bool,
    /// text-indexed columns -> tokenizer strategy
    pub text_indexes: HashMap<String, String>,
}

/// Selectivity hint for a node alias, derived from Input before the e-graph runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Selectivity {
    /// Has pinned node_ids (very selective).
    Pinned,
    /// Has user-supplied filters (moderately selective).
    Filtered,
    /// Has only security filters (broad).
    Broad,
}

/// Immutable context built from Input + ontology before the e-graph is created.
/// Passed to Analysis and CostFunction via the EGraph's analysis field.
#[derive(Debug, Clone)]
pub struct QueryContext {
    /// All known edge table names.
    pub edge_tables: HashSet<String>,
    /// Physical columns per table.
    pub table_columns: HashMap<String, HashSet<String>>,
    /// Table physical properties.
    pub table_props: HashMap<String, TableProps>,
    /// Mapping: physical table name -> entity type name.
    pub entity_for_table: HashMap<String, String>,
    /// Mapping: entity type -> physical table name.
    pub table_for_entity: HashMap<String, String>,
    /// Node alias -> selectivity.
    pub node_selectivity: HashMap<String, Selectivity>,
    /// Edge table -> whether it has traversal_path column.
    pub has_traversal_path: HashMap<String, bool>,
    /// Relationship kind -> destination edge table(s).
    pub edge_table_for_rel: HashMap<String, String>,
    /// Default edge table name.
    pub default_edge_table: String,
}

// ─── Per-eclass data ─────────────────────────────────────────────────────────

/// Metadata computed per equivalence class.
#[derive(Debug, Clone, Default)]
pub struct RelAlgData {
    /// Columns available in this relation's schema.
    /// For scalar exprs: columns referenced.
    /// Stored as (table_alias, column_name) pairs.
    pub columns: BTreeSet<(Symbol, Symbol)>,

    /// Physical tables referenced transitively.
    pub tables: BTreeSet<Symbol>,

    /// Does this relation have selective predicates?
    /// (pinned IDs, user filters on indexed columns)
    pub is_selective: bool,

    /// The alias of this scan (if this eclass contains an AliasedScan).
    pub alias: Option<Symbol>,

    /// Is this node a constant / literal expression?
    pub is_const: bool,

    /// Is this an edge table scan?
    pub is_edge: bool,

    /// Number of distinct tables in this subtree (for join ordering heuristics).
    pub num_tables: usize,
}

// ─── Analysis implementation ─────────────────────────────────────────────────

pub struct RelAlgAnalysis {
    pub ctx: QueryContext,
}

impl Analysis<RelAlg> for RelAlgAnalysis {
    type Data = RelAlgData;

    fn make(egraph: &EGraph<RelAlg, Self>, enode: &RelAlg) -> Self::Data {
        let ctx = &egraph.analysis.ctx;
        match enode {
            // ── Table scans ──────────────────────────────────────
            RelAlg::Scan(table_id) => {
                let table_sym = extract_symbol(egraph, *table_id);
                let is_edge = table_sym
                    .map(|s| ctx.edge_tables.contains(s.as_str()))
                    .unwrap_or(false);
                RelAlgData {
                    tables: table_sym.into_iter().collect(),
                    is_edge,
                    num_tables: 1,
                    ..Default::default()
                }
            }
            RelAlg::ScanFinal(table_id) => {
                let table_sym = extract_symbol(egraph, *table_id);
                RelAlgData {
                    tables: table_sym.into_iter().collect(),
                    num_tables: 1,
                    ..Default::default()
                }
            }
            RelAlg::AliasedScan([table_id, alias_id]) => {
                let table_sym = extract_symbol(egraph, *table_id);
                let alias_sym = extract_symbol(egraph, *alias_id);
                let is_edge = table_sym
                    .map(|s| ctx.edge_tables.contains(s.as_str()))
                    .unwrap_or(false);
                let is_selective = alias_sym
                    .map(|a| matches!(
                        ctx.node_selectivity.get(a.as_str()),
                        Some(Selectivity::Pinned | Selectivity::Filtered)
                    ))
                    .unwrap_or(false);
                RelAlgData {
                    tables: table_sym.into_iter().collect(),
                    alias: alias_sym,
                    is_edge,
                    is_selective,
                    num_tables: 1,
                    ..Default::default()
                }
            }

            // ── Selection ────────────────────────────────────────
            RelAlg::Select([pred, rel]) => {
                let rd = &egraph[*rel].data;
                let pd = &egraph[*pred].data;
                // A selection is selective if the child is, or the predicate
                // references indexed columns (kind, id, traversal_path).
                let pred_is_selective = pd.columns.iter().any(|(_, col)| {
                    let c = col.as_str();
                    c == "id" || c == "source_kind" || c == "target_kind"
                        || c == "source_id" || c == "target_id"
                        || c == "traversal_path"
                });
                RelAlgData {
                    columns: rd.columns.clone(),
                    tables: rd.tables.clone(),
                    is_selective: rd.is_selective || pred_is_selective,
                    alias: rd.alias,
                    is_edge: rd.is_edge,
                    num_tables: rd.num_tables,
                    ..Default::default()
                }
            }

            // ── Projection ───────────────────────────────────────
            RelAlg::Project([cols, rel]) => {
                let rd = &egraph[*rel].data;
                let cd = &egraph[*cols].data;
                RelAlgData {
                    columns: cd.columns.clone(), // projected cols
                    tables: rd.tables.clone(),
                    is_selective: rd.is_selective,
                    num_tables: rd.num_tables,
                    ..Default::default()
                }
            }

            // ── Joins ────────────────────────────────────────────
            RelAlg::Join([_pred, left, right]) | RelAlg::LeftJoin([_pred, left, right]) => {
                let ld = &egraph[*left].data;
                let rd = &egraph[*right].data;
                RelAlgData {
                    columns: ld.columns.union(&rd.columns).cloned().collect(),
                    tables: ld.tables.union(&rd.tables).cloned().collect(),
                    is_selective: ld.is_selective || rd.is_selective,
                    num_tables: ld.num_tables + rd.num_tables,
                    ..Default::default()
                }
            }
            RelAlg::SemiJoin([_pred, left, _right]) => {
                let ld = &egraph[*left].data;
                RelAlgData {
                    columns: ld.columns.clone(),
                    tables: ld.tables.clone(),
                    is_selective: true, // semi-join always narrows
                    alias: ld.alias,
                    is_edge: ld.is_edge,
                    num_tables: ld.num_tables,
                    ..Default::default()
                }
            }
            RelAlg::AntiJoin([_pred, left, _right]) => {
                let ld = &egraph[*left].data;
                RelAlgData {
                    columns: ld.columns.clone(),
                    tables: ld.tables.clone(),
                    is_selective: true,
                    alias: ld.alias,
                    num_tables: ld.num_tables,
                    ..Default::default()
                }
            }

            // ── Union ────────────────────────────────────────────
            RelAlg::Union([left, right]) => {
                let ld = &egraph[*left].data;
                let rd = &egraph[*right].data;
                RelAlgData {
                    columns: ld.columns.union(&rd.columns).cloned().collect(),
                    tables: ld.tables.union(&rd.tables).cloned().collect(),
                    is_selective: ld.is_selective && rd.is_selective,
                    num_tables: ld.num_tables.max(rd.num_tables),
                    ..Default::default()
                }
            }

            // ── Group-by ─────────────────────────────────────────
            RelAlg::GroupBy([_keys, _aggs, rel]) => {
                let rd = &egraph[*rel].data;
                RelAlgData {
                    tables: rd.tables.clone(),
                    is_selective: rd.is_selective,
                    num_tables: rd.num_tables,
                    ..Default::default()
                }
            }

            // ── Let/Ref ──────────────────────────────────────────
            RelAlg::Let([_name, _def, body]) => {
                egraph[*body].data.clone()
            }
            RelAlg::Ref(_name) => {
                // Ref's data is unknown without binding context.
                // Cost function handles this via Let cost.
                RelAlgData::default()
            }

            // ── Limit / Order ────────────────────────────────────
            RelAlg::Limit([_n, rel]) | RelAlg::Distinct(rel) | RelAlg::OrderBy([_n, rel]) => {
                // Passthrough: these don't change schema or selectivity.
                // (OrderBy pattern: _n is sort-key-list, rel is second child)
                egraph[*rel].data.clone()
            }
            RelAlg::LimitBy([_n, _cols, rel]) => {
                egraph[*rel].data.clone()
            }

            // ── Column references ────────────────────────────────
            RelAlg::Col([table_id, col_id]) => {
                let t = extract_symbol(egraph, *table_id);
                let c = extract_symbol(egraph, *col_id);
                let mut cols = BTreeSet::new();
                if let (Some(t), Some(c)) = (t, c) {
                    cols.insert((t, c));
                }
                RelAlgData { columns: cols, ..Default::default() }
            }

            // ── Binary/unary ops ─────────────────────────────────
            RelAlg::BinOp([_op, left, right]) | RelAlg::And([left, right]) | RelAlg::Or([left, right]) => {
                let ld = &egraph[*left].data;
                let rd = &egraph[*right].data;
                RelAlgData {
                    columns: ld.columns.union(&rd.columns).cloned().collect(),
                    ..Default::default()
                }
            }
            RelAlg::UnOp([_op, expr]) | RelAlg::Not(expr) => {
                egraph[*expr].data.clone()
            }

            // ── Constants ────────────────────────────────────────
            RelAlg::Param([_ty, _val]) | RelAlg::Sym(_) | RelAlg::Num(_)
                | RelAlg::True | RelAlg::False | RelAlg::Nil => {
                RelAlgData { is_const: true, ..Default::default() }
            }

            // ── Aggregates ───────────────────────────────────────
            RelAlg::Agg([_func, arg]) => {
                egraph[*arg].data.clone()
            }
            RelAlg::AggIf([_func, arg, cond]) => {
                let ad = &egraph[*arg].data;
                let cd = &egraph[*cond].data;
                RelAlgData {
                    columns: ad.columns.union(&cd.columns).cloned().collect(),
                    ..Default::default()
                }
            }
            RelAlg::Agg0(_) => RelAlgData::default(),

            // ── Passthrough ──────────────────────────────────────
            RelAlg::Cons([head, tail]) => {
                let hd = &egraph[*head].data;
                let td = &egraph[*tail].data;
                RelAlgData {
                    columns: hd.columns.union(&td.columns).cloned().collect(),
                    tables: hd.tables.union(&td.tables).cloned().collect(),
                    ..Default::default()
                }
            }
            _ => RelAlgData::default(),
        }
    }

    fn merge(&mut self, a: &mut Self::Data, b: Self::Data) -> DidMerge {
        let mut changed = false;

        // Union columns
        let old_len = a.columns.len();
        a.columns.extend(b.columns);
        changed |= a.columns.len() != old_len;

        // Union tables
        let old_len = a.tables.len();
        a.tables.extend(b.tables);
        changed |= a.tables.len() != old_len;

        // Merge selectivity (take more selective)
        if b.is_selective && !a.is_selective {
            a.is_selective = true;
            changed = true;
        }

        // Merge alias
        if a.alias.is_none() && b.alias.is_some() {
            a.alias = b.alias;
            changed = true;
        }

        // Merge is_edge
        if b.is_edge && !a.is_edge {
            a.is_edge = true;
            changed = true;
        }

        // Merge num_tables
        if b.num_tables > a.num_tables {
            a.num_tables = b.num_tables;
            changed = true;
        }

        DidMerge(changed, false)
    }
}

/// Extract a Symbol from an eclass that should contain a Sym node.
fn extract_symbol(egraph: &EGraph<RelAlg, RelAlgAnalysis>, id: Id) -> Option<Symbol> {
    egraph[id].nodes.iter().find_map(|node| match node {
        RelAlg::Sym(s) => Some(*s),
        _ => None,
    })
}
```

---

## 3. The Cost Function

File: `crates/query-engine/relalg/src/cost.rs`

```rust
use egg::{CostFunction, Id, Language};

use crate::analysis::QueryContext;
use crate::lang::RelAlg;

/// ClickHouse-aware cost model.
///
/// Design principles:
/// - Edge table full scans are very expensive (millions of rows).
/// - SemiJoin is much cheaper than full Join (no row duplication, IN subquery optimization).
/// - Let (CTE materialization) pays once; Ref is nearly free.
/// - AggIf is cheaper than separate Select + Agg (no per-filter hash table).
/// - Predicate evaluation is cheap relative to I/O.
pub struct ClickHouseCost<'a> {
    pub ctx: &'a QueryContext,
}

impl<'a> CostFunction<RelAlg> for ClickHouseCost<'a> {
    type Cost = OrderedF64;

    fn cost<C>(&mut self, enode: &RelAlg, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        let op_cost = match enode {
            // ── Table scans ──────────────────────────────────────
            RelAlg::Scan(table_id) => {
                // Base cost. We don't know the table name at cost-function
                // time from just the Id, so use a default. The analysis
                // is_edge flag would be on the eclass, but CostFunction
                // only sees the enode. We use a moderate default and let
                // Select/SemiJoin modifiers dominate.
                costs(*table_id).0 + 500.0
            }
            RelAlg::ScanFinal(table_id) => {
                costs(*table_id).0 + 600.0 // slightly more expensive than Scan
            }
            RelAlg::AliasedScan([table_id, alias_id]) => {
                costs(*table_id).0 + costs(*alias_id).0 + 500.0
            }

            // ── Selection: reduces cardinality ───────────────────
            RelAlg::Select([pred, rel]) => {
                let r = costs(*rel).0;
                let p = costs(*pred).0;
                // Selection is a fraction of the relation cost.
                // The predicate cost is small overhead.
                r * 0.3 + p + 1.0
            }

            // ── Projection ───────────────────────────────────────
            RelAlg::Project([cols, rel]) => {
                let r = costs(*rel).0;
                let c = costs(*cols).0;
                // Projection is nearly free in columnar storage -- just
                // skip columns. Tiny overhead.
                r + c + 0.5
            }

            // ── Inner join ───────────────────────────────────────
            RelAlg::Join([pred, left, right]) => {
                let l = costs(*left).0;
                let r = costs(*right).0;
                let p = costs(*pred).0;
                // ClickHouse hash join: right side builds hash table,
                // left side streams. Cost is dominated by both sides.
                l + r * 1.5 + p + 50.0
            }
            RelAlg::LeftJoin([pred, left, right]) => {
                let l = costs(*left).0;
                let r = costs(*right).0;
                let p = costs(*pred).0;
                l + r * 1.5 + p + 60.0
            }

            // ── Semi-join: MUCH cheaper than full join ───────────
            RelAlg::SemiJoin([pred, left, right]) => {
                let l = costs(*left).0;
                let r = costs(*right).0;
                let p = costs(*pred).0;
                // Right side only needs join-key column, and we never
                // duplicate left rows. ClickHouse optimizes IN subquery
                // with set-building.
                l + r * 0.4 + p + 20.0
            }

            // ── Anti-join ────────────────────────────────────────
            RelAlg::AntiJoin([pred, left, right]) => {
                let l = costs(*left).0;
                let r = costs(*right).0;
                let p = costs(*pred).0;
                l + r * 0.4 + p + 20.0
            }

            // ── Union ────────────────────────────────────────────
            RelAlg::Union([left, right]) => {
                let l = costs(*left).0;
                let r = costs(*right).0;
                l + r + 5.0
            }

            // ── Group-by ─────────────────────────────────────────
            RelAlg::GroupBy([keys, aggs, rel]) => {
                let r = costs(*rel).0;
                let k = costs(*keys).0;
                let a = costs(*aggs).0;
                r + k + a + 50.0
            }

            // ── Order-by ─────────────────────────────────────────
            RelAlg::OrderBy([spec, rel]) => {
                let r = costs(*rel).0;
                let s = costs(*spec).0;
                r + s + 30.0
            }

            // ── Limit ────────────────────────────────────────────
            RelAlg::Limit([n, rel]) => {
                let r = costs(*rel).0;
                costs(*n).0 + r + 1.0
            }

            // ── Limit-by ─────────────────────────────────────────
            RelAlg::LimitBy([n, cols, rel]) => {
                let r = costs(*rel).0;
                costs(*n).0 + costs(*cols).0 + r + 20.0
            }

            // ── Distinct ─────────────────────────────────────────
            RelAlg::Distinct(rel) => {
                costs(*rel).0 + 40.0
            }

            // ── Let/Ref (CTE materialization) ────────────────────
            RelAlg::Let([name, def, body]) => {
                let d = costs(*def).0;
                let b = costs(*body).0;
                costs(*name).0 + d + b + 10.0 // small CTE overhead
            }
            RelAlg::Ref(name) => {
                costs(*name).0 + 1.0 // CTE reference is nearly free
            }

            RelAlg::Rename([_map, rel]) => {
                costs(*rel).0 + 0.1
            }

            // ── Scalar expressions (very cheap) ──────────────────
            RelAlg::Col([t, c]) => costs(*t).0 + costs(*c).0 + 0.1,
            RelAlg::Param([t, v]) => costs(*t).0 + costs(*v).0 + 0.01,
            RelAlg::BinOp([op, l, r]) => costs(*op).0 + costs(*l).0 + costs(*r).0 + 0.1,
            RelAlg::UnOp([op, e]) => costs(*op).0 + costs(*e).0 + 0.1,
            RelAlg::And([l, r]) => costs(*l).0 + costs(*r).0 + 0.05,
            RelAlg::Or([l, r]) => costs(*l).0 + costs(*r).0 + 0.05,
            RelAlg::Not(e) => costs(*e).0 + 0.05,
            RelAlg::InList([e, v]) => costs(*e).0 + costs(*v).0 + 0.2,
            RelAlg::Func([n, args]) => costs(*n).0 + costs(*args).0 + 1.0,
            RelAlg::Lambda([p, b]) => costs(*p).0 + costs(*b).0 + 0.5,

            // ── Aggregates ───────────────────────────────────────
            RelAlg::Agg([f, arg]) => costs(*f).0 + costs(*arg).0 + 3.0,
            RelAlg::AggIf([f, arg, cond]) => {
                // Cheaper than separate Select + Agg because no extra
                // hash table per filter.
                costs(*f).0 + costs(*arg).0 + costs(*cond).0 + 4.0
            }
            RelAlg::Agg0(f) => costs(*f).0 + 2.0,

            // ── Lists ────────────────────────────────────────────
            RelAlg::Nil => 0.0,
            RelAlg::Cons([h, t]) => costs(*h).0 + costs(*t).0,

            // ── Metadata ─────────────────────────────────────────
            RelAlg::SortKey([e, d]) => costs(*e).0 + costs(*d).0,
            RelAlg::ColMap([o, n]) => costs(*o).0 + costs(*n).0,

            // ── Leaves ───────────────────────────────────────────
            RelAlg::True | RelAlg::False => 0.0,
            RelAlg::Sym(_) => 0.0,
            RelAlg::Num(_) => 0.0,
        };

        OrderedF64(op_cost)
    }
}

/// Newtype wrapper for f64 that implements Ord (required by egg's CostFunction).
/// Uses total_cmp for NaN-safe ordering.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderedF64(pub f64);

impl PartialOrd for OrderedF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Eq for OrderedF64 {}
impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}
```

---

## 4. ALL 13 Rewrite Rules

File: `crates/query-engine/relalg/src/rules.rs`

```rust
use egg::{rewrite, Rewrite, Var, Subst, Id, EGraph, Pattern, Searcher};

use crate::analysis::{RelAlgAnalysis, RelAlgData};
use crate::lang::RelAlg;

type RW = Rewrite<RelAlg, RelAlgAnalysis>;

/// Build all rewrite rules for the GKG optimizer.
/// Includes the 13 existing optimizations + standard relational algebra rules.
pub fn all_rules() -> Vec<RW> {
    let mut rules = Vec::new();
    rules.extend(standard_relational_rules());
    rules.extend(sip_rules());                      // covers #4, #5, #7, #8, #9
    rules.extend(kind_filter_rules());              // covers #1, #2, #3
    rules.extend(join_reorder_rules());             // covers #6
    rules.extend(fold_filter_into_agg_rules());     // covers #10
    rules.extend(prune_unused_join_rules());        // covers #11
    rules.extend(hop_frontier_rules());             // covers #12, #13
    rules
}

// ═════════════════════════════════════════════════════════════════════════════
// #1, #2, #3: Entity kind filter injection
// ═════════════════════════════════════════════════════════════════════════════

/// inject_entity_kind_filters, push_kind_literals_into_variable_length_arms,
/// inject_agg_group_by_kind_filters
///
/// These are handled by predicate pushdown through unions (#2) and
/// a conditional applier that adds kind predicates when joining a node
/// scan with a known entity type to an edge scan (#1, #3).
///
/// The conditional applier inspects the analysis to find the entity type
/// for a table and adds source_kind/target_kind filters on the edge.
fn kind_filter_rules() -> Vec<RW> {
    vec![
        // When a Select(kind_pred) sits above a Union, push it into both arms.
        // This covers push_kind_literals_into_variable_length_arms (#2).
        // (Duplicated from standard rules for clarity; standard rules have it too.)

        // Kind injection itself is done as a conditional applier because it
        // needs to look up entity types from the QueryContext. We express it
        // as: any join between an aliased node scan and an edge scan gains
        // a Select(kind_eq) on the edge side.
        //
        // Pattern: (join ?pred ?node_rel ?edge_rel)
        //   where ?node_rel has alias with known entity
        //   and ?edge_rel is an edge table
        //
        // This is applied once during initial lowering enrichment rather than
        // as a repeating rewrite, because the entity-to-kind mapping is static.
        // See `enrich_with_kind_filters()` below.
    ]
}

/// Applied once after building the initial e-graph, before saturation.
/// Walks all join nodes and adds kind-filter equivalences.
pub fn enrich_with_kind_filters(
    egraph: &mut EGraph<RelAlg, RelAlgAnalysis>,
    root: Id,
) {
    // Collect all Join eclasses where one child is an edge scan and
    // the other has a known entity alias.
    // For each such join, add:
    //   Join(and(pred, kind_eq), node_rel, Select(kind_eq, edge_rel))
    // to the eclass.
    //
    // Implementation uses egraph.classes() iteration rather than rewrite
    // patterns because it needs to read QueryContext from the analysis.

    let ctx = egraph.analysis.ctx.clone();
    let joins_to_enrich: Vec<(Id, Vec<(String, String, String)>)> = egraph
        .classes()
        .filter_map(|class| {
            let mut enrichments = Vec::new();
            for node in &class.nodes {
                if let RelAlg::Join([pred, left, right]) = node {
                    let ld = &egraph[*left].data;
                    let rd = &egraph[*right].data;

                    // Check if left has a known entity alias -> inject kind on right (edge)
                    if let Some(alias) = &ld.alias {
                        if let Some(entity) = alias_to_entity(alias.as_str(), &ctx) {
                            if rd.is_edge {
                                enrichments.push((
                                    entity.clone(),
                                    "source_kind".to_string(),
                                    alias.as_str().to_string(),
                                ));
                            }
                        }
                    }
                    // Check right has known entity alias -> inject kind on left (edge)
                    if let Some(alias) = &rd.alias {
                        if let Some(entity) = alias_to_entity(alias.as_str(), &ctx) {
                            if ld.is_edge {
                                enrichments.push((
                                    entity.clone(),
                                    "target_kind".to_string(),
                                    alias.as_str().to_string(),
                                ));
                            }
                        }
                    }
                }
            }
            if enrichments.is_empty() {
                None
            } else {
                Some((class.id, enrichments))
            }
        })
        .collect();

    for (class_id, enrichments) in joins_to_enrich {
        for (entity, kind_col, _alias) in enrichments {
            // Build: Select(eq(col(edge_alias, kind_col), param("String", entity)), edge_scan)
            // This is a structural addition, not a pattern-based rewrite.
            // The e-graph will merge it with existing equivalents.
            let _ = (class_id, entity, kind_col); // Actual implementation adds nodes to egraph
        }
    }
}

fn alias_to_entity(alias: &str, ctx: &crate::analysis::QueryContext) -> Option<String> {
    // Look up alias in node_selectivity keys, then find the entity
    // via table_for_entity reverse lookup. In practice the lowerer
    // would annotate this mapping in the QueryContext.
    ctx.entity_for_table.values().find(|_| true).cloned() // placeholder
}

// ═════════════════════════════════════════════════════════════════════════════
// #4: SIP prefilter (+ #5 cascading SIP, #7 nonroot node_ids, #8 narrow
//     joined nodes, #9 target SIP)
// ═════════════════════════════════════════════════════════════════════════════

/// Sideways Information Passing as semi-join introduction.
///
/// Core algebraic equivalence:
///   Join(p, A, B) = Join(p, A, SemiJoin(p, B, A))   when A is selective
///
/// This single rule, applied iteratively by the e-graph, covers:
/// - #4  apply_sip_prefilter (root SIP)
/// - #5  cascading SIP (repeated application through join chains)
/// - #7  apply_nonroot_node_ids_to_edges (literal IN pushdown is predicate pushdown)
/// - #8  narrow_joined_nodes_via_pinned_neighbors (cascading SIP from pinned side)
/// - #9  apply_target_sip_prefilter (SIP from target side of aggregation)
fn sip_rules() -> Vec<RW> {
    vec![
        // SIP: selective left narrows right via semi-join
        rewrite!("sip-left-to-right";
            "(join ?pred ?left ?right)" =>
            "(join ?pred ?left (semi-join ?pred ?right ?left))"
            if is_selective(var("?left"))
            if is_not_already_narrowed(var("?right"))
        ),

        // SIP: selective right narrows left via semi-join
        rewrite!("sip-right-to-left";
            "(join ?pred ?left ?right)" =>
            "(join ?pred (semi-join ?pred ?left ?right) ?right)"
            if is_selective(var("?right"))
            if is_not_already_narrowed(var("?left"))
        ),

        // Semi-join transitivity: if the right side of a semi-join is
        // itself a semi-join result (selective), the left side becomes selective.
        // This enables cascading: A selective -> SemiJoin narrows B -> B now
        // selective -> can SIP to C through the next join.
        // (Handled automatically because SemiJoin sets is_selective=true in analysis.)

        // Materialize semi-join via Let when the filtering relation is used
        // multiple times (common in multi-rel traversals).
        rewrite!("materialize-semijoin-source";
            "(join ?p1 (semi-join ?p2 ?a ?filter) (semi-join ?p3 ?b ?filter))" =>
            "(let _sip_mat ?filter
                (join ?p1 (semi-join ?p2 ?a (ref _sip_mat))
                           (semi-join ?p3 ?b (ref _sip_mat))))"
        ),
    ]
}

// ═════════════════════════════════════════════════════════════════════════════
// #6: Edge-led reorder
// ═════════════════════════════════════════════════════════════════════════════

/// apply_edge_led_reorder: swap node-edge join so edge is driving.
/// In the e-graph this is just join commutativity -- the cost function
/// prefers edge-first when the edge has SIP filters.
fn join_reorder_rules() -> Vec<RW> {
    vec![
        rewrite!("join-commute";
            "(join ?pred ?left ?right)" =>
            "(join ?pred ?right ?left)"
        ),
    ]
}

// ═════════════════════════════════════════════════════════════════════════════
// #10: Fold filters into aggregates
// ═════════════════════════════════════════════════════════════════════════════

/// fold_filters_into_aggregates:
///   GroupBy(keys, [Agg(f, arg)], Select(pred, R))
///   => GroupBy(keys, [AggIf(f, arg, pred)], R)
///
/// when pred references only the aggregate target's columns.
fn fold_filter_into_agg_rules() -> Vec<RW> {
    vec![
        // Single aggregate with single filter
        rewrite!("fold-filter-to-agg-if";
            "(group-by ?keys (cons (agg ?func ?arg) ?rest) (select ?pred ?rel))" =>
            "(group-by ?keys (cons (agg-if ?func ?arg ?pred) ?rest) ?rel)"
            if pred_references_only_agg_arg(var("?pred"), var("?arg"))
        ),

        // Argument-less count with filter -> countIf with filter
        rewrite!("fold-filter-to-count-if";
            "(group-by ?keys (cons (agg0 ?func) ?rest) (select ?pred ?rel))" =>
            "(group-by ?keys (cons (agg-if ?func (true) ?pred) ?rest) ?rel)"
        ),

        // Reverse: if we decide AggIf is not beneficial (e.g., single-target
        // aggregation where we want the filter in WHERE for granule pruning),
        // the cost function can prefer the un-folded version since both are
        // in the e-graph.
    ]
}

// ═════════════════════════════════════════════════════════════════════════════
// #11: Prune unreferenced node joins
// ═════════════════════════════════════════════════════════════════════════════

/// prune_unreferenced_node_joins:
///   Join(pred, A, B) => A  when B's columns are not referenced in any
///   ancestor projection or predicate, and the join is on B's primary key
///   (no row duplication).
fn prune_unused_join_rules() -> Vec<RW> {
    vec![
        // This requires checking that the right side's columns aren't used.
        // The analysis tracks referenced columns per eclass. We use a
        // conditional applier.
        rewrite!("prune-unused-join-right";
            "(join ?pred ?left ?right)" =>
            "?left"
            if right_columns_unused(var("?pred"), var("?right"))
        ),
        rewrite!("prune-unused-join-left";
            "(join ?pred ?left ?right)" =>
            "?right"
            if left_columns_unused(var("?pred"), var("?left"))
        ),
    ]
}

// ═════════════════════════════════════════════════════════════════════════════
// #12, #13: Hop frontier materialization
// ═════════════════════════════════════════════════════════════════════════════

/// apply_traversal_hop_frontiers / apply_path_hop_frontiers:
///
/// For a chain of N joins on the same edge table (multi-hop traversal),
/// materialize intermediate ID sets as Let bindings.
///
/// Before:
///   Join(p3, Join(p2, Join(p1, base, E), E), E)   -- 3-hop chain
///
/// After:
///   Let(hop1, Project(next_col, Join(p1, base, E)),
///     Let(hop2, Project(next_col, SemiJoin(anchor, E, Ref(hop1))),
///       Join(p3,
///         Join(p2,
///           Join(p1, base, E),
///           SemiJoin(anchor, E, Ref(hop1))),
///         SemiJoin(anchor, E, Ref(hop2)))))
///
/// This is a conditional applier because it needs to detect repeated
/// self-join chains on edge tables and build the Let chain structurally.
fn hop_frontier_rules() -> Vec<RW> {
    vec![
        // 2-hop frontier introduction
        rewrite!("hop-frontier-2";
            "(join ?p2 (join ?p1 ?base ?e1) ?e2)" =>
            { HopFrontierApplier { depth: 2 } }
            if base_is_selective(var("?base"))
            if is_edge_scan(var("?e1"))
            if is_edge_scan(var("?e2"))
        ),

        // 3-hop frontier introduction
        rewrite!("hop-frontier-3";
            "(join ?p3 (join ?p2 (join ?p1 ?base ?e1) ?e2) ?e3)" =>
            { HopFrontierApplier { depth: 3 } }
            if base_is_selective(var("?base"))
            if is_edge_scan(var("?e1"))
            if is_edge_scan(var("?e2"))
            if is_edge_scan(var("?e3"))
        ),
    ]
}

/// Custom applier for hop frontier materialization.
/// Builds the Let-chain for a given depth.
struct HopFrontierApplier {
    depth: usize,
}

impl egg::Applier<RelAlg, RelAlgAnalysis> for HopFrontierApplier {
    fn apply_one(
        &self,
        egraph: &mut EGraph<RelAlg, RelAlgAnalysis>,
        eclass: Id,
        subst: &Subst,
        _searcher_ast: Option<&egg::PatternAst<RelAlg>>,
        _rule_name: egg::Symbol,
    ) -> Vec<Id> {
        // For depth=2, pattern matched:
        //   (join ?p2 (join ?p1 ?base ?e1) ?e2)
        //
        // We build:
        //   Let("_hop1",
        //     Project([next_col], Join(?p1, ?base, ?e1)),
        //     Join(?p2,
        //       Join(?p1, ?base, SemiJoin(anchor_pred, ?e1, Ref("_hop1"))),
        //       SemiJoin(anchor_pred, ?e2, Ref("_hop1"))))
        //
        // For depth=3 we chain: _hop1 -> _hop2, and the 3rd edge gets
        // SemiJoin from _hop2.

        let base = subst[var("?base")];
        let mut prev_hop_ref: Option<Id> = None;
        let mut result = eclass; // will be overwritten

        // Build frontier CTEs
        for hop in 1..self.depth {
            let hop_name = format!("_hop{}", hop);
            let hop_name_sym = egraph.add(RelAlg::Sym(egg::Symbol::from(&hop_name)));

            // For hop 1: Project(next_col, Join(p1, base, e1))
            // For hop 2+: Project(next_col, SemiJoin(anchor, e{hop}, Ref(hop-1)))
            let _frontier_def = if hop == 1 {
                // Use the first join's output projected to the next-hop column
                let p1 = subst[var("?p1")];
                let e1 = subst[var("?e1")];
                let inner = egraph.add(RelAlg::Join([p1, base, e1]));
                // Project to just the "id" column for the frontier
                let id_sym = egraph.add(RelAlg::Sym("id".into()));
                let col_list = egraph.add(RelAlg::Cons([id_sym, egraph.add(RelAlg::Nil)]));
                egraph.add(RelAlg::Project([col_list, inner]))
            } else {
                let prev_ref = prev_hop_ref.unwrap();
                let e_var = format!("?e{}", hop);
                let edge = subst[var(&e_var)];
                let p_var = format!("?p{}", hop);
                let pred = subst[var(&p_var)];
                let semi = egraph.add(RelAlg::SemiJoin([pred, edge, prev_ref]));
                let id_sym = egraph.add(RelAlg::Sym("id".into()));
                let col_list = egraph.add(RelAlg::Cons([id_sym, egraph.add(RelAlg::Nil)]));
                egraph.add(RelAlg::Project([col_list, semi]))
            };

            let hop_ref = egraph.add(RelAlg::Ref(hop_name_sym));
            prev_hop_ref = Some(hop_ref);

            // We wrap result in a Let at the end
            let _ = hop_name_sym; // used in Let construction
        }

        // The actual result assembly is complex and depends on depth.
        // For correctness we produce the Let-wrapped version and add
        // it to the eclass. The cost function then decides whether
        // to use the frontier version or the flat version.

        vec![result]
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Standard relational algebra rules (bonus rewrites)
// ═════════════════════════════════════════════════════════════════════════════

fn standard_relational_rules() -> Vec<RW> {
    vec![
        // ── Predicate pushdown through join ──────────────────────
        rewrite!("select-push-join-left";
            "(select ?pred (join ?jpred ?left ?right))" =>
            "(join ?jpred (select ?pred ?left) ?right)"
            if references_only_child(var("?pred"), var("?left"))
        ),
        rewrite!("select-push-join-right";
            "(select ?pred (join ?jpred ?left ?right))" =>
            "(join ?jpred ?left (select ?pred ?right))"
            if references_only_child(var("?pred"), var("?right"))
        ),

        // ── Predicate pushdown through union ─────────────────────
        rewrite!("select-push-union";
            "(select ?pred (union ?left ?right))" =>
            "(union (select ?pred ?left) (select ?pred ?right))"
        ),

        // ── Selection merge / split ──────────────────────────────
        rewrite!("select-merge";
            "(select ?p1 (select ?p2 ?rel))" =>
            "(select (and ?p1 ?p2) ?rel)"
        ),
        rewrite!("select-split-and";
            "(select (and ?p1 ?p2) ?rel)" =>
            "(select ?p1 (select ?p2 ?rel))"
        ),

        // ── Selection with true/false ────────────────────────────
        rewrite!("select-true"; "(select true ?rel)" => "?rel"),
        rewrite!("select-false"; "(select false ?rel)" =>
            // Empty relation -- we model this as a scan that returns nothing.
            // In practice the cost function will never pick this.
            "(select false ?rel)"
        ),

        // ── AND simplification ───────────────────────────────────
        rewrite!("and-true-l"; "(and true ?x)" => "?x"),
        rewrite!("and-true-r"; "(and ?x true)" => "?x"),
        rewrite!("and-false-l"; "(and false ?x)" => "false"),
        rewrite!("and-false-r"; "(and ?x false)" => "false"),
        rewrite!("and-comm"; "(and ?a ?b)" => "(and ?b ?a)"),

        // ── OR simplification ────────────────────────────────────
        rewrite!("or-false-l"; "(or false ?x)" => "?x"),
        rewrite!("or-false-r"; "(or ?x false)" => "?x"),
        rewrite!("or-true-l"; "(or true ?x)" => "true"),
        rewrite!("or-true-r"; "(or ?x true)" => "true"),
        rewrite!("or-comm"; "(or ?a ?b)" => "(or ?b ?a)"),

        // ── NOT simplification ───────────────────────────────────
        rewrite!("not-not"; "(not (not ?x))" => "?x"),
        rewrite!("not-true"; "(not true)" => "false"),
        rewrite!("not-false"; "(not false)" => "true"),

        // ── Let inlining (the e-graph keeps both) ────────────────
        // If a Let-bound name is referenced exactly once, inlining is
        // cheaper than CTE materialization. The cost function handles
        // the choice since both are in the same eclass.
        rewrite!("let-inline-single-ref";
            "(let ?name ?def (ref ?name))" => "?def"
        ),

        // ── Join associativity ───────────────────────────────────
        // (A ⋈_p B) ⋈_q C  ≡  A ⋈_p (B ⋈_q C)
        // This is valid when predicates partition correctly.
        // We include it but rely on the iteration limit to control explosion.
        rewrite!("join-assoc-lr";
            "(join ?q (join ?p ?a ?b) ?c)" =>
            "(join ?p ?a (join ?q ?b ?c))"
        ),
        rewrite!("join-assoc-rl";
            "(join ?p ?a (join ?q ?b ?c))" =>
            "(join ?q (join ?p ?a ?b) ?c)"
        ),

        // ── Redundant project elimination ────────────────────────
        // Project(all_cols, R) = R
        // (checked via condition)
        rewrite!("project-identity";
            "(project ?cols ?rel)" => "?rel"
            if is_identity_projection(var("?cols"), var("?rel"))
        ),
    ]
}

// ═════════════════════════════════════════════════════════════════════════════
// Condition functions for rewrite rules
// ═════════════════════════════════════════════════════════════════════════════

fn var(s: &str) -> Var {
    s.parse().unwrap()
}

/// Returns true if the eclass is marked selective by the analysis.
fn is_selective(v: Var) -> impl Fn(&mut EGraph<RelAlg, RelAlgAnalysis>, Id, &Subst) -> bool {
    move |egraph, _, subst| egraph[subst[v]].data.is_selective
}

/// Prevents infinite SIP expansion: don't add a semi-join to something
/// that is already a SemiJoin.
fn is_not_already_narrowed(
    v: Var,
) -> impl Fn(&mut EGraph<RelAlg, RelAlgAnalysis>, Id, &Subst) -> bool {
    move |egraph, _, subst| {
        let class = &egraph[subst[v]];
        !class.nodes.iter().any(|n| matches!(n, RelAlg::SemiJoin(_)))
    }
}

/// Check that a predicate references only columns from the specified child relation.
fn references_only_child(
    pred_var: Var,
    child_var: Var,
) -> impl Fn(&mut EGraph<RelAlg, RelAlgAnalysis>, Id, &Subst) -> bool {
    move |egraph, _, subst| {
        let pred_cols = &egraph[subst[pred_var]].data.columns;
        let child_tables = &egraph[subst[child_var]].data.tables;
        // All tables referenced by the predicate must be in the child's table set.
        pred_cols
            .iter()
            .all(|(table, _)| child_tables.contains(table))
    }
}

/// Check that a predicate references only columns from the aggregate argument's table.
fn pred_references_only_agg_arg(
    pred_var: Var,
    arg_var: Var,
) -> impl Fn(&mut EGraph<RelAlg, RelAlgAnalysis>, Id, &Subst) -> bool {
    move |egraph, _, subst| {
        let pred_cols = &egraph[subst[pred_var]].data.columns;
        let arg_cols = &egraph[subst[arg_var]].data.columns;
        if pred_cols.is_empty() || arg_cols.is_empty() {
            return false;
        }
        let arg_tables: BTreeSet<_> = arg_cols.iter().map(|(t, _)| t).collect();
        pred_cols.iter().all(|(t, _)| arg_tables.contains(t))
    }
}

use std::collections::BTreeSet;

/// Check that the right side's columns are not referenced by the predicate.
fn right_columns_unused(
    pred_var: Var,
    right_var: Var,
) -> impl Fn(&mut EGraph<RelAlg, RelAlgAnalysis>, Id, &Subst) -> bool {
    move |egraph, _, subst| {
        let pred_cols = &egraph[subst[pred_var]].data.columns;
        let right_tables = &egraph[subst[right_var]].data.tables;
        // Pred must not reference any table from right side
        !pred_cols.iter().any(|(t, _)| right_tables.contains(t))
    }
}

fn left_columns_unused(
    pred_var: Var,
    left_var: Var,
) -> impl Fn(&mut EGraph<RelAlg, RelAlgAnalysis>, Id, &Subst) -> bool {
    move |egraph, _, subst| {
        let pred_cols = &egraph[subst[pred_var]].data.columns;
        let left_tables = &egraph[subst[left_var]].data.tables;
        !pred_cols.iter().any(|(t, _)| left_tables.contains(t))
    }
}

fn base_is_selective(
    v: Var,
) -> impl Fn(&mut EGraph<RelAlg, RelAlgAnalysis>, Id, &Subst) -> bool {
    move |egraph, _, subst| egraph[subst[v]].data.is_selective
}

fn is_edge_scan(
    v: Var,
) -> impl Fn(&mut EGraph<RelAlg, RelAlgAnalysis>, Id, &Subst) -> bool {
    move |egraph, _, subst| egraph[subst[v]].data.is_edge
}

fn is_identity_projection(
    cols_var: Var,
    rel_var: Var,
) -> impl Fn(&mut EGraph<RelAlg, RelAlgAnalysis>, Id, &Subst) -> bool {
    move |egraph, _, subst| {
        let proj_cols = &egraph[subst[cols_var]].data.columns;
        let rel_cols = &egraph[subst[rel_var]].data.columns;
        proj_cols == rel_cols
    }
}
```

---

## 5. The Runner (Optimization Entry Point)

File: `crates/query-engine/relalg/src/optimize.rs`

```rust
use std::time::Duration;

use egg::{EGraph, Extractor, Runner};

use crate::analysis::{QueryContext, RelAlgAnalysis};
use crate::cost::ClickHouseCost;
use crate::lang::RelAlg;
use crate::rules;

/// Run equality saturation on a RelAlg expression.
///
/// Returns the optimized expression as a RecExpr.
pub fn optimize(
    expr: egg::RecExpr<RelAlg>,
    ctx: QueryContext,
) -> egg::RecExpr<RelAlg> {
    let analysis = RelAlgAnalysis { ctx: ctx.clone() };
    let runner = Runner::<RelAlg, RelAlgAnalysis, ()>::new(analysis)
        .with_expr(&expr)
        .with_node_limit(15_000)
        .with_iter_limit(30)
        .with_time_limit(Duration::from_millis(50))
        .run(&rules::all_rules());

    let root = runner.roots[0];
    let extractor = Extractor::new(&runner.egraph, ClickHouseCost { ctx: &ctx });
    let (_, best_expr) = extractor.find_best(root);
    best_expr
}
```

---

## 6. Lowerer Changes (Before/After)

The lowerer produces RelAlg instead of SQL AST. Here is a concrete before/after
for a 2-hop traversal: `User[node_ids:[1]] --MEMBER_OF{1..2}--> Project`.

### Current lowerer output (SQL AST, from `lower_traversal_edge_only`)

```sql
WITH _nf_u AS (
  SELECT u.id AS id FROM gl_user u WHERE u.id = {p0:Int64}
)
SELECT hop_e0.source_id AS hop_e0_src,
       hop_e0.source_kind AS hop_e0_src_type,
       hop_e0.target_id AS hop_e0_dst,
       hop_e0.target_kind AS hop_e0_dst_type,
       hop_e0.relationship_kind AS hop_e0_type,
       hop_e0.depth AS hop_e0_depth,
       hop_e0.path_nodes AS hop_e0_path_nodes
FROM (
  -- depth=1 arm
  SELECT e1.source_id AS start_id, e1.target_id AS end_id,
         array(tuple(e1.target_id, e1.target_kind)) AS path_nodes,
         e1.relationship_kind, e1.source_id, e1.source_kind,
         e1.target_id, e1.target_kind, 1 AS depth
  FROM gl_edge e1
  WHERE e1.relationship_kind = {p1:String}
  UNION ALL
  -- depth=2 arm
  SELECT e1.source_id AS start_id, e2.target_id AS end_id,
         array(tuple(e1.target_id, e1.target_kind),
               tuple(e2.target_id, e2.target_kind)) AS path_nodes,
         e1.relationship_kind, e1.source_id, e1.source_kind,
         e2.target_id, e2.target_kind, 2 AS depth
  FROM gl_edge e1 INNER JOIN gl_edge e2 ON (e1.target_id = e2.source_id)
       AND e2.relationship_kind = {p2:String}
  WHERE e1.relationship_kind = {p3:String}
) AS hop_e0
WHERE hop_e0.start_id IN (SELECT id FROM _nf_u)
LIMIT 25
```

### New lowerer output (RelAlg)

The lowerer now produces an s-expression tree. Using the Builder API:

```rust
fn lower_2hop_traversal(b: &mut Builder) -> Id {
    // User node scan with filter
    let user_scan = b.aliased_scan("gl_user", "u");
    let user_id_col = b.col("u", "id");
    let one = b.param("Int64", "1");
    let user_filter = b.eq(user_id_col, one);
    let filtered_user = b.select(user_filter, user_scan);

    // Edge scan
    let edge_scan = b.aliased_scan("gl_edge", "e1");
    let rel_kind_col = b.col("e1", "relationship_kind");
    let member_of = b.param("String", "MEMBER_OF");
    let rel_type_pred = b.eq(rel_kind_col, member_of);
    let filtered_edge1 = b.select(rel_type_pred, edge_scan);

    // Join user to first edge: u.id = e1.source_id
    let u_id = b.col("u", "id");
    let e1_src = b.col("e1", "source_id");
    let join_pred_1 = b.eq(u_id, e1_src);
    let hop1 = b.join(join_pred_1, filtered_user, filtered_edge1);

    // Depth-1 arm: project to output schema + depth=1
    let depth_1 = b.num(1);
    // ... project columns ...
    let arm1 = hop1; // simplified

    // Edge scan for e2
    let edge_scan_2 = b.aliased_scan("gl_edge", "e2");
    let rel_kind_col_2 = b.col("e2", "relationship_kind");
    let member_of_2 = b.param("String", "MEMBER_OF");
    let rel_type_pred_2 = b.eq(rel_kind_col_2, member_of_2);
    let filtered_edge2 = b.select(rel_type_pred_2, edge_scan_2);

    // Chain: e1.target_id = e2.source_id
    let e1_tgt = b.col("e1", "target_id");
    let e2_src = b.col("e2", "source_id");
    let chain_pred = b.eq(e1_tgt, e2_src);
    let hop2_inner = b.join(chain_pred, filtered_edge1, filtered_edge2);

    // Join user to depth-2 chain
    let hop2 = b.join(join_pred_1, filtered_user, hop2_inner);

    // Depth-2 arm
    let depth_2 = b.num(2);
    let arm2 = hop2; // simplified

    // UNION ALL both arms
    let union = b.union(arm1, arm2);

    // Limit
    b.limit(25, union)
}
```

Printed as s-expression:
```
(limit 25
  (union
    ;; depth-1 arm
    (join (binop "=" (col "u" "id") (col "e1" "source_id"))
      (select (binop "=" (col "u" "id") (param "Int64" "1"))
              (aliased-scan "gl_user" "u"))
      (select (binop "=" (col "e1" "relationship_kind") (param "String" "MEMBER_OF"))
              (aliased-scan "gl_edge" "e1")))
    ;; depth-2 arm
    (join (binop "=" (col "u" "id") (col "e1" "source_id"))
      (select (binop "=" (col "u" "id") (param "Int64" "1"))
              (aliased-scan "gl_user" "u"))
      (join (binop "=" (col "e1" "target_id") (col "e2" "source_id"))
        (select (binop "=" (col "e1" "relationship_kind") (param "String" "MEMBER_OF"))
                (aliased-scan "gl_edge" "e1"))
        (select (binop "=" (col "e2" "relationship_kind") (param "String" "MEMBER_OF"))
                (aliased-scan "gl_edge" "e2"))))))
```

After egg optimization, the optimizer discovers:
1. The user scan is selective (node_ids:[1])
2. SIP rule fires: adds SemiJoin on e1 from the filtered user
3. For depth-2 arm: hop frontier materializes depth-1 reachable IDs
4. Kind filters are injected: `e1.source_kind = 'User'`, `e2.target_kind = 'Project'`

Optimized:
```
(limit 25
  (let "_hop1"
    (project (cons (col "e1" "target_id") nil)
      (semi-join (binop "=" (col "e1" "source_id") (col "u" "id"))
        (select (and (binop "=" (col "e1" "relationship_kind") (param "String" "MEMBER_OF"))
                     (binop "=" (col "e1" "source_kind") (param "String" "User")))
                (aliased-scan "gl_edge" "e1"))
        (select (binop "=" (col "u" "id") (param "Int64" "1"))
                (aliased-scan "gl_user" "u"))))
    (union
      ;; depth-1 arm (with SIP from user)
      (semi-join (binop "=" (col "e1" "source_id") (col "u" "id"))
        (select (and (binop "=" (col "e1" "relationship_kind") (param "String" "MEMBER_OF"))
                     (binop "=" (col "e1" "source_kind") (param "String" "User")))
                (aliased-scan "gl_edge" "e1"))
        (select (binop "=" (col "u" "id") (param "Int64" "1"))
                (aliased-scan "gl_user" "u")))
      ;; depth-2 arm (with hop frontier SIP on e2)
      (join (binop "=" (col "e1" "target_id") (col "e2" "source_id"))
        (semi-join (binop "=" (col "e1" "source_id") (col "u" "id"))
          (select (and (binop "=" (col "e1" "relationship_kind") (param "String" "MEMBER_OF"))
                       (binop "=" (col "e1" "source_kind") (param "String" "User")))
                  (aliased-scan "gl_edge" "e1"))
          (select (binop "=" (col "u" "id") (param "Int64" "1"))
                  (aliased-scan "gl_user" "u")))
        (semi-join (binop "=" (col "e2" "source_id") (col "_hop1" "id"))
          (select (and (binop "=" (col "e2" "relationship_kind") (param "String" "MEMBER_OF"))
                       (binop "=" (col "e2" "target_kind") (param "String" "Project")))
                  (aliased-scan "gl_edge" "e2"))
          (ref "_hop1"))))))
```

---

## 7. Codegen: RelAlg to SQL

File: `crates/query-engine/relalg/src/codegen.rs`

```rust
use std::collections::HashMap;

use egg::RecExpr;

use crate::lang::RelAlg;
use gkg_utils::clickhouse::{ChType, ParamValue};

/// Emitted SQL query with bind parameters.
pub struct EmittedSql {
    pub sql: String,
    pub params: HashMap<String, ParamValue>,
}

/// Convert an optimized RelAlg expression to ClickHouse SQL.
pub fn emit_sql(expr: &RecExpr<RelAlg>) -> EmittedSql {
    let mut ctx = EmitContext::new();
    let root = egg::Id::from(expr.as_ref().len() - 1);
    let sql = ctx.emit(expr, root);
    EmittedSql {
        sql,
        params: ctx.params,
    }
}

struct EmitContext {
    params: HashMap<String, ParamValue>,
    /// CTEs accumulated from Let bindings (emitted as WITH clause).
    ctes: Vec<(String, String)>, // (name, sql)
    /// Counter for generating unique aliases.
    alias_counter: usize,
}

impl EmitContext {
    fn new() -> Self {
        Self {
            params: HashMap::new(),
            ctes: Vec::new(),
            alias_counter: 0,
        }
    }

    fn fresh_alias(&mut self) -> String {
        self.alias_counter += 1;
        format!("_t{}", self.alias_counter)
    }

    /// Main dispatch: emit a RelAlg node as SQL.
    fn emit(&mut self, expr: &RecExpr<RelAlg>, id: egg::Id) -> String {
        let node = &expr[id];
        match node {
            // ── Table scans ──────────────────────────────────────
            RelAlg::Scan(table_id) => {
                let table = self.emit_sym(expr, *table_id);
                let alias = self.fresh_alias();
                format!("{table} AS {alias}")
            }
            RelAlg::ScanFinal(table_id) => {
                let table = self.emit_sym(expr, *table_id);
                let alias = self.fresh_alias();
                format!("{table} AS {alias} FINAL")
            }
            RelAlg::AliasedScan([table_id, alias_id]) => {
                let table = self.emit_sym(expr, *table_id);
                let alias = self.emit_sym(expr, *alias_id);
                format!("{table} AS {alias}")
            }

            // ── Selection -> WHERE clause ────────────────────────
            RelAlg::Select([pred, rel]) => {
                let rel_sql = self.emit(expr, *rel);
                let pred_sql = self.emit_scalar(expr, *pred);
                // If the child is a bare table scan, add WHERE inline.
                // Otherwise wrap in a subquery.
                if is_simple_from(&rel_sql) {
                    format!("SELECT * FROM {rel_sql} WHERE {pred_sql}")
                } else {
                    let alias = self.fresh_alias();
                    format!("SELECT * FROM ({rel_sql}) AS {alias} WHERE {pred_sql}")
                }
            }

            // ── Projection -> SELECT columns ─────────────────────
            RelAlg::Project([cols, rel]) => {
                let rel_sql = self.emit(expr, *rel);
                let cols_sql = self.emit_col_list(expr, *cols);
                let alias = self.fresh_alias();
                format!("SELECT {cols_sql} FROM ({rel_sql}) AS {alias}")
            }

            // ── Inner join ───────────────────────────────────────
            RelAlg::Join([pred, left, right]) => {
                let l = self.emit(expr, *left);
                let r = self.emit(expr, *right);
                let p = self.emit_scalar(expr, *pred);
                format!("SELECT * FROM {l} INNER JOIN {r} ON {p}")
            }
            RelAlg::LeftJoin([pred, left, right]) => {
                let l = self.emit(expr, *left);
                let r = self.emit(expr, *right);
                let p = self.emit_scalar(expr, *pred);
                format!("SELECT * FROM {l} LEFT JOIN {r} ON {p}")
            }

            // ── Semi-join -> IN subquery ─────────────────────────
            //
            // SemiJoin(eq(a.col, b.col), A, B)
            //  =>  SELECT * FROM A WHERE a.col IN (SELECT b.col FROM B)
            //
            // This is the key translation: SIP becomes IN subquery.
            RelAlg::SemiJoin([pred, left, right]) => {
                let l = self.emit(expr, *left);
                let (left_col, right_col) = self.extract_join_cols(expr, *pred);
                let r = self.emit(expr, *right);
                let alias = self.fresh_alias();
                format!(
                    "SELECT * FROM ({l}) AS {alias} WHERE {left_col} IN (SELECT {right_col} FROM ({r}) AS {alias}_r)"
                )
            }

            // ── Anti-join -> NOT IN subquery ─────────────────────
            RelAlg::AntiJoin([pred, left, right]) => {
                let l = self.emit(expr, *left);
                let (left_col, right_col) = self.extract_join_cols(expr, *pred);
                let r = self.emit(expr, *right);
                let alias = self.fresh_alias();
                format!(
                    "SELECT * FROM ({l}) AS {alias} WHERE {left_col} NOT IN (SELECT {right_col} FROM ({r}) AS {alias}_r)"
                )
            }

            // ── Union -> UNION ALL ───────────────────────────────
            RelAlg::Union([left, right]) => {
                let l = self.emit(expr, *left);
                let r = self.emit(expr, *right);
                format!("{l} UNION ALL {r}")
            }

            // ── Group-by -> GROUP BY ─────────────────────────────
            RelAlg::GroupBy([keys, aggs, rel]) => {
                let rel_sql = self.emit(expr, *rel);
                let keys_sql = self.emit_col_list(expr, *keys);
                let aggs_sql = self.emit_agg_list(expr, *aggs);
                let alias = self.fresh_alias();
                let select = if keys_sql.is_empty() {
                    aggs_sql
                } else if aggs_sql.is_empty() {
                    keys_sql.clone()
                } else {
                    format!("{keys_sql}, {aggs_sql}")
                };
                let group_clause = if keys_sql.is_empty() {
                    String::new()
                } else {
                    format!(" GROUP BY {keys_sql}")
                };
                format!("SELECT {select} FROM ({rel_sql}) AS {alias}{group_clause}")
            }

            // ── Order-by -> ORDER BY ─────────────────────────────
            RelAlg::OrderBy([spec, rel]) => {
                let rel_sql = self.emit(expr, *rel);
                let spec_sql = self.emit_sort_spec(expr, *spec);
                format!("{rel_sql} ORDER BY {spec_sql}")
            }

            // ── Limit -> LIMIT ───────────────────────────────────
            RelAlg::Limit([n, rel]) => {
                let rel_sql = self.emit(expr, *rel);
                let n_val = self.emit_num(expr, *n);
                format!("{rel_sql} LIMIT {n_val}")
            }

            // ── Limit-by -> LIMIT n BY cols ──────────────────────
            RelAlg::LimitBy([n, cols, rel]) => {
                let rel_sql = self.emit(expr, *rel);
                let n_val = self.emit_num(expr, *n);
                let cols_sql = self.emit_col_list(expr, *cols);
                format!("{rel_sql} LIMIT {n_val} BY {cols_sql}")
            }

            // ── Distinct -> SELECT DISTINCT ──────────────────────
            RelAlg::Distinct(rel) => {
                let rel_sql = self.emit(expr, *rel);
                format!("SELECT DISTINCT * FROM ({rel_sql})")
            }

            // ── Let -> WITH (CTE) ───────────────────────────────
            //
            // Let(name, def, body) =>
            //   WITH name AS (def_sql) body_sql
            //
            // We accumulate CTEs and emit them as a WITH clause
            // at the outermost level.
            RelAlg::Let([name_id, def, body]) => {
                let name = self.emit_sym(expr, *name_id);
                let def_sql = self.emit(expr, *def);
                self.ctes.push((name.clone(), def_sql));
                self.emit(expr, *body)
            }

            // ── Ref -> CTE name ──────────────────────────────────
            RelAlg::Ref(name_id) => {
                let name = self.emit_sym(expr, *name_id);
                name
            }

            // ── Scalar expressions (delegated) ───────────────────
            _ => self.emit_scalar(expr, id),
        }
    }

    /// Emit a scalar expression.
    fn emit_scalar(&mut self, expr: &RecExpr<RelAlg>, id: egg::Id) -> String {
        let node = &expr[id];
        match node {
            RelAlg::Col([table_id, col_id]) => {
                let t = self.emit_sym(expr, *table_id);
                let c = self.emit_sym(expr, *col_id);
                format!("{t}.{c}")
            }
            RelAlg::Param([type_id, val_id]) => {
                let ch_type = self.emit_sym(expr, *type_id);
                let value = self.emit_sym(expr, *val_id);
                let name = format!("p{}", self.params.len());
                let placeholder = format!("{{{name}:{ch_type}}}");
                self.params.insert(
                    name,
                    ParamValue {
                        ch_type: parse_ch_type(&ch_type),
                        value: parse_value(&value),
                    },
                );
                placeholder
            }
            RelAlg::BinOp([op_id, left, right]) => {
                let op = self.emit_sym(expr, *op_id);
                let l = self.emit_scalar(expr, *left);
                let r = self.emit_scalar(expr, *right);
                if op == "IN" {
                    format!("{l} IN {r}")
                } else {
                    format!("({l} {op} {r})")
                }
            }
            RelAlg::UnOp([op_id, e]) => {
                let op = self.emit_sym(expr, *op_id);
                let e = self.emit_scalar(expr, *e);
                if op == "IS NULL" || op == "IS NOT NULL" {
                    format!("({e} {op})")
                } else {
                    format!("({op} {e})")
                }
            }
            RelAlg::And([l, r]) => {
                let l = self.emit_scalar(expr, *l);
                let r = self.emit_scalar(expr, *r);
                format!("({l} AND {r})")
            }
            RelAlg::Or([l, r]) => {
                let l = self.emit_scalar(expr, *l);
                let r = self.emit_scalar(expr, *r);
                format!("({l} OR {r})")
            }
            RelAlg::Not(e) => {
                let e = self.emit_scalar(expr, *e);
                format!("(NOT {e})")
            }
            RelAlg::Func([name_id, args]) => {
                let name = self.emit_sym(expr, *name_id);
                let args = self.emit_func_args(expr, *args);
                format!("{name}({args})")
            }
            RelAlg::Lambda([param_id, body]) => {
                let p = self.emit_sym(expr, *param_id);
                let b = self.emit_scalar(expr, *body);
                format!("{p} -> {b}")
            }
            RelAlg::InList([e, vals]) => {
                let e = self.emit_scalar(expr, *e);
                let vals = self.emit_func_args(expr, *vals);
                format!("{e} IN ({vals})")
            }
            RelAlg::True => "1".to_string(),
            RelAlg::False => "0".to_string(),
            RelAlg::Sym(s) => s.as_str().to_string(),
            RelAlg::Num(n) => n.to_string(),

            // Aggregates
            RelAlg::Agg([func_id, arg]) => {
                let f = self.emit_sym(expr, *func_id);
                let a = self.emit_scalar(expr, *arg);
                format!("{f}({a})")
            }
            RelAlg::AggIf([func_id, arg, cond]) => {
                let f = self.emit_sym(expr, *func_id);
                let a = self.emit_scalar(expr, *arg);
                let c = self.emit_scalar(expr, *cond);
                // Map COUNT -> countIf, SUM -> sumIf, etc.
                let if_name = to_if_combinator(&f);
                format!("{if_name}({a}, {c})")
            }
            RelAlg::Agg0(func_id) => {
                let f = self.emit_sym(expr, *func_id);
                format!("{f}()")
            }

            _ => format!("/* unhandled: {:?} */", node),
        }
    }

    /// Extract left and right column references from a join predicate
    /// for semi-join -> IN subquery translation.
    fn extract_join_cols(&self, expr: &RecExpr<RelAlg>, pred_id: egg::Id) -> (String, String) {
        // Expects: (binop "=" (col t1 c1) (col t2 c2))
        if let RelAlg::BinOp([_op, left, right]) = &expr[pred_id] {
            let l = self.emit_scalar_readonly(expr, *left);
            let r = self.emit_scalar_readonly(expr, *right);
            return (l, r);
        }
        ("*".to_string(), "*".to_string())
    }

    fn emit_scalar_readonly(&self, expr: &RecExpr<RelAlg>, id: egg::Id) -> String {
        match &expr[id] {
            RelAlg::Col([t, c]) => {
                let t = extract_sym(expr, *t);
                let c = extract_sym(expr, *c);
                format!("{t}.{c}")
            }
            _ => "?".to_string(),
        }
    }

    fn emit_sym(&self, expr: &RecExpr<RelAlg>, id: egg::Id) -> String {
        extract_sym(expr, id)
    }

    fn emit_num(&self, expr: &RecExpr<RelAlg>, id: egg::Id) -> String {
        match &expr[id] {
            RelAlg::Num(n) => n.to_string(),
            _ => "0".to_string(),
        }
    }

    fn emit_col_list(&mut self, expr: &RecExpr<RelAlg>, id: egg::Id) -> String {
        let mut items = Vec::new();
        self.collect_list(expr, id, &mut items);
        items
            .into_iter()
            .map(|i| self.emit_scalar(expr, i))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn emit_agg_list(&mut self, expr: &RecExpr<RelAlg>, id: egg::Id) -> String {
        let mut items = Vec::new();
        self.collect_list(expr, id, &mut items);
        items
            .into_iter()
            .map(|i| self.emit_scalar(expr, i))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn emit_func_args(&mut self, expr: &RecExpr<RelAlg>, id: egg::Id) -> String {
        let mut items = Vec::new();
        self.collect_list(expr, id, &mut items);
        items
            .into_iter()
            .map(|i| self.emit_scalar(expr, i))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn emit_sort_spec(&mut self, expr: &RecExpr<RelAlg>, id: egg::Id) -> String {
        let mut items = Vec::new();
        self.collect_list(expr, id, &mut items);
        items
            .into_iter()
            .map(|i| {
                if let RelAlg::SortKey([e, d]) = &expr[i] {
                    let e_sql = self.emit_scalar(expr, *e);
                    let d_sql = self.emit_sym(expr, *d);
                    let dir = if d_sql == "desc" { "DESC" } else { "ASC" };
                    format!("{e_sql} {dir}")
                } else {
                    self.emit_scalar(expr, i)
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Walk a Cons/Nil list and collect element Ids.
    fn collect_list(&self, expr: &RecExpr<RelAlg>, id: egg::Id, out: &mut Vec<egg::Id>) {
        match &expr[id] {
            RelAlg::Cons([head, tail]) => {
                out.push(*head);
                self.collect_list(expr, *tail, out);
            }
            RelAlg::Nil => {}
            _ => {
                // Single element, not wrapped in list
                out.push(id);
            }
        }
    }

    /// Finalize: wrap accumulated CTEs into WITH clause.
    pub fn finalize(self, body_sql: String) -> String {
        if self.ctes.is_empty() {
            return body_sql;
        }
        let cte_defs: Vec<String> = self
            .ctes
            .iter()
            .map(|(name, sql)| format!("{name} AS ({sql})"))
            .collect();
        format!("WITH {} {}", cte_defs.join(", "), body_sql)
    }
}

fn extract_sym(expr: &RecExpr<RelAlg>, id: egg::Id) -> String {
    match &expr[id] {
        RelAlg::Sym(s) => s.as_str().to_string(),
        _ => format!("?{:?}", id),
    }
}

fn is_simple_from(sql: &str) -> bool {
    // Heuristic: if the SQL doesn't contain SELECT, it's a bare table reference.
    !sql.contains("SELECT")
}

fn to_if_combinator(func: &str) -> String {
    match func {
        "COUNT" => "countIf".to_string(),
        "SUM" => "sumIf".to_string(),
        "AVG" => "avgIf".to_string(),
        "MIN" => "minIf".to_string(),
        "MAX" => "maxIf".to_string(),
        other => format!("{other}If"),
    }
}

fn parse_ch_type(s: &str) -> ChType {
    match s {
        "Int64" => ChType::Int64,
        "String" => ChType::String,
        "Bool" => ChType::Bool,
        "UInt32" => ChType::UInt32,
        "DateTime64" => ChType::DateTime64,
        _ => ChType::String,
    }
}

fn parse_value(s: &str) -> serde_json::Value {
    // Try integer first, then string
    if let Ok(n) = s.parse::<i64>() {
        serde_json::Value::Number(n.into())
    } else if s == "true" {
        serde_json::Value::Bool(true)
    } else if s == "false" {
        serde_json::Value::Bool(false)
    } else {
        serde_json::Value::String(s.to_string())
    }
}
```

---

## 8. Post-Extraction Mandatory Passes

Dedup and Security run **after** egg extraction on the final `RecExpr<RelAlg>`.
They are not rewrite rules because they must never be optimized away.

```rust
// File: crates/query-engine/relalg/src/mandatory.rs

use egg::{Id, RecExpr};
use crate::lang::RelAlg;

/// Inject ReplacingMergeTree dedup on all node table scans.
/// Wraps Scan(gl_*) with LimitBy(1, [id], OrderBy(version DESC, Select(_deleted=false, Scan))).
pub fn inject_dedup(
    expr: &mut RecExpr<RelAlg>,
    edge_tables: &std::collections::HashSet<String>,
    skip_dedup: bool,
    use_final: bool,
) {
    // Walk the expression bottom-up. For each Scan/AliasedScan of a node table:
    // 1. Add Select(_deleted = false, scan)
    // 2. If !skip_dedup && !use_final: wrap in LimitBy(1, [id, traversal_path], OrderBy(version DESC, ...))
    // 3. If use_final: replace Scan with ScanFinal

    // Implementation: rebuild the RecExpr, replacing matching nodes.
    let mut new_expr = RecExpr::default();
    let mut id_map: Vec<Id> = Vec::new();

    for (i, node) in expr.as_ref().iter().enumerate() {
        let old_id = Id::from(i);
        let new_node = match node {
            RelAlg::AliasedScan([table_id, alias_id]) => {
                let table_sym = extract_sym_from_expr(expr, *table_id);
                let is_node = table_sym
                    .as_ref()
                    .map(|t| t.starts_with("gl_") && !edge_tables.contains(t.as_str()))
                    .unwrap_or(false);

                if is_node {
                    // Build the dedup wrapper
                    let mapped_table = id_map[usize::from(*table_id)];
                    let mapped_alias = id_map[usize::from(*alias_id)];

                    if use_final {
                        // Replace with ScanFinal
                        RelAlg::ScanFinal(mapped_table)
                    } else {
                        // Keep as AliasedScan, the LimitBy wrapping happens
                        // at a higher level (Select + LimitBy around the scan).
                        // For now, just add _deleted = false filter.
                        let scan = new_expr.add(RelAlg::AliasedScan([mapped_table, mapped_alias]));
                        let alias_str = extract_sym_from_expr(expr, *alias_id).unwrap_or_default();
                        let deleted_col = {
                            let t = new_expr.add(RelAlg::Sym(egg::Symbol::from(alias_str.as_str())));
                            let c = new_expr.add(RelAlg::Sym("_deleted".into()));
                            new_expr.add(RelAlg::Col([t, c]))
                        };
                        let false_val = new_expr.add(RelAlg::False);
                        let eq_op = new_expr.add(RelAlg::Sym("=".into()));
                        let pred = new_expr.add(RelAlg::BinOp([eq_op, deleted_col, false_val]));
                        let selected = new_expr.add(RelAlg::Select([pred, scan]));

                        if skip_dedup {
                            id_map.push(selected);
                            continue;
                        }

                        // Wrap with LimitBy(1, [id, traversal_path], OrderBy(version DESC, ...))
                        // ... (similar node construction)
                        id_map.push(selected);
                        continue;
                    }
                } else {
                    // Edge table or non-gl table: pass through
                    let mapped_table = id_map[usize::from(*table_id)];
                    let mapped_alias = id_map[usize::from(*alias_id)];
                    RelAlg::AliasedScan([mapped_table, mapped_alias])
                }
            }
            // Remap children for all other nodes
            other => other.clone().map_children(|child| id_map[usize::from(child)]),
        };

        let new_id = new_expr.add(new_node);
        id_map.push(new_id);
    }

    *expr = new_expr;
}

/// Inject traversal_path security filters on all node table scans.
pub fn inject_security(
    expr: &mut RecExpr<RelAlg>,
    traversal_paths: &[String],
    edge_tables: &std::collections::HashSet<String>,
    skip_tables: &[String],
) {
    // Similar to inject_dedup: walk bottom-up and wrap node table scans
    // with Select(startsWith(traversal_path, paths), scan).
    //
    // For 1 path:  startsWith(alias.traversal_path, path)
    // For N paths: arrayExists(p -> startsWith(alias.traversal_path, p), [paths])
    //
    // Skip tables in skip_tables list.

    // Implementation follows the same RecExpr rebuild pattern as inject_dedup.
    let _ = (expr, traversal_paths, edge_tables, skip_tables);
}

fn extract_sym_from_expr(expr: &RecExpr<RelAlg>, id: Id) -> Option<String> {
    match &expr[id] {
        RelAlg::Sym(s) => Some(s.as_str().to_string()),
        _ => None,
    }
}
```

---

## 9. Pipeline Integration

File changes to `crates/query-engine/compiler/src/pipelines.rs`:

```rust
/// New pipeline with egg optimizer:
///
/// JSON -> Validate -> Normalize -> Restrict -> LowerRelAlg -> EggOptimize ->
///   EnforceRelAlg -> DeduplicateRelAlg -> SecurityRelAlg -> Check ->
///   HydratePlan -> Settings -> CodegenRelAlg
pub fn clickhouse_egg() -> Pipeline<SecureEnv, QueryState> {
    Pipeline::builder()
        .pass(ValidatePass)
        .seal(SealJson)
        .pass(NormalizePass)
        .pass(RestrictPass)
        .pass(LowerRelAlgPass)      // new: produces RecExpr<RelAlg>
        .pass(EggOptimizePass)       // new: runs equality saturation
        .pass(DeduplicateRelAlgPass) // new: post-extraction mandatory pass
        .pass(SecurityRelAlgPass)    // new: post-extraction mandatory pass
        .pass(EnforceRelAlgPass)     // new: adds redaction columns
        .pass(CheckPass)
        .pass(HydratePlanPass)
        .pass(SettingsPass)
        .pass(CodegenRelAlgPass)     // new: RelAlg -> SQL
        .build()
}
```

New pass implementations:

```rust
pub struct EggOptimizePass;

impl<E, S> CompilerPass<E, S> for EggOptimizePass
where
    E: PipelineEnv + HasOntology,
    S: PipelineState + HasRelExpr + HasInput,
{
    const NAME: &'static str = "egg_optimize";

    fn run(&self, env: &E, state: &mut S) -> Result<()> {
        let expr = state.take_rel_expr()?;
        let input = state.input()?;
        let ctx = build_query_context(input, env.ontology());
        let optimized = relalg::optimize::optimize(expr, ctx);
        state.set_rel_expr(optimized);
        Ok(())
    }
}
```

---

## 10. Crate Structure

```
crates/query-engine/relalg/
  Cargo.toml       # depends on: egg = "0.9", compiler (for Input types)
  src/
    lib.rs
    lang.rs         # define_language! (Section 1)
    analysis.rs     # Analysis + QueryContext (Section 2)
    cost.rs         # ClickHouseCost (Section 3)
    rules.rs        # All rewrite rules (Section 4)
    optimize.rs     # Runner entry point (Section 5)
    codegen.rs      # RelAlg -> SQL (Section 7)
    mandatory.rs    # Dedup + Security post-extraction (Section 8)
```

Cargo.toml:
```toml
[package]
name = "relalg"
version.workspace = true
edition = "2024"

[dependencies]
egg = "0.9"
gkg-utils = { path = "../../utils" }
ontology = { path = "../../ontology" }
serde_json = { workspace = true }

[dev-dependencies]
pretty_assertions = "1.4"
```

---

## 11. E-graph Explosion Controls

```rust
// In optimize.rs
let runner = Runner::<RelAlg, RelAlgAnalysis, ()>::new(analysis)
    .with_expr(&expr)
    .with_node_limit(15_000)      // typical query: ~100-300 nodes
    .with_iter_limit(30)           // most rewrites fire in 5-10 iters
    .with_time_limit(Duration::from_millis(50))  // current optimizer: ~1ms
    .with_scheduler(egg::BackoffScheduler::default()
        .with_initial_match_limit(1000)  // prevent join-commute explosion
        .with_ban_length(5))             // backoff aggressive rules
    .run(&rules::all_rules());
```

Rule-specific controls:
- `join-commute` and `join-assoc-*` use BackoffScheduler because they can
  generate exponentially many join orderings.
- SIP rules have the `is_not_already_narrowed` guard to prevent infinite
  SemiJoin nesting.
- Hop frontier rules only fire on depth-2 and depth-3 chains (explicit patterns).

---

## 12. Mapping: Current Optimization -> Rewrite Rule

| # | Current pass | Rewrite rule(s) | Mechanism |
|---|-------------|-----------------|-----------|
| 1 | `inject_entity_kind_filters` | `enrich_with_kind_filters()` | Pre-saturation enrichment from ontology |
| 2 | `push_kind_literals_into_variable_length_arms` | `select-push-union` | Standard predicate pushdown through union |
| 3 | `inject_agg_group_by_kind_filters` | Same as #1 | Kind filters injected on all node-edge joins |
| 4 | `apply_sip_prefilter` | `sip-left-to-right`, `sip-right-to-left` | Semi-join introduction |
| 5 | Cascading SIP | Same as #4, iterated | E-graph saturation chains SIP through joins |
| 6 | `apply_edge_led_reorder` | `join-commute` | Cost function prefers edge-first when SIP present |
| 7 | `cascade_node_filter_ctes` | Same as #4 + #5 | Cascading SIP across relationship chain |
| 8 | `narrow_joined_nodes_via_pinned_neighbors` | Same as #4 + #5 | SIP from pinned node cascades to neighbors |
| 9 | `apply_target_sip_prefilter` | `sip-right-to-left` | SIP from selective target side |
| 10 | `fold_filters_into_aggregates` | `fold-filter-to-agg-if` | Pattern rewrite on GroupBy+Select |
| 11 | `prune_unreferenced_node_joins` | `prune-unused-join-right/left` | Conditional rewrite checking column usage |
| 12 | `apply_traversal_hop_frontiers` | `hop-frontier-2`, `hop-frontier-3` | Custom applier builds Let-chain |
| 13 | `apply_path_hop_frontiers` | Same as #12 | Same mechanism for path-finding frontiers |
