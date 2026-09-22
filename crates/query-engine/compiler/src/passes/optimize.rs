use super::plan_v2::*;
use crate::input::*;
use ontology::constants as ontology_constants;

pub struct RuleCtx<'a> {
    pub input: &'a Input,
    pub graph: &'a JoinGraph,
}

pub fn optimize(tree: PhysOp, ctx: &RuleCtx) -> PhysOp {
    let mut tree = tree;
    loop {
        let prev = tree.clone();
        tree = apply_rules(tree, ctx);
        if tree == prev {
            break;
        }
    }
    tree
}

fn apply_rules(tree: PhysOp, ctx: &RuleCtx) -> PhysOp {
    let tree = map_children(tree, |child| apply_rules(child, ctx));
    let rules: &[fn(&PhysOp, &RuleCtx) -> Option<PhysOp>] = &[
        rule_elide_empty_sort,
        rule_merge_filters,
        rule_edge_dedup,
        rule_fk_elision,
        rule_prune_duplicate_node_join,
        rule_denorm_tag_pushdown,
        rule_column_pushdown,
        rule_prune_unreferenced_agg_node,
        rule_remove_stale_edge_predicates,
        rule_narrowing_cte,
        rule_cascade_sip,
        rule_fk_edge_metadata,
        rule_scope_anchor_elision,
        rule_count_target_fk_rejoin,
        rule_limit_by_single_hop_agg,
    ];
    for rule in rules {
        if let Some(rewritten) = rule(&tree, ctx) {
            return rewritten;
        }
    }
    tree
}

fn map_children(op: PhysOp, f: impl Fn(PhysOp) -> PhysOp) -> PhysOp {
    match op {
        PhysOp::Scan { .. } => op,
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
        PhysOp::Union { arms } => PhysOp::Union {
            arms: arms.into_iter().map(&f).collect(),
        },
        PhysOp::Sort { input, keys } => PhysOp::Sort {
            input: Box::new(f(*input)),
            keys,
        },
        PhysOp::Limit { input, count } => PhysOp::Limit {
            input: Box::new(f(*input)),
            count,
        },
    }
}

// ── Rule 1: Elide empty Sort ────────────────────────────────────────────────

fn rule_elide_empty_sort(op: &PhysOp, _ctx: &RuleCtx) -> Option<PhysOp> {
    match op {
        PhysOp::Sort { keys, input } if keys.is_empty() => Some(*input.clone()),
        _ => None,
    }
}

// ── Rule 2: Merge adjacent Filters ──────────────────────────────────────────

fn rule_merge_filters(op: &PhysOp, _ctx: &RuleCtx) -> Option<PhysOp> {
    match op {
        PhysOp::Filter {
            predicates: p1,
            input,
        } => {
            if let PhysOp::Filter {
                predicates: p2,
                input: inner,
            } = input.as_ref()
            {
                let mut merged = p1.clone();
                merged.extend(p2.clone());
                Some(PhysOp::Filter {
                    predicates: merged,
                    input: inner.clone(),
                })
            } else {
                None
            }
        }
        _ => None,
    }
}

// ── Rule 3: Edge dedup for multi-edge chains ────────────────────────────────

fn rule_edge_dedup(op: &PhysOp, _ctx: &RuleCtx) -> Option<PhysOp> {
    let edge_count = count_edge_scans(op);
    if edge_count < 2 {
        return None;
    }
    let updated = set_edge_dedup(op.clone(), true);
    if updated == *op { None } else { Some(updated) }
}

fn count_edge_scans(op: &PhysOp) -> usize {
    match op {
        PhysOp::Scan {
            table,
            dedup: false,
            ..
        } if table.starts_with("gl_")
            && table != "gl_user"
            && !table.ends_with("_request")
            && !table.ends_with("_item") =>
        {
            // Heuristic: edge tables are gl_edge, gl_code_edge, etc.
            // Node tables are gl_merge_request, gl_project, gl_user, etc.
            // Better: check if the table is in the edge table config
            if table.contains("edge") { 1 } else { 0 }
        }
        PhysOp::Scan { .. } => 0,
        PhysOp::Filter { input, .. } => count_edge_scans(input),
        PhysOp::Project { input, .. } => count_edge_scans(input),
        PhysOp::Join { left, right, .. } => count_edge_scans(left) + count_edge_scans(right),
        PhysOp::Aggregate { input, .. } => count_edge_scans(input),
        PhysOp::Union { arms } => arms.iter().map(count_edge_scans).sum(),
        PhysOp::Sort { input, .. } => count_edge_scans(input),
        PhysOp::Limit { input, .. } => count_edge_scans(input),
    }
}

fn set_edge_dedup(op: PhysOp, dedup_val: bool) -> PhysOp {
    match op {
        PhysOp::Scan {
            table,
            alias,
            dedup,
        } if table.contains("edge") && !dedup => PhysOp::Scan {
            table,
            alias,
            dedup: dedup_val,
        },
        other => map_children(other, |child| set_edge_dedup(child, dedup_val)),
    }
}

// ── Rule 4: FK elision ──────────────────────────────────────────────────────
// Replace edge scans with FK joins when the relationship has an FK column.
// Only fires when ALL edge scans in the tree can be FK-elided.

// ── Rule 4: FK elision (per-hop, bottom-up) ─────────────────────────────────
// Replaces Filter(Scan(edge_table)) with Join(node_a, node_b, on: fk)

fn rule_fk_elision(op: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    // Match: Join where either child is an FK-eligible edge scan
    if let PhysOp::Join {
        left,
        right,
        on,
        kind: JoinKind::Inner,
    } = op
    {
        // Try right child first (most common: chain builds left-to-right)
        if let Some(edge_alias) = edge_scan_alias(right) {
            if let Some(result) = fk_elide_edge(&edge_alias, left, on, false, ctx) {
                return Some(result);
            }
        }
        // Try left child (first edge in chain, or reversed)
        if let Some(edge_alias) = edge_scan_alias(left) {
            if let Some(result) = fk_elide_edge(&edge_alias, right, on, true, ctx) {
                return Some(result);
            }
        }
    }

    // Bare Filter(Scan(edge)) — single relationship with no joins at all
    if let Some(edge_alias) = edge_scan_alias(op) {
        let idx = alias_to_rel_index(&edge_alias)?;
        let rel = ctx.input.relationships.get(idx)?;
        let fk_col = rel.fk_column.as_ref()?;
        if rel.hops.max != 1 || matches!(rel.direction, Direction::Both) || !rel.filters.is_empty()
        {
            return None;
        }
        let (fk_alias, tgt_alias) = fk_sides(rel, fk_col, ctx);
        let fk_node = ctx.input.nodes.iter().find(|n| n.id == fk_alias)?;
        let tgt_node = ctx.input.nodes.iter().find(|n| n.id == tgt_alias)?;
        return Some(PhysOp::Join {
            left: Box::new(filtered_node_scan(fk_node)),
            right: Box::new(filtered_node_scan(tgt_node)),
            on: JoinOn {
                left: (fk_alias.to_string(), fk_col.clone()),
                right: (
                    tgt_alias.to_string(),
                    ontology_constants::DEFAULT_PRIMARY_KEY.to_string(),
                ),
            },
            kind: JoinKind::Inner,
        });
    }

    None
}

fn fk_elide_edge(
    edge_alias: &str,
    other: &PhysOp,
    on: &JoinOn,
    edge_is_left: bool,
    ctx: &RuleCtx,
) -> Option<PhysOp> {
    let idx = alias_to_rel_index(edge_alias)?;
    let rel = ctx.input.relationships.get(idx)?;
    let fk_col = rel.fk_column.as_ref()?;
    if rel.hops.max != 1 || matches!(rel.direction, Direction::Both) || !rel.filters.is_empty() {
        return None;
    }

    let (fk_alias, tgt_alias) = fk_sides(rel, fk_col, ctx);
    let fk_node = ctx.input.nodes.iter().find(|n| n.id == fk_alias)?;
    let tgt_node = ctx.input.nodes.iter().find(|n| n.id == tgt_alias)?;

    let fk_join = PhysOp::Join {
        left: Box::new(filtered_node_scan(fk_node)),
        right: Box::new(filtered_node_scan(tgt_node)),
        on: JoinOn {
            left: (fk_alias.to_string(), fk_col.clone()),
            right: (
                tgt_alias.to_string(),
                ontology_constants::DEFAULT_PRIMARY_KEY.to_string(),
            ),
        },
        kind: JoinKind::Inner,
    };

    let from_alias = &rel.from;
    let new_on = if edge_is_left {
        JoinOn {
            left: (
                from_alias.clone(),
                ontology_constants::DEFAULT_PRIMARY_KEY.to_string(),
            ),
            right: on.right.clone(),
        }
    } else {
        JoinOn {
            left: on.left.clone(),
            right: (
                from_alias.clone(),
                ontology_constants::DEFAULT_PRIMARY_KEY.to_string(),
            ),
        }
    };

    let (new_left, new_right) = if edge_is_left {
        (Box::new(fk_join), Box::new(other.clone()))
    } else {
        (Box::new(other.clone()), Box::new(fk_join))
    };

    Some(PhysOp::Join {
        left: new_left,
        right: new_right,
        on: new_on,
        kind: JoinKind::Inner,
    })
}

fn edge_scan_alias(op: &PhysOp) -> Option<String> {
    match op {
        PhysOp::Scan { alias, table, .. } if table.contains("edge") => Some(alias.clone()),
        PhysOp::Filter { input, .. } => edge_scan_alias(input),
        _ => None,
    }
}

fn rule_prune_duplicate_node_join(op: &PhysOp, _ctx: &RuleCtx) -> Option<PhysOp> {
    let PhysOp::Join {
        left,
        right,
        kind: JoinKind::Inner,
        ..
    } = op
    else {
        return None;
    };
    let right_alias = scan_alias(right)?;
    if has_alias(left, &right_alias) {
        Some(*left.clone())
    } else {
        None
    }
}

fn scan_alias(op: &PhysOp) -> Option<String> {
    match op {
        PhysOp::Scan { alias, .. } => Some(alias.clone()),
        PhysOp::Filter { input, .. } => scan_alias(input),
        _ => None,
    }
}

fn has_alias(op: &PhysOp, target: &str) -> bool {
    match op {
        PhysOp::Scan { alias, .. } => alias == target,
        PhysOp::Filter { input, .. } => has_alias(input, target),
        PhysOp::Join { left, right, .. } => has_alias(left, target) || has_alias(right, target),
        PhysOp::Project { input, .. }
        | PhysOp::Sort { input, .. }
        | PhysOp::Limit { input, .. }
        | PhysOp::Aggregate { input, .. } => has_alias(input, target),
        PhysOp::Union { arms } => arms.iter().any(|a| has_alias(a, target)),
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn alias_to_rel_index(alias: &str) -> Option<usize> {
    alias.strip_prefix('e')?.parse().ok()
}

fn fk_sides<'a>(rel: &'a InputRelationship, fk_col: &str, ctx: &'a RuleCtx) -> (&'a str, &'a str) {
    let from_has = ctx
        .input
        .nodes
        .iter()
        .find(|n| n.id == rel.from)
        .and_then(|n| n.table.as_deref())
        .and_then(|t| ctx.input.compiler.table_columns.get(t))
        .is_some_and(|cols| cols.contains(fk_col));
    if from_has {
        (&rel.from, &rel.to)
    } else {
        (&rel.to, &rel.from)
    }
}

fn filtered_node_scan(n: &InputNode) -> PhysOp {
    let mut preds = Vec::new();
    let mut props: Vec<_> = n.filters.iter().collect();
    props.sort_unstable_by_key(|(k, _)| *k);
    for (prop, fs) in props {
        for f in fs {
            preds.push(Predicate::NodeFilter {
                property: prop.clone(),
                filter: f.clone(),
            });
        }
    }
    if !n.node_ids.is_empty() {
        preds.push(Predicate::In {
            column: ontology_constants::DEFAULT_PRIMARY_KEY.to_string(),
            values: n.node_ids.iter().map(|&id| Value::Int(id)).collect(),
        });
    }
    if let Some(ref r) = n.id_range {
        preds.push(Predicate::Range {
            column: ontology_constants::DEFAULT_PRIMARY_KEY.to_string(),
            start: r.start,
            end: r.end,
        });
    }
    preds.push(Predicate::Eq {
        column: "_deleted".to_string(),
        value: Value::Bool(false),
    });

    PhysOp::Filter {
        input: Box::new(PhysOp::Scan {
            table: n.table.as_deref().unwrap_or("").to_string(),
            alias: n.id.clone(),
            dedup: true,
        }),
        predicates: preds,
    }
}

// ── Rule 6: Denorm tag pushdown ─────────────────────────────────────────────
// When a node filter matches a denormalized property on the edge, push a
// has(tag_col, "key:value") predicate onto the edge scan.

fn rule_denorm_tag_pushdown(op: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    // Match: Filter(Scan(edge_table)) — push denorm tags from endpoint nodes
    let PhysOp::Filter { predicates, input } = op else {
        return None;
    };
    let PhysOp::Scan { table, alias, .. } = input.as_ref() else {
        return None;
    };
    if !table.contains("edge") {
        return None;
    }
    let rel_idx = alias_to_rel_index(alias)?;
    let rel = ctx.input.relationships.get(rel_idx)?;
    if crate::passes::normalize::is_wildcard(&rel.types) {
        return None;
    }

    let (sc, ec) = rel.direction.edge_columns();
    let meta = &ctx.input.compiler;
    let mut new_preds = Vec::new();

    for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
        let Some(node) = ctx.input.nodes.iter().find(|n| &n.id == nid) else {
            continue;
        };
        let entity = node.entity.as_deref().unwrap_or("");
        let dir = if ic == ontology_constants::SOURCE_ID_COLUMN {
            "source"
        } else {
            "target"
        };

        for (prop, fs) in &node.filters {
            let key = (entity.to_string(), prop.clone(), dir.to_string());
            let denorm_kinds = meta.denorm_rel_kinds.get(&key);
            if !denorm_kinds.is_some_and(|ks| rel.types.iter().any(|t| ks.contains(t))) {
                continue;
            }
            let Some((tc, tk)) = meta.denormalized_columns.get(&key) else {
                continue;
            };

            for f in fs {
                match (&f.op, &f.value) {
                    (None | Some(FilterOp::Eq), Some(val)) => {
                        let tag_val = match val {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Bool(b) => b.to_string(),
                            serde_json::Value::Number(n) => n.to_string(),
                            _ => continue,
                        };
                        new_preds.push(Predicate::Func {
                            name: "has".to_string(),
                            column: tc.clone(),
                            value: Value::Str(format!("{tk}:{tag_val}")),
                        });
                    }
                    (Some(FilterOp::In), Some(serde_json::Value::Array(arr))) => {
                        let tags: Vec<String> = arr
                            .iter()
                            .filter_map(|v| {
                                let s = match v {
                                    serde_json::Value::String(s) => s.clone(),
                                    serde_json::Value::Bool(b) => b.to_string(),
                                    serde_json::Value::Number(n) => n.to_string(),
                                    _ => return None,
                                };
                                Some(format!("{tk}:{s}"))
                            })
                            .collect();
                        if tags.len() == 1 {
                            new_preds.push(Predicate::Func {
                                name: "has".to_string(),
                                column: tc.clone(),
                                value: Value::Str(tags.into_iter().next().unwrap()),
                            });
                        } else if !tags.is_empty() {
                            new_preds.push(Predicate::Func {
                                name: "hasAny".to_string(),
                                column: tc.clone(),
                                value: Value::Strs(tags),
                            });
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    if new_preds.is_empty() {
        return None;
    }

    // Check we haven't already pushed these (idempotency)
    if predicates
        .iter()
        .any(|p| matches!(p, Predicate::Func { name, .. } if name == "has" || name == "hasAny"))
    {
        return None;
    }

    let mut merged = predicates.clone();
    merged.extend(new_preds);
    Some(PhysOp::Filter {
        predicates: merged,
        input: input.clone(),
    })
}

fn rule_column_pushdown(op: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    // Match: Filter(Scan(edge_table)) — push node filters when edge table has the column
    let PhysOp::Filter { predicates, input } = op else {
        return None;
    };
    let PhysOp::Scan { table, alias, .. } = input.as_ref() else {
        return None;
    };
    if !table.contains("edge") {
        return None;
    }
    let rel_idx = alias_to_rel_index(alias)?;
    let rel = ctx.input.relationships.get(rel_idx)?;

    let meta = &ctx.input.compiler;
    let ecols = meta.table_columns.get(table)?;
    let reserved: std::collections::HashSet<&str> = ontology_constants::EDGE_RESERVED_COLUMNS
        .iter()
        .copied()
        .collect();

    let mut new_preds = Vec::new();
    for nid in [&rel.from, &rel.to] {
        let Some(node) = ctx.input.nodes.iter().find(|n| &n.id == nid) else {
            continue;
        };
        for (prop, fs) in &node.filters {
            if ecols.contains(prop) && !reserved.contains(prop.as_str()) {
                // Check we haven't already pushed this
                if predicates.iter().any(
                    |p| matches!(p, Predicate::NodeFilter { property, .. } if property == prop),
                ) {
                    continue;
                }
                for f in fs {
                    new_preds.push(Predicate::NodeFilter {
                        property: prop.clone(),
                        filter: f.clone(),
                    });
                }
            }
        }
    }

    if new_preds.is_empty() {
        return None;
    }
    let mut merged = predicates.clone();
    merged.extend(new_preds);
    Some(PhysOp::Filter {
        predicates: merged,
        input: input.clone(),
    })
}

fn add_predicates_to_filter(op: PhysOp, extra: Vec<Predicate>) -> PhysOp {
    match op {
        PhysOp::Filter {
            mut predicates,
            input,
        } => {
            predicates.extend(extra);
            PhysOp::Filter { predicates, input }
        }
        other => PhysOp::Filter {
            predicates: extra,
            input: Box::new(other),
        },
    }
}

fn edge_table_name(op: &PhysOp) -> Option<String> {
    match op {
        PhysOp::Scan { table, .. } => Some(table.clone()),
        PhysOp::Filter { input, .. } => edge_table_name(input),
        _ => None,
    }
}

// ── Rule 8: Prune unreferenced node in aggregation ──────────────────────────

fn rule_prune_unreferenced_agg_node(op: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    if ctx.input.query_type != QueryType::Aggregation {
        return None;
    }
    let PhysOp::Join {
        left,
        right,
        kind: JoinKind::Inner,
        ..
    } = op
    else {
        return None;
    };
    let alias = scan_alias(right)?;
    let node = ctx.input.nodes.iter().find(|n| n.id == alias)?;

    let in_group_by = ctx
        .input
        .aggregation
        .group_by
        .iter()
        .any(|g| g.node() == alias.as_str());
    let in_metrics = ctx
        .input
        .aggregation
        .metrics
        .iter()
        .any(|m| m.expr.node() == alias.as_str());
    let has_filters =
        !node.filters.is_empty() || !node.node_ids.is_empty() || node.id_range.is_some();
    let in_order_by = ctx
        .input
        .order_by
        .as_ref()
        .is_some_and(|ob| ob.node == alias);

    if !in_group_by && !in_metrics && !has_filters && !in_order_by {
        Some(*left.clone())
    } else {
        None
    }
}

// ── Rule 9: Remove stale edge predicates after FK elision ───────────────────

fn rule_remove_stale_edge_predicates(op: &PhysOp, _ctx: &RuleCtx) -> Option<PhysOp> {
    let PhysOp::Filter { predicates, input } = op else {
        return None;
    };
    // If the input is NOT an edge scan (no edge table underneath), then
    // edge-specific predicates (relationship_kind, source_kind, target_kind)
    // are stale and should be removed.
    if edge_scan_alias(input).is_some() {
        return None; // edge scan still exists, predicates are valid
    }

    let edge_columns: std::collections::HashSet<&str> = [
        ontology_constants::RELATIONSHIP_KIND_COLUMN,
        ontology_constants::SOURCE_KIND_COLUMN,
        ontology_constants::TARGET_KIND_COLUMN,
        ontology_constants::SOURCE_ID_COLUMN,
        ontology_constants::TARGET_ID_COLUMN,
        ontology_constants::SOURCE_TAGS_COLUMN,
        ontology_constants::TARGET_TAGS_COLUMN,
    ]
    .into_iter()
    .collect();

    let cleaned: Vec<Predicate> = predicates
        .iter()
        .filter(|p| match p {
            Predicate::Eq { column, .. } | Predicate::In { column, .. } => {
                !edge_columns.contains(column.as_str())
            }
            _ => true,
        })
        .cloned()
        .collect();

    if cleaned.len() == predicates.len() {
        return None; // nothing removed
    }

    if cleaned.is_empty() {
        Some(*input.clone()) // no predicates left, unwrap the Filter
    } else {
        Some(PhysOp::Filter {
            predicates: cleaned,
            input: input.clone(),
        })
    }
}

// ── Rule 10: Narrowing CTE for selective endpoints ──────────────────────────

fn rule_narrowing_cte(op: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    // Match: Join(left, Filter(Scan(node_table, alias, FINAL), preds))
    // where the node has high-selectivity filters
    // and there's an edge scan somewhere in `left` that references this node
    let PhysOp::Join {
        left,
        right,
        on,
        kind: JoinKind::Inner,
    } = op
    else {
        return None;
    };
    let node_alias = scan_alias(right)?;
    let node = ctx.input.nodes.iter().find(|n| n.id == node_alias)?;
    if !is_selective(node) || node.table.is_none() {
        return None;
    }

    // Check that left has an edge scan that references this node via the join ON
    let edge_alias_str = &on.left.0;
    if alias_to_rel_index(edge_alias_str).is_none() && !edge_alias_str.starts_with('e') {
        return None; // left side isn't an edge
    }

    // Don't add narrowing if already present
    if has_semi_join_for(left, &node_alias) {
        return None;
    }

    let cte_name = format!("_nf_{node_alias}");
    let edge_col = &on.left.1;

    // Wrap the left (edge chain) in a Semi join with the node scan as CTE
    let narrowed = PhysOp::Join {
        left: left.clone(),
        right: Box::new(filtered_node_scan(node)),
        on: JoinOn {
            left: (edge_alias_str.clone(), edge_col.clone()),
            right: (
                cte_name,
                ontology_constants::DEFAULT_PRIMARY_KEY.to_string(),
            ),
        },
        kind: JoinKind::Semi { materialize: true },
    };

    Some(PhysOp::Join {
        left: Box::new(narrowed),
        right: right.clone(),
        on: on.clone(),
        kind: JoinKind::Inner,
    })
}

fn is_selective(node: &InputNode) -> bool {
    !node.node_ids.is_empty()
        || node.id_range.is_some()
        || node.filters.iter().any(|(_, fs)| {
            fs.iter()
                .any(|f| f.selectivity == ontology::FieldSelectivity::High)
        })
}

fn has_semi_join_for(op: &PhysOp, alias: &str) -> bool {
    match op {
        PhysOp::Join {
            kind: JoinKind::Semi { .. },
            on,
            ..
        } => on.right.0.contains(alias),
        PhysOp::Join { left, right, .. } => {
            has_semi_join_for(left, alias) || has_semi_join_for(right, alias)
        }
        PhysOp::Filter { input, .. }
        | PhysOp::Project { input, .. }
        | PhysOp::Sort { input, .. }
        | PhysOp::Limit { input, .. }
        | PhysOp::Aggregate { input, .. } => has_semi_join_for(input, alias),
        _ => false,
    }
}

// ── Rule 11: Cascade SIP ────────────────────────────────────────────────────

fn rule_cascade_sip(op: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    // Match: Join(prev_chain, Filter(Scan(edge, e{N})), on: prev.col = e{N}.col)
    // where N > 0 and the previous hop has selective endpoints
    let PhysOp::Join {
        left,
        right,
        on,
        kind: JoinKind::Inner,
    } = op
    else {
        return None;
    };
    let edge_alias = edge_scan_alias(right)?;
    let idx = alias_to_rel_index(&edge_alias)?;
    if idx == 0 {
        return None;
    } // first hop, no previous to anchor from

    // Check previous hop has selective nodes
    let prev_rel = ctx.input.relationships.get(idx - 1)?;
    let prev_selective = [&prev_rel.from, &prev_rel.to].iter().any(|na| {
        ctx.input
            .nodes
            .iter()
            .find(|n| &n.id == *na)
            .is_some_and(|n| is_selective(n))
    });
    if !prev_selective {
        return None;
    }

    // Don't add if already has a semi-join
    if matches!(
        right.as_ref(),
        PhysOp::Join {
            kind: JoinKind::Semi { .. },
            ..
        }
    ) {
        return None;
    }

    // Add Semi join: e{N}.start_col IN (SELECT prev.end_col FROM prev WHERE prev_preds)
    let (_, prev_end) = prev_rel.direction.edge_columns();
    let prev_alias = format!("e{}", idx - 1);
    let (curr_start, _) = ctx.input.relationships[idx].direction.edge_columns();

    let new_right = PhysOp::Join {
        left: right.clone(),
        right: Box::new(PhysOp::Scan {
            table: String::new(),
            alias: prev_alias.clone(),
            dedup: false,
        }),
        on: JoinOn {
            left: (edge_alias.clone(), curr_start.to_string()),
            right: (prev_alias, prev_end.to_string()),
        },
        kind: JoinKind::Semi { materialize: false },
    };

    Some(PhysOp::Join {
        left: left.clone(),
        right: Box::new(new_right),
        on: on.clone(),
        kind: JoinKind::Inner,
    })
}

// ── Rule 12: FK edge metadata synthesis ─────────────────────────────────────

fn rule_fk_edge_metadata(op: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    let PhysOp::Project { input, columns } = op else {
        return None;
    };

    // Check if any columns reference edge aliases that don't exist in the tree
    let has_stale_edge_refs = columns.iter().any(|c| match c {
        ProjectedColumn::Ref { table, .. } => {
            table.starts_with('e')
                && alias_to_rel_index(table).is_some()
                && !has_alias(input, table)
        }
        _ => false,
    });
    if !has_stale_edge_refs {
        return None;
    }

    // Rewrite edge column refs to literals/computed values
    let new_columns: Vec<ProjectedColumn> = columns
        .iter()
        .map(|c| match c {
            ProjectedColumn::Ref {
                table,
                column,
                alias,
            } if table.starts_with('e')
                && alias_to_rel_index(table).is_some()
                && !has_alias(input, table) =>
            {
                let idx = alias_to_rel_index(table).unwrap();
                if let Some(rel) = ctx.input.relationships.get(idx) {
                    let rel_type = rel.types.first().map(|s| s.as_str()).unwrap_or("");
                    let from_entity = ctx
                        .input
                        .nodes
                        .iter()
                        .find(|n| n.id == rel.from)
                        .and_then(|n| n.entity.as_deref())
                        .unwrap_or("");
                    let to_entity = ctx
                        .input
                        .nodes
                        .iter()
                        .find(|n| n.id == rel.to)
                        .and_then(|n| n.entity.as_deref())
                        .unwrap_or("");

                    match column.as_str() {
                        c if c == ontology_constants::RELATIONSHIP_KIND_COLUMN => {
                            ProjectedColumn::Computed {
                                expr: ColumnExpr::Lit(Value::Str(rel_type.to_string())),
                                alias: alias.clone(),
                            }
                        }
                        c if c == ontology_constants::SOURCE_ID_COLUMN => {
                            ProjectedColumn::Computed {
                                expr: ColumnExpr::Col(
                                    rel.from.clone(),
                                    ontology_constants::DEFAULT_PRIMARY_KEY.to_string(),
                                ),
                                alias: alias.clone(),
                            }
                        }
                        c if c == ontology_constants::SOURCE_KIND_COLUMN => {
                            ProjectedColumn::Computed {
                                expr: ColumnExpr::Lit(Value::Str(from_entity.to_string())),
                                alias: alias.clone(),
                            }
                        }
                        c if c == ontology_constants::TARGET_ID_COLUMN => {
                            ProjectedColumn::Computed {
                                expr: ColumnExpr::Col(
                                    rel.to.clone(),
                                    ontology_constants::DEFAULT_PRIMARY_KEY.to_string(),
                                ),
                                alias: alias.clone(),
                            }
                        }
                        c if c == ontology_constants::TARGET_KIND_COLUMN => {
                            ProjectedColumn::Computed {
                                expr: ColumnExpr::Lit(Value::Str(to_entity.to_string())),
                                alias: alias.clone(),
                            }
                        }
                        _ => c.clone(),
                    }
                } else {
                    c.clone()
                }
            }
            _ => c.clone(),
        })
        .collect();

    if new_columns == *columns {
        return None;
    }
    Some(PhysOp::Project {
        input: input.clone(),
        columns: new_columns,
    })
}

// ── Rule 13: Scope anchor elision ───────────────────────────────────────────

fn rule_scope_anchor_elision(op: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    if ctx.input.query_type != QueryType::Aggregation {
        return None;
    }
    let PhysOp::Join {
        left,
        right,
        kind: JoinKind::Inner,
        ..
    } = op
    else {
        return None;
    };
    let alias = scan_alias(right)?;
    let node = ctx.input.nodes.iter().find(|n| n.id == alias)?;

    // Pure scope anchor: only scope filters, not in group-by/metrics/order-by
    let in_group_by = ctx
        .input
        .aggregation
        .group_by
        .iter()
        .any(|g| g.node() == alias.as_str());
    let in_metrics = ctx
        .input
        .aggregation
        .metrics
        .iter()
        .any(|m| m.expr.node() == alias.as_str());
    let in_order_by = ctx
        .input
        .order_by
        .as_ref()
        .is_some_and(|ob| ob.node == alias);
    if in_group_by || in_metrics || in_order_by {
        return None;
    }

    // Must have only scope-related filters (no user-visible filters)
    let has_only_scope =
        node.filters.is_empty() && node.node_ids.is_empty() && node.id_range.is_none();
    if !has_only_scope {
        return None;
    }

    // Check if a scope prefix has been resolved (restrict sets this)
    let has_scope = ctx
        .input
        .relationships
        .iter()
        .any(|r| (r.from == alias || r.to == alias) && r.scope_prefix.is_some());
    if !has_scope {
        return None;
    }

    Some(*left.clone())
}

// ── Rule 14: Count target FK re-join ────────────────────────────────────────

fn rule_count_target_fk_rejoin(op: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    if ctx.input.query_type != QueryType::Aggregation {
        return None;
    }
    let PhysOp::Aggregate {
        input,
        group_by,
        metrics,
    } = op
    else {
        return None;
    };

    for m in metrics {
        let node_alias = &m.node;
        if !has_alias(input, node_alias) {
            // Count target not in tree — need to re-join
            let node = ctx.input.nodes.iter().find(|n| &n.id == node_alias)?;
            // Find the FK relationship that connects this node
            for rel in &ctx.input.relationships {
                if let Some(ref fk_col) = rel.fk_column {
                    let (fk_alias, tgt_alias) = fk_sides(rel, fk_col, ctx);
                    if tgt_alias == node_alias || fk_alias == node_alias {
                        let new_input = PhysOp::Join {
                            left: input.clone(),
                            right: Box::new(filtered_node_scan(node)),
                            on: JoinOn {
                                left: (fk_alias.to_string(), fk_col.clone()),
                                right: (
                                    node_alias.clone(),
                                    ontology_constants::DEFAULT_PRIMARY_KEY.to_string(),
                                ),
                            },
                            kind: JoinKind::Inner,
                        };
                        return Some(PhysOp::Aggregate {
                            input: Box::new(new_input),
                            group_by: group_by.clone(),
                            metrics: metrics.clone(),
                        });
                    }
                }
            }
        }
    }
    None
}

// ── Rule 15: LIMIT BY for single-hop aggregation ────────────────────────────
// Single-hop edge aggregation: use dedup=false (LIMIT BY in lower) instead of FINAL

fn rule_limit_by_single_hop_agg(_op: &PhysOp, _ctx: &RuleCtx) -> Option<PhysOp> {
    // TODO: This requires lower_v2 to emit LIMIT BY instead of FINAL when dedup=false
    // on an edge scan inside an Aggregate. For now, skip.
    None
}
