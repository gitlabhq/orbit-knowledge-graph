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
    let PhysOp::Filter {
        predicates: _,
        input,
    } = op
    else {
        return None;
    };
    let PhysOp::Scan { table, alias, .. } = input.as_ref() else {
        return None;
    };
    if !table.contains("edge") {
        return None;
    }
    let idx = alias_to_rel_index(alias)?;
    let rel = ctx.input.relationships.get(idx)?;
    let fk_col = rel.fk_column.as_ref()?;
    if rel.hops.max != 1 || matches!(rel.direction, Direction::Both) || !rel.filters.is_empty() {
        return None;
    }

    let (fk_alias, tgt_alias) = fk_sides(rel, fk_col, ctx);
    let fk_node = ctx.input.nodes.iter().find(|n| n.id == fk_alias)?;
    let tgt_node = ctx.input.nodes.iter().find(|n| n.id == tgt_alias)?;

    Some(PhysOp::Join {
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
    })
}

// ── Rule 5: Prune duplicate node joins ──────────────────────────────────────
// After FK elision, the naive plan's node joins become redundant

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
