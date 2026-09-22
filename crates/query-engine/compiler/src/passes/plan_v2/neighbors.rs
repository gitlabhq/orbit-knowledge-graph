//! Neighbors: every edge touching a center node, per direction. Emits its
//! own `_gkg_*` redaction columns since the center's edge column differs per
//! direction arm.

use super::*;
use crate::constants::*;

/// Physical edge tables per direction, narrowed to the tables whose
/// relationships can have the center as source (outgoing) or target.
struct EdgeTables {
    all: Vec<String>,
    outgoing: Vec<String>,
    incoming: Vec<String>,
}

/// Column roles of one direction arm.
struct Arm {
    dir: Direction,
    center_id: &'static str,
    neighbor_id: &'static str,
    denorm_dir: &'static str,
    is_outgoing: i64,
}

const OUT: Arm = Arm {
    dir: Direction::Outgoing,
    center_id: SOURCE_ID_COLUMN,
    neighbor_id: TARGET_ID_COLUMN,
    denorm_dir: "source",
    is_outgoing: 1,
};
const IN: Arm = Arm {
    dir: Direction::Incoming,
    center_id: TARGET_ID_COLUMN,
    neighbor_id: SOURCE_ID_COLUMN,
    denorm_dir: "target",
    is_outgoing: 0,
};

impl<'a> PlanCtx<'a> {
    fn neighbor_edge_tables(&self, rel_types: &[String], entity: &str) -> EdgeTables {
        let meta = &self.input.compiler;
        let all = meta.resolve_edge_tables(rel_types);
        let rels: Vec<&String> = if rel_types.is_empty() {
            meta.edge_table_for_rel.keys().collect()
        } else {
            rel_types.iter().collect()
        };
        let tables_for = |kinds: &HashMap<String, Vec<String>>| -> Vec<String> {
            let mut t: Vec<String> = rels
                .iter()
                .filter(|r| {
                    kinds
                        .get(**r)
                        .is_some_and(|ks| ks.iter().any(|k| k == entity))
                })
                .map(|r| {
                    meta.edge_table_for_rel
                        .get(*r)
                        .cloned()
                        .unwrap_or_else(|| meta.default_edge_table.clone())
                })
                .collect();
            t.sort();
            t.dedup();
            if t.is_empty() { all.clone() } else { t }
        };
        EdgeTables {
            outgoing: tables_for(&meta.edge_source_kinds),
            incoming: tables_for(&meta.edge_target_kinds),
            all,
        }
    }

    pub fn plan_neighbors(&self, limit: u32) -> PhysOp {
        let config = self.input.neighbors.as_ref().expect("neighbors config");
        let center = &self.input.nodes[0];
        let cid = center.id.as_str();
        let entity = center.entity.as_deref().unwrap_or("");
        let meta = &self.input.compiler;
        let tables = self.neighbor_edge_tables(&config.rel_types, entity);
        let default_pk = center.redaction_id_column == DEFAULT_PRIMARY_KEY;
        let has_non_denorm = center.id_range.is_some()
            || center.filters.keys().any(|prop| {
                !["source", "target"].iter().any(|d| {
                    meta.denormalized_columns.contains_key(&(
                        entity.to_string(),
                        prop.clone(),
                        d.to_string(),
                    ))
                })
            });
        let e = "e";

        // Center-side predicates on an edge alias for one direction.
        let center_preds = |a: &str, arm: &Arm| -> Vec<PExpr> {
            let mut p = vec![eq(
                col(a, super::chain::kind_col(arm.center_id)),
                lit(entity),
            )];
            if !center.node_ids.is_empty() {
                p.push(id_in(a, arm.center_id, &center.node_ids));
            }
            p.extend(rel_kind(a, &config.rel_types));
            p.push(deleted_false(a));
            p
        };

        let build_arm = |arm: &Arm| -> PhysOp {
            let arm_tables = if arm.dir == Direction::Outgoing {
                &tables.outgoing
            } else {
                &tables.incoming
            };
            let mut base = self.edge_scan(arm_tables, e, |a| center_preds(a, arm));
            // Incoming edges to a namespace center sit at the center's own
            // tp; pin to the resolved paths for a leading-PK point lookup.
            if arm.dir == Direction::Incoming
                && !center.node_ids.is_empty()
                && let Some((dict, key_col)) = meta.tp_id_lookup.get(entity)
            {
                let lookup = scan(dict, "_tpd", Dedup::None).filter(vec![
                    id_in("_tpd", key_col, &center.node_ids),
                    deleted_false("_tpd"),
                ]);
                base = base.semi(
                    lookup,
                    (
                        (e.into(), TRAVERSAL_PATH_COLUMN.into()),
                        ("_tpd".into(), TRAVERSAL_PATH_COLUMN.into()),
                    ),
                );
            }
            // Denorm tags aren't in the per-arm projection, so they filter
            // the union output alias.
            base = base.filter(self.denorm_tags(center, arm.denorm_dir, e, None));

            let mut columns = vec![
                named(col(e, arm.neighbor_id), neighbor_id_column()),
                named(
                    col(e, super::chain::kind_col(arm.neighbor_id)),
                    neighbor_type_column(),
                ),
                named(col(e, RELATIONSHIP_KIND_COLUMN), relationship_type_column()),
                named(lit(arm.is_outgoing), neighbor_is_outgoing_column()),
            ];
            let on = vec![(
                (e.into(), arm.center_id.into()),
                (cid.into(), DEFAULT_PRIMARY_KEY.into()),
            )];
            if has_non_denorm {
                base = base.join(self.node_scan(center), on.clone());
            }
            if default_pk {
                columns.push(named(col(e, arm.center_id), redaction_id_column(cid)));
            } else {
                if !has_non_denorm {
                    let table = center.table.as_deref().unwrap_or("");
                    let node = scan(table, cid, Dedup::Final).filter(vec![deleted_false(cid)]);
                    base = base.join(node, on);
                }
                columns.push(named(
                    col(cid, &center.redaction_id_column),
                    redaction_id_column(cid),
                ));
                columns.push(named(
                    col(cid, DEFAULT_PRIMARY_KEY),
                    primary_key_column(cid),
                ));
            }
            columns.push(named(lit(entity), redaction_type_column(cid)));
            if center.has_traversal_path {
                columns.push(named(
                    col(e, TRAVERSAL_PATH_COLUMN),
                    traversal_path_column(cid),
                ));
            }
            base.project(columns)
        };

        let both = config.direction == Direction::Both;
        let mut keys = self.sort_keys();
        if self.input.cursor.is_some() {
            let tie: Vec<PExpr> = if both {
                [
                    redaction_id_column(cid),
                    neighbor_id_column().to_string(),
                    relationship_type_column().to_string(),
                    neighbor_is_outgoing_column().to_string(),
                ]
                .into_iter()
                .map(PExpr::Ident)
                .collect()
            } else {
                [SOURCE_ID_COLUMN, TARGET_ID_COLUMN, RELATIONSHIP_KIND_COLUMN]
                    .iter()
                    .map(|c| col(e, c))
                    .collect()
            };
            keys.extend(tie.into_iter().map(|k| (k, false)));
        }

        // Default-PK center, denorm-only filters, one physical table: both
        // directions collapse into one scan.
        let body = if both && !has_non_denorm && default_pk && tables.all.len() == 1 {
            self.fused_both_arm(center, entity, &tables.all[0], e, &config.rel_types)
        } else if both {
            PhysOp::union(vec![build_arm(&OUT), build_arm(&IN)], "_union")
        } else if config.direction == Direction::Outgoing {
            build_arm(&OUT)
        } else {
            build_arm(&IN)
        };
        body.sort(keys).limit(limit)
    }

    /// Scan the edge once with `WHERE (source side) OR (target side)`, then
    /// `arrayJoin(arrayFilter(matched, [out_tuple, in_tuple]))` so each row
    /// yields one entry per matched arm; a self-loop still yields two rows.
    fn fused_both_arm(
        &self,
        center: &InputNode,
        entity: &str,
        table: &str,
        e: &str,
        rel_types: &[String],
    ) -> PhysOp {
        let cid = center.id.as_str();
        let side = |arm: &Arm| -> PExpr {
            let mut parts = vec![eq(
                col(e, super::chain::kind_col(arm.center_id)),
                lit(entity),
            )];
            if !center.node_ids.is_empty() {
                parts.push(id_in(e, arm.center_id, &center.node_ids));
            }
            parts.extend(self.denorm_tags(center, arm.denorm_dir, e, None));
            PExpr::And(parts)
        };
        let (source_arm, target_arm) = (side(&OUT), side(&IN));
        // (matched, is_outgoing, neighbor_id, neighbor_kind, center_id)
        let tuple = |matched: &PExpr, arm: &Arm| {
            func(
                "tuple",
                vec![
                    matched.clone(),
                    lit(arm.is_outgoing),
                    col(e, arm.neighbor_id),
                    col(e, super::chain::kind_col(arm.neighbor_id)),
                    col(e, arm.center_id),
                ],
            )
        };
        let matched = func(
            "arrayFilter",
            vec![
                PExpr::Lambda(
                    "_gkg_arm".into(),
                    Box::new(func(
                        "tupleElement",
                        vec![PExpr::Ident("_gkg_arm".into()), lit(1)],
                    )),
                ),
                func(
                    "array",
                    vec![tuple(&source_arm, &OUT), tuple(&target_arm, &IN)],
                ),
            ],
        );
        const ROW: &str = "_gkg_arm_row";
        let rel_col = relationship_type_column();
        let tp_col = traversal_path_column(cid);
        let mut inner_cols = vec![
            named(func("arrayJoin", vec![matched]), ROW),
            named(col(e, RELATIONSHIP_KIND_COLUMN), rel_col),
        ];
        if center.has_traversal_path {
            inner_cols.push(named(col(e, TRAVERSAL_PATH_COLUMN), tp_col.clone()));
        }
        let mut inner_where = vec![PExpr::Or(vec![source_arm, target_arm])];
        inner_where.extend(rel_kind(e, rel_types));
        inner_where.push(deleted_false(e));
        let inner = scan(table, e, Dedup::None)
            .filter(inner_where)
            .project(inner_cols);

        let te = |n: i64| func("tupleElement", vec![col(e, ROW), lit(n)]);
        let mut columns = vec![
            named(te(3), neighbor_id_column()),
            named(te(4), neighbor_type_column()),
            named(col(e, rel_col), rel_col),
            named(te(2), neighbor_is_outgoing_column()),
            named(te(5), redaction_id_column(cid)),
            named(lit(entity), redaction_type_column(cid)),
        ];
        if center.has_traversal_path {
            columns.push(named(col(e, &tp_col), tp_col.clone()));
        }
        inner.project(columns)
    }
}
