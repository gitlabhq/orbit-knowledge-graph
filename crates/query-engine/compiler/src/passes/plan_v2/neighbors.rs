//! Neighbors: every edge touching a center node, per direction. Emits its
//! own `_gkg_*` redaction columns since the center's edge column differs per
//! direction arm.

use super::chain::kind_col;
use super::expr::on;
use super::*;
use crate::constants::*;
use crate::{pe, pn};

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
    /// Physical edge tables whose relationships can have the center on the
    /// given side (`edge_source_kinds` / `edge_target_kinds`); all tables
    /// when no relationship qualifies.
    fn tables_for_side(
        &self,
        rel_types: &[String],
        entity: &str,
        kinds: &HashMap<String, Vec<String>>,
    ) -> Vec<String> {
        let meta = &self.input.compiler;
        let rels: Vec<&String> = if rel_types.is_empty() {
            meta.edge_table_for_rel.keys().collect()
        } else {
            rel_types.iter().collect()
        };
        let mut t: Vec<String> = rels
            .into_iter()
            .filter(|r| {
                kinds
                    .get(*r)
                    .is_some_and(|ks| ks.iter().any(|k| k == entity))
            })
            .map(|r| {
                meta.edge_table_for_rel
                    .get(r)
                    .cloned()
                    .unwrap_or_else(|| meta.default_edge_table.clone())
            })
            .collect();
        t.sort();
        t.dedup();
        if t.is_empty() {
            meta.resolve_edge_tables(rel_types)
        } else {
            t
        }
    }

    pub fn plan_neighbors(&self, limit: u32) -> PhysOp {
        let config = self.input.neighbors.as_ref().expect("neighbors config");
        let center = &self.input.nodes[0];
        let cid = center.id.as_str();
        let entity = center.entity.as_deref().unwrap_or("");
        let meta = &self.input.compiler;
        let all_tables = meta.resolve_edge_tables(&config.rel_types);
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
        let both = config.direction == Direction::Both;

        let build_arm = |arm: &Arm| -> PhysOp {
            let kinds = if arm.dir == Direction::Outgoing {
                &meta.edge_source_kinds
            } else {
                &meta.edge_target_kinds
            };
            let tables = self.tables_for_side(&config.rel_types, entity, kinds);
            let center_kind = kind_col(arm.center_id);
            let mut base = self.edge_scan(&tables, e, |a| {
                let mut p = vec![pe!("{a}.{center_kind} = {entity:?}")];
                if !center.node_ids.is_empty() {
                    p.push(id_in(a, arm.center_id, &center.node_ids));
                }
                p.extend(rel_kind(a, &config.rel_types));
                p.push(deleted_false(a));
                p
            });
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
                    on(&[&format!("{e}.traversal_path = _tpd.traversal_path")])[0].clone(),
                );
            }
            // Denorm tags aren't in the per-arm projection, so they filter
            // the union output alias.
            base = base.filter(self.denorm_tags(center, arm.denorm_dir, e, None));

            let (nb, nb_kind, is_out) =
                (arm.neighbor_id, kind_col(arm.neighbor_id), arm.is_outgoing);
            let mut columns = vec![
                pn!("{e}.{nb} AS {}", neighbor_id_column()),
                pn!("{e}.{nb_kind} AS {}", neighbor_type_column()),
                pn!("{e}.relationship_kind AS {}", relationship_type_column()),
                pn!("{is_out} AS {}", neighbor_is_outgoing_column()),
            ];
            let center_on = on(&[&format!("{e}.{} = {cid}.id", arm.center_id)]);
            if has_non_denorm {
                base = base.join(self.node_scan(center), center_on.clone());
            }
            if default_pk {
                columns.push(pn!("{e}.{} AS {}", arm.center_id, redaction_id_column(cid)));
            } else {
                if !has_non_denorm {
                    let table = center.table.as_deref().unwrap_or("");
                    base = base.join(
                        scan(table, cid, Dedup::Final).filter(vec![deleted_false(cid)]),
                        center_on,
                    );
                }
                columns.push(pn!(
                    "{cid}.{} AS {}",
                    center.redaction_id_column,
                    redaction_id_column(cid)
                ));
                columns.push(pn!("{cid}.id AS {}", primary_key_column(cid)));
            }
            columns.push(pn!("{entity:?} AS {}", redaction_type_column(cid)));
            if center.has_traversal_path {
                columns.push(pn!("{e}.traversal_path AS {}", traversal_path_column(cid)));
            }
            base.project(columns)
        };

        let mut keys = self.sort_keys();
        if self.input.cursor.is_some() {
            let tie: Vec<String> = if both {
                vec![
                    redaction_id_column(cid),
                    neighbor_id_column().into(),
                    relationship_type_column().into(),
                    neighbor_is_outgoing_column().into(),
                ]
            } else {
                [SOURCE_ID_COLUMN, TARGET_ID_COLUMN, RELATIONSHIP_KIND_COLUMN]
                    .iter()
                    .map(|c| format!("{e}.{c}"))
                    .collect()
            };
            keys.extend(tie.iter().map(|k| (pe!("{k}"), false)));
        }

        // Default-PK center, denorm-only filters, one physical table: both
        // directions collapse into one scan.
        let body = if both && !has_non_denorm && default_pk && all_tables.len() == 1 {
            self.fused_both_arm(center, entity, &all_tables[0], e, &config.rel_types)
        } else if both {
            PhysOp::union(vec![build_arm(&OUT), build_arm(&IN)], "_union")
        } else {
            build_arm(if config.direction == Direction::Outgoing {
                &OUT
            } else {
                &IN
            })
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
            let mut parts = vec![pe!("{e}.{} = {entity:?}", kind_col(arm.center_id))];
            if !center.node_ids.is_empty() {
                parts.push(id_in(e, arm.center_id, &center.node_ids));
            }
            parts.extend(self.denorm_tags(center, arm.denorm_dir, e, None));
            PExpr::And(parts)
        };
        let (src, tgt) = (side(&OUT), side(&IN));
        // (matched, is_outgoing, neighbor_id, neighbor_kind, center_id)
        let tuple = |matched: &PExpr, arm: &Arm| {
            let mut rest = pe!(
                "tuple({}, {e}.{}, {e}.{}, {e}.{})",
                arm.is_outgoing,
                arm.neighbor_id,
                kind_col(arm.neighbor_id),
                arm.center_id
            );
            if let PExpr::Func(_, args) = &mut rest {
                args.insert(0, matched.clone());
            }
            rest
        };
        let rows = PExpr::Func("array".into(), vec![tuple(&src, &OUT), tuple(&tgt, &IN)]);
        let matched = PExpr::Func(
            "arrayFilter".into(),
            vec![pe!("_gkg_arm -> tupleElement(_gkg_arm, 1)"), rows],
        );
        let (rel_col, tp_col) = (relationship_type_column(), traversal_path_column(cid));
        let mut inner_cols = vec![
            named(
                PExpr::Func("arrayJoin".into(), vec![matched]),
                "_gkg_arm_row",
            ),
            pn!("{e}.relationship_kind AS {rel_col}"),
        ];
        if center.has_traversal_path {
            inner_cols.push(pn!("{e}.traversal_path AS {tp_col}"));
        }
        let mut inner_where = vec![PExpr::Or(vec![src, tgt])];
        inner_where.extend(rel_kind(e, rel_types));
        inner_where.push(deleted_false(e));

        let mut columns = vec![
            pn!(
                "tupleElement({e}._gkg_arm_row, 3) AS {}",
                neighbor_id_column()
            ),
            pn!(
                "tupleElement({e}._gkg_arm_row, 4) AS {}",
                neighbor_type_column()
            ),
            pn!("{e}.{rel_col} AS {rel_col}"),
            pn!(
                "tupleElement({e}._gkg_arm_row, 2) AS {}",
                neighbor_is_outgoing_column()
            ),
            pn!(
                "tupleElement({e}._gkg_arm_row, 5) AS {}",
                redaction_id_column(cid)
            ),
            pn!("{entity:?} AS {}", redaction_type_column(cid)),
        ];
        if center.has_traversal_path {
            columns.push(pn!("{e}.{tp_col} AS {tp_col}"));
        }
        scan(table, e, Dedup::None)
            .filter(inner_where)
            .project(inner_cols)
            .project(columns)
    }
}
