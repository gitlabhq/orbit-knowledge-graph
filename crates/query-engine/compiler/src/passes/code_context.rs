//! Fail-closed source-code context injection and effective-view rewriting.

use std::collections::HashSet;

use ontology::Ontology;

use crate::ast::{Expr, Node, OrderExpr, Query, SelectExpr, TableRef};
use crate::error::{QueryError, Result};
use crate::input::{CodeContext, CodeContextState, Input};

pub fn apply(node: &mut Node, input: &Input, ontology: &Ontology) -> Result<()> {
    let code_tables = code_tables(ontology);
    let scans_code = match node {
        Node::Query(query) => query_scans_code(query, &code_tables),
        Node::Insert(_) => false,
    };
    if !scans_code {
        return Ok(());
    }

    let context = match input.code_contexts.as_slice() {
        [] => {
            return Err(QueryError::Authorization(
                "code scans require one resolved code_context; default-branch fallback is disabled"
                    .into(),
            ));
        }
        [context] => context,
        _ => {
            return Err(QueryError::Validation(
                "the PoC supports exactly one code_context".into(),
            ));
        }
    };
    if context.state != CodeContextState::Ready {
        return Err(QueryError::Authorization(format!(
            "code_context for project {} is {:?}, not ready",
            context.project_id, context.state
        )));
    }

    match node {
        Node::Query(query) => rewrite_query(query, context, &code_tables),
        Node::Insert(_) => Ok(()),
    }
}

fn code_tables(ontology: &Ontology) -> HashSet<String> {
    let mut tables: HashSet<_> = ontology
        .nodes()
        .filter(|node| node.domain == "source_code")
        .map(|node| node.destination_table.clone())
        .collect();
    if ontology
        .edge_tables()
        .into_iter()
        .any(|table| table.contains("code_edge"))
    {
        tables.extend(
            ontology
                .edge_tables()
                .into_iter()
                .filter(|table| table.contains("code_edge"))
                .map(str::to_string),
        );
    }
    tables
}

fn query_scans_code(query: &Query, code_tables: &HashSet<String>) -> bool {
    query
        .ctes
        .iter()
        .any(|cte| query_scans_code(&cte.query, code_tables))
        || table_ref_scans_code(&query.from, code_tables)
        || query
            .where_clause
            .as_ref()
            .is_some_and(|expr| expr_scans_code(expr, code_tables))
        || query
            .having
            .as_ref()
            .is_some_and(|expr| expr_scans_code(expr, code_tables))
        || query
            .union_all
            .iter()
            .any(|arm| query_scans_code(arm, code_tables))
}

fn expr_scans_code(expr: &Expr, code_tables: &HashSet<String>) -> bool {
    match expr {
        Expr::InSelect { expr, query } => {
            expr_scans_code(expr, code_tables) || query_scans_code(query, code_tables)
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_scans_code(left, code_tables) || expr_scans_code(right, code_tables)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Lambda { body: expr, .. }
        | Expr::InSubquery { expr, .. } => expr_scans_code(expr, code_tables),
        Expr::FuncCall { args, .. } => args
            .iter()
            .any(|argument| expr_scans_code(argument, code_tables)),
        Expr::Column { .. }
        | Expr::Identifier(_)
        | Expr::Literal(_)
        | Expr::Param { .. }
        | Expr::Star => false,
    }
}

fn table_ref_scans_code(table_ref: &TableRef, code_tables: &HashSet<String>) -> bool {
    match table_ref {
        TableRef::Scan { table, .. } => code_tables.contains(unprefixed(table)),
        TableRef::Join { left, right, .. } => {
            table_ref_scans_code(left, code_tables) || table_ref_scans_code(right, code_tables)
        }
        TableRef::Union { queries, .. } => queries
            .iter()
            .any(|query| query_scans_code(query, code_tables)),
        TableRef::Subquery { query, .. } => query_scans_code(query, code_tables),
    }
}

fn rewrite_query(
    query: &mut Query,
    context: &CodeContext,
    code_tables: &HashSet<String>,
) -> Result<()> {
    for cte in &mut query.ctes {
        rewrite_query(&mut cte.query, context, code_tables)?;
    }
    rewrite_table_ref(&mut query.from, context, code_tables)?;
    rewrite_expr(query.where_clause.as_mut(), context, code_tables)?;
    rewrite_expr(query.having.as_mut(), context, code_tables)?;
    for arm in &mut query.union_all {
        rewrite_query(arm, context, code_tables)?;
    }
    Ok(())
}

fn rewrite_table_ref(
    table_ref: &mut TableRef,
    context: &CodeContext,
    code_tables: &HashSet<String>,
) -> Result<()> {
    match table_ref {
        TableRef::Scan {
            table,
            alias,
            final_,
        } if code_tables.contains(unprefixed(table)) => {
            let source_table = table.clone();
            let output_alias = alias.clone();
            let final_ = *final_;
            let base_alias = format!("{output_alias}_base");
            let overlay_alias = format!("{output_alias}_overlay");
            let base = context_arm(
                &source_table,
                &base_alias,
                &context.base_ref,
                context.project_id,
                final_,
            );
            let mut overlay = context_arm(
                &source_table,
                &overlay_alias,
                &context.ref_,
                context.project_id,
                final_,
            );
            overlay.union_all.push(base);
            let union_alias = format!("{output_alias}_contexts");
            let identity = effective_identity(unprefixed(&source_table), &union_alias);
            let effective = Query {
                select: vec![SelectExpr::star()],
                from: TableRef::subquery(overlay, &union_alias),
                order_by: vec![OrderExpr::desc(Expr::eq(
                    Expr::col(&union_alias, "branch"),
                    Expr::string(&context.ref_),
                ))],
                limit_by: Some((1, identity)),
                ..Query::default()
            };
            *table_ref = TableRef::subquery(effective, output_alias);
            Ok(())
        }
        TableRef::Scan { .. } => Ok(()),
        TableRef::Join { left, right, .. } => {
            rewrite_table_ref(left, context, code_tables)?;
            rewrite_table_ref(right, context, code_tables)
        }
        TableRef::Union { queries, .. } => {
            for query in queries {
                rewrite_query(query, context, code_tables)?;
            }
            Ok(())
        }
        TableRef::Subquery { query, .. } => rewrite_query(query, context, code_tables),
    }
}

fn effective_identity(table: &str, alias: &str) -> Vec<Expr> {
    if table.contains("code_edge") {
        [
            "source_id",
            "source_kind",
            "relationship_kind",
            "target_id",
            "target_kind",
        ]
        .into_iter()
        .map(|column| Expr::col(alias, column))
        .collect()
    } else {
        vec![Expr::col(alias, "id")]
    }
}

fn context_arm(table: &str, alias: &str, branch: &str, project_id: i64, final_: bool) -> Query {
    Query {
        select: vec![SelectExpr::star()],
        from: TableRef::Scan {
            table: table.to_string(),
            alias: alias.to_string(),
            final_,
        },
        where_clause: Some(Expr::and(
            Expr::eq(Expr::col(alias, "project_id"), Expr::int(project_id)),
            Expr::eq(Expr::col(alias, "branch"), Expr::string(branch)),
        )),
        ..Query::default()
    }
}

fn rewrite_expr(
    expr: Option<&mut Expr>,
    context: &CodeContext,
    code_tables: &HashSet<String>,
) -> Result<()> {
    let Some(expr) = expr else {
        return Ok(());
    };
    match expr {
        Expr::InSelect { expr, query } => {
            rewrite_expr(Some(expr), context, code_tables)?;
            rewrite_query(query, context, code_tables)
        }
        Expr::BinaryOp { left, right, .. } => {
            rewrite_expr(Some(left), context, code_tables)?;
            rewrite_expr(Some(right), context, code_tables)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Lambda { body: expr, .. }
        | Expr::InSubquery { expr, .. } => rewrite_expr(Some(expr), context, code_tables),
        Expr::FuncCall { args, .. } => {
            for argument in args {
                rewrite_expr(Some(argument), context, code_tables)?;
            }
            Ok(())
        }
        Expr::Column { .. }
        | Expr::Identifier(_)
        | Expr::Literal(_)
        | Expr::Param { .. }
        | Expr::Star => Ok(()),
    }
}

fn unprefixed(table: &str) -> &str {
    table
        .strip_prefix('v')
        .and_then(|tail| tail.split_once('_'))
        .filter(|(version, name)| {
            version.chars().all(|character| character.is_ascii_digit()) && name.starts_with("gl_")
        })
        .map_or(table, |(_, name)| name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{JoinType, Node};

    fn context(state: CodeContextState) -> CodeContext {
        CodeContext {
            project_id: 42,
            ref_: "feature".into(),
            commit_sha: Some("head".into()),
            base_ref: "main".into(),
            indexed_sha: "head".into(),
            base_sha: "base".into(),
            generation: 7,
            state,
        }
    }

    fn code_query() -> Node {
        Node::Query(Box::new(Query {
            select: vec![SelectExpr::star()],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("gl_definition", "d"),
                TableRef::scan("gl_code_edge", "e"),
                Expr::eq(Expr::col("d", "id"), Expr::col("e", "target_id")),
            ),
            ..Query::default()
        }))
    }

    #[test]
    fn rejects_uncontextualized_code_scans() {
        let mut node = code_query();
        let error = apply(
            &mut node,
            &Input::default(),
            &Ontology::load_embedded().unwrap(),
        )
        .expect_err("code scans must fail closed");
        assert!(
            error
                .to_string()
                .contains("default-branch fallback is disabled")
        );
    }

    #[test]
    fn rejects_non_ready_contexts() {
        let mut node = code_query();
        let input = Input {
            code_contexts: vec![context(CodeContextState::Building)],
            ..Input::default()
        };
        assert!(apply(&mut node, &input, &Ontology::load_embedded().unwrap()).is_err());
    }

    #[test]
    fn rejects_code_scan_reachable_only_from_where_subquery() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::star()],
            from: TableRef::scan("gl_project", "p"),
            where_clause: Some(Expr::InSelect {
                expr: Box::new(Expr::col("p", "id")),
                query: Box::new(Query {
                    select: vec![SelectExpr::col("e", "source_id")],
                    from: TableRef::scan("gl_code_edge", "e"),
                    ..Query::default()
                }),
            }),
            ..Query::default()
        }));

        let error = apply(
            &mut node,
            &Input::default(),
            &Ontology::load_embedded().unwrap(),
        )
        .expect_err("predicate subqueries that scan code must fail closed");
        assert!(
            error
                .to_string()
                .contains("default-branch fallback is disabled")
        );
    }

    #[test]
    fn rewrites_every_code_scan_to_base_and_overlay() {
        let mut node = code_query();
        let input = Input {
            code_contexts: vec![context(CodeContextState::Ready)],
            ..Input::default()
        };
        apply(&mut node, &input, &Ontology::load_embedded().unwrap()).unwrap();
        let rendered = crate::passes::codegen::codegen(
            &node,
            crate::passes::enforce::ResultContext::default(),
            Default::default(),
        )
        .unwrap()
        .render();
        assert_eq!(rendered.matches("branch = 'feature'").count(), 4);
        assert_eq!(rendered.matches("branch = 'main'").count(), 2);
        assert_eq!(rendered.matches("project_id = 42").count(), 4);
        assert_eq!(rendered.matches("LIMIT 1 BY").count(), 2);
    }

    #[test]
    fn leaves_sdlc_scans_unchanged_without_a_context() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::star()],
            from: TableRef::scan("gl_project", "p"),
            ..Query::default()
        }));
        apply(
            &mut node,
            &Input::default(),
            &Ontology::load_embedded().unwrap(),
        )
        .unwrap();
    }
}
