use std::sync::{Arc, LazyLock};

use orbit_utils::query_types::SqlType;
use query_data_model::implementations::relational::plan::{
    AggregateFunction, BinaryOp, Expr, JoinKind, Measure, Order, ReadMode, RelationalError,
};
use query_data_model::plan::tracking::Declare;
use query_data_model::{ClickHouseDataModel, DuckDbDataModel};
use serde_json::json;

static ONTOLOGY: LazyLock<Arc<ontology::Ontology>> =
    LazyLock::new(|| Arc::new(ontology::Ontology::load_embedded().unwrap()));

fn clickhouse() -> ClickHouseDataModel {
    ClickHouseDataModel::derive(Arc::clone(&ONTOLOGY)).unwrap()
}

fn duckdb() -> DuckDbDataModel {
    let mut ontology = ONTOLOGY.as_ref().clone();
    ontology.remove_data_model_optimizations();
    DuckDbDataModel::derive(Arc::new(ontology)).unwrap()
}

fn eq(left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn text(value: &str) -> Expr {
    Expr::Parameter {
        data_type: SqlType::String,
        value: json!(value),
    }
}

#[test]
fn a_clickhouse_plan_renders_clauses_in_sql_order_with_bound_parameters() {
    let model = clickhouse();
    let catalog = model.catalog();
    let users = catalog.table("gl_user").unwrap();
    let plan = catalog
        .build(|q| {
            let rows = q.scan(users, ReadMode::Current, "u")?;
            let id = rows.qualified("u", "id")?;
            let name = rows.qualified("u", "username")?;
            let rows = q.filter(rows, eq(name.expr(), text("root")))?;
            let (rows, computed) = q.compute(rows, vec![Declare::named("handle", name.expr())])?;
            let order = Order {
                value: id.expr(),
                descending: true,
            };
            let rows = q.order(rows, vec![order])?;
            let rows = q.limit(rows, Some(10), Some(20))?;
            q.settings([("max_threads".to_string(), "4".to_string())])?;
            q.select(rows, vec![id, computed[0]])
        })
        .unwrap();
    let query = catalog.render(&plan);
    assert_eq!(
        query.sql,
        "SELECT u.id, u.username AS handle FROM gl_user AS u FINAL \
         WHERE (u.username = {p0:String}) ORDER BY u.id DESC LIMIT 10 OFFSET 20 \
         SETTINGS max_threads = 4"
    );
    assert_eq!(query.parameters["p0"].value, json!("root"));
}

#[test]
fn shared_bodies_membership_and_latest_rows_render_for_clickhouse() {
    let model = clickhouse();
    let catalog = model.catalog();
    let users = catalog.table("gl_user").unwrap();
    let edges = catalog.table("gl_edge").unwrap();
    let plan = catalog
        .build(|q| {
            let authors = q.cte("authors", true, |q| {
                let rows = q.scan(edges, ReadMode::Raw, "e")?;
                let source = rows.qualified("e", "source_id")?;
                let kind = rows.qualified("e", "relationship_kind")?;
                let rows = q.filter(rows, eq(kind.expr(), text("AUTHORED")))?;
                q.select(rows, vec![source])
            })?;
            let keys = q.subquery("keys", |q| {
                let rows = q.read(&authors, None)?;
                let source = rows.column("source_id")?;
                q.select(rows, vec![source])
            })?;
            let rows = q.scan(users, ReadMode::Raw, "u")?;
            let id = rows.qualified("u", "id")?;
            let version = rows.qualified("u", "_version")?;
            let rows = q.filter(rows, keys.contains(id.expr()))?;
            let rows = q.latest(rows, vec![id], version)?;
            q.select(rows, vec![id])
        })
        .unwrap();
    assert_eq!(
        catalog.render(&plan).sql,
        "WITH authors AS MATERIALIZED (SELECT e.source_id FROM gl_edge AS e \
         WHERE (e.relationship_kind = {p0:String})) \
         SELECT u.id FROM gl_user AS u WHERE u.id IN (SELECT source_id FROM authors) \
         ORDER BY u.id ASC, u._version DESC LIMIT 1 BY u.id"
    );
}

#[test]
fn a_duckdb_plan_uses_positional_parameters_and_native_functions() {
    let model = duckdb();
    let catalog = model.catalog();
    let files = catalog.table("gl_file").unwrap();
    let edges = catalog.table("gl_edge").unwrap();
    let plan = catalog
        .build(|q| {
            let left = q.scan(files, ReadMode::Current, "f")?;
            let right = q.scan(edges, ReadMode::Current, "e")?;
            let id = left.qualified("f", "id")?;
            let source = right.qualified("e", "source_id")?;
            let on = eq(id.expr(), source.expr());
            let rows = q.join(left, right, JoinKind::Left(on))?;
            let collect = Measure {
                function: AggregateFunction::Collect,
                args: vec![source.expr()],
                distinct: false,
                condition: None,
            };
            let aggregated = q.aggregate(rows, vec![id.expr()], vec![collect])?;
            let seven = Expr::Parameter {
                data_type: SqlType::Int64,
                value: json!(7),
            };
            let rows = q.filter(aggregated.rows, eq(aggregated.groups[0].expr(), seven))?;
            let edges = Declare::named("edges", aggregated.measures[0].expr());
            let (rows, computed) = q.compute(rows, vec![edges])?;
            q.select(rows, vec![aggregated.groups[0], computed[0]])
        })
        .unwrap();
    let query = catalog.render(&plan);
    assert_eq!(
        query.sql,
        "SELECT f.id, coalesce(array_agg(e.source_id) FILTER (WHERE e.source_id IS NOT NULL), []) \
         AS edges FROM gl_file AS f LEFT JOIN gl_edge AS e ON (f.id = e.source_id) \
         GROUP BY f.id HAVING (f.id = $1)"
    );
    assert_eq!(query.parameters["p1"].value, json!(7));
}

#[test]
fn backend_capabilities_and_declarations_are_checked_at_construction() {
    let model = duckdb();
    let catalog = model.catalog();
    assert_eq!(
        catalog.table("gl_user").unwrap_err(),
        RelationalError::UnknownTable("gl_user".into())
    );
    let files = catalog.table("gl_file").unwrap();
    let raw = catalog.build(|q| q.scan(files, ReadMode::Raw, "f"));
    assert_eq!(
        raw.unwrap_err(),
        RelationalError::Unsupported("raw reads of an unversioned store")
    );
    let settings = catalog.build(|q| {
        q.settings([("max_threads".to_string(), "4".to_string())])?;
        q.scan(files, ReadMode::Current, "f")
    });
    assert_eq!(
        settings.unwrap_err(),
        RelationalError::Unsupported("query settings")
    );
    let misplaced = catalog.build(|q| {
        let rows = q.scan(files, ReadMode::Current, "f")?;
        let id = rows.qualified("f", "id")?;
        let rows = q.select(rows, vec![id])?;
        q.filter(rows, Expr::Bool(true))
    });
    assert_eq!(misplaced.unwrap_err(), RelationalError::Misplaced("filter"));
    let unknown = catalog.build(|q| {
        let rows = q.scan(files, ReadMode::Current, "f")?;
        rows.qualified("f", "nope")?;
        q.select_all(rows)
    });
    assert!(matches!(
        unknown.unwrap_err(),
        RelationalError::UnknownColumn { name, .. } if name == "f.nope"
    ));
}
