use orbit_utils::query_types::SqlType;
use query_data_model::implementations::clickhouse::ClickHouse;
use query_data_model::implementations::relational::plan::{
    AggregateFunction, BinaryOp, Expr, JoinKind, Measure, Operation, Order, ReadMode,
    RelationalError, RelationalPlan, UnaryOp,
};
use query_data_model::plan::tracking::{Declare, Rejected};
use query_data_model::{Catalog, ClickHouseDataModel, DuckDbDataModel};
use serde_json::json;

fn clickhouse() -> ClickHouseDataModel {
    ClickHouseDataModel::embedded().unwrap()
}

fn duckdb() -> DuckDbDataModel {
    DuckDbDataModel::embedded().unwrap()
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

fn handles(catalog: &Catalog<ClickHouse>) -> RelationalPlan<ClickHouse> {
    let users = catalog.table("gl_user").unwrap();
    catalog
        .build(|q| {
            let keys = q.subquery("keys", |q| {
                let rows = q.scan(users, ReadMode::Raw, "k")?;
                let id = rows.qualified("k", "id")?;
                q.select(rows, vec![id])
            })?;
            let rows = q.scan(users, ReadMode::Current, "u")?;
            let id = rows.qualified("u", "id")?;
            let name = rows.qualified("u", "username")?;
            let rows = q.filter(rows, keys.contains(id.expr()))?;
            let (rows, computed) = q.compute(rows, vec![Declare::named("handle", name.expr())])?;
            q.select(rows, vec![id, computed[0]])
        })
        .unwrap()
}

#[test]
fn a_relational_rewrite_keeps_unchanged_plans_and_guards_every_scan() {
    let model = clickhouse();
    let catalog = model.catalog();
    let original = catalog.render(&handles(catalog)).sql;
    let same = catalog
        .rewrite(handles(catalog), |_, rows| Ok(rows))
        .unwrap();
    assert_eq!(catalog.render(&same).sql, original);

    let mut visits = 0;
    let guarded = catalog
        .rewrite(handles(catalog), |q, rows| {
            visits += 1;
            if !matches!(q.operation(&rows)?, Operation::Scan { .. }) {
                return Ok(rows);
            }
            let deleted = rows.column("_deleted")?;
            let live = Expr::Unary {
                op: UnaryOp::Not,
                value: Box::new(deleted.expr()),
            };
            q.filter(rows, live)
        })
        .unwrap();
    assert_eq!(visits, 6);
    assert_eq!(
        catalog.render(&guarded).sql,
        "SELECT u.id, u.username AS handle FROM gl_user AS u FINAL \
         WHERE ((NOT u._deleted) AND u.id IN (SELECT k.id FROM gl_user AS k WHERE (NOT k._deleted)))"
    );
}

#[test]
fn relational_rewrites_reconnect_changed_outputs() {
    let model = clickhouse();
    let catalog = model.catalog();
    let users = catalog.table("gl_user").unwrap();
    let build = || {
        catalog
            .build(|q| {
                let rows = q.scan(users, ReadMode::Current, "u")?;
                let id = rows.qualified("u", "id")?;
                let name = rows.qualified("u", "username")?;
                let rows = q.filter(rows, eq(name.expr(), text("root")))?;
                let order = Order {
                    value: name.expr(),
                    descending: false,
                };
                let rows = q.order(rows, vec![order])?;
                q.select(rows, vec![id, name])
            })
            .unwrap()
    };
    let alias = |reconnect: bool| {
        catalog.rewrite(build(), move |q, rows| {
            if !matches!(q.operation(&rows)?, Operation::Filter(_)) {
                return Ok(rows);
            }
            let name = rows.qualified("u", "username")?;
            let (rows, computed) = q.compute(rows, vec![Declare::named("handle", name.expr())])?;
            if reconnect {
                q.reconnect(name.output(), computed[0].output())?;
            }
            Ok(rows)
        })
    };

    assert_eq!(
        alias(false).unwrap_err(),
        RelationalError::Rejected(Rejected::ChangedOutputs("handle".into()))
    );
    assert_eq!(
        catalog.render(&alias(true).unwrap()).sql,
        "SELECT u.id, u.username AS handle FROM gl_user AS u FINAL \
         WHERE (u.username = {p0:String}) ORDER BY handle ASC"
    );
}
