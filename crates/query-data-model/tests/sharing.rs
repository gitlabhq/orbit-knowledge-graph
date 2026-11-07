use orbit_utils::query_types::SqlType;
use query_data_model::ClickHouseDataModel;
use query_data_model::implementations::clickhouse::ClickHouse;
use query_data_model::implementations::relational::plan::{
    BinaryOp, Expr, JoinKind, ReadMode, SharePolicy,
};
use serde_json::json;

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

fn policy() -> SharePolicy {
    SharePolicy::of::<ClickHouse>("_orbit_relation_")
}

#[test]
fn shares_read_twice_become_pruned_bodies_and_shares_read_once_inline() {
    let model = ClickHouseDataModel::embedded().unwrap();
    let catalog = model.catalog();
    let users = catalog.table("gl_user").unwrap();
    let plan = catalog
        .build(|q| {
            let rows = q.scan(users, ReadMode::Current, "u")?;
            let name = rows.qualified("u", "username")?;
            let rows = q.filter(rows, eq(name.expr(), text("root")))?;
            let rows = q.select_all(rows)?;
            let named = q.share(rows)?;
            let rows = q.scan(users, ReadMode::Current, "a")?;
            let state = rows.qualified("a", "state")?;
            let rows = q.filter(rows, eq(state.expr(), text("active")))?;
            let rows = q.select_all(rows)?;
            let active = q.share(rows)?;
            let ids = q.subquery("ids", |q| {
                let rows = q.read(&named, None)?;
                let id = rows.column("id")?;
                q.select(rows, vec![id])
            })?;
            let left = q.read(&named, Some("u"))?;
            let right = q.read(&active, Some("a"))?;
            let on = eq(
                left.qualified("u", "id")?.expr(),
                right.qualified("a", "id")?.expr(),
            );
            let rows = q.join(left, right, JoinKind::Inner(on))?;
            let id = rows.qualified("a", "id")?;
            let rows = q.filter(rows, ids.contains(id.expr()))?;
            q.select(rows, vec![id])
        })
        .unwrap();
    let planned = catalog.render(&plan).sql;
    assert!(planned.starts_with("SELECT a.id FROM (SELECT * FROM gl_user AS u FINAL"));
    let plan = catalog.materialize(plan, &policy()).unwrap();
    assert_eq!(
        catalog.render(&plan).sql,
        "WITH _orbit_relation_0 AS MATERIALIZED (SELECT u.id FROM gl_user AS u FINAL \
         WHERE (u.username = {p0:String})) \
         SELECT a.id FROM _orbit_relation_0 AS u INNER JOIN gl_user AS a FINAL ON (u.id = a.id) \
         WHERE ((a.state = {p1:String}) AND a.id IN (SELECT id FROM _orbit_relation_0))"
    );
}

#[test]
fn a_share_read_once_without_an_alias_inlines_as_a_named_subquery() {
    let model = ClickHouseDataModel::embedded().unwrap();
    let catalog = model.catalog();
    let users = catalog.table("gl_user").unwrap();
    let plan = catalog
        .build(|q| {
            let rows = q.scan(users, ReadMode::Current, "u")?;
            let id = rows.qualified("u", "id")?;
            let rows = q.select(rows, vec![id])?;
            let ids = q.share(rows)?;
            let keys = q.subquery("keys", |q| {
                let rows = q.read(&ids, None)?;
                let id = rows.column("id")?;
                q.select(rows, vec![id])
            })?;
            let rows = q.scan(users, ReadMode::Current, "v")?;
            let id = rows.qualified("v", "id")?;
            let rows = q.filter(rows, keys.contains(id.expr()))?;
            q.select(rows, vec![id])
        })
        .unwrap();
    let plan = catalog.materialize(plan, &policy()).unwrap();
    assert_eq!(
        catalog.render(&plan).sql,
        "SELECT v.id FROM gl_user AS v FINAL WHERE v.id IN (SELECT _orbit_relation_0.id \
         FROM (SELECT u.id FROM gl_user AS u FINAL) AS _orbit_relation_0)"
    );
}

#[test]
fn a_point_lookup_read_twice_is_recomputed_at_each_read() {
    let model = ClickHouseDataModel::embedded().unwrap();
    let catalog = model.catalog();
    let users = catalog.table("gl_user").unwrap();
    let plan = catalog
        .build(|q| {
            let rows = q.scan(users, ReadMode::Current, "u")?;
            let name = rows.qualified("u", "username")?;
            let rows = q.filter(rows, eq(name.expr(), text("root")))?;
            let rows = q.select_all(rows)?;
            let found = q.share_lookup(rows)?;
            let left = q.read(&found, Some("u"))?;
            let right = q.read(&found, Some("v"))?;
            let on = eq(
                left.qualified("u", "id")?.expr(),
                right.qualified("v", "id")?.expr(),
            );
            let rows = q.join(left, right, JoinKind::Inner(on))?;
            let id = rows.qualified("u", "id")?;
            q.select(rows, vec![id])
        })
        .unwrap();
    let sql = catalog
        .render(&catalog.materialize(plan, &policy()).unwrap())
        .sql;
    assert!(!sql.starts_with("WITH "), "{sql}");
    assert_eq!(sql.matches("FROM gl_user AS u FINAL").count(), 2, "{sql}");
}
