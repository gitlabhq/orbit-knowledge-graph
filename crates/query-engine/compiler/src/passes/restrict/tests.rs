use std::sync::LazyLock;

use query_data_model::{Catalog, ClickHouse};

use crate::surface;
use crate::testkit::{admin_ctx, non_admin_ctx};
use crate::types::{AccessLevel, AuthorizedPath, SecurityContext};
use crate::{QueryError, Result};

static MODEL: LazyLock<Catalog<ClickHouse>> =
    LazyLock::new(|| Catalog::<ClickHouse>::embedded().unwrap());

const USER_ADMIN_ONLY_COLUMNS: &[&str] = &[
    "email",
    "first_name",
    "last_name",
    "preferred_language",
    "private_profile",
    "is_external",
    "is_admin",
    "is_auditor",
    "updated_at",
];

fn restrict(gql: &str, ctx: &SecurityContext) -> Result<()> {
    let model = MODEL.model();
    let statement = surface::parse(gql, model.logical().as_admin())?;
    super::restrict(&statement.plan, model, &*MODEL, ctx)
}

fn restricted(gql: &str) -> String {
    match restrict(gql, &non_admin_ctx()) {
        Err(QueryError::Restrict(message)) => message,
        other => panic!("{gql}: expected a restriction, got {other:?}"),
    }
}

#[test]
fn non_admin_reads_of_admin_only_user_columns_are_rejected() {
    for column in USER_ADMIN_ONLY_COLUMNS {
        let select = format!("MATCH (u:User {{id: 1}}) RETURN u.{column}");
        assert!(restricted(&select).contains(column), "{select}");
        let filter = format!("MATCH (u:User {{id: 1}}) WHERE u.{column} IS NULL RETURN u.username");
        assert!(restricted(&filter).starts_with("filter on"), "{filter}");
        let order = format!("MATCH (u:User {{id: 1}}) RETURN u.username ORDER BY u.{column}");
        assert!(restricted(&order).starts_with("order_by on"), "{order}");
        let admin = format!("MATCH (u:User {{id: 1}}) WHERE u.{column} IS NULL RETURN u.{column}");
        restrict(&admin, &admin_ctx()).unwrap();
    }
}

#[test]
fn aggregations_on_admin_only_columns_are_rejected() {
    let message = restricted(
        "MATCH (p:Project {id: 1})<-[:MEMBER_OF]-(u:User) RETURN u.email, count(u) AS n",
    );
    assert!(message.starts_with("group_by on"), "{message}");
}

#[test]
fn aggregations_need_a_scoped_node() {
    let message = restricted("MATCH (u:User {id: 1}) RETURN count(u) AS n");
    assert!(message.contains("traversal_path"), "{message}");
    restrict("MATCH (u:User {id: 1}) RETURN count(u) AS n", &admin_ctx()).unwrap();
    restrict(
        "MATCH (p:Project {id: 1})<-[:MEMBER_OF]-(u:User) RETURN count(u) AS n",
        &non_admin_ctx(),
    )
    .unwrap();
}

#[test]
fn entities_above_every_authorized_role_are_rejected() {
    let reporter = SecurityContext::new_with_roles(
        1,
        vec![AuthorizedPath::new("1/", AccessLevel::Reporter as u32)],
    )
    .unwrap();
    let error = restrict("MATCH (v:Vulnerability {id: 1}) RETURN v", &reporter).unwrap_err();
    assert!(matches!(error, QueryError::Authorization(_)), "{error}");
    restrict("MATCH (v:Vulnerability {id: 1}) RETURN v", &admin_ctx()).unwrap();
}
