use std::sync::LazyLock;

use query_data_model::{Catalog, ClickHouse};

use crate::surface;
use crate::testkit::{admin_ctx, non_admin_ctx};
use crate::{QueryError, Result};
use orbit_authorization::Grants;

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

fn restrict(gql: &str, ctx: &Grants) -> Result<()> {
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
    let reporter = non_admin_ctx();
    let error = restrict("MATCH (v:Vulnerability {id: 1}) RETURN v", &reporter).unwrap_err();
    assert!(matches!(error, QueryError::Authorization(_)), "{error}");
    restrict("MATCH (v:Vulnerability {id: 1}) RETURN v", &admin_ctx()).unwrap();
}

#[test]
fn plans_built_in_code_cannot_read_hidden_or_admin_only_fields() {
    let model = MODEL.model();
    let arm = |entity: &str, column: &str| crate::hydration::HydrationRequest {
        arms: vec![
            crate::hydration::HydrationArm::new(model, entity, &[1], &[column.into()], Vec::new())
                .unwrap(),
        ],
        options: crate::hydration::HydrationCompileOptions::default(),
    };
    let restricted = |request: crate::hydration::HydrationRequest, grants: &Grants| {
        super::restrict_hydration(&request, model, &*MODEL, grants)
    };
    for grants in [non_admin_ctx(), admin_ctx()] {
        let error = restricted(arm("Project", "traversal_path"), &grants).unwrap_err();
        assert!(error.to_string().contains("field is hidden"), "{error}");
    }
    let (reporter, admin) = (non_admin_ctx(), admin_ctx());
    let error = restricted(arm("User", "email"), &reporter).unwrap_err();
    assert!(
        error.to_string().contains("requires administrator access"),
        "{error}"
    );
    restricted(arm("User", "email"), &admin).unwrap();
}
