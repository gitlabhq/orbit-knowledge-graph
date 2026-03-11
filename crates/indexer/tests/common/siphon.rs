use integration_testkit::TestContext;

pub async fn create_namespace(
    ctx: &TestContext,
    id: i64,
    parent_id: Option<i64>,
    visibility_level: i32,
    traversal_path: &str,
) {
    let parent_val = parent_id.map_or("NULL".to_string(), |p| p.to_string());
    ctx.execute(&format!(
        "INSERT INTO siphon_namespaces \
         (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at) \
         VALUES ({id}, 'namespace-{id}', 'namespace-{id}', {visibility_level}, {parent_val}, 1, \
                 '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')"
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO siphon_namespace_details (namespace_id, description) VALUES ({id}, NULL)"
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO namespace_traversal_paths (id, traversal_path) VALUES ({id}, '{traversal_path}')"
    ))
    .await;
}

pub async fn create_project(
    ctx: &TestContext,
    id: i64,
    namespace_id: i64,
    creator_id: i64,
    visibility_level: i32,
    traversal_path: &str,
) {
    ctx.execute(&format!(
        "INSERT INTO siphon_projects \
         (id, name, description, visibility_level, path, namespace_id, creator_id, \
          created_at, updated_at, archived, star_count, last_activity_at, _siphon_replicated_at) \
         VALUES ({id}, 'project-{id}', NULL, {visibility_level}, 'project-{id}', {namespace_id}, {creator_id}, \
                 '2023-01-01', '2024-01-15', false, 0, '2024-01-15', '2024-01-20 12:00:00')"
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO project_namespace_traversal_paths (id, traversal_path) VALUES ({id}, '{traversal_path}')"
    ))
    .await;
}

pub async fn create_user(ctx: &TestContext, id: i64) {
    ctx.execute(&format!(
        "INSERT INTO siphon_users \
         (id, email, username, name, state, organization_id, _siphon_replicated_at) \
         VALUES ({id}, 'user{id}@example.com', 'user{id}', 'User {id}', 'active', 1, '2024-01-20 12:00:00')"
    ))
    .await;
}

pub async fn create_member(
    ctx: &TestContext,
    user_id: i64,
    source_id: i64,
    source_type: &str,
    traversal_path: &str,
) {
    let id = user_id * 10_000 + source_id;
    ctx.execute(&format!(
        "INSERT INTO siphon_members \
         (id, access_level, source_id, source_type, user_id, state, traversal_path, _siphon_replicated_at) \
         VALUES ({id}, 40, {source_id}, '{source_type}', {user_id}, 0, '{traversal_path}', '2024-01-20 12:00:00')"
    ))
    .await;
}
