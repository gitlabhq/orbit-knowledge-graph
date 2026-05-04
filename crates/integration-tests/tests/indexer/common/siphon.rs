use integration_testkit::TestContext;

fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

pub async fn create_namespace(
    ctx: &TestContext,
    id: i64,
    parent_id: Option<i64>,
    visibility_level: i32,
    traversal_path: &str,
) {
    create_namespace_with_path(ctx, id, parent_id, visibility_level, traversal_path, None).await;
}

pub async fn create_namespace_with_path(
    ctx: &TestContext,
    id: i64,
    parent_id: Option<i64>,
    visibility_level: i32,
    traversal_path: &str,
    slug: Option<&str>,
) {
    let parent_val = parent_id.map_or("NULL".to_string(), |p| p.to_string());
    let default_slug = format!("namespace-{id}");
    let path_slug = sql_escape(slug.unwrap_or(&default_slug));
    let traversal_ids: Vec<i64> = traversal_path
        .trim_end_matches('/')
        .split('/')
        .filter_map(|s| s.parse().ok())
        .collect();
    let traversal_ids_str = format!(
        "[{}]",
        traversal_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    ctx.execute(&format!(
        "INSERT INTO siphon_namespaces \
         (id, name, path, type, visibility_level, parent_id, owner_id, traversal_ids, created_at, updated_at, _siphon_replicated_at) \
         VALUES ({id}, '{path_slug}', '{path_slug}', 'Group', {visibility_level}, {parent_val}, 1, \
                 {traversal_ids_str}, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')"
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
    create_project_with_path(
        ctx,
        id,
        namespace_id,
        creator_id,
        visibility_level,
        traversal_path,
        None,
    )
    .await;
}

pub async fn create_project_with_path(
    ctx: &TestContext,
    id: i64,
    namespace_id: i64,
    creator_id: i64,
    visibility_level: i32,
    traversal_path: &str,
    slug: Option<&str>,
) {
    let default_slug = format!("project-{id}");
    let path_slug = sql_escape(slug.unwrap_or(&default_slug));
    ctx.execute(&format!(
        "INSERT INTO siphon_projects \
         (id, name, description, visibility_level, path, namespace_id, creator_id, \
          created_at, updated_at, archived, star_count, last_activity_at, _siphon_replicated_at) \
         VALUES ({id}, '{path_slug}', NULL, {visibility_level}, '{path_slug}', {namespace_id}, {creator_id}, \
                 '2023-01-01', '2024-01-15', false, 0, '2024-01-15', '2024-01-20 12:00:00')"
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO project_namespace_traversal_paths (id, traversal_path) VALUES ({id}, '{traversal_path}')"
    ))
    .await;
}

pub async fn create_route(
    ctx: &TestContext,
    id: i64,
    source_id: i64,
    source_type: &str,
    path: &str,
    namespace_id: i64,
    traversal_path: &str,
) {
    let escaped_path = sql_escape(path);
    ctx.execute(&format!(
        "INSERT INTO siphon_routes \
         (id, source_id, source_type, path, namespace_id, traversal_path, created_at, updated_at, _siphon_replicated_at) \
         VALUES ({id}, {source_id}, '{source_type}', '{escaped_path}', {namespace_id}, '{traversal_path}', \
                 '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')"
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

pub async fn create_runner(
    ctx: &TestContext,
    id: i64,
    runner_type: i16,
    name: &str,
    organization_id: Option<i64>,
) {
    let org = organization_id
        .map(|o| o.to_string())
        .unwrap_or_else(|| "NULL".to_string());
    ctx.execute(&format!(
        "INSERT INTO siphon_ci_runners \
         (id, runner_type, name, description, active, locked, run_untagged, access_level, organization_id, created_at, updated_at, contacted_at, _siphon_replicated_at) \
         VALUES ({id}, {runner_type}, '{name}', 'test runner', true, false, true, 0, {org}, '2024-01-15', '2024-01-15', '2024-01-15', '2024-01-20 12:00:00')"
    ))
    .await;
}

pub async fn create_runner_namespace(
    ctx: &TestContext,
    id: i64,
    runner_id: i64,
    namespace_id: i64,
    traversal_path: &str,
) {
    ctx.execute(&format!(
        "INSERT INTO siphon_ci_runner_namespaces \
         (id, runner_id, namespace_id, traversal_path, _siphon_replicated_at) \
         VALUES ({id}, {runner_id}, {namespace_id}, '{traversal_path}', '2024-01-20 12:00:00')"
    ))
    .await;
}

pub async fn create_runner_project(
    ctx: &TestContext,
    id: i64,
    runner_id: i64,
    project_id: i64,
    traversal_path: &str,
) {
    ctx.execute(&format!(
        "INSERT INTO siphon_ci_runner_projects \
         (id, runner_id, project_id, created_at, updated_at, traversal_path, _siphon_replicated_at) \
         VALUES ({id}, {runner_id}, {project_id}, '2024-01-15', '2024-01-15', '{traversal_path}', '2024-01-20 12:00:00')"
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
