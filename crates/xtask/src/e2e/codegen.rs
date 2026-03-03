//! Generates Ruby e2e test scripts from `e2e/tests/scenarios.yaml`.
//!
//! Reads the declarative YAML scenario file and emits two Ruby scripts:
//!
//! - `create_test_data.rb` — creates users, groups, projects, memberships,
//!   and SDLC entities (MRs, work items, notes, milestones, labels) via
//!   GitLab service objects. Writes a JSON manifest with all dynamic IDs.
//!
//! - `redaction_test.rb` — loads the manifest, fires gRPC queries against
//!   the GKG server as each test user, and asserts result row counts.
//!
//! ## Usage
//!
//! ```shell
//! cargo xtask e2e codegen          # regenerate .rb files
//! cargo xtask e2e codegen --check  # verify committed files match (CI)
//! ```
//!
//! The generated files are committed to git. CI runs `--check` to prevent
//! drift between `scenarios.yaml` and the Ruby scripts.

use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use super::constants;

// ---------------------------------------------------------------------------
// YAML schema types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct Scenario {
    pub password: String,
    pub users: BTreeMap<String, UserDef>,
    pub groups: BTreeMap<String, GroupDef>,
    pub projects: BTreeMap<String, ProjectDef>,
    #[serde(default)]
    pub memberships: BTreeMap<String, Vec<MembershipDef>>,
    pub assertions: Vec<AssertionSection>,
}

#[derive(Debug, Deserialize)]
pub struct UserDef {
    pub name: String,
    pub email: String,
    #[serde(default)]
    pub username: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GroupDef {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub visibility: Option<String>,
    #[serde(default)]
    pub children: BTreeMap<String, GroupDef>,
}

#[derive(Debug, Deserialize)]
pub struct ProjectDef {
    pub name: String,
    pub group: String,
    #[serde(default)]
    pub visibility: Option<String>,
    #[serde(default)]
    pub entities: Option<EntityCounts>,
}

#[derive(Debug, Deserialize)]
pub struct EntityCounts {
    #[serde(default)]
    pub milestones: u32,
    #[serde(default)]
    pub labels: u32,
    #[serde(default)]
    pub work_items: u32,
    #[serde(default)]
    pub merge_requests: Option<MrCounts>,
    #[serde(default)]
    pub notes: Option<NoteCounts>,
}

#[derive(Debug, Deserialize)]
pub struct MrCounts {
    #[serde(default)]
    pub total: u32,
    #[serde(default)]
    pub merged: u32,
}

#[derive(Debug, Deserialize)]
pub struct NoteCounts {
    #[serde(default)]
    pub per_mr: u32,
    #[serde(default)]
    pub first_n_mrs: u32,
    #[serde(default)]
    pub per_issue: u32,
    #[serde(default)]
    pub first_n_issues: u32,
}

#[derive(Debug, Deserialize)]
pub struct MembershipDef {
    pub group: String,
    pub access: String,
}

#[derive(Debug, Deserialize)]
pub struct AssertionSection {
    pub section: String,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub users: Option<Vec<String>>,
    /// Reserved: when true, the user is the root admin (bypasses redaction).
    #[serde(default)]
    #[allow(dead_code)]
    pub admin: Option<bool>,
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
pub struct TestCase {
    pub name: String,
    pub query: String,
    pub expect: Expect,
    #[serde(default)]
    pub user: Option<String>,
}

/// Expected result count for a test assertion.
///
/// Deserialized from YAML like `{ eq: 3 }`, `{ gte: 1 }`, or `{ range: [5, 10] }`.
#[derive(Debug, Deserialize)]
pub struct Expect {
    #[serde(default)]
    pub eq: Option<serde_yaml::Value>,
    #[serde(default)]
    pub gte: Option<serde_yaml::Value>,
    #[serde(default)]
    pub range: Option<Vec<serde_yaml::Value>>,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Load scenarios.yaml, generate Ruby scripts, write or verify them.
pub fn run(gkg_root: &Path, check: bool) -> Result<()> {
    let yaml_path = gkg_root.join(constants::SCENARIOS_YAML);
    let content = fs::read_to_string(&yaml_path)
        .with_context(|| format!("failed to read {}", yaml_path.display()))?;
    let scenario: Scenario =
        serde_yaml::from_str(&content).context("failed to parse scenarios.yaml")?;

    let create_data_rb = generate_create_test_data(&scenario);
    let redaction_test_rb = generate_redaction_test(&scenario);

    let tests_dir = gkg_root.join(constants::E2E_TESTS_DIR);
    let create_data_path = tests_dir.join("create_test_data.rb");
    let redaction_test_path = tests_dir.join(constants::REDACTION_TEST_RB);

    if check {
        let existing_create = fs::read_to_string(&create_data_path)
            .with_context(|| format!("failed to read {}", create_data_path.display()))?;
        let existing_redaction = fs::read_to_string(&redaction_test_path)
            .with_context(|| format!("failed to read {}", redaction_test_path.display()))?;

        let mut drift = false;
        if existing_create != create_data_rb {
            eprintln!(
                "DRIFT: {} does not match scenarios.yaml",
                create_data_path.display()
            );
            drift = true;
        }
        if existing_redaction != redaction_test_rb {
            eprintln!(
                "DRIFT: {} does not match scenarios.yaml",
                redaction_test_path.display()
            );
            drift = true;
        }
        if drift {
            bail!(
                "Generated Ruby files are out of date. Run `cargo xtask e2e codegen` to regenerate."
            );
        }
        println!("codegen --check passed: Ruby files match scenarios.yaml");
    } else {
        fs::write(&create_data_path, &create_data_rb)
            .with_context(|| format!("failed to write {}", create_data_path.display()))?;
        println!("Wrote {}", create_data_path.display());

        fs::write(&redaction_test_path, &redaction_test_rb)
            .with_context(|| format!("failed to write {}", redaction_test_path.display()))?;
        println!("Wrote {}", redaction_test_path.display());
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolve the Ruby username for a user key from the scenario.
fn ruby_username<'a>(scenario: &'a Scenario, key: &'a str) -> &'a str {
    if key == "root" {
        return "root";
    }
    scenario
        .users
        .get(key)
        .and_then(|u| u.username.as_deref())
        .unwrap_or(key)
}

/// Return the Ruby access level constant for a string like "developer".
fn ruby_access_level(access: &str) -> &str {
    match access {
        "guest" => "Gitlab::Access::GUEST",
        "reporter" => "Gitlab::Access::REPORTER",
        "developer" => "Gitlab::Access::DEVELOPER",
        "maintainer" => "Gitlab::Access::MAINTAINER",
        "owner" => "Gitlab::Access::OWNER",
        other => other,
    }
}

/// Return the visibility level integer for a visibility string.
fn visibility_level(vis: Option<&str>) -> u32 {
    match vis {
        Some("private") => 0,
        _ => 20,
    }
}

/// Collect all group keys in definition order (parent before children), returning
/// `(key, display_name, path, parent_key, visibility)` tuples.
fn flatten_groups(groups: &BTreeMap<String, GroupDef>) -> Vec<GroupInfo> {
    let mut out = Vec::new();
    for (key, def) in groups {
        let display_name = def.name.clone().unwrap_or_else(|| key.replace('_', "-"));
        let path = display_name.clone();
        let vis = def.visibility.as_deref();
        out.push(GroupInfo {
            key: key.clone(),
            name: display_name,
            path,
            parent_key: None,
            visibility: visibility_level(vis),
        });
        flatten_children(&def.children, key, &mut out);
    }
    out
}

fn flatten_children(
    children: &BTreeMap<String, GroupDef>,
    parent_key: &str,
    out: &mut Vec<GroupInfo>,
) {
    for (key, def) in children {
        let display_name = def.name.clone().unwrap_or_else(|| key.replace('_', "-"));
        let path = display_name.clone();
        let vis = def.visibility.as_deref();
        out.push(GroupInfo {
            key: key.clone(),
            name: display_name,
            path,
            parent_key: Some(parent_key.to_string()),
            visibility: visibility_level(vis),
        });
        flatten_children(&def.children, key, out);
    }
}

struct GroupInfo {
    key: String,
    name: String,
    path: String,
    parent_key: Option<String>,
    visibility: u32,
}

/// Determine which project keys are "public" (have MRs created, get notes).
fn public_project_keys(scenario: &Scenario) -> Vec<String> {
    scenario
        .projects
        .iter()
        .filter(|(_, p)| p.visibility.as_deref() != Some("private"))
        .map(|(k, _)| k.clone())
        .collect()
}

/// Determine which project keys are visible to a user based on memberships.
/// Root sees everything.
fn visible_project_keys(scenario: &Scenario, user_key: &str) -> Vec<String> {
    if user_key == "root" {
        return scenario.projects.keys().cloned().collect();
    }
    let memberships = match scenario.memberships.get(user_key) {
        Some(m) => m,
        None => return Vec::new(),
    };

    let all_groups = flatten_groups(&scenario.groups);
    let mut visible_group_keys: Vec<String> = Vec::new();

    for m in memberships {
        visible_group_keys.push(m.group.clone());
        collect_descendant_group_keys(&all_groups, &m.group, &mut visible_group_keys);
    }

    scenario
        .projects
        .iter()
        .filter(|(_, p)| visible_group_keys.contains(&p.group))
        .map(|(k, _)| k.clone())
        .collect()
}

fn collect_descendant_group_keys(
    all_groups: &[GroupInfo],
    parent_key: &str,
    out: &mut Vec<String>,
) {
    for g in all_groups {
        if g.parent_key.as_deref() == Some(parent_key) {
            out.push(g.key.clone());
            collect_descendant_group_keys(all_groups, &g.key, out);
        }
    }
}

/// Determine which root group keys exist (for kg_enabled_namespaces).
fn root_group_keys(scenario: &Scenario) -> Vec<String> {
    scenario.groups.keys().cloned().collect()
}

// ---------------------------------------------------------------------------
// Ruby codegen: create_test_data.rb
// ---------------------------------------------------------------------------

fn generate_create_test_data(scenario: &Scenario) -> String {
    let mut rb = String::with_capacity(16384);

    // Header
    writeln!(rb, "# frozen_string_literal: true").unwrap();
    writeln!(rb).unwrap();
    writeln!(
        rb,
        "# AUTO-GENERATED from e2e/tests/scenarios.yaml — do not edit directly."
    )
    .unwrap();
    writeln!(rb, "# Regenerate: cargo xtask e2e codegen").unwrap();
    writeln!(rb, "# Verify:     cargo xtask e2e codegen --check").unwrap();
    writeln!(rb).unwrap();

    // Requires and constants
    writeln!(rb, "require 'json'").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "E2E_POD_DIR = ENV.fetch('E2E_POD_DIR', '/tmp/e2e')").unwrap();
    writeln!(rb, "MANIFEST_PATH = \"#{{E2E_POD_DIR}}/manifest.json\"").unwrap();
    writeln!(rb, "TEST_PASSWORD = '{}'", scenario.password).unwrap();
    writeln!(rb).unwrap();

    // unwrap helper
    emit_unwrap_helper(&mut rb);

    writeln!(rb, "puts '=== CREATING E2E TEST DATA ==='").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "Feature.enable(:knowledge_graph)").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "org = Organizations::Organization.default_organization || Organizations::Organization.first").unwrap();
    writeln!(rb, "admin = User.find_by!(username: 'root')").unwrap();
    writeln!(rb, "puts \"Organization: #{{org.name}} (id: #{{org.id}})\"").unwrap();
    writeln!(
        rb,
        "puts \"Admin: #{{admin.username}} (id: #{{admin.id}})\""
    )
    .unwrap();
    writeln!(rb).unwrap();

    // Manifest initialization
    writeln!(rb, "manifest = {{").unwrap();
    writeln!(rb, "  organization_id: org.id,").unwrap();
    writeln!(rb, "  admin_id: admin.id,").unwrap();
    writeln!(rb, "  users: {{}},").unwrap();
    writeln!(rb, "  groups: {{}},").unwrap();
    writeln!(rb, "  projects: {{}},").unwrap();
    writeln!(rb, "  merge_requests: {{}},").unwrap();
    writeln!(rb, "  work_items: {{}},").unwrap();
    writeln!(rb, "  notes: {{}},").unwrap();
    writeln!(rb, "  milestones: {{}},").unwrap();
    writeln!(rb, "  labels: {{}},").unwrap();
    writeln!(rb, "  counts: {{}}").unwrap();
    writeln!(rb, "}}").unwrap();
    writeln!(rb).unwrap();

    // 1. Users
    emit_users_section(scenario, &mut rb);

    // 2. Groups
    emit_groups_section(scenario, &mut rb);

    // 3. Projects
    emit_projects_section(scenario, &mut rb);

    // 4. Memberships
    emit_memberships_section(scenario, &mut rb);

    // 5. kg_enabled_namespaces
    emit_kg_enabled_section(scenario, &mut rb);

    // 6. Milestones
    emit_milestones_section(scenario, &mut rb);

    // 7. Labels
    emit_labels_section(scenario, &mut rb);

    // 8. Work items
    emit_work_items_section(scenario, &mut rb);

    // 9. Merge requests
    emit_merge_requests_section(scenario, &mut rb);

    // 10. Notes
    emit_notes_section(scenario, &mut rb);

    // 11. Counts
    emit_counts_section(scenario, &mut rb);

    // 12. Write manifest
    emit_write_manifest(scenario, &mut rb);

    // Summary
    emit_summary(scenario, &mut rb);

    rb
}

fn emit_unwrap_helper(rb: &mut String) {
    writeln!(rb, "def unwrap(result, key)").unwrap();
    writeln!(
        rb,
        "  if result.respond_to?(:payload) && result.payload.is_a?(Hash)"
    )
    .unwrap();
    writeln!(rb, "    result.payload[key] || result.payload").unwrap();
    writeln!(rb, "  elsif result.is_a?(Hash)").unwrap();
    writeln!(rb, "    result[key] || result").unwrap();
    writeln!(rb, "  else").unwrap();
    writeln!(rb, "    result").unwrap();
    writeln!(rb, "  end").unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb).unwrap();
}

fn emit_users_section(scenario: &Scenario, rb: &mut String) {
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "# 1. CREATE TEST USERS").unwrap();
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "puts \"\\n--- 1. Creating test users ---\"").unwrap();
    writeln!(rb).unwrap();

    // find_or_create_user helper
    writeln!(
        rb,
        "def find_or_create_user(username, name, email, admin, org)"
    )
    .unwrap();
    writeln!(rb, "  user = User.find_by(username: username)").unwrap();
    writeln!(rb, "  if user").unwrap();
    writeln!(
        rb,
        "    puts \"  User '#{{username}}' already exists (id: #{{user.id}})\""
    )
    .unwrap();
    writeln!(rb, "  else").unwrap();
    writeln!(rb, "    user = User.new(").unwrap();
    writeln!(rb, "      username: username,").unwrap();
    writeln!(rb, "      name: name,").unwrap();
    writeln!(rb, "      email: email,").unwrap();
    writeln!(rb, "      password: TEST_PASSWORD,").unwrap();
    writeln!(rb, "      password_confirmation: TEST_PASSWORD,").unwrap();
    writeln!(rb, "      confirmed_at: Time.current,").unwrap();
    writeln!(rb, "      organization_id: org.id,").unwrap();
    writeln!(rb, "      skip_confirmation: true").unwrap();
    writeln!(rb, "    )").unwrap();
    writeln!(rb, "    user.assign_personal_namespace(org)").unwrap();
    writeln!(rb, "    user.save!").unwrap();
    writeln!(
        rb,
        "    puts \"  Created user '#{{username}}' (id: #{{user.id}})\""
    )
    .unwrap();
    writeln!(rb, "  end").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "  Organizations::OrganizationUser.find_or_create_by!(organization: org, user: user) do |record|").unwrap();
    writeln!(rb, "    record.access_level = :default").unwrap();
    writeln!(rb, "  end").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "  user").unwrap();
    writeln!(rb, "rescue StandardError => e").unwrap();
    writeln!(
        rb,
        "  puts \"  Direct creation failed (#{{e.message[0..80]}}), trying CreateService...\""
    )
    .unwrap();
    writeln!(rb, "  result = Users::CreateService.new(admin, {{").unwrap();
    writeln!(
        rb,
        "                                      username: username,"
    )
    .unwrap();
    writeln!(rb, "                                      name: name,").unwrap();
    writeln!(rb, "                                      email: email,").unwrap();
    writeln!(
        rb,
        "                                      password: TEST_PASSWORD,"
    )
    .unwrap();
    writeln!(
        rb,
        "                                      skip_confirmation: true,"
    )
    .unwrap();
    writeln!(
        rb,
        "                                      organization_id: org.id"
    )
    .unwrap();
    writeln!(rb, "                                    }}).execute").unwrap();
    writeln!(rb, "  user = result.is_a?(User) ? result : result[:user]").unwrap();
    writeln!(rb, "  raise \"Failed to create user '#{{username}}': #{{result.inspect}}\" unless user&.persisted?").unwrap();
    writeln!(rb).unwrap();
    writeln!(
        rb,
        "  puts \"  Created user '#{{username}}' via service (id: #{{user.id}})\""
    )
    .unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "  Organizations::OrganizationUser.find_or_create_by!(organization: org, user: user) do |record|").unwrap();
    writeln!(rb, "    record.access_level = :default").unwrap();
    writeln!(rb, "  end").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "  user").unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb).unwrap();

    for (key, user) in &scenario.users {
        let username = user.username.as_deref().unwrap_or(key.as_str());
        writeln!(
            rb,
            "{key} = find_or_create_user('{username}', '{}', '{}', admin, org)",
            user.name, user.email,
        )
        .unwrap();
    }
    writeln!(rb).unwrap();

    // Manifest users hash
    writeln!(rb, "manifest[:users] = {{").unwrap();
    writeln!(rb, "  root: {{ id: admin.id, username: 'root' }},").unwrap();
    for (key, user) in &scenario.users {
        let username = user.username.as_deref().unwrap_or(key.as_str());
        writeln!(rb, "  {key}: {{ id: {key}.id, username: '{username}' }},").unwrap();
    }
    writeln!(rb, "}}").unwrap();
    writeln!(rb).unwrap();
}

fn emit_groups_section(scenario: &Scenario, rb: &mut String) {
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "# 2. CREATE GROUPS").unwrap();
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "puts \"\\n--- 2. Creating group hierarchy ---\"").unwrap();
    writeln!(rb).unwrap();

    // find_or_create_group helper
    writeln!(
        rb,
        "def find_or_create_group(name, path, admin, org, parent: nil, visibility: 20)"
    )
    .unwrap();
    writeln!(
        rb,
        "  group = Group.find_by(path: path, parent_id: parent&.id)"
    )
    .unwrap();
    writeln!(rb, "  if group").unwrap();
    writeln!(
        rb,
        "    puts \"  Group '#{{name}}' already exists (id: #{{group.id}})\""
    )
    .unwrap();
    writeln!(rb, "    return group").unwrap();
    writeln!(rb, "  end").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "  params = {{").unwrap();
    writeln!(rb, "    name: name,").unwrap();
    writeln!(rb, "    path: path,").unwrap();
    writeln!(rb, "    visibility_level: visibility,").unwrap();
    writeln!(rb, "    organization_id: org.id").unwrap();
    writeln!(rb, "  }}").unwrap();
    writeln!(rb, "  params[:parent_id] = parent.id if parent").unwrap();
    writeln!(rb).unwrap();
    writeln!(
        rb,
        "  result = Groups::CreateService.new(admin, params).execute"
    )
    .unwrap();
    writeln!(rb, "  group = unwrap(result, :group)").unwrap();
    writeln!(rb, "  raise \"Failed to create group '#{{name}}': #{{result.inspect}}\" unless group.is_a?(Group) && group.persisted?").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "  puts \"  Created group '#{{name}}' (id: #{{group.id}}, traversal: #{{group.traversal_ids.join('/')}}/) #{{visibility == 0 ? '[PRIVATE]' : '[PUBLIC]'}}\"").unwrap();
    writeln!(rb, "  group").unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb).unwrap();

    let flat = flatten_groups(&scenario.groups);
    for g in &flat {
        let parent_arg = match &g.parent_key {
            Some(pk) => format!(", parent: {pk}_group"),
            None => String::new(),
        };
        writeln!(
            rb,
            "{key}_group = find_or_create_group('{name}', '{path}', admin, org{parent_arg}, visibility: {vis})",
            key = g.key,
            name = g.name,
            path = g.path,
            vis = g.visibility,
        )
        .unwrap();
    }
    writeln!(rb).unwrap();

    // Manifest groups hash
    writeln!(rb, "manifest[:groups] = {{").unwrap();
    for g in &flat {
        let vis_str = if g.visibility == 0 {
            ",\n              visibility: 'private'"
        } else {
            ""
        };
        writeln!(
            rb,
            "  {key}: {{ id: {key}_group.id, path: {key}_group.full_path,\n              traversal: \"#{{{key}_group.traversal_ids.join('/')}}/\"{vis_str} }},",
            key = g.key,
        )
        .unwrap();
    }
    writeln!(rb, "}}").unwrap();
    writeln!(rb).unwrap();
}

fn emit_projects_section(scenario: &Scenario, rb: &mut String) {
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "# 3. CREATE PROJECTS").unwrap();
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "puts \"\\n--- 3. Creating projects ---\"").unwrap();
    writeln!(rb).unwrap();

    // find_or_create_project helper
    writeln!(
        rb,
        "def find_or_create_project(name, path, namespace, admin, org, visibility: 20)"
    )
    .unwrap();
    writeln!(
        rb,
        "  project = Project.find_by(path: path, namespace_id: namespace.id)"
    )
    .unwrap();
    writeln!(rb, "  if project").unwrap();
    writeln!(
        rb,
        "    puts \"  Project '#{{name}}' already exists (id: #{{project.id}})\""
    )
    .unwrap();
    writeln!(rb, "    return project").unwrap();
    writeln!(rb, "  end").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "  result = Projects::CreateService.new(admin, {{").unwrap();
    writeln!(rb, "                                         name: name,").unwrap();
    writeln!(rb, "                                         path: path,").unwrap();
    writeln!(
        rb,
        "                                         namespace_id: namespace.id,"
    )
    .unwrap();
    writeln!(
        rb,
        "                                         visibility_level: visibility,"
    )
    .unwrap();
    writeln!(
        rb,
        "                                         organization_id: org.id,"
    )
    .unwrap();
    writeln!(
        rb,
        "                                         initialize_with_readme: true"
    )
    .unwrap();
    writeln!(rb, "                                       }}).execute").unwrap();
    writeln!(rb, "  project = unwrap(result, :project)").unwrap();
    writeln!(rb, "  raise \"Failed to create project '#{{name}}': #{{result.inspect}}\" unless project.is_a?(Project) && project.persisted?").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "  puts \"  Created project '#{{name}}' (id: #{{project.id}}, visibility: #{{visibility == 0 ? 'private' : 'public'}})\"").unwrap();
    writeln!(rb, "  project").unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb).unwrap();

    let pub_keys = public_project_keys(scenario);

    for (key, proj) in &scenario.projects {
        let vis = visibility_level(proj.visibility.as_deref());
        writeln!(
            rb,
            "proj_{key} = find_or_create_project('{name}', '{name}', {group}_group, admin, org, visibility: {vis})",
            name = proj.name,
            group = proj.group,
        )
        .unwrap();
    }
    writeln!(rb).unwrap();

    // all_public_projects and all_projects arrays
    let pub_list: Vec<String> = pub_keys.iter().map(|k| format!("proj_{k}")).collect();
    writeln!(rb, "all_public_projects = [{}]", pub_list.join(", ")).unwrap();

    let all_list: Vec<String> = scenario
        .projects
        .keys()
        .map(|k| format!("proj_{k}"))
        .collect();
    writeln!(rb, "all_projects = [{}]", all_list.join(", ")).unwrap();
    writeln!(rb).unwrap();

    // Manifest projects hash
    writeln!(rb, "manifest[:projects] = {{").unwrap();
    for (key, proj) in &scenario.projects {
        let vis_str = if proj.visibility.as_deref() == Some("private") {
            "private"
        } else {
            "public"
        };
        writeln!(
            rb,
            "  {key}: {{ id: proj_{key}.id, name: proj_{key}.name, path: proj_{key}.full_path,\n           group_key: :{group}, visibility: '{vis_str}' }},",
            group = proj.group,
        )
        .unwrap();
    }
    writeln!(rb, "}}").unwrap();
    writeln!(rb).unwrap();
}

fn emit_memberships_section(scenario: &Scenario, rb: &mut String) {
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "# 4. MEMBERSHIPS").unwrap();
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "puts \"\\n--- 4. Setting up memberships ---\"").unwrap();
    writeln!(rb).unwrap();

    // add_group_member helper
    writeln!(rb, "def add_group_member(group, user, access_level, label)").unwrap();
    writeln!(rb, "  return if group.member?(user)").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "  member = group.add_member(user, access_level)").unwrap();
    writeln!(rb, "  if member.persisted?").unwrap();
    writeln!(
        rb,
        "    puts \"  Added #{{user.username}} to group '#{{group.name}}' as #{{label}}\""
    )
    .unwrap();
    writeln!(rb, "  else").unwrap();
    writeln!(rb, "    puts \"  ERROR: Failed to add #{{user.username}} to group '#{{group.name}}': #{{member.errors.full_messages.join(', ')}}\"").unwrap();
    writeln!(rb, "  end").unwrap();
    writeln!(rb, "rescue StandardError => e").unwrap();
    writeln!(rb, "  puts \"  ERROR: Could not add #{{user.username}} to group '#{{group.name}}': #{{e.class}}: #{{e.message[0..120]}}\"").unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb).unwrap();

    for (user_key, memberships) in &scenario.memberships {
        for m in memberships {
            let access_const = ruby_access_level(&m.access);
            writeln!(
                rb,
                "add_group_member({group}_group, {user_key}, {access_const}, '{access}')",
                group = m.group,
                access = m.access,
            )
            .unwrap();
        }
    }
    writeln!(rb).unwrap();

    // Membership summary for manifest
    writeln!(rb, "manifest[:memberships] = {{").unwrap();
    for user_key in scenario.users.keys() {
        let memberships = scenario.memberships.get(user_key.as_str());
        let vis_keys = visible_project_keys(scenario, user_key);
        match memberships {
            Some(ms) if !ms.is_empty() => {
                let groups_list: Vec<String> = ms.iter().map(|m| format!(":{}", m.group)).collect();
                let vis_proj_list: Vec<String> = vis_keys.iter().map(|k| format!(":{k}")).collect();
                let traversal_list: Vec<String> = ms
                    .iter()
                    .map(|m| format!("manifest[:groups][:{}][:traversal]", m.group))
                    .collect();
                writeln!(rb, "  {user_key}: {{").unwrap();
                writeln!(rb, "    groups: [{}],", groups_list.join(", ")).unwrap();
                writeln!(rb, "    access_level: '{}',", ms[0].access).unwrap();
                writeln!(
                    rb,
                    "    visible_project_keys: [{}],",
                    vis_proj_list.join(", ")
                )
                .unwrap();
                writeln!(rb, "    visible_group_traversals: [").unwrap();
                for t in &traversal_list {
                    writeln!(rb, "      {t},").unwrap();
                }
                writeln!(rb, "    ]").unwrap();
                writeln!(rb, "  }},").unwrap();
            }
            _ => {
                writeln!(
                    rb,
                    "  {user_key}: {{ groups: [], visible_project_keys: [], visible_group_traversals: [] }},"
                )
                .unwrap();
            }
        }
    }
    writeln!(rb, "}}").unwrap();
    writeln!(rb).unwrap();
}

fn emit_kg_enabled_section(scenario: &Scenario, rb: &mut String) {
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "# 5. POPULATE knowledge_graph_enabled_namespaces").unwrap();
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(
        rb,
        "puts \"\\n--- 5. Populating knowledge_graph_enabled_namespaces ---\""
    )
    .unwrap();
    writeln!(rb).unwrap();

    let root_keys = root_group_keys(scenario);
    let list: Vec<String> = root_keys.iter().map(|k| format!("{k}_group")).collect();
    writeln!(rb, "root_groups = [{}]", list.join(", ")).unwrap();
    writeln!(rb, "root_groups.each do |group|").unwrap();
    writeln!(rb, "  ActiveRecord::Base.connection.execute(<<~SQL)").unwrap();
    writeln!(rb, "    INSERT INTO knowledge_graph_enabled_namespaces (root_namespace_id, created_at, updated_at)").unwrap();
    writeln!(rb, "    VALUES (#{{group.id}}, NOW(), NOW())").unwrap();
    writeln!(rb, "    ON CONFLICT (root_namespace_id) DO NOTHING").unwrap();
    writeln!(rb, "  SQL").unwrap();
    writeln!(
        rb,
        "  puts \"  Enabled namespace: #{{group.name}} (root_namespace_id: #{{group.id}})\""
    )
    .unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb).unwrap();
}

fn emit_milestones_section(scenario: &Scenario, rb: &mut String) {
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "# 6. CREATE MILESTONES").unwrap();
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "puts \"\\n--- 6. Creating milestones ---\"").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "milestone_count = 0").unwrap();

    // Group projects by milestone count to emit compact loops
    writeln!(rb, "all_projects.each do |proj|").unwrap();

    // Find the max milestone count across all projects (they're all the same in practice)
    let max_ms: u32 = scenario
        .projects
        .values()
        .filter_map(|p| p.entities.as_ref())
        .map(|e| e.milestones)
        .max()
        .unwrap_or(0);

    writeln!(rb, "  {max_ms}.times do |i|").unwrap();
    writeln!(rb, "    title = \"#{{proj.name}} Milestone #{{i + 1}}\"").unwrap();
    writeln!(rb, "    next if proj.milestones.find_by(title: title)").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "    ms = Milestones::CreateService.new(proj, admin, {{").unwrap();
    writeln!(rb, "                                         title: title,").unwrap();
    writeln!(rb, "                                         description: \"Milestone #{{i + 1}} for #{{proj.name}}\",").unwrap();
    writeln!(
        rb,
        "                                         start_date: Date.today - (30 * ({max_ms} - i)),"
    )
    .unwrap();
    writeln!(
        rb,
        "                                         due_date: Date.today + (30 * (i + 1))"
    )
    .unwrap();
    writeln!(rb, "                                       }}).execute").unwrap();
    writeln!(rb, "    milestone = unwrap(ms, :milestone)").unwrap();
    writeln!(rb, "    milestone_count += 1 if milestone&.persisted?").unwrap();
    writeln!(rb, "  end").unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb, "puts \"  Created #{{milestone_count}} new milestones (#{{all_projects.sum {{ |p| p.milestones.count }}}} total)\"").unwrap();
    writeln!(rb).unwrap();

    writeln!(
        rb,
        "manifest[:milestones] = all_projects.each_with_object({{}}) do |proj, h|"
    )
    .unwrap();
    writeln!(
        rb,
        "  key = manifest[:projects].find {{ |_k, v| v[:id] == proj.id }}&.first"
    )
    .unwrap();
    writeln!(rb, "  h[key] = proj.milestones.pluck(:id, :title).map {{ |id, title| {{ id: id, title: title }} }}").unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb).unwrap();
}

fn emit_labels_section(scenario: &Scenario, rb: &mut String) {
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "# 7. CREATE LABELS").unwrap();
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "puts \"\\n--- 7. Creating labels ---\"").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "label_count = 0").unwrap();
    writeln!(
        rb,
        "colors = %w[#FF0000 #00FF00 #0000FF #FF6600 #9900CC #009999]"
    )
    .unwrap();

    let max_labels: u32 = scenario
        .projects
        .values()
        .filter_map(|p| p.entities.as_ref())
        .map(|e| e.labels)
        .max()
        .unwrap_or(0);

    writeln!(rb, "all_projects.each_with_index do |proj, pi|").unwrap();
    writeln!(rb, "  {max_labels}.times do |i|").unwrap();
    writeln!(rb, "    title = \"#{{proj.name}}-label-#{{i + 1}}\"").unwrap();
    writeln!(rb, "    unless proj.labels.find_by(title: title)").unwrap();
    writeln!(rb, "      Labels::CreateService.new({{ title: title, color: colors[(pi * {max_labels} + i) % colors.size] }}).execute(project: proj)").unwrap();
    writeln!(rb, "      label_count += 1").unwrap();
    writeln!(rb, "    end").unwrap();
    writeln!(rb, "  end").unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb, "puts \"  Created #{{label_count}} new labels (#{{all_projects.sum {{ |p| p.labels.count }}}} total)\"").unwrap();
    writeln!(rb).unwrap();

    writeln!(
        rb,
        "manifest[:labels] = all_projects.each_with_object({{}}) do |proj, h|"
    )
    .unwrap();
    writeln!(
        rb,
        "  key = manifest[:projects].find {{ |_k, v| v[:id] == proj.id }}&.first"
    )
    .unwrap();
    writeln!(
        rb,
        "  h[key] = proj.labels.pluck(:id, :title).map {{ |id, title| {{ id: id, title: title }} }}"
    )
    .unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb).unwrap();
}

fn emit_work_items_section(scenario: &Scenario, rb: &mut String) {
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "# 8. CREATE WORK ITEMS").unwrap();
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "puts \"\\n--- 8. Creating work items (issues) ---\"").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "work_item_count = 0").unwrap();

    let max_wi: u32 = scenario
        .projects
        .values()
        .filter_map(|p| p.entities.as_ref())
        .map(|e| e.work_items)
        .max()
        .unwrap_or(0);

    writeln!(rb, "all_projects.each do |proj|").unwrap();
    writeln!(rb, "  milestones = proj.milestones.to_a").unwrap();
    writeln!(rb, "  labels = proj.labels.to_a").unwrap();
    writeln!(rb, "  {max_wi}.times do |i|").unwrap();
    writeln!(rb, "    title = \"#{{proj.name}} Issue #{{i + 1}}\"").unwrap();
    writeln!(rb, "    next if proj.issues.find_by(title: title)").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "    params = {{").unwrap();
    writeln!(rb, "      title: title,").unwrap();
    writeln!(rb, "      description: \"Test issue #{{i + 1}} for #{{proj.name}}. This exercises work item queries.\",").unwrap();
    writeln!(
        rb,
        "      milestone_id: milestones[i % milestones.size]&.id,"
    )
    .unwrap();
    writeln!(rb, "      label_ids: [labels[i % labels.size]&.id].compact").unwrap();
    writeln!(rb, "    }}").unwrap();
    writeln!(rb, "    result = Issues::CreateService.new(").unwrap();
    writeln!(rb, "      container: proj,").unwrap();
    writeln!(rb, "      current_user: admin,").unwrap();
    writeln!(rb, "      params: params").unwrap();
    writeln!(rb, "    ).execute").unwrap();
    writeln!(rb, "    issue = unwrap(result, :issue)").unwrap();
    writeln!(
        rb,
        "    work_item_count += 1 if issue.is_a?(Issue) && issue.persisted?"
    )
    .unwrap();
    writeln!(rb, "  end").unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb, "puts \"  Created #{{work_item_count}} new work items (#{{all_projects.sum {{ |p| p.issues.count }}}} total)\"").unwrap();
    writeln!(rb).unwrap();

    writeln!(
        rb,
        "manifest[:work_items] = all_projects.each_with_object({{}}) do |proj, h|"
    )
    .unwrap();
    writeln!(
        rb,
        "  key = manifest[:projects].find {{ |_k, v| v[:id] == proj.id }}&.first"
    )
    .unwrap();
    writeln!(
        rb,
        "  h[key] = proj.issues.pluck(:id, :title).map {{ |id, title| {{ id: id, title: title }} }}"
    )
    .unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb).unwrap();
}

fn emit_merge_requests_section(scenario: &Scenario, rb: &mut String) {
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "# 9. CREATE MERGE REQUESTS").unwrap();
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "puts \"\\n--- 9. Creating merge requests ---\"").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "mr_count = 0").unwrap();
    writeln!(rb).unwrap();

    // MRs are only created on public projects
    writeln!(rb, "all_public_projects.each do |proj|").unwrap();

    // Find typical MR counts (they're uniform across public projects)
    let (total_mrs, merged_mrs) = scenario
        .projects
        .values()
        .filter(|p| p.visibility.as_deref() != Some("private"))
        .find_map(|p| {
            p.entities
                .as_ref()
                .and_then(|e| e.merge_requests.as_ref())
                .map(|mr| (mr.total, mr.merged))
        })
        .unwrap_or((0, 0));

    writeln!(rb, "  {total_mrs}.times do |i|").unwrap();
    writeln!(rb, "    title = \"#{{proj.name}} MR #{{i + 1}}\"").unwrap();
    writeln!(
        rb,
        "    source_branch = \"feature/#{{proj.path}}-mr-#{{i + 1}}\""
    )
    .unwrap();
    writeln!(rb, "    next if proj.merge_requests.find_by(title: title)").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "    begin").unwrap();
    writeln!(
        rb,
        "      proj.repository.create_branch(source_branch, proj.default_branch || 'main')"
    )
    .unwrap();
    writeln!(rb, "    rescue StandardError").unwrap();
    writeln!(rb, "      # Branch may already exist").unwrap();
    writeln!(rb, "    end").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "    state = i < {merged_mrs} ? 'merged' : 'opened'").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "    result = MergeRequests::CreateService.new(").unwrap();
    writeln!(rb, "      project: proj,").unwrap();
    writeln!(rb, "      current_user: admin,").unwrap();
    writeln!(rb, "      params: {{").unwrap();
    writeln!(rb, "        title: title,").unwrap();
    writeln!(
        rb,
        "        description: \"Test MR #{{i + 1}} for #{{proj.name}}\","
    )
    .unwrap();
    writeln!(rb, "        source_branch: source_branch,").unwrap();
    writeln!(rb, "        target_branch: proj.default_branch || 'main'").unwrap();
    writeln!(rb, "      }}").unwrap();
    writeln!(rb, "    ).execute").unwrap();
    writeln!(rb, "    mr = unwrap(result, :merge_request)").unwrap();
    writeln!(rb).unwrap();
    writeln!(
        rb,
        "    if mr.is_a?(MergeRequest) && mr.persisted? && state == 'merged'"
    )
    .unwrap();
    writeln!(rb, "      mr.update_columns(state_id: 3)").unwrap();
    writeln!(rb, "      begin").unwrap();
    writeln!(
        rb,
        "        mr.metrics&.update_columns(merged_at: Time.current - ({total_mrs} - i).days)"
    )
    .unwrap();
    writeln!(rb, "      rescue StandardError").unwrap();
    writeln!(rb, "        nil").unwrap();
    writeln!(rb, "      end").unwrap();
    writeln!(rb, "    end").unwrap();
    writeln!(rb).unwrap();
    writeln!(
        rb,
        "    mr_count += 1 if mr.is_a?(MergeRequest) && mr.persisted?"
    )
    .unwrap();
    writeln!(rb, "  end").unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "puts \"  Created #{{mr_count}} new MRs (#{{all_projects.sum {{ |p| p.merge_requests.count }}}} total)\"").unwrap();
    writeln!(rb).unwrap();

    writeln!(
        rb,
        "manifest[:merge_requests] = all_projects.each_with_object({{}}) do |proj, h|"
    )
    .unwrap();
    writeln!(
        rb,
        "  key = manifest[:projects].find {{ |_k, v| v[:id] == proj.id }}&.first"
    )
    .unwrap();
    writeln!(
        rb,
        "  state_map = {{ 1 => 'opened', 2 => 'closed', 3 => 'merged', 4 => 'locked' }}"
    )
    .unwrap();
    writeln!(
        rb,
        "  mrs = proj.merge_requests.pluck(:id, :iid, :title, :state_id)"
    )
    .unwrap();
    writeln!(rb, "  h[key] = mrs.map {{ |id, iid, title, sid| {{ id: id, iid: iid, title: title, state: state_map[sid] || 'unknown' }} }}").unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb).unwrap();
}

fn emit_notes_section(scenario: &Scenario, rb: &mut String) {
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "# 10. CREATE NOTES").unwrap();
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "puts \"\\n--- 10. Creating notes ---\"").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "note_count = 0").unwrap();

    // Find typical note counts from the first public project
    let (per_mr, first_n_mrs, per_issue, first_n_issues) = scenario
        .projects
        .values()
        .filter(|p| p.visibility.as_deref() != Some("private"))
        .find_map(|p| {
            p.entities
                .as_ref()
                .and_then(|e| e.notes.as_ref())
                .map(|n| (n.per_mr, n.first_n_mrs, n.per_issue, n.first_n_issues))
        })
        .unwrap_or((0, 0, 0, 0));

    writeln!(rb, "all_public_projects.each do |proj|").unwrap();

    // Notes on MRs
    writeln!(
        rb,
        "  proj.merge_requests.limit({first_n_mrs}).each do |mr|"
    )
    .unwrap();
    writeln!(rb, "    {per_mr}.times do |i|").unwrap();
    writeln!(
        rb,
        "      body = \"Review comment #{{i + 1}} on #{{mr.title}}\""
    )
    .unwrap();
    writeln!(rb, "      next if Note.find_by(noteable: mr, note: body)").unwrap();
    writeln!(rb).unwrap();
    writeln!(
        rb,
        "      result = Notes::CreateService.new(proj, admin, {{"
    )
    .unwrap();
    writeln!(
        rb,
        "                                          noteable: mr,"
    )
    .unwrap();
    writeln!(rb, "                                          note: body").unwrap();
    writeln!(rb, "                                        }}).execute").unwrap();
    writeln!(rb, "      note = unwrap(result, :note)").unwrap();
    writeln!(
        rb,
        "      note_count += 1 if note.is_a?(Note) && note.persisted?"
    )
    .unwrap();
    writeln!(rb, "    end").unwrap();
    writeln!(rb, "  end").unwrap();
    writeln!(rb).unwrap();

    // Notes on issues
    writeln!(rb, "  proj.issues.limit({first_n_issues}).each do |issue|").unwrap();
    writeln!(rb, "    {per_issue}.times do |i|").unwrap();
    writeln!(
        rb,
        "      body = \"Discussion comment #{{i + 1}} on #{{issue.title}}\""
    )
    .unwrap();
    writeln!(
        rb,
        "      next if Note.find_by(noteable: issue, note: body)"
    )
    .unwrap();
    writeln!(rb).unwrap();
    writeln!(
        rb,
        "      result = Notes::CreateService.new(proj, admin, {{"
    )
    .unwrap();
    writeln!(
        rb,
        "                                          noteable: issue,"
    )
    .unwrap();
    writeln!(rb, "                                          note: body").unwrap();
    writeln!(rb, "                                        }}).execute").unwrap();
    writeln!(rb, "      note = unwrap(result, :note)").unwrap();
    writeln!(
        rb,
        "      note_count += 1 if note.is_a?(Note) && note.persisted?"
    )
    .unwrap();
    writeln!(rb, "    end").unwrap();
    writeln!(rb, "  end").unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb, "puts \"  Created #{{note_count}} new notes\"").unwrap();
    writeln!(rb).unwrap();

    writeln!(
        rb,
        "manifest[:notes] = all_projects.each_with_object({{}}) do |proj, h|"
    )
    .unwrap();
    writeln!(
        rb,
        "  key = manifest[:projects].find {{ |_k, v| v[:id] == proj.id }}&.first"
    )
    .unwrap();
    writeln!(rb, "  count = Note.joins(\"INNER JOIN issues ON notes.noteable_type = 'Issue' AND notes.noteable_id = issues.id\")").unwrap();
    writeln!(
        rb,
        "              .where(issues: {{ project_id: proj.id }})"
    )
    .unwrap();
    writeln!(rb, "              .where(system: false).count +").unwrap();
    writeln!(rb, "          Note.joins(\"INNER JOIN merge_requests ON notes.noteable_type = 'MergeRequest' AND notes.noteable_id = merge_requests.id\")").unwrap();
    writeln!(
        rb,
        "              .where(merge_requests: {{ target_project_id: proj.id }})"
    )
    .unwrap();
    writeln!(rb, "              .where(system: false).count").unwrap();
    writeln!(rb, "  h[key] = {{ count: count }}").unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb).unwrap();
}

fn emit_counts_section(scenario: &Scenario, rb: &mut String) {
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "# 11. COMPUTE COUNTS FOR MANIFEST").unwrap();
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "puts \"\\n--- 11. Computing entity counts ---\"").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "total_projects = Project.count").unwrap();
    writeln!(rb, "total_users = User.count").unwrap();
    writeln!(rb, "total_groups = Group.count").unwrap();
    writeln!(rb, "total_mrs = MergeRequest.count").unwrap();
    writeln!(rb, "total_work_items = Issue.count").unwrap();
    writeln!(rb, "total_labels = Label.count").unwrap();
    writeln!(rb, "total_milestones = Milestone.count").unwrap();
    writeln!(rb, "total_notes = Note.where(system: false).count").unwrap();
    writeln!(rb).unwrap();

    writeln!(rb, "manifest[:counts] = {{").unwrap();
    writeln!(rb, "  total_projects: total_projects,").unwrap();
    writeln!(rb, "  total_users: total_users,").unwrap();
    writeln!(rb, "  total_groups: total_groups,").unwrap();
    writeln!(rb, "  total_merge_requests: total_mrs,").unwrap();
    writeln!(rb, "  total_work_items: total_work_items,").unwrap();
    writeln!(rb, "  total_labels: total_labels,").unwrap();
    writeln!(rb, "  total_milestones: total_milestones,").unwrap();
    writeln!(rb, "  total_notes: total_notes,").unwrap();
    writeln!(rb, "  per_project: {{}}").unwrap();
    writeln!(rb, "}}").unwrap();
    writeln!(rb).unwrap();

    writeln!(rb, "all_projects.each do |proj|").unwrap();
    writeln!(
        rb,
        "  key = manifest[:projects].find {{ |_k, v| v[:id] == proj.id }}&.first"
    )
    .unwrap();
    writeln!(rb, "  manifest[:counts][:per_project][key] = {{").unwrap();
    writeln!(rb, "    merge_requests: proj.merge_requests.count,").unwrap();
    writeln!(rb, "    work_items: proj.issues.count,").unwrap();
    writeln!(rb, "    milestones: proj.milestones.count,").unwrap();
    writeln!(rb, "    labels: proj.labels.count,").unwrap();
    writeln!(rb, "    notes: manifest[:notes][key][:count]").unwrap();
    writeln!(rb, "  }}").unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb).unwrap();

    // Per-user expected visible counts
    writeln!(
        rb,
        "# Per-user expected visible counts (based on group memberships)"
    )
    .unwrap();
    for user_key in scenario.users.keys() {
        let vis = visible_project_keys(scenario, user_key);
        if vis.is_empty() {
            continue;
        }
        let list: Vec<String> = vis.iter().map(|k| format!(":{k}")).collect();
        writeln!(rb, "{user_key}_visible = [{}]", list.join(", ")).unwrap();
    }
    writeln!(rb).unwrap();

    writeln!(rb, "manifest[:counts][:per_user] = {{").unwrap();
    // root sees everything
    writeln!(rb, "  root: {{").unwrap();
    writeln!(rb, "    projects: total_projects,").unwrap();
    writeln!(rb, "    merge_requests: total_mrs,").unwrap();
    writeln!(rb, "    work_items: total_work_items,").unwrap();
    writeln!(rb, "    notes: total_notes").unwrap();
    writeln!(rb, "  }},").unwrap();

    for user_key in scenario.users.keys() {
        let vis = visible_project_keys(scenario, user_key);
        if vis.is_empty() {
            writeln!(
                rb,
                "  {user_key}: {{ projects: 0, merge_requests: 0, work_items: 0, notes: 0 }},"
            )
            .unwrap();
        } else {
            writeln!(rb, "  {user_key}: {{").unwrap();
            writeln!(rb, "    projects: {user_key}_visible.size,").unwrap();
            writeln!(rb, "    merge_requests: {user_key}_visible.sum {{ |k| manifest[:counts][:per_project][k][:merge_requests] }},").unwrap();
            writeln!(rb, "    work_items: {user_key}_visible.sum {{ |k| manifest[:counts][:per_project][k][:work_items] }},").unwrap();
            writeln!(rb, "    notes: {user_key}_visible.sum {{ |k| manifest[:counts][:per_project][k][:notes] }}").unwrap();
            writeln!(rb, "  }},").unwrap();
        }
    }
    writeln!(rb, "}}").unwrap();
    writeln!(rb).unwrap();
}

fn emit_write_manifest(_scenario: &Scenario, rb: &mut String) {
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "# 12. WRITE MANIFEST").unwrap();
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "puts \"\\n--- 12. Writing manifest ---\"").unwrap();
    writeln!(rb).unwrap();
    writeln!(
        rb,
        "File.write(MANIFEST_PATH, JSON.pretty_generate(manifest))"
    )
    .unwrap();
    writeln!(rb, "puts \"  Manifest written to #{{MANIFEST_PATH}}\"").unwrap();
    writeln!(rb).unwrap();
}

fn emit_summary(scenario: &Scenario, rb: &mut String) {
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "# SUMMARY").unwrap();
    writeln!(
        rb,
        "# ============================================================================="
    )
    .unwrap();
    writeln!(rb, "puts \"\\n=== E2E TEST DATA SUMMARY ===\"").unwrap();
    writeln!(rb, "puts \"Organization: #{{org.id}}\"").unwrap();
    writeln!(rb, "puts ''").unwrap();
    writeln!(rb, "puts 'Users:'").unwrap();
    writeln!(rb, "manifest[:users].each {{ |k, v| puts \"  #{{k}}: id=#{{v[:id]}} username=#{{v[:username]}}\" }}").unwrap();
    writeln!(rb, "puts ''").unwrap();
    writeln!(rb, "puts 'Groups:'").unwrap();
    writeln!(rb, "manifest[:groups].each {{ |k, v| puts \"  #{{k}}: id=#{{v[:id]}} path=#{{v[:path]}} traversal=#{{v[:traversal]}}\" }}").unwrap();
    writeln!(rb, "puts ''").unwrap();
    writeln!(rb, "puts 'Projects:'").unwrap();
    writeln!(rb, "manifest[:projects].each {{ |k, v| puts \"  #{{k}}: id=#{{v[:id]}} name=#{{v[:name]}} visibility=#{{v[:visibility]}}\" }}").unwrap();
    writeln!(rb, "puts ''").unwrap();
    writeln!(rb, "puts 'Entity counts:'").unwrap();
    writeln!(rb, "puts \"  Projects:        #{{total_projects}}\"").unwrap();
    writeln!(rb, "puts \"  Users:           #{{total_users}}\"").unwrap();
    writeln!(rb, "puts \"  Groups:          #{{total_groups}}\"").unwrap();
    writeln!(rb, "puts \"  MergeRequests:   #{{total_mrs}}\"").unwrap();
    writeln!(rb, "puts \"  WorkItems:       #{{total_work_items}}\"").unwrap();
    writeln!(rb, "puts \"  Labels:          #{{total_labels}}\"").unwrap();
    writeln!(rb, "puts \"  Milestones:      #{{total_milestones}}\"").unwrap();
    writeln!(rb, "puts \"  Notes:           #{{total_notes}}\"").unwrap();
    writeln!(rb, "puts ''").unwrap();
    writeln!(rb, "puts 'Per-user visible counts:'").unwrap();
    writeln!(rb, "manifest[:counts][:per_user].each do |user, counts|").unwrap();
    writeln!(
        rb,
        "  puts \"  #{{user}}: #{{counts.map {{ |k, v| \"#{{k}}=#{{v}}\" }}.join(', ')}}\""
    )
    .unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb, "puts ''").unwrap();

    // Membership summary
    writeln!(rb, "puts 'Memberships:'").unwrap();
    for user_key in scenario.users.keys() {
        let vis = visible_project_keys(scenario, user_key);
        let memberships = scenario.memberships.get(user_key.as_str());
        match memberships {
            Some(ms) if !ms.is_empty() => {
                let groups: Vec<&str> = ms.iter().map(|m| m.group.as_str()).collect();
                let projects: Vec<&str> = vis.iter().map(|s| s.as_str()).collect();
                writeln!(
                    rb,
                    "puts '  {user_key}: {} on {} -> sees {} projects'",
                    ms[0].access,
                    groups.join(" + "),
                    projects.join(", "),
                )
                .unwrap();
            }
            _ => {
                writeln!(rb, "puts '  {user_key}: no memberships -> sees nothing'").unwrap();
            }
        }
    }
    writeln!(rb, "puts ''").unwrap();
    writeln!(rb, "puts '=== DONE ==='").unwrap();
}

// ---------------------------------------------------------------------------
// Ruby codegen: redaction_test.rb
// ---------------------------------------------------------------------------

fn generate_redaction_test(scenario: &Scenario) -> String {
    let mut rb = String::with_capacity(8192);

    // Header
    writeln!(rb, "# frozen_string_literal: true").unwrap();
    writeln!(rb).unwrap();
    writeln!(
        rb,
        "# AUTO-GENERATED from e2e/tests/scenarios.yaml — do not edit directly."
    )
    .unwrap();
    writeln!(rb, "# Regenerate: cargo xtask e2e codegen").unwrap();
    writeln!(rb, "# Verify:     cargo xtask e2e codegen --check").unwrap();
    writeln!(rb).unwrap();

    writeln!(rb, "require_relative 'test_helper'").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "Feature.enable(:knowledge_graph)").unwrap();
    writeln!(rb).unwrap();
    writeln!(rb, "manifest = load_manifest!").unwrap();
    writeln!(rb, "m = manifest").unwrap();
    writeln!(rb).unwrap();

    // gRPC client setup
    writeln!(
        rb,
        "grpc_endpoint = ENV.fetch('KNOWLEDGE_GRAPH_GRPC_ENDPOINT',"
    )
    .unwrap();
    writeln!(
        rb,
        "                          'gkg-webserver.default.svc.cluster.local:50054')"
    )
    .unwrap();
    writeln!(rb, "puts \"  gRPC endpoint: #{{grpc_endpoint}}\"").unwrap();
    writeln!(
        rb,
        "client = Ai::KnowledgeGraph::GrpcClient.new(endpoint: grpc_endpoint)"
    )
    .unwrap();
    writeln!(rb, "org_id = m[:organization_id]").unwrap();
    writeln!(rb).unwrap();

    // Load users
    writeln!(rb, "# Load users from manifest").unwrap();
    writeln!(
        rb,
        "root = User.find_by!(username: m[:users][:root][:username])"
    )
    .unwrap();
    for key in scenario.users.keys() {
        writeln!(
            rb,
            "{key} = User.find_by!(username: m[:users][:{key}][:username])"
        )
        .unwrap();
    }
    writeln!(rb).unwrap();

    // Build vars hash for variable resolution
    writeln!(rb, "# Variable resolution hash for $variable references").unwrap();
    writeln!(rb, "vars = {{").unwrap();
    writeln!(rb, "  'total_projects' => m[:counts][:total_projects],").unwrap();
    writeln!(
        rb,
        "  'total_merge_requests' => m[:counts][:total_merge_requests],"
    )
    .unwrap();
    writeln!(rb, "  'total_work_items' => m[:counts][:total_work_items],").unwrap();
    writeln!(rb, "  'total_notes' => m[:counts][:total_notes],").unwrap();

    for key in scenario.projects.keys() {
        writeln!(rb, "  'proj.{key}' => m[:projects][:{key}][:id],").unwrap();
    }
    for key in flatten_groups(&scenario.groups).iter().map(|g| &g.key) {
        writeln!(rb, "  'group.{key}' => m[:groups][:{key}][:id],").unwrap();
    }

    // Per-user counts
    let all_user_keys: Vec<&str> = std::iter::once("root")
        .chain(scenario.users.keys().map(|s| s.as_str()))
        .collect();
    for uk in &all_user_keys {
        for entity in &["projects", "merge_requests", "work_items", "notes"] {
            writeln!(
                rb,
                "  'user_counts.{uk}.{entity}' => m[:counts][:per_user][:{uk}][:{entity}],"
            )
            .unwrap();
        }
    }

    // Per-project counts
    for pk in scenario.projects.keys() {
        for entity in &[
            "merge_requests",
            "work_items",
            "milestones",
            "labels",
            "notes",
        ] {
            writeln!(
                rb,
                "  'project_counts.{pk}.{entity}' => m[:counts][:per_project][:{pk}][:{entity}],"
            )
            .unwrap();
        }
    }
    writeln!(rb, "}}").unwrap();
    writeln!(rb).unwrap();

    // resolve helper
    writeln!(rb, "def resolve(val, vars)").unwrap();
    writeln!(
        rb,
        "  return val unless val.is_a?(String) && val.start_with?('$')"
    )
    .unwrap();
    writeln!(rb, "  key = val[1..]").unwrap();
    writeln!(rb, "  resolved = vars[key]").unwrap();
    writeln!(rb, "  raise \"Unknown variable: #{{val}} (available: #{{vars.keys.sort.join(', ')}})\" if resolved.nil?").unwrap();
    writeln!(rb, "  resolved").unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb).unwrap();

    // resolve_query helper: replace $variable references inside JSON strings
    writeln!(rb, "def resolve_query(json_str, vars)").unwrap();
    writeln!(rb, "  json_str.gsub(/\"\\$([a-zA-Z0-9_.]+)\"/) do").unwrap();
    writeln!(rb, "    key = $1").unwrap();
    writeln!(rb, "    resolved = vars[key]").unwrap();
    writeln!(rb, "    raise \"Unknown variable in query: $#{{key}} (available: #{{vars.keys.sort.join(', ')}})\" if resolved.nil?").unwrap();
    writeln!(rb, "    resolved.to_s").unwrap();
    writeln!(rb, "  end").unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb).unwrap();

    // Emit assertion sections
    for section in &scenario.assertions {
        emit_assertion_section(scenario, section, &mut rb);
    }

    // Summary
    writeln!(rb, "TestHarness.summary").unwrap();

    rb
}

fn emit_assertion_section(scenario: &Scenario, section: &AssertionSection, rb: &mut String) {
    if let Some(users_list) = &section.users {
        // Fan-out section: same tests for multiple users
        for user_key in users_list {
            let username = ruby_username(scenario, user_key);
            writeln!(
                rb,
                "# ============================================================================="
            )
            .unwrap();
            writeln!(rb, "TestHarness.section('{}')", section.section).unwrap();
            writeln!(rb).unwrap();

            for test in &section.tests {
                let prefixed_name = format!("{username}: {}", test.name);
                emit_test_case(scenario, &prefixed_name, user_key, test, rb);
            }
        }
    } else if let Some(user_key) = &section.user {
        // Single user section
        writeln!(
            rb,
            "# ============================================================================="
        )
        .unwrap();
        writeln!(rb, "TestHarness.section('{}')", section.section).unwrap();
        writeln!(rb).unwrap();

        for test in &section.tests {
            let test_user = test.user.as_deref().unwrap_or(user_key.as_str());
            emit_test_case(scenario, &test.name, test_user, test, rb);
        }
    } else {
        // Mixed section (tests specify their own users)
        writeln!(
            rb,
            "# ============================================================================="
        )
        .unwrap();
        writeln!(rb, "TestHarness.section('{}')", section.section).unwrap();
        writeln!(rb).unwrap();

        for test in &section.tests {
            let test_user = test
                .user
                .as_deref()
                .expect("test must specify user when section has no user");
            emit_test_case(scenario, &test.name, test_user, test, rb);
        }
    }
}

fn emit_test_case(
    scenario: &Scenario,
    name: &str,
    user_key: &str,
    test: &TestCase,
    rb: &mut String,
) {
    let _ = scenario; // available for future use

    // Resolve expect into expected_min/expected_max args
    let expect_args = if let Some(val) = &test.expect.eq {
        let val_str = expect_value_to_ruby(val);
        format!("expected_min: {val_str}, expected_max: {val_str}")
    } else if let Some(val) = &test.expect.gte {
        let val_str = expect_value_to_ruby(val);
        format!("expected_min: {val_str}")
    } else if let Some(vals) = &test.expect.range {
        let min_str = expect_value_to_ruby(&vals[0]);
        let max_str = expect_value_to_ruby(&vals[1]);
        format!("expected_min: {min_str}, expected_max: {max_str}")
    } else {
        panic!(
            "test '{}' has no expect clause (need eq, gte, or range)",
            test.name
        );
    };

    // Compact the query JSON onto one line for the resolve_query call
    let query_trimmed = test.query.trim();

    // Resolve $variable references in the test name for display
    let ruby_name = if name.contains('$') {
        // Build a Ruby expression that interpolates variables
        let mut parts = String::new();
        let mut rest = name;
        while let Some(dollar_pos) = rest.find('$') {
            parts.push_str(&rest[..dollar_pos]);
            let after = &rest[dollar_pos + 1..];
            let var_end = after
                .find(|c: char| !c.is_alphanumeric() && c != '_' && c != '.')
                .unwrap_or(after.len());
            let var_name = &after[..var_end];
            write!(parts, "#{{resolve('${var_name}', vars)}}").unwrap();
            rest = &after[var_end..];
        }
        parts.push_str(rest);
        parts
    } else {
        name.to_string()
    };

    writeln!(rb, "TestHarness.run(\"{ruby_name}\", {expect_args}) do").unwrap();
    writeln!(
        rb,
        "  q(client, {user_key}, org_id, JSON.parse(resolve_query('{query}', vars)))",
        query = escape_ruby_single_quote(query_trimmed),
    )
    .unwrap();
    writeln!(rb, "end").unwrap();
    writeln!(rb).unwrap();
}

fn expect_value_to_ruby(val: &serde_yaml::Value) -> String {
    match val {
        serde_yaml::Value::Number(n) => n.to_string(),
        serde_yaml::Value::String(s) if s.starts_with('$') => {
            format!("resolve('{s}', vars)")
        }
        serde_yaml::Value::String(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

fn escape_ruby_single_quote(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}
