//! Generates Ruby e2e test scripts from `e2e/tests/scenarios.yaml`.
//!
//! Reads the declarative YAML scenario file and renders two Tera templates:
//!
//! - `create_test_data.rb` — creates users, groups, projects, memberships,
//!   and SDLC entities via GitLab service objects. Writes a JSON manifest.
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

use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tera::Tera;

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
// Tera context types (Serialize for template rendering)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct UserCtx {
    key: String,
    username: String,
    name: String,
    email: String,
}

#[derive(Serialize)]
struct GroupCtx {
    key: String,
    name: String,
    path: String,
    parent_arg: String,
    visibility: u32,
}

#[derive(Serialize)]
struct ProjectCtx {
    key: String,
    name: String,
    group: String,
    vis_level: u32,
    vis_str: String,
}

#[derive(Serialize)]
struct MembershipCallCtx {
    group: String,
    user_key: String,
    access_const: String,
    access: String,
}

#[derive(Serialize)]
struct MembershipManifestCtx {
    user_key: String,
    has_memberships: bool,
    groups_list: String,
    access_level: String,
    vis_proj_list: String,
    traversal_list: Vec<String>,
}

#[derive(Serialize)]
struct UserCountCtx {
    user_key: String,
    visible_keys: Vec<String>,
    visible_keys_str: String,
}

/// Flattened test case entry for redaction_test template.
/// Each entry is either a section header or a test.
#[derive(Serialize)]
struct TestCaseCtx {
    is_section_header: bool,
    section: String,
    is_test: bool,
    ruby_name: String,
    expect_args: String,
    user_key: String,
    query: String,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub fn run(gkg_root: &Path, check: bool) -> Result<()> {
    let yaml_path = gkg_root.join(constants::SCENARIOS_YAML);
    let content = fs::read_to_string(&yaml_path)
        .with_context(|| format!("failed to read {}", yaml_path.display()))?;
    let scenario: Scenario =
        serde_yaml::from_str(&content).context("failed to parse scenarios.yaml")?;

    let templates_dir = gkg_root.join("e2e/tests/templates");
    let glob = format!("{}/**/*.tera", templates_dir.display());
    let tera = Tera::new(&glob).context("failed to load Tera templates")?;

    let create_ctx = build_create_context(&scenario);
    let create_data_rb = collapse_blank_lines(
        &tera
            .render("create_test_data.rb.tera", &create_ctx)
            .context("failed to render create_test_data.rb.tera")?,
    );

    let redaction_ctx = build_redaction_context(&scenario);
    let redaction_test_rb = collapse_blank_lines(
        &tera
            .render("redaction_test.rb.tera", &redaction_ctx)
            .context("failed to render redaction_test.rb.tera")?,
    );

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
// Context builders
// ---------------------------------------------------------------------------

fn build_create_context(scenario: &Scenario) -> tera::Context {
    let mut ctx = tera::Context::new();

    ctx.insert("password", &scenario.password);

    // Users
    let users: Vec<UserCtx> = scenario
        .users
        .iter()
        .map(|(key, def)| UserCtx {
            key: key.clone(),
            username: def.username.clone().unwrap_or_else(|| key.clone()),
            name: def.name.clone(),
            email: def.email.clone(),
        })
        .collect();
    ctx.insert("users", &users);

    // Groups
    let flat = flatten_groups(&scenario.groups);
    let groups: Vec<GroupCtx> = flat
        .iter()
        .map(|g| GroupCtx {
            key: g.key.clone(),
            name: g.name.clone(),
            path: g.path.clone(),
            parent_arg: match &g.parent_key {
                Some(pk) => format!(", parent: {pk}_group"),
                None => String::new(),
            },
            visibility: g.visibility,
        })
        .collect();
    ctx.insert("groups", &groups);

    // Projects
    let projects: Vec<ProjectCtx> = scenario
        .projects
        .iter()
        .map(|(key, def)| {
            let vis = visibility_level(def.visibility.as_deref());
            ProjectCtx {
                key: key.clone(),
                name: def.name.clone(),
                group: def.group.clone(),
                vis_level: vis,
                vis_str: if def.visibility.as_deref() == Some("private") {
                    "private"
                } else {
                    "public"
                }
                .to_string(),
            }
        })
        .collect();
    ctx.insert("projects", &projects);

    let pub_keys = public_project_keys(scenario);
    let pub_project_vars: Vec<String> = pub_keys.iter().map(|k| format!("proj_{k}")).collect();
    let all_project_vars: Vec<String> = scenario
        .projects
        .keys()
        .map(|k| format!("proj_{k}"))
        .collect();
    ctx.insert("pub_project_vars", &pub_project_vars);
    ctx.insert("all_project_vars", &all_project_vars);

    // Membership calls
    let mut membership_calls = Vec::new();
    for (user_key, memberships) in &scenario.memberships {
        for m in memberships {
            membership_calls.push(MembershipCallCtx {
                group: m.group.clone(),
                user_key: user_key.clone(),
                access_const: ruby_access_level(&m.access).to_string(),
                access: m.access.clone(),
            });
        }
    }
    ctx.insert("membership_calls", &membership_calls);

    // Membership manifest
    let membership_manifest: Vec<MembershipManifestCtx> = scenario
        .users
        .keys()
        .map(|user_key| {
            let memberships = scenario.memberships.get(user_key.as_str());
            let vis_keys = visible_project_keys(scenario, user_key);
            match memberships {
                Some(ms) if !ms.is_empty() => {
                    let groups_list: Vec<String> =
                        ms.iter().map(|m| format!(":{}", m.group)).collect();
                    let vis_proj_list: Vec<String> =
                        vis_keys.iter().map(|k| format!(":{k}")).collect();
                    let traversal_list: Vec<String> = ms
                        .iter()
                        .map(|m| format!("manifest[:groups][:{}][:traversal]", m.group))
                        .collect();
                    MembershipManifestCtx {
                        user_key: user_key.clone(),
                        has_memberships: true,
                        groups_list: groups_list.join(", "),
                        access_level: ms[0].access.clone(),
                        vis_proj_list: vis_proj_list.join(", "),
                        traversal_list,
                    }
                }
                _ => MembershipManifestCtx {
                    user_key: user_key.clone(),
                    has_memberships: false,
                    groups_list: String::new(),
                    access_level: String::new(),
                    vis_proj_list: String::new(),
                    traversal_list: Vec::new(),
                },
            }
        })
        .collect();
    ctx.insert("membership_manifest", &membership_manifest);

    // Root groups for kg_enabled_namespaces
    let root_group_vars: Vec<String> = root_group_keys(scenario)
        .iter()
        .map(|k| format!("{k}_group"))
        .collect();
    ctx.insert("root_group_vars", &root_group_vars);

    // Entity counts
    let max_ms: u32 = scenario
        .projects
        .values()
        .filter_map(|p| p.entities.as_ref())
        .map(|e| e.milestones)
        .max()
        .unwrap_or(0);
    let max_labels: u32 = scenario
        .projects
        .values()
        .filter_map(|p| p.entities.as_ref())
        .map(|e| e.labels)
        .max()
        .unwrap_or(0);
    let max_wi: u32 = scenario
        .projects
        .values()
        .filter_map(|p| p.entities.as_ref())
        .map(|e| e.work_items)
        .max()
        .unwrap_or(0);
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

    ctx.insert("max_ms", &max_ms);
    ctx.insert("max_labels", &max_labels);
    ctx.insert("max_wi", &max_wi);
    ctx.insert("total_mrs", &total_mrs);
    ctx.insert("merged_mrs", &merged_mrs);
    ctx.insert("per_mr", &per_mr);
    ctx.insert("first_n_mrs", &first_n_mrs);
    ctx.insert("per_issue", &per_issue);
    ctx.insert("first_n_issues", &first_n_issues);

    // Per-user visible counts
    let user_counts: Vec<UserCountCtx> = scenario
        .users
        .keys()
        .map(|user_key| {
            let vis = visible_project_keys(scenario, user_key);
            let vis_str: Vec<String> = vis.iter().map(|k| format!(":{k}")).collect();
            UserCountCtx {
                user_key: user_key.clone(),
                visible_keys: vis.clone(),
                visible_keys_str: vis_str.join(", "),
            }
        })
        .collect();
    ctx.insert("user_counts", &user_counts);

    // Membership summary lines
    let membership_summary: Vec<String> = scenario
        .users
        .keys()
        .map(|user_key| {
            let vis = visible_project_keys(scenario, user_key);
            let memberships = scenario.memberships.get(user_key.as_str());
            match memberships {
                Some(ms) if !ms.is_empty() => {
                    let groups: Vec<&str> = ms.iter().map(|m| m.group.as_str()).collect();
                    let projects: Vec<&str> = vis.iter().map(|s| s.as_str()).collect();
                    format!(
                        "{user_key}: {} on {} -> sees {} projects",
                        ms[0].access,
                        groups.join(" + "),
                        projects.join(", "),
                    )
                }
                _ => format!("{user_key}: no memberships -> sees nothing"),
            }
        })
        .collect();
    ctx.insert("membership_summary", &membership_summary);

    ctx
}

fn build_redaction_context(scenario: &Scenario) -> tera::Context {
    let mut ctx = tera::Context::new();

    // Users
    let users: Vec<UserCtx> = scenario
        .users
        .iter()
        .map(|(key, def)| UserCtx {
            key: key.clone(),
            username: def.username.clone().unwrap_or_else(|| key.clone()),
            name: def.name.clone(),
            email: def.email.clone(),
        })
        .collect();
    ctx.insert("users", &users);

    // Keys for vars hash
    let project_keys: Vec<String> = scenario.projects.keys().cloned().collect();
    let group_keys: Vec<String> = flatten_groups(&scenario.groups)
        .iter()
        .map(|g| g.key.clone())
        .collect();
    let all_user_keys: Vec<String> = std::iter::once("root".to_string())
        .chain(scenario.users.keys().cloned())
        .collect();
    ctx.insert("project_keys", &project_keys);
    ctx.insert("group_keys", &group_keys);
    ctx.insert("all_user_keys", &all_user_keys);

    // Flatten all assertion sections into a linear sequence of section headers + tests
    let mut test_cases: Vec<TestCaseCtx> = Vec::new();
    for section in &scenario.assertions {
        if let Some(users_list) = &section.users {
            for user_key in users_list {
                let username = ruby_username(scenario, user_key);
                test_cases.push(TestCaseCtx {
                    is_section_header: true,
                    section: section.section.clone(),
                    is_test: false,
                    ruby_name: String::new(),
                    expect_args: String::new(),
                    user_key: String::new(),
                    query: String::new(),
                });
                for test in &section.tests {
                    let prefixed_name = format!("{username}: {}", test.name);
                    test_cases.push(build_test_ctx(&prefixed_name, user_key, test));
                }
            }
        } else if let Some(user_key) = &section.user {
            test_cases.push(TestCaseCtx {
                is_section_header: true,
                section: section.section.clone(),
                is_test: false,
                ruby_name: String::new(),
                expect_args: String::new(),
                user_key: String::new(),
                query: String::new(),
            });
            for test in &section.tests {
                let test_user = test.user.as_deref().unwrap_or(user_key.as_str());
                test_cases.push(build_test_ctx(&test.name, test_user, test));
            }
        } else {
            test_cases.push(TestCaseCtx {
                is_section_header: true,
                section: section.section.clone(),
                is_test: false,
                ruby_name: String::new(),
                expect_args: String::new(),
                user_key: String::new(),
                query: String::new(),
            });
            for test in &section.tests {
                let test_user = test
                    .user
                    .as_deref()
                    .expect("test must specify user when section has no user");
                test_cases.push(build_test_ctx(&test.name, test_user, test));
            }
        }
    }
    ctx.insert("test_cases", &test_cases);

    ctx
}

fn build_test_ctx(name: &str, user_key: &str, test: &TestCase) -> TestCaseCtx {
    let expect_args = if let Some(val) = &test.expect.eq {
        let v = expect_value_to_ruby(val);
        format!("expected_min: {v}, expected_max: {v}")
    } else if let Some(val) = &test.expect.gte {
        let v = expect_value_to_ruby(val);
        format!("expected_min: {v}")
    } else if let Some(vals) = &test.expect.range {
        let min = expect_value_to_ruby(&vals[0]);
        let max = expect_value_to_ruby(&vals[1]);
        format!("expected_min: {min}, expected_max: {max}")
    } else {
        panic!(
            "test '{}' has no expect clause (need eq, gte, or range)",
            test.name
        );
    };

    let ruby_name = if name.contains('$') {
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

    TestCaseCtx {
        is_section_header: false,
        section: String::new(),
        is_test: true,
        ruby_name,
        expect_args,
        user_key: user_key.to_string(),
        query: escape_ruby_single_quote(test.query.trim()),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

fn visibility_level(vis: Option<&str>) -> u32 {
    match vis {
        Some("private") => 0,
        _ => 20,
    }
}

struct GroupInfo {
    key: String,
    name: String,
    path: String,
    parent_key: Option<String>,
    visibility: u32,
}

fn flatten_groups(groups: &BTreeMap<String, GroupDef>) -> Vec<GroupInfo> {
    let mut out = Vec::new();
    for (key, def) in groups {
        let display_name = def.name.clone().unwrap_or_else(|| key.replace('_', "-"));
        let path = display_name.clone();
        out.push(GroupInfo {
            key: key.clone(),
            name: display_name,
            path,
            parent_key: None,
            visibility: visibility_level(def.visibility.as_deref()),
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
        out.push(GroupInfo {
            key: key.clone(),
            name: display_name,
            path,
            parent_key: Some(parent_key.to_string()),
            visibility: visibility_level(def.visibility.as_deref()),
        });
        flatten_children(&def.children, key, out);
    }
}

fn public_project_keys(scenario: &Scenario) -> Vec<String> {
    scenario
        .projects
        .iter()
        .filter(|(_, p)| p.visibility.as_deref() != Some("private"))
        .map(|(k, _)| k.clone())
        .collect()
}

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

fn root_group_keys(scenario: &Scenario) -> Vec<String> {
    scenario.groups.keys().cloned().collect()
}

fn expect_value_to_ruby(val: &serde_yaml::Value) -> String {
    match val {
        serde_yaml::Value::Number(n) => n.to_string(),
        serde_yaml::Value::String(s) if s.starts_with('$') => format!("resolve('{s}', vars)"),
        serde_yaml::Value::String(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

fn escape_ruby_single_quote(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Collapse runs of 2+ blank lines into a single blank line.
fn collapse_blank_lines(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut blank_run = 0u32;
    for line in s.lines() {
        if line.trim().is_empty() {
            blank_run += 1;
            if blank_run <= 1 {
                out.push('\n');
            }
        } else {
            blank_run = 0;
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}
