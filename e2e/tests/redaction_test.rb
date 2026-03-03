# frozen_string_literal: true

# AUTO-GENERATED from e2e/tests/scenarios.yaml — do not edit directly.
# Regenerate: cargo xtask e2e codegen
# Verify:     cargo xtask e2e codegen --check

require_relative 'test_helper'

Feature.enable(:knowledge_graph)

manifest = load_manifest!
m = manifest

grpc_endpoint = ENV.fetch('KNOWLEDGE_GRAPH_GRPC_ENDPOINT',
                          'gkg-webserver.default.svc.cluster.local:50054')
puts "  gRPC endpoint: #{grpc_endpoint}"
client = Ai::KnowledgeGraph::GrpcClient.new(endpoint: grpc_endpoint)
org_id = m[:organization_id]

# Load users from manifest
root = User.find_by!(username: m[:users][:root][:username])
franklyn = User.find_by!(username: m[:users][:franklyn][:username])
hanna = User.find_by!(username: m[:users][:hanna][:username])
lois = User.find_by!(username: m[:users][:lois][:username])
vickey = User.find_by!(username: m[:users][:vickey][:username])

# Variable resolution hash for $variable references
vars = {
  'total_projects' => m[:counts][:total_projects],
  'total_merge_requests' => m[:counts][:total_merge_requests],
  'total_work_items' => m[:counts][:total_work_items],
  'total_notes' => m[:counts][:total_notes],
  'proj.backend' => m[:projects][:backend][:id],
  'proj.frontend' => m[:projects][:frontend][:id],
  'proj.redaction' => m[:projects][:redaction][:id],
  'proj.smoke' => m[:projects][:smoke][:id],
  'group.gitlab_org' => m[:groups][:gitlab_org][:id],
  'group.backend' => m[:groups][:backend][:id],
  'group.frontend' => m[:groups][:frontend][:id],
  'group.redaction' => m[:groups][:redaction][:id],
  'group.toolbox' => m[:groups][:toolbox][:id],
  'group.smoke_tests' => m[:groups][:smoke_tests][:id],
  'user_counts.root.projects' => m[:counts][:per_user][:root][:projects],
  'user_counts.root.merge_requests' => m[:counts][:per_user][:root][:merge_requests],
  'user_counts.root.work_items' => m[:counts][:per_user][:root][:work_items],
  'user_counts.root.notes' => m[:counts][:per_user][:root][:notes],
  'user_counts.franklyn.projects' => m[:counts][:per_user][:franklyn][:projects],
  'user_counts.franklyn.merge_requests' => m[:counts][:per_user][:franklyn][:merge_requests],
  'user_counts.franklyn.work_items' => m[:counts][:per_user][:franklyn][:work_items],
  'user_counts.franklyn.notes' => m[:counts][:per_user][:franklyn][:notes],
  'user_counts.hanna.projects' => m[:counts][:per_user][:hanna][:projects],
  'user_counts.hanna.merge_requests' => m[:counts][:per_user][:hanna][:merge_requests],
  'user_counts.hanna.work_items' => m[:counts][:per_user][:hanna][:work_items],
  'user_counts.hanna.notes' => m[:counts][:per_user][:hanna][:notes],
  'user_counts.lois.projects' => m[:counts][:per_user][:lois][:projects],
  'user_counts.lois.merge_requests' => m[:counts][:per_user][:lois][:merge_requests],
  'user_counts.lois.work_items' => m[:counts][:per_user][:lois][:work_items],
  'user_counts.lois.notes' => m[:counts][:per_user][:lois][:notes],
  'user_counts.vickey.projects' => m[:counts][:per_user][:vickey][:projects],
  'user_counts.vickey.merge_requests' => m[:counts][:per_user][:vickey][:merge_requests],
  'user_counts.vickey.work_items' => m[:counts][:per_user][:vickey][:work_items],
  'user_counts.vickey.notes' => m[:counts][:per_user][:vickey][:notes],
  'project_counts.backend.merge_requests' => m[:counts][:per_project][:backend][:merge_requests],
  'project_counts.backend.work_items' => m[:counts][:per_project][:backend][:work_items],
  'project_counts.backend.milestones' => m[:counts][:per_project][:backend][:milestones],
  'project_counts.backend.labels' => m[:counts][:per_project][:backend][:labels],
  'project_counts.backend.notes' => m[:counts][:per_project][:backend][:notes],
  'project_counts.frontend.merge_requests' => m[:counts][:per_project][:frontend][:merge_requests],
  'project_counts.frontend.work_items' => m[:counts][:per_project][:frontend][:work_items],
  'project_counts.frontend.milestones' => m[:counts][:per_project][:frontend][:milestones],
  'project_counts.frontend.labels' => m[:counts][:per_project][:frontend][:labels],
  'project_counts.frontend.notes' => m[:counts][:per_project][:frontend][:notes],
  'project_counts.redaction.merge_requests' => m[:counts][:per_project][:redaction][:merge_requests],
  'project_counts.redaction.work_items' => m[:counts][:per_project][:redaction][:work_items],
  'project_counts.redaction.milestones' => m[:counts][:per_project][:redaction][:milestones],
  'project_counts.redaction.labels' => m[:counts][:per_project][:redaction][:labels],
  'project_counts.redaction.notes' => m[:counts][:per_project][:redaction][:notes],
  'project_counts.smoke.merge_requests' => m[:counts][:per_project][:smoke][:merge_requests],
  'project_counts.smoke.work_items' => m[:counts][:per_project][:smoke][:work_items],
  'project_counts.smoke.milestones' => m[:counts][:per_project][:smoke][:milestones],
  'project_counts.smoke.labels' => m[:counts][:per_project][:smoke][:labels],
  'project_counts.smoke.notes' => m[:counts][:per_project][:smoke][:notes],
}

def resolve(val, vars)
  return val unless val.is_a?(String) && val.start_with?('$')
  key = val[1..]
  resolved = vars[key]
  raise "Unknown variable: #{val} (available: #{vars.keys.sort.join(', ')})" if resolved.nil?
  resolved
end

def resolve_query(json_str, vars)
  json_str.gsub(/"\$([a-zA-Z0-9_.]+)"/) do
    key = $1
    resolved = vars[key]
    raise "Unknown variable in query: $#{key} (available: #{vars.keys.sort.join(', ')})" if resolved.nil?
    resolved.to_s
  end
end

# =============================================================================
TestHarness.section('1. Admin (root) -- sees all entities')

TestHarness.run("root: all projects", expected_min: resolve('$total_projects', vars), expected_max: resolve('$total_projects', vars)) do
  q(client, root, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"]},"limit":100}', vars)))
end

TestHarness.run("root: MRs in smoke project via traversal", expected_min: 1) do
  q(client, root, org_id, JSON.parse(resolve_query('{
  "query_type": "traversal",
  "nodes": [
    { "id": "p", "entity": "Project", "columns": ["name"], "node_ids": ["$proj.smoke"] },
    { "id": "mr", "entity": "MergeRequest", "columns": ["iid"] }
  ],
  "relationships": [{ "type": "IN_PROJECT", "from": "mr", "to": "p" }],
  "limit": 50
}', vars)))
end

TestHarness.run("root: private redaction project visible", expected_min: 1, expected_max: 1) do
  q(client, root, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"],"node_ids":["$proj.redaction"]},"limit":5}', vars)))
end

TestHarness.run("root: private group kg-redaction-test-group visible", expected_min: 1) do
  q(client, root, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"g","entity":"Group","columns":["name"],"filters":{"name":{"op":"eq","value":"kg-redaction-test-group"}}},"limit":5}', vars)))
end

# =============================================================================
TestHarness.section('2. lois -- visible projects: frontend, backend, redaction ($user_counts.lois.projects total)')

TestHarness.run("lois: #{resolve('$user_counts.lois.projects', vars)} projects", expected_min: resolve('$user_counts.lois.projects', vars), expected_max: resolve('$user_counts.lois.projects', vars)) do
  q(client, lois, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"]},"limit":100}', vars)))
end

TestHarness.run("lois: frontend project visible", expected_min: 1, expected_max: 1) do
  q(client, lois, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"],"node_ids":["$proj.frontend"]},"limit":5}', vars)))
end

TestHarness.run("lois: backend project visible", expected_min: 1, expected_max: 1) do
  q(client, lois, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"],"node_ids":["$proj.backend"]},"limit":5}', vars)))
end

TestHarness.run("lois: private redaction project visible (member via group)", expected_min: 1, expected_max: 1) do
  q(client, lois, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"],"node_ids":["$proj.redaction"]},"limit":5}', vars)))
end

TestHarness.run("lois: smoke project NOT visible (not in toolbox group)", expected_min: 0, expected_max: 0) do
  q(client, lois, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"],"node_ids":["$proj.smoke"]},"limit":5}', vars)))
end

TestHarness.run("lois: #{resolve('$user_counts.lois.merge_requests', vars)} MRs", expected_min: resolve('$user_counts.lois.merge_requests', vars), expected_max: resolve('$user_counts.lois.merge_requests', vars)) do
  q(client, lois, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"mr","entity":"MergeRequest","columns":["iid"]},"limit":200}', vars)))
end

TestHarness.run("lois: #{resolve('$user_counts.lois.notes', vars)} notes", expected_min: resolve('$user_counts.lois.notes', vars), expected_max: resolve('$user_counts.lois.notes', vars)) do
  q(client, lois, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"n","entity":"Note","columns":["id"]},"limit":200}', vars)))
end

TestHarness.run("lois: #{resolve('$user_counts.lois.work_items', vars)} work items", expected_min: resolve('$user_counts.lois.work_items', vars), expected_max: resolve('$user_counts.lois.work_items', vars)) do
  q(client, lois, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"wi","entity":"WorkItem","columns":["title"]},"limit":200}', vars)))
end

TestHarness.run("lois: private group kg-redaction-test-group visible", expected_min: 1) do
  q(client, lois, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"g","entity":"Group","columns":["name"],"filters":{"name":{"op":"eq","value":"kg-redaction-test-group"}}},"limit":5}', vars)))
end

# =============================================================================
TestHarness.section('3. franklyn -- visible projects: smoke ($user_counts.franklyn.projects total)')

TestHarness.run("franklyn: #{resolve('$user_counts.franklyn.projects', vars)} project", expected_min: resolve('$user_counts.franklyn.projects', vars), expected_max: resolve('$user_counts.franklyn.projects', vars)) do
  q(client, franklyn, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"]},"limit":100}', vars)))
end

TestHarness.run("franklyn: smoke project visible", expected_min: 1, expected_max: 1) do
  q(client, franklyn, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"],"node_ids":["$proj.smoke"]},"limit":5}', vars)))
end

TestHarness.run("franklyn: frontend project NOT visible", expected_min: 0, expected_max: 0) do
  q(client, franklyn, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"],"node_ids":["$proj.frontend"]},"limit":5}', vars)))
end

TestHarness.run("franklyn: redaction project NOT visible", expected_min: 0, expected_max: 0) do
  q(client, franklyn, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"],"node_ids":["$proj.redaction"]},"limit":5}', vars)))
end

TestHarness.run("franklyn: #{resolve('$user_counts.franklyn.merge_requests', vars)} MRs", expected_min: resolve('$user_counts.franklyn.merge_requests', vars), expected_max: resolve('$user_counts.franklyn.merge_requests', vars)) do
  q(client, franklyn, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"mr","entity":"MergeRequest","columns":["iid"]},"limit":200}', vars)))
end

TestHarness.run("franklyn: #{resolve('$user_counts.franklyn.notes', vars)} notes", expected_min: resolve('$user_counts.franklyn.notes', vars), expected_max: resolve('$user_counts.franklyn.notes', vars)) do
  q(client, franklyn, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"n","entity":"Note","columns":["id"]},"limit":200}', vars)))
end

TestHarness.run("franklyn: #{resolve('$user_counts.franklyn.work_items', vars)} work items", expected_min: resolve('$user_counts.franklyn.work_items', vars), expected_max: resolve('$user_counts.franklyn.work_items', vars)) do
  q(client, franklyn, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"wi","entity":"WorkItem","columns":["title"]},"limit":200}', vars)))
end

TestHarness.run("franklyn: private group kg-redaction-test-group NOT visible", expected_min: 0, expected_max: 0) do
  q(client, franklyn, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"g","entity":"Group","columns":["name"],"filters":{"name":{"op":"eq","value":"kg-redaction-test-group"}}},"limit":5}', vars)))
end

TestHarness.run("franklyn: private redaction project NOT visible", expected_min: 0, expected_max: 0) do
  q(client, franklyn, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"],"node_ids":["$proj.redaction"]},"limit":5}', vars)))
end

# =============================================================================
TestHarness.section('4. vickey & hanna -- no group memberships -> zero results')

TestHarness.run("vickey.schmidt: 0 projects", expected_min: 0, expected_max: 0) do
  q(client, vickey, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"]},"limit":100}', vars)))
end

TestHarness.run("vickey.schmidt: 0 MRs", expected_min: 0, expected_max: 0) do
  q(client, vickey, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"mr","entity":"MergeRequest","columns":["iid"]},"limit":100}', vars)))
end

TestHarness.run("vickey.schmidt: 0 notes", expected_min: 0, expected_max: 0) do
  q(client, vickey, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"n","entity":"Note","columns":["id"]},"limit":100}', vars)))
end

TestHarness.run("vickey.schmidt: 0 work items", expected_min: 0, expected_max: 0) do
  q(client, vickey, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"wi","entity":"WorkItem","columns":["title"]},"limit":100}', vars)))
end

TestHarness.run("vickey.schmidt: private group NOT visible", expected_min: 0, expected_max: 0) do
  q(client, vickey, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"g","entity":"Group","columns":["name"],"filters":{"name":{"op":"eq","value":"kg-redaction-test-group"}}},"limit":5}', vars)))
end

TestHarness.run("vickey.schmidt: private redaction project NOT visible", expected_min: 0, expected_max: 0) do
  q(client, vickey, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"],"node_ids":["$proj.redaction"]},"limit":5}', vars)))
end

# =============================================================================
TestHarness.section('4. vickey & hanna -- no group memberships -> zero results')

TestHarness.run("hanna: 0 projects", expected_min: 0, expected_max: 0) do
  q(client, hanna, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"]},"limit":100}', vars)))
end

TestHarness.run("hanna: 0 MRs", expected_min: 0, expected_max: 0) do
  q(client, hanna, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"mr","entity":"MergeRequest","columns":["iid"]},"limit":100}', vars)))
end

TestHarness.run("hanna: 0 notes", expected_min: 0, expected_max: 0) do
  q(client, hanna, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"n","entity":"Note","columns":["id"]},"limit":100}', vars)))
end

TestHarness.run("hanna: 0 work items", expected_min: 0, expected_max: 0) do
  q(client, hanna, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"wi","entity":"WorkItem","columns":["title"]},"limit":100}', vars)))
end

TestHarness.run("hanna: private group NOT visible", expected_min: 0, expected_max: 0) do
  q(client, hanna, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"g","entity":"Group","columns":["name"],"filters":{"name":{"op":"eq","value":"kg-redaction-test-group"}}},"limit":5}', vars)))
end

TestHarness.run("hanna: private redaction project NOT visible", expected_min: 0, expected_max: 0) do
  q(client, hanna, org_id, JSON.parse(resolve_query('{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"],"node_ids":["$proj.redaction"]},"limit":5}', vars)))
end

# =============================================================================
TestHarness.section('5. Cross-user isolation')

TestHarness.run("lois cannot see smoke project MRs (not in toolbox group)", expected_min: 0, expected_max: 0) do
  q(client, lois, org_id, JSON.parse(resolve_query('{
  "query_type": "traversal",
  "nodes": [
    { "id": "p", "entity": "Project", "columns": ["name"], "node_ids": ["$proj.smoke"] },
    { "id": "mr", "entity": "MergeRequest", "columns": ["iid"] }
  ],
  "relationships": [{ "type": "IN_PROJECT", "from": "mr", "to": "p" }],
  "limit": 50
}', vars)))
end

TestHarness.run("franklyn cannot see frontend project MRs (not in gitlab-org group)", expected_min: 0, expected_max: 0) do
  q(client, franklyn, org_id, JSON.parse(resolve_query('{
  "query_type": "traversal",
  "nodes": [
    { "id": "p", "entity": "Project", "columns": ["name"], "node_ids": ["$proj.frontend"] },
    { "id": "mr", "entity": "MergeRequest", "columns": ["iid"] }
  ],
  "relationships": [{ "type": "IN_PROJECT", "from": "mr", "to": "p" }],
  "limit": 50
}', vars)))
end

TestHarness.run("lois sees MRs in frontend project (>=#{resolve('$project_counts.frontend.merge_requests', vars)})", expected_min: resolve('$project_counts.frontend.merge_requests', vars)) do
  q(client, lois, org_id, JSON.parse(resolve_query('{
  "query_type": "traversal",
  "nodes": [
    { "id": "p", "entity": "Project", "columns": ["name"], "node_ids": ["$proj.frontend"] },
    { "id": "mr", "entity": "MergeRequest", "columns": ["iid"] }
  ],
  "relationships": [{ "type": "IN_PROJECT", "from": "mr", "to": "p" }],
  "limit": 50
}', vars)))
end

TestHarness.run("franklyn sees MRs in smoke project (>=#{resolve('$project_counts.smoke.merge_requests', vars)})", expected_min: resolve('$project_counts.smoke.merge_requests', vars)) do
  q(client, franklyn, org_id, JSON.parse(resolve_query('{
  "query_type": "traversal",
  "nodes": [
    { "id": "p", "entity": "Project", "columns": ["name"], "node_ids": ["$proj.smoke"] },
    { "id": "mr", "entity": "MergeRequest", "columns": ["iid"] }
  ],
  "relationships": [{ "type": "IN_PROJECT", "from": "mr", "to": "p" }],
  "limit": 50
}', vars)))
end

TestHarness.summary
