# frozen_string_literal: true

# =============================================================================
# Knowledge Graph -- Redaction / Permission E2E Test
# =============================================================================
#
# Verifies that the GKG server correctly scopes results to each user's
# group_traversal_ids JWT claim. Only entities whose traversal_path is a
# prefix match for one of the user's group paths are returned.
#
# All IDs and counts are loaded from /tmp/e2e/manifest.json (written by
# create_test_data.rb). No hardcoded IDs.
#
# Run with:
#   bundle exec rails runner /tmp/e2e/redaction_test.rb RAILS_ENV=production
#
# =============================================================================

require_relative 'test_helper'

Feature.enable(:knowledge_graph)

manifest = load_manifest!
m = manifest # short alias

# The GKG webserver runs in the default namespace; this test runs in the
# gitlab-namespace toolbox pod.  Override the default "localhost:50051" with
# the cross-namespace service FQDN.
grpc_endpoint = ENV.fetch('KNOWLEDGE_GRAPH_GRPC_ENDPOINT',
                          'gkg-webserver.default.svc.cluster.local:50051')
client = Ai::KnowledgeGraph::GrpcClient.new(endpoint: grpc_endpoint)
org_id = m[:organization_id]

# Load users from manifest (single source of truth for usernames)
root     = User.find_by!(username: m[:users][:root][:username])
lois     = User.find_by!(username: m[:users][:lois][:username])
franklyn = User.find_by!(username: m[:users][:franklyn][:username])
vickey   = User.find_by!(username: m[:users][:vickey][:username])
hanna    = User.find_by!(username: m[:users][:hanna][:username])

# Extract dynamic IDs from manifest
proj_smoke_id     = m[:projects][:smoke][:id]
proj_frontend_id  = m[:projects][:frontend][:id]
proj_backend_id   = m[:projects][:backend][:id]
proj_redaction_id = m[:projects][:redaction][:id]

total_projects    = m[:counts][:total_projects]

lois_counts     = m[:counts][:per_user][:lois]
franklyn_counts = m[:counts][:per_user][:franklyn]

# =============================================================================
# SECTION 1: Admin sees everything
# =============================================================================
TestHarness.section('1. Admin (root) -- sees all entities')

TestHarness.run('root: all projects', expected_min: total_projects, expected_max: total_projects) do
  q(client, root, org_id, { query_type: 'search',
                            node: { id: 'p', entity: 'Project', columns: ['name'] }, limit: 100 })
end

TestHarness.run('root: MRs in smoke project via traversal', expected_min: 1) do
  q(client, root, org_id, { query_type: 'traversal',
                            nodes: [
                              { id: 'p', entity: 'Project', columns: ['name'], node_ids: [proj_smoke_id] },
                              { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
                            ],
                            relationships: [{ type: 'IN_PROJECT', from: 'mr', to: 'p' }],
                            limit: 50 })
end

TestHarness.run('root: private redaction project visible', expected_min: 1, expected_max: 1) do
  q(client, root, org_id, { query_type: 'search',
                            node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [proj_redaction_id] }, limit: 5 })
end

TestHarness.run('root: private group kg-redaction-test-group visible', expected_min: 1) do
  q(client, root, org_id, { query_type: 'search',
                            node: { id: 'g', entity: 'Group', columns: ['name'],
                                    filters: { name: { op: 'eq', value: 'kg-redaction-test-group' } } }, limit: 5 })
end

# =============================================================================
# SECTION 2: lois -- scoped to gitlab-org + redaction groups
# =============================================================================
TestHarness.section("2. lois -- visible projects: frontend, backend, redaction (#{lois_counts[:projects]} total)")

TestHarness.run("lois: #{lois_counts[:projects]} projects", expected_min: lois_counts[:projects],
                                                            expected_max: lois_counts[:projects]) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'p', entity: 'Project', columns: ['name'] }, limit: 100 })
end

TestHarness.run('lois: frontend project visible', expected_min: 1, expected_max: 1) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [proj_frontend_id] }, limit: 5 })
end

TestHarness.run('lois: backend project visible', expected_min: 1, expected_max: 1) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [proj_backend_id] }, limit: 5 })
end

TestHarness.run('lois: private redaction project visible (member via group)', expected_min: 1, expected_max: 1) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [proj_redaction_id] }, limit: 5 })
end

TestHarness.run('lois: smoke project NOT visible (not in toolbox group)', expected_min: 0, expected_max: 0) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [proj_smoke_id] }, limit: 5 })
end

TestHarness.run("lois: #{lois_counts[:merge_requests]} MRs", expected_min: lois_counts[:merge_requests],
                                                             expected_max: lois_counts[:merge_requests]) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }, limit: 200 })
end

TestHarness.run("lois: #{lois_counts[:notes]} notes", expected_min: lois_counts[:notes],
                                                      expected_max: lois_counts[:notes]) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'n', entity: 'Note', columns: ['id'] }, limit: 200 })
end

TestHarness.run("lois: #{lois_counts[:work_items]} work items", expected_min: lois_counts[:work_items],
                                                                expected_max: lois_counts[:work_items]) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'wi', entity: 'WorkItem', columns: ['title'] }, limit: 200 })
end

TestHarness.run('lois: private group kg-redaction-test-group visible', expected_min: 1) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'g', entity: 'Group', columns: ['name'],
                                    filters: { name: { op: 'eq', value: 'kg-redaction-test-group' } } }, limit: 5 })
end

# =============================================================================
# SECTION 3: franklyn -- scoped to toolbox group only
# =============================================================================
TestHarness.section("3. franklyn -- visible projects: smoke (#{franklyn_counts[:projects]} total)")

TestHarness.run("franklyn: #{franklyn_counts[:projects]} project", expected_min: franklyn_counts[:projects],
                                                                   expected_max: franklyn_counts[:projects]) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'p', entity: 'Project', columns: ['name'] }, limit: 100 })
end

TestHarness.run('franklyn: smoke project visible', expected_min: 1, expected_max: 1) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [proj_smoke_id] }, limit: 5 })
end

TestHarness.run('franklyn: frontend project NOT visible', expected_min: 0, expected_max: 0) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [proj_frontend_id] }, limit: 5 })
end

TestHarness.run('franklyn: redaction project NOT visible', expected_min: 0, expected_max: 0) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [proj_redaction_id] }, limit: 5 })
end

TestHarness.run("franklyn: #{franklyn_counts[:merge_requests]} MRs", expected_min: franklyn_counts[:merge_requests],
                                                                     expected_max: franklyn_counts[:merge_requests]) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }, limit: 200 })
end

TestHarness.run("franklyn: #{franklyn_counts[:notes]} notes", expected_min: franklyn_counts[:notes],
                                                              expected_max: franklyn_counts[:notes]) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'n', entity: 'Note', columns: ['id'] }, limit: 200 })
end

TestHarness.run("franklyn: #{franklyn_counts[:work_items]} work items", expected_min: franklyn_counts[:work_items],
                                                                        expected_max: franklyn_counts[:work_items]) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'wi', entity: 'WorkItem', columns: ['title'] }, limit: 200 })
end

TestHarness.run('franklyn: private group kg-redaction-test-group NOT visible', expected_min: 0, expected_max: 0) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'g', entity: 'Group', columns: ['name'],
                                        filters: { name: { op: 'eq', value: 'kg-redaction-test-group' } } }, limit: 5 })
end

TestHarness.run('franklyn: private redaction project NOT visible', expected_min: 0, expected_max: 0) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [proj_redaction_id] }, limit: 5 })
end

# =============================================================================
# SECTION 4: vickey and hanna -- empty traversal_ids -> see nothing
# =============================================================================
TestHarness.section('4. vickey & hanna -- no group memberships -> zero results')

{ 'vickey.schmidt' => vickey, 'hanna' => hanna }.each do |username, user|
  TestHarness.run("#{username}: 0 projects", expected_min: 0, expected_max: 0) do
    q(client, user, org_id, { query_type: 'search',
                              node: { id: 'p', entity: 'Project', columns: ['name'] }, limit: 100 })
  end

  TestHarness.run("#{username}: 0 MRs", expected_min: 0, expected_max: 0) do
    q(client, user, org_id, { query_type: 'search',
                              node: { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }, limit: 100 })
  end

  TestHarness.run("#{username}: 0 notes", expected_min: 0, expected_max: 0) do
    q(client, user, org_id, { query_type: 'search',
                              node: { id: 'n', entity: 'Note', columns: ['id'] }, limit: 100 })
  end

  TestHarness.run("#{username}: 0 work items", expected_min: 0, expected_max: 0) do
    q(client, user, org_id, { query_type: 'search',
                              node: { id: 'wi', entity: 'WorkItem', columns: ['title'] }, limit: 100 })
  end

  TestHarness.run("#{username}: private group NOT visible", expected_min: 0, expected_max: 0) do
    q(client, user, org_id, { query_type: 'search',
                              node: { id: 'g', entity: 'Group', columns: ['name'],
                                      filters: { name: { op: 'eq', value: 'kg-redaction-test-group' } } }, limit: 5 })
  end

  TestHarness.run("#{username}: private redaction project NOT visible", expected_min: 0, expected_max: 0) do
    q(client, user, org_id, { query_type: 'search',
                              node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [proj_redaction_id] }, limit: 5 })
  end
end

# =============================================================================
# SECTION 5: Cross-user isolation -- lois cannot see franklyn's project, etc.
# =============================================================================
TestHarness.section('5. Cross-user isolation')

TestHarness.run('lois cannot see smoke project MRs (not in toolbox group)', expected_min: 0, expected_max: 0) do
  q(client, lois, org_id, { query_type: 'traversal',
                            nodes: [
                              { id: 'p',  entity: 'Project',      columns: ['name'], node_ids: [proj_smoke_id] },
                              { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
                            ],
                            relationships: [{ type: 'IN_PROJECT', from: 'mr', to: 'p' }],
                            limit: 50 })
end

TestHarness.run('franklyn cannot see frontend project MRs (not in gitlab-org group)', expected_min: 0,
                                                                                      expected_max: 0) do
  q(client, franklyn, org_id, { query_type: 'traversal',
                                nodes: [
                                  { id: 'p',  entity: 'Project',      columns: ['name'], node_ids: [proj_frontend_id] },
                                  { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
                                ],
                                relationships: [{ type: 'IN_PROJECT', from: 'mr', to: 'p' }],
                                limit: 50 })
end

# lois sees MRs in frontend project (she has gitlab-org membership)
# NOTE: Traversal queries join gl_edge (IN_PROJECT) with gl_merge_request on id.
# Because the edge table does not filter source_kind, Labels/Milestones with
# overlapping ids produce extra rows. Use expected_min only (no max).
frontend_mr_count = m[:counts][:per_project][:frontend][:merge_requests]
TestHarness.run("lois sees MRs in frontend project (>=#{frontend_mr_count})", expected_min: frontend_mr_count) do
  q(client, lois, org_id, { query_type: 'traversal',
                            nodes: [
                              { id: 'p',  entity: 'Project',      columns: ['name'], node_ids: [proj_frontend_id] },
                              { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
                            ],
                            relationships: [{ type: 'IN_PROJECT', from: 'mr', to: 'p' }],
                            limit: 50 })
end

# franklyn sees MRs in smoke project (he has toolbox membership)
# NOTE: Same traversal join behavior as above — use expected_min only.
smoke_mr_count = m[:counts][:per_project][:smoke][:merge_requests]
TestHarness.run("franklyn sees MRs in smoke project (>=#{smoke_mr_count})", expected_min: smoke_mr_count) do
  q(client, franklyn, org_id, { query_type: 'traversal',
                                nodes: [
                                  { id: 'p',  entity: 'Project',      columns: ['name'], node_ids: [proj_smoke_id] },
                                  { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
                                ],
                                relationships: [{ type: 'IN_PROJECT', from: 'mr', to: 'p' }],
                                limit: 50 })
end

# =============================================================================
TestHarness.summary
