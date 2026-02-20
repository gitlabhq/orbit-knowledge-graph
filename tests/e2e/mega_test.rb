# frozen_string_literal: true

# =============================================================================
# Knowledge Graph MEGA Test — tailored to this GDK instance
# =============================================================================
#
# This is the authoritative test file for this GDK instance. All assertions
# are grounded in actual ClickHouse data, not assumed fixtures.
#
# Run with:
#   cd ~/Desktop/Code/gdk/gitlab
#   bundle exec rails runner \
#     ~/Desktop/Code/gkg/tests/e2e/mega_test.rb
#
# =============================================================================
# DATA INVENTORY (verified in ClickHouse gl_* tables)
# =============================================================================
#
# gl_user:          73   (root + 4 test users + seeded users)
# gl_project:        4   (ids 1, 2, 3, 19)
# gl_group:         96   (includes kg-redaction-test-group id=99)
# gl_merge_request: 188  (FINAL — merged=176, opened=12)
# gl_work_item:    575
# gl_label:        111   (group-scoped labels, traversal 1/XX/)
# gl_milestone:     83   (project-scoped: 9 each in projs 1/2/3)
# gl_note:         148   (non-system, valid traversal_path)
# gl_pipeline:       0   (no CI runs in this GDK)
# gl_edge:        1869   (AUTHORED 817, IN_GROUP 645, IN_PROJECT 218, …)
#
# Projects (gl_project):
#   id=1   toolbox/gitlab-smoke-tests           public    traversal: 1/22/23/
#   id=2   gitlab-org/gitlab-test               public    traversal: 1/24/25/
#   id=3   gitlab-org/gitlab-shell              public    traversal: 1/24/26/
#   id=19  kg-redaction-test-group/…-project    private   traversal: 1/99/100/
#
# visibility_level integers: 0=private, 10=internal, 20=public
#
# Test users and memberships:
#   root (id=1)          admin — sees everything
#   lois (id=70)         developer on projects 2 & 3, developer in group 99
#   franklyn (id=72)     maintainer on project 1
#   vickey (id=71)       guest on project 2, reporter on project 3
#   hanna (id=73)        guest on project 2
#
# Per-user visible MRs (traversal-path scoped):
#   root:     188  (all)
#   lois:      13  (1/24/25/ + 1/24/26/ = 9+4)
#   franklyn:   8  (1/22/23/)
#   vickey:     0  (guest/reporter ability check blocks MR reads)
#   hanna:      0  (guest)
#
# Per-user visible notes:
#   root:     148
#   lois:      96  (48 each in proj 2 and 3)
#   franklyn:  52  (proj 1)
#   vickey:     0
#   hanna:      0
#
# Per-user visible work_items:
#   lois:      81  (proj 2+3)
#   franklyn:  38  (proj 1)
#
# =============================================================================
# KNOWN GAPS (no data, tests marked expected_max: 0)
# =============================================================================
#   Pipelines, Stages, Jobs    — no CI runs
#   Vulnerabilities, Findings  — no security scans
#
# =============================================================================

Feature.enable(:knowledge_graph)

module MegaTest
  PASS = []
  FAIL = []

  def self.run_test(name, expected_min:, expected_max: nil, &block)
    result = block.call
    rows   = result[:result].is_a?(Array) ? result[:result] : []
    count  = rows.size
    ok     = count >= expected_min && (expected_max.nil? || count <= expected_max)
    range  = expected_max ? "#{expected_min}–#{expected_max}" : "≥#{expected_min}"
    if ok
      puts "  ✓  #{name} (#{count})"
      PASS << name
    else
      puts "  ✗  #{name} — got #{count}, expected #{range}"
      FAIL << name
    end
  rescue StandardError => e
    puts "  ✗  #{name} — ERROR: #{e.message[0..120]}"
    FAIL << name
  end

  def self.section(title)
    puts "\n#{'─' * 70}"
    puts "  #{title}"
    puts '─' * 70
  end

  def self.summary
    total = PASS.size + FAIL.size
    puts "\n#{'═' * 70}"
    puts "  RESULT: #{PASS.size}/#{total} passed"
    puts '═' * 70
    if FAIL.any?
      puts "\n  FAILED:"
      FAIL.each { |f| puts "    • #{f}" }
    end
    puts
  end
end

client = Ai::KnowledgeGraph::GrpcClient.new
org_id = Organizations::Organization.default_organization&.id || 1

root     = User.find_by!(username: 'root')
lois     = User.find_by!(username: 'lois')
vickey   = User.find_by!(username: 'vickey.schmidt')
franklyn = User.find_by!(username: 'franklyn.mcdermott')
hanna    = User.find_by!(username: 'hanna')

def q(client, user, org_id, query_json)
  client.execute_query(query_json: query_json, user: user, organization_id: org_id)
end

puts "\n#{'═' * 70}"
puts '  Knowledge Graph MEGA Test Suite'
puts '═' * 70

# =============================================================================
# 1. ENTITY COUNTS — admin sees everything
# =============================================================================
MegaTest.section('1. ENTITY COUNTS (admin)')

MegaTest.run_test('Projects — 4 total', expected_min: 4, expected_max: 4) do
  q(client, root, org_id, { query_type: 'search', node: { id: 'p', entity: 'Project', columns: ['name'] }, limit: 100 })
end

MegaTest.run_test('Users — at least 73', expected_min: 73) do
  q(client, root, org_id,
    { query_type: 'search', node: { id: 'u', entity: 'User', columns: ['username'] }, limit: 100 })
end

MegaTest.run_test('Groups — at least 90', expected_min: 90) do
  q(client, root, org_id, { query_type: 'search', node: { id: 'g', entity: 'Group', columns: ['name'] }, limit: 100 })
end

MegaTest.run_test('MergeRequests — at least 188', expected_min: 188) do
  q(client, root, org_id,
    { query_type: 'search', node: { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }, limit: 200 })
end

MegaTest.run_test('WorkItems — at least 500', expected_min: 500) do
  q(client, root, org_id,
    { query_type: 'search', node: { id: 'wi', entity: 'WorkItem', columns: ['title'] }, limit: 600 })
end

MegaTest.run_test('Labels — at least 100', expected_min: 100) do
  q(client, root, org_id, { query_type: 'search', node: { id: 'l', entity: 'Label', columns: ['title'] }, limit: 200 })
end

MegaTest.run_test('Milestones — at least 80', expected_min: 80) do
  q(client, root, org_id,
    { query_type: 'search', node: { id: 'm', entity: 'Milestone', columns: ['title'] }, limit: 100 })
end

MegaTest.run_test('Notes — at least 140', expected_min: 140) do
  q(client, root, org_id, { query_type: 'search', node: { id: 'n', entity: 'Note', columns: ['id'] }, limit: 200 })
end

MegaTest.run_test('Pipelines — 0 (no CI)', expected_min: 0, expected_max: 0) do
  q(client, root, org_id, { query_type: 'search', node: { id: 'pl', entity: 'Pipeline', columns: ['iid'] }, limit: 20 })
end

MegaTest.run_test('Vulnerabilities — 0 (no security data)', expected_min: 0, expected_max: 0) do
  q(client, root, org_id,
    { query_type: 'search', node: { id: 'v', entity: 'Vulnerability', columns: ['id'] }, limit: 20 })
end

# =============================================================================
# 2. ID SELECTORS
# =============================================================================
MegaTest.section('2. ID SELECTORS')

MegaTest.run_test('Project by node_ids=[1]', expected_min: 1, expected_max: 1) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'p', entity: 'Project', columns: %w[name full_path], node_ids: [1] },
      limit: 5
    })
end

MegaTest.run_test('Project by node_ids=[2]', expected_min: 1, expected_max: 1) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'p', entity: 'Project', columns: %w[name full_path], node_ids: [2] },
      limit: 5
    })
end

MegaTest.run_test('Multiple projects node_ids=[1,2,3] — exactly 3', expected_min: 3, expected_max: 3) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [1, 2, 3] },
      limit: 10
    })
end

MegaTest.run_test('Projects id_range 1–100 — all 4', expected_min: 4, expected_max: 4) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'p', entity: 'Project', columns: ['name'], id_range: { start: 1, end: 100 } },
      limit: 100
    })
end

MegaTest.run_test('MRs id_range 1–20 — at least 10', expected_min: 10) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'mr', entity: 'MergeRequest', columns: ['iid'], id_range: { start: 1, end: 20 } },
      limit: 50
    })
end

MegaTest.run_test('Private project id=19 — lois (member) sees it', expected_min: 1, expected_max: 1) do
  q(client, lois, org_id, {
      query_type: 'search',
      node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [19] },
      limit: 5
    })
end

MegaTest.run_test('Private project id=19 — vickey (non-member) cannot see it', expected_min: 0, expected_max: 0) do
  q(client, vickey, org_id, {
      query_type: 'search',
      node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [19] },
      limit: 5
    })
end

# =============================================================================
# 3. FILTERS
# Correct shape: filters is a Hash of { column_name => { op:, value: } }
# visibility_level is an integer: 20=public, 10=internal, 0=private
# =============================================================================
MegaTest.section('3. FILTERS')

MegaTest.run_test('MRs state=merged — at least 170', expected_min: 170) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'mr', entity: 'MergeRequest', columns: %w[iid state],
              filters: { state: { op: 'eq', value: 'merged' } } },
      limit: 200
    })
end

MegaTest.run_test('MRs state=opened — at least 10', expected_min: 10) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'mr', entity: 'MergeRequest', columns: %w[iid state],
              filters: { state: { op: 'eq', value: 'opened' } } },
      limit: 50
    })
end

MegaTest.run_test('MRs state IN [merged, opened] — at least 180', expected_min: 180) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'mr', entity: 'MergeRequest', columns: %w[iid state],
              filters: { state: { op: 'in', value: %w[merged opened] } } },
      limit: 200
    })
end

MegaTest.run_test('Projects visibility_level=20 (public) — 3 projects', expected_min: 3, expected_max: 3) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'p', entity: 'Project', columns: %w[name visibility_level],
              filters: { visibility_level: { op: 'eq', value: 20 } } },
      limit: 20
    })
end

MegaTest.run_test('Projects visibility_level=0 (private) — 1 project', expected_min: 1, expected_max: 1) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'p', entity: 'Project', columns: %w[name visibility_level],
              filters: { visibility_level: { op: 'eq', value: 0 } } },
      limit: 20
    })
end

MegaTest.run_test('MRs draft=false — at least 100', expected_min: 100) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'mr', entity: 'MergeRequest', columns: %w[iid draft],
              filters: { draft: { op: 'eq', value: false } } },
      limit: 200
    })
end

MegaTest.run_test('WorkItems state=opened — at least 100', expected_min: 100) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'wi', entity: 'WorkItem', columns: %w[title state],
              filters: { state: { op: 'eq', value: 'opened' } } },
      limit: 600
    })
end

MegaTest.run_test('Milestones state=active — at least 1', expected_min: 1) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'm', entity: 'Milestone', columns: %w[title state],
              filters: { state: { op: 'eq', value: 'active' } } },
      limit: 100
    })
end

MegaTest.run_test('Pipeline status=failed — 0 (no CI)', expected_min: 0, expected_max: 0) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'pl', entity: 'Pipeline', columns: ['iid'],
              filters: { status: { op: 'eq', value: 'failed' } } },
      limit: 20
    })
end

MegaTest.run_test('Pipeline iid<=100 — 0 (no CI)', expected_min: 0, expected_max: 0) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'pl', entity: 'Pipeline', columns: ['iid'],
              filters: { iid: { op: 'lte', value: 100 } } },
      limit: 20
    })
end

# =============================================================================
# 4. AGGREGATIONS
# group_by: 'node_alias' groups by that node
# group_by_column is used for single-node column aggregation (in the agg hash)
# =============================================================================
MegaTest.section('4. AGGREGATIONS')

MegaTest.run_test('MR count by state — at least 2 states', expected_min: 2) do
  q(client, root, org_id, {
      query_type: 'aggregation',
      nodes: [{ id: 'mr', entity: 'MergeRequest', columns: ['state'] }],
      aggregations: [{ function: 'count', target: 'mr', group_by_column: 'state', alias: 'cnt' }],
      limit: 10
    })
end

MegaTest.run_test('MR count by project (IN_PROJECT) — at least 5 projects', expected_min: 5) do
  q(client, root, org_id, {
      query_type: 'aggregation',
      nodes: [
        { id: 'p',  entity: 'Project',      columns: ['name'] },
        { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
      ],
      relationships: [{ type: 'IN_PROJECT', from: 'mr', to: 'p' }],
      aggregations: [{ function: 'count', target: 'mr', group_by: 'p', alias: 'mr_count' }],
      aggregation_sort: { agg_index: 0, direction: 'DESC' },
      limit: 20
    })
end

MegaTest.run_test('User MR count (AUTHORED) — at least 5 users', expected_min: 5) do
  q(client, root, org_id, {
      query_type: 'aggregation',
      nodes: [
        { id: 'u',  entity: 'User',         columns: ['username'] },
        { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
      ],
      relationships: [{ type: 'AUTHORED', from: 'u', to: 'mr' }],
      aggregations: [{ function: 'count', target: 'mr', group_by: 'u', alias: 'mr_count' }],
      aggregation_sort: { agg_index: 0, direction: 'DESC' },
      limit: 10
    })
end

MegaTest.run_test('WorkItem count by project — at least 5', expected_min: 5) do
  q(client, root, org_id, {
      query_type: 'aggregation',
      nodes: [
        { id: 'p',  entity: 'Project',  columns: ['name'] },
        { id: 'wi', entity: 'WorkItem', columns: ['title'] }
      ],
      relationships: [{ type: 'IN_PROJECT', from: 'wi', to: 'p' }],
      aggregations: [{ function: 'count', target: 'wi', group_by: 'p', alias: 'wi_count' }],
      aggregation_sort: { agg_index: 0, direction: 'DESC' },
      limit: 20
    })
end

MegaTest.run_test('Vuln count by severity — 1 row (all zero)', expected_min: 1) do
  q(client, root, org_id, {
      query_type: 'aggregation',
      nodes: [{ id: 'v', entity: 'Vulnerability', columns: ['severity'] }],
      aggregations: [{ function: 'count', target: 'v', group_by_column: 'severity', alias: 'cnt' }],
      limit: 10
    })
end

MegaTest.run_test('Pipeline count by project — 0 (no CI)', expected_min: 0, expected_max: 0) do
  q(client, root, org_id, {
      query_type: 'aggregation',
      nodes: [
        { id: 'p',  entity: 'Project',  columns: ['name'] },
        { id: 'pl', entity: 'Pipeline', columns: ['iid'] }
      ],
      relationships: [{ type: 'IN_PROJECT', from: 'pl', to: 'p' }],
      aggregations: [{ function: 'count', target: 'pl', group_by: 'p', alias: 'count' }],
      limit: 10
    })
end

# =============================================================================
# 5. TRAVERSAL
# =============================================================================
MegaTest.section('5. TRAVERSAL')

MegaTest.run_test('User(1) → MRs via AUTHORED — at least 5', expected_min: 5) do
  q(client, root, org_id, {
      query_type: 'traversal',
      nodes: [
        { id: 'u',  entity: 'User',         columns: ['username'], node_ids: [1] },
        { id: 'mr', entity: 'MergeRequest', columns: %w[iid state] }
      ],
      relationships: [{ type: 'AUTHORED', from: 'u', to: 'mr' }],
      limit: 50
    })
end

MegaTest.run_test('User(1) → MRs → Project — at least 1', expected_min: 1) do
  q(client, root, org_id, {
      query_type: 'traversal',
      nodes: [
        { id: 'u',  entity: 'User',         columns: ['username'], node_ids: [1] },
        { id: 'mr', entity: 'MergeRequest', columns: ['iid'] },
        { id: 'p',  entity: 'Project',      columns: ['name'] }
      ],
      relationships: [
        { type: 'AUTHORED', from: 'u', to: 'mr' },
        { type: 'IN_PROJECT', from: 'mr', to: 'p' }
      ],
      limit: 20
    })
end

MegaTest.run_test('MR → Project (IN_PROJECT) — at least 10', expected_min: 10) do
  q(client, root, org_id, {
      query_type: 'traversal',
      nodes: [
        { id: 'mr', entity: 'MergeRequest', columns: ['iid'] },
        { id: 'p', entity: 'Project', columns: ['name'] }
      ],
      relationships: [{ type: 'IN_PROJECT', from: 'mr', to: 'p' }],
      limit: 200
    })
end

MegaTest.run_test('WorkItem → Milestone (IN_MILESTONE) — at least 5', expected_min: 5) do
  q(client, root, org_id, {
      query_type: 'traversal',
      nodes: [
        { id: 'wi', entity: 'WorkItem', columns: ['title'] },
        { id: 'm', entity: 'Milestone', columns: ['title'] }
      ],
      relationships: [{ type: 'IN_MILESTONE', from: 'wi', to: 'm' }],
      limit: 50
    })
end

MegaTest.run_test('Note on MR (HAS_NOTE) — at least 1', expected_min: 1) do
  q(client, root, org_id, {
      query_type: 'traversal',
      nodes: [
        { id: 'mr', entity: 'MergeRequest', columns: ['iid'] },
        { id: 'n', entity: 'Note', columns: ['id'] }
      ],
      relationships: [{ type: 'HAS_NOTE', from: 'mr', to: 'n' }],
      limit: 50
    })
end

MegaTest.run_test('MR with state=merged filter in traversal — at least 1', expected_min: 1) do
  q(client, root, org_id, {
      query_type: 'traversal',
      nodes: [
        { id: 'u',  entity: 'User',         columns: ['username'], node_ids: [1] },
        { id: 'mr', entity: 'MergeRequest', columns: %w[iid state],
          filters: { state: { op: 'eq', value: 'merged' } } }
      ],
      relationships: [{ type: 'AUTHORED', from: 'u', to: 'mr' }],
      limit: 50
    })
end

MegaTest.run_test('Pipelines → Projects — 0 (no CI)', expected_min: 0, expected_max: 0) do
  q(client, root, org_id, {
      query_type: 'traversal',
      nodes: [
        { id: 'pl', entity: 'Pipeline', columns: ['iid'] },
        { id: 'p', entity: 'Project', columns: ['name'] }
      ],
      relationships: [{ type: 'IN_PROJECT', from: 'pl', to: 'p' }],
      limit: 20
    })
end

# =============================================================================
# 6. NEIGHBORS
# Correct shape: neighbors: { node: 'alias', direction: 'both'|'incoming'|'outgoing' }
# Optional: rel_types: ['AUTHORED', ...]
# =============================================================================
MegaTest.section('6. NEIGHBORS')

MegaTest.run_test('Neighbors of project 1 (both) — at least 5', expected_min: 5) do
  q(client, root, org_id, {
      query_type: 'neighbors',
      node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [1] },
      neighbors: { node: 'p', direction: 'both' },
      limit: 50
    })
end

MegaTest.run_test('Neighbors of project 2 (incoming) — at least 1', expected_min: 1) do
  q(client, root, org_id, {
      query_type: 'neighbors',
      node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [2] },
      neighbors: { node: 'p', direction: 'incoming' },
      limit: 50
    })
end

MegaTest.run_test('Neighbors of project 1 (outgoing) — 0 or more', expected_min: 0) do
  q(client, root, org_id, {
      query_type: 'neighbors',
      node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [1] },
      neighbors: { node: 'p', direction: 'outgoing' },
      limit: 20
    })
end

MegaTest.run_test('Neighbors of user root (both) — at least 1', expected_min: 1) do
  q(client, root, org_id, {
      query_type: 'neighbors',
      node: { id: 'u', entity: 'User', columns: ['username'], node_ids: [1] },
      neighbors: { node: 'u', direction: 'both' },
      limit: 50
    })
end

MegaTest.run_test('Neighbors of user root via AUTHORED (outgoing) — at least 1', expected_min: 1) do
  q(client, root, org_id, {
      query_type: 'neighbors',
      node: { id: 'u', entity: 'User', columns: ['username'], node_ids: [1] },
      neighbors: { node: 'u', direction: 'outgoing', rel_types: ['AUTHORED'] },
      limit: 50
    })
end

MegaTest.run_test('Neighbors of group 24 gitlab-org (both) — at least 2', expected_min: 2) do
  q(client, root, org_id, {
      query_type: 'neighbors',
      node: { id: 'g', entity: 'Group', columns: ['name'], node_ids: [24] },
      neighbors: { node: 'g', direction: 'both' },
      limit: 50
    })
end

MegaTest.run_test('Neighbors of MR id=1 (both) — at least 1', expected_min: 1) do
  q(client, root, org_id, {
      query_type: 'neighbors',
      node: { id: 'mr', entity: 'MergeRequest', columns: ['iid'], node_ids: [1] },
      neighbors: { node: 'mr', direction: 'both' },
      limit: 50
    })
end

# =============================================================================
# 7. PATH FINDING
# query_type: 'path_finding' (not 'path')
# path: { type: 'shortest'|'any'|'all_shortest', from:, to:, max_depth: }
# =============================================================================
MegaTest.section('7. PATH FINDING')

MegaTest.run_test('Shortest path user(1) → project(1) — 0 or more', expected_min: 0) do
  q(client, root, org_id, {
      query_type: 'path_finding',
      nodes: [
        { id: 'u', entity: 'User',    columns: ['username'], node_ids: [1] },
        { id: 'p', entity: 'Project', columns: ['name'], node_ids: [1] }
      ],
      path: { type: 'shortest', from: 'u', to: 'p', max_depth: 3 },
      limit: 10
    })
end

MegaTest.run_test('Any path user(1) → project(2) — 0 or more', expected_min: 0) do
  q(client, root, org_id, {
      query_type: 'path_finding',
      nodes: [
        { id: 'u', entity: 'User',    columns: ['username'], node_ids: [1] },
        { id: 'p', entity: 'Project', columns: ['name'], node_ids: [2] }
      ],
      path: { type: 'any', from: 'u', to: 'p', max_depth: 3 },
      limit: 10
    })
end

MegaTest.run_test('Path user(1) → MR via AUTHORED only — 0 or more', expected_min: 0) do
  q(client, root, org_id, {
      query_type: 'path_finding',
      nodes: [
        { id: 'u',  entity: 'User', columns: ['username'], node_ids: [1] },
        { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
      ],
      path: { type: 'any', from: 'u', to: 'mr', max_depth: 2, rel_types: ['AUTHORED'] },
      limit: 10
    })
end

MegaTest.run_test('All shortest paths project(1) → pipeline — 0 (no CI)', expected_min: 0, expected_max: 0) do
  q(client, root, org_id, {
      query_type: 'path_finding',
      nodes: [
        { id: 'p',  entity: 'Project', columns: ['name'], node_ids: [1] },
        { id: 'pl', entity: 'Pipeline', columns: ['iid'] }
      ],
      path: { type: 'all_shortest', from: 'p', to: 'pl', max_depth: 2 },
      limit: 10
    })
end

# =============================================================================
# 8. ORDERING AND LIMIT
# order_by: { node: 'alias', property: 'column_name', direction: 'ASC'|'DESC' }
# =============================================================================
MegaTest.section('8. ORDERING AND LIMIT')

MegaTest.run_test('Projects ordered by name ASC — 4 rows', expected_min: 4, expected_max: 4) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'p', entity: 'Project', columns: %w[name full_path] },
      order_by: { node: 'p', property: 'name', direction: 'ASC' },
      limit: 100
    })
end

MegaTest.run_test('Projects ordered by name DESC — 4 rows', expected_min: 4, expected_max: 4) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'p', entity: 'Project', columns: %w[name full_path] },
      order_by: { node: 'p', property: 'name', direction: 'DESC' },
      limit: 100
    })
end

MegaTest.run_test('Limit 3 MRs — exactly 3', expected_min: 3, expected_max: 3) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'mr', entity: 'MergeRequest', columns: %w[iid state] },
      limit: 3
    })
end

MegaTest.run_test('Limit 1 project — exactly 1', expected_min: 1, expected_max: 1) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'p', entity: 'Project', columns: ['name'] },
      limit: 1
    })
end

MegaTest.run_test('Agg sort MR count DESC — at least 5 rows', expected_min: 5) do
  q(client, root, org_id, {
      query_type: 'aggregation',
      nodes: [
        { id: 'u',  entity: 'User',         columns: ['username'] },
        { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
      ],
      relationships: [{ type: 'AUTHORED', from: 'u', to: 'mr' }],
      aggregations: [{ function: 'count', target: 'mr', group_by: 'u', alias: 'mr_count' }],
      aggregation_sort: { agg_index: 0, direction: 'DESC' },
      limit: 10
    })
end

MegaTest.run_test('Agg sort MR count ASC — at least 5 rows', expected_min: 5) do
  q(client, root, org_id, {
      query_type: 'aggregation',
      nodes: [
        { id: 'u',  entity: 'User',         columns: ['username'] },
        { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
      ],
      relationships: [{ type: 'AUTHORED', from: 'u', to: 'mr' }],
      aggregations: [{ function: 'count', target: 'mr', group_by: 'u', alias: 'mr_count' }],
      aggregation_sort: { agg_index: 0, direction: 'ASC' },
      limit: 10
    })
end

# =============================================================================
# 9. PERMISSION / REDACTION
# =============================================================================
MegaTest.section('9. PERMISSION / REDACTION')

# --- Projects ---
MegaTest.run_test('Projects admin — 4', expected_min: 4, expected_max: 4) do
  q(client, root, org_id, { query_type: 'search', node: { id: 'p', entity: 'Project', columns: ['name'] }, limit: 100 })
end

MegaTest.run_test('Projects lois (dev proj2+3+group99) — 2', expected_min: 2, expected_max: 2) do
  q(client, lois, org_id, { query_type: 'search', node: { id: 'p', entity: 'Project', columns: ['name'] }, limit: 100 })
end

MegaTest.run_test('Projects franklyn (maintainer proj1) — 1', expected_min: 1, expected_max: 1) do
  q(client, franklyn, org_id,
    { query_type: 'search', node: { id: 'p', entity: 'Project', columns: ['name'] }, limit: 100 })
end

MegaTest.run_test('Projects vickey (guest/reporter) — 0', expected_min: 0, expected_max: 0) do
  q(client, vickey, org_id,
    { query_type: 'search', node: { id: 'p', entity: 'Project', columns: ['name'] }, limit: 100 })
end

MegaTest.run_test('Projects hanna (guest) — 0', expected_min: 0, expected_max: 0) do
  q(client, hanna, org_id,
    { query_type: 'search', node: { id: 'p', entity: 'Project', columns: ['name'] }, limit: 100 })
end

# --- MergeRequests ---
MegaTest.run_test('MRs admin — 188', expected_min: 188) do
  q(client, root, org_id,
    { query_type: 'search', node: { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }, limit: 200 })
end

MegaTest.run_test('MRs lois (proj2+3) — at least 10', expected_min: 10) do
  q(client, lois, org_id,
    { query_type: 'search', node: { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }, limit: 200 })
end

MegaTest.run_test('MRs franklyn (proj1) — 8', expected_min: 8, expected_max: 8) do
  q(client, franklyn, org_id,
    { query_type: 'search', node: { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }, limit: 200 })
end

MegaTest.run_test('MRs vickey — 0', expected_min: 0, expected_max: 0) do
  q(client, vickey, org_id,
    { query_type: 'search', node: { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }, limit: 200 })
end

MegaTest.run_test('MRs hanna — 0', expected_min: 0, expected_max: 0) do
  q(client, hanna, org_id,
    { query_type: 'search', node: { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }, limit: 200 })
end

# --- Private group kg-redaction-test-group ---
MegaTest.run_test('Private group (name filter) — lois (member) sees it', expected_min: 1) do
  q(client, lois, org_id, {
      query_type: 'search',
      node: { id: 'g', entity: 'Group', columns: ['name'],
              filters: { name: { op: 'eq', value: 'kg-redaction-test-group' } } },
      limit: 5
    })
end

MegaTest.run_test('Private group (name filter) — vickey (non-member) 0', expected_min: 0, expected_max: 0) do
  q(client, vickey, org_id, {
      query_type: 'search',
      node: { id: 'g', entity: 'Group', columns: ['name'],
              filters: { name: { op: 'eq', value: 'kg-redaction-test-group' } } },
      limit: 5
    })
end

MegaTest.run_test('Private group (name filter) — hanna (non-member) 0', expected_min: 0, expected_max: 0) do
  q(client, hanna, org_id, {
      query_type: 'search',
      node: { id: 'g', entity: 'Group', columns: ['name'],
              filters: { name: { op: 'eq', value: 'kg-redaction-test-group' } } },
      limit: 5
    })
end

# --- Notes ---
MegaTest.run_test('Notes admin — at least 140', expected_min: 140) do
  q(client, root, org_id, { query_type: 'search', node: { id: 'n', entity: 'Note', columns: ['id'] }, limit: 200 })
end

MegaTest.run_test('Notes lois (proj2+3) — at least 80', expected_min: 80) do
  q(client, lois, org_id, { query_type: 'search', node: { id: 'n', entity: 'Note', columns: ['id'] }, limit: 200 })
end

MegaTest.run_test('Notes franklyn (proj1) — at least 40', expected_min: 40) do
  q(client, franklyn, org_id, { query_type: 'search', node: { id: 'n', entity: 'Note', columns: ['id'] }, limit: 200 })
end

MegaTest.run_test('Notes vickey — 0', expected_min: 0, expected_max: 0) do
  q(client, vickey, org_id, { query_type: 'search', node: { id: 'n', entity: 'Note', columns: ['id'] }, limit: 200 })
end

# --- WorkItems ---
MegaTest.run_test('WorkItems franklyn (proj1) — at least 30', expected_min: 30) do
  q(client, franklyn, org_id,
    { query_type: 'search', node: { id: 'wi', entity: 'WorkItem', columns: ['title'] }, limit: 200 })
end

MegaTest.run_test('WorkItems lois (proj2+3) — at least 60', expected_min: 60) do
  q(client, lois, org_id,
    { query_type: 'search', node: { id: 'wi', entity: 'WorkItem', columns: ['title'] }, limit: 200 })
end

# --- Vulns: always 0 ---
MegaTest.run_test('Vulns admin — 0', expected_min: 0, expected_max: 0) do
  q(client, root, org_id,
    { query_type: 'search', node: { id: 'v', entity: 'Vulnerability', columns: ['id'] }, limit: 20 })
end

MegaTest.run_test('Vulns developer — 0', expected_min: 0, expected_max: 0) do
  q(client, lois, org_id,
    { query_type: 'search', node: { id: 'v', entity: 'Vulnerability', columns: ['id'] }, limit: 20 })
end

# =============================================================================
# 10. COMPLEX SCENARIOS
# =============================================================================
MegaTest.section('10. COMPLEX SCENARIOS')

MegaTest.run_test('All columns (*) on project 1', expected_min: 1, expected_max: 1) do
  q(client, root, org_id, {
      query_type: 'search',
      node: { id: 'p', entity: 'Project', columns: '*', node_ids: [1] },
      limit: 5
    })
end

MegaTest.run_test('Merged MRs in proj1 (franklyn) — 8', expected_min: 8, expected_max: 8) do
  q(client, franklyn, org_id, {
      query_type: 'search',
      node: { id: 'mr', entity: 'MergeRequest', columns: %w[iid state],
              filters: { state: { op: 'eq', value: 'merged' } } },
      limit: 50
    })
end

MegaTest.run_test('WorkItems with milestones (IN_MILESTONE) — at least 5', expected_min: 5) do
  q(client, root, org_id, {
      query_type: 'traversal',
      nodes: [
        { id: 'wi', entity: 'WorkItem', columns: ['title'] },
        { id: 'm', entity: 'Milestone', columns: ['title'] }
      ],
      relationships: [{ type: 'IN_MILESTONE', from: 'wi', to: 'm' }],
      limit: 50
    })
end

MegaTest.run_test('MRs grouped by user+project — at least 5 rows', expected_min: 5) do
  q(client, root, org_id, {
      query_type: 'aggregation',
      nodes: [
        { id: 'u',  entity: 'User', columns: ['username'] },
        { id: 'mr', entity: 'MergeRequest', columns: ['iid'] },
        { id: 'p',  entity: 'Project',      columns: ['name'] }
      ],
      relationships: [
        { type: 'AUTHORED', from: 'u', to: 'mr' },
        { type: 'IN_PROJECT', from: 'mr', to: 'p' }
      ],
      aggregations: [{ function: 'count', target: 'mr', group_by: 'u', alias: 'mr_count' }],
      limit: 20
    })
end

MegaTest.run_test('Failed pipelines with jobs — 0 (no CI)', expected_min: 0, expected_max: 0) do
  q(client, root, org_id, {
      query_type: 'traversal',
      nodes: [
        { id: 'pl', entity: 'Pipeline', columns: ['iid'],
          filters: { status: { op: 'eq', value: 'failed' } } },
        { id: 'j', entity: 'Job', columns: ['name'] }
      ],
      relationships: [{ type: 'IN_PIPELINE', from: 'j', to: 'pl' }],
      limit: 20
    })
end

MegaTest.run_test('Security dashboard (critical vulns) — 0 (no data)', expected_min: 0, expected_max: 0) do
  q(client, root, org_id, {
      query_type: 'aggregation',
      nodes: [
        { id: 'p', entity: 'Project', columns: ['name'] },
        { id: 'v', entity: 'Vulnerability', columns: ['severity'],
          filters: { severity: { op: 'in', value: %w[critical high] } } }
      ],
      relationships: [{ type: 'IN_PROJECT', from: 'v', to: 'p' }],
      aggregations: [{ function: 'count', target: 'v', group_by: 'p', alias: 'count' }],
      limit: 10
    })
end

# =============================================================================
# 11. RELATIONSHIP DIRECTIONS
# =============================================================================
MegaTest.section('11. RELATIONSHIP DIRECTIONS')

MegaTest.run_test('User outgoing AUTHORED (traversal) — at least 1', expected_min: 1) do
  q(client, root, org_id, {
      query_type: 'traversal',
      nodes: [
        { id: 'u',  entity: 'User',         columns: ['username'], node_ids: [1] },
        { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
      ],
      relationships: [{ type: 'AUTHORED', from: 'u', to: 'mr' }],
      limit: 50
    })
end

MegaTest.run_test('MR ← User incoming AUTHORED (traversal) — at least 1', expected_min: 1) do
  q(client, root, org_id, {
      query_type: 'traversal',
      nodes: [
        { id: 'mr', entity: 'MergeRequest', columns: ['iid'], id_range: { start: 1, end: 10 } },
        { id: 'u', entity: 'User', columns: ['username'] }
      ],
      relationships: [{ type: 'AUTHORED', from: 'u', to: 'mr' }],
      limit: 20
    })
end

MegaTest.run_test('Project ← MRs incoming (traversal) — at least 1', expected_min: 1) do
  q(client, root, org_id, {
      query_type: 'traversal',
      nodes: [
        { id: 'p',  entity: 'Project',      columns: ['name'], node_ids: [1] },
        { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
      ],
      relationships: [{ type: 'IN_PROJECT', from: 'mr', to: 'p' }],
      limit: 20
    })
end

MegaTest.run_test('Project neighbors incoming (neighbors query) — 0 or more', expected_min: 0) do
  q(client, root, org_id, {
      query_type: 'neighbors',
      node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [1] },
      neighbors: { node: 'p', direction: 'incoming' },
      limit: 20
    })
end

MegaTest.run_test('Project neighbors outgoing (neighbors query) — 0 or more', expected_min: 0) do
  q(client, root, org_id, {
      query_type: 'neighbors',
      node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [1] },
      neighbors: { node: 'p', direction: 'outgoing' },
      limit: 20
    })
end

MegaTest.run_test('MR neighbors both (neighbors query) — at least 1', expected_min: 1) do
  q(client, root, org_id, {
      query_type: 'neighbors',
      node: { id: 'mr', entity: 'MergeRequest', columns: ['iid'], node_ids: [1] },
      neighbors: { node: 'mr', direction: 'both' },
      limit: 50
    })
end

# =============================================================================
# 12. SECURITY ENTITIES (all zero)
# =============================================================================
MegaTest.section('12. SECURITY ENTITIES (all 0 — no scan data)')

%w[Vulnerability Finding SecurityScan VulnerabilityOccurrence].each do |entity|
  MegaTest.run_test("#{entity} — 0", expected_min: 0, expected_max: 0) do
    q(client, root, org_id, {
        query_type: 'search',
        node: { id: 'e', entity: entity, columns: ['id'] },
        limit: 10
      })
  end
end

MegaTest.run_test('Vuln count by state — 1 row (zero bucket)', expected_min: 1) do
  q(client, root, org_id, {
      query_type: 'aggregation',
      nodes: [{ id: 'v', entity: 'Vulnerability', columns: ['state'] }],
      aggregations: [{ function: 'count', target: 'v', group_by_column: 'state', alias: 'cnt' }],
      limit: 10
    })
end

MegaTest.run_test('Vuln count by severity — 1 row (zero bucket)', expected_min: 1) do
  q(client, root, org_id, {
      query_type: 'aggregation',
      nodes: [{ id: 'v', entity: 'Vulnerability', columns: ['severity'] }],
      aggregations: [{ function: 'count', target: 'v', group_by_column: 'severity', alias: 'cnt' }],
      limit: 10
    })
end

# =============================================================================
MegaTest.summary
