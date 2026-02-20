# frozen_string_literal: true

# =============================================================================
# Knowledge Graph — Redaction / Permission E2E Test
# =============================================================================
#
# Verifies that the GKG server correctly scopes results to each user's
# group_traversal_ids JWT claim. Only entities whose traversal_path is a
# prefix match for one of the user's group paths are returned.
#
# Run with:
#   cd ~/Desktop/Code/gdk/gitlab
#   bundle exec rails runner \
#     ~/Desktop/Code/gkg/tests/e2e/redaction_test.rb
#
# =============================================================================
# TEST DATA SETUP (already in this GDK instance)
# =============================================================================
#
# Private group 99  kg-redaction-test-group  traversal: 1/99/
# Private project 19  kg-redaction-test-project  traversal: 1/99/19/ (under group 99)
#
# Test users and JWT group_traversal_ids:
#   root (id=1)   admin              → sees everything (org-wide "1/")
#   lois (id=70)  reporter in group 24 (gitlab-org) + group 99
#                 → group_traversal_ids: ["1/24/", "1/99/"]
#   franklyn (id=72)  reporter in group 22 (toolbox)
#                 → group_traversal_ids: ["1/22/"]
#   vickey (id=71)  no reporter+ group memberships
#                 → group_traversal_ids: [] → sees NOTHING
#   hanna (id=73)   no reporter+ group memberships
#                 → group_traversal_ids: [] → sees NOTHING
#
# Entity counts by traversal scope (verified in ClickHouse):
#
#   Scope        Projects  MRs  Notes  WorkItems
#   1/22/           1        8    52      38       (franklyn)
#   1/24/25/        1        9    48      39
#   1/24/26/        1        4    48      42
#   1/99/ (proj19)  1        0     0       1
#   lois total      3       13    96      82       (1/24/ + 1/99/)
#
# =============================================================================

Feature.enable(:knowledge_graph)

module RedactionTest
  PASS = []
  FAIL = []

  def self.run(name, expected_min:, expected_max: nil, &block)
    result = block.call
    rows   = result[:result].is_a?(Array) ? result[:result] : []
    count  = rows.size
    ok     = count >= expected_min && (expected_max.nil? || count <= expected_max)
    range  = expected_max ? "#{expected_min}–#{expected_max}" : ">=#{expected_min}"
    if ok
      puts "  PASS  #{name} (#{count})"
      PASS << name
    else
      puts "  FAIL  #{name} — got #{count}, expected #{range}"
      FAIL << name
    end
  rescue StandardError => e
    puts "  FAIL  #{name} — ERROR: #{e.message[0..150]}"
    FAIL << name
  end

  def self.section(title)
    puts "\n--- #{title} ---"
  end

  def self.summary
    total = PASS.size + FAIL.size
    puts "\n#{'=' * 60}"
    puts "  RESULT: #{PASS.size}/#{total} passed"
    puts '=' * 60
    if FAIL.any?
      puts "\nFAILED:"
      FAIL.each { |f| puts "  * #{f}" }
    end
    puts
    exit(1) if FAIL.any?
  end
end

client = Ai::KnowledgeGraph::GrpcClient.new
org_id = Organizations::Organization.default_organization&.id || 1

root     = User.find_by!(username: 'root')
lois     = User.find_by!(username: 'lois')
franklyn = User.find_by!(username: 'franklyn.mcdermott')
User.find_by!(username: 'vickey.schmidt')
User.find_by!(username: 'hanna')

def q(client, user, org_id, query_json)
  client.execute_query(query_json: query_json, user: user, organization_id: org_id)
end

puts "\n#{'=' * 60}"
puts '  Knowledge Graph — Redaction Test Suite'
puts '=' * 60

# =============================================================================
# SECTION 1: Admin sees everything
# =============================================================================
RedactionTest.section('1. Admin (root) — sees all entities')

RedactionTest.run('root: all 4 projects', expected_min: 4, expected_max: 4) do
  q(client, root, org_id, { query_type: 'search',
                            node: { id: 'p', entity: 'Project', columns: ['name'] }, limit: 100 })
end

RedactionTest.run('root: MRs in project 1 via traversal (node_ids=[1])', expected_min: 8) do
  # Root traverses: project 1 → its MRs via IN_PROJECT
  q(client, root, org_id, { query_type: 'traversal',
                            nodes: [
                              { id: 'p', entity: 'Project', columns: ['name'], node_ids: [1] },
                              { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
                            ],
                            relationships: [{ type: 'IN_PROJECT', from: 'mr', to: 'p' }],
                            limit: 50 })
end

RedactionTest.run('root: private project id=19 visible', expected_min: 1, expected_max: 1) do
  q(client, root, org_id, { query_type: 'search',
                            node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [19] }, limit: 5 })
end

RedactionTest.run('root: private group kg-redaction-test-group visible', expected_min: 1) do
  q(client, root, org_id, { query_type: 'search',
                            node: { id: 'g', entity: 'Group', columns: ['name'],
                                    filters: { name: { op: 'eq', value: 'kg-redaction-test-group' } } }, limit: 5 })
end

# =============================================================================
# SECTION 2: lois — scoped to 1/24/ (gitlab-org) + 1/99/ (kg-redaction-test-group)
# =============================================================================
RedactionTest.section('2. lois — group_traversal_ids: ["1/24/", "1/99/"]')

RedactionTest.run('lois: 3 projects (proj 2, 3, 19)', expected_min: 3, expected_max: 3) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'p', entity: 'Project', columns: ['name'] }, limit: 100 })
end

RedactionTest.run('lois: project 2 visible (1/24/25/)', expected_min: 1, expected_max: 1) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [2] }, limit: 5 })
end

RedactionTest.run('lois: project 3 visible (1/24/26/)', expected_min: 1, expected_max: 1) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [3] }, limit: 5 })
end

RedactionTest.run('lois: private project 19 visible (member via group 99)', expected_min: 1, expected_max: 1) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [19] }, limit: 5 })
end

RedactionTest.run('lois: project 1 NOT visible (not in 1/24/ or 1/99/)', expected_min: 0, expected_max: 0) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [1] }, limit: 5 })
end

RedactionTest.run('lois: 13 MRs (proj 2+3, no proj 19 MRs)', expected_min: 13, expected_max: 13) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }, limit: 200 })
end

RedactionTest.run('lois: 96 notes (proj 2+3)', expected_min: 96, expected_max: 96) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'n', entity: 'Note', columns: ['id'] }, limit: 200 })
end

RedactionTest.run('lois: 82 work items (proj 2+3+19)', expected_min: 82, expected_max: 82) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'wi', entity: 'WorkItem', columns: ['title'] }, limit: 200 })
end

RedactionTest.run('lois: private group kg-redaction-test-group visible', expected_min: 1) do
  q(client, lois, org_id, { query_type: 'search',
                            node: { id: 'g', entity: 'Group', columns: ['name'],
                                    filters: { name: { op: 'eq', value: 'kg-redaction-test-group' } } }, limit: 5 })
end

# =============================================================================
# SECTION 3: franklyn — scoped to 1/22/ (toolbox) only
# =============================================================================
RedactionTest.section('3. franklyn — group_traversal_ids: ["1/22/"]')

RedactionTest.run('franklyn: 1 project (proj 1 only)', expected_min: 1, expected_max: 1) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'p', entity: 'Project', columns: ['name'] }, limit: 100 })
end

RedactionTest.run('franklyn: project 1 visible (1/22/23/)', expected_min: 1, expected_max: 1) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [1] }, limit: 5 })
end

RedactionTest.run('franklyn: project 2 NOT visible', expected_min: 0, expected_max: 0) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [2] }, limit: 5 })
end

RedactionTest.run('franklyn: project 19 NOT visible (not in 1/22/)', expected_min: 0, expected_max: 0) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [19] }, limit: 5 })
end

RedactionTest.run('franklyn: 8 MRs (proj 1 only)', expected_min: 8, expected_max: 8) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }, limit: 200 })
end

RedactionTest.run('franklyn: 52 notes (proj 1 only)', expected_min: 52, expected_max: 52) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'n', entity: 'Note', columns: ['id'] }, limit: 200 })
end

RedactionTest.run('franklyn: 38 work items (proj 1 only)', expected_min: 38, expected_max: 38) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'wi', entity: 'WorkItem', columns: ['title'] }, limit: 200 })
end

RedactionTest.run('franklyn: private group kg-redaction-test-group NOT visible', expected_min: 0, expected_max: 0) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'g', entity: 'Group', columns: ['name'],
                                        filters: { name: { op: 'eq', value: 'kg-redaction-test-group' } } }, limit: 5 })
end

RedactionTest.run('franklyn: private project 19 NOT visible', expected_min: 0, expected_max: 0) do
  q(client, franklyn, org_id, { query_type: 'search',
                                node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [19] }, limit: 5 })
end

# =============================================================================
# SECTION 4: vickey and hanna — empty traversal_ids → see nothing
# =============================================================================
RedactionTest.section('4. vickey & hanna — group_traversal_ids: [] → zero results')

%w[vickey.schmidt hanna].each do |username|
  u = User.find_by!(username: username)

  RedactionTest.run("#{username}: 0 projects", expected_min: 0, expected_max: 0) do
    q(client, u, org_id, { query_type: 'search',
                           node: { id: 'p', entity: 'Project', columns: ['name'] }, limit: 100 })
  end

  RedactionTest.run("#{username}: 0 MRs", expected_min: 0, expected_max: 0) do
    q(client, u, org_id, { query_type: 'search',
                           node: { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }, limit: 100 })
  end

  RedactionTest.run("#{username}: 0 notes", expected_min: 0, expected_max: 0) do
    q(client, u, org_id, { query_type: 'search',
                           node: { id: 'n', entity: 'Note', columns: ['id'] }, limit: 100 })
  end

  RedactionTest.run("#{username}: 0 work items", expected_min: 0, expected_max: 0) do
    q(client, u, org_id, { query_type: 'search',
                           node: { id: 'wi', entity: 'WorkItem', columns: ['title'] }, limit: 100 })
  end

  RedactionTest.run("#{username}: private group NOT visible", expected_min: 0, expected_max: 0) do
    q(client, u, org_id, { query_type: 'search',
                           node: { id: 'g', entity: 'Group', columns: ['name'],
                                   filters: { name: { op: 'eq', value: 'kg-redaction-test-group' } } }, limit: 5 })
  end

  RedactionTest.run("#{username}: private project 19 NOT visible", expected_min: 0, expected_max: 0) do
    q(client, u, org_id, { query_type: 'search',
                           node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [19] }, limit: 5 })
  end
end

# =============================================================================
# SECTION 5: Cross-user isolation — lois cannot see franklyn's project, and vice versa
# =============================================================================
RedactionTest.section('5. Cross-user isolation')

RedactionTest.run('lois cannot see proj 1 MRs (1/22/ not in her claims)', expected_min: 0, expected_max: 0) do
  # Traversal from proj 1: lois has no 1/22/ claim so proj 1 itself is blocked
  q(client, lois, org_id, { query_type: 'traversal',
                            nodes: [
                              { id: 'p',  entity: 'Project',      columns: ['name'], node_ids: [1] },
                              { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
                            ],
                            relationships: [{ type: 'IN_PROJECT', from: 'mr', to: 'p' }],
                            limit: 50 })
end

RedactionTest.run('franklyn cannot see proj 2 MRs (1/24/ not in his claims)', expected_min: 0, expected_max: 0) do
  # Traversal from proj 2: franklyn has no 1/24/ claim so proj 2 itself is blocked
  q(client, franklyn, org_id, { query_type: 'traversal',
                                nodes: [
                                  { id: 'p',  entity: 'Project',      columns: ['name'], node_ids: [2] },
                                  { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
                                ],
                                relationships: [{ type: 'IN_PROJECT', from: 'mr', to: 'p' }],
                                limit: 50 })
end

RedactionTest.run('lois sees MRs in proj 2 via traversal (9-10 MRs)', expected_min: 9, expected_max: 10) do
  q(client, lois, org_id, { query_type: 'traversal',
                            nodes: [
                              { id: 'p',  entity: 'Project',      columns: ['name'], node_ids: [2] },
                              { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
                            ],
                            relationships: [{ type: 'IN_PROJECT', from: 'mr', to: 'p' }],
                            limit: 50 })
end

RedactionTest.run('franklyn sees MRs in proj 1 via traversal (8 MRs)', expected_min: 8, expected_max: 8) do
  q(client, franklyn, org_id, { query_type: 'traversal',
                                nodes: [
                                  { id: 'p',  entity: 'Project',      columns: ['name'], node_ids: [1] },
                                  { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
                                ],
                                relationships: [{ type: 'IN_PROJECT', from: 'mr', to: 'p' }],
                                limit: 50 })
end

# =============================================================================
RedactionTest.summary
