# frozen_string_literal: true

# AUTO-GENERATED from e2e/tests/scenarios.yaml — do not edit directly.
# Regenerate: cargo xtask e2e codegen
# Verify:     cargo xtask e2e codegen --check

require 'json'

E2E_POD_DIR = ENV.fetch('E2E_POD_DIR', '/tmp/e2e')
MANIFEST_PATH = "#{E2E_POD_DIR}/manifest.json"
TEST_PASSWORD = 'TestPass123!'

def unwrap(result, key)
  if result.respond_to?(:payload) && result.payload.is_a?(Hash)
    result.payload[key] || result.payload
  elsif result.is_a?(Hash)
    result[key] || result
  else
    result
  end
end

puts '=== CREATING E2E TEST DATA ==='

Feature.enable(:knowledge_graph)

org = Organizations::Organization.default_organization || Organizations::Organization.first
admin = User.find_by!(username: 'root')
puts "Organization: #{org.name} (id: #{org.id})"
puts "Admin: #{admin.username} (id: #{admin.id})"

manifest = {
  organization_id: org.id,
  admin_id: admin.id,
  users: {},
  groups: {},
  projects: {},
  merge_requests: {},
  work_items: {},
  notes: {},
  milestones: {},
  labels: {},
  counts: {}
}

# =============================================================================
# 1. CREATE TEST USERS
# =============================================================================
puts "\n--- 1. Creating test users ---"

def find_or_create_user(username, name, email, admin, org)
  user = User.find_by(username: username)
  if user
    puts "  User '#{username}' already exists (id: #{user.id})"
  else
    user = User.new(
      username: username,
      name: name,
      email: email,
      password: TEST_PASSWORD,
      password_confirmation: TEST_PASSWORD,
      confirmed_at: Time.current,
      organization_id: org.id,
      skip_confirmation: true
    )
    user.assign_personal_namespace(org)
    user.save!
    puts "  Created user '#{username}' (id: #{user.id})"
  end

  Organizations::OrganizationUser.find_or_create_by!(organization: org, user: user) do |record|
    record.access_level = :default
  end

  user
rescue StandardError => e
  puts "  Direct creation failed (#{e.message[0..80]}), trying CreateService..."
  result = Users::CreateService.new(admin, {
                                      username: username,
                                      name: name,
                                      email: email,
                                      password: TEST_PASSWORD,
                                      skip_confirmation: true,
                                      organization_id: org.id
                                    }).execute
  user = result.is_a?(User) ? result : result[:user]
  raise "Failed to create user '#{username}': #{result.inspect}" unless user&.persisted?

  puts "  Created user '#{username}' via service (id: #{user.id})"

  Organizations::OrganizationUser.find_or_create_by!(organization: org, user: user) do |record|
    record.access_level = :default
  end

  user
end

franklyn = find_or_create_user('franklyn.mcdermott', 'Franklyn McDermott', 'franklyn@example.com', admin, org)
hanna = find_or_create_user('hanna', 'Hanna Baker', 'hanna@example.com', admin, org)
lois = find_or_create_user('lois', 'Lois Lane', 'lois@example.com', admin, org)
vickey = find_or_create_user('vickey.schmidt', 'Vickey Schmidt', 'vickey@example.com', admin, org)

manifest[:users] = {
  root: { id: admin.id, username: 'root' },
  franklyn: { id: franklyn.id, username: 'franklyn.mcdermott' },
  hanna: { id: hanna.id, username: 'hanna' },
  lois: { id: lois.id, username: 'lois' },
  vickey: { id: vickey.id, username: 'vickey.schmidt' },
}

# =============================================================================
# 2. CREATE GROUPS
# =============================================================================
puts "\n--- 2. Creating group hierarchy ---"

def find_or_create_group(name, path, admin, org, parent: nil, visibility: 20)
  group = Group.find_by(path: path, parent_id: parent&.id)
  if group
    puts "  Group '#{name}' already exists (id: #{group.id})"
    return group
  end

  params = {
    name: name,
    path: path,
    visibility_level: visibility,
    organization_id: org.id
  }
  params[:parent_id] = parent.id if parent

  result = Groups::CreateService.new(admin, params).execute
  group = unwrap(result, :group)
  raise "Failed to create group '#{name}': #{result.inspect}" unless group.is_a?(Group) && group.persisted?

  puts "  Created group '#{name}' (id: #{group.id}, traversal: #{group.traversal_ids.join('/')}/) #{visibility == 0 ? '[PRIVATE]' : '[PUBLIC]'}"
  group
end

gitlab_org_group = find_or_create_group('gitlab-org', 'gitlab-org', admin, org, visibility: 20)
backend_group = find_or_create_group('backend', 'backend', admin, org, parent: gitlab_org_group, visibility: 20)
frontend_group = find_or_create_group('frontend', 'frontend', admin, org, parent: gitlab_org_group, visibility: 20)
redaction_group = find_or_create_group('kg-redaction-test-group', 'kg-redaction-test-group', admin, org, visibility: 0)
toolbox_group = find_or_create_group('toolbox', 'toolbox', admin, org, visibility: 20)
smoke_tests_group = find_or_create_group('smoke-tests', 'smoke-tests', admin, org, parent: toolbox_group, visibility: 20)

manifest[:groups] = {
  gitlab_org: { id: gitlab_org_group.id, path: gitlab_org_group.full_path,
              traversal: "#{gitlab_org_group.traversal_ids.join('/')}/" },
  backend: { id: backend_group.id, path: backend_group.full_path,
              traversal: "#{backend_group.traversal_ids.join('/')}/" },
  frontend: { id: frontend_group.id, path: frontend_group.full_path,
              traversal: "#{frontend_group.traversal_ids.join('/')}/" },
  redaction: { id: redaction_group.id, path: redaction_group.full_path,
              traversal: "#{redaction_group.traversal_ids.join('/')}/",
              visibility: 'private' },
  toolbox: { id: toolbox_group.id, path: toolbox_group.full_path,
              traversal: "#{toolbox_group.traversal_ids.join('/')}/" },
  smoke_tests: { id: smoke_tests_group.id, path: smoke_tests_group.full_path,
              traversal: "#{smoke_tests_group.traversal_ids.join('/')}/" },
}

# =============================================================================
# 3. CREATE PROJECTS
# =============================================================================
puts "\n--- 3. Creating projects ---"

def find_or_create_project(name, path, namespace, admin, org, visibility: 20)
  project = Project.find_by(path: path, namespace_id: namespace.id)
  if project
    puts "  Project '#{name}' already exists (id: #{project.id})"
    return project
  end

  result = Projects::CreateService.new(admin, {
                                         name: name,
                                         path: path,
                                         namespace_id: namespace.id,
                                         visibility_level: visibility,
                                         organization_id: org.id,
                                         initialize_with_readme: true
                                       }).execute
  project = unwrap(result, :project)
  raise "Failed to create project '#{name}': #{result.inspect}" unless project.is_a?(Project) && project.persisted?

  puts "  Created project '#{name}' (id: #{project.id}, visibility: #{visibility == 0 ? 'private' : 'public'})"
  project
end

proj_backend = find_or_create_project('gitlab-shell', 'gitlab-shell', backend_group, admin, org, visibility: 20)
proj_frontend = find_or_create_project('gitlab-test', 'gitlab-test', frontend_group, admin, org, visibility: 20)
proj_redaction = find_or_create_project('kg-redaction-test-project', 'kg-redaction-test-project', redaction_group, admin, org, visibility: 0)
proj_smoke = find_or_create_project('gitlab-smoke-tests', 'gitlab-smoke-tests', smoke_tests_group, admin, org, visibility: 20)

all_public_projects = [proj_backend, proj_frontend, proj_smoke]
all_projects = [proj_backend, proj_frontend, proj_redaction, proj_smoke]

manifest[:projects] = {
  backend: { id: proj_backend.id, name: proj_backend.name, path: proj_backend.full_path,
           group_key: :backend, visibility: 'public' },
  frontend: { id: proj_frontend.id, name: proj_frontend.name, path: proj_frontend.full_path,
           group_key: :frontend, visibility: 'public' },
  redaction: { id: proj_redaction.id, name: proj_redaction.name, path: proj_redaction.full_path,
           group_key: :redaction, visibility: 'private' },
  smoke: { id: proj_smoke.id, name: proj_smoke.name, path: proj_smoke.full_path,
           group_key: :smoke_tests, visibility: 'public' },
}

# =============================================================================
# 4. MEMBERSHIPS
# =============================================================================
puts "\n--- 4. Setting up memberships ---"

def add_group_member(group, user, access_level, label)
  return if group.member?(user)

  member = group.add_member(user, access_level)
  if member.persisted?
    puts "  Added #{user.username} to group '#{group.name}' as #{label}"
  else
    puts "  ERROR: Failed to add #{user.username} to group '#{group.name}': #{member.errors.full_messages.join(', ')}"
  end
rescue StandardError => e
  puts "  ERROR: Could not add #{user.username} to group '#{group.name}': #{e.class}: #{e.message[0..120]}"
end

add_group_member(toolbox_group, franklyn, Gitlab::Access::DEVELOPER, 'developer')
add_group_member(gitlab_org_group, lois, Gitlab::Access::DEVELOPER, 'developer')
add_group_member(redaction_group, lois, Gitlab::Access::DEVELOPER, 'developer')

manifest[:memberships] = {
  franklyn: {
    groups: [:toolbox],
    access_level: 'developer',
    visible_project_keys: [:smoke],
    visible_group_traversals: [
      manifest[:groups][:toolbox][:traversal],
    ]
  },
  hanna: { groups: [], visible_project_keys: [], visible_group_traversals: [] },
  lois: {
    groups: [:gitlab_org, :redaction],
    access_level: 'developer',
    visible_project_keys: [:backend, :frontend, :redaction],
    visible_group_traversals: [
      manifest[:groups][:gitlab_org][:traversal],
      manifest[:groups][:redaction][:traversal],
    ]
  },
  vickey: { groups: [], visible_project_keys: [], visible_group_traversals: [] },
}

# =============================================================================
# 5. POPULATE knowledge_graph_enabled_namespaces
# =============================================================================
puts "\n--- 5. Populating knowledge_graph_enabled_namespaces ---"

root_groups = [gitlab_org_group, redaction_group, toolbox_group]
root_groups.each do |group|
  ActiveRecord::Base.connection.execute(<<~SQL)
    INSERT INTO knowledge_graph_enabled_namespaces (root_namespace_id, created_at, updated_at)
    VALUES (#{group.id}, NOW(), NOW())
    ON CONFLICT (root_namespace_id) DO NOTHING
  SQL
  puts "  Enabled namespace: #{group.name} (root_namespace_id: #{group.id})"
end

# =============================================================================
# 6. CREATE MILESTONES
# =============================================================================
puts "\n--- 6. Creating milestones ---"

milestone_count = 0
all_projects.each do |proj|
  3.times do |i|
    title = "#{proj.name} Milestone #{i + 1}"
    next if proj.milestones.find_by(title: title)

    ms = Milestones::CreateService.new(proj, admin, {
                                         title: title,
                                         description: "Milestone #{i + 1} for #{proj.name}",
                                         start_date: Date.today - (30 * (3 - i)),
                                         due_date: Date.today + (30 * (i + 1))
                                       }).execute
    milestone = unwrap(ms, :milestone)
    milestone_count += 1 if milestone&.persisted?
  end
end
puts "  Created #{milestone_count} new milestones (#{all_projects.sum { |p| p.milestones.count }} total)"

manifest[:milestones] = all_projects.each_with_object({}) do |proj, h|
  key = manifest[:projects].find { |_k, v| v[:id] == proj.id }&.first
  h[key] = proj.milestones.pluck(:id, :title).map { |id, title| { id: id, title: title } }
end

# =============================================================================
# 7. CREATE LABELS
# =============================================================================
puts "\n--- 7. Creating labels ---"

label_count = 0
colors = %w[#FF0000 #00FF00 #0000FF #FF6600 #9900CC #009999]
all_projects.each_with_index do |proj, pi|
  3.times do |i|
    title = "#{proj.name}-label-#{i + 1}"
    unless proj.labels.find_by(title: title)
      Labels::CreateService.new({ title: title, color: colors[(pi * 3 + i) % colors.size] }).execute(project: proj)
      label_count += 1
    end
  end
end
puts "  Created #{label_count} new labels (#{all_projects.sum { |p| p.labels.count }} total)"

manifest[:labels] = all_projects.each_with_object({}) do |proj, h|
  key = manifest[:projects].find { |_k, v| v[:id] == proj.id }&.first
  h[key] = proj.labels.pluck(:id, :title).map { |id, title| { id: id, title: title } }
end

# =============================================================================
# 8. CREATE WORK ITEMS
# =============================================================================
puts "\n--- 8. Creating work items (issues) ---"

work_item_count = 0
all_projects.each do |proj|
  milestones = proj.milestones.to_a
  labels = proj.labels.to_a
  8.times do |i|
    title = "#{proj.name} Issue #{i + 1}"
    next if proj.issues.find_by(title: title)

    params = {
      title: title,
      description: "Test issue #{i + 1} for #{proj.name}. This exercises work item queries.",
      milestone_id: milestones[i % milestones.size]&.id,
      label_ids: [labels[i % labels.size]&.id].compact
    }
    result = Issues::CreateService.new(
      container: proj,
      current_user: admin,
      params: params
    ).execute
    issue = unwrap(result, :issue)
    work_item_count += 1 if issue.is_a?(Issue) && issue.persisted?
  end
end
puts "  Created #{work_item_count} new work items (#{all_projects.sum { |p| p.issues.count }} total)"

manifest[:work_items] = all_projects.each_with_object({}) do |proj, h|
  key = manifest[:projects].find { |_k, v| v[:id] == proj.id }&.first
  h[key] = proj.issues.pluck(:id, :title).map { |id, title| { id: id, title: title } }
end

# =============================================================================
# 9. CREATE MERGE REQUESTS
# =============================================================================
puts "\n--- 9. Creating merge requests ---"

mr_count = 0

all_public_projects.each do |proj|
  6.times do |i|
    title = "#{proj.name} MR #{i + 1}"
    source_branch = "feature/#{proj.path}-mr-#{i + 1}"
    next if proj.merge_requests.find_by(title: title)

    begin
      proj.repository.create_branch(source_branch, proj.default_branch || 'main')
    rescue StandardError
      # Branch may already exist
    end

    state = i < 4 ? 'merged' : 'opened'

    result = MergeRequests::CreateService.new(
      project: proj,
      current_user: admin,
      params: {
        title: title,
        description: "Test MR #{i + 1} for #{proj.name}",
        source_branch: source_branch,
        target_branch: proj.default_branch || 'main'
      }
    ).execute
    mr = unwrap(result, :merge_request)

    if mr.is_a?(MergeRequest) && mr.persisted? && state == 'merged'
      mr.update_columns(state_id: 3)
      begin
        mr.metrics&.update_columns(merged_at: Time.current - (6 - i).days)
      rescue StandardError
        nil
      end
    end

    mr_count += 1 if mr.is_a?(MergeRequest) && mr.persisted?
  end
end

puts "  Created #{mr_count} new MRs (#{all_projects.sum { |p| p.merge_requests.count }} total)"

manifest[:merge_requests] = all_projects.each_with_object({}) do |proj, h|
  key = manifest[:projects].find { |_k, v| v[:id] == proj.id }&.first
  state_map = { 1 => 'opened', 2 => 'closed', 3 => 'merged', 4 => 'locked' }
  mrs = proj.merge_requests.pluck(:id, :iid, :title, :state_id)
  h[key] = mrs.map { |id, iid, title, sid| { id: id, iid: iid, title: title, state: state_map[sid] || 'unknown' } }
end

# =============================================================================
# 10. CREATE NOTES
# =============================================================================
puts "\n--- 10. Creating notes ---"

note_count = 0
all_public_projects.each do |proj|
  proj.merge_requests.limit(4).each do |mr|
    2.times do |i|
      body = "Review comment #{i + 1} on #{mr.title}"
      next if Note.find_by(noteable: mr, note: body)

      result = Notes::CreateService.new(proj, admin, {
                                          noteable: mr,
                                          note: body
                                        }).execute
      note = unwrap(result, :note)
      note_count += 1 if note.is_a?(Note) && note.persisted?
    end
  end

  proj.issues.limit(4).each do |issue|
    2.times do |i|
      body = "Discussion comment #{i + 1} on #{issue.title}"
      next if Note.find_by(noteable: issue, note: body)

      result = Notes::CreateService.new(proj, admin, {
                                          noteable: issue,
                                          note: body
                                        }).execute
      note = unwrap(result, :note)
      note_count += 1 if note.is_a?(Note) && note.persisted?
    end
  end
end
puts "  Created #{note_count} new notes"

manifest[:notes] = all_projects.each_with_object({}) do |proj, h|
  key = manifest[:projects].find { |_k, v| v[:id] == proj.id }&.first
  count = Note.joins("INNER JOIN issues ON notes.noteable_type = 'Issue' AND notes.noteable_id = issues.id")
              .where(issues: { project_id: proj.id })
              .where(system: false).count +
          Note.joins("INNER JOIN merge_requests ON notes.noteable_type = 'MergeRequest' AND notes.noteable_id = merge_requests.id")
              .where(merge_requests: { target_project_id: proj.id })
              .where(system: false).count
  h[key] = { count: count }
end

# =============================================================================
# 11. COMPUTE COUNTS FOR MANIFEST
# =============================================================================
puts "\n--- 11. Computing entity counts ---"

total_projects = Project.count
total_users = User.count
total_groups = Group.count
total_mrs = MergeRequest.count
total_work_items = Issue.count
total_labels = Label.count
total_milestones = Milestone.count
total_notes = Note.where(system: false).count

manifest[:counts] = {
  total_projects: total_projects,
  total_users: total_users,
  total_groups: total_groups,
  total_merge_requests: total_mrs,
  total_work_items: total_work_items,
  total_labels: total_labels,
  total_milestones: total_milestones,
  total_notes: total_notes,
  per_project: {}
}

all_projects.each do |proj|
  key = manifest[:projects].find { |_k, v| v[:id] == proj.id }&.first
  manifest[:counts][:per_project][key] = {
    merge_requests: proj.merge_requests.count,
    work_items: proj.issues.count,
    milestones: proj.milestones.count,
    labels: proj.labels.count,
    notes: manifest[:notes][key][:count]
  }
end

# Per-user expected visible counts (based on group memberships)
franklyn_visible = [:smoke]
lois_visible = [:backend, :frontend, :redaction]

manifest[:counts][:per_user] = {
  root: {
    projects: total_projects,
    merge_requests: total_mrs,
    work_items: total_work_items,
    notes: total_notes
  },
  franklyn: {
    projects: franklyn_visible.size,
    merge_requests: franklyn_visible.sum { |k| manifest[:counts][:per_project][k][:merge_requests] },
    work_items: franklyn_visible.sum { |k| manifest[:counts][:per_project][k][:work_items] },
    notes: franklyn_visible.sum { |k| manifest[:counts][:per_project][k][:notes] }
  },
  hanna: { projects: 0, merge_requests: 0, work_items: 0, notes: 0 },
  lois: {
    projects: lois_visible.size,
    merge_requests: lois_visible.sum { |k| manifest[:counts][:per_project][k][:merge_requests] },
    work_items: lois_visible.sum { |k| manifest[:counts][:per_project][k][:work_items] },
    notes: lois_visible.sum { |k| manifest[:counts][:per_project][k][:notes] }
  },
  vickey: { projects: 0, merge_requests: 0, work_items: 0, notes: 0 },
}

# =============================================================================
# 12. WRITE MANIFEST
# =============================================================================
puts "\n--- 12. Writing manifest ---"

File.write(MANIFEST_PATH, JSON.pretty_generate(manifest))
puts "  Manifest written to #{MANIFEST_PATH}"

# =============================================================================
# SUMMARY
# =============================================================================
puts "\n=== E2E TEST DATA SUMMARY ==="
puts "Organization: #{org.id}"
puts ''
puts 'Users:'
manifest[:users].each { |k, v| puts "  #{k}: id=#{v[:id]} username=#{v[:username]}" }
puts ''
puts 'Groups:'
manifest[:groups].each { |k, v| puts "  #{k}: id=#{v[:id]} path=#{v[:path]} traversal=#{v[:traversal]}" }
puts ''
puts 'Projects:'
manifest[:projects].each { |k, v| puts "  #{k}: id=#{v[:id]} name=#{v[:name]} visibility=#{v[:visibility]}" }
puts ''
puts 'Entity counts:'
puts "  Projects:        #{total_projects}"
puts "  Users:           #{total_users}"
puts "  Groups:          #{total_groups}"
puts "  MergeRequests:   #{total_mrs}"
puts "  WorkItems:       #{total_work_items}"
puts "  Labels:          #{total_labels}"
puts "  Milestones:      #{total_milestones}"
puts "  Notes:           #{total_notes}"
puts ''
puts 'Per-user visible counts:'
manifest[:counts][:per_user].each do |user, counts|
  puts "  #{user}: #{counts.map { |k, v| "#{k}=#{v}" }.join(', ')}"
end
puts ''
puts 'Memberships:'
puts '  franklyn: developer on toolbox -> sees smoke projects'
puts '  hanna: no memberships -> sees nothing'
puts '  lois: developer on gitlab_org + redaction -> sees backend, frontend, redaction projects'
puts '  vickey: no memberships -> sees nothing'
puts ''
puts '=== DONE ==='
