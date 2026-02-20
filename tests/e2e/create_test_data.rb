# frozen_string_literal: true

# Script to create test data for membership-based redaction testing
# Run with: mise exec -- bundle exec rails runner /path/to/create_test_data.rb

puts "=== CREATING TEST DATA FOR MEMBERSHIP-BASED REDACTION ==="

org = Organizations::Organization.first
admin = User.find(1)
puts "Using organization: #{org.name} (id: #{org.id})"

# Create test group
test_group = Group.find_by(name: "kg-redaction-test-group")
if test_group
  puts "Test group already exists: #{test_group.id}"
else
  result = Groups::CreateService.new(
    admin,
    { name: "kg-redaction-test-group", path: "kg-redaction-test-group", visibility_level: 0, organization_id: org.id }
  ).execute
  test_group = result[:group] || result
  puts "Created test group: #{test_group.id}"
end

# Create test project in the group
test_project = Project.find_by(name: "kg-redaction-test-project")
if test_project
  puts "Test project already exists: #{test_project.id}"
else
  result = Projects::CreateService.new(
    admin,
    { name: "kg-redaction-test-project", path: "kg-redaction-test-project", namespace_id: test_group.id, visibility_level: 0, organization_id: org.id }
  ).execute
  test_project = result[:project] || result
  puts "Created test project: #{test_project.id}"
end

# Use existing users for membership testing
lois = User.find_by(username: "lois")
vickey = User.find_by(username: "vickey.schmidt")
hanna = User.find_by(username: "hanna")

# Add lois to test group as developer (she should see the data)
if lois && !test_group.member?(lois)
  test_group.add_member(lois, Gitlab::Access::DEVELOPER)
  puts "Added lois to test group as developer"
elsif lois
  puts "lois already in test group"
end

# vickey and hanna should NOT be added - they should not see the private group data

# Create an issue (work item) in the test project
issue = test_project.issues.find_by(title: "KG Redaction Test Issue")
if issue
  puts "Test issue already exists: #{issue.id}"
else
  result = Issues::CreateService.new(
    container: test_project,
    current_user: admin,
    params: { title: "KG Redaction Test Issue", description: "Testing membership-based redaction" }
  ).execute
  issue = result[:issue]
  puts "Created test issue: #{issue&.id} (iid: #{issue&.iid})"
end

# Create a milestone
milestone = test_project.milestones.find_by(title: "KG Test Milestone")
if milestone
  puts "Test milestone already exists: #{milestone.id}"
else
  result = Milestones::CreateService.new(
    test_project,
    admin,
    { title: "KG Test Milestone", description: "Test milestone for KG" }
  ).execute
  milestone = result.is_a?(Milestone) ? result : (result[:milestone] rescue result)
  puts "Created test milestone: #{milestone&.id}"
end

# Create a label
label = test_project.labels.find_by(title: "kg-test-label")
if label
  puts "Test label already exists: #{label.id}"
else
  label = Labels::CreateService.new({ title: "kg-test-label", color: "#FF0000" }).execute(project: test_project)
  puts "Created test label: #{label.id}"
end

puts "\n=== TEST DATA SUMMARY ==="
puts "Organization: #{org.id}"
puts "Group: #{test_group.name} (id: #{test_group.id}, visibility: #{test_group.visibility_level})"
puts "Project: #{test_project.name} (id: #{test_project.id}, visibility: #{test_project.visibility_level})"
puts "Issue: #{issue&.title} (id: #{issue&.id})"
puts "Milestone: #{milestone&.title} (id: #{milestone&.id})"
puts "Label: #{label&.title} (id: #{label&.id})"

puts "\n=== MEMBERSHIP CHECK ==="
puts "lois in kg-redaction-test-group: #{test_group.member?(lois)}"
puts "vickey.schmidt in kg-redaction-test-group: #{test_group.member?(vickey)}"
puts "hanna in kg-redaction-test-group: #{test_group.member?(hanna)}"

puts "\n=== EXPECTED REDACTION BEHAVIOR ==="
puts "lois (developer in group): SHOULD see test project entities"
puts "vickey.schmidt (not in group): SHOULD NOT see test project entities"
puts "hanna (not in group): SHOULD NOT see test project entities"

puts "\n=== DONE ==="
