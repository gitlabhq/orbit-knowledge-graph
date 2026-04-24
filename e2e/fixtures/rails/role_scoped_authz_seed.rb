require 'digest/sha1'
require 'json'
require 'securerandom'

org = Organizations::Organization.first
admin = User.find_by(username: 'root') || User.admins.first
suffix = "kg347e2e#{SecureRandom.hex(4)}"

raise 'No organization found' unless org
raise 'No admin user found' unless admin

def create_group!(name, org, parent: nil)
  Group.create!(name: name, path: name, organization: org, parent: parent)
end

def enable_knowledge_graph!(group)
  Analytics::KnowledgeGraph::EnabledNamespace.find_or_create_by!(root_namespace_id: group.id)
end

def create_project!(name, group, admin, org)
  Project.create!(name: name, path: name, namespace: group, creator: admin, organization: org)
end

def create_vulnerability!(project, admin, title:, severity:, report_type:, created_at:)
  scanner = Vulnerabilities::Scanner.find_or_create_by!(
    project: project,
    external_id: "kg347-#{project.id}",
    name: 'kg347',
    vendor: 'GitLab'
  )

  identifier = Vulnerabilities::Identifier.find_or_create_by!(
    project: project,
    external_type: 'cwe',
    external_id: 'CWE-89',
    fingerprint: Digest::SHA1.hexdigest("kg347-#{project.id}"),
    name: 'SQL Injection'
  )

  finding = Vulnerabilities::Finding.find_or_initialize_by(
    project: project,
    primary_identifier: identifier,
    scanner: scanner,
    location_fingerprint: Digest::SHA1.hexdigest("kg347-finding-#{project.id}")
  )

  finding.assign_attributes(
    severity: severity,
    report_type: report_type,
    name: title,
    metadata_version: 'sast:1.0',
    raw_metadata: '{}',
    uuid: SecureRandom.uuid,
    created_at: created_at,
    updated_at: created_at
  )
  finding.save!

  vulnerability = Vulnerability.find_or_initialize_by(project: project, title: title)
  vulnerability.assign_attributes(
    author: admin,
    severity: severity,
    state: :detected,
    report_type: report_type,
    present_on_default_branch: true,
    created_at: created_at,
    updated_at: created_at
  )
  vulnerability.finding_id = finding.id
  vulnerability.save!
  finding.update_column(:vulnerability_id, vulnerability.id)

  vulnerability
end

victim_result = Users::CreateService.new(
  admin,
  username: suffix,
  email: "#{suffix}@example.com",
  name: "KG 347 E2E #{suffix}",
  password: 'Password1234!',
  password_confirmation: 'Password1234!',
  organization_id: org.id,
  skip_confirmation: true
).execute

victim = if victim_result.respond_to?(:success?) && !victim_result.success?
           raise "Failed to create user: #{victim_result.message}"
         else
           User.find_by!(username: suffix)
         end

reporter_group = create_group!("#{suffix}-reporter", org)
security_group = create_group!("#{suffix}-security", org)
developer_group = create_group!("#{suffix}-developer", org)
maintainer_group = create_group!("#{suffix}-maintainer", org)
nested_parent_group = create_group!("#{suffix}-nested-parent", org)
nested_reporter_group = create_group!("#{suffix}-nested-reporter", org, parent: nested_parent_group)
nested_developer_group = create_group!("#{suffix}-nested-developer", org, parent: nested_parent_group)
[reporter_group, security_group, developer_group, maintainer_group, nested_parent_group].each { |group| enable_knowledge_graph!(group) }

reporter_project = create_project!("#{suffix}-reporter-project", reporter_group, admin, org)
security_project = create_project!("#{suffix}-security-project", security_group, admin, org)
developer_project = create_project!("#{suffix}-developer-project", developer_group, admin, org)
maintainer_project = create_project!("#{suffix}-maintainer-project", maintainer_group, admin, org)
nested_reporter_project = create_project!("#{suffix}-nested-reporter-project", nested_reporter_group, admin, org)
nested_developer_project = create_project!("#{suffix}-nested-developer-project", nested_developer_group, admin, org)

reporter_group.add_reporter(victim)
authorized_member =
  if Gitlab::Security::SecurityManagerConfig.enabled?
    security_group.add_security_manager(victim)
  else
    security_group.add_developer(victim)
  end

unless authorized_member&.persisted?
  raise "Failed to add authorized member: #{authorized_member&.errors&.full_messages&.join(', ')}"
end
developer_group.add_developer(victim)
maintainer_group.add_maintainer(victim)
nested_parent_group.add_reporter(victim)
nested_developer_group.add_developer(victim)

reporter_vulnerability = create_vulnerability!(
  reporter_project,
  admin,
  title: "#{suffix} reporter-only SQLi",
  severity: :critical,
  report_type: :generic,
  created_at: Time.zone.parse('2026-03-24 12:00:00 UTC')
)
security_vulnerability = create_vulnerability!(
  security_project,
  admin,
  title: "#{suffix} security-manager XSS",
  severity: :high,
  report_type: :generic,
  created_at: Time.zone.parse('2026-03-25 12:00:00 UTC')
)
developer_vulnerability = create_vulnerability!(
  developer_project,
  admin,
  title: "#{suffix} developer XSS",
  severity: :high,
  report_type: :generic,
  created_at: Time.zone.parse('2026-03-26 12:00:00 UTC')
)
maintainer_vulnerability = create_vulnerability!(
  maintainer_project,
  admin,
  title: "#{suffix} maintainer XSS",
  severity: :high,
  report_type: :generic,
  created_at: Time.zone.parse('2026-03-27 12:00:00 UTC')
)
nested_reporter_vulnerability = create_vulnerability!(
  nested_reporter_project,
  admin,
  title: "#{suffix} nested reporter-only SQLi",
  severity: :critical,
  report_type: :generic,
  created_at: Time.zone.parse('2026-03-28 12:00:00 UTC')
)
nested_developer_vulnerability = create_vulnerability!(
  nested_developer_project,
  admin,
  title: "#{suffix} nested developer XSS",
  severity: :high,
  report_type: :generic,
  created_at: Time.zone.parse('2026-03-29 12:00:00 UTC')
)

pat_attrs = {
  user: victim,
  name: "#{suffix}-pat",
  scopes: %w[api read_api],
  expires_at: 30.days.from_now
}
pat_attrs[:organization] = org if PersonalAccessToken.reflect_on_association(:organization)
pat = PersonalAccessToken.create!(pat_attrs)

puts "ROLE_SCOPED_AUTHZ_FIXTURE_JSON=#{JSON.generate(
  username: victim.username,
  token: pat.token,
  reporter_group_id: reporter_group.id,
  security_group_id: security_group.id,
  developer_group_id: developer_group.id,
  maintainer_group_id: maintainer_group.id,
  nested_parent_group_id: nested_parent_group.id,
  nested_reporter_group_id: nested_reporter_group.id,
  nested_developer_group_id: nested_developer_group.id,
  reporter_project_id: reporter_project.id,
  security_project_id: security_project.id,
  developer_project_id: developer_project.id,
  maintainer_project_id: maintainer_project.id,
  nested_reporter_project_id: nested_reporter_project.id,
  nested_developer_project_id: nested_developer_project.id,
  reporter_vulnerability_id: reporter_vulnerability.id,
  security_vulnerability_id: security_vulnerability.id,
  developer_vulnerability_id: developer_vulnerability.id,
  maintainer_vulnerability_id: maintainer_vulnerability.id,
  nested_reporter_vulnerability_id: nested_reporter_vulnerability.id,
  nested_developer_vulnerability_id: nested_developer_vulnerability.id,
  authorized_access_level: authorized_member.access_level,
  reporter_vulnerability_title: reporter_vulnerability.title,
  security_vulnerability_title: security_vulnerability.title,
  developer_vulnerability_title: developer_vulnerability.title,
  maintainer_vulnerability_title: maintainer_vulnerability.title,
  nested_reporter_vulnerability_title: nested_reporter_vulnerability.title,
  nested_developer_vulnerability_title: nested_developer_vulnerability.title,
  reporter_vulnerability_created_at: reporter_vulnerability.created_at.iso8601,
  security_vulnerability_created_at: security_vulnerability.created_at.iso8601,
  developer_vulnerability_created_at: developer_vulnerability.created_at.iso8601,
  maintainer_vulnerability_created_at: maintainer_vulnerability.created_at.iso8601,
  nested_reporter_vulnerability_created_at: nested_reporter_vulnerability.created_at.iso8601,
  nested_developer_vulnerability_created_at: nested_developer_vulnerability.created_at.iso8601
)}"
