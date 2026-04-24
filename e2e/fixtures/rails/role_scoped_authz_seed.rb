require 'digest/sha1'
require 'json'
require 'securerandom'

org = Organizations::Organization.first
admin = User.find_by(username: 'root') || User.admins.first
suffix = "kg347e2e#{SecureRandom.hex(4)}"

raise 'No organization found' unless org
raise 'No admin user found' unless admin

def create_group!(name, org)
  Group.create!(name: name, path: name, organization: org)
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
[reporter_group, security_group].each { |group| enable_knowledge_graph!(group) }

reporter_project = create_project!("#{suffix}-reporter-project", reporter_group, admin, org)
security_project = create_project!("#{suffix}-security-project", security_group, admin, org)

reporter_group.add_reporter(victim)
security_group.add_member(victim, Gitlab::Access::SECURITY_MANAGER)

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
  reporter_project_id: reporter_project.id,
  security_project_id: security_project.id,
  reporter_vulnerability_id: reporter_vulnerability.id,
  security_vulnerability_id: security_vulnerability.id,
  reporter_vulnerability_title: reporter_vulnerability.title,
  security_vulnerability_title: security_vulnerability.title,
  reporter_vulnerability_created_at: reporter_vulnerability.created_at.iso8601,
  security_vulnerability_created_at: security_vulnerability.created_at.iso8601
)}"
