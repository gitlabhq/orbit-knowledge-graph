# Seed data for e2e testing: EE license, feature flags, Cloud Connector key,
# Duo namespace, and knowledge graph configuration.
#
# Run via: RAILS_ENV=development bundle exec rails runner seed-data.rb

puts "--- Installing EE Ultimate license ---"
key = OpenSSL::PKey::RSA.generate(2048)
Gitlab::License.encryption_key = key

license = Gitlab::License.new
license.licensee = { "Name" => "E2E Test", "Email" => "e2e@gdk.test", "Company" => "GitLab" }
license.starts_at = Date.today - 30
license.expires_at = Date.today + 365
license.notify_admins_at = Date.today + 365
license.notify_users_at = Date.today + 365
license.block_changes_at = Date.today + 365 + 14
license.restrictions = {
  plan: "ultimate",
  active_user_count: 10000,
  add_ons: { "GitLab_FileLocks" => 1, "GitLab_Auditor_User" => 1 },
  subscription_id: "e2e-test-001"
}

exported = license.export
File.write(Rails.root.join(".license_encryption_key.pub"), key.public_key.to_pem)
Gitlab::License.fallback_decryption_keys = [key.public_key]
lic = License.new(data: exported)
lic.save!
puts "License installed: #{lic.plan}, expires: #{lic.expires_at}"

puts "--- Creating Cloud Connector key ---"
key_pair = OpenSSL::PKey::RSA.generate(2048)
cc_key = CloudConnector::Keys.first_or_create! do |k|
  k.secret_key = key_pair.to_pem
end
cc_key.update!(secret_key: key_pair.to_pem) unless cc_key.secret_key.present?
puts "Cloud Connector key: id=#{cc_key.id}"

puts "--- Enabling feature flags ---"
Feature.enable(:knowledge_graph)
Feature.enable(:knowledge_graph_infra)
puts "knowledge_graph: #{Feature.enabled?(:knowledge_graph)}"

puts "--- Duo setup skipped (not needed for GKG e2e) ---"

puts "--- Setting AI Gateway URL ---"
begin
  setting = Ai::Setting.safe_find_or_create_by!(singleton: true)
  setting.update!(ai_gateway_url: "http://gdk.test:5052")
  puts "AI Gateway URL set"
rescue => e
  puts "AI Gateway URL skipped: #{e.message}"
end

puts "--- Enabling knowledge graph namespaces ---"
Namespace.where(type: "Group", parent_id: nil).each do |ns|
  ActiveRecord::Base.connection.execute(
    "INSERT INTO knowledge_graph_enabled_namespaces (root_namespace_id, created_at, updated_at) " \
    "VALUES (#{ns.id}, NOW(), NOW()) ON CONFLICT DO NOTHING"
  )
end
puts "Knowledge graph namespaces enabled"

puts "--- Creating PAT for e2e tests ---"
root = User.find_by(username: "root")
token = root.personal_access_tokens.create!(
  name: "e2e-test-token",
  scopes: [:api, :read_api, :read_user, :read_repository],
  expires_at: 1.year.from_now
)
File.write("/home/gdk/.gdk_token", token.token)
puts "PAT created and saved to ~/.gdk_token"

puts "--- Seed complete ---"
