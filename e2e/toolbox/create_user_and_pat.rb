# Creates an admin e2e-bot user and prints a fresh PAT to stdout.
# Expects E2E_BOT_PASS env var.
# Usage: gitlab-rails runner /path/to/create_user_and_pat.rb

password = ENV.fetch("E2E_BOT_PASS")

admin = User.find_by(admin: true)
user = User.find_by(username: "e2e-bot")

unless user
  result = Users::CreateService.new(admin, {
    username: "e2e-bot",
    email: "e2e-bot@example.com",
    name: "E2E Bot",
    password: password,
    password_confirmation: password,
    skip_confirmation: true,
    admin: true,
    organization_id: 1
  }).execute
  user = result[:user] if result[:status] != :error
end

abort("Failed to create or find e2e-bot user") unless user

user.personal_access_tokens.where(name: "e2e-pat").destroy_all
token = user.personal_access_tokens.create!(
  name: "e2e-pat",
  scopes: %w[api read_api],
  expires_at: 30.days.from_now
)
print token.token
