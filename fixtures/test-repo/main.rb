#!/usr/bin/env ruby

# Main application file that demonstrates cross-file references
require_relative 'lib/authentication'
require_relative 'lib/authentication/providers'
require_relative 'lib/authentication/tokens'
require_relative 'lib/user_management'
require_relative 'app/models/user_model'

class Application
  def initialize
    @users = []
    setup_authentication
  end

  def run
    puts "Starting Knowledge Graph Test Application"
    
    # Demonstrate user creation
    create_sample_users
    
    # Demonstrate authentication
    test_authentication
    
    # Demonstrate token management
    test_token_management
    
    # Demonstrate providers
    test_authentication_providers
    
    puts "Application completed successfully!"
  end

  private

  def setup_authentication
    # Configure authentication providers
    Authentication.configure_provider(:ldap, {
      host: 'ldap.example.com',
      port: 389,
      base_dn: 'dc=example,dc=com'
    })

    Authentication.configure_provider(:oauth, {
      client_id: 'test_client_id',
      client_secret: 'test_client_secret'
    })
  end

  def create_sample_users
    puts "\n=== Creating Sample Users ==="
    
    # Create users using UserManagement module
    user1 = UserManagement.create_user(
      username: 'alice',
      email: 'alice@example.com',
      password: 'password123'
    )
    
    user2 = UserManagement.create_user(
      username: 'bob',
      email: 'bob@example.com',
      password: 'securepass456'
    )

    # Create users using UserModel class
    user_model1 = UserModel.create(
      username: 'charlie',
      email: 'charlie@example.com',
      first_name: 'Charlie',
      last_name: 'Brown'
    )

    user_model2 = UserModel.create(
      username: 'diana',
      email: 'diana@example.com',
      first_name: 'Diana',
      last_name: 'Prince',
      active: false
    )

    @users = [user1, user2, user_model1, user_model2]
    puts "Created #{@users.length} users"
  end

  def test_authentication
    puts "\n=== Testing Authentication ==="
    
    # Test basic authentication
    result = Authentication.authenticate_user('alice', 'password123')
    puts "Authentication result for alice: #{result}"
    
    # Test UserManagement authentication
    user = UserManagement.authenticate('bob', 'securepass456')
    puts "UserManagement auth for bob: #{user ? 'Success' : 'Failed'}"
    
    # Test failed authentication
    failed_result = Authentication.authenticate_user('alice', 'wrongpassword')
    puts "Failed authentication result: #{failed_result}"
  end

  def test_token_management
    puts "\n=== Testing Token Management ==="
    
    # Create session tokens
    session = Authentication.create_session('user123')
    puts "Created session with access token: #{session[:access_token].value[0..10]}..."
    puts "Refresh token expires at: #{session[:refresh_token].expires_at}"
    
    # Test token validation
    token_value = session[:access_token].value
    validated_token = Authentication.validate_token(token_value)
    puts "Token validation: #{validated_token ? 'Valid' : 'Invalid'}"
    
    # Test token expiration
    puts "Token expired? #{session[:access_token].expired?}"
    
    # Refresh token
    session[:access_token].refresh
    puts "Token refreshed, new expiry: #{session[:access_token].expires_at}"
  end

  def test_authentication_providers
    puts "\n=== Testing Authentication Providers ==="
    
    # Test LDAP provider
    ldap_provider = Authentication.get_provider(:ldap)
    if ldap_provider
      puts "LDAP provider configured: #{ldap_provider.class.name}"
      # In real app: ldap_result = ldap_provider.authenticate('user', 'pass')
    end
    
    # Test OAuth provider
    oauth_provider = Authentication.get_provider(:oauth)
    if oauth_provider
      puts "OAuth provider configured: #{oauth_provider.class.name}"
      # In real app: oauth_result = oauth_provider.authenticate('auth_code')
    end
    
    # Test provider error handling
    begin
      Authentication.configure_provider(:unknown, {})
    rescue Authentication::AuthenticationError => e
      puts "Expected error for unknown provider: #{e.message}"
    end
  end
end

# Utility module to demonstrate module methods
module ApplicationUtils
  def self.format_timestamp(time)
    time.strftime("%Y-%m-%d %H:%M:%S")
  end

  def self.generate_report(users)
    active_count = users.count { |u| u.respond_to?(:active) ? u.active : true }
    total_count = users.length
    
    {
      total_users: total_count,
      active_users: active_count,
      inactive_users: total_count - active_count,
      generated_at: format_timestamp(Time.now)
    }
  end
end

# Run the application if this file is executed directly
if __FILE__ == $0
  app = Application.new
  app.run
  
  # Generate a sample report
  puts "\n=== Application Report ==="
  report = ApplicationUtils.generate_report([])
  puts "Report generated at: #{report[:generated_at]}"
end 
