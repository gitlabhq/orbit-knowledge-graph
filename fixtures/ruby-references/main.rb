#!/usr/bin/env ruby

# Main application file demonstrating complex cross-file references
require_relative 'app/models/user'
require_relative 'app/models/profile'
require_relative 'app/services/email_service'
require_relative 'app/services/notification_service'
require_relative 'app/controllers/users_controller'
require_relative 'lib/auth_service'

class Application
  def initialize
    @users = []
    setup_services
  end

  def run
    puts "Starting Ruby References Test Application"
    
    # Demonstrate complex reference chains
    test_user_creation_flow
    test_authentication_flow
    test_notification_flow
    test_controller_actions
    test_method_chaining
    
    puts "Application completed successfully!"
  end

  private

  def setup_services
    # Configure services (would normally be in initializers)
    EmailService.configure if EmailService.respond_to?(:configure)
    NotificationService.setup if NotificationService.respond_to?(:setup)
  end

  def test_user_creation_flow
    puts "\n=== Testing User Creation Flow ==="
    
    # Complex chained calls
    user = User.create_with_profile(
      {
        username: 'testuser',
        email: 'test@example.com',
        first_name: 'Test',
        last_name: 'User'
      },
      {
        bio: 'Test user profile',
        avatar_url: 'https://example.com/avatar.jpg'
      }
    )
    
    # Method calls on returned objects
    full_name = user.full_name
    profile = user.get_profile
    profile_data = profile.full_profile_data if profile
    
    @users << user
    puts "Created user: #{full_name}"
  end

  def test_authentication_flow
    puts "\n=== Testing Authentication Flow ==="
    
    user = @users.first
    return unless user
    
    # Method chaining through service classes
    session = AuthService.create_session(user)
    token = session.access_token
    
    # Token validation chain
    authenticated_user = AuthService.authenticate_token(token)
    puts "Authentication successful: #{authenticated_user&.email}"
    
    # Session refresh
    refreshed_session = AuthService.refresh_session(session.refresh_token)
    puts "Session refreshed: #{refreshed_session&.expires_at}"
  end

  def test_notification_flow
    puts "\n=== Testing Notification Flow ==="
    
    user = @users.first
    return unless user
    
    # Single notification
    NotificationService.notify(user, "Welcome to the platform!")
    
    # Batch notifications
    notification_batch = [
      { user_id: user.id, message: "First message", options: { priority: :high } },
      { user_id: user.id, message: "Second message", options: { method: :email } }
    ]
    
    NotificationService.send_batch_notifications(notification_batch)
    
    # Email service chain
    user.send_welcome_email
    EmailService.send_reset_password(user)
  end

  def test_controller_actions
    puts "\n=== Testing Controller Actions ==="
    
    controller = UsersController.new
    
    # Simulate controller actions (would normally have params/request objects)
    users_data = controller.index if controller.respond_to?(:index)
    
    user = @users.first
    if user
      show_data = controller.show if controller.respond_to?(:show)
      controller.activate if controller.respond_to?(:activate)
    end
  end

  def test_method_chaining
    puts "\n=== Testing Method Chaining ==="
    
    # Complex method chains
    active_users = User.all.select(&:active?)
    user_emails = active_users.map(&:email)
    
    # Chained service calls
    user = @users.first
    if user
      profile = user.get_profile
      summary = profile&.generate_summary
      
      # Chain through multiple services
      user.update_profile({ bio: "Updated bio" })
      user.activate!
    end
    
    # Class method chains
    new_user = User.find_by_email('new@example.com')
    new_user&.create_profile({ bio: "Auto-generated profile" })
    
    puts "Method chaining tests completed"
  end
end

# Utility class demonstrating static method calls
class TestUtilities
  def self.create_test_data
    users = []
    
    5.times do |i|
      user = User.create(
        username: "user#{i}",
        email: "user#{i}@example.com",
        first_name: "User",
        last_name: "#{i}"
      )
      
      Profile.create_default(user)
      users << user
    end
    
    users
  end

  def self.cleanup_test_data(users)
    users.each do |user|
      profile = user.get_profile
      profile&.destroy
      user.destroy
    end
  end

  def self.send_bulk_notifications(users, message)
    NotificationService.notify_all(users, message)
  end
end

# Run the application if this file is executed directly
if __FILE__ == $0
  app = Application.new
  app.run
  
  # Create and cleanup test data
  test_users = TestUtilities.create_test_data
  TestUtilities.send_bulk_notifications(test_users, "Bulk notification test")
  TestUtilities.cleanup_test_data(test_users)
end
