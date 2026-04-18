require_relative '../models/user'

module AuthenticationService
  class CredentialsChecker
    def initialize(user, password)
      @user = user
      @password = password
    end

    # Instance method within nested class
    def valid_password?
      # Real check would go here
      puts "Checking password for user: #{@user.email}"
      @password == "password123"
    end
  end

  # Module function (like a class method on the module)
  def self.authenticate(email, password)
    user = User.find_by_email(email) # Qualified call to User class method
    return nil unless user

    checker = CredentialsChecker.new(user, password) # Instantiation of nested class

    if checker.valid_password? # Call to instance method on checker
      puts "Authentication successful for #{user.email}"
      audit_logger = AuthenticationService::Utilities::Audit.new(user)
      audit_logger.log_event("Authentication successful for #{user.email}")
      user
    else
      puts "Authentication failed for #{user.email}"
      nil
    end
  end

  # Nested Module
  module Utilities
    # Nested Class within Nested Module
    class Audit
      def self.log_event(message)
        puts "[AUDIT][#{Time.now}] #{message}"
      end
    end
  end
end 
