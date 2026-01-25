# User management module
module UserManagement
  class User
    attr_reader :id, :username, :email, :created_at
    attr_accessor :active

    def initialize(username:, email:, password:)
      @id = generate_id
      @username = username
      @email = email
      @password_hash = hash_password(password)
      @created_at = Time.now
      @active = true
    end

    def self.find_by_username(username)
      # Database lookup simulation
      users_db.find { |user| user.username == username }
    end

    def self.find_by_email(email)
      users_db.find { |user| user.email == email }
    end

    def self.create(username:, email:, password:)
      user = new(username: username, email: email, password: password)
      users_db << user
      user
    end

    def verify_password(password)
      hash_password(password) == @password_hash
    end

    def update_password(new_password)
      @password_hash = hash_password(new_password)
    end

    def deactivate!
      @active = false
    end

    def activate!
      @active = true
    end

    def to_h
      {
        id: @id,
        username: @username,
        email: @email,
        active: @active,
        created_at: @created_at
      }
    end

    private

    def generate_id
      SecureRandom.uuid
    end

    def hash_password(password)
      # Simple hash simulation - in real app would use bcrypt
      Digest::SHA256.hexdigest("#{password}#{@username}")
    end

    def self.users_db
      @users_db ||= []
    end
  end

  class UserRepository
    def self.all_users
      User.users_db
    end

    def self.active_users
      all_users.select(&:active)
    end

    def self.inactive_users
      all_users.reject(&:active)
    end

    def self.count
      all_users.length
    end
  end

  # Module methods
  def self.create_user(username:, email:, password:)
    # Validation
    raise ArgumentError, "Username cannot be empty" if username.nil? || username.strip.empty?
    raise ArgumentError, "Email cannot be empty" if email.nil? || email.strip.empty?
    raise ArgumentError, "Password must be at least 8 characters" if password.length < 8

    # Check for existing user
    existing_user = User.find_by_username(username) || User.find_by_email(email)
    raise ArgumentError, "User already exists" if existing_user

    User.create(username: username, email: email, password: password)
  end

  def self.authenticate(username, password)
    user = User.find_by_username(username)
    return nil unless user&.active
    return user if user.verify_password(password)
    nil
  end
end 
