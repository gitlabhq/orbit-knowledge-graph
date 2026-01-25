# User model that extends BaseModel
require_relative 'base_model'

class UserModel < BaseModel
  attr_accessor :username, :email, :first_name, :last_name, :active

  def initialize(attributes = {})
    super(attributes)
    @username = attributes[:username]
    @email = attributes[:email]
    @first_name = attributes[:first_name]
    @last_name = attributes[:last_name]
    @active = attributes.fetch(:active, true)
  end

  def self.find_by_username(username)
    storage.find { |user| user.username == username }
  end

  def self.find_by_email(email)
    storage.find { |user| user.email == email }
  end

  def self.active_users
    where(active: true)
  end

  def self.inactive_users
    where(active: false)
  end

  def full_name
    "#{@first_name} #{@last_name}".strip
  end

  def display_name
    full_name.empty? ? @username : full_name
  end

  def activate!
    update(active: true)
  end

  def deactivate!
    update(active: false)
  end

  def change_email(new_email)
    raise ArgumentError, "Email already taken" if self.class.find_by_email(new_email)
    update(email: new_email)
  end

  def change_username(new_username)
    raise ArgumentError, "Username already taken" if self.class.find_by_username(new_username)
    update(username: new_username)
  end

  def to_h
    super.merge({
      username: @username,
      email: @email,
      first_name: @first_name,
      last_name: @last_name,
      active: @active,
      full_name: full_name,
      display_name: display_name
    })
  end

  # Validation methods
  def valid?
    validate_username && validate_email
  end

  def errors
    @errors ||= []
  end

  private

  def validate_username
    if @username.nil? || @username.strip.empty?
      errors << "Username cannot be empty"
      return false
    end
    
    if @username.length < 3
      errors << "Username must be at least 3 characters"
      return false
    end
    
    true
  end

  def validate_email
    if @email.nil? || @email.strip.empty?
      errors << "Email cannot be empty"
      return false
    end
    
    unless @email.match?(/\A[\w+\-.]+@[a-z\d\-]+(\.[a-z\d\-]+)*\.[a-z]+\z/i)
      errors << "Email format is invalid"
      return false
    end
    
    true
  end
end 
