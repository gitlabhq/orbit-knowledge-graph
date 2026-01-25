# User model demonstrating basic method calls and inheritance
class User < ActiveRecord::Base
  validates :email, presence: true
  validates :username, presence: true, uniqueness: true

  def initialize(attributes = {})
    super(attributes)
    @created_at = Time.now
  end

  def self.find_by_email(email)
    where(email: email).first
  end

  def self.create_with_profile(user_attrs, profile_attrs)
    user = create(user_attrs)
    user.create_profile(profile_attrs)
    user
  end

  def full_name
    "#{first_name} #{last_name}".strip
  end

  def create_profile(attributes)
    Profile.create(attributes.merge(user_id: id))
  end

  def update_profile(attributes)
    profile = get_profile
    profile.update(attributes) if profile
  end

  def get_profile
    Profile.find_by_user_id(id)
  end

  def send_welcome_email
    EmailService.send_welcome(self)
  end

  def activate!
    update(active: true)
    send_notification("User activated")
  end

  private

  def send_notification(message)
    NotificationService.notify(self, message)
  end
end
