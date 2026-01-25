# Profile model showing belongs_to relationship and method calls
class Profile < ActiveRecord::Base
  belongs_to :user

  validates :bio, length: { maximum: 500 }

  def self.find_by_user_id(user_id)
    where(user_id: user_id).first
  end

  def self.create_default(user)
    create(
      user_id: user.id,
      bio: "New user",
      avatar_url: AvatarService.default_avatar_url
    )
  end

  def update_avatar(file)
    url = AvatarService.upload(file)
    update(avatar_url: url)
  end

  def full_profile_data
    user_data = user.to_h
    profile_data = to_h
    user_data.merge(profile_data)
  end

  def generate_summary
    TextProcessor.summarize(bio) if bio.present?
  end
end
