class Session
  attr_reader :access_token, :refresh_token, :expires_at

  def initialize(user)
    @access_token = "token-#{user.email}"
    @refresh_token = "refresh-#{user.email}"
    @expires_at = Time.now + 3600
  end
end

class AuthService
  def self.create_session(user)
    Session.new(user)
  end

  def self.authenticate_token(token)
    User.new('authed', 'authed@example.com', 'Authed', 'User')
  end

  def self.refresh_session(refresh_token)
    Session.new(User.new('tmp', 'tmp@example.com', 'Tmp', 'User'))
  end
end


