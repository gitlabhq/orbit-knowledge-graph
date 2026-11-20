# Authentication module reopening - adding token management
module Authentication
  class Token
    attr_reader :value, :expires_at, :user_id

    def initialize(user_id, expires_in = SESSION_TIMEOUT)
      @user_id = user_id
      @value = generate_token
      @expires_at = Time.now + expires_in
    end

    def expired?
      Time.now > @expires_at
    end

    def refresh(extends_by = SESSION_TIMEOUT)
      @expires_at = Time.now + extends_by
    end

    private

    def generate_token
      SecureRandom.hex(32)
    end
  end

  class RefreshToken < Token
    def initialize(user_id, expires_in = 7.days)
      super(user_id, expires_in)
    end
  end

  # Add token management methods to main module  
  def self.create_session(user_id)
    token = Token.new(user_id)
    refresh_token = RefreshToken.new(user_id)
    
    {
      access_token: token,
      refresh_token: refresh_token
    }
  end

  def self.validate_token(token_value)
    # Token validation logic
    stored_token = find_token(token_value)
    return nil unless stored_token
    return nil if stored_token.expired?
    
    stored_token
  end

  def self.revoke_token(token_value)
    # Token revocation logic
    remove_token(token_value)
  end

  private

  def self.find_token(value)
    # Token lookup logic - in real app would use database
    @tokens ||= {}
    @tokens[value]
  end

  def self.remove_token(value)
    @tokens ||= {}
    @tokens.delete(value)
  end
end 
