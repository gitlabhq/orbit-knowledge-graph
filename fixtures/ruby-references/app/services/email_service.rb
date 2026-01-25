# Service class demonstrating static method calls and cross-class references
class EmailService
  def self.send_welcome(user)
    template = TemplateEngine.load("welcome_email")
    content = template.render(user: user)
    
    mailer = MailerFactory.create_mailer
    mailer.send_email(
      to: user.email,
      subject: "Welcome to our platform!",
      body: content
    )
  end

  def self.send_reset_password(user)
    token = TokenGenerator.generate_reset_token(user)
    template = TemplateEngine.load("password_reset")
    
    content = template.render(
      user: user,
      reset_url: build_reset_url(token)
    )
    
    mailer = MailerFactory.create_mailer
    mailer.send_email(
      to: user.email,
      subject: "Password Reset Request",
      body: content
    )
  end

  def self.send_notification(user, message)
    Logger.info("Sending notification to #{user.email}: #{message}")
    
    template = TemplateEngine.load("notification")
    content = template.render(user: user, message: message)
    
    delivery_service = DeliveryService.new
    delivery_service.send_immediate(user.email, content)
  end

  private

  def self.build_reset_url(token)
    "#{ConfigService.base_url}/reset_password?token=#{token}"
  end
end
