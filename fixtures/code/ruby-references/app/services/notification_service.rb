# Notification service showing method chaining and complex references
class NotificationService
  def self.notify(user, message, options = {})
    notification = build_notification(user, message, options)
    delivery_method = determine_delivery_method(user, options)
    
    case delivery_method
    when :email
      EmailService.send_notification(user, message)
    when :sms
      SmsService.send_notification(user.phone, message)
    when :push
      PushService.send_to_device(user.device_token, message)
    end
    
    log_notification(notification)
  end

  def self.notify_all(users, message)
    users.each do |user|
      notify(user, message)
    end
  end

  def self.send_batch_notifications(notification_batch)
    notification_batch.each do |item|
      user = User.find(item[:user_id])
      notify(user, item[:message], item[:options] || {})
    end
  end

  private

  def self.build_notification(user, message, options)
    {
      user_id: user.id,
      message: message,
      created_at: Time.now,
      delivery_method: determine_delivery_method(user, options),
      priority: options[:priority] || :normal
    }
  end

  def self.determine_delivery_method(user, options)
    return options[:method] if options[:method]
    
    preferences = user.get_notification_preferences
    preferences.preferred_method || :email
  end

  def self.log_notification(notification)
    Logger.info("Notification sent: #{notification.inspect}")
    Analytics.track_event("notification_sent", notification)
  end
end
