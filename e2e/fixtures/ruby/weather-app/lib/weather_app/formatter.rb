require "json"

module WeatherApp
  class Formatter
    FORMATS = %i[text json].freeze

    def initialize(format: :text)
      raise ArgumentError, "unknown format #{format}" unless FORMATS.include?(format)
      @format = format
    end

    def render(forecast)
      case @format
      when :text then render_text(forecast)
      when :json then JSON.generate(forecast.to_h)
      end
    end

    private

    def render_text(forecast)
      [
        "City:        #{forecast.city}",
        "Condition:   #{forecast.condition}",
        "Temperature: #{format('%.1f', forecast.temperature_c)} C / " \
          "#{format('%.1f', forecast.temperature_f)} F",
        "Humidity:    #{forecast.humidity}%"
      ].join("\n")
    end
  end
end
