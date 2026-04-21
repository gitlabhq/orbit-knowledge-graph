module WeatherApp
  class Forecast
    attr_reader :city, :temperature_c, :condition, :humidity

    def initialize(city:, temperature_c:, condition:, humidity:)
      @city = city
      @temperature_c = temperature_c
      @condition = condition
      @humidity = humidity
    end

    def temperature_f
      (@temperature_c * 9.0 / 5.0) + 32.0
    end

    def to_h
      { city: city, temperature_c: temperature_c, temperature_f: temperature_f,
        condition: condition, humidity: humidity }
    end
  end
end
