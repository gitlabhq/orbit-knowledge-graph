module WeatherApp
  class UnknownCityError < StandardError; end

  class WeatherService
    SAMPLES = {
      "berlin"    => { temperature_c: 14.5, condition: "cloudy",       humidity: 72 },
      "tokyo"     => { temperature_c: 21.0, condition: "clear",        humidity: 60 },
      "san francisco" => { temperature_c: 17.2, condition: "foggy",    humidity: 80 },
      "new york"  => { temperature_c: 9.8,  condition: "rain",         humidity: 88 },
      "kyiv"      => { temperature_c: 11.4, condition: "partly cloudy", humidity: 65 }
    }.freeze

    def initialize(samples: SAMPLES)
      @samples = samples
    end

    def fetch(city)
      key = city.to_s.downcase.strip
      data = @samples[key] or raise UnknownCityError, "no sample data for #{city.inspect}"
      Forecast.new(city: city, **data)
    end

    def known_cities
      @samples.keys.map { |k| k.split.map(&:capitalize).join(" ") }
    end
  end
end
