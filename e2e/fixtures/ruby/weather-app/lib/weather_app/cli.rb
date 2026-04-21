require "optparse"

module WeatherApp
  class CLI
    def self.run(argv, stdout: $stdout, stderr: $stderr)
      new.run(argv, stdout: stdout, stderr: stderr)
    end

    def run(argv, stdout:, stderr:)
      options = parse(argv)
      if options[:list]
        stdout.puts WeatherService.new.known_cities.join("\n")
        return 0
      end

      city = options[:city] or return usage_error("--city is required", stderr)
      forecast = WeatherService.new.fetch(city)
      stdout.puts Formatter.new(format: options[:format]).render(forecast)
      0
    rescue UnknownCityError => e
      stderr.puts "error: #{e.message}"
      2
    end

    private

    def parse(argv)
      options = { format: :text, list: false }
      OptionParser.new do |o|
        o.banner = "Usage: weather --city CITY [--format text|json]"
        o.on("-c", "--city CITY", "City name") { |v| options[:city] = v }
        o.on("-f", "--format FMT", FORMATS_SYM, "Output format (text|json)") { |v| options[:format] = v }
        o.on("-l", "--list", "List supported cities") { options[:list] = true }
        o.on("-h", "--help") { puts o; exit 0 }
      end.parse!(argv)
      options
    end

    FORMATS_SYM = Formatter::FORMATS

    def usage_error(msg, stderr)
      stderr.puts "error: #{msg}"
      1
    end
  end
end
