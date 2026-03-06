# Comprehensive Ruby definitions fixture
# This file tests all supported definition types in realistic scenarios

module DataProcessing
  # Constants with various types
  VERSION = "1.0.0"
  CONFIG = { timeout: 30, retries: 3 }

  # Lambda definitions (assigned to constants and variables)
  VALIDATOR = lambda do |input|
    input.is_a?(Hash) && input[:type].present?
  end

  TRANSFORMER = lambda { |data| data.transform_keys(&:to_sym) }

  class Processor
    # Attribute declarations
    attr_reader :name, :status
    attr_writer :config
    attr_accessor :timeout, :retries

    # Attribute with string (dynamic attribute)
    attr_reader "computed_field"

    # Constants within class
    DEFAULT_TIMEOUT = 30
    PRIORITY_LEVELS = %w[low medium high critical].freeze

    def initialize(name)
      @name = name
      @status = :ready
      @config = {}
      
      # Lambda assigned to instance variable
      @formatter = lambda { |data| JSON.pretty_generate(data) }
      
      # Proc assigned to instance variable
      @error_handler = Proc.new do |error|
        puts "Error in #{@name}: #{error.message}"
      end
    end

    # Instance methods
    def process(data)
      return false unless VALIDATOR.call(data)
      
      # Block with parameters
      data.each_with_index do |item, index|
        yield item, index if block_given?
      end
      
      @status = :complete
      true
    end

    def handle_batch(items)
      # Nested block within method
      items.map.with_index do |item, index|
        transform_item(item, index)
      end
    end

    # Class methods (singleton methods)
    def self.create_default
      new("default_processor")
    end

    def self.bulk_process(items)
      processor = create_default
      processor.handle_batch(items)
    end

    private

    def transform_item(item, index)
      TRANSFORMER.call(item.merge(index: index))
    end
  end

  # Module with lambda assigned to variable
  module Utilities
    # Lambda assigned to module variable  
    logger = lambda do |level, message|
      puts "[#{level.upcase}] #{Time.now}: #{message}"
    end

    # Class variable with Proc
    @@cache_cleaner = Proc.new { |key| Rails.cache.delete(key) }

    # Singleton method
    def self.log_info(message)
      logger.call(:info, message)
    end

    # Nested class with attributes
    class Cache
      attr_accessor :store, :ttl
      attr_reader :hits, :misses

      CACHE_STRATEGIES = {
        memory: Proc.new { |key, value| @memory_store[key] = value },
        redis: Proc.new { |key, value| Redis.current.set(key, value) }
      }.freeze

      def initialize
        @store = {}
        @hits = 0
        @misses = 0
        
        # Complex lambda with block parameter
        @hit_tracker = lambda do |operation|
          case operation
          when :hit
            @hits += 1
          when :miss  
            @misses += 1
          end
        end
      end

      def get(key, &default_block)
        if @store.key?(key)
          @hit_tracker.call(:hit)
          @store[key]
        else
          @hit_tracker.call(:miss)
          default_block&.call
        end
      end
    end
  end

  # Global variable with lambda (at module level)
  $global_processor = lambda { |data| DataProcessing::Processor.new("global").process(data) }
end

# Top-level class with comprehensive attribute usage
class ConfigurationManager
  # Multiple attribute types
  attr_reader :environment, :version
  attr_writer :debug_mode
  attr_accessor :log_level, :max_connections

  # Attributes with multiple symbols in one declaration
  attr_reader :created_at, :updated_at, :last_accessed

  # Constants with Proc assignments
  ENV_DEFAULTS = {
    development: Proc.new { { debug: true, log_level: :debug } },
    production: Proc.new { { debug: false, log_level: :warn } }
  }.freeze

  # Lambda for validation
  CONFIG_VALIDATOR = lambda do |config|
    required_keys = %i[environment version log_level]
    required_keys.all? { |key| config.key?(key) }
  end

  def initialize(env = :development)
    @environment = env
    @version = "1.0"
    @log_level = :info
    @created_at = Time.now
    
    # Instance method with block
    configure_defaults do |config|
      config[:timeout] = 30
      config[:retries] = 3
    end
  end

  def configure_defaults(&block)
    config = ENV_DEFAULTS[@environment].call
    block.call(config) if block_given?
    @config = config
  end

  # Singleton method
  def self.load_from_file(path)
    # Implementation would load from file
    new
  end
end

# Standalone Proc (should be captured)
error_reporter = Proc.new do |exception, context|
  puts "Error: #{exception.message} in #{context}"
end 
