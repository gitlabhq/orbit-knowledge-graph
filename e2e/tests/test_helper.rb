# frozen_string_literal: true

# =============================================================================
# E2E Test Helper — shared test harness and manifest loader
# =============================================================================
#
# Loaded by redaction_test.rb.
# Provides:
#   - Manifest loading (JSON with dynamic IDs from create_test_data.rb)
#   - Test runner module (PASS/FAIL tracking, range assertions, summary)
#   - gRPC client + query helper
#
# The manifest is written by create_test_data.rb to /tmp/e2e/manifest.json.
# =============================================================================

require 'json'

MANIFEST_PATH = '/tmp/e2e/manifest.json'

def load_manifest!
  unless File.exist?(MANIFEST_PATH)
    abort <<~MSG
      ERROR: Manifest not found at #{MANIFEST_PATH}
      Run create_test_data.rb first to generate test data and the manifest.
    MSG
  end
  manifest = JSON.parse(File.read(MANIFEST_PATH), symbolize_names: true)
  puts "  Loaded manifest from #{MANIFEST_PATH}"
  puts "    Users:    #{manifest[:users].keys.join(', ')}"
  puts "    Groups:   #{manifest[:groups].keys.join(', ')}"
  puts "    Projects: #{manifest[:projects].keys.join(', ')}"
  manifest
end

# Shared test harness — tracks PASS/FAIL, runs assertions, prints summary.
module TestHarness
  PASS = []
  FAIL = []

  def self.run(name, expected_min:, expected_max: nil, &block)
    result = block.call
    rows   = result[:result].is_a?(Array) ? result[:result] : []
    count  = rows.size
    ok     = count >= expected_min && (expected_max.nil? || count <= expected_max)
    range  = expected_max ? "#{expected_min}-#{expected_max}" : ">=#{expected_min}"
    if ok
      puts "  PASS  #{name} (#{count})"
      PASS << name
    else
      puts "  FAIL  #{name} -- got #{count}, expected #{range}"
      FAIL << name
    end
  rescue StandardError => e
    puts "  FAIL  #{name} -- ERROR: #{e.message[0..150]}"
    FAIL << name
  end

  def self.section(title)
    puts "\n--- #{title} ---"
  end

  def self.summary
    total = PASS.size + FAIL.size
    puts "\n#{'=' * 60}"
    puts "  RESULT: #{PASS.size}/#{total} passed"
    puts '=' * 60
    if FAIL.any?
      puts "\nFAILED:"
      FAIL.each { |f| puts "  * #{f}" }
    end
    puts
    exit(1) if FAIL.any?
  end
end

def q(client, user, org_id, query_json)
  client.execute_query(query_json: query_json, user: user, organization_id: org_id)
end
