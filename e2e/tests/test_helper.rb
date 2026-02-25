# frozen_string_literal: true

# =============================================================================
# E2E Test Helper — shared test harness and manifest loader
# =============================================================================
#
# Loaded by redaction_test.rb.
# Provides:
#   - Manifest loading (JSON with dynamic IDs from create_test_data.rb)
#   - Test runner module (PASS/FAIL tracking, range assertions, JSON report)
#   - gRPC client + query helper
#
# The manifest is written by create_test_data.rb to /tmp/e2e/manifest.json.
#
# All human-readable output is handled by the Rust harness — Ruby only
# writes a structured JSON report to RESULTS_PATH.
# =============================================================================

require 'json'

E2E_POD_DIR = ENV.fetch('E2E_POD_DIR', '/tmp/e2e')
MANIFEST_PATH = "#{E2E_POD_DIR}/manifest.json"

def load_manifest!
  unless File.exist?(MANIFEST_PATH)
    abort <<~MSG
      ERROR: Manifest not found at #{MANIFEST_PATH}
      Run create_test_data.rb first to generate test data and the manifest.
    MSG
  end
  JSON.parse(File.read(MANIFEST_PATH), symbolize_names: true)
end

# Shared test harness — tracks results and writes a JSON report.
# All human-readable output is rendered by the Rust caller.
module TestHarness
  RESULTS = []

  RESULTS_PATH = "#{E2E_POD_DIR}/test-results.json"

  @current_section = nil

  def self.run(name, expected_min:, expected_max: nil, &block)
    result = block.call
    rows   = result[:result].is_a?(Array) ? result[:result] : []
    count  = rows.size
    ok     = count >= expected_min && (expected_max.nil? || count <= expected_max)

    entry = {
      name: name,
      section: @current_section,
      actual: count,
      expected_min: expected_min,
      expected_max: expected_max,
      status: ok ? 'pass' : 'fail'
    }
    entry[:error] = "got #{count}, expected #{expected_max ? "#{expected_min}-#{expected_max}" : ">=#{expected_min}"}" unless ok
    RESULTS << entry
  rescue StandardError => e
    RESULTS << { name: name, section: @current_section, status: 'error', error: e.message[0..300] }
  end

  def self.section(title)
    @current_section = title
  end

  def self.summary
    passed = RESULTS.count { |r| r[:status] == 'pass' }
    failed = RESULTS.size - passed
    total  = RESULTS.size

    report = {
      passed: passed,
      failed: failed,
      total: total,
      results: RESULTS,
      failures: RESULTS.select { |r| r[:status] != 'pass' }
    }

    File.write(RESULTS_PATH, JSON.pretty_generate(report))
    exit(1) if failed > 0
  end
end

def q(client, user, org_id, query_json)
  client.execute_query(query_json: query_json, user: user, organization_id: org_id)
end
