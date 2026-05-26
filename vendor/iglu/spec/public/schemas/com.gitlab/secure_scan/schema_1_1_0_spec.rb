# frozen_string_literal: true

require 'spec_helper'

describe 'public/schemas/com.gitlab/secure_scan/jsonschema/1-1-0' do

  context 'all fields populated' do
    it 'should be valid' do
      data = {
        analyzer: 'gitlab-dast',
        analyzer_vendor: 'GitLab',
        analyzer_version: '2.0.1',
        end_time: '2021-06-11T07:27:50',
        findings_count: 42,
        scan_type: 'dast',
        scanner: 'zaproxy-browserker',
        scanner_vendor: 'GitLab',
        scanner_version: 'D-2020-08-26',
        start_time: '2021-06-11T07:26:17',
        status: 'success',
        report_schema_version: '14.0.2'
      }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end
  end

  context 'analyzer has no value' do
    it 'should be valid' do
      data = {
        analyzer: nil,
        analyzer_vendor: nil,
        analyzer_version: nil,
        end_time: '2021-06-11T07:27:50',
        scan_type: 'dast',
        scanner: 'zaproxy-browserker',
        scanner_vendor: 'GitLab',
        scanner_version: 'D-2020-08-26',
        start_time: '2021-06-11T07:26:17',
        status: 'success',
        report_schema_version: '14.0.2'
      }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end
  end

  context 'scanner has no value' do
    it 'should be valid' do
      data = {
        analyzer: 'gitlab-dast',
        analyzer_vendor: 'GitLab',
        analyzer_version: '2.0.1',
        end_time: '2021-06-11T07:27:50',
        scan_type: 'dast',
        scanner: nil,
        scanner_vendor: nil,
        scanner_version: nil,
        start_time: '2021-06-11T07:26:17',
        status: 'success',
        report_schema_version: '14.0.2'
      }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end
  end

  context 'missing fields' do
    it 'should be invalid' do
      errors = validate_schema(self.class.top_level_description, {})
      expect(errors).not_to be_empty
    end
  end
end
