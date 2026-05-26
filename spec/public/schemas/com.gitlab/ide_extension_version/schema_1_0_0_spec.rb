# frozen_string_literal: true

require 'spec_helper'

describe 'public/schemas/com.gitlab/ide_extension_version/jsonschema/1-0-0' do

  context 'all fields populated' do
    it 'should be valid' do
      data = {
        ide_name: 'RubyMine',
        ide_vendor: 'Jetbrains',
        ide_version: '2.0.1',
        extension_name: 'GitLab Workflow',
        extension_version: '1.0.2'
      }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end
  end

  context 'all fields present but not populated' do
    it 'should be valid' do
      data = {
        ide_name: nil,
        ide_vendor: nil,
        ide_version: nil,
        extension_name: nil,
        extension_version: nil
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
