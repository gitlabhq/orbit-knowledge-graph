# frozen_string_literal: true

require 'spec_helper'

describe 'public/schemas/com.gitlab/code_suggestions_context/jsonschema/2-0-0' do

  context 'all fields populated' do
    it 'should be valid' do
      data = {
        language: 'Ruby',
        gitlab_realm: 'SaaS',
        model_engine: '2.0.1',
        model_name: 'GitLab Workflow',
        prefix_length: 1,
        suffix_length: 2,
        user_agent: 'node-fetch'
      }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end
  end

  context 'all fields present but optional fields are not populated' do
    it 'should be valid' do
      data = {
        language: 'Ruby',
        gitlab_realm: 'SaaS',
        model_engine: nil,
        model_name: nil,
        prefix_length: nil,
        suffix_length: nil,
        user_agent: nil
      }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end
  end

  context 'only required fields are present' do
    it 'should be valid' do
      data = {
        language: 'Ruby',
        gitlab_realm: 'SaaS'
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
