# frozen_string_literal: true

require 'spec_helper'

describe 'public/schemas/com.gitlab/ai_context/jsonschema/1-0-0' do
  context 'all fields populated' do
    it 'should be valid' do
      data = {
        session_id: 'session_abc123',
        flow_type: 'chat',
        agent_name: 'duo_chat',
        agent_type: 'chat_agent',
        input_tokens: 1500,
        output_tokens: 500,
        total_tokens: 2000,
        ephemeral_5m_input_tokens: 100,
        ephemeral_1h_input_tokens: 500,
        cache_read: 2,
        model_engine: 'claude-3',
        model_name: 'claude-3-sonnet',
        model_provider: 'anthropic',
        flow_version: '2.1.0',
        flow_registry_version: '1.0.0'
      }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end
  end

  context 'all fields present but optional fields are null' do
    it 'should be valid' do
      data = {
        session_id: nil,
        flow_type: nil,
        agent_name: nil,
        agent_type: nil,
        input_tokens: nil,
        output_tokens: nil,
        total_tokens: nil,
        ephemeral_5m_input_tokens: nil,
        ephemeral_1h_input_tokens: nil,
        cache_read: nil,
        model_engine: nil,
        model_name: nil,
        model_provider: nil,
        flow_version: nil,
        flow_registry_version: nil
      }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end
  end

  context 'empty object' do
    it 'should be valid' do
      data = {}

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end
  end

  context 'valid flow_type values' do
    %w[chat software_development issue_to_merge_request convert_to_gitlab_ci].each do |flow_type|
      it "should be valid with flow_type: #{flow_type}" do
        data = { flow_type: flow_type }

        errors = validate_schema(self.class.top_level_description, data)
        expect(errors).to be_empty
      end
    end
  end

  context 'valid agent_name values' do
    %w[duo_chat code_agent planning_agent].each do |agent_name|
      it "should be valid with agent_name: #{agent_name}" do
        data = { agent_name: agent_name }

        errors = validate_schema(self.class.top_level_description, data)
        expect(errors).to be_empty
      end
    end
  end

  context 'null values for enum fields' do
    it 'should be valid with null flow_type' do
      data = { flow_type: nil }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end

    it 'should be valid with null agent_name' do
      data = { agent_name: nil }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end

    it 'should be valid with null agent_type' do
      data = { agent_type: nil }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end
  end

  context 'negative token values' do
    it 'should be invalid with negative input_tokens' do
      data = { input_tokens: -1 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).not_to be_empty
    end

    it 'should be invalid with negative output_tokens' do
      data = { output_tokens: -1 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).not_to be_empty
    end

    it 'should be invalid with negative total_tokens' do
      data = { total_tokens: -1 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).not_to be_empty
    end

    it 'should be invalid with negative ephemeral_5m_input_tokens' do
      data = { ephemeral_5m_input_tokens: -1 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).not_to be_empty
    end

    it 'should be invalid with negative ephemeral_1h_input_tokens' do
      data = { ephemeral_1h_input_tokens: -1 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).not_to be_empty
    end

    it 'should be invalid with negative cache_read' do
      data = { cache_read: -1 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).not_to be_empty
    end
  end

  context 'token values at maximum boundary' do
    it 'should be valid with maximum integer values' do
      data = {
        input_tokens: 2147483647,
        output_tokens: 2147483647,
        total_tokens: 2147483647,
        ephemeral_5m_input_tokens: 2147483647,
        ephemeral_1h_input_tokens: 2147483647,
        cache_read: 2147483647
      }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end
  end

  context 'string length limits' do
    it 'should be invalid with session_id exceeding 255 characters' do
      data = { session_id: 'a' * 256 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).not_to be_empty
    end


    it 'should be invalid with agent_type exceeding 64 characters' do
      data = { agent_type: 'a' * 65 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).not_to be_empty
    end

    it 'should be valid with session_id at 255 characters' do
      data = { session_id: 'a' * 255 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end


    it 'should be valid with agent_type at 64 characters' do
      data = { agent_type: 'a' * 64 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end

    it 'should be invalid with model_engine exceeding 64 characters' do
      data = { model_engine: 'a' * 65 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).not_to be_empty
    end

    it 'should be valid with model_engine at 64 characters' do
      data = { model_engine: 'a' * 64 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end

    it 'should be invalid with model_name exceeding 64 characters' do
      data = { model_name: 'a' * 65 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).not_to be_empty
    end

    it 'should be valid with model_name at 64 characters' do
      data = { model_name: 'a' * 64 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end

    it 'should be invalid with model_provider exceeding 64 characters' do
      data = { model_provider: 'a' * 65 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).not_to be_empty
    end

    it 'should be valid with model_provider at 64 characters' do
      data = { model_provider: 'a' * 64 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end

    it 'should be invalid with flow_version exceeding 64 characters' do
      data = { flow_version: 'a' * 65 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).not_to be_empty
    end

    it 'should be valid with flow_version at 64 characters' do
      data = { flow_version: 'a' * 64 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end

    it 'should be invalid with flow_registry_version exceeding 64 characters' do
      data = { flow_registry_version: 'a' * 65 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).not_to be_empty
    end

    it 'should be valid with flow_registry_version at 64 characters' do
      data = { flow_registry_version: 'a' * 64 }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end
  end

  context 'additional properties' do
    it 'should be invalid with additional properties' do
      data = { unknown_field: 'value' }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).not_to be_empty
    end
  end

  context 'realistic DAP workflow scenario' do
    it 'should be valid with typical DAP workflow data' do
      data = {
        session_id: 'wf_session_12345',
        flow_type: 'software_development',
        agent_name: 'code_agent',
        agent_type: 'code_agent',
        input_tokens: 3200,
        output_tokens: 1200,
        total_tokens: 4400,
        ephemeral_5m_input_tokens: 250,
        ephemeral_1h_input_tokens: 1000,
        cache_read: 5,
        model_engine: 'claude-3',
        model_name: 'claude-3-opus',
        model_provider: 'anthropic',
        flow_version: '1.2.0',
        flow_registry_version: '0.9.0'
      }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end
  end

  context 'realistic chat scenario' do
    it 'should be valid with typical chat data' do
      data = {
        session_id: 'chat_session_67890',
        flow_type: 'chat',
        agent_name: 'duo_chat',
        agent_type: 'chat_agent',
        input_tokens: 1500,
        output_tokens: 500,
        total_tokens: 2000,
        ephemeral_5m_input_tokens: 100,
        ephemeral_1h_input_tokens: 500,
        cache_read: 2,
        model_engine: 'claude-3',
        model_name: 'claude-3-sonnet',
        model_provider: 'anthropic',
        flow_version: '3.0.1',
        flow_registry_version: '1.1.0'
      }

      errors = validate_schema(self.class.top_level_description, data)
      expect(errors).to be_empty
    end
  end
end
