# frozen_string_literal: true

require 'spec_helper'

describe 'public/schemas/' do
  schema_files = Dir.glob(metadata[:description] + '**/*').reject { |fi| File.directory?(fi) }

  schema_files.each do |schema_file|
    context schema_file do
      schema_path = metadata[:description]
      subject { JSON.parse(File.read(schema_path)) }
      schema_path_array = schema_path.split('/')

      it 'should contain vendor: "com.gitlab"' do
        expect(subject['self']['vendor']).to eq 'com.gitlab'
      end

      it 'should contain format: "jsonschema"' do
        expect(subject['self']['format']).to eq 'jsonschema'
      end

      it 'should contain a version' do
        expect(subject['self']['version']).to match /^[0-9]+-[0-9]+-[0-9]+$/
      end

      it 'should contain a name in file path' do
        expect(subject['self']['name']).not_to eq expect(schema_path_array[3])
      end

      it 'com.gitlab must be in file path' do
        expect(schema_path_array[2]).to eq 'com.gitlab'
      end

      it 'version must be in file path' do
        expect(schema_path_array[5]).to eq subject['self']['version']
      end
    end
  end
end
