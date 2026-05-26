# frozen_string_literal: true

require 'json_schemer'

module SchemaFile

  def load_schema(schema_path)
    schema_content = File.read(File.join(__dir__, '..', '..', schema_path))

    json = JSON.parse(schema_content)

    # replaces the schemas with draft-04 so that json schemer can validate
    json['$schema'] = 'http://json-schema.org/draft-04/schema#'

    # ensures that the schema is correct according to Snowplow
    # http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#
    validate_field(json, %w(self vendor), /^[a-zA-Z0-9\\-_.]+$/)
    validate_field(json, %w(self name), /^[a-zA-Z0-9\-_.]+$/)
    validate_field(json, %w(self format), /^[a-zA-Z0-9\-_.]+$/)
    validate_field(json, %w(self version), /^[0-9]+-[0-9]+-[0-9]+$/)

    JSONSchemer.schema(json)
  end

  def validate_schema(schema_path, data)
    schema = load_schema(schema_path)

    normalized_data = data.transform_keys(&:to_s)
    schema.validate(normalized_data).map { |error| JSONSchemer::Errors.pretty(error) }
  end

  def validate_field(data, fields, regex)
    value = data.dig(*fields)

    if !value or !value.match(regex)
      raise "schema field #{fields.join('.')} is not present or does not satisfy regex #{regex}"
    end
  end
end
