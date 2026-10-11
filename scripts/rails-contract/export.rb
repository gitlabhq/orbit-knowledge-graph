# frozen_string_literal: true

# Prints the Rails facts that Orbit redaction depends on as sorted JSON, for `rails runner`.

require "json"

EXPORTER_VERSION = 1
ROLE_LADDER = %i[guest planner reporter security_manager developer maintainer owner].freeze
CONTAINER_TARGETS = {
  "subject_container" => %w[Project Group],
  "resource_parent" => %w[Project Group],
  "preloaded_parent_container" => %w[Project Group],
  "container" => %w[Project Group],
  "packages_policy_subject" => %w[Packages::Policies::Project]
}.freeze

Rails.application.eager_load!
Dir[Rails.root.join("{,ee/}app/policies/**/*.rb")].sort.each { |file| require file }

UNRESOLVED = []
NOTEABLE_CLASSES = ApplicationRecord.descendants.select { |c| c.include?(Noteable) && c.base_class == c }.map(&:name).sort

resource_classes = Authz::RedactionService::RESOURCE_CLASSES.dup
resource_classes.merge!(EE::Authz::RedactionService::EE_RESOURCE_CLASSES) if defined?(EE::Authz::RedactionService)

def policy_for(klass)
  DeclarativePolicy.send(:class_for_class, klass)
rescue StandardError
  nil
end

def concrete_classes(klass)
  ([klass] + klass.descendants).uniq.select { |c| policy_for(c) }.sort_by(&:name)
end

def association_class(subject_class, name)
  association = subject_class.respond_to?(:reflect_on_association) && subject_class.reflect_on_association(name.to_sym)
  association.klass.name if association && !association.polymorphic?
end

def delegate_targets(subject_class, name, block)
  if block.source_location&.first&.include?("declarative_policy")
    target = association_class(subject_class, name)
    return [target] if target
  end

  line = File.readlines(block.source_location.first)[block.source_location.last - 1]
  method = line[/@subject&?\.(?:\w+&?\.)*(\w+)/, 1] || line[/delegate\s*\{\s*(\w+)\s*\}/, 1]
  return CONTAINER_TARGETS.fetch(method) if CONTAINER_TARGETS.key?(method)
  return NOTEABLE_CLASSES if method == "noteable"

  target = method && association_class(subject_class, method)
  return [target] if target
  return [method.camelize] if %w[project group].include?(method) && !subject_class.respond_to?(:reflect_on_association)

  UNRESOLVED << "#{policy_for(subject_class)} #{name} for #{subject_class}: #{line.strip}"
  []
end

def read_enables(policy)
  policy.ability_map.map.filter_map do |ability, rules|
    ability.to_s if ability.to_s.start_with?("read_") && rules.any? { |action, _| action == :enable }
  end.sort
end

classes = {}
policies = {}
queue = resource_classes.values.flat_map { |klass| concrete_classes(klass) }
until queue.empty?
  klass = queue.shift
  next if classes.key?(klass.name)

  policy = policy_for(klass)
  classes[klass.name] = { "policy" => policy.name, "table" => (klass.respond_to?(:table_name) ? klass.table_name : nil) }
  targets = policy.delegations.flat_map { |name, block| delegate_targets(klass, name, block) }.uniq.sort
  entry = policies[policy.name] ||= { "delegates" => [], "enables" => read_enables(policy), "overrides" => policy.overrides.map(&:to_s).sort }
  entry["delegates"] = (entry["delegates"] | targets).sort
  targets.each do |name|
    target = name.safe_constantize
    raise "unknown delegate target #{name}" unless target
    queue.concat(concrete_classes(target).empty? ? [target] : concrete_classes(target)) if policy_for(target)
  end
end

abilities = policies.values.flat_map { |p| p["enables"] }.uniq.sort
roles = abilities.to_h do |ability|
  floors = %i[project group].to_h do |scope|
    role = ROLE_LADDER.find { |r| Authz::Role.get(r).permissions(scope).include?(ability.to_sym) rescue false }
    [scope.to_s, role&.to_s]
  end
  [ability, floors.compact]
end

snapshot = {
  "classes" => classes.sort.to_h,
  "policies" => policies.sort.to_h,
  "resource_types" => resource_classes.sort_by { |type, _| type.to_s }.to_h do |type, klass|
    [type.to_s, { "classes" => concrete_classes(klass).map(&:name), "model" => klass.name }]
  end,
  "roles" => roles.reject { |_, floors| floors.empty? },
  "source" => {
    "declarative_policy" => Gem.loaded_specs["declarative_policy"]&.version.to_s,
    "exporter" => EXPORTER_VERSION,
    "gitlab_revision" => Gitlab.revision,
    "gitlab_version" => Gitlab::VERSION
  }
}

abort("unresolved delegates:\n#{UNRESOLVED.uniq.sort.join("\n")}") if UNRESOLVED.any?

puts JSON.pretty_generate(snapshot)
