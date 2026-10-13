# frozen_string_literal: true

# Prints the Rails facts that Orbit redaction depends on as sorted JSON, for `rails runner`.

EXPORTER_VERSION = 1
ROLE_LADDER = %i[guest planner reporter security_manager developer maintainer owner].freeze
CONTAINER_TARGETS = %w[subject_container resource_parent preloaded_parent_container container]
  .to_h { |method| [method, %w[Project Group]] }.merge("packages_policy_subject" => %w[Packages::Policies::Project]).freeze

Rails.application.eager_load!

UNRESOLVED = []
NOTEABLE_CLASSES = ApplicationRecord.descendants.select { |c| c.include?(Noteable) && c.base_class == c }.map(&:name).sort

resource_classes = Authz::RedactionService::RESOURCE_CLASSES.merge(EE::Authz::RedactionService::EE_RESOURCE_CLASSES)

def policy_for(klass) = (DeclarativePolicy.send(:class_for_class, klass) rescue nil)

def concrete_classes(klass) = ([klass] + klass.descendants).select { |c| policy_for(c) }.sort_by(&:name)

def association_class(subject_class, name)
  association = subject_class.respond_to?(:reflect_on_association) && subject_class.reflect_on_association(name.to_sym)
  association.klass.name if association && !association.polymorphic?
end

def delegate_targets(subject_class, name, block)
  file, lineno = block.source_location
  target = file.include?("declarative_policy") && association_class(subject_class, name)
  return [target] if target

  line = File.readlines(file)[lineno - 1]
  method = line[/@subject&?\.(?:\w+&?\.)*(\w+)/, 1] || line[/delegate\s*\{\s*(\w+)\s*\}/, 1]
  return CONTAINER_TARGETS.fetch(method) if CONTAINER_TARGETS.key?(method)
  return NOTEABLE_CLASSES if method == "noteable"

  target = method && association_class(subject_class, method)
  return [target] if target
  return [method.camelize] if %w[project group].include?(method) && !subject_class.respond_to?(:reflect_on_association)

  UNRESOLVED << "#{policy_for(subject_class)} #{name} for #{subject_class}: #{line.strip}"
  []
end

classes = {}
policies = {}
queue = resource_classes.values.flat_map { |klass| concrete_classes(klass) }
until queue.empty?
  klass = queue.shift
  next if classes.key?(klass.name)

  policy = policy_for(klass)
  classes[klass.name] = { "policy" => policy.name, "table" => klass.try(:table_name) }
  targets = policy.delegations.flat_map { |name, block| delegate_targets(klass, name, block) }.uniq.sort
  enables = policy.ability_map.map.select { |ability, rules| ability.start_with?("read_") && rules.assoc(:enable) }.keys.map(&:to_s).sort
  entry = policies[policy.name] ||= { "delegates" => [], "enables" => enables, "overrides" => policy.overrides.map(&:to_s).sort }
  entry["delegates"] = (entry["delegates"] | targets).sort
  targets.each do |name|
    target = name.safe_constantize or raise "unknown delegate target #{name}"
    queue.concat(concrete_classes(target)) if policy_for(target)
  end
end

roles = policies.values.flat_map { |p| p["enables"] }.uniq.sort.to_h do |ability|
  floors = %i[project group].to_h do |scope|
    [scope.to_s, ROLE_LADDER.find { |r| Authz::Role.get(r).permissions(scope).include?(ability.to_sym) rescue false }&.to_s]
  end
  [ability, floors.compact]
end

abort("unresolved delegates:\n#{UNRESOLVED.uniq.sort.join("\n")}") if UNRESOLVED.any?

puts JSON.pretty_generate(
  "classes" => classes.sort.to_h,
  "policies" => policies.sort.to_h,
  "resource_types" => resource_classes.sort_by { |type, _| type.to_s }.to_h { |type, klass|
    [type.to_s, { "classes" => concrete_classes(klass).map(&:name), "model" => klass.name }]
  },
  "roles" => roles.reject { |_, floors| floors.empty? },
  "source" => {
    "declarative_policy" => Gem.loaded_specs["declarative_policy"]&.version.to_s,
    "exporter" => EXPORTER_VERSION,
    "gitlab_version" => Gitlab::VERSION
  }
)
