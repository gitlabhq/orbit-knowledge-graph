# Enables knowledge graph feature flags.
# Usage: gitlab-rails runner /path/to/enable_feature_flags.rb

Feature.enable(:knowledge_graph_infra)
Feature.enable(:knowledge_graph)
