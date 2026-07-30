//! Channel gating (ADR 013): which request surface an entity may be queried
//! through, orthogonal to role-based redaction. The set of channels an entity
//! allows is declared once as a matrix in `schema.yaml` (`settings.channel_gating`)
//! and resolved onto each node at load time; nothing lives per-node in the YAML.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// A raw request surface. Every request reaches the server through exactly one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    ExternalAgent,
    DapInternal,
    CoreFeature,
    Frontend,
}

impl Channel {
    pub const ALL: [Channel; 4] = [
        Channel::ExternalAgent,
        Channel::DapInternal,
        Channel::CoreFeature,
        Channel::Frontend,
    ];
}

/// A named set of channels. Adding a group is one arm here, not a sweep across
/// every entity in the matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelGroup {
    /// Every channel.
    AllInterfaces,
    /// Server-mediated internal features only. Excludes `frontend` because a
    /// browser-held JWT is inspectable; frontend features that need this data
    /// call Rails, which calls the server with `core_feature` (ADR 013 §8).
    InternalOnly,
}

impl ChannelGroup {
    fn channels(self) -> BTreeSet<Channel> {
        match self {
            ChannelGroup::AllInterfaces => Channel::ALL.into_iter().collect(),
            ChannelGroup::InternalOnly => [Channel::CoreFeature].into_iter().collect(),
        }
    }
}

/// A matrix entry: either a raw channel or a group alias. The mixed list on an
/// entity resolves to the union of every entry's channels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum ChannelAllowlistEntry {
    Channel(Channel),
    Group(ChannelGroup),
}

impl ChannelAllowlistEntry {
    fn channels(self) -> BTreeSet<Channel> {
        match self {
            ChannelAllowlistEntry::Channel(c) => [c].into_iter().collect(),
            ChannelAllowlistEntry::Group(g) => g.channels(),
        }
    }
}

/// Resolve a mixed list of raw channels and group aliases to a concrete set.
#[must_use]
pub fn resolve(entries: &[ChannelAllowlistEntry]) -> BTreeSet<Channel> {
    entries.iter().flat_map(|e| e.channels()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> Vec<ChannelAllowlistEntry> {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn all_interfaces_expands_to_every_channel() {
        assert_eq!(
            resolve(&parse("[all_interfaces]")),
            Channel::ALL.into_iter().collect()
        );
    }

    #[test]
    fn internal_only_excludes_frontend() {
        let set = resolve(&parse("[internal_only]"));
        assert!(set.contains(&Channel::CoreFeature));
        assert!(!set.contains(&Channel::Frontend));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn raw_and_group_entries_union() {
        assert_eq!(
            resolve(&parse("[frontend, internal_only]")),
            [Channel::Frontend, Channel::CoreFeature]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn empty_list_resolves_to_no_channels() {
        assert!(resolve(&[]).is_empty());
    }
}
