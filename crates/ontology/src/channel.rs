//! Entity-level channel gating (ADR 013).
//!
//! A **channel** is the request-authentication surface through which a query
//! reached GKG (frontend session, DAP composite identity, internal Rails JWT,
//! or external agent). It is orthogonal to the user's role — the two combine
//! by AND, so channel gating can only narrow what role-based access already
//! permits, never widen it.
//!
//! Each entity declares a `channel_allowlist` alongside `required_role` in
//! its `redaction:` block. An **empty or absent** allowlist resolves to the
//! empty channel set, so nobody (including `core_feature`) can see the
//! entity; visibility is an explicit opt-in.
//!
//! Groups are named aliases for common channel sets, resolved by the
//! central `ChannelGroup::channels` table so a policy change touches one
//! row rather than every entity that references the group.
//!
//! See `docs/design-documents/decisions/013_entity_channel_gating.md`.

use std::collections::BTreeSet;

use serde::{Deserialize, Deserializer};

/// The four request-authentication surfaces an Orbit query can arrive
/// through. Rails derives which one applies from the auth mechanism itself;
/// clients never assert their own channel. See the auth-mechanism table in
/// ADR 013 §2 for the full derivation.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Deserialize,
    serde::Serialize,
    strum::IntoStaticStr,
    strum::EnumString,
    strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Channel {
    /// A customer calling in on their own credentials — MCP, REST/PAT, CLI —
    /// but not through the frontend session or a DAP composite identity.
    ExternalAgent,
    /// Duo Agent Platform: `ai_workflows`-scoped token with composite
    /// identity (service account + user).
    DapInternal,
    /// System JWT from an internal Rails service. Never held by a browser.
    CoreFeature,
    /// User JWT (HS256) issued to and held by the browser for the frontend
    /// session. Trust posture differs from `CoreFeature`: this token is
    /// visible in devtools and lift-able, so `internal_only` deliberately
    /// excludes it (see ADR 013 §8).
    Frontend,
}

/// Named alias for a set of channels. Groups exist purely as a convenience
/// for combinations that come up repeatedly across entities; entries in an
/// allowlist may mix groups and raw channels freely.
///
/// The group→channel-set mapping is centrally maintained here rather than
/// duplicated per entity, so adding a new group later means adding one
/// arm below (and updating the docs), not touching every entity that
/// references an existing group.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Deserialize,
    serde::Serialize,
    strum::IntoStaticStr,
    strum::EnumString,
    strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ChannelGroup {
    /// No restriction. Explicit opt-in to "everyone," not an accidental
    /// default: an entity that means "unrestricted" must say so.
    AllInterfaces,
    /// Backs internal GitLab features/tooling. Not exposed to any
    /// agent-facing surface — DAP included. `Frontend` is deliberately
    /// excluded (ADR 013 §8) because the frontend JWT is holdable by the
    /// browser and would collapse `internal_only` to "anyone with a tab."
    InternalOnly,
}

impl ChannelGroup {
    /// Central group→channel-set table. All expansion goes through here.
    #[must_use]
    pub fn channels(self) -> BTreeSet<Channel> {
        match self {
            Self::AllInterfaces => [
                Channel::ExternalAgent,
                Channel::DapInternal,
                Channel::CoreFeature,
                Channel::Frontend,
            ]
            .into_iter()
            .collect(),
            Self::InternalOnly => [Channel::CoreFeature].into_iter().collect(),
        }
    }
}

/// A single entry in `channel_allowlist`: either a raw channel name or a
/// group alias. Groups expand via [`ChannelGroup::channels`] at resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelAllowlistEntry {
    Channel(Channel),
    Group(ChannelGroup),
}

impl<'de> Deserialize<'de> for ChannelAllowlistEntry {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        if let Ok(g) = raw.parse::<ChannelGroup>() {
            return Ok(Self::Group(g));
        }
        raw.parse::<Channel>().map(Self::Channel).map_err(|_| {
            serde::de::Error::custom(format!(
                "`{raw}` is not a recognized channel or channel group; \
                 valid channels: external_agent, dap_internal, core_feature, frontend; \
                 valid groups: all_interfaces, internal_only"
            ))
        })
    }
}

/// Full `channel_allowlist:` value: an ordered list of entries. The
/// **resolved** set is the union of every entry's expansion. An empty list
/// resolves to the empty set — `channel_allowlist: []` is a deliberate
/// "nobody sees this," not a shorthand for "everybody."
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(transparent)]
pub struct ChannelAllowlist(pub Vec<ChannelAllowlistEntry>);

impl ChannelAllowlist {
    #[must_use]
    pub fn resolve(&self) -> BTreeSet<Channel> {
        let mut out = BTreeSet::new();
        for entry in &self.0 {
            match entry {
                ChannelAllowlistEntry::Channel(c) => {
                    out.insert(*c);
                }
                ChannelAllowlistEntry::Group(g) => out.extend(g.channels()),
            }
        }
        out
    }

    /// True when this allowlist is missing or empty — nobody, not even
    /// `core_feature`, is allowed. Used by CI to fail the build on entities
    /// that would ship invisible.
    #[must_use]
    pub fn is_fail_closed_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// True when the resolved set is `{ExternalAgent, DapInternal, CoreFeature, Frontend}`.
    /// Used by the widening review gate to identify entities that have opened
    /// up to "everyone."
    #[must_use]
    pub fn is_all_interfaces(&self) -> bool {
        self.resolve() == ChannelGroup::AllInterfaces.channels()
    }

    #[must_use]
    pub fn from_entries(entries: Vec<ChannelAllowlistEntry>) -> Self {
        Self(entries)
    }

    #[must_use]
    pub fn allows(&self, channel: Channel) -> bool {
        self.resolve().contains(&channel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_interfaces_group_expands_to_every_channel() {
        assert_eq!(
            ChannelGroup::AllInterfaces.channels(),
            [
                Channel::ExternalAgent,
                Channel::DapInternal,
                Channel::CoreFeature,
                Channel::Frontend,
            ]
            .into_iter()
            .collect()
        );
    }

    // Frontend is deliberately excluded: ADR 013 §8 documents that frontend
    // JWTs are browser-holdable and would collapse `internal_only`'s meaning.
    #[test]
    fn internal_only_excludes_frontend_and_external() {
        let set = ChannelGroup::InternalOnly.channels();
        assert!(set.contains(&Channel::CoreFeature));
        assert!(!set.contains(&Channel::Frontend));
        assert!(!set.contains(&Channel::ExternalAgent));
        assert!(!set.contains(&Channel::DapInternal));
    }

    #[test]
    fn resolve_unions_groups_and_raw_channels() {
        let list = ChannelAllowlist(vec![
            ChannelAllowlistEntry::Group(ChannelGroup::InternalOnly),
            ChannelAllowlistEntry::Channel(Channel::DapInternal),
        ]);
        let resolved = list.resolve();
        assert!(resolved.contains(&Channel::CoreFeature));
        assert!(resolved.contains(&Channel::DapInternal));
        assert!(!resolved.contains(&Channel::Frontend));
    }

    // An empty allowlist is the fail-closed default: no channel — including
    // `core_feature` — can see the entity. This closes the "forgot to
    // populate" gap that a fail-open default (`required_role`'s posture)
    // would leave.
    #[test]
    fn empty_allowlist_resolves_to_empty_set() {
        let list = ChannelAllowlist::default();
        assert!(list.resolve().is_empty());
        assert!(list.is_fail_closed_empty());
        assert!(!list.allows(Channel::CoreFeature));
    }

    #[test]
    fn deserialize_mixed_entries() {
        let yaml = "- all_interfaces\n- dap_internal\n";
        let list: ChannelAllowlist = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(list.0.len(), 2);
        assert!(matches!(
            list.0[0],
            ChannelAllowlistEntry::Group(ChannelGroup::AllInterfaces)
        ));
        assert!(matches!(
            list.0[1],
            ChannelAllowlistEntry::Channel(Channel::DapInternal)
        ));
    }

    #[test]
    fn deserialize_rejects_unknown_entry() {
        let yaml = "- not_a_real_channel\n";
        let err = serde_yaml::from_str::<ChannelAllowlist>(yaml).unwrap_err();
        assert!(
            err.to_string().contains("not_a_real_channel"),
            "error should name the offending entry, got: {err}"
        );
    }

    #[test]
    fn is_all_interfaces_matches_group_expansion() {
        let group_form = ChannelAllowlist(vec![ChannelAllowlistEntry::Group(
            ChannelGroup::AllInterfaces,
        )]);
        assert!(group_form.is_all_interfaces());

        let raw_form = ChannelAllowlist(vec![
            ChannelAllowlistEntry::Channel(Channel::ExternalAgent),
            ChannelAllowlistEntry::Channel(Channel::DapInternal),
            ChannelAllowlistEntry::Channel(Channel::CoreFeature),
            ChannelAllowlistEntry::Channel(Channel::Frontend),
        ]);
        assert!(raw_form.is_all_interfaces());

        let narrow = ChannelAllowlist(vec![ChannelAllowlistEntry::Channel(Channel::DapInternal)]);
        assert!(!narrow.is_all_interfaces());
    }
}
