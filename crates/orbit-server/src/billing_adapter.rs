//! The single permitted orbit-server↔orbit-billing seam.
//!
//! Billing logic lives in `crates/orbit-billing/`. The only data that crosses
//! the boundary is `BillingInputs` (defined there). This file is the
//! complete declaration of which `auth::Claims` fields populate that struct.
//! All billing-related call sites in orbit-server consume `BillingInputs`
//! built via `billing_inputs` — they never construct `BillingInputs`
//! directly. Per SOX boundary policy, this file plus the `orbit-billing`
//! crate are the entire auditable surface for billing in this repository.

use orbit_billing::{BillingInputs, QuotaCheckInputs};

use crate::auth::Claims;

pub fn billing_inputs(claims: &Claims, coding_agent: Option<String>) -> BillingInputs {
    BillingInputs {
        realm: claims.realm.clone(),
        user_id: claims.user_id as i64,
        source_type: <&str>::from(claims.source_type).to_string(),
        organization_id: claims.organization_id.map(|id| id as i64),
        instance_id: claims.instance_id.clone(),
        unique_instance_id: claims.unique_instance_id.clone(),
        instance_version: claims.instance_version.clone(),
        global_user_id: claims.global_user_id.clone(),
        host_name: claims.host_name.clone(),
        root_namespace_id: claims.root_namespace_id,
        deployment_type: claims.deployment_type.clone(),
        is_gitlab_team_member: claims.is_gitlab_team_member,
        coding_agent,
    }
}

impl From<&Claims> for QuotaCheckInputs {
    fn from(c: &Claims) -> Self {
        Self {
            source_type: <&str>::from(c.source_type).to_string(),
            user_id: c.user_id as i64,
            realm: c.realm.clone(),
            global_user_id: c.global_user_id.clone(),
            root_namespace_id: c.root_namespace_id,
            instance_id: c.instance_id.clone(),
            unique_instance_id: c.unique_instance_id.clone(),
        }
    }
}
