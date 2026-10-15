//! The single claims-conversion seam between orbit-server and orbit-billing.
//!
//! Converts `auth::Claims` into `BillingInputs` and `QuotaCheckInputs`.
//! Billing-related call sites use these conversions rather than mapping
//! claims into the billing input structs themselves.
//!
//! This file, the orbit-billing crate, and the extended hook points form
//! the SOX billing surface defined in ADR 020:
//! `docs/design-documents/decisions/020_billing_sox_scope.md`.

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
            instance_version: c.instance_version.clone(),
            license_checksum: c.license_checksum.clone(),
        }
    }
}
