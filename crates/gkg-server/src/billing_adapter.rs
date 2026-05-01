//! The single permitted gkg-server↔gkg-billing seam.
//!
//! Billing logic lives in `crates/gkg-billing/`. The only data that crosses
//! the boundary is `BillingInputs` (defined there). This file is the
//! complete declaration of which `auth::Claims` fields populate that struct.
//! All billing-related call sites in gkg-server consume `BillingInputs`
//! built via this `From` impl — they never construct `BillingInputs`
//! directly. Per SOX boundary policy, this file plus the `gkg-billing`
//! crate are the entire auditable surface for billing in this repository.

use gkg_billing::BillingInputs;

use crate::auth::Claims;

impl From<&Claims> for BillingInputs {
    fn from(c: &Claims) -> Self {
        Self {
            realm: c.realm.clone(),
            user_id: c.user_id as i64,
            source_type: c.source_type.clone(),
            organization_id: c.organization_id.map(|id| id as i64),
            instance_id: c.instance_id.clone(),
            unique_instance_id: c.unique_instance_id.clone(),
            instance_version: c.instance_version.clone(),
            global_user_id: c.global_user_id.clone(),
            host_name: c.host_name.clone(),
            root_namespace_id: c.root_namespace_id,
            deployment_type: c.deployment_type.clone(),
        }
    }
}
