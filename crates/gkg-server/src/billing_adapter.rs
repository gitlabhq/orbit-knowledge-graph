//! The single permitted gkg-server↔gkg-billing seam (ADR 013:
//! `docs/design-documents/decisions/013_billing_sox_scope.md`).
//!
//! Billing logic lives in `crates/gkg-billing/`. The only data that crosses
//! the boundary is `BillingInputs` and `QuotaCheckInputs`, defined there.
//! This file is the complete declaration of which `auth::Claims` fields
//! populate them. All billing-related call sites in gkg-server consume these
//! inputs via the `From` impls below — they never construct the structs
//! directly. Per SOX boundary policy, this file plus the `gkg-billing` crate
//! are the primary auditable surface for billing in this repository.

use gkg_billing::{BillingInputs, QuotaCheckInputs};

use crate::auth::Claims;

impl From<&Claims> for BillingInputs {
    fn from(c: &Claims) -> Self {
        Self {
            realm: c.realm.clone(),
            user_id: c.user_id as i64,
            source_type: <&str>::from(c.source_type).to_string(),
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
