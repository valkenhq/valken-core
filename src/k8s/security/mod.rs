//! Read-only cluster inventory for the Security lens.
//!
//! Separate from [`crate::k8s::KubeWatcher`] on purpose: it watches the extra
//! kinds the lens needs (NetworkPolicy, ReplicaSet, StatefulSet, DaemonSet,
//! Job, CronJob, Gateway API HTTPRoute) and keeps only slimmed, redacted
//! views of them, so the topology pipeline is untouched and nothing runs
//! unless the lens is used.
//!
//! - [`SecurityWatch`] publishes [`SecurityInventory`] snapshots.
//! - Each kind is optional: a 403 or a missing CRD marks it unavailable
//!   instead of failing the watch.
//! - [`redacted_manifest`] exports workload specs for misconfiguration
//!   scanning, without status, managedFields or env values.
//! - [`SecuritySource::pull_credentials`] reads registry credentials from an
//!   `imagePullSecret` — opt-in only, and the Secret never leaves the source.

mod export;
mod inventory;
mod model;
mod pull_secrets;
mod source;

pub use export::{redact, redacted_manifest, REDACTED};
pub use inventory::{KindAvailability, SecurityInventory, SecurityKind, SecurityWatch};
pub use model::{
    normalize_image_id, registry_of, service_selects, EffectiveSecurity, ImageDigest, ObjectRef,
    OwnerRef, Route, SecContainer, SecHttpRoute, SecIngress, SecNetworkPolicy, SecPod,
    SecReplicaSet, SecService, SecServicePort, SecWorkload, Selector, SelectorExpr, WorkloadKind,
    WorkloadRef,
};
pub use pull_secrets::RegistryCredential;
pub use source::{http_route_resource, KubeSecuritySource, SecuritySource};
