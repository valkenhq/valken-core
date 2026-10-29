use std::sync::Arc;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Pod, Service};
use k8s_openapi::api::networking::v1::Ingress;

use super::KubeResource;

#[derive(Clone, Debug)]
pub struct WatchedResources {
    pub resources: Vec<KubeResource>,
    /// Cluster integer ID from the `clusters` DB table, resolved before
    /// `begin_watching()` sends this message.  `0` means unknown.
    pub cluster_id: u32,
}

/// Metadata for one Namespace object — name plus its labels.
///
/// Carried separately from `KubeResource` because namespaces are not graph
/// nodes; only their labels are needed (district grouping in the topology view).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceInfo {
    pub name: String,
    pub labels: std::collections::BTreeMap<String, String>,
    /// `.metadata.creationTimestamp` as unix seconds (drives namespace age).
    pub created_secs: Option<i64>,
}

impl NamespaceInfo {
    pub fn from_namespace(ns: &k8s_openapi::api::core::v1::Namespace) -> Self {
        Self {
            name: ns.metadata.name.clone().unwrap_or_default(),
            labels: ns.metadata.labels.clone().unwrap_or_default(),
            created_secs: ns
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| t.0.as_second()),
        }
    }
}

/// Events emitted by [`super::KubeWatcher`].
///
/// A [`Watched::FullResourceList`] is a complete snapshot: consumers replace
/// their state with it.  Every other variant is an incremental change.
///
/// Objects are `Arc`s shared with the watcher's reflector store, so the
/// clone each fan-out receiver gets is a reference-count bump, not a copy.
/// `Debug` prints the whole object — a Pod spec includes its `env` — so log
/// these with `%`-formatted names, not `?`.
#[derive(Clone, Debug)]
pub enum Watched {
    Pod(Arc<Pod>),
    Deployment(Arc<Deployment>),
    Service(Arc<Service>),
    Ingress(Arc<Ingress>),
    /// Complete snapshot of every watched resource, sent after the initial
    /// sync and again whenever a watch has to re-list.
    FullResourceList(WatchedResources),
    /// The named pod was deleted from the cluster — remove it from the store.
    DeletedPod(Arc<Pod>),
    /// The named deployment was deleted from the cluster — remove it from the store.
    DeletedDeployment(Arc<Deployment>),
    DeletedService(Arc<Service>),
    DeletedIngress(Arc<Ingress>),
    /// Full snapshot of all Namespace objects (name + labels), sent before
    /// the first [`Watched::FullResourceList`] and on every namespace change.
    NamespaceList(Vec<NamespaceInfo>),
}

/// A [`Watched`] value that is not a single applied resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Watched event is not an applied resource")]
pub struct NotAppliedResource;

impl TryFrom<Watched> for KubeResource {
    type Error = NotAppliedResource;

    fn try_from(val: Watched) -> Result<Self, Self::Error> {
        match val {
            Watched::Pod(pod) => Ok(KubeResource::Pod(pod)),
            Watched::Deployment(deployment) => Ok(KubeResource::Deployment(deployment)),
            Watched::Service(service) => Ok(KubeResource::Service(service)),
            Watched::Ingress(ingress) => Ok(KubeResource::Ingress(ingress)),
            Watched::FullResourceList(_)
            | Watched::DeletedPod(_)
            | Watched::DeletedDeployment(_)
            | Watched::DeletedService(_)
            | Watched::DeletedIngress(_)
            | Watched::NamespaceList(_) => Err(NotAppliedResource),
        }
    }
}
