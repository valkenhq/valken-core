use std::sync::Arc;

/// A Kubernetes resource as the store and graph see it.
///
/// The typed variants hold an `Arc` shared with the watch's reflector
/// store, the [`crate::k8s::Watched`] event that carried it and every
/// fan-out receiver, so one object lives in memory once however many
/// consumers hold it; cloning a `KubeResource` is a reference-count bump.
#[derive(Debug, Clone, PartialEq)]
pub enum KubeResource {
    Pod(Arc<k8s_openapi::api::core::v1::Pod>),
    Service(Arc<k8s_openapi::api::core::v1::Service>),
    Ingress(Arc<k8s_openapi::api::networking::v1::Ingress>),
    Deployment(Arc<k8s_openapi::api::apps::v1::Deployment>),
    /// Catch-all for any Kubernetes resource type that does not have a
    /// dedicated variant (e.g. CRDs, StatefulSets, CronJobs, …).
    /// We store the minimal metadata needed to display a useful graph card.
    CustomResource {
        /// The Kubernetes `kind` string, e.g. `"CronJob"` or `"CertificateRequest"`.
        kind: String,
        /// API group + version, e.g. `"batch/v1"` or `"cert-manager.io/v1"`.
        api_version: String,
        /// Resource name.
        name: String,
        /// Namespace the resource lives in, or `None` for cluster-scoped resources.
        namespace: Option<String>,
        /// Creation timestamp from `.metadata.creationTimestamp`.
        creation_timestamp: Option<k8s_openapi::apimachinery::pkg::apis::meta::v1::Time>,
        /// Optional human-readable status summary extracted from `.status`.
        status_summary: Option<String>,
    },
    /// Placeholder with no Kubernetes identity, for store tests that need
    /// a value without constructing a full object.
    #[cfg(test)]
    Dummy(String),
}

impl KubeResource {
    /// Returns the Kubernetes resource kind as a safe string for logging.
    /// This is safe to log and doesn't leak sensitive data.
    pub fn kind(&self) -> &str {
        match self {
            KubeResource::Pod(_) => "Pod",
            KubeResource::Service(_) => "Service",
            KubeResource::Ingress(_) => "Ingress",
            KubeResource::Deployment(_) => "Deployment",
            KubeResource::CustomResource { kind, .. } => kind.as_str(),
            #[cfg(test)]
            KubeResource::Dummy(_) => "Dummy",
        }
    }
}
