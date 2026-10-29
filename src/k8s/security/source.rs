//! The seam between [`super::SecurityWatch`] and the Kubernetes API, mirroring
//! [`crate::k8s::ClusterSource`]: production wraps a [`kube::Client`], tests
//! feed events through channels.

use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{Pod, Secret, Service};
use k8s_openapi::api::networking::v1::{Ingress, NetworkPolicy};
use kube::api::{ApiResource, DynamicObject, GroupVersionKind};
use kube::runtime::watcher::{self, watcher};
use kube::runtime::WatchStreamExt;
use kube::{Api, Client};

use super::pull_secrets::{credentials_from_secret, RegistryCredential};
use crate::k8s::WatchStream;

/// Read-only access to everything the Security lens needs.
pub trait SecuritySource: Send + Sync + 'static {
    fn watch_pods(&self) -> WatchStream<Pod>;
    fn watch_replica_sets(&self) -> WatchStream<ReplicaSet>;
    fn watch_deployments(&self) -> WatchStream<Deployment>;
    fn watch_stateful_sets(&self) -> WatchStream<StatefulSet>;
    fn watch_daemon_sets(&self) -> WatchStream<DaemonSet>;
    fn watch_jobs(&self) -> WatchStream<Job>;
    fn watch_cron_jobs(&self) -> WatchStream<CronJob>;
    fn watch_services(&self) -> WatchStream<Service>;
    fn watch_ingresses(&self) -> WatchStream<Ingress>;
    fn watch_network_policies(&self) -> WatchStream<NetworkPolicy>;
    /// Gateway API `HTTPRoute`s; the stream fails with 404 when the CRD
    /// isn't installed.
    fn watch_http_routes(&self) -> WatchStream<DynamicObject>;

    /// The registry credentials in one `imagePullSecret` (the opt-in
    /// registry scan).  Empty if the Secret is absent or isn't a Docker
    /// config.  The Secret itself stays inside the implementation.
    fn pull_credentials(
        &self,
        namespace: &str,
        name: &str,
    ) -> BoxFuture<'static, Result<Vec<RegistryCredential>, kube::Error>>;
}

/// `gateway.networking.k8s.io/v1` `HTTPRoute`.
pub fn http_route_resource() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind::gvk(
        "gateway.networking.k8s.io",
        "v1",
        "HTTPRoute",
    ))
}

/// [`SecuritySource`] backed by a real API server.
#[derive(Clone)]
pub struct KubeSecuritySource {
    client: Client,
}

impl KubeSecuritySource {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    fn watch<K>(&self) -> WatchStream<K>
    where
        K: kube::Resource + Clone + std::fmt::Debug + Send + serde::de::DeserializeOwned + 'static,
        K::DynamicType: Default,
    {
        watcher(
            Api::<K>::all(self.client.clone()),
            watcher::Config::default(),
        )
        .default_backoff()
        .boxed()
    }
}

impl SecuritySource for KubeSecuritySource {
    fn watch_pods(&self) -> WatchStream<Pod> {
        self.watch()
    }
    fn watch_replica_sets(&self) -> WatchStream<ReplicaSet> {
        self.watch()
    }
    fn watch_deployments(&self) -> WatchStream<Deployment> {
        self.watch()
    }
    fn watch_stateful_sets(&self) -> WatchStream<StatefulSet> {
        self.watch()
    }
    fn watch_daemon_sets(&self) -> WatchStream<DaemonSet> {
        self.watch()
    }
    fn watch_jobs(&self) -> WatchStream<Job> {
        self.watch()
    }
    fn watch_cron_jobs(&self) -> WatchStream<CronJob> {
        self.watch()
    }
    fn watch_services(&self) -> WatchStream<Service> {
        self.watch()
    }
    fn watch_ingresses(&self) -> WatchStream<Ingress> {
        self.watch()
    }
    fn watch_network_policies(&self) -> WatchStream<NetworkPolicy> {
        self.watch()
    }

    fn watch_http_routes(&self) -> WatchStream<DynamicObject> {
        let resource = http_route_resource();
        watcher(
            Api::<DynamicObject>::all_with(self.client.clone(), &resource),
            watcher::Config::default(),
        )
        .default_backoff()
        .boxed()
    }

    fn pull_credentials(
        &self,
        namespace: &str,
        name: &str,
    ) -> BoxFuture<'static, Result<Vec<RegistryCredential>, kube::Error>> {
        let api: Api<Secret> = Api::namespaced(self.client.clone(), namespace);
        let name = name.to_owned();
        async move {
            // Parsed and dropped in one expression: the Secret is never
            // held across an await or handed out.
            Ok(api
                .get_opt(&name)
                .await?
                .map(|s| credentials_from_secret(&s))
                .unwrap_or_default())
        }
        .boxed()
    }
}
