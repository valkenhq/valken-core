//! The seam between [`crate::k8s::KubeWatcher`] and the Kubernetes API.
//!
//! [`ClusterSource`] is everything the watcher needs from a cluster: one
//! watch stream per resource kind plus the cluster identity.  The
//! production impl ([`KubeClusterSource`]) wraps a [`kube::Client`];
//! tests drive the watcher with an in-memory source fed by channels.

use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Namespace, Pod, Service};
use k8s_openapi::api::networking::v1::Ingress;
use kube::api::ListParams;
use kube::runtime::watcher::{self, watcher, Event};
use kube::runtime::WatchStreamExt;
use kube::{Api, Client, ResourceExt};
use tracing::{info, warn};

use crate::k8s::error::describe;
use crate::k8s::kubeconfig::probe::{classify, ProbeError};
use crate::k8s::metrics::{KubeMetricsSource, MetricsSource};

/// A watch stream for one resource kind, in kube-runtime's event shape.
pub type WatchStream<K> = BoxStream<'static, Result<Event<K>, watcher::Error>>;

/// Everything the watcher needs from a cluster.
pub trait ClusterSource: Send + Sync + 'static {
    fn watch_pods(&self) -> WatchStream<Pod>;
    fn watch_deployments(&self) -> WatchStream<Deployment>;
    fn watch_services(&self) -> WatchStream<Service>;
    fn watch_ingresses(&self) -> WatchStream<Ingress>;
    fn watch_namespaces(&self) -> WatchStream<Namespace>;

    /// A stable, unique identifier for the cluster (the `kube-system`
    /// namespace UID on a real cluster).  `None` if it cannot be read.
    fn cluster_uuid(&self) -> BoxFuture<'static, Option<String>>;

    /// Metrics access for the same cluster, if available.
    fn metrics(&self) -> Option<Arc<dyn MetricsSource>> {
        None
    }

    /// One cheap, *authenticated* round trip, used by the health monitor
    /// for latency and to notice expired credentials (`GET /version` is
    /// often anonymous, so it can't).  Sources without a network say yes.
    fn ping(&self) -> BoxFuture<'static, Result<(), ProbeError>> {
        async { Ok(()) }.boxed()
    }
}

/// [`ClusterSource`] backed by a real API server.
#[derive(Clone)]
pub struct KubeClusterSource {
    client: Client,
}

impl KubeClusterSource {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Cluster-wide watch of one kind (`Api::all` lists and watches both
    /// namespaced and cluster-scoped kinds).
    fn watch<K>(&self) -> WatchStream<K>
    where
        K: kube::Resource + Clone + std::fmt::Debug + Send + serde::de::DeserializeOwned + 'static,
        K::DynamicType: Default,
    {
        // Bookmarks are on by default and keep the watch alive across the
        // API server's idle timeout; the default backoff turns a failing
        // API server into a slow retry loop instead of a hot one.
        // `managedFields` is a third to a half of a Pod's size and nothing
        // here reads it, so it is dropped before the object is stored.
        watcher(
            Api::<K>::all(self.client.clone()),
            watcher::Config::default(),
        )
        .default_backoff()
        .modify(|o| o.managed_fields_mut().clear())
        .boxed()
    }
}

impl ClusterSource for KubeClusterSource {
    fn watch_pods(&self) -> WatchStream<Pod> {
        self.watch()
    }

    fn watch_deployments(&self) -> WatchStream<Deployment> {
        self.watch()
    }

    fn watch_services(&self) -> WatchStream<Service> {
        self.watch()
    }

    fn watch_ingresses(&self) -> WatchStream<Ingress> {
        self.watch()
    }

    fn watch_namespaces(&self) -> WatchStream<Namespace> {
        self.watch()
    }

    fn cluster_uuid(&self) -> BoxFuture<'static, Option<String>> {
        let api: Api<Namespace> = Api::all(self.client.clone());
        async move {
            match api.get("kube-system").await {
                Ok(ns) => {
                    let uid = ns.metadata.uid;
                    if let Some(u) = &uid {
                        info!(uid = %u, "Cluster UUID read from kube-system namespace");
                    }
                    uid
                }
                Err(e) => {
                    warn!(error = %describe(&e), "Could not read kube-system namespace UID");
                    None
                }
            }
        }
        .boxed()
    }

    fn metrics(&self) -> Option<Arc<dyn MetricsSource>> {
        Some(Arc::new(KubeMetricsSource::new(self.client.clone())))
    }

    fn ping(&self) -> BoxFuture<'static, Result<(), ProbeError>> {
        let api: Api<Namespace> = Api::all(self.client.clone());
        async move {
            match api.list_metadata(&ListParams::default().limit(1)).await {
                // Authenticated but not allowed to list namespaces: the
                // link and the credentials are fine.
                Ok(_) => Ok(()),
                Err(kube::Error::Api(s)) if s.code == 403 => Ok(()),
                Err(e) => Err(classify(e)),
            }
        }
        .boxed()
    }
}
