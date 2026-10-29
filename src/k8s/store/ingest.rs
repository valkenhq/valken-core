//! Engine-free core of the resource store node.
//!
//! [`ResourceIngest`] owns the [`KubeResourceStore`], the [`KubeGoat`] graph
//! and the namespace metadata, and folds [`Watched`] events into them.
//! [`ResourceIngest::apply`] returns an [`IngestReport`] describing which
//! signals a UI wrapper should emit; it never touches Godot, so the whole
//! ingest path is unit-testable.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use futures::FutureExt;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::{
    kri_for, EdgeType, GraphParts, KubeGoat, KubeResource, KubeResourceIdentifier,
    KubeResourceStore,
};
use crate::k8s::Watched;

/// What changed during one [`ResourceIngest::apply`] call.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct IngestReport {
    /// A full snapshot was received and the store was replaced.
    pub full_list_received: bool,
    /// Incrementally inserted (`true`) or updated (`false`) resources.
    pub updated: Vec<(KubeResourceIdentifier, bool)>,
    /// Resources removed from the store.
    pub deleted: Vec<KubeResourceIdentifier>,
    /// A namespace snapshot was stored.
    pub namespaces_updated: bool,
    /// Events that could not be applied (no name, unsupported kind).
    pub errors: usize,
}

impl IngestReport {
    pub fn is_empty(&self) -> bool {
        !self.full_list_received
            && self.updated.is_empty()
            && self.deleted.is_empty()
            && !self.namespaces_updated
    }
}

pub struct ResourceIngest {
    resource_store: Arc<RwLock<KubeResourceStore>>,
    kube_goat: Arc<RwLock<KubeGoat>>,
    /// Background graph rebuild in flight, if any.
    pending_graph_build: Option<JoinHandle<GraphParts>>,
    /// A full list arrived while a build was in flight; rebuild again once
    /// it lands so the graph never misses the newest snapshot.
    rebuild_again: bool,
    /// Labels of every Namespace object, keyed by namespace name.
    namespace_labels: HashMap<String, HashMap<String, String>>,
    /// Creation time (unix seconds) of every Namespace object.
    namespace_created: HashMap<String, i64>,
}

impl Default for ResourceIngest {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceIngest {
    pub fn new() -> Self {
        Self {
            resource_store: Arc::new(RwLock::new(KubeResourceStore::new())),
            kube_goat: Arc::new(RwLock::new(KubeGoat::new())),
            pending_graph_build: None,
            rebuild_again: false,
            namespace_labels: HashMap::new(),
            namespace_created: HashMap::new(),
        }
    }

    fn store_read(&self) -> std::sync::RwLockReadGuard<'_, KubeResourceStore> {
        self.resource_store
            .read()
            .unwrap_or_else(|p| p.into_inner())
    }

    fn store_write(&self) -> std::sync::RwLockWriteGuard<'_, KubeResourceStore> {
        self.resource_store
            .write()
            .unwrap_or_else(|p| p.into_inner())
    }

    fn goat_read(&self) -> std::sync::RwLockReadGuard<'_, KubeGoat> {
        self.kube_goat.read().unwrap_or_else(|p| p.into_inner())
    }

    fn goat_write(&self) -> std::sync::RwLockWriteGuard<'_, KubeGoat> {
        self.kube_goat.write().unwrap_or_else(|p| p.into_inner())
    }

    // ── Apply ─────────────────────────────────────────────────────────────

    /// Fold a batch of watch events into the store and graph.
    ///
    /// A full list replaces the store and schedules a background graph
    /// rebuild on `handle`; incremental events are applied to both the store
    /// and the live graph immediately.
    pub fn apply(&mut self, msgs: Vec<Watched>, handle: &Handle) -> IngestReport {
        let started = Instant::now();
        let msg_count = msgs.len();
        let mut report = IngestReport::default();

        for watched in msgs {
            match watched {
                Watched::FullResourceList(list) => {
                    self.apply_full_list(list.cluster_id, list.resources, &mut report);
                }
                Watched::NamespaceList(infos) => {
                    debug!(count = infos.len(), "Namespace snapshot received");
                    self.namespace_created = infos
                        .iter()
                        .filter_map(|info| info.created_secs.map(|t| (info.name.clone(), t)))
                        .collect();
                    self.namespace_labels = infos
                        .into_iter()
                        .map(|info| (info.name, info.labels.into_iter().collect()))
                        .collect();
                    report.namespaces_updated = true;
                }
                Watched::DeletedPod(p) => self.apply_delete(KubeResource::Pod(p), &mut report),
                Watched::DeletedDeployment(d) => {
                    self.apply_delete(KubeResource::Deployment(d), &mut report)
                }
                Watched::DeletedService(s) => {
                    self.apply_delete(KubeResource::Service(s), &mut report)
                }
                Watched::DeletedIngress(i) => {
                    self.apply_delete(KubeResource::Ingress(i), &mut report)
                }
                applied @ (Watched::Pod(_)
                | Watched::Deployment(_)
                | Watched::Service(_)
                | Watched::Ingress(_)) => {
                    let resource = KubeResource::try_from(applied)
                        .expect("apply variants convert to KubeResource");
                    self.apply_upsert(resource, &mut report);
                }
            }
        }

        if report.full_list_received {
            self.spawn_graph_build(handle);
        }

        let elapsed = started.elapsed();
        if elapsed.as_millis() > 16 {
            warn!(
                elapsed_us = elapsed.as_micros() as u64,
                msg_count, "ResourceIngest::apply took longer than a frame"
            );
        } else {
            debug!(
                elapsed_us = elapsed.as_micros() as u64,
                msg_count, "ResourceIngest::apply"
            );
        }
        report
    }

    fn apply_full_list(
        &mut self,
        cluster_id: u32,
        resources: Vec<KubeResource>,
        report: &mut IngestReport,
    ) {
        let count = resources.len();
        let mut store = self.store_write();
        // A snapshot replaces everything: resources that vanished while we
        // were disconnected must not linger.
        store.clear();
        store.set_cluster_id(cluster_id);
        let mut ok = 0usize;
        for resource in resources {
            match store.insert(resource) {
                Ok(_) => ok += 1,
                Err(e) => {
                    report.errors += 1;
                    error!(error = %e, "Full list: resource skipped");
                }
            }
        }
        drop(store);
        info!(count, ok, cluster_id, "Full resource list applied");
        report.full_list_received = true;
    }

    fn apply_upsert(&mut self, resource: KubeResource, report: &mut IngestReport) {
        let mut store = self.store_write();
        let (kri, is_new) = match store.insert(resource) {
            Ok(r) => r,
            Err(e) => {
                report.errors += 1;
                error!(error = %e, "Incremental event skipped");
                return;
            }
        };
        // The stored copy is the one the graph sees.
        let resource = store.get_ref(&kri).expect("just inserted");
        let mut goat = self.goat_write();
        goat.upsert_node(&kri, resource, &store);
        if KubeGoat::affects_ns_adjacency(resource) {
            goat.rebuild_ns_adjacency(&store);
        }
        drop(goat);
        drop(store);
        self.note_change_during_build();
        debug!(kri = %kri, is_new, "Incremental resource applied");
        report.updated.push((kri, is_new));
    }

    fn apply_delete(&mut self, resource: KubeResource, report: &mut IngestReport) {
        let mut store = self.store_write();
        let kri = match kri_for(store.cluster_id(), &resource) {
            Ok(kri) => kri,
            Err(e) => {
                report.errors += 1;
                error!(error = %e, "Delete event skipped");
                return;
            }
        };
        if !store.remove(&kri) {
            debug!(kri = %kri, "Delete for unknown resource ignored");
            return;
        }
        let mut goat = self.goat_write();
        goat.remove_node(&kri);
        if KubeGoat::affects_ns_adjacency(&resource) {
            goat.rebuild_ns_adjacency(&store);
        }
        drop(goat);
        drop(store);
        self.note_change_during_build();
        debug!(kri = %kri, "Resource deleted");
        report.deleted.push(kri);
    }

    // ── Graph build ───────────────────────────────────────────────────────

    /// A build in flight snapshotted the store *before* the change that was
    /// just applied to the live graph; the graph it installs would lose it,
    /// so ask for one more build.
    fn note_change_during_build(&mut self) {
        if self.pending_graph_build.is_some() {
            self.rebuild_again = true;
        }
    }

    /// Rebuild the graph from the current store on a blocking thread of
    /// `handle`.  If a build is already in flight, one more is queued.
    fn spawn_graph_build(&mut self, handle: &Handle) {
        if self.pending_graph_build.is_some() {
            debug!("Graph build already in flight — queuing another");
            self.rebuild_again = true;
            return;
        }
        // Resources are Arc-backed, so this snapshot is one reference bump
        // per resource; the build then runs without holding the store lock,
        // so incremental events keep flowing while it does.
        let snapshot = self.store_read().clone();
        self.pending_graph_build = Some(handle.spawn_blocking(move || {
            let started = Instant::now();
            let parts = KubeGoat::build_from_store(&snapshot);
            info!(
                node_count = parts.graph.node_count(),
                edge_count = parts.graph.edge_count(),
                build_ms = started.elapsed().as_millis() as u64,
                "Background graph build complete"
            );
            parts
        }));
    }

    /// Install a finished background build, if any.  Non-blocking; call once
    /// per frame.  Returns `true` when a new graph was swapped in.
    pub fn poll_graph_build(&mut self, handle: &Handle) -> bool {
        let Some(mut join) = self.pending_graph_build.take() else {
            return false;
        };
        let parts = match (&mut join).now_or_never() {
            None => {
                self.pending_graph_build = Some(join);
                return false;
            }
            Some(Ok(parts)) => parts,
            Some(Err(e)) => {
                error!(error = %e, "Background graph build panicked");
                return false;
            }
        };
        self.goat_write().replace_graph(parts);
        if std::mem::take(&mut self.rebuild_again) {
            self.spawn_graph_build(handle);
        }
        true
    }

    /// Build the graph synchronously on the calling thread (tests, tools).
    pub fn rebuild_graph_now(&mut self) {
        let store = self.store_read();
        self.goat_write().rebuild(&store);
    }

    pub fn graph_build_in_flight(&self) -> bool {
        self.pending_graph_build.is_some()
    }

    // ── Getters ───────────────────────────────────────────────────────────

    /// Forget everything: resources, graph, namespace labels and any graph
    /// build in flight (its result is dropped).  For disconnecting before
    /// connecting to another cluster.
    pub fn reset(&mut self) {
        self.pending_graph_build = None;
        self.rebuild_again = false;
        {
            let mut store = self.store_write();
            store.clear();
            store.set_cluster_id(0);
        }
        *self.goat_write() = KubeGoat::new();
        self.namespace_labels.clear();
        self.namespace_created.clear();
        info!("Resource store reset");
    }

    pub fn set_cluster_id(&mut self, id: u32) {
        self.store_write().set_cluster_id(id);
    }

    pub fn cluster_id(&self) -> u32 {
        self.store_read().cluster_id()
    }

    pub fn get_all_resources(&self) -> Vec<(KubeResourceIdentifier, KubeResource)> {
        self.store_read().get_all_resources()
    }

    pub fn get_all_kris(&self) -> Vec<KubeResourceIdentifier> {
        self.store_read().get_all_kris()
    }

    /// Read the store in place (no clones): for views that only need a few
    /// fields of every resource (the list view's rows).  Hold it briefly —
    /// ingest waits for the read lock.
    pub fn with_store<R>(&self, f: impl FnOnce(&KubeResourceStore) -> R) -> R {
        f(&self.store_read())
    }

    pub fn get_resource_by_kri(&self, kri: &KubeResourceIdentifier) -> Option<KubeResource> {
        self.store_read().get(kri)
    }

    /// Known namespace frame keys, sorted.
    pub fn get_all_namespaces(&self) -> Vec<String> {
        let mut namespaces = self.store_read().get_known_namespaces();
        namespaces.sort();
        namespaces
    }

    pub fn get_namespace_labels(&self) -> &HashMap<String, HashMap<String, String>> {
        &self.namespace_labels
    }

    pub fn get_namespace_creation_times(&self) -> &HashMap<String, i64> {
        &self.namespace_created
    }

    pub fn get_namespace_adjacency(
        &self,
    ) -> HashMap<KubeResourceIdentifier, std::collections::HashSet<KubeResourceIdentifier>> {
        self.goat_read().ns_adjacency().clone()
    }

    pub fn get_affected_namespaces(
        &self,
        ns_kri: &KubeResourceIdentifier,
    ) -> Vec<KubeResourceIdentifier> {
        self.goat_read().affected_namespaces(ns_kri)
    }

    pub fn get_edges_for_kri(
        &self,
        kri: &KubeResourceIdentifier,
    ) -> Option<Vec<KubeResourceIdentifier>> {
        self.goat_read().connected_component(kri)
    }

    pub fn get_outgoing_edges_for_kri(
        &self,
        kri: &KubeResourceIdentifier,
    ) -> Option<Vec<(KubeResourceIdentifier, EdgeType)>> {
        self.goat_read().get_outgoing_edges(kri)
    }

    pub fn graph_node_count(&self) -> usize {
        self.goat_read().node_count()
    }

    pub fn graph_edge_count(&self) -> usize {
        self.goat_read().edge_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::{NamespaceInfo, WatchedResources};
    use k8s_openapi::api::core::v1::{Pod, Service, ServiceSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;

    fn labels() -> BTreeMap<String, String> {
        BTreeMap::from([("app".to_string(), "web".to_string())])
    }

    fn pod(ns: &str, name: &str) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some(ns.into()),
                labels: Some(labels()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn service(ns: &str, name: &str) -> Service {
        Service {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some(ns.into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                selector: Some(labels()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn full(cluster_id: u32, resources: Vec<KubeResource>) -> Watched {
        Watched::FullResourceList(WatchedResources {
            resources,
            cluster_id,
        })
    }

    /// Drive a build to completion on the test runtime.
    fn settle(ingest: &mut ResourceIngest, handle: &Handle) {
        for _ in 0..200 {
            if ingest.poll_graph_build(handle) && !ingest.graph_build_in_flight() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        panic!("graph build never finished");
    }

    #[test]
    fn reset_forgets_everything_and_drops_builds_in_flight() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle().clone();
        let mut ingest = ResourceIngest::new();
        ingest.apply(
            vec![
                Watched::NamespaceList(vec![NamespaceInfo {
                    name: "ns".into(),
                    labels: BTreeMap::new(),
                    created_secs: None,
                }]),
                full(
                    7,
                    vec![
                        KubeResource::Pod(pod("ns", "p1").into()),
                        KubeResource::Service(service("ns", "s").into()),
                    ],
                ),
            ],
            &handle,
        );
        settle(&mut ingest, &handle);
        // A build in flight when the reset lands must not resurrect anything.
        ingest.apply(
            vec![full(7, vec![KubeResource::Pod(pod("ns", "p2").into())])],
            &handle,
        );
        ingest.reset();
        assert!(!ingest.graph_build_in_flight());
        assert!(ingest.get_all_kris().is_empty());
        assert_eq!(ingest.cluster_id(), 0);
        assert_eq!(ingest.graph_node_count(), 0);
        assert!(ingest.get_namespace_labels().is_empty());
        assert!(!ingest.poll_graph_build(&handle));

        // The next session fills it again.
        ingest.apply(
            vec![full(8, vec![KubeResource::Pod(pod("other", "q").into())])],
            &handle,
        );
        settle(&mut ingest, &handle);
        assert_eq!(ingest.cluster_id(), 8);
        assert_eq!(ingest.graph_node_count(), 1);
    }

    #[test]
    fn full_list_replaces_store_and_rebuilds_graph() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle().clone();
        let mut ingest = ResourceIngest::new();

        let r = ingest.apply(
            vec![full(
                7,
                vec![
                    KubeResource::Pod(pod("ns", "p1").into()),
                    KubeResource::Service(service("ns", "s").into()),
                ],
            )],
            &handle,
        );
        assert!(r.full_list_received);
        assert_eq!(ingest.cluster_id(), 7);
        assert_eq!(ingest.get_all_kris().len(), 2);
        settle(&mut ingest, &handle);
        assert_eq!(ingest.graph_node_count(), 2);
        assert_eq!(ingest.graph_edge_count(), 1);

        // A later snapshot without p1 drops it everywhere.
        let r = ingest.apply(
            vec![full(
                7,
                vec![KubeResource::Service(service("ns", "s").into())],
            )],
            &handle,
        );
        assert!(r.full_list_received);
        assert_eq!(ingest.get_all_kris().len(), 1);
        assert_eq!(ingest.get_all_namespaces(), vec!["ns".to_string()]);
        settle(&mut ingest, &handle);
        assert_eq!(ingest.graph_node_count(), 1);
        assert_eq!(ingest.graph_edge_count(), 0);
    }

    #[test]
    fn incremental_events_update_store_and_graph_immediately() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle().clone();
        let mut ingest = ResourceIngest::new();
        ingest.set_cluster_id(3);

        let r = ingest.apply(vec![Watched::Service(service("ns", "s").into())], &handle);
        assert_eq!(r.updated.len(), 1);
        assert!(r.updated[0].1, "new");
        assert_eq!(r.updated[0].0.cluster_id, 3);
        assert_eq!(ingest.graph_node_count(), 1);

        let r = ingest.apply(vec![Watched::Pod(pod("ns", "p").into())], &handle);
        assert_eq!(r.updated.len(), 1);
        assert_eq!(ingest.graph_edge_count(), 1, "service selects the new pod");

        let r = ingest.apply(vec![Watched::Pod(pod("ns", "p").into())], &handle);
        assert!(!r.updated[0].1, "update, not new");
        assert_eq!(ingest.graph_edge_count(), 1, "no duplicate edge on update");

        let r = ingest.apply(vec![Watched::DeletedPod(pod("ns", "p").into())], &handle);
        assert_eq!(r.deleted.len(), 1);
        assert_eq!(ingest.graph_node_count(), 1);
        assert_eq!(ingest.graph_edge_count(), 0);
        assert!(ingest.get_resource_by_kri(&r.deleted[0]).is_none());

        let r = ingest.apply(vec![Watched::DeletedPod(pod("ns", "p").into())], &handle);
        assert!(r.deleted.is_empty(), "second delete is ignored");
        assert!(!ingest.graph_build_in_flight());
    }

    #[test]
    fn deleting_a_service_drops_its_edges() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle().clone();
        let mut ingest = ResourceIngest::new();
        ingest.apply(
            vec![full(
                1,
                vec![
                    KubeResource::Pod(pod("ns", "p").into()),
                    KubeResource::Service(service("ns", "s").into()),
                ],
            )],
            &handle,
        );
        settle(&mut ingest, &handle);
        assert_eq!(
            (ingest.graph_node_count(), ingest.graph_edge_count()),
            (2, 1)
        );

        let r = ingest.apply(
            vec![Watched::DeletedService(service("ns", "s").into())],
            &handle,
        );
        assert_eq!(r.deleted.len(), 1);
        assert_eq!(
            (ingest.graph_node_count(), ingest.graph_edge_count()),
            (1, 0)
        );
        let pod_kri = ingest.get_all_kris().remove(0);
        assert_eq!(pod_kri.name, "p");
        assert_eq!(
            ingest.get_edges_for_kri(&pod_kri).unwrap().len(),
            1,
            "pod alone"
        );

        // The service coming back re-derives the edge from the pod's side.
        ingest.apply(vec![Watched::Service(service("ns", "s").into())], &handle);
        assert_eq!(
            (ingest.graph_node_count(), ingest.graph_edge_count()),
            (2, 1)
        );
    }

    #[test]
    fn namespace_snapshot_and_errors_are_reported() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle().clone();
        let mut ingest = ResourceIngest::new();
        let r = ingest.apply(
            vec![
                Watched::NamespaceList(vec![NamespaceInfo {
                    name: "ns".into(),
                    labels: BTreeMap::from([("team".to_string(), "a".to_string())]),
                    created_secs: Some(5),
                }]),
                Watched::Pod(Pod::default().into()), // no name
            ],
            &handle,
        );
        assert!(r.namespaces_updated);
        assert_eq!(r.errors, 1);
        assert_eq!(ingest.get_namespace_labels()["ns"]["team"], "a");
        assert_eq!(ingest.get_namespace_creation_times()["ns"], 5);
        assert!(IngestReport::default().is_empty());
        assert!(!r.is_empty());
    }

    #[test]
    fn incremental_during_build_is_not_lost() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle().clone();
        let mut ingest = ResourceIngest::new();
        // The full list spawns a build that snapshots the store.  Let it
        // finish (but not be installed — nothing has polled yet) so the
        // snapshot certainly predates the incremental.
        ingest.apply(
            vec![full(1, vec![KubeResource::Pod(pod("a", "p").into())])],
            &handle,
        );
        built(&ingest);
        // An incremental applied now goes into the live graph; installing
        // the snapshot must not throw it away.
        ingest.apply(vec![Watched::Pod(pod("a", "q").into())], &handle);
        settle(&mut ingest, &handle);
        assert_eq!(ingest.get_all_kris().len(), 2);
        assert_eq!(ingest.graph_node_count(), 2, "upsert during build kept");

        ingest.apply(
            vec![full(1, vec![KubeResource::Pod(pod("a", "p").into())])],
            &handle,
        );
        built(&ingest);
        ingest.apply(vec![Watched::DeletedPod(pod("a", "p").into())], &handle);
        settle(&mut ingest, &handle);
        assert_eq!(ingest.graph_node_count(), 0, "delete during build kept");
    }

    /// Wait for the in-flight build to have run (without installing it).
    fn built(ingest: &ResourceIngest) {
        let join = ingest
            .pending_graph_build
            .as_ref()
            .expect("build in flight");
        for _ in 0..400 {
            if join.is_finished() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        panic!("graph build never ran");
    }

    #[test]
    fn full_list_during_build_queues_a_second_build() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle().clone();
        let mut ingest = ResourceIngest::new();
        ingest.apply(
            vec![full(1, vec![KubeResource::Pod(pod("a", "p").into())])],
            &handle,
        );
        ingest.apply(
            vec![full(1, vec![KubeResource::Pod(pod("b", "q").into())])],
            &handle,
        );
        settle(&mut ingest, &handle);
        let kris = ingest.get_all_kris();
        assert_eq!(kris.len(), 1);
        assert_eq!(kris[0].name, "q");
        assert_eq!(ingest.graph_node_count(), 1);
    }
}
