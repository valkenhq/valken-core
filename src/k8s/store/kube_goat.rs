//! KubeGoat — the Kube Graph of All Things.
//!
//! A directed graph whose nodes are [`KubeResourceIdentifier`]s and whose
//! edges describe how resources relate:
//!
//! | Edge       | From       | To      | Derived from                              |
//! |------------|------------|---------|-------------------------------------------|
//! | `Owns`     | owner      | Pod     | the pod's first `ownerReference`          |
//! | `Owns`     | Deployment | Pod     | `spec.selector.matchLabels` ⊆ pod labels  |
//! | `Selects`  | Service    | Pod     | `spec.selector` ⊆ pod labels              |
//! | `RoutesTo` | Ingress    | Service | `spec.rules[].http.paths[].backend.service` |
//!
//! Relations never cross namespaces.  The graph can be rebuilt from a
//! [`KubeResourceStore`] in one pass ([`KubeGoat::build_from_store`]), or
//! maintained incrementally ([`KubeGoat::upsert_node`] /
//! [`KubeGoat::remove_node`]) as watch events arrive.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::io::{self, Write};
use std::path::Path;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Pod, Service};
use k8s_openapi::api::networking::v1::Ingress;
use petgraph::dot::Dot;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableDiGraph;
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use tracing::{debug, trace};

use super::{KubeResource, KubeResourceIdentifier, KubeResourceStore};
use crate::k8s::ingress_controller::{
    detect_ingress_controller_from_deployment, detect_ingress_controller_from_pod,
};

/// Why two resources are connected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EdgeType {
    /// Deployment → Pod (by selector or ownerReference), Pod → Pod owner.
    Owns,
    /// Service → Pod by label selector.
    Selects,
    /// Ingress → backend Service.
    RoutesTo,
}

impl std::fmt::Display for EdgeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// The resource graph.  `StableDiGraph` keeps node indices valid across
/// removals, which `node_indices` relies on.
pub(crate) type ResourceGraph = StableDiGraph<KubeResourceIdentifier, EdgeType>;
/// Namespace KRI → adjacent namespace KRIs.
pub type NsAdjacency = HashMap<KubeResourceIdentifier, HashSet<KubeResourceIdentifier>>;

/// Graph, node index and namespace adjacency built together from the store.
#[derive(Debug, Default)]
pub(crate) struct GraphParts {
    pub graph: ResourceGraph,
    pub node_indices: HashMap<KubeResourceIdentifier, NodeIndex>,
    pub ns_adjacency: NsAdjacency,
}

/// Result of [`KubeGoat::upsert_node`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Upsert {
    Inserted,
    Updated,
}

#[derive(Debug, Default)]
pub struct KubeGoat {
    graph: ResourceGraph,
    node_indices: HashMap<KubeResourceIdentifier, NodeIndex>,
    /// Namespace-level adjacency index, built from Ingress routing.  Keyed
    /// by namespace KRI so it stays correct across clusters.
    ns_adjacency: NsAdjacency,
}

// ---------------------------------------------------------------------------
// Relation helpers (pure)
// ---------------------------------------------------------------------------

/// Split an `apiVersion` into `(group, version)`; core resources (`"v1"`)
/// get the group `core`, matching [`KubeResourceIdentifier::from_pod`].
pub(crate) fn parse_api_version(api_version: &str) -> (&str, &str) {
    match api_version.split_once('/') {
        Some((group, version)) => (group, version),
        None => ("core", api_version),
    }
}

/// `matchLabels` semantics: every selector pair must be present on the
/// labels.  An empty selector selects nothing (a Service without a
/// selector is manually endpointed; a Deployment selector is never empty).
fn selector_matches(
    selector: &BTreeMap<String, String>,
    labels: Option<&BTreeMap<String, String>>,
) -> bool {
    if selector.is_empty() {
        return false;
    }
    match labels {
        Some(labels) => selector.iter().all(|(k, v)| labels.get(k) == Some(v)),
        None => false,
    }
}

fn service_selector(svc: &Service) -> Option<&BTreeMap<String, String>> {
    svc.spec.as_ref()?.selector.as_ref()
}

fn deployment_selector(dep: &Deployment) -> Option<&BTreeMap<String, String>> {
    dep.spec.as_ref()?.selector.match_labels.as_ref()
}

fn ingress_backend_services(ing: &Ingress) -> impl Iterator<Item = &str> {
    ing.spec
        .iter()
        .flat_map(|spec| spec.rules.iter().flatten())
        .filter_map(|rule| rule.http.as_ref())
        .flat_map(|http| http.paths.iter())
        .filter_map(|path| path.backend.service.as_ref())
        .map(|svc| svc.name.as_str())
}

/// KRI of the pod's first owner, in the pod's namespace.
fn pod_owner_kri(kri: &KubeResourceIdentifier, pod: &Pod) -> Option<KubeResourceIdentifier> {
    let owner = pod.metadata.owner_references.as_ref()?.first()?;
    let (group, version) = parse_api_version(&owner.api_version);
    Some(KubeResourceIdentifier::new(
        kri.cluster_id,
        kri.namespace.clone(),
        group,
        version,
        &owner.kind,
        &owner.name,
    ))
}

fn service_kri(kri: &KubeResourceIdentifier, name: &str) -> KubeResourceIdentifier {
    KubeResourceIdentifier::new(
        kri.cluster_id,
        kri.namespace.clone(),
        "core",
        "v1",
        "Service",
        name,
    )
}

type Edge = (KubeResourceIdentifier, KubeResourceIdentifier, EdgeType);

/// Edges in which `resource` (at `kri`) takes part, given the other
/// resources of the same namespace frame.
///
/// With `forward_only` each relation is reported from one side only
/// (Service→Pod from the Service, Ingress→Service from the Ingress, …), so a
/// full build visiting every resource finds each edge exactly once.  Without
/// it both sides are reported, which an incremental upsert needs because its
/// peers are not revisited.
fn edges_for<'a>(
    kri: &KubeResourceIdentifier,
    resource: &KubeResource,
    peers: impl Iterator<Item = (&'a KubeResourceIdentifier, &'a KubeResource)>,
    forward_only: bool,
) -> Vec<Edge> {
    let mut out = Vec::new();
    match resource {
        KubeResource::Pod(pod) => {
            if let Some(owner) = pod_owner_kri(kri, pod) {
                out.push((owner, kri.clone(), EdgeType::Owns));
            }
            if !forward_only {
                let labels = pod.metadata.labels.as_ref();
                for (peer_kri, peer) in peers {
                    match peer {
                        KubeResource::Service(svc)
                            if service_selector(svc)
                                .is_some_and(|s| selector_matches(s, labels)) =>
                        {
                            out.push((peer_kri.clone(), kri.clone(), EdgeType::Selects));
                        }
                        KubeResource::Deployment(dep)
                            if deployment_selector(dep)
                                .is_some_and(|s| selector_matches(s, labels)) =>
                        {
                            out.push((peer_kri.clone(), kri.clone(), EdgeType::Owns));
                        }
                        _ => {}
                    }
                }
            }
        }
        KubeResource::Service(svc) => {
            let selector = service_selector(svc);
            for (peer_kri, peer) in peers {
                match peer {
                    KubeResource::Pod(pod) => {
                        if selector
                            .is_some_and(|s| selector_matches(s, pod.metadata.labels.as_ref()))
                        {
                            out.push((kri.clone(), peer_kri.clone(), EdgeType::Selects));
                        }
                    }
                    KubeResource::Ingress(ing)
                        if !forward_only
                            && ingress_backend_services(ing).any(|name| name == kri.name) =>
                    {
                        out.push((peer_kri.clone(), kri.clone(), EdgeType::RoutesTo));
                    }
                    _ => {}
                }
            }
        }
        KubeResource::Deployment(dep) => {
            let selector = deployment_selector(dep);
            for (peer_kri, peer) in peers {
                if let KubeResource::Pod(pod) = peer {
                    let selected =
                        selector.is_some_and(|s| selector_matches(s, pod.metadata.labels.as_ref()));
                    let owned = !forward_only && pod_owner_kri(peer_kri, pod).as_ref() == Some(kri);
                    if selected || owned {
                        out.push((kri.clone(), peer_kri.clone(), EdgeType::Owns));
                    }
                }
            }
        }
        KubeResource::Ingress(ing) => {
            let names: BTreeSet<&str> = ingress_backend_services(ing).collect();
            for name in names {
                out.push((kri.clone(), service_kri(kri, name), EdgeType::RoutesTo));
            }
        }
        KubeResource::CustomResource { .. } => {}
        #[cfg(test)]
        KubeResource::Dummy(_) => {}
    }
    out
}

// ---------------------------------------------------------------------------
// KubeGoat
// ---------------------------------------------------------------------------

impl KubeGoat {
    pub fn new() -> Self {
        Self::default()
    }

    /// Install a graph built by [`KubeGoat::build_from_store`] on another
    /// thread.  Just a pointer swap; no computation happens here.
    pub(crate) fn replace_graph(&mut self, parts: GraphParts) {
        self.graph = parts.graph;
        self.node_indices = parts.node_indices;
        self.ns_adjacency = parts.ns_adjacency;
        debug!(
            node_count = self.graph.node_count(),
            edge_count = self.graph.edge_count(),
            adj_count = self.ns_adjacency.len(),
            "KubeGoat: graph installed"
        );
    }

    /// Rebuild this graph in place from `store`.
    pub fn rebuild(&mut self, store: &KubeResourceStore) {
        self.replace_graph(Self::build_from_store(store));
    }

    /// Build graph parts from a store in one pass.  Safe to call from a
    /// background thread while holding only a read guard on the store.
    pub(crate) fn build_from_store(store: &KubeResourceStore) -> GraphParts {
        let mut graph = ResourceGraph::default();
        let mut node_indices: HashMap<KubeResourceIdentifier, NodeIndex> =
            HashMap::with_capacity(store.len());

        // Group by namespace frame so relation checks only see peers.
        let mut by_frame: HashMap<&str, Vec<(&KubeResourceIdentifier, &KubeResource)>> =
            HashMap::new();
        for (kri, resource) in store.iter() {
            node_indices.insert(kri.clone(), graph.add_node(kri.clone()));
            by_frame
                .entry(kri.frame_key())
                .or_default()
                .push((kri, resource));
        }

        let mut seen: HashSet<(NodeIndex, NodeIndex, EdgeType)> = HashSet::new();
        for peers in by_frame.values() {
            for (kri, resource) in peers {
                for (from, to, ty) in edges_for(kri, resource, peers.iter().copied(), true) {
                    if let (Some(&a), Some(&b)) = (node_indices.get(&from), node_indices.get(&to)) {
                        if seen.insert((a, b, ty)) {
                            graph.add_edge(a, b, ty);
                        }
                    }
                }
            }
        }

        let ns_adjacency = build_ns_adjacency(store, store.cluster_id());
        debug!(
            node_count = graph.node_count(),
            edge_count = graph.edge_count(),
            "KubeGoat: graph built from store"
        );
        GraphParts {
            graph,
            node_indices,
            ns_adjacency,
        }
    }

    /// Insert or update one node and recompute every edge it takes part in.
    /// `store` supplies the resource's namespace peers.
    pub fn upsert_node(
        &mut self,
        kri: &KubeResourceIdentifier,
        resource: &KubeResource,
        store: &KubeResourceStore,
    ) -> Upsert {
        let (index, kind) = match self.node_indices.get(kri) {
            Some(&index) => {
                let edges: Vec<_> = self
                    .graph
                    .edges_directed(index, Direction::Outgoing)
                    .chain(self.graph.edges_directed(index, Direction::Incoming))
                    .map(|e| e.id())
                    .collect();
                for e in edges {
                    self.graph.remove_edge(e);
                }
                (index, Upsert::Updated)
            }
            None => {
                let index = self.graph.add_node(kri.clone());
                self.node_indices.insert(kri.clone(), index);
                (index, Upsert::Inserted)
            }
        };

        let peers = store.iter_frame(kri.frame_key()).filter(|(k, _)| *k != kri);
        let mut added = 0usize;
        for (from, to, ty) in edges_for(kri, resource, peers, false) {
            let (Some(&a), Some(&b)) = (self.node_indices.get(&from), self.node_indices.get(&to))
            else {
                continue;
            };
            if self.graph.edges_connecting(a, b).any(|e| *e.weight() == ty) {
                continue;
            }
            self.graph.add_edge(a, b, ty);
            added += 1;
        }
        trace!(kri = %kri, ?kind, edges = added, index = index.index(), "KubeGoat: node upserted");
        kind
    }

    /// Remove a node and all its edges.  Returns `true` if it was present.
    pub fn remove_node(&mut self, kri: &KubeResourceIdentifier) -> bool {
        match self.node_indices.remove(kri) {
            Some(index) => {
                self.graph.remove_node(index);
                true
            }
            None => false,
        }
    }

    /// Recompute the namespace adjacency index from `store`.
    pub fn rebuild_ns_adjacency(&mut self, store: &KubeResourceStore) {
        self.ns_adjacency = build_ns_adjacency(store, store.cluster_id());
    }

    /// Whether a change to `resource` can affect the namespace adjacency
    /// index (it is an Ingress or an ingress-controller workload).
    pub fn affects_ns_adjacency(resource: &KubeResource) -> bool {
        match resource {
            KubeResource::Ingress(_) => true,
            KubeResource::Pod(p) => detect_ingress_controller_from_pod(p).is_some(),
            KubeResource::Deployment(d) => detect_ingress_controller_from_deployment(d).is_some(),
            _ => false,
        }
    }

    pub fn contains(&self, kri: &KubeResourceIdentifier) -> bool {
        self.node_indices.contains_key(kri)
    }

    /// Namespace-level adjacency index.  Keys and values are Namespace KRIs
    /// produced by [`KubeResourceIdentifier::for_namespace`].
    pub fn ns_adjacency(&self) -> &NsAdjacency {
        &self.ns_adjacency
    }

    /// All namespace KRIs directly connected to `ns_kri`, sorted by name.
    pub fn affected_namespaces(
        &self,
        ns_kri: &KubeResourceIdentifier,
    ) -> Vec<KubeResourceIdentifier> {
        self.ns_adjacency
            .get(ns_kri)
            .map(|s| {
                let mut v: Vec<_> = s.iter().cloned().collect();
                v.sort_by(|a, b| a.name.cmp(&b.name));
                v
            })
            .unwrap_or_default()
    }

    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    /// Every node reachable from `kri` following edges in either direction,
    /// including `kri` itself.  `None` if the node is unknown.
    pub fn connected_component(
        &self,
        kri: &KubeResourceIdentifier,
    ) -> Option<Vec<KubeResourceIdentifier>> {
        let &start = self.node_indices.get(kri)?;
        let mut visited = HashSet::from([start]);
        let mut queue = VecDeque::from([start]);
        let mut out = Vec::new();
        while let Some(current) = queue.pop_front() {
            out.push(self.graph[current].clone());
            for neighbor in self
                .graph
                .neighbors_directed(current, Direction::Outgoing)
                .chain(self.graph.neighbors_directed(current, Direction::Incoming))
            {
                if visited.insert(neighbor) {
                    queue.push_back(neighbor);
                }
            }
        }
        Some(out)
    }

    /// Alias for [`KubeGoat::connected_component`], kept for existing callers.
    #[deprecated(note = "use connected_component")]
    pub fn get_edges_for_node(
        &self,
        kri: &KubeResourceIdentifier,
    ) -> Option<Vec<KubeResourceIdentifier>> {
        self.connected_component(kri)
    }

    /// Outgoing edges from a node with their edge types and target nodes.
    pub fn get_outgoing_edges(
        &self,
        kri: &KubeResourceIdentifier,
    ) -> Option<Vec<(KubeResourceIdentifier, EdgeType)>> {
        let &index = self.node_indices.get(kri)?;
        Some(
            self.graph
                .edges_directed(index, Direction::Outgoing)
                .map(|edge| (self.graph[edge.target()].clone(), *edge.weight()))
                .collect(),
        )
    }

    /// Write the current graph as DOT to `path`.
    pub fn write_graph_to_dot_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        Self::write_graph_to_file(&self.graph, path)
    }

    /// Write a graph as DOT to `path`.
    pub(crate) fn write_graph_to_file<N, E, P>(
        graph: &StableDiGraph<N, E>,
        path: P,
    ) -> io::Result<()>
    where
        N: std::fmt::Display,
        E: std::fmt::Display,
        P: AsRef<Path>,
    {
        let mut file = std::fs::File::create(path)?;
        write!(file, "{}", Dot::with_config(graph, &[]))
    }
}

/// Build a namespace KRI → adjacent namespace KRIs index from Ingress
/// resources in the store.  The ingress-controller namespace (first in sorted
/// order when several are detected) is linked bidirectionally to every
/// namespace that owns an Ingress.  O(R): one pass over the store.
fn build_ns_adjacency(store: &KubeResourceStore, cluster_id: u32) -> NsAdjacency {
    let mut controller_candidates: BTreeSet<&str> = BTreeSet::new();
    let mut ingress_namespaces: BTreeSet<&str> = BTreeSet::new();

    for (kri, resource) in store.iter() {
        let Some(ns) = kri.namespace.as_deref() else {
            continue;
        };
        match resource {
            KubeResource::Pod(p) if detect_ingress_controller_from_pod(p).is_some() => {
                controller_candidates.insert(ns);
            }
            KubeResource::Deployment(d)
                if detect_ingress_controller_from_deployment(d).is_some() =>
            {
                controller_candidates.insert(ns);
            }
            KubeResource::Ingress(_) => {
                ingress_namespaces.insert(ns);
            }
            _ => {}
        }
    }

    let mut adj = NsAdjacency::new();
    let Some(ctrl_ns) = controller_candidates.into_iter().next() else {
        return adj;
    };
    let ctrl_kri = KubeResourceIdentifier::for_namespace(cluster_id, ctrl_ns);
    for ing_ns in ingress_namespaces {
        if ing_ns == ctrl_ns {
            continue; // self-adjacency not useful
        }
        let ing_kri = KubeResourceIdentifier::for_namespace(cluster_id, ing_ns);
        adj.entry(ctrl_kri.clone())
            .or_default()
            .insert(ing_kri.clone());
        adj.entry(ing_kri).or_default().insert(ctrl_kri.clone());
    }
    adj
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec};
    use k8s_openapi::api::core::v1::{Container, Pod, PodSpec, Service, ServicePort, ServiceSpec};
    use k8s_openapi::api::networking::v1::{
        HTTPIngressPath, HTTPIngressRuleValue, Ingress, IngressBackend, IngressRule,
        IngressServiceBackend, IngressSpec, ServiceBackendPort,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
        LabelSelector, ObjectMeta, OwnerReference,
    };
    use petgraph::visit::IntoEdgeReferences;

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn pod(name: &str, ns: &str, labels: Option<BTreeMap<String, String>>) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some(ns.into()),
                labels,
                ..Default::default()
            },
            spec: Some(PodSpec::default()),
            ..Default::default()
        }
    }

    fn pod_with_owner(name: &str, ns: &str, owner: &str, kind: &str, api_version: &str) -> Pod {
        let mut p = pod(name, ns, None);
        p.metadata.owner_references = Some(vec![OwnerReference {
            api_version: api_version.into(),
            kind: kind.into(),
            name: owner.into(),
            uid: "uid".into(),
            ..Default::default()
        }]);
        p
    }

    fn service(name: &str, ns: &str, selector: Option<BTreeMap<String, String>>) -> Service {
        Service {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some(ns.into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                selector,
                ports: Some(vec![ServicePort {
                    port: 80,
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn deployment(
        name: &str,
        ns: &str,
        match_labels: Option<BTreeMap<String, String>>,
    ) -> Deployment {
        Deployment {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some(ns.into()),
                ..Default::default()
            },
            spec: Some(DeploymentSpec {
                selector: LabelSelector {
                    match_labels,
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn ingress(name: &str, ns: &str, services: &[&str]) -> Ingress {
        Ingress {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some(ns.into()),
                ..Default::default()
            },
            spec: Some(IngressSpec {
                rules: Some(vec![IngressRule {
                    host: Some("example.com".into()),
                    http: Some(HTTPIngressRuleValue {
                        paths: services
                            .iter()
                            .map(|svc| HTTPIngressPath {
                                backend: IngressBackend {
                                    service: Some(IngressServiceBackend {
                                        name: svc.to_string(),
                                        port: Some(ServiceBackendPort {
                                            number: Some(80),
                                            ..Default::default()
                                        }),
                                    }),
                                    ..Default::default()
                                },
                                path: Some("/".into()),
                                path_type: "Prefix".into(),
                            })
                            .collect(),
                    }),
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn store_of(resources: Vec<KubeResource>) -> KubeResourceStore {
        let mut store = KubeResourceStore::new();
        for r in resources {
            store.insert(r).unwrap();
        }
        store
    }

    fn built(resources: Vec<KubeResource>) -> (KubeGoat, KubeResourceStore) {
        let store = store_of(resources);
        let mut goat = KubeGoat::new();
        goat.rebuild(&store);
        (goat, store)
    }

    fn edge_set(goat: &KubeGoat) -> BTreeSet<(String, String, String)> {
        goat.graph
            .edge_references()
            .map(|e| {
                (
                    goat.graph[e.source()].name.clone(),
                    goat.graph[e.target()].name.clone(),
                    e.weight().to_string(),
                )
            })
            .collect()
    }

    fn e(from: &str, to: &str, ty: &str) -> (String, String, String) {
        (from.into(), to.into(), ty.into())
    }

    // ── relation helpers ──────────────────────────────────────────────────

    #[test]
    fn api_version_parsing_defaults_core_group() {
        assert_eq!(parse_api_version("apps/v1"), ("apps", "v1"));
        assert_eq!(parse_api_version("v1"), ("core", "v1"));
        assert_eq!(
            parse_api_version("cert-manager.io/v1"),
            ("cert-manager.io", "v1")
        );
    }

    #[test]
    fn empty_selector_matches_nothing() {
        let l = labels(&[("app", "x")]);
        assert!(!selector_matches(&BTreeMap::new(), Some(&l)));
        assert!(!selector_matches(&l, None));
        assert!(selector_matches(
            &l,
            Some(&labels(&[("app", "x"), ("tier", "web")]))
        ));
        assert!(!selector_matches(&labels(&[("app", "y")]), Some(&l)));
    }

    // ── full build ────────────────────────────────────────────────────────

    #[test]
    fn empty_store_builds_empty_graph() {
        let (goat, _) = built(vec![]);
        assert_eq!(goat.node_count(), 0);
        assert_eq!(goat.edge_count(), 0);
    }

    #[test]
    fn full_stack_builds_three_edges() {
        let l = labels(&[("app", "web")]);
        let (goat, _) = built(vec![
            KubeResource::Deployment(deployment("web", "default", Some(l.clone())).into()),
            KubeResource::Pod(pod("web-1", "default", Some(l.clone())).into()),
            KubeResource::Service(service("web", "default", Some(l)).into()),
            KubeResource::Ingress(ingress("web", "default", &["web"]).into()),
        ]);
        assert_eq!(goat.node_count(), 4);
        assert_eq!(
            edge_set(&goat),
            BTreeSet::from([
                e("web", "web-1", "Owns"),
                e("web", "web-1", "Selects"),
                e("web", "web", "RoutesTo"),
            ])
        );
    }

    #[test]
    fn relations_never_cross_namespaces() {
        let l = labels(&[("app", "web")]);
        let (goat, _) = built(vec![
            KubeResource::Pod(pod("web-1", "team-b", Some(l.clone())).into()),
            KubeResource::Service(service("web", "team-a", Some(l.clone())).into()),
            KubeResource::Deployment(deployment("web", "team-a", Some(l)).into()),
            KubeResource::Ingress(ingress("web", "team-b", &["web"]).into()),
        ]);
        assert_eq!(goat.node_count(), 4);
        assert_eq!(goat.edge_count(), 0, "{:?}", edge_set(&goat));
    }

    #[test]
    fn owner_reference_edge_in_same_namespace_only() {
        let (goat, _) = built(vec![
            KubeResource::Deployment(deployment("d", "ns", None).into()),
            KubeResource::Pod(pod_with_owner("p", "ns", "d", "Deployment", "apps/v1").into()),
            KubeResource::Pod(pod_with_owner("q", "other", "d", "Deployment", "apps/v1").into()),
        ]);
        assert_eq!(edge_set(&goat), BTreeSet::from([e("d", "p", "Owns")]));
    }

    #[test]
    fn selector_and_owner_reference_to_same_owner_yield_one_edge() {
        let l = labels(&[("app", "web")]);
        let mut p = pod_with_owner("p", "ns", "d", "Deployment", "apps/v1");
        p.metadata.labels = Some(l.clone());
        let (goat, _) = built(vec![
            KubeResource::Deployment(deployment("d", "ns", Some(l)).into()),
            KubeResource::Pod(p.into()),
        ]);
        assert_eq!(goat.edge_count(), 1);
    }

    #[test]
    fn core_owner_api_version_builds_the_same_kri_as_from_pod() {
        // A pod "owned" by a Pod (apiVersion "v1") must resolve to the
        // owner's from_pod KRI, which uses the "core" group.
        let owner = pod("owner", "ns", None);
        let (goat, _) = built(vec![
            KubeResource::Pod(owner.into()),
            KubeResource::Pod(pod_with_owner("child", "ns", "owner", "Pod", "v1").into()),
        ]);
        assert_eq!(
            edge_set(&goat),
            BTreeSet::from([e("owner", "child", "Owns")])
        );
    }

    #[test]
    fn multiple_pods_and_multiple_ingress_paths() {
        let l = labels(&[("app", "shared")]);
        let (goat, _) = built(vec![
            KubeResource::Pod(pod("p1", "ns", Some(l.clone())).into()),
            KubeResource::Pod(pod("p2", "ns", Some(l.clone())).into()),
            KubeResource::Service(service("s1", "ns", Some(l.clone())).into()),
            KubeResource::Service(service("s2", "ns", None).into()),
            KubeResource::Deployment(deployment("d", "ns", Some(l)).into()),
            KubeResource::Ingress(ingress("i", "ns", &["s1", "s2", "missing"]).into()),
        ]);
        assert_eq!(goat.node_count(), 6);
        assert_eq!(
            edge_set(&goat),
            BTreeSet::from([
                e("s1", "p1", "Selects"),
                e("s1", "p2", "Selects"),
                e("d", "p1", "Owns"),
                e("d", "p2", "Owns"),
                e("i", "s1", "RoutesTo"),
                e("i", "s2", "RoutesTo"),
            ])
        );
    }

    #[test]
    fn unlabeled_pod_and_selectorless_service_have_no_edges() {
        let (goat, _) = built(vec![
            KubeResource::Pod(pod("p", "ns", None).into()),
            KubeResource::Service(service("s", "ns", Some(labels(&[("app", "x")]))).into()),
            KubeResource::Service(service("headless", "ns", None).into()),
            KubeResource::Deployment(deployment("d", "ns", None).into()),
        ]);
        assert_eq!(goat.edge_count(), 0);
    }

    // ── incremental ───────────────────────────────────────────────────────

    #[test]
    fn upsert_into_empty_graph_works_without_a_build() {
        let mut goat = KubeGoat::new();
        let mut store = KubeResourceStore::new();
        let p = KubeResource::Pod(pod("p", "ns", None).into());
        let (kri, _) = store.insert(p.clone()).unwrap();
        assert_eq!(goat.upsert_node(&kri, &p, &store), Upsert::Inserted);
        assert_eq!(goat.upsert_node(&kri, &p, &store), Upsert::Updated);
        assert_eq!(goat.node_count(), 1);
    }

    #[test]
    fn upsert_builds_edges_in_both_directions() {
        let l = labels(&[("app", "web")]);
        let (mut goat, mut store) = built(vec![
            KubeResource::Deployment(deployment("d", "ns", Some(l.clone())).into()),
            KubeResource::Service(service("s", "ns", Some(l.clone())).into()),
        ]);
        // A pod arrives: existing Service and Deployment must link to it.
        let p = KubeResource::Pod(pod("p", "ns", Some(l.clone())).into());
        let (pk, _) = store.insert(p.clone()).unwrap();
        goat.upsert_node(&pk, &p, &store);
        assert_eq!(
            edge_set(&goat),
            BTreeSet::from([e("d", "p", "Owns"), e("s", "p", "Selects")])
        );
        // An ingress arrives pointing at the service.
        let i = KubeResource::Ingress(ingress("i", "ns", &["s"]).into());
        let (ik, _) = store.insert(i.clone()).unwrap();
        goat.upsert_node(&ik, &i, &store);
        assert!(edge_set(&goat).contains(&e("i", "s", "RoutesTo")));
        // A second service arrives that the existing ingress routes to.
        let s2 = KubeResource::Service(service("s2", "ns", None).into());
        let (s2k, _) = store.insert(s2.clone()).unwrap();
        goat.upsert_node(&s2k, &s2, &store);
        assert!(
            !edge_set(&goat).contains(&e("i", "s2", "RoutesTo")),
            "ingress does not list s2"
        );
        let i2 = KubeResource::Ingress(ingress("i2", "ns", &["s2"]).into());
        let (i2k, _) = store.insert(i2.clone()).unwrap();
        goat.upsert_node(&i2k, &i2, &store);
        let s3 = KubeResource::Service(service("s3", "ns", None).into());
        let (s3k, _) = store.insert(s3.clone()).unwrap();
        goat.upsert_node(&s3k, &s3, &store);
        // Service upsert finds ingresses that already route to it.
        let i3 = KubeResource::Ingress(ingress("i3", "ns", &["s3"]).into());
        let (i3k, _) = store.insert(i3.clone()).unwrap();
        goat.upsert_node(&i3k, &i3, &store);
        goat.upsert_node(&s3k, &s3, &store);
        assert!(edge_set(&goat).contains(&e("i3", "s3", "RoutesTo")));
    }

    #[test]
    fn update_recomputes_edges_when_labels_change() {
        let l = labels(&[("app", "web")]);
        let (mut goat, mut store) = built(vec![
            KubeResource::Service(service("s", "ns", Some(l.clone())).into()),
            KubeResource::Pod(pod("p", "ns", Some(l)).into()),
        ]);
        assert_eq!(goat.edge_count(), 1);
        // The pod is relabelled: the service no longer selects it.
        let p = KubeResource::Pod(pod("p", "ns", Some(labels(&[("app", "other")]))).into());
        let (pk, is_new) = store.insert(p.clone()).unwrap();
        assert!(!is_new);
        assert_eq!(goat.upsert_node(&pk, &p, &store), Upsert::Updated);
        assert_eq!(goat.edge_count(), 0);
        assert_eq!(goat.node_count(), 2);
    }

    #[test]
    fn remove_node_drops_its_edges_and_keeps_other_indices_valid() {
        let l = labels(&[("app", "web")]);
        let (mut goat, store) = built(vec![
            KubeResource::Service(service("s", "ns", Some(l.clone())).into()),
            KubeResource::Pod(pod("p1", "ns", Some(l.clone())).into()),
            KubeResource::Pod(pod("p2", "ns", Some(l)).into()),
        ]);
        assert_eq!(goat.edge_count(), 2);
        let p1 = store
            .get_all_kris()
            .into_iter()
            .find(|k| k.name == "p1")
            .unwrap();
        assert!(goat.remove_node(&p1));
        assert!(!goat.remove_node(&p1));
        assert_eq!(goat.node_count(), 2);
        assert_eq!(edge_set(&goat), BTreeSet::from([e("s", "p2", "Selects")]));
        // Indices of the remaining nodes are still correct.
        let s = store
            .get_all_kris()
            .into_iter()
            .find(|k| k.name == "s")
            .unwrap();
        let out = goat.get_outgoing_edges(&s).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0.name, "p2");
    }

    // ── queries ───────────────────────────────────────────────────────────

    #[test]
    fn connected_component_and_unknown_node() {
        let l = labels(&[("app", "t")]);
        let (goat, store) = built(vec![
            KubeResource::Deployment(deployment("d", "ns", Some(l.clone())).into()),
            KubeResource::Pod(pod("p", "ns", Some(l.clone())).into()),
            KubeResource::Service(service("s", "ns", Some(l)).into()),
            KubeResource::Pod(pod("lonely", "ns", None).into()),
        ]);
        let kri = |n: &str| {
            store
                .get_all_kris()
                .into_iter()
                .find(|k| k.name == n)
                .unwrap()
        };
        let mut comp: Vec<String> = goat
            .connected_component(&kri("p"))
            .unwrap()
            .into_iter()
            .map(|k| k.name)
            .collect();
        comp.sort();
        assert_eq!(comp, vec!["d", "p", "s"]);
        assert_eq!(goat.connected_component(&kri("lonely")).unwrap().len(), 1);
        let unknown = KubeResourceIdentifier::new(0, Some("ns".into()), "core", "v1", "Pod", "x");
        assert!(goat.connected_component(&unknown).is_none());
        assert!(goat.get_outgoing_edges(&unknown).is_none());
    }

    #[test]
    fn dot_file_is_written() {
        let (goat, _) = built(vec![KubeResource::Pod(pod("p", "ns", None).into())]);
        let path = std::env::temp_dir().join(format!("valken-goat-{}.dot", std::process::id()));
        goat.write_graph_to_dot_file(&path).unwrap();
        let dot = std::fs::read_to_string(&path).unwrap();
        std::fs::remove_file(&path).ok();
        assert!(dot.contains("digraph"));
        assert!(dot.contains("pod::p"));
    }

    #[test]
    fn edge_type_display_and_eq() {
        assert_eq!(EdgeType::Owns.to_string(), "Owns");
        assert_eq!(EdgeType::RoutesTo.to_string(), "RoutesTo");
        assert_ne!(EdgeType::Owns, EdgeType::Selects);
    }

    // ── namespace adjacency ───────────────────────────────────────────────

    fn controller_pod(name: &str, ns: &str) -> Pod {
        let mut p = pod(
            name,
            ns,
            Some(labels(&[("app.kubernetes.io/name", "ingress-nginx")])),
        );
        p.spec = Some(PodSpec {
            containers: vec![Container {
                name: "controller".into(),
                image: Some("registry.k8s.io/ingress-nginx/controller:v1.11.0".into()),
                ..Default::default()
            }],
            ..Default::default()
        });
        p
    }

    #[test]
    fn ns_adjacency_links_controller_namespace_to_ingress_namespaces() {
        let (goat, _) = built(vec![
            KubeResource::Pod(controller_pod("ctl", "ingress-nginx").into()),
            KubeResource::Ingress(ingress("a", "shop", &["s"]).into()),
            KubeResource::Ingress(ingress("b", "blog", &["s"]).into()),
            KubeResource::Ingress(ingress("self", "ingress-nginx", &["s"]).into()),
        ]);
        let ctl = KubeResourceIdentifier::for_namespace(0, "ingress-nginx");
        let names: Vec<String> = goat
            .affected_namespaces(&ctl)
            .into_iter()
            .map(|k| k.name)
            .collect();
        assert_eq!(names, vec!["blog", "shop"]);
        let shop = KubeResourceIdentifier::for_namespace(0, "shop");
        assert_eq!(goat.affected_namespaces(&shop).len(), 1);
        assert!(goat
            .affected_namespaces(&KubeResourceIdentifier::for_namespace(0, "none"))
            .is_empty());
    }

    #[test]
    fn ns_adjacency_relevance() {
        assert!(KubeGoat::affects_ns_adjacency(&KubeResource::Ingress(
            ingress("i", "ns", &["s"]).into()
        )));
        assert!(KubeGoat::affects_ns_adjacency(&KubeResource::Pod(
            controller_pod("c", "ns").into()
        )));
        assert!(!KubeGoat::affects_ns_adjacency(&KubeResource::Pod(
            pod("p", "ns", None).into()
        )));
        assert!(!KubeGoat::affects_ns_adjacency(&KubeResource::Service(
            service("s", "ns", None).into()
        )));
    }
}
