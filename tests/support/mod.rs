//! Shared fakes for the integration tests: an in-memory [`ClusterSource`]
//! fed by channels, an in-memory [`LogSource`], and resource builders.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};
use futures::FutureExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{
    Container, Namespace, Node, NodeCondition, NodeSpec, NodeStatus, Pod, PodSpec, PodStatus,
    ResourceRequirements, Service, ServiceSpec,
};
use k8s_openapi::api::networking::v1::{
    HTTPIngressPath, HTTPIngressRuleValue, Ingress, IngressBackend, IngressRule,
    IngressServiceBackend, IngressSpec, ServiceBackendPort,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::runtime::watcher::{self, Event};
use tokio::sync::mpsc;

use valken_core::k8s::cluster_health::Resources;
use valken_core::k8s::kubeconfig::probe::ProbeError;
use valken_core::k8s::metrics::UsageSample;
use valken_core::k8s::{
    ClusterSource, MetricsError, MetricsSource, NodeCapacity, NodeUsage, PodUsage, WatchStream,
};
use valken_core::traffic::PodTarget;
use valken_core::traffic::StreamError;
use valken_core::traffic::{LogLines, LogSource};

// ─── Fake cluster ─────────────────────────────────────────────────────────────

type Tx<K> = mpsc::UnboundedSender<Result<Event<K>, watcher::Error>>;
type Rx<K> = mpsc::UnboundedReceiver<Result<Event<K>, watcher::Error>>;

/// One watch channel: the test holds the sender, the watcher takes the
/// receiver when it subscribes.
struct Feed<K> {
    tx: Tx<K>,
    rx: Mutex<Option<Rx<K>>>,
}

impl<K> Feed<K> {
    fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            tx,
            rx: Mutex::new(Some(rx)),
        }
    }

    fn stream(&self) -> BoxStream<'static, Result<Event<K>, watcher::Error>>
    where
        K: Send + 'static,
    {
        let rx = self
            .rx
            .lock()
            .unwrap()
            .take()
            .expect("each kind is watched once per FakeCluster");
        stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|e| (e, rx)) }).boxed()
    }
}

/// In-memory [`ClusterSource`].  Push kube-runtime events with the
/// `send_*` methods; [`FakeCluster::sync`] emits a complete initial list.
pub struct FakeCluster {
    pods: Feed<Pod>,
    deployments: Feed<Deployment>,
    services: Feed<Service>,
    ingresses: Feed<Ingress>,
    namespaces: Feed<Namespace>,
    /// The current node watch's sender.  Unlike the other kinds, nodes may
    /// be watched more than once (the health monitor retries a forbidden
    /// watch), so every `watch_nodes` opens a fresh channel.
    nodes: Mutex<Option<Tx<Node>>>,
    pub node_watches: std::sync::atomic::AtomicU32,
    metrics: Mutex<Option<Arc<dyn MetricsSource>>>,
    pub uuid: Option<String>,
    /// What [`ClusterSource::ping`] answers; `None` means healthy.
    ping_error: Mutex<Option<ProbeError>>,
    pub pings: std::sync::atomic::AtomicU32,
}

impl FakeCluster {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            pods: Feed::new(),
            deployments: Feed::new(),
            services: Feed::new(),
            ingresses: Feed::new(),
            namespaces: Feed::new(),
            nodes: Mutex::new(None),
            node_watches: std::sync::atomic::AtomicU32::new(0),
            metrics: Mutex::new(None),
            uuid: Some("fake-cluster-uuid".into()),
            ping_error: Mutex::new(None),
            pings: std::sync::atomic::AtomicU32::new(0),
        })
    }

    /// What [`ClusterSource::metrics`] hands out from now on.
    pub fn set_metrics(&self, metrics: Arc<dyn MetricsSource>) {
        *self.metrics.lock().unwrap() = Some(metrics);
    }

    /// Send a node event to the current node watch (dropped if none).
    pub fn send_node(&self, ev: Event<Node>) {
        if let Some(tx) = self.nodes.lock().unwrap().as_ref() {
            let _ = tx.send(Ok(ev));
        }
    }

    /// Fail the current node watch with an API error of `code`.
    pub fn send_node_error(&self, code: u16) {
        let status = kube::core::Status::failure("nodes", "Forbidden").with_code(code);
        if let Some(tx) = self.nodes.lock().unwrap().as_ref() {
            let _ = tx.send(Err(watcher::Error::InitialListFailed(kube::Error::Api(
                status.boxed(),
            ))));
        }
    }

    /// `Init`, one `InitApply` per node, `InitDone`.
    pub fn sync_nodes(&self, nodes: &[Node]) {
        self.send_node(Event::Init);
        for n in nodes {
            self.send_node(Event::InitApply(n.clone()));
        }
        self.send_node(Event::InitDone);
    }

    pub fn node_watch_count(&self) -> u32 {
        self.node_watches.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Make every later ping fail with `err` (or succeed again with `None`).
    pub fn set_ping_error(&self, err: Option<ProbeError>) {
        *self.ping_error.lock().unwrap() = err;
    }

    pub fn ping_count(&self) -> u32 {
        self.pings.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// A pod watch error that says the credentials were rejected.
    pub fn send_pod_unauthorized(&self) {
        let status = kube::core::Status::failure("Unauthorized", "Unauthorized").with_code(401);
        self.pods
            .tx
            .send(Err(watcher::Error::WatchFailed(kube::Error::Api(
                status.boxed(),
            ))))
            .unwrap();
    }

    pub fn send_pod(&self, ev: Event<Pod>) {
        // Infallible from the test's side: a stopped watcher has dropped its receiver.
        let _ = self.pods.tx.send(Ok(ev));
    }
    pub fn send_deployment(&self, ev: Event<Deployment>) {
        // Infallible from the test's side: a stopped watcher has dropped its receiver.
        let _ = self.deployments.tx.send(Ok(ev));
    }
    pub fn send_service(&self, ev: Event<Service>) {
        // Infallible from the test's side: a stopped watcher has dropped its receiver.
        let _ = self.services.tx.send(Ok(ev));
    }
    pub fn send_ingress(&self, ev: Event<Ingress>) {
        // Infallible from the test's side: a stopped watcher has dropped its receiver.
        let _ = self.ingresses.tx.send(Ok(ev));
    }
    pub fn send_namespace(&self, ev: Event<Namespace>) {
        // Infallible from the test's side: a stopped watcher has dropped its receiver.
        let _ = self.namespaces.tx.send(Ok(ev));
    }
    pub fn send_pod_error(&self) {
        self.pods
            .tx
            .send(Err(watcher::Error::NoResourceVersion))
            .unwrap();
    }

    /// Emit `Init`, one `InitApply` per object, and `InitDone` for every
    /// kind — exactly what kube-runtime does on (re)connect.
    pub fn sync(&self, snapshot: &Snapshot) {
        self.send_pod(Event::Init);
        for p in &snapshot.pods {
            self.send_pod(Event::InitApply(p.clone()));
        }
        self.send_pod(Event::InitDone);

        self.send_deployment(Event::Init);
        for d in &snapshot.deployments {
            self.send_deployment(Event::InitApply(d.clone()));
        }
        self.send_deployment(Event::InitDone);

        self.send_service(Event::Init);
        for s in &snapshot.services {
            self.send_service(Event::InitApply(s.clone()));
        }
        self.send_service(Event::InitDone);

        self.send_ingress(Event::Init);
        for i in &snapshot.ingresses {
            self.send_ingress(Event::InitApply(i.clone()));
        }
        self.send_ingress(Event::InitDone);

        self.send_namespace(Event::Init);
        for n in &snapshot.namespaces {
            self.send_namespace(Event::InitApply(n.clone()));
        }
        self.send_namespace(Event::InitDone);
    }
}

impl ClusterSource for FakeCluster {
    fn watch_pods(&self) -> WatchStream<Pod> {
        self.pods.stream()
    }
    fn watch_deployments(&self) -> WatchStream<Deployment> {
        self.deployments.stream()
    }
    fn watch_services(&self) -> WatchStream<Service> {
        self.services.stream()
    }
    fn watch_ingresses(&self) -> WatchStream<Ingress> {
        self.ingresses.stream()
    }
    fn watch_namespaces(&self) -> WatchStream<Namespace> {
        self.namespaces.stream()
    }
    fn watch_nodes(&self) -> Option<WatchStream<Node>> {
        let (tx, rx) = mpsc::unbounded_channel();
        *self.nodes.lock().unwrap() = Some(tx);
        // Counted only once the sender is in place, so a test that waits on
        // the count can send right away.
        self.node_watches
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Some(stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|e| (e, rx)) }).boxed())
    }
    fn metrics(&self) -> Option<Arc<dyn MetricsSource>> {
        self.metrics.lock().unwrap().clone()
    }
    fn cluster_uuid(&self) -> BoxFuture<'static, Option<String>> {
        let uuid = self.uuid.clone();
        async move { uuid }.boxed()
    }
    fn ping(&self) -> BoxFuture<'static, Result<(), ProbeError>> {
        self.pings.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let result = match self.ping_error.lock().unwrap().clone() {
            Some(e) => Err(e),
            None => Ok(()),
        };
        async move { result }.boxed()
    }
}

/// Everything a [`FakeCluster::sync`] emits.
#[derive(Default, Clone)]
pub struct Snapshot {
    pub pods: Vec<Pod>,
    pub deployments: Vec<Deployment>,
    pub services: Vec<Service>,
    pub ingresses: Vec<Ingress>,
    pub namespaces: Vec<Namespace>,
}

// ─── Fake metrics ─────────────────────────────────────────────────────────────

/// Scripted [`MetricsSource`]: node usage answers with whatever was set
/// last (or an API error code); pod usage per namespace.
#[derive(Default)]
pub struct FakeMetrics {
    nodes: Mutex<Option<Result<Vec<UsageSample>, u16>>>,
    pods: Mutex<HashMap<String, Vec<UsageSample>>>,
    pub node_reads: std::sync::atomic::AtomicU32,
    pub pod_namespaces_read: Mutex<Vec<String>>,
}

impl FakeMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn set_nodes(&self, usage: &[(&str, i64, i64)]) {
        *self.nodes.lock().unwrap() = Some(Ok(usage
            .iter()
            .map(|(n, c, m)| UsageSample {
                name: n.to_string(),
                usage: Resources::new(*c, *m),
            })
            .collect()));
    }

    /// Every later node read fails with this HTTP status.
    pub fn fail_nodes(&self, code: u16) {
        *self.nodes.lock().unwrap() = Some(Err(code));
    }

    pub fn set_pods(&self, namespace: &str, usage: &[(&str, i64, i64)]) {
        self.pods.lock().unwrap().insert(
            namespace.to_string(),
            usage
                .iter()
                .map(|(n, c, m)| UsageSample {
                    name: n.to_string(),
                    usage: Resources::new(*c, *m),
                })
                .collect(),
        );
    }

    pub fn node_read_count(&self) -> u32 {
        self.node_reads.load(std::sync::atomic::Ordering::SeqCst)
    }
}

fn api_error(code: u16) -> MetricsError {
    MetricsError::Api(kube::Error::Api(
        kube::core::Status::failure("metrics", "Failure")
            .with_code(code)
            .boxed(),
    ))
}

#[async_trait]
impl MetricsSource for FakeMetrics {
    async fn node_capacity(&self) -> Result<NodeCapacity, MetricsError> {
        Err(MetricsError::Unsupported)
    }
    async fn node_usage(&self) -> Result<NodeUsage, MetricsError> {
        Err(MetricsError::Unsupported)
    }
    async fn pod_usage(&self, _: &str, _: &str) -> Result<PodUsage, MetricsError> {
        Err(MetricsError::Unsupported)
    }
    async fn pod_restarts(&self, _: &str, _: &str) -> Result<i32, MetricsError> {
        Err(MetricsError::Unsupported)
    }
    async fn node_usages(&self) -> Result<Vec<UsageSample>, MetricsError> {
        self.node_reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        match self.nodes.lock().unwrap().clone() {
            Some(Ok(v)) => Ok(v),
            Some(Err(code)) => Err(api_error(code)),
            None => Err(api_error(503)),
        }
    }
    async fn pod_usages(&self, namespace: &str) -> Result<Vec<UsageSample>, MetricsError> {
        self.pod_namespaces_read
            .lock()
            .unwrap()
            .push(namespace.to_string());
        Ok(self
            .pods
            .lock()
            .unwrap()
            .get(namespace)
            .cloned()
            .unwrap_or_default())
    }
}

// ─── Fake log source ──────────────────────────────────────────────────────────

/// In-memory [`LogSource`].  Each `open` for a pod label yields a stream
/// fed by the sender returned from [`FakeLogs::feed`].  Dropping the sender
/// ends the stream (the listener then reconnects and gets a new one).
#[derive(Default)]
pub struct FakeLogs {
    feeds: Mutex<HashMap<String, mpsc::UnboundedReceiver<String>>>,
    pub opens: Mutex<Vec<String>>,
}

impl FakeLogs {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register the next stream for `label` (`namespace/pod`) and return its
    /// sender.
    pub fn feed(&self, label: &str) -> mpsc::UnboundedSender<String> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.feeds.lock().unwrap().insert(label.to_string(), rx);
        tx
    }
}

#[async_trait]
impl LogSource for FakeLogs {
    async fn open(&self, target: &PodTarget) -> Result<LogLines, StreamError> {
        self.opens.lock().unwrap().push(target.label());
        let rx = self.feeds.lock().unwrap().remove(&target.label());
        match rx {
            Some(rx) => Ok(stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|line| (Ok(line), rx))
            })
            .boxed()),
            None => Err(StreamError {
                kind: valken_core::traffic::StreamErrorKind::NotFound,
                message: format!("no fake log feed registered for {}", target.label()),
            }),
        }
    }
}

// ─── Builders ─────────────────────────────────────────────────────────────────

pub fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

pub fn pod(ns: &str, name: &str, uid: &str, labels: BTreeMap<String, String>) -> Pod {
    Pod {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some(ns.into()),
            uid: Some(uid.into()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "app".into(),
                image: Some("example/app:1".into()),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some("Running".into()),
            pod_ip: Some("10.42.0.5".into()),
            ..Default::default()
        }),
    }
}

/// A running ingress-nginx controller pod in `ns`.
pub fn nginx_controller_pod(ns: &str, name: &str, uid: &str) -> Pod {
    let mut p = pod(
        ns,
        name,
        uid,
        labels(&[("app.kubernetes.io/name", "ingress-nginx")]),
    );
    p.spec.as_mut().unwrap().containers = vec![Container {
        name: "controller".into(),
        image: Some("registry.k8s.io/ingress-nginx/controller:v1.11.0".into()),
        ..Default::default()
    }];
    p.status.as_mut().unwrap().pod_ip = Some("10.42.0.2".into());
    p
}

pub fn service(ns: &str, name: &str, selector: BTreeMap<String, String>) -> Service {
    Service {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some(ns.into()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(selector),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn ingress(ns: &str, name: &str, backend: &str) -> Ingress {
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
                    paths: vec![HTTPIngressPath {
                        backend: IngressBackend {
                            service: Some(IngressServiceBackend {
                                name: backend.into(),
                                port: Some(ServiceBackendPort {
                                    number: Some(80),
                                    ..Default::default()
                                }),
                            }),
                            ..Default::default()
                        },
                        path: Some("/".into()),
                        path_type: "Prefix".into(),
                    }],
                }),
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn namespace(name: &str, labels: BTreeMap<String, String>) -> Namespace {
    Namespace {
        metadata: ObjectMeta {
            name: Some(name.into()),
            labels: Some(labels),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn quantities(pairs: &[(&str, &str)]) -> BTreeMap<String, Quantity> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), Quantity(v.to_string())))
        .collect()
}

/// A Ready node with 4 cores and 16 GiB allocatable, 110 pod slots.
pub fn node(name: &str) -> Node {
    Node {
        metadata: ObjectMeta {
            name: Some(name.into()),
            labels: Some(labels(&[("topology.kubernetes.io/zone", "zone-a")])),
            ..Default::default()
        },
        spec: Some(NodeSpec::default()),
        status: Some(NodeStatus {
            capacity: Some(quantities(&[
                ("cpu", "4"),
                ("memory", "16Gi"),
                ("pods", "110"),
            ])),
            allocatable: Some(quantities(&[
                ("cpu", "4"),
                ("memory", "16Gi"),
                ("pods", "110"),
            ])),
            conditions: Some(vec![node_condition("Ready", "True")]),
            ..Default::default()
        }),
    }
}

pub fn node_condition(type_: &str, status: &str) -> NodeCondition {
    NodeCondition {
        type_: type_.into(),
        status: status.into(),
        reason: Some(format!("{type_}{status}")),
        ..Default::default()
    }
}

/// `node` with its `Ready` condition set to `status`.
pub fn node_ready(name: &str, status: &str) -> Node {
    let mut n = node(name);
    n.status.as_mut().unwrap().conditions = Some(vec![node_condition("Ready", status)]);
    n
}

/// A running pod on `node` requesting `cpu` / `memory`.
pub fn pod_on(ns: &str, name: &str, node: &str, cpu: &str, memory: &str) -> Pod {
    let mut p = pod(ns, name, &format!("uid-{ns}-{name}"), labels(&[]));
    let spec = p.spec.as_mut().unwrap();
    spec.node_name = Some(node.into());
    spec.containers[0].resources = Some(ResourceRequirements {
        requests: Some(quantities(&[("cpu", cpu), ("memory", memory)])),
        ..Default::default()
    });
    p
}

/// Block until `pred` holds or `timeout` elapses; returns whether it held.
pub fn wait_until(timeout: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if pred() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    pred()
}

/// Receive from a crossbeam channel with a timeout, panicking with context.
pub fn recv<T>(rx: &crossbeam_channel::Receiver<T>, what: &str) -> T {
    rx.recv_timeout(Duration::from_secs(5))
        .unwrap_or_else(|e| panic!("timed out waiting for {what}: {e}"))
}
