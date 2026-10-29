//! Listener lifecycle: one listener per running ingress controller pod.
//!
//! [`Reconciler`] is the pure decision core — it watches pod events and emits
//! [`Action`]s (start/stop) that the manager thread applies.  Keeping the
//! decision logic free of I/O makes the "exactly one listener per pod" rules
//! unit-testable.
//!
//! [`spawn_traffic_manager_with`] runs the manager on its own OS thread with
//! a private tokio runtime, consuming a fan-out copy of the [`Watched`]
//! stream.  What each listener does with its lines is supplied by the
//! application through a [`ProcessorFactory`]; this crate only decides
//! *which* pods to follow and keeps the namespace / pod-IP hints current.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError};
use k8s_openapi::api::core::v1::Pod;
use tracing::{debug, error, info, warn};

use crate::k8s::ingress_controller::detect_ingress_controller_from_pod;
use crate::k8s::{IngressControllerKind, KubeResource, Watched};
use crate::traffic::listener::{
    spawn_listener_with, LineProcessor, ListenerDeps, ListenerHandle, ListenerStats,
    NamespaceHints, PodHints, Sink,
};
use crate::traffic::log_source::LogSource;
use crate::traffic::telemetry::{
    new_telemetry_cell, read_telemetry, ControllerCensus, ListenerTelemetry, TelemetryCell,
    TrafficTelemetry,
};

/// Which controller kinds the application can process logs for.
pub type SupportFn = Arc<dyn Fn(IngressControllerKind) -> bool + Send + Sync>;

/// A [`SupportFn`] accepting the given kinds.
pub fn supports(kinds: &'static [IngressControllerKind]) -> SupportFn {
    Arc::new(move |k| kinds.contains(&k))
}

// ─── Pod target ───────────────────────────────────────────────────────────────

/// Everything a listener needs to know about the pod it follows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodTarget {
    /// Stable identity: pod UID, or `namespace/name` if the UID is missing.
    pub key: String,
    pub namespace: String,
    pub name: String,
    /// Container whose stdout carries the access log.
    pub container: String,
    pub kind: IngressControllerKind,
}

impl PodTarget {
    pub fn label(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
}

/// Stable key for a pod: UID when present, else `namespace/name`.
pub fn pod_key(pod: &Pod) -> Option<String> {
    if let Some(uid) = pod.metadata.uid.as_deref().filter(|u| !u.is_empty()) {
        return Some(uid.to_string());
    }
    let ns = pod.metadata.namespace.as_deref()?;
    let name = pod.metadata.name.as_deref()?;
    Some(format!("{}/{}", ns, name))
}

/// Build a [`PodTarget`] if this pod is an ingress controller of a kind
/// `supported` accepts.
pub fn pod_target(pod: &Pod, supported: &SupportFn) -> Option<PodTarget> {
    let kind = detect_ingress_controller_from_pod(pod)?;
    if !supported(kind) {
        return None;
    }
    let key = pod_key(pod)?;
    let namespace = pod.metadata.namespace.clone()?;
    let name = pod.metadata.name.clone()?;
    let container = pick_log_container(pod, kind)?;
    Some(PodTarget {
        key,
        namespace,
        name,
        container,
        kind,
    })
}

/// Should this pod currently have a listener?  Only running, non-terminating
/// pods of a supported kind can be followed; anything else is stopped.
pub fn wants_listener(pod: &Pod, supported: &SupportFn) -> bool {
    if pod.metadata.deletion_timestamp.is_some() {
        return false;
    }
    let running = pod.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running");
    running && pod_target(pod, supported).is_some()
}

/// Choose the container that emits access logs: an image matching the
/// controller kind, then a container literally named `controller`, then the
/// first container.
fn pick_log_container(pod: &Pod, kind: IngressControllerKind) -> Option<String> {
    let containers = &pod.spec.as_ref()?.containers;
    if containers.is_empty() {
        return None;
    }
    let image_matches = |image: &str| {
        let image = image.to_lowercase();
        match kind {
            IngressControllerKind::Nginx => {
                image.contains("ingress-nginx") || image.contains("nginx-ingress")
            }
            IngressControllerKind::Traefik => image.contains("traefik"),
            IngressControllerKind::HaProxy => image.contains("haproxy"),
            IngressControllerKind::Istio => image.contains("proxyv2"),
            IngressControllerKind::Contour => image.contains("envoy") || image.contains("contour"),
            IngressControllerKind::Kong => image.contains("kong"),
            IngressControllerKind::Ambassador => {
                image.contains("ambassador") || image.contains("emissary")
            }
            IngressControllerKind::Unknown => false,
        }
    };
    containers
        .iter()
        .find(|c| c.image.as_deref().is_some_and(image_matches))
        .or_else(|| containers.iter().find(|c| c.name == "controller"))
        .or_else(|| containers.first())
        .map(|c| c.name.clone())
}

// ─── Reconciler ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Start(PodTarget),
    Stop(String),
}

/// Pure listener-set reconciler.  Tracks which pod keys currently have a
/// listener and emits the minimal set of actions for each observation.
pub struct Reconciler {
    active: HashSet<String>,
    supported: SupportFn,
}

impl std::fmt::Debug for Reconciler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reconciler")
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

impl Reconciler {
    pub fn new(supported: SupportFn) -> Self {
        Self {
            active: HashSet::new(),
            supported,
        }
    }

    pub fn active_len(&self) -> usize {
        self.active.len()
    }

    pub fn is_active(&self, key: &str) -> bool {
        self.active.contains(key)
    }

    /// A pod was applied (added or updated).
    pub fn observe_applied(&mut self, pod: &Pod) -> Vec<Action> {
        let Some(key) = pod_key(pod) else {
            return Vec::new();
        };
        let want = wants_listener(pod, &self.supported);
        let have = self.active.contains(&key);
        match (want, have) {
            (true, false) => match pod_target(pod, &self.supported) {
                Some(target) => {
                    self.active.insert(key);
                    vec![Action::Start(target)]
                }
                None => Vec::new(),
            },
            (false, true) => {
                self.active.remove(&key);
                vec![Action::Stop(key)]
            }
            _ => Vec::new(),
        }
    }

    /// A pod was deleted.
    pub fn observe_deleted(&mut self, pod: &Pod) -> Vec<Action> {
        match pod_key(pod) {
            Some(key) if self.active.remove(&key) => vec![Action::Stop(key)],
            _ => Vec::new(),
        }
    }

    /// The listener for `key` was not started after all (the processor
    /// factory declined): forget it so the pod's next event tries again.
    pub fn forget(&mut self, key: &str) -> bool {
        self.active.remove(key)
    }

    /// A full pod list (initial list or reconnect): start what is missing,
    /// stop anything no longer present or no longer eligible.
    pub fn observe_full_list<'a>(
        &mut self,
        pods: impl IntoIterator<Item = &'a Pod>,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for pod in pods {
            if let Some(key) = pod_key(pod) {
                if wants_listener(pod, &self.supported) {
                    seen.insert(key);
                }
            }
            actions.extend(self.observe_applied(pod));
        }
        let stale: Vec<String> = self
            .active
            .iter()
            .filter(|k| !seen.contains(*k))
            .cloned()
            .collect();
        for key in stale {
            self.active.remove(&key);
            actions.push(Action::Stop(key));
        }
        actions
    }
}

/// Namespace names, owned by the manager thread and published to the
/// listeners' shared [`NamespaceHints`].
///
/// Two sources are kept apart so that each full snapshot *replaces* its own
/// half instead of appending to it.  Append-only hints would let a dashed
/// namespace from a previously connected cluster win a longest-prefix
/// match for the new one:
///
/// * `snapshot` is the authoritative `NamespaceList`, replaced whole.
/// * `observed` holds namespaces of pods seen since the last full resource
///   list.  It is reset by each `FullResourceList` and grows with pod events,
///   so hints work before the first snapshot and between re-lists.
///
/// `begin_watching` sends the NamespaceList just before the FullResourceList,
/// so after a reconnect both halves describe only the new cluster.
#[derive(Debug)]
pub struct NamespaceHintSet {
    snapshot: HashSet<String>,
    observed: HashSet<String>,
    shared: NamespaceHints,
}

impl NamespaceHintSet {
    pub fn new(shared: NamespaceHints) -> Self {
        Self {
            snapshot: HashSet::new(),
            observed: HashSet::new(),
            shared,
        }
    }

    /// Replace the authoritative namespace snapshot.
    pub fn replace_snapshot<'a>(&mut self, names: impl IntoIterator<Item = &'a str>) {
        self.snapshot = names
            .into_iter()
            .filter(|n| !n.is_empty())
            .map(str::to_string)
            .collect();
        self.publish();
    }

    /// Replace the pod-derived set with the namespaces in a full pod list.
    pub fn reset_observed<'a>(&mut self, names: impl IntoIterator<Item = &'a str>) {
        self.observed = names
            .into_iter()
            .filter(|n| !n.is_empty())
            .map(str::to_string)
            .collect();
        self.publish();
    }

    /// Record one pod's namespace.  Publishes only when the hints change.
    pub fn observe(&mut self, name: Option<&str>) {
        let Some(name) = name.filter(|n| !n.is_empty()) else {
            return;
        };
        if self.snapshot.contains(name) || self.observed.contains(name) {
            return;
        }
        self.observed.insert(name.to_string());
        self.publish();
    }

    fn publish(&self) {
        let mut names: Vec<String> = self.snapshot.union(&self.observed).cloned().collect();
        names.sort();
        if let Ok(mut guard) = self.shared.write() {
            *guard = names;
        }
    }
}

// ─── Pod IP index ─────────────────────────────────────────────────────────────

/// Pod IP → pod name, from the pod watch, published to the listeners'
/// shared [`PodHints`] so an upstream address resolves at write time.
#[derive(Debug)]
pub struct PodIpIndex {
    /// IP → `(pod name, pod key)`.  The key records which pod owns the
    /// mapping, so a late delete for a pod whose IP has already been reused
    /// by its replacement never erases the replacement's entry.
    by_ip: HashMap<String, (String, String)>,
    /// pod key → its IPs, so a deleted / re-IPed pod drops its old entries.
    by_key: HashMap<String, Vec<String>>,
    shared: PodHints,
}

impl PodIpIndex {
    pub fn new(shared: PodHints) -> Self {
        Self {
            by_ip: HashMap::new(),
            by_key: HashMap::new(),
            shared,
        }
    }

    fn ips_of(pod: &Pod) -> Vec<String> {
        let status = pod.status.as_ref();
        let mut ips: Vec<String> = status
            .and_then(|s| s.pod_ips.as_ref())
            .map(|v| v.iter().map(|p| p.ip.clone()).collect())
            .unwrap_or_default();
        if let Some(ip) = status.and_then(|s| s.pod_ip.clone()) {
            if !ips.contains(&ip) {
                ips.push(ip);
            }
        }
        ips.retain(|ip| !ip.is_empty());
        ips
    }

    /// Drop `key`'s IPs — but only those it still owns: a replacement pod
    /// may have claimed an IP before the old pod's delete event arrived.
    /// Returns the IPs actually dropped.
    fn remove_key(&mut self, key: &str) -> Vec<String> {
        let mut dropped = Vec::new();
        if let Some(old) = self.by_key.remove(key) {
            for ip in old {
                if self.by_ip.get(&ip).is_some_and(|(_, owner)| owner == key) {
                    self.by_ip.remove(&ip);
                    dropped.push(ip);
                }
            }
        }
        dropped
    }

    fn insert(&mut self, key: &str, name: &str, ips: Vec<String>) {
        for ip in &ips {
            self.by_ip
                .insert(ip.clone(), (name.to_string(), key.to_string()));
        }
        if !ips.is_empty() {
            self.by_key.insert(key.to_string(), ips);
        }
    }

    /// A pod was applied: (re)map its IPs.  Terminating pods keep their
    /// mapping until deleted so in-flight lines still resolve.  Only the
    /// delta is pushed to the shared map, so a status-only pod update on a
    /// large cluster costs O(1) rather than O(pods).
    pub fn observe_applied(&mut self, pod: &Pod) {
        let (Some(key), Some(name)) = (pod_key(pod), pod.metadata.name.clone()) else {
            return;
        };
        let dropped = self.remove_key(&key);
        let ips = Self::ips_of(pod);
        self.insert(&key, &name, ips.clone());
        self.publish_delta(&dropped, &ips, &name);
    }

    pub fn observe_deleted(&mut self, pod: &Pod) {
        if let Some(key) = pod_key(pod) {
            let dropped = self.remove_key(&key);
            self.publish_delta(&dropped, &[], "");
        }
    }

    /// Replace the index with a full pod list (reconnect / new cluster).
    pub fn observe_full_list<'a>(&mut self, pods: impl IntoIterator<Item = &'a Pod>) {
        self.by_ip.clear();
        self.by_key.clear();
        for pod in pods {
            if let (Some(key), Some(name)) = (pod_key(pod), pod.metadata.name.clone()) {
                self.insert(&key, &name, Self::ips_of(pod));
            }
        }
        self.publish();
    }

    /// Apply one pod's change to the shared map: `dropped` IPs are removed
    /// unless re-claimed, `added` IPs map to `name`.
    fn publish_delta(&self, dropped: &[String], added: &[String], name: &str) {
        if dropped.is_empty() && added.is_empty() {
            return;
        }
        if let Ok(mut guard) = self.shared.write() {
            for ip in dropped {
                // A dropped IP may already be owned by another pod; keep that.
                if !self.by_ip.contains_key(ip) {
                    guard.remove(ip);
                }
            }
            for ip in added {
                guard.insert(ip.clone(), name.to_string());
            }
        }
    }

    pub fn resolve(&self, ip: &str) -> Option<&str> {
        self.by_ip.get(ip).map(|(name, _)| name.as_str())
    }

    pub fn len(&self) -> usize {
        self.by_ip.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_ip.is_empty()
    }

    fn publish(&self) {
        if let Ok(mut guard) = self.shared.write() {
            *guard = self
                .by_ip
                .iter()
                .map(|(ip, (name, _))| (ip.clone(), name.clone()))
                .collect();
        }
    }
}

// ─── Processor factory ────────────────────────────────────────────────────────

/// What a [`ProcessorFactory`] gets to build a processor for one listener.
pub struct ProcessorContext {
    pub target: PodTarget,
    pub cluster_id: u32,
    /// Live namespace names (see [`NamespaceHints`]).
    pub namespaces: NamespaceHints,
    /// Live pod IP → name map (see [`PodHints`]).
    pub pods: PodHints,
    /// The listener's counters, for the processor to report what it classified.
    pub stats: Arc<ListenerStats>,
}

/// Supplies the per-listener [`LineProcessor`] and says which controller
/// kinds it can handle.
pub trait ProcessorFactory<T: Send + 'static>: Send + Sync + 'static {
    /// Controller kinds this factory can build a processor for.
    fn supported(&self) -> SupportFn;

    /// Build the processor for one pod.  `None` declines the pod (its
    /// listener is not started).
    fn build(&self, ctx: ProcessorContext) -> Option<Box<dyn LineProcessor<Output = T>>>;
}

impl<T: Send + 'static> LineProcessor for Box<dyn LineProcessor<Output = T>> {
    type Output = T;

    fn ingest(&mut self, raw: &str) {
        (**self).ingest(raw)
    }

    fn flush_closed(&mut self, now: i64) -> Vec<T> {
        (**self).flush_closed(now)
    }

    fn flush_all(&mut self) -> Vec<T> {
        (**self).flush_all()
    }

    fn open_count(&self) -> usize {
        (**self).open_count()
    }
}

// ─── Manager thread ───────────────────────────────────────────────────────────

/// Handle to the traffic manager thread.
pub struct TrafficManagerHandle {
    stop: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
    telemetry: TelemetryCell,
}

impl TrafficManagerHandle {
    /// Current pipeline health (a cheap clone of the manager's published value).
    pub fn telemetry(&self) -> TrafficTelemetry {
        read_telemetry(&self.telemetry)
    }

    /// Stop every listener (each flushes its open items) and join the thread.
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(join) = self.join.take() {
            match join.join() {
                Ok(()) => info!("Traffic manager thread joined cleanly"),
                Err(_) => warn!("Traffic manager thread panicked during shutdown"),
            }
        }
    }
}

const RECV_POLL: Duration = Duration::from_millis(500);
const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// Spawn the manager thread.
///
/// * `watched_rx` — a fan-out copy of the kube watcher stream.
/// * `sink` — receives what the processors finish (must never block for long).
/// * `cluster_id` — shared with the kube watcher; read when a listener starts.
/// * `log_source` — where listeners get their lines.
/// * `factory` — builds one [`LineProcessor`] per listener.
pub fn spawn_traffic_manager_with<T: Send + 'static>(
    watched_rx: Receiver<Watched>,
    sink: Sink<T>,
    cluster_id: Arc<AtomicU32>,
    log_source: Arc<dyn LogSource>,
    factory: Arc<dyn ProcessorFactory<T>>,
) -> TrafficManagerHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = stop.clone();
    let telemetry = new_telemetry_cell();
    let telemetry_thread = telemetry.clone();

    let join = thread::Builder::new()
        .name("traffic-manager".into())
        .spawn(move || {
            run_manager(
                watched_rx,
                sink,
                cluster_id,
                log_source,
                factory,
                stop_thread,
                telemetry_thread,
            )
        })
        .expect("failed to spawn traffic-manager thread");

    TrafficManagerHandle {
        stop,
        join: Some(join),
        telemetry,
    }
}

/// Fold the live listener counters and the pod census into one telemetry value.
fn publish_telemetry(
    cell: &TelemetryCell,
    listeners: &HashMap<String, ListenerHandle>,
    census: &ControllerCensus,
) {
    let mut entries: Vec<ListenerTelemetry> = listeners
        .values()
        .map(|h| ListenerTelemetry {
            label: h.target.label(),
            namespace: h.target.namespace.clone(),
            kind: Some(h.target.kind),
            lines: h.stats.lines.load(Ordering::Relaxed),
            requests: h.stats.requests.load(Ordering::Relaxed),
            ignored: h.stats.ignored.load(Ordering::Relaxed),
            malformed: h.stats.malformed.load(Ordering::Relaxed),
            reconnects: h.stats.reconnects.load(Ordering::Relaxed),
            flushed_buckets: h.stats.flushed_buckets.load(Ordering::Relaxed),
            stream_open: h.stats.stream_open.load(Ordering::Relaxed),
            first_open_at: Some(h.stats.first_open_at.load(Ordering::Relaxed)).filter(|t| *t > 0),
            last_error: h.stats.last_error(),
        })
        .collect();
    entries.sort_by(|a, b| a.label.cmp(&b.label));
    let next = TrafficTelemetry {
        listeners: entries,
        controller_pods: census.controller_pods(),
        unsupported_kinds: census.unsupported_kinds(),
    };
    if let Ok(mut guard) = cell.write() {
        if *guard != next {
            *guard = next;
        }
    }
}

fn run_manager<T: Send + 'static>(
    watched_rx: Receiver<Watched>,
    sink: Sink<T>,
    cluster_id: Arc<AtomicU32>,
    log_source: Arc<dyn LogSource>,
    factory: Arc<dyn ProcessorFactory<T>>,
    stop: Arc<AtomicBool>,
    telemetry: TelemetryCell,
) {
    info!("Traffic manager thread started");

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("traffic-listener")
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            error!(error = %e, "Traffic manager could not build its tokio runtime; traffic capture disabled");
            return;
        }
    };

    let supported = factory.supported();
    let mut reconciler = Reconciler::new(supported.clone());
    let mut census = ControllerCensus::new(supported.clone());
    let mut listeners: HashMap<String, ListenerHandle> = HashMap::new();
    let namespaces: NamespaceHints = Arc::new(RwLock::new(Vec::new()));
    let mut hint_set = NamespaceHintSet::new(namespaces.clone());
    let pod_hints: PodHints = Arc::new(RwLock::new(HashMap::new()));
    let mut pod_index = PodIpIndex::new(pod_hints.clone());

    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let watched = match watched_rx.recv_timeout(RECV_POLL) {
            Ok(w) => w,
            Err(RecvTimeoutError::Timeout) => {
                // Counters move without pod events; keep the UI's view fresh.
                publish_telemetry(&telemetry, &listeners, &census);
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => {
                info!("Traffic manager: watched channel disconnected; exiting");
                break;
            }
        };

        let actions = match &watched {
            Watched::Pod(pod) => {
                hint_set.observe(pod.metadata.namespace.as_deref());
                census.observe_applied(pod);
                pod_index.observe_applied(pod);
                reconciler.observe_applied(pod)
            }
            Watched::DeletedPod(pod) => {
                census.observe_deleted(pod);
                pod_index.observe_deleted(pod);
                reconciler.observe_deleted(pod)
            }
            Watched::FullResourceList(list) => {
                let pods: Vec<&Pod> = list
                    .resources
                    .iter()
                    .filter_map(|r| match r {
                        KubeResource::Pod(p) => Some(p.as_ref()),
                        _ => None,
                    })
                    .collect();
                // A full list is a snapshot of the (possibly new) cluster:
                // replace, never append, the pod-derived hints.
                hint_set
                    .reset_observed(pods.iter().filter_map(|p| p.metadata.namespace.as_deref()));
                census.observe_full_list(pods.iter().copied());
                pod_index.observe_full_list(pods.iter().copied());
                reconciler.observe_full_list(pods)
            }
            Watched::NamespaceList(infos) => {
                hint_set.replace_snapshot(infos.iter().map(|n| n.name.as_str()));
                Vec::new()
            }
            Watched::Deployment(_)
            | Watched::DeletedDeployment(_)
            | Watched::Service(_)
            | Watched::DeletedService(_)
            | Watched::Ingress(_)
            | Watched::DeletedIngress(_) => Vec::new(),
        };

        for action in actions {
            match action {
                Action::Start(target) => {
                    let key = target.key.clone();
                    let label = target.label();
                    info!(pod = %label, kind = target.kind.display_name(), "Starting traffic listener");
                    let cluster = cluster_id.load(Ordering::SeqCst);
                    let stats = Arc::new(ListenerStats::default());
                    let Some(processor) = factory.build(ProcessorContext {
                        target: target.clone(),
                        cluster_id: cluster,
                        namespaces: namespaces.clone(),
                        pods: pod_hints.clone(),
                        stats: stats.clone(),
                    }) else {
                        warn!(pod = %label, "Processor factory declined the pod; not listening");
                        reconciler.forget(&key);
                        continue;
                    };
                    let handle = spawn_listener_with(
                        runtime.handle(),
                        target,
                        ListenerDeps {
                            cluster_id: cluster,
                            log_source: log_source.clone(),
                            sink: sink.clone(),
                        },
                        stats,
                        processor,
                    );
                    if let Some(old) = listeners.insert(key, handle) {
                        // Should not happen (reconciler guards it) — be safe.
                        old.stop();
                    }
                }
                Action::Stop(key) => {
                    if let Some(handle) = listeners.remove(&key) {
                        info!(pod = %handle.target.label(), "Stopping traffic listener");
                        handle.stop();
                    }
                }
            }
        }
        publish_telemetry(&telemetry, &listeners, &census);
        debug!(active = listeners.len(), "Traffic manager reconciled");
    }

    // Shutdown: stop everything and wait for final flushes.
    info!(
        active = listeners.len(),
        "Traffic manager stopping all listeners"
    );
    for handle in listeners.values() {
        handle.stop();
    }
    publish_telemetry(
        &telemetry,
        &HashMap::new(),
        &ControllerCensus::new(supported),
    );
    let joins: Vec<_> = listeners
        .values_mut()
        .filter_map(|h| h.take_join())
        .collect();
    runtime.block_on(async move {
        for join in joins {
            let _ = tokio::time::timeout(SHUTDOWN_FLUSH_TIMEOUT, join).await;
        }
    });
    runtime.shutdown_timeout(Duration::from_secs(2));
    info!("Traffic manager thread exited cleanly");
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{Container, PodSpec, PodStatus};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
    use k8s_openapi::jiff::Timestamp;
    use std::collections::BTreeMap;

    fn nginx_and_traefik() -> SupportFn {
        supports(&[IngressControllerKind::Nginx, IngressControllerKind::Traefik])
    }

    fn reconciler() -> Reconciler {
        Reconciler::new(nginx_and_traefik())
    }

    #[test]
    fn a_forgotten_pod_is_started_again_on_its_next_event() {
        let mut r = reconciler();
        let pod = nginx_pod("ctl-1", "u1", "Running");
        assert!(matches!(
            r.observe_applied(&pod).as_slice(),
            [Action::Start(_)]
        ));
        assert!(r.observe_applied(&pod).is_empty(), "already active");

        assert!(r.forget(&pod_key(&pod).unwrap()));
        assert!(matches!(
            r.observe_applied(&pod).as_slice(),
            [Action::Start(_)]
        ));
        assert!(!r.forget("nope"));
    }

    fn nginx_pod(name: &str, uid: &str, phase: &str) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some("ingress-nginx".into()),
                uid: Some(uid.into()),
                labels: Some(BTreeMap::from([(
                    "app.kubernetes.io/name".to_string(),
                    "ingress-nginx".to_string(),
                )])),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![
                    Container {
                        name: "sidecar".into(),
                        image: Some("busybox".into()),
                        ..Default::default()
                    },
                    Container {
                        name: "controller".into(),
                        image: Some("registry.k8s.io/ingress-nginx/controller:v1.11.0".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(phase.into()),
                ..Default::default()
            }),
        }
    }

    fn app_pod(name: &str, uid: &str) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some("default".into()),
                uid: Some(uid.into()),
                labels: Some(BTreeMap::from([("app".to_string(), "web".to_string())])),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "web".into(),
                    image: Some("nginx:1.25".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some("Running".into()),
                ..Default::default()
            }),
        }
    }

    fn starts(actions: &[Action]) -> usize {
        actions
            .iter()
            .filter(|a| matches!(a, Action::Start(_)))
            .count()
    }
    fn stops(actions: &[Action]) -> usize {
        actions
            .iter()
            .filter(|a| matches!(a, Action::Stop(_)))
            .count()
    }

    #[test]
    fn pod_target_picks_controller_container_by_image() {
        let t = pod_target(&nginx_pod("ctl-1", "u1", "Running"), &nginx_and_traefik()).unwrap();
        assert_eq!(t.container, "controller");
        assert_eq!(t.kind, IngressControllerKind::Nginx);
        assert_eq!(t.key, "u1");
        assert_eq!(t.label(), "ingress-nginx/ctl-1");
    }

    #[test]
    fn pod_key_falls_back_to_namespace_name() {
        let mut p = nginx_pod("ctl-1", "", "Running");
        p.metadata.uid = None;
        assert_eq!(pod_key(&p).as_deref(), Some("ingress-nginx/ctl-1"));
    }

    #[test]
    fn non_ingress_pods_never_get_listeners() {
        let mut r = reconciler();
        assert!(r.observe_applied(&app_pod("web-1", "w1")).is_empty());
        assert_eq!(r.active_len(), 0);
        assert!(pod_target(&app_pod("web-1", "w1"), &nginx_and_traefik()).is_none());
    }

    #[test]
    fn unsupported_controller_kind_is_skipped() {
        let mut p = nginx_pod("haproxy-1", "h1", "Running");
        p.metadata.labels = Some(BTreeMap::from([(
            "app.kubernetes.io/name".to_string(),
            "haproxy-ingress".to_string(),
        )]));
        p.spec.as_mut().unwrap().containers[1].image =
            Some("haproxytech/kubernetes-ingress:3.0".into());
        assert!(!wants_listener(&p, &nginx_and_traefik()));
        assert!(reconciler().observe_applied(&p).is_empty());
        // …unless the application says it can handle HAProxy.
        let all = supports(&[IngressControllerKind::HaProxy]);
        assert!(wants_listener(&p, &all));
    }

    #[test]
    fn traefik_pod_gets_a_listener_on_its_traefik_container() {
        let mut p = nginx_pod("traefik-1", "t1", "Running");
        p.metadata.namespace = Some("traefik".into());
        p.metadata.labels = Some(BTreeMap::from([(
            "app.kubernetes.io/name".to_string(),
            "traefik".to_string(),
        )]));
        // The Helm chart names the container `traefik`, not `controller`.
        let containers = &mut p.spec.as_mut().unwrap().containers;
        containers[1].name = "traefik".into();
        containers[1].image = Some("docker.io/traefik:v3.1.2".into());
        let t = pod_target(&p, &nginx_and_traefik()).unwrap();
        assert_eq!(t.kind, IngressControllerKind::Traefik);
        assert_eq!(t.container, "traefik");
        assert_eq!(t.label(), "traefik/traefik-1");
        assert!(wants_listener(&p, &nginx_and_traefik()));
        assert_eq!(starts(&reconciler().observe_applied(&p)), 1);
    }

    #[test]
    fn adding_a_running_pod_starts_exactly_one_listener() {
        let mut r = reconciler();
        let pod = nginx_pod("ctl-1", "u1", "Running");
        let a = r.observe_applied(&pod);
        assert_eq!(starts(&a), 1);
        assert_eq!(stops(&a), 0);
        // repeated Apply events for the same pod are no-ops
        assert!(r.observe_applied(&pod).is_empty());
        assert!(r.observe_applied(&pod).is_empty());
        assert_eq!(r.active_len(), 1);
    }

    #[test]
    fn pending_pod_is_not_started_until_running() {
        let mut r = reconciler();
        assert!(r
            .observe_applied(&nginx_pod("ctl-1", "u1", "Pending"))
            .is_empty());
        assert_eq!(
            starts(&r.observe_applied(&nginx_pod("ctl-1", "u1", "Running"))),
            1
        );
    }

    #[test]
    fn killing_a_pod_stops_exactly_one_listener_once() {
        let mut r = reconciler();
        r.observe_applied(&nginx_pod("ctl-1", "u1", "Running"));
        r.observe_applied(&nginx_pod("ctl-2", "u2", "Running"));
        assert_eq!(r.active_len(), 2);

        // Termination arrives first as an Apply with deletionTimestamp …
        let mut dying = nginx_pod("ctl-1", "u1", "Running");
        dying.metadata.deletion_timestamp = Some(Time(Timestamp::now()));
        let a = r.observe_applied(&dying);
        assert_eq!(a, vec![Action::Stop("u1".into())]);

        // … then as a Delete, which must not produce a second stop.
        assert!(r.observe_deleted(&dying).is_empty());
        assert_eq!(r.active_len(), 1);
        assert!(r.is_active("u2"));
    }

    #[test]
    fn delete_without_prior_terminating_apply_stops_once() {
        let mut r = reconciler();
        let pod = nginx_pod("ctl-1", "u1", "Running");
        r.observe_applied(&pod);
        assert_eq!(stops(&r.observe_deleted(&pod)), 1);
        assert!(r.observe_deleted(&pod).is_empty());
    }

    #[test]
    fn pod_leaving_running_phase_stops_listener() {
        let mut r = reconciler();
        r.observe_applied(&nginx_pod("ctl-1", "u1", "Running"));
        assert_eq!(
            stops(&r.observe_applied(&nginx_pod("ctl-1", "u1", "Failed"))),
            1
        );
        assert_eq!(r.active_len(), 0);
    }

    #[test]
    fn replacement_pod_with_same_name_gets_its_own_listener() {
        let mut r = reconciler();
        r.observe_applied(&nginx_pod("ctl-abc", "old-uid", "Running"));
        let a = r.observe_applied(&nginx_pod("ctl-abc", "new-uid", "Running"));
        assert_eq!(starts(&a), 1);
        assert_eq!(r.active_len(), 2);
        assert_eq!(
            stops(&r.observe_deleted(&nginx_pod("ctl-abc", "old-uid", "Running"))),
            1
        );
        assert_eq!(r.active_len(), 1);
    }

    #[test]
    fn full_list_starts_new_and_stops_stale() {
        let mut r = reconciler();
        r.observe_applied(&nginx_pod("ctl-1", "u1", "Running"));
        r.observe_applied(&nginx_pod("ctl-2", "u2", "Running"));

        // Reconnect: u2 gone, u3 new, plus an unrelated app pod.
        let pods = [
            nginx_pod("ctl-1", "u1", "Running"),
            nginx_pod("ctl-3", "u3", "Running"),
            app_pod("web", "w"),
        ];
        let a = r.observe_full_list(pods.iter());
        assert_eq!(starts(&a), 1);
        assert_eq!(stops(&a), 1);
        assert!(a.contains(&Action::Stop("u2".into())));
        assert!(r.is_active("u1") && r.is_active("u3") && !r.is_active("u2"));
        assert_eq!(r.active_len(), 2);
    }

    #[test]
    fn full_list_from_another_cluster_replaces_everything() {
        let mut r = reconciler();
        r.observe_full_list([nginx_pod("a", "cluster-a-1", "Running")].iter());
        let a = r.observe_full_list([nginx_pod("a", "cluster-b-1", "Running")].iter());
        assert_eq!(starts(&a), 1);
        assert_eq!(
            a.iter()
                .filter(|x| **x == Action::Stop("cluster-a-1".into()))
                .count(),
            1
        );
        assert_eq!(r.active_len(), 1);
    }

    #[test]
    fn empty_full_list_stops_all() {
        let mut r = reconciler();
        r.observe_applied(&nginx_pod("ctl-1", "u1", "Running"));
        let a = r.observe_full_list(std::iter::empty());
        assert_eq!(stops(&a), 1);
        assert_eq!(r.active_len(), 0);
    }

    fn hints_of(shared: &NamespaceHints) -> Vec<String> {
        shared.read().unwrap().clone()
    }

    #[test]
    fn pod_namespaces_are_published_sorted_and_deduplicated() {
        let shared: NamespaceHints = Arc::new(RwLock::new(Vec::new()));
        let mut set = NamespaceHintSet::new(shared.clone());

        set.reset_observed(["eldr-io-web", "default", ""]);
        assert_eq!(hints_of(&shared), vec!["default", "eldr-io-web"]);

        set.observe(Some("kube-system"));
        set.observe(Some("default")); // already known: no change
        set.observe(None);
        assert_eq!(
            hints_of(&shared),
            vec!["default", "eldr-io-web", "kube-system"]
        );
    }

    #[test]
    fn namespace_snapshot_replaces_instead_of_appending() {
        let shared: NamespaceHints = Arc::new(RwLock::new(Vec::new()));
        let mut set = NamespaceHintSet::new(shared.clone());
        set.replace_snapshot(["default", "shop-prod"]);
        set.replace_snapshot(["default", "billing"]);
        assert_eq!(
            hints_of(&shared),
            vec!["billing", "default"],
            "shop-prod must be dropped"
        );
    }

    #[test]
    fn reconnect_to_another_cluster_drops_stale_namespaces() {
        let shared: NamespaceHints = Arc::new(RwLock::new(Vec::new()));
        let mut set = NamespaceHintSet::new(shared.clone());

        // Cluster A has namespace "shop-prod", seen via snapshot and pods.
        set.replace_snapshot(["shop-prod", "default"]);
        set.reset_observed(["shop-prod"]);
        set.observe(Some("shop-prod"));
        assert_eq!(hints_of(&shared), vec!["default", "shop-prod"]);

        // Cluster B has namespace "shop".  Same order as begin_watching:
        // NamespaceList, then FullResourceList.
        set.replace_snapshot(["shop", "default"]);
        set.reset_observed(["shop"]);
        assert_eq!(hints_of(&shared), vec!["default", "shop"]);
    }

    #[test]
    fn full_list_after_snapshot_keeps_snapshot_names() {
        // The full list only carries pod namespaces; resetting the observed
        // half must not wipe namespaces (e.g. empty ones) from the snapshot.
        let shared: NamespaceHints = Arc::new(RwLock::new(Vec::new()));
        let mut set = NamespaceHintSet::new(shared.clone());
        set.replace_snapshot(["default", "empty-ns", "kube-system"]);
        set.reset_observed(["default"]);
        assert_eq!(
            hints_of(&shared),
            vec!["default", "empty-ns", "kube-system"]
        );
    }

    fn pod_with_ip(name: &str, uid: &str, ip: &str) -> Pod {
        let mut p = app_pod(name, uid);
        p.status.as_mut().unwrap().pod_ip = Some(ip.into());
        p
    }

    #[test]
    fn pod_ip_index_follows_the_watch() {
        let shared: PodHints = Arc::new(RwLock::new(HashMap::new()));
        let mut idx = PodIpIndex::new(shared.clone());
        idx.observe_applied(&pod_with_ip("web-a", "a", "10.42.3.17"));
        idx.observe_applied(&pod_with_ip("web-b", "b", "10.42.1.9"));
        assert_eq!(idx.resolve("10.42.3.17"), Some("web-a"));
        assert_eq!(
            shared.read().unwrap().get("10.42.1.9").map(String::as_str),
            Some("web-b")
        );

        // Restart: a new pod takes the old IP; the old entry must not linger.
        idx.observe_deleted(&pod_with_ip("web-a", "a", "10.42.3.17"));
        assert_eq!(idx.resolve("10.42.3.17"), None);
        assert!(!shared.read().unwrap().contains_key("10.42.3.17"));
        idx.observe_applied(&pod_with_ip("web-c", "c", "10.42.3.17"));
        assert_eq!(idx.resolve("10.42.3.17"), Some("web-c"));

        // Re-IP of the same pod drops the stale address.
        idx.observe_applied(&pod_with_ip("web-c", "c", "10.42.3.18"));
        assert_eq!(idx.resolve("10.42.3.17"), None);
        assert!(!shared.read().unwrap().contains_key("10.42.3.17"));
        assert_eq!(idx.resolve("10.42.3.18"), Some("web-c"));

        // Replacement observed *before* the old pod's delete: the delete
        // must not erase the replacement's claim on the reused IP.
        idx.observe_applied(&pod_with_ip("web-d", "d", "10.42.3.18"));
        assert_eq!(idx.resolve("10.42.3.18"), Some("web-d"));
        idx.observe_deleted(&pod_with_ip("web-c", "c", "10.42.3.18"));
        assert_eq!(
            idx.resolve("10.42.3.18"),
            Some("web-d"),
            "late delete of the old owner is ignored"
        );
        assert_eq!(
            shared.read().unwrap().get("10.42.3.18").map(String::as_str),
            Some("web-d")
        );
        // Same race via a terminating re-apply of the old pod with its old IP.
        idx.observe_applied(&pod_with_ip("web-c", "c", "10.42.3.18"));
        assert_eq!(
            idx.resolve("10.42.3.18"),
            Some("web-c"),
            "a fresh apply does claim it"
        );
        idx.observe_deleted(&pod_with_ip("web-c", "c", "10.42.3.18"));
        assert_eq!(idx.resolve("10.42.3.18"), None);
        idx.observe_applied(&pod_with_ip("web-c", "c", "10.42.3.17"));

        // Pending pods without an IP are not indexed.
        let mut pending = app_pod("web-p", "p");
        pending.status.as_mut().unwrap().pod_ip = None;
        idx.observe_applied(&pending);
        assert_eq!(idx.len(), 2);

        idx.observe_full_list([pod_with_ip("z", "z", "10.0.0.1")].iter());
        assert_eq!(idx.len(), 1);
        assert_eq!(shared.read().unwrap().len(), 1);
        assert!(!idx.is_empty());
    }

    /// A processor that keeps every line and emits them on flush.
    struct Lines(Vec<String>);

    impl LineProcessor for Lines {
        type Output = String;
        fn ingest(&mut self, raw: &str) {
            self.0.push(raw.to_string());
        }
        fn flush_closed(&mut self, _now: i64) -> Vec<String> {
            std::mem::take(&mut self.0)
        }
        fn flush_all(&mut self) -> Vec<String> {
            std::mem::take(&mut self.0)
        }
    }

    struct NoClient;

    #[async_trait::async_trait]
    impl LogSource for NoClient {
        async fn open(
            &self,
            _target: &PodTarget,
        ) -> Result<crate::traffic::log_source::LogLines, crate::traffic::StreamError> {
            Err(crate::traffic::StreamError {
                kind: crate::traffic::StreamErrorKind::Other,
                message: "no kube client available".into(),
            })
        }
    }

    #[test]
    fn spawn_listener_and_stop_flushes_without_a_cluster() {
        // Isolation smoke test: two listeners, no log source, stop them; the
        // manager must not hang and each must exit cleanly.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let flushed = Arc::new(std::sync::Mutex::new(0usize));
        let f2 = flushed.clone();
        let sink: Sink<String> = Arc::new(move |rows| *f2.lock().unwrap() += rows.len());

        let mut handles: Vec<ListenerHandle> = (0..2)
            .map(|i| {
                let target = pod_target(
                    &nginx_pod(&format!("ctl-{i}"), &format!("u{i}"), "Running"),
                    &nginx_and_traefik(),
                )
                .unwrap();
                spawn_listener_with(
                    rt.handle(),
                    target,
                    ListenerDeps {
                        cluster_id: 1,
                        log_source: Arc::new(NoClient),
                        sink: sink.clone(),
                    },
                    Arc::new(ListenerStats::default()),
                    Lines(Vec::new()),
                )
            })
            .collect();

        std::thread::sleep(Duration::from_millis(100));
        for h in &handles {
            h.stop();
        }
        rt.block_on(async {
            for h in handles.iter_mut() {
                let join = h.take_join().unwrap();
                tokio::time::timeout(Duration::from_secs(5), join)
                    .await
                    .expect("listener did not stop")
                    .unwrap();
            }
        });
        assert_eq!(*flushed.lock().unwrap(), 0);
        assert!(handles.iter().all(|h| h.is_stopped()));
        assert!(handles.iter().all(|h| h.stats.last_error().is_some()));
        rt.shutdown_timeout(Duration::from_secs(1));
    }
}
