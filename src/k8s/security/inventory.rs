//! [`SecurityWatch`]: one watch per kind, slimmed into a
//! [`SecurityInventory`] snapshot that is republished (debounced) on change.
//!
//! Each kind is optional. If its initial list fails with 403 (no `list`
//! permission) or 404 (e.g. the Gateway API CRD isn't installed), that kind is
//! marked [`KindAvailability::Forbidden`] / [`KindAvailability::NotInstalled`],
//! its watch stops, and the rest carry on. Consumers must treat an
//! unavailable kind as *unknown*, not as empty. Other errors are retried with
//! kube-runtime's backoff.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures::stream::{self, BoxStream, StreamExt};
use kube::runtime::watcher::{self, Event};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn, Instrument};

use super::model::ObjectRef;
use super::model::{
    OwnerRef, SecHttpRoute, SecIngress, SecNetworkPolicy, SecPod, SecReplicaSet, SecService,
    SecWorkload, WorkloadKind, WorkloadRef,
};
use super::source::SecuritySource;
use crate::k8s::error::describe_watch;

/// Publish once events have been quiet this long…
const QUIET: Duration = Duration::from_millis(500);
/// …or at the latest this long after the first unpublished change.
const MAX_DELAY: Duration = Duration::from_secs(3);
/// Publish a partial snapshot if some kinds still haven't listed by then.
const INITIAL_GRACE: Duration = Duration::from_secs(30);

// ─── Public types ─────────────────────────────────────────────────────────────

/// The kinds the Security lens watches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SecurityKind {
    Pod,
    ReplicaSet,
    Deployment,
    StatefulSet,
    DaemonSet,
    Job,
    CronJob,
    Service,
    Ingress,
    HttpRoute,
    NetworkPolicy,
}

impl SecurityKind {
    pub const ALL: [SecurityKind; 11] = [
        SecurityKind::Pod,
        SecurityKind::ReplicaSet,
        SecurityKind::Deployment,
        SecurityKind::StatefulSet,
        SecurityKind::DaemonSet,
        SecurityKind::Job,
        SecurityKind::CronJob,
        SecurityKind::Service,
        SecurityKind::Ingress,
        SecurityKind::HttpRoute,
        SecurityKind::NetworkPolicy,
    ];
}

/// Whether a kind's data can be trusted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KindAvailability {
    /// Initial list not finished yet.
    Pending,
    Ready,
    /// 403 — no permission to list it.
    Forbidden,
    /// 404 — the resource type doesn't exist (e.g. no Gateway API CRDs).
    NotInstalled,
    /// Failing before the first list completed; being retried.
    Failing(String),
}

/// Everything the Security lens knows about the cluster, at one moment.
#[derive(Debug, Clone, Default)]
pub struct SecurityInventory {
    pub cluster_id: u32,
    /// Increments with every published snapshot.
    pub generation: u64,
    pub pods: Vec<SecPod>,
    pub replica_sets: Vec<SecReplicaSet>,
    pub workloads: Vec<SecWorkload>,
    pub services: Vec<SecService>,
    pub ingresses: Vec<SecIngress>,
    pub http_routes: Vec<SecHttpRoute>,
    pub network_policies: Vec<SecNetworkPolicy>,
    pub availability: BTreeMap<SecurityKind, KindAvailability>,
}

impl SecurityInventory {
    /// Whether `kind`'s data is complete (listed successfully).
    pub fn is_ready(&self, kind: SecurityKind) -> bool {
        self.availability.get(&kind) == Some(&KindAvailability::Ready)
    }

    /// The workload a pod belongs to: Pod → ReplicaSet → Deployment and
    /// Pod → Job → CronJob are followed; a bare pod is its own workload.
    pub fn workload_of(&self, pod: &SecPod) -> WorkloadRef {
        let ns = &pod.object.namespace;
        let Some(owner) = &pod.owner else {
            return WorkloadRef {
                kind: WorkloadKind::Pod,
                object: pod.object.clone(),
            };
        };
        let parent_of = |owner: &OwnerRef| -> Option<OwnerRef> {
            match WorkloadKind::from_kind(&owner.kind) {
                WorkloadKind::ReplicaSet => self
                    .replica_sets
                    .iter()
                    .find(|rs| matches_owner(&rs.object, ns, owner))
                    .and_then(|rs| rs.owner.clone())
                    .filter(|o| o.kind == "Deployment"),
                WorkloadKind::Job => self
                    .workloads
                    .iter()
                    .find(|w| {
                        w.workload.kind == WorkloadKind::Job
                            && matches_owner(&w.workload.object, ns, owner)
                    })
                    .and_then(|w| w.owner.clone())
                    .filter(|o| o.kind == "CronJob"),
                _ => None,
            }
        };
        let top = parent_of(owner).unwrap_or_else(|| owner.clone());
        WorkloadRef {
            kind: WorkloadKind::from_kind(&top.kind),
            object: ObjectRef {
                namespace: ns.clone(),
                name: top.name,
                uid: top.uid,
            },
        }
    }
}

fn matches_owner(object: &ObjectRef, ns: &str, owner: &OwnerRef) -> bool {
    object.namespace == ns
        && (if owner.uid.is_empty() || object.uid.is_empty() {
            object.name == owner.name
        } else {
            object.uid == owner.uid
        })
}

/// A running security watch. Dropping it (or [`SecurityWatch::stop`]) cancels
/// every watch.
pub struct SecurityWatch {
    cancel: CancellationToken,
    latest: Arc<RwLock<Option<Arc<SecurityInventory>>>>,
}

impl SecurityWatch {
    /// Start watching on `runtime`. Every published snapshot is also sent on
    /// `updates` (closed receiver → the watch stops).
    pub fn spawn(
        runtime: &tokio::runtime::Handle,
        source: Arc<dyn SecuritySource>,
        cluster_id: u32,
        updates: crossbeam_channel::Sender<Arc<SecurityInventory>>,
    ) -> Self {
        let cancel = CancellationToken::new();
        let latest = Arc::new(RwLock::new(None));
        runtime.spawn(
            run(source, cluster_id, updates, latest.clone(), cancel.clone())
                .instrument(tracing::info_span!("security_watch", cluster_id)),
        );
        Self { cancel, latest }
    }

    /// The most recently published snapshot.
    pub fn latest(&self) -> Option<Arc<SecurityInventory>> {
        self.latest.read().ok()?.clone()
    }

    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

impl Drop for SecurityWatch {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

// ─── Watch task ───────────────────────────────────────────────────────────────

/// A converted object from any kind.
#[derive(Debug, Clone)]
enum Obj {
    Pod(SecPod),
    ReplicaSet(SecReplicaSet),
    Workload(SecWorkload),
    Service(SecService),
    Ingress(SecIngress),
    HttpRoute(SecHttpRoute),
    NetworkPolicy(SecNetworkPolicy),
}

impl Obj {
    fn key(&self) -> String {
        match self {
            Obj::Pod(o) => o.object.key(),
            Obj::ReplicaSet(o) => o.object.key(),
            Obj::Workload(o) => o.workload.object.key(),
            Obj::Service(o) => o.object.key(),
            Obj::Ingress(o) => o.object.key(),
            Obj::HttpRoute(o) => o.object.key(),
            Obj::NetworkPolicy(o) => o.object.key(),
        }
    }
}

type Item = (SecurityKind, Result<Event<Obj>, watcher::Error>);

/// Per-kind state: the live objects plus the buffer for an in-flight re-list.
struct Slot {
    items: BTreeMap<String, Obj>,
    relist: Option<BTreeMap<String, Obj>>,
    availability: KindAvailability,
}

impl Slot {
    fn new() -> Self {
        Self {
            items: BTreeMap::new(),
            relist: None,
            availability: KindAvailability::Pending,
        }
    }

    /// Apply one event; returns whether visible state changed.
    fn apply(&mut self, event: Event<Obj>) -> bool {
        match event {
            Event::Init => {
                self.relist = Some(BTreeMap::new());
                false
            }
            Event::InitApply(o) => {
                self.relist
                    .get_or_insert_with(BTreeMap::new)
                    .insert(o.key(), o);
                false
            }
            Event::InitDone => {
                self.items = self.relist.take().unwrap_or_default();
                self.availability = KindAvailability::Ready;
                true
            }
            Event::Apply(o) => {
                self.items.insert(o.key(), o);
                true
            }
            Event::Delete(o) => self.items.remove(&o.key()).is_some(),
        }
    }
}

/// Map a typed watch stream into [`Item`]s, ending it after a terminal
/// (403/404) error during the initial list.
fn kind_stream<K: Send + 'static>(
    kind: SecurityKind,
    stream: BoxStream<'static, Result<Event<K>, watcher::Error>>,
    convert: fn(&K) -> Obj,
) -> BoxStream<'static, Item> {
    stream
        .scan((false, false), move |(listed, stop), result| {
            if *stop {
                return futures::future::ready(None);
            }
            let item = match result {
                Ok(event) => {
                    if matches!(event, Event::InitDone) {
                        *listed = true;
                    }
                    Ok(map_event(event, convert))
                }
                Err(e) => {
                    if !*listed && terminal_status(&e).is_some() {
                        *stop = true;
                    }
                    Err(e)
                }
            };
            futures::future::ready(Some((kind, item)))
        })
        .boxed()
}

fn map_event<K>(event: Event<K>, convert: fn(&K) -> Obj) -> Event<Obj> {
    match event {
        Event::Apply(k) => Event::Apply(convert(&k)),
        Event::Delete(k) => Event::Delete(convert(&k)),
        Event::Init => Event::Init,
        Event::InitApply(k) => Event::InitApply(convert(&k)),
        Event::InitDone => Event::InitDone,
    }
}

/// 403 → Forbidden, 404 → NotInstalled.
fn terminal_status(e: &watcher::Error) -> Option<KindAvailability> {
    let code = match e {
        watcher::Error::InitialListFailed(kube::Error::Api(s))
        | watcher::Error::WatchStartFailed(kube::Error::Api(s))
        | watcher::Error::WatchFailed(kube::Error::Api(s))
        | watcher::Error::WatchError(s) => s.code,
        _ => return None,
    };
    match code {
        403 => Some(KindAvailability::Forbidden),
        404 => Some(KindAvailability::NotInstalled),
        _ => None,
    }
}

async fn run(
    source: Arc<dyn SecuritySource>,
    cluster_id: u32,
    updates: crossbeam_channel::Sender<Arc<SecurityInventory>>,
    latest: Arc<RwLock<Option<Arc<SecurityInventory>>>>,
    cancel: CancellationToken,
) {
    use SecurityKind as K;
    let mut merged = stream::select_all(vec![
        kind_stream(K::Pod, source.watch_pods(), |o| {
            Obj::Pod(SecPod::from_pod(o))
        }),
        kind_stream(K::ReplicaSet, source.watch_replica_sets(), |o| {
            Obj::ReplicaSet(SecReplicaSet::from_replica_set(o))
        }),
        kind_stream(K::Deployment, source.watch_deployments(), |o| {
            Obj::Workload(SecWorkload::from_deployment(o))
        }),
        kind_stream(K::StatefulSet, source.watch_stateful_sets(), |o| {
            Obj::Workload(SecWorkload::from_stateful_set(o))
        }),
        kind_stream(K::DaemonSet, source.watch_daemon_sets(), |o| {
            Obj::Workload(SecWorkload::from_daemon_set(o))
        }),
        kind_stream(K::Job, source.watch_jobs(), |o| {
            Obj::Workload(SecWorkload::from_job(o))
        }),
        kind_stream(K::CronJob, source.watch_cron_jobs(), |o| {
            Obj::Workload(SecWorkload::from_cron_job(o))
        }),
        kind_stream(K::Service, source.watch_services(), |o| {
            Obj::Service(SecService::from_service(o))
        }),
        kind_stream(K::Ingress, source.watch_ingresses(), |o| {
            Obj::Ingress(SecIngress::from_ingress(o))
        }),
        kind_stream(K::HttpRoute, source.watch_http_routes(), |o| {
            Obj::HttpRoute(SecHttpRoute::from_dynamic(o))
        }),
        kind_stream(K::NetworkPolicy, source.watch_network_policies(), |o| {
            Obj::NetworkPolicy(SecNetworkPolicy::from_network_policy(o))
        }),
    ]);

    let mut slots: BTreeMap<SecurityKind, Slot> = SecurityKind::ALL
        .iter()
        .map(|k| (*k, Slot::new()))
        .collect();
    let started = Instant::now();
    let mut generation = 0u64;
    let mut dirty_since: Option<Instant> = None;
    let mut last_event = Instant::now();
    let mut published_once = false;

    info!("Security watch started");
    loop {
        let settled = slots
            .values()
            .all(|s| s.availability != KindAvailability::Pending);
        let deadline = match dirty_since {
            Some(first) if settled || published_once => {
                Some((last_event + QUIET).min(first + MAX_DELAY))
            }
            Some(_) => Some(started + INITIAL_GRACE),
            None => None,
        };

        tokio::select! {
            _ = cancel.cancelled() => break,
            item = merged.next() => {
                let Some((kind, result)) = item else {
                    debug!("security watch: all streams ended");
                    // Everything terminal (e.g. all forbidden): publish what we know.
                    if dirty_since.is_some() && !publish(&slots, cluster_id, &mut generation, &updates, &latest) {
                        break;
                    }
                    cancel.cancelled().await;
                    break;
                };
                let slot = slots.get_mut(&kind).expect("slot per kind");
                let changed = match result {
                    Ok(event) => slot.apply(event),
                    Err(e) => match terminal_status(&e) {
                        Some(availability) if slot.availability != KindAvailability::Ready => {
                            info!(?kind, ?availability, "security watch: kind unavailable — treated as unknown");
                            slot.availability = availability;
                            true
                        }
                        _ => {
                            if slot.availability == KindAvailability::Ready {
                                debug!(?kind, error = %describe_watch(&e), "security watch: transient error, retrying");
                                false
                            } else {
                                let text = describe_watch(&e);
                                warn!(?kind, error = %text, "security watch: list failing, retrying");
                                slot.availability = KindAvailability::Failing(text);
                                true
                            }
                        }
                    },
                };
                if changed {
                    last_event = Instant::now();
                    dirty_since.get_or_insert(last_event);
                }
            }
            _ = sleep_until_opt(deadline) => {
                if !publish(&slots, cluster_id, &mut generation, &updates, &latest) {
                    break;
                }
                published_once = true;
                dirty_since = None;
            }
        }
    }
    info!("Security watch stopped");
}

async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Build and send a snapshot. Returns `false` once nobody is listening.
fn publish(
    slots: &BTreeMap<SecurityKind, Slot>,
    cluster_id: u32,
    generation: &mut u64,
    updates: &crossbeam_channel::Sender<Arc<SecurityInventory>>,
    latest: &RwLock<Option<Arc<SecurityInventory>>>,
) -> bool {
    *generation += 1;
    let mut inv = SecurityInventory {
        cluster_id,
        generation: *generation,
        availability: slots
            .iter()
            .map(|(k, s)| (*k, s.availability.clone()))
            .collect(),
        ..Default::default()
    };
    for obj in slots.values().flat_map(|s| s.items.values()).cloned() {
        match obj {
            Obj::Pod(o) => inv.pods.push(o),
            Obj::ReplicaSet(o) => inv.replica_sets.push(o),
            Obj::Workload(o) => inv.workloads.push(o),
            Obj::Service(o) => inv.services.push(o),
            Obj::Ingress(o) => inv.ingresses.push(o),
            Obj::HttpRoute(o) => inv.http_routes.push(o),
            Obj::NetworkPolicy(o) => inv.network_policies.push(o),
        }
    }
    debug!(
        generation = inv.generation,
        pods = inv.pods.len(),
        workloads = inv.workloads.len(),
        "security watch: publishing snapshot"
    );
    let inv = Arc::new(inv);
    if let Ok(mut guard) = latest.write() {
        *guard = Some(inv.clone());
    }
    updates.send(inv).is_ok()
}
