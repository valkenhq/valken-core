//! The per-session task that keeps [`ClusterHealth`] current.
//!
//! It owns a Node reflector, reads the watcher's Pod store (shared, not a
//! second watch), polls `metrics.k8s.io` for node usage and recomputes the
//! snapshot when anything changed, at most once per
//! [`ClusterHealthSettings::coalesce`]: pod churn during a rollout is
//! hundreds of events a second and none of them needs its own snapshot.
//!
//! Failure is partial by design.  Without permission to list nodes the
//! snapshot is pod-derived only ([`NodeAccess::Forbidden`]) and the node
//! watch is retried every [`ClusterHealthSettings::node_retry`]; without a
//! metrics-server it falls back to requests ([`UsageStatus::NotInstalled`]).
//! Neither ever fails the watch session.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::runtime::reflector::{self, Store};
use kube::runtime::watcher::{self, Event};
use tokio::sync::{watch, Notify};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::api_stats::ApiStats;
use super::history::{History, DEFAULT_HISTORY_LEN};
use super::snapshot::{ClusterHealth, NodeAccess, ResourceKind, Thresholds, UsageStatus};
use super::summarize::{summarize, Inputs, NodeUsageMap};
use crate::k8s::error::describe_watch;
use crate::k8s::metrics::{MetricsSource, UsageSample};
use crate::k8s::{ClusterSource, MetricsError};

/// Timing and thresholds for the health monitor.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClusterHealthSettings {
    /// How often node usage is read from the metrics-server, which is also
    /// the sparkline sample interval.
    pub usage_interval: Duration,
    /// Minimum time between two recomputed snapshots.
    pub coalesce: Duration,
    /// Points kept per sparkline.
    pub history_len: usize,
    /// How long to wait before retrying a node watch that was forbidden.
    pub node_retry: Duration,
    /// A failed usage read keeps the last good one for this long, so a
    /// single slow answer doesn't flip the UI from usage to requests.
    pub usage_grace: Duration,
    pub thresholds: Thresholds,
}

impl Default for ClusterHealthSettings {
    fn default() -> Self {
        Self {
            usage_interval: Duration::from_secs(30),
            coalesce: Duration::from_secs(1),
            history_len: DEFAULT_HISTORY_LEN,
            node_retry: Duration::from_secs(300),
            usage_grace: Duration::from_secs(90),
            thresholds: Thresholds::default(),
        }
    }
}

/// The sending side of a watcher's health snapshots.  Lives as long as the
/// [`crate::k8s::KubeWatcher`], across sessions.
#[derive(Clone)]
pub(crate) struct HealthPublisher {
    tx: Arc<watch::Sender<Arc<ClusterHealth>>>,
}

impl HealthPublisher {
    pub(crate) fn new() -> Self {
        Self {
            tx: Arc::new(watch::Sender::new(Arc::default())),
        }
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<Arc<ClusterHealth>> {
        self.tx.subscribe()
    }

    pub(crate) fn current(&self) -> Arc<ClusterHealth> {
        self.tx.borrow().clone()
    }

    pub(crate) fn publish(&self, snapshot: ClusterHealth) {
        self.tx.send_replace(Arc::new(snapshot));
    }

    /// Back to the empty snapshot (session ended).
    pub(crate) fn reset(&self) {
        self.tx.send_replace(Arc::default());
    }
}

/// What the monitor needs from its session.
pub(crate) struct MonitorInputs {
    pub source: Arc<dyn ClusterSource>,
    /// The watcher's pod reflector store.
    pub pods: Store<Pod>,
    /// Signalled by the watch loop on every pod event.
    pub pods_changed: Arc<Notify>,
    pub api: Arc<ApiStats>,
    pub publisher: HealthPublisher,
    pub settings: ClusterHealthSettings,
}

type NodeEvents = BoxStream<'static, Result<Event<Node>, watcher::Error>>;

struct NodeWatch {
    store: Store<Node>,
    events: NodeEvents,
}

fn start_node_watch(source: &dyn ClusterSource) -> Option<NodeWatch> {
    let stream = source.watch_nodes()?;
    let (store, writer) = reflector::store();
    Some(NodeWatch {
        store,
        events: reflector::reflector(writer, stream).boxed(),
    })
}

/// Next node event, or pending forever without a watch (so `select!` can
/// always poll it).
async fn next_node(watch: &mut Option<NodeWatch>) -> Option<Result<Event<Node>, watcher::Error>> {
    match watch {
        Some(w) => w.events.next().await,
        None => std::future::pending().await,
    }
}

fn is_forbidden(e: &watcher::Error) -> bool {
    match e {
        watcher::Error::InitialListFailed(kube::Error::Api(s))
        | watcher::Error::WatchStartFailed(kube::Error::Api(s))
        | watcher::Error::WatchFailed(kube::Error::Api(s)) => s.code == 403,
        watcher::Error::WatchError(s) => s.code == 403,
        _ => false,
    }
}

/// How a failed usage read maps onto a status.
pub(crate) fn usage_status_of(e: &MetricsError) -> UsageStatus {
    match e {
        MetricsError::Api(kube::Error::Api(s)) => match s.code {
            404 => UsageStatus::NotInstalled,
            503 => UsageStatus::Unavailable,
            401 | 403 => UsageStatus::Forbidden,
            _ => UsageStatus::Failed(e.to_string()),
        },
        MetricsError::Unsupported => UsageStatus::NotInstalled,
        _ => UsageStatus::Failed(e.to_string()),
    }
}

type UsageFetch = BoxFuture<'static, Result<Vec<UsageSample>, MetricsError>>;

fn fetch_usage(metrics: &Arc<dyn MetricsSource>, timeout: Duration) -> UsageFetch {
    let metrics = metrics.clone();
    async move {
        match tokio::time::timeout(timeout, metrics.node_usages()).await {
            Ok(r) => r,
            Err(_) => Err(MetricsError::Timeout(timeout)),
        }
    }
    .boxed()
}

/// The monitor's mutable state between recomputes.
struct State {
    node_access: NodeAccess,
    usage: NodeUsageMap,
    usage_status: UsageStatus,
    last_usage_ok: Option<Instant>,
    history: History,
    since: SystemTime,
}

impl State {
    fn apply_usage(&mut self, result: Result<Vec<UsageSample>, MetricsError>, grace: Duration) {
        match result {
            Ok(samples) => {
                self.usage = samples.into_iter().map(|s| (s.name, s.usage)).collect();
                self.usage_status = UsageStatus::MetricsServer {
                    sampled: SystemTime::now(),
                };
                self.last_usage_ok = Some(Instant::now());
            }
            Err(e) => {
                let status = usage_status_of(&e);
                let recent = self.last_usage_ok.is_some_and(|t| t.elapsed() < grace);
                if recent && matches!(status, UsageStatus::Unavailable | UsageStatus::Failed(_)) {
                    debug!(error = %e, "Node usage read failed; keeping the last reading");
                    return;
                }
                if status != self.usage_status {
                    match &status {
                        UsageStatus::NotInstalled => {
                            info!("No metrics-server: cluster health shows requests")
                        }
                        _ => warn!(error = %e, "Node usage unavailable"),
                    }
                }
                self.usage.clear();
                self.usage_status = status;
                self.last_usage_ok = None;
            }
        }
    }
}

fn compute(
    state: &State,
    nodes: Option<&NodeWatch>,
    pods: &Store<Pod>,
    api: &ApiStats,
    thresholds: Thresholds,
) -> ClusterHealth {
    let nodes = match (state.node_access, nodes) {
        (NodeAccess::Visible, Some(w)) => w.store.state(),
        _ => Vec::new(),
    };
    let pods = pods.state();
    let mut snapshot = summarize(&Inputs {
        nodes: &nodes,
        node_access: state.node_access,
        pods: &pods,
        usage: &state.usage,
        usage_status: &state.usage_status,
        api: api.summary(),
        thresholds,
    });
    snapshot.history = state.history.clone();
    snapshot.since = Some(state.since);
    snapshot.updated = Some(SystemTime::now());
    snapshot
}

fn sample(history: &mut History, snapshot: &ClusterHealth, api: &ApiStats, interval: Duration) {
    let t = &snapshot.totals;
    history.cpu_percent.push(t.percent(ResourceKind::Cpu));
    history.memory_percent.push(t.percent(ResourceKind::Memory));
    history.pods_running.push(Some(t.pods.running as f32));
    history
        .api_p95_ms
        .push(api.p95_over(interval).map(|d| d.as_secs_f32() * 1000.0));
}

/// Run until `cancel` fires.  Publishes nothing before the pod store has
/// its initial list, so the first snapshot is never a misleading "0 pods".
pub(crate) async fn run_cluster_health(inputs: MonitorInputs, cancel: CancellationToken) {
    let MonitorInputs {
        source,
        pods,
        pods_changed,
        api,
        publisher,
        settings,
    } = inputs;

    tokio::select! {
        _ = cancel.cancelled() => return,
        ready = pods.wait_until_ready() => if ready.is_err() {
            debug!("Pod store dropped before it was ready; health monitor exiting");
            return;
        },
    }

    let mut nodes = start_node_watch(source.as_ref());
    let metrics = source.metrics();
    let mut state = State {
        node_access: if nodes.is_some() {
            NodeAccess::Pending
        } else {
            NodeAccess::Unsupported
        },
        usage: NodeUsageMap::new(),
        usage_status: if metrics.is_some() {
            UsageStatus::Pending
        } else {
            UsageStatus::NotInstalled
        },
        last_usage_ok: None,
        history: History::with_capacity(settings.history_len),
        since: SystemTime::now(),
    };
    info!(
        nodes = nodes.is_some(),
        metrics = metrics.is_some(),
        "Cluster health monitor started"
    );

    let mut tick = tokio::time::interval(settings.usage_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut in_flight: Option<UsageFetch> = None;
    let mut node_retry_at: Option<Instant> = None;
    let mut dirty = true;
    let mut last_compute: Option<Instant> = None;
    // Set on a tick: the next snapshot is also a sparkline sample.
    let mut sample_due = false;

    loop {
        let compute_at = last_compute.map_or_else(Instant::now, |t| t + settings.coalesce);
        tokio::select! {
            _ = cancel.cancelled() => break,

            ev = next_node(&mut nodes) => match ev {
                Some(Ok(Event::InitDone)) => {
                    if state.node_access != NodeAccess::Visible {
                        info!("Node list received");
                    }
                    state.node_access = NodeAccess::Visible;
                    dirty = true;
                }
                Some(Ok(Event::Apply(_) | Event::Delete(_))) => dirty = true,
                Some(Ok(Event::Init | Event::InitApply(_))) => {}
                Some(Err(e)) if is_forbidden(&e) => {
                    info!(
                        retry_secs = settings.node_retry.as_secs(),
                        "Not allowed to list nodes; cluster health is pod-derived"
                    );
                    nodes = None;
                    state.node_access = NodeAccess::Forbidden;
                    node_retry_at = Some(Instant::now() + settings.node_retry);
                    dirty = true;
                }
                // Transient: the stream's backoff retries; keep the store.
                Some(Err(e)) => debug!(error = %describe_watch(&e), "Node watch error"),
                None => {
                    debug!("Node watch ended");
                    nodes = None;
                    node_retry_at = Some(Instant::now() + settings.node_retry);
                }
            },

            _ = tokio::time::sleep_until(node_retry_at.unwrap_or_else(Instant::now)),
                if node_retry_at.is_some() =>
            {
                node_retry_at = None;
                nodes = start_node_watch(source.as_ref());
            }

            _ = pods_changed.notified() => dirty = true,

            _ = tick.tick() => {
                match &metrics {
                    // A read still in flight from the last tick: skip this one.
                    Some(_) if in_flight.is_some() => {}
                    Some(m) => in_flight = Some(fetch_usage(m, settings.usage_interval)),
                    None => {
                        sample_due = true;
                        dirty = true;
                    }
                }
            }

            result = async { in_flight.as_mut().expect("guarded").await }, if in_flight.is_some() => {
                in_flight = None;
                state.apply_usage(result, settings.usage_grace);
                sample_due = true;
                dirty = true;
                // A sample is a fresh reading: don't wait out the coalesce window.
                last_compute = None;
            }

            _ = tokio::time::sleep_until(compute_at), if dirty => {
                dirty = false;
                last_compute = Some(Instant::now());
                let mut snapshot = compute(&state, nodes.as_ref(), &pods, &api, settings.thresholds);
                if sample_due {
                    sample_due = false;
                    sample(&mut state.history, &snapshot, &api, settings.usage_interval);
                    snapshot.history = state.history.clone();
                }
                publisher.publish(snapshot);
            }
        }
    }
    info!("Cluster health monitor stopped");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(code: u16) -> MetricsError {
        MetricsError::Api(kube::Error::Api(
            kube::core::Status::failure("x", "y")
                .with_code(code)
                .boxed(),
        ))
    }

    #[test]
    fn usage_errors_map_to_statuses() {
        assert_eq!(usage_status_of(&status(404)), UsageStatus::NotInstalled);
        assert_eq!(usage_status_of(&status(503)), UsageStatus::Unavailable);
        assert_eq!(usage_status_of(&status(403)), UsageStatus::Forbidden);
        assert!(matches!(
            usage_status_of(&status(500)),
            UsageStatus::Failed(_)
        ));
        assert_eq!(
            usage_status_of(&MetricsError::Unsupported),
            UsageStatus::NotInstalled
        );
    }

    #[test]
    fn forbidden_watch_errors_are_recognised() {
        let api = |code| {
            kube::Error::Api(
                kube::core::Status::failure("x", "y")
                    .with_code(code)
                    .boxed(),
            )
        };
        assert!(is_forbidden(&watcher::Error::InitialListFailed(api(403))));
        assert!(is_forbidden(&watcher::Error::WatchFailed(api(403))));
        assert!(!is_forbidden(&watcher::Error::InitialListFailed(api(500))));
        assert!(!is_forbidden(&watcher::Error::NoResourceVersion));
    }

    fn state() -> State {
        State {
            node_access: NodeAccess::Visible,
            usage: NodeUsageMap::new(),
            usage_status: UsageStatus::Pending,
            last_usage_ok: None,
            history: History::default(),
            since: SystemTime::UNIX_EPOCH,
        }
    }

    fn ok(name: &str) -> Result<Vec<UsageSample>, MetricsError> {
        Ok(vec![UsageSample {
            name: name.into(),
            usage: super::super::Resources::new(1, 2),
        }])
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_read_keeps_a_recent_reading_then_gives_up() {
        let grace = Duration::from_secs(90);
        let mut s = state();
        s.apply_usage(ok("a"), grace);
        assert!(s.usage_status.is_available());

        tokio::time::advance(Duration::from_secs(30)).await;
        s.apply_usage(Err(status(503)), grace);
        assert!(s.usage_status.is_available(), "within grace");
        assert!(s.usage.contains_key("a"));

        tokio::time::advance(Duration::from_secs(61)).await;
        s.apply_usage(Err(status(503)), grace);
        assert_eq!(s.usage_status, UsageStatus::Unavailable);
        assert!(s.usage.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn not_installed_is_reported_at_once_even_within_grace() {
        let mut s = state();
        s.apply_usage(ok("a"), Duration::from_secs(90));
        s.apply_usage(Err(status(404)), Duration::from_secs(90));
        assert_eq!(s.usage_status, UsageStatus::NotInstalled);
    }
}
