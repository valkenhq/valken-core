//! Cluster-wide CPU / memory utilisation, polled from the metrics-server.
//!
//! Node *capacity* changes only when nodes join or leave, so it is cached
//! for [`ClusterMetricsSettings::capacity_ttl`].  Node *usage* is the only
//! hot path and is fetched every [`ClusterMetricsSettings::poll_interval`].
//! The UI is notified only when a value moved by more than the trend
//! threshold, which is also exactly when a trend arrow could have flipped.

use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::async_runtime::{AsyncMessage, MetricTrend, Notifier};
use crate::k8s::metrics::{MetricsSource, NodeCapacity};

/// Threshold for showing trend arrows (relative change, percent).
pub(crate) const METRICS_TREND_THRESHOLD_PERCENT: f32 = 8.0;

/// Timing knobs for the monitor.  Tests shrink them and run under
/// `tokio::time::pause()`.
#[derive(Debug, Clone, Copy)]
pub struct ClusterMetricsSettings {
    /// Delay before the first API request, so the rest of the app can finish
    /// initialising.
    pub initial_delay: Duration,
    /// How often to fetch usage.
    pub poll_interval: Duration,
    /// How long cached node capacity stays valid.
    pub capacity_ttl: Duration,
}

impl Default for ClusterMetricsSettings {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(5),
            poll_interval: Duration::from_secs(60),
            capacity_ttl: Duration::from_secs(300),
        }
    }
}

// ---------------------------------------------------------------------------
// Pure trend / change logic
// ---------------------------------------------------------------------------

/// Relative change of `current` against `previous`, in percent.
/// `previous == 0` is treated as an infinite change when `current > 0`.
fn relative_change_percent(current: f32, previous: f32) -> f32 {
    if previous == 0.0 {
        if current > 0.0 {
            f32::INFINITY
        } else {
            0.0
        }
    } else {
        ((current - previous) / previous).abs() * 100.0
    }
}

/// Returns `true` when `current` differs from `previous` by at least
/// [`METRICS_TREND_THRESHOLD_PERCENT`] relative to `previous`.
pub(crate) fn changed_meaningfully(current: f32, previous: f32) -> bool {
    relative_change_percent(current, previous) >= METRICS_TREND_THRESHOLD_PERCENT
}

/// Trend of `current` relative to `previous`, or `Stable` with no history.
pub(crate) fn calculate_trend(current: f32, previous: Option<f32>) -> MetricTrend {
    match previous {
        None => MetricTrend::Stable,
        Some(prev) if !changed_meaningfully(current, prev) => MetricTrend::Stable,
        Some(prev) if current > prev => MetricTrend::Up,
        Some(_) => MetricTrend::Down,
    }
}

/// The previous reading, used for trend arrows and change suppression.
#[derive(Debug, Default, Clone, Copy)]
pub struct MetricsTrendState {
    cluster_id: u32,
    last: Option<(f32, f32)>,
}

impl MetricsTrendState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Trend state whose messages are tagged with `cluster_id`.
    pub fn for_cluster(cluster_id: u32) -> Self {
        Self {
            cluster_id,
            last: None,
        }
    }

    /// Fold in a new reading.  Returns the message to send, or `None` when
    /// neither value changed enough to be worth a UI update.
    pub fn observe(&mut self, cpu_percent: f32, memory_percent: f32) -> Option<AsyncMessage> {
        let prev = self.last;
        self.last = Some((cpu_percent, memory_percent));
        let should_send = prev.is_none_or(|(c, m)| {
            changed_meaningfully(cpu_percent, c) || changed_meaningfully(memory_percent, m)
        });
        should_send.then(|| AsyncMessage::ClusterMetrics {
            cluster_id: self.cluster_id,
            cpu_percent,
            memory_percent,
            cpu_trend: calculate_trend(cpu_percent, prev.map(|p| p.0)),
            memory_trend: calculate_trend(memory_percent, prev.map(|p| p.1)),
        })
    }
}

fn percent(used: i64, capacity: i64) -> f32 {
    if capacity > 0 {
        (used as f32 / capacity as f32) * 100.0
    } else {
        0.0
    }
}

// ---------------------------------------------------------------------------
// Monitor
// ---------------------------------------------------------------------------

/// Handle to a running cluster metrics monitor.  Dropping it does not stop
/// the task; cancel the token passed at spawn time.
pub struct ClusterMetricsMonitor {
    join: JoinHandle<()>,
}

impl ClusterMetricsMonitor {
    pub fn is_finished(&self) -> bool {
        self.join.is_finished()
    }

    pub fn abort(&self) {
        self.join.abort();
    }

    /// The task's join handle, for callers that await shutdown.
    pub fn into_join(self) -> JoinHandle<()> {
        self.join
    }
}

/// Spawn [`run_cluster_metrics_monitor`] on `handle`.
pub fn spawn_cluster_metrics_monitor(
    handle: &Handle,
    source: Arc<dyn MetricsSource>,
    notifier: Arc<dyn Notifier>,
    cluster_id: u32,
    cancel: CancellationToken,
    settings: ClusterMetricsSettings,
) -> ClusterMetricsMonitor {
    let join = handle.spawn(run_cluster_metrics_monitor(
        source, notifier, cluster_id, cancel, settings,
    ));
    ClusterMetricsMonitor { join }
}

/// Poll `source` until `cancel` fires, sending
/// [`AsyncMessage::ClusterMetrics`] (tagged with `cluster_id`) to `notifier`
/// on meaningful change.
pub async fn run_cluster_metrics_monitor(
    source: Arc<dyn MetricsSource>,
    notifier: Arc<dyn Notifier>,
    cluster_id: u32,
    cancel: CancellationToken,
    settings: ClusterMetricsSettings,
) {
    info!(
        poll_interval_secs = settings.poll_interval.as_secs(),
        capacity_ttl_secs = settings.capacity_ttl.as_secs(),
        "Cluster metrics monitor started",
    );

    if sleep_or_cancel(settings.initial_delay, &cancel).await {
        return;
    }

    let mut capacity: Option<(NodeCapacity, tokio::time::Instant)> = None;
    let mut trend = MetricsTrendState::for_cluster(cluster_id);
    let mut poll_count: u64 = 0;

    loop {
        poll_count += 1;
        debug!(poll_count, "Cluster metrics: poll tick");

        let expired = capacity.is_none_or(|(_, at)| at.elapsed() >= settings.capacity_ttl);
        if expired {
            match source.node_capacity().await {
                Ok(fresh) => {
                    info!(
                        cpu_millicores = fresh.cpu_millicores,
                        memory_bytes = fresh.memory_bytes,
                        "Cluster metrics: node capacity refreshed",
                    );
                    capacity = Some((fresh, tokio::time::Instant::now()));
                }
                Err(e) => error!(error = %e, "Cluster metrics: failed to fetch node capacity"),
            }
        }

        if let Some((cap, _)) = capacity {
            match source.node_usage().await {
                Ok(usage) => {
                    let cpu_percent = percent(usage.cpu_millicores, cap.cpu_millicores);
                    let memory_percent = percent(usage.memory_bytes, cap.memory_bytes);
                    debug!(
                        cpu_percent,
                        memory_percent, "Cluster metrics: usage fetched"
                    );
                    match trend.observe(cpu_percent, memory_percent) {
                        Some(msg) => notifier.notify(msg),
                        None => debug!("Cluster metrics: no meaningful change"),
                    }
                }
                Err(e) => error!(
                    error = %e,
                    "Cluster metrics: failed to fetch node usage — metrics-server may not be installed"
                ),
            }
        } else {
            debug!("Cluster metrics: skipping usage fetch — no capacity data yet");
        }

        if sleep_or_cancel(settings.poll_interval, &cancel).await {
            break;
        }
    }

    info!("Cluster metrics monitor stopped");
}

/// Sleep for `dur`; returns `true` if cancelled first.
async fn sleep_or_cancel(dur: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => true,
        _ = tokio::time::sleep(dur) => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_runtime::RecordingNotifier;
    use crate::k8s::metrics::{NodeUsage, PodUsage};
    use crate::k8s::MetricsError;
    use async_trait::async_trait;
    use std::sync::Mutex;

    #[test]
    fn changed_meaningfully_threshold() {
        assert!(!changed_meaningfully(50.0, 50.0));
        assert!(!changed_meaningfully(52.0, 50.0)); // 4 %
        assert!(changed_meaningfully(54.0, 50.0)); // exactly 8 %
        assert!(changed_meaningfully(80.0, 50.0));
        assert!(changed_meaningfully(20.0, 50.0));
        assert!(changed_meaningfully(1.0, 0.0));
        assert!(!changed_meaningfully(0.0, 0.0));
    }

    #[test]
    fn trend_direction() {
        assert_eq!(calculate_trend(50.0, None), MetricTrend::Stable);
        assert_eq!(calculate_trend(50.0, Some(50.0)), MetricTrend::Stable);
        assert_eq!(calculate_trend(53.0, Some(50.0)), MetricTrend::Stable);
        assert_eq!(calculate_trend(55.0, Some(50.0)), MetricTrend::Up);
        assert_eq!(calculate_trend(45.0, Some(50.0)), MetricTrend::Down);
        assert_eq!(calculate_trend(10.0, Some(0.0)), MetricTrend::Up);
        assert_eq!(calculate_trend(0.0, Some(0.0)), MetricTrend::Stable);
    }

    #[test]
    fn trend_state_sends_first_reading_then_only_meaningful_changes() {
        let mut s = MetricsTrendState::new();
        let first = s.observe(40.0, 60.0).expect("first reading always sent");
        assert!(matches!(
            first,
            AsyncMessage::ClusterMetrics {
                cpu_trend: MetricTrend::Stable,
                ..
            }
        ));
        assert!(s.observe(41.0, 61.0).is_none(), "tiny change suppressed");
        let up = s.observe(50.0, 61.0).expect("cpu moved 25 %");
        assert!(matches!(
            up,
            AsyncMessage::ClusterMetrics {
                cpu_trend: MetricTrend::Up,
                memory_trend: MetricTrend::Stable,
                ..
            }
        ));
    }

    #[test]
    fn percent_handles_zero_capacity() {
        assert_eq!(percent(500, 0), 0.0);
        assert_eq!(percent(500, 1000), 50.0);
    }

    /// Scripted source: fixed capacity, a queue of usage readings.
    struct FakeSource {
        capacity_calls: Mutex<u32>,
        usage: Mutex<Vec<NodeUsage>>,
    }

    #[async_trait]
    impl MetricsSource for FakeSource {
        async fn node_capacity(&self) -> Result<NodeCapacity, MetricsError> {
            *self.capacity_calls.lock().unwrap() += 1;
            Ok(NodeCapacity {
                cpu_millicores: 4000,
                memory_bytes: 8 << 30,
            })
        }
        async fn node_usage(&self) -> Result<NodeUsage, MetricsError> {
            let mut q = self.usage.lock().unwrap();
            if q.is_empty() {
                Ok(NodeUsage::default())
            } else {
                Ok(q.remove(0))
            }
        }
        async fn pod_usage(&self, _: &str, _: &str) -> Result<PodUsage, MetricsError> {
            unimplemented!()
        }
        async fn pod_restarts(&self, _: &str, _: &str) -> Result<i32, MetricsError> {
            unimplemented!()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn monitor_caches_capacity_and_notifies_on_change() {
        let source = Arc::new(FakeSource {
            capacity_calls: Mutex::new(0),
            usage: Mutex::new(vec![
                NodeUsage {
                    cpu_millicores: 1000,
                    memory_bytes: 4 << 30,
                },
                NodeUsage {
                    cpu_millicores: 1010,
                    memory_bytes: 4 << 30,
                }, // ~1 % change: suppressed
                NodeUsage {
                    cpu_millicores: 2000,
                    memory_bytes: 4 << 30,
                }, // up
            ]),
        });
        let notifier = Arc::new(RecordingNotifier::new());
        let cancel = CancellationToken::new();
        let settings = ClusterMetricsSettings {
            initial_delay: Duration::from_millis(10),
            poll_interval: Duration::from_secs(10),
            capacity_ttl: Duration::from_secs(1000),
        };
        let task = tokio::spawn(run_cluster_metrics_monitor(
            source.clone(),
            notifier.clone(),
            7,
            cancel.clone(),
            settings,
        ));

        // Three polls happen at t=10ms, 10s, 20s under paused time.
        tokio::time::sleep(Duration::from_secs(25)).await;
        cancel.cancel();
        task.await.unwrap();

        let msgs = notifier.messages();
        assert_eq!(
            msgs.len(),
            2,
            "first reading + the meaningful change: {msgs:?}"
        );
        assert!(matches!(
            msgs[1],
            AsyncMessage::ClusterMetrics { cluster_id: 7, cpu_trend: MetricTrend::Up, cpu_percent, .. } if (cpu_percent - 50.0).abs() < 0.01
        ));
        assert_eq!(
            *source.capacity_calls.lock().unwrap(),
            1,
            "capacity fetched once within its TTL"
        );
    }
}
