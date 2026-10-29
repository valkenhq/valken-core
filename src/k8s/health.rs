//! Per-cluster connection health.
//!
//! Every [`crate::k8s::KubeWatcher`] publishes a [`Health`] on a
//! [`tokio::sync::watch`] channel while it is watching.  A light pinger
//! ([`ClusterSource::ping`], an authenticated one-item namespace list) runs
//! every [`HealthSettings::interval`] and is the only thing that moves the
//! state; a failing watch stream wakes it early so an expired token or a
//! dropped VPN shows up in seconds rather than at the next tick.
//!
//! Readers are UI frames polling many clusters, so reading is a cheap
//! [`tokio::sync::watch::Receiver::borrow`] with no locking on the hot path
//! of the watch loop.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::{watch, Notify};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn, Instrument};

use crate::k8s::kubeconfig::probe::{classify_ref, ProbeError};
use crate::k8s::ClusterSource;

/// Where the connection to one cluster stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LinkState {
    /// Watching has not been confirmed yet (before the first ping).
    #[default]
    Connecting,
    /// The last ping succeeded.
    Live,
    /// The API server rejected the credentials, or the credential plugin
    /// failed: data is stale until the user re-authenticates.
    AuthExpired,
    /// No answer, connection refused, DNS / TLS failure.
    Unreachable,
}

impl LinkState {
    /// Stable lowercase name for the Godot boundary and logs.
    pub fn as_str(self) -> &'static str {
        match self {
            LinkState::Connecting => "connecting",
            LinkState::Live => "live",
            LinkState::AuthExpired => "auth_expired",
            LinkState::Unreachable => "unreachable",
        }
    }

    /// How a failed ping maps onto a state.
    pub fn from_error(err: &ProbeError) -> Self {
        match err {
            ProbeError::Unauthorized(_) | ProbeError::Plugin(_) | ProbeError::PluginNotFound(_) => {
                LinkState::AuthExpired
            }
            ProbeError::Timeout(_) | ProbeError::Unreachable(_) | ProbeError::Config(_) => {
                LinkState::Unreachable
            }
        }
    }
}

/// A snapshot of one cluster's connection health.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Health {
    pub state: LinkState,
    /// Round-trip of the last successful ping.
    pub latency: Option<Duration>,
    /// When the cluster last answered.  Drives "stale · 14m".
    pub last_ok: Option<SystemTime>,
    /// When `state` last changed.
    pub since: Option<SystemTime>,
    /// Human-readable reason for a non-live state.
    pub detail: Option<String>,
}

impl Health {
    /// Fold in a successful ping.
    pub fn record_ok(&mut self, latency: Duration, now: SystemTime) {
        self.set_state(LinkState::Live, now);
        self.latency = Some(latency);
        self.last_ok = Some(now);
        self.detail = None;
    }

    /// Fold in a failed ping.  Latency is kept: it describes the last time
    /// the cluster answered, which is what a stale region still shows.
    pub fn record_err(&mut self, err: &ProbeError, now: SystemTime) {
        self.set_state(LinkState::from_error(err), now);
        self.detail = Some(err.to_string());
    }

    /// Age of the data relative to `now`, once the cluster has answered at
    /// least once.
    pub fn stale_for(&self, now: SystemTime) -> Option<Duration> {
        self.last_ok.and_then(|t| now.duration_since(t).ok())
    }

    fn set_state(&mut self, state: LinkState, now: SystemTime) {
        if self.state != state || self.since.is_none() {
            self.state = state;
            self.since = Some(now);
        }
    }
}

/// Timing knobs for the pinger.  Tests shrink them.
#[derive(Debug, Clone, Copy)]
pub struct HealthSettings {
    /// Time between pings while nothing is wrong.
    pub interval: Duration,
    /// How long a ping may take before it counts as unreachable.
    pub timeout: Duration,
    /// Minimum gap between pings, so a watch stream failing in a hot loop
    /// cannot turn into a ping storm.
    pub min_gap: Duration,
}

impl Default for HealthSettings {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(15),
            timeout: Duration::from_secs(10),
            min_gap: Duration::from_secs(2),
        }
    }
}

/// Write side of a cluster's health, shared by the pinger and the watch
/// loop.  Cheap to clone.
#[derive(Clone)]
pub(crate) struct HealthReporter {
    tx: Arc<watch::Sender<Health>>,
    wake: Arc<Notify>,
}

impl HealthReporter {
    pub(crate) fn new() -> Self {
        Self {
            tx: Arc::new(watch::Sender::new(Health::default())),
            wake: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<Health> {
        self.tx.subscribe()
    }

    pub(crate) fn current(&self) -> Health {
        self.tx.borrow().clone()
    }

    /// A watch stream failed: ping now instead of at the next tick.
    pub(crate) fn nudge(&self) {
        self.wake.notify_one();
    }

    pub(crate) fn reset(&self) {
        self.tx.send_replace(Health::default());
    }

    fn update(&self, f: impl FnOnce(&mut Health)) {
        self.tx.send_if_modified(|h| {
            let before = h.clone();
            f(h);
            *h != before
        });
    }
}

/// Map a kube-runtime watch error onto a probe error when it says something
/// about reachability or credentials; `None` for errors that don't (e.g. a
/// missing resource version, which kube-runtime recovers from on its own).
pub(crate) fn watch_error_cause(err: &kube::runtime::watcher::Error) -> Option<ProbeError> {
    use kube::runtime::watcher::Error as W;
    match err {
        W::InitialListFailed(e) | W::WatchStartFailed(e) | W::WatchFailed(e) => {
            Some(classify_ref(e))
        }
        W::WatchError(status) if status.code == 401 => {
            Some(ProbeError::Unauthorized(status.message.clone()))
        }
        _ => None,
    }
}

/// Ping `source` until `cancel` fires, publishing each outcome.
pub(crate) async fn run_health_monitor(
    source: Arc<dyn ClusterSource>,
    reporter: HealthReporter,
    cancel: CancellationToken,
    settings: HealthSettings,
) {
    debug!(
        interval_secs = settings.interval.as_secs(),
        "Health monitor started"
    );
    loop {
        let started = Instant::now();
        let outcome = tokio::select! {
            _ = cancel.cancelled() => break,
            r = tokio::time::timeout(settings.timeout, source.ping())
                .instrument(tracing::debug_span!("kube.health.ping")) => r,
        };
        let now = SystemTime::now();
        let previous = reporter.current().state;
        match outcome {
            Ok(Ok(())) => {
                let latency = started.elapsed();
                reporter.update(|h| h.record_ok(latency, now));
            }
            Ok(Err(e)) => reporter.update(|h| h.record_err(&e, now)),
            Err(_) => {
                reporter.update(|h| h.record_err(&ProbeError::Timeout(settings.timeout), now))
            }
        }
        let state = reporter.current().state;
        if state != previous {
            match state {
                LinkState::Live => info!(from = previous.as_str(), "Cluster reachable"),
                _ => {
                    warn!(state = state.as_str(), detail = ?reporter.current().detail, "Cluster health changed")
                }
            }
        }

        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(settings.interval) => {}
            _ = reporter.wake.notified() => {
                // Rate-limit nudges from a failing stream.
                let elapsed = started.elapsed();
                if elapsed < settings.min_gap {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = tokio::time::sleep(settings.min_gap - elapsed) => {}
                    }
                }
            }
        }
    }
    debug!("Health monitor stopped");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000 + secs)
    }

    #[test]
    fn errors_map_to_states() {
        let cases = [
            (ProbeError::Unauthorized("x".into()), LinkState::AuthExpired),
            (ProbeError::Plugin("x".into()), LinkState::AuthExpired),
            (
                ProbeError::Timeout(Duration::from_secs(1)),
                LinkState::Unreachable,
            ),
            (ProbeError::Unreachable("x".into()), LinkState::Unreachable),
            (ProbeError::Config("x".into()), LinkState::Unreachable),
        ];
        for (err, want) in cases {
            assert_eq!(LinkState::from_error(&err), want, "{err}");
        }
    }

    #[test]
    fn ok_then_failure_keeps_latency_and_last_ok() {
        let mut h = Health::default();
        assert_eq!(h.state, LinkState::Connecting);
        h.record_ok(Duration::from_millis(18), t(0));
        assert_eq!(h.state, LinkState::Live);
        assert_eq!(h.since, Some(t(0)));

        h.record_err(&ProbeError::Unreachable("refused".into()), t(30));
        assert_eq!(h.state, LinkState::Unreachable);
        assert_eq!(h.latency, Some(Duration::from_millis(18)));
        assert_eq!(h.last_ok, Some(t(0)));
        assert_eq!(h.since, Some(t(30)));
        assert_eq!(
            h.stale_for(t(30 + 14 * 60)),
            Some(Duration::from_secs(30 + 14 * 60))
        );
        assert!(h.detail.as_deref().unwrap().contains("refused"));
    }

    #[test]
    fn since_only_moves_when_the_state_changes() {
        let mut h = Health::default();
        h.record_err(&ProbeError::Unauthorized("401".into()), t(0));
        h.record_err(&ProbeError::Unauthorized("401".into()), t(15));
        assert_eq!(h.since, Some(t(0)));
        h.record_ok(Duration::from_millis(5), t(30));
        assert_eq!(h.since, Some(t(30)));
        assert_eq!(h.detail, None);
        h.record_ok(Duration::from_millis(6), t(45));
        assert_eq!(h.since, Some(t(30)), "still live");
        assert_eq!(h.last_ok, Some(t(45)));
    }

    #[test]
    fn stale_for_is_none_before_first_answer() {
        assert_eq!(Health::default().stale_for(t(5)), None);
    }

    #[test]
    fn watch_errors_that_say_nothing_about_the_link_are_ignored() {
        use kube::runtime::watcher::Error as W;
        assert!(watch_error_cause(&W::NoResourceVersion).is_none());
        let unauthorized =
            kube::core::Status::failure("Unauthorized", "Unauthorized").with_code(401);
        assert!(matches!(
            watch_error_cause(&W::WatchFailed(kube::Error::Api(unauthorized.boxed()))),
            Some(ProbeError::Unauthorized(_))
        ));
    }

    #[test]
    fn reporter_only_notifies_on_change() {
        let r = HealthReporter::new();
        let mut rx = r.subscribe();
        rx.mark_unchanged();
        r.update(|h| h.record_ok(Duration::from_millis(3), t(0)));
        assert!(rx.has_changed().unwrap());
        rx.mark_unchanged();
        r.update(|_| {});
        assert!(!rx.has_changed().unwrap());
        r.reset();
        assert_eq!(r.current(), Health::default());
    }
}
