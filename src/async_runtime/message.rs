//! Messages from background tasks to the UI thread, and the [`Notifier`]
//! seam through which they are sent.
//!
//! Nothing in this file depends on Godot.  The Godot `AsyncNotifier` node
//! drains [`global_receiver`] on a timer and turns each message into a
//! signal; tests implement [`Notifier`] with a `Vec` behind a mutex.

use std::sync::{Arc, Mutex, OnceLock};

use crossbeam_channel::{unbounded, Receiver, Sender};

/// Messages that can be sent from async tasks to the main thread.
///
/// The metrics monitors send `ClusterMetrics` and `PodMetrics`; the
/// connection messages are sent by the host application once its own
/// connect flow (database row, [`crate::k8s::KubeWatcher::set_cluster_id`])
/// has finished, so the UI learns of a connection and its cluster id in one
/// step.
#[derive(Debug, Clone, PartialEq)]
pub enum AsyncMessage {
    /// A cluster connection succeeded.
    ConnectionSuccess {
        cluster_name: String,
        namespace: String,
        /// kube-system namespace UID — stable, unique per cluster.
        cluster_uuid: String,
        /// Local integer ID from the `clusters` DB table.
        cluster_id: u32,
    },
    /// A cluster connection failed; `error` is already safe to display.
    ConnectionFailure { error: String },
    /// Cluster resource metrics update.
    ClusterMetrics {
        /// Local integer ID from the `clusters` DB table.
        cluster_id: u32,
        cpu_percent: f32,
        memory_percent: f32,
        cpu_trend: MetricTrend,
        memory_trend: MetricTrend,
    },
    /// Per-pod CPU / memory metrics update, emitted every ~5 seconds while a
    /// pod is selected in the sidebar.
    PodMetrics {
        /// Cluster the pod lives in (local `clusters` table ID).
        cluster_id: u32,
        pod_name: String,
        namespace: String,
        cpu_millicores: f32,
        memory_mib: f32,
        restart_count: i32,
    },
}

/// Trend direction for metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricTrend {
    /// Increased by more than the threshold.
    Up,
    /// Decreased by more than the threshold.
    Down,
    /// Changed less than the threshold.
    Stable,
}

impl MetricTrend {
    /// Integer form used on the Godot signal boundary: `1` up, `-1` down, `0` stable.
    pub fn as_i32(self) -> i32 {
        match self {
            MetricTrend::Up => 1,
            MetricTrend::Down => -1,
            MetricTrend::Stable => 0,
        }
    }
}

/// Destination for [`AsyncMessage`]s produced by background tasks.
///
/// Production code uses the global channel ([`global_notifier`]); tests pass
/// a [`RecordingNotifier`] and assert on what was sent.
pub trait Notifier: Send + Sync {
    fn notify(&self, message: AsyncMessage);
}

impl Notifier for Sender<AsyncMessage> {
    fn notify(&self, message: AsyncMessage) {
        // The receiver is held in the global channel for the life of the
        // process, so a send only fails during shutdown.
        if let Err(e) = self.send(message) {
            tracing::debug!(error = %e, "AsyncMessage dropped: receiver gone");
        }
    }
}

impl<T: Notifier + ?Sized> Notifier for Arc<T> {
    fn notify(&self, message: AsyncMessage) {
        (**self).notify(message)
    }
}

/// Test notifier that records every message it is given.
#[derive(Debug, Default)]
pub struct RecordingNotifier {
    messages: Mutex<Vec<AsyncMessage>>,
}

impl RecordingNotifier {
    pub fn new() -> Self {
        Self::default()
    }

    /// Copy of every message received so far, in order.
    pub fn messages(&self) -> Vec<AsyncMessage> {
        self.messages
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    pub fn len(&self) -> usize {
        self.messages
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Notifier for RecordingNotifier {
    fn notify(&self, message: AsyncMessage) {
        self.messages
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(message);
    }
}

/// The process-wide channel between background tasks and the UI thread.
///
/// Both halves live here so that a UI node which is freed and re-created
/// (scene change) picks the receiver back up instead of leaving an
/// unbounded channel with no reader.
static GLOBAL_CHANNEL: OnceLock<(Sender<AsyncMessage>, Receiver<AsyncMessage>)> = OnceLock::new();

fn global_channel() -> &'static (Sender<AsyncMessage>, Receiver<AsyncMessage>) {
    GLOBAL_CHANNEL.get_or_init(unbounded)
}

/// Sender half of the global channel.  Never fails to exist.
pub fn global_sender() -> Sender<AsyncMessage> {
    global_channel().0.clone()
}

/// Receiver half of the global channel.  Drained by the UI node.
pub fn global_receiver() -> Receiver<AsyncMessage> {
    global_channel().1.clone()
}

/// The global channel as a [`Notifier`], for production spawn sites.
pub fn global_notifier() -> Arc<dyn Notifier> {
    Arc::new(global_sender())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_channel_round_trips_and_survives_requesting_the_receiver() {
        let rx1 = global_receiver();
        global_sender().notify(AsyncMessage::ConnectionFailure { error: "x".into() });
        // A "re-created" UI node asks for the receiver again and still sees
        // the message — the channel is process-wide, not per node.
        let rx2 = global_receiver();
        let got = rx2
            .try_recv()
            .or_else(|_| rx1.try_recv())
            .expect("message is in the global channel");
        assert_eq!(got, AsyncMessage::ConnectionFailure { error: "x".into() });
    }

    #[test]
    fn recording_notifier_keeps_order() {
        let n = RecordingNotifier::new();
        n.notify(AsyncMessage::ConnectionFailure { error: "a".into() });
        n.notify(AsyncMessage::ConnectionFailure { error: "b".into() });
        assert_eq!(n.len(), 2);
        assert!(matches!(
            &n.messages()[1],
            AsyncMessage::ConnectionFailure { error } if error == "b"
        ));
    }

    #[test]
    fn trend_as_i32() {
        assert_eq!(MetricTrend::Up.as_i32(), 1);
        assert_eq!(MetricTrend::Down.as_i32(), -1);
        assert_eq!(MetricTrend::Stable.as_i32(), 0);
    }
}
