//! Godot node that turns [`AsyncMessage`]s into signals on the main thread.

use godot::classes::{INode, Node, Timer};
use godot::prelude::*;
use tracing::debug;

use super::message::{global_receiver, AsyncMessage};

/// A Godot node that receives messages from async tasks and processes them
/// on the main thread.  Messages are drained from the global channel on a
/// repeating timer rather than every `process` frame.
///
/// The channel itself lives in [`super::message`], so a notifier that is
/// freed and re-created (scene change) resumes draining the same channel.
#[derive(GodotClass)]
#[class(base=Node)]
pub struct AsyncNotifier {
    base: Base<Node>,
    /// Repeating timer that drives channel draining at a fixed interval.
    drain_timer: Option<Gd<Timer>>,
}

impl AsyncNotifier {
    /// Sender for the global channel.  Kept for callers that predate
    /// [`super::global_sender`]; prefer passing a [`super::Notifier`].
    #[deprecated(note = "use async_runtime::global_sender(), or pass a Notifier")]
    pub fn get_sender() -> Option<crossbeam_channel::Sender<AsyncMessage>> {
        Some(super::message::global_sender())
    }
}

#[godot_api]
impl INode for AsyncNotifier {
    // Written by hand: `#[class(init)]` generates `base: base`, which clippy
    // flags as `redundant_field_names` against this file.
    fn init(base: Base<Node>) -> Self {
        Self {
            base,
            drain_timer: None,
        }
    }

    fn ready(&mut self) {
        debug!("AsyncNotifier ready()");
        self.setup_drain_timer();
    }
}

// ---------------------------------------------------------------------------
// Godot API — signals and #[func] callbacks
// ---------------------------------------------------------------------------

#[godot_api]
impl AsyncNotifier {
    /// Emitted when a connection succeeds
    #[signal]
    fn connection_success(
        cluster_name: GString,
        namespace: GString,
        cluster_uuid: GString,
        cluster_id: i64,
    );

    /// Emitted when a connection fails
    #[signal]
    fn connection_failure(error: GString);

    /// Emitted when a cluster's metrics are updated.
    /// cpu_trend and memory_trend: 1=up, -1=down, 0=stable
    #[signal]
    fn cluster_metrics(
        cluster_id: i64,
        cpu_percent: f32,
        memory_percent: f32,
        cpu_trend: i32,
        memory_trend: i32,
    );

    /// Emitted when pod-level metrics arrive for a specific pod.
    #[signal]
    fn pod_metrics(
        cluster_id: i64,
        pod_name: GString,
        namespace: GString,
        cpu_millicores: f32,
        memory_mib: f32,
        restart_count: i32,
    );

    /// Drains all pending messages from the global channel and emits the
    /// corresponding Godot signals.  Called by the repeating `drain_timer`.
    #[func]
    fn drain_async_channel(&mut self) {
        let receiver = global_receiver();
        let messages: Vec<AsyncMessage> = receiver.try_iter().collect();
        if messages.is_empty() {
            return;
        }
        debug!(count = messages.len(), "AsyncNotifier: emitting signals");
        for message in messages {
            self.emit(message);
        }
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

impl AsyncNotifier {
    /// How often the async message channel is drained (seconds).
    const DRAIN_INTERVAL_SECS: f64 = 0.5;

    fn emit(&mut self, message: AsyncMessage) {
        match message {
            AsyncMessage::ConnectionSuccess {
                cluster_name,
                namespace,
                cluster_uuid,
                cluster_id,
            } => {
                self.signals().connection_success().emit(
                    &GString::from(&cluster_name),
                    &GString::from(&namespace),
                    &GString::from(&cluster_uuid),
                    i64::from(cluster_id),
                );
            }
            AsyncMessage::ConnectionFailure { error } => {
                self.signals()
                    .connection_failure()
                    .emit(&GString::from(&error));
            }
            AsyncMessage::PodMetrics {
                cluster_id,
                pod_name,
                namespace,
                cpu_millicores,
                memory_mib,
                restart_count,
            } => {
                self.signals().pod_metrics().emit(
                    i64::from(cluster_id),
                    &GString::from(&pod_name),
                    &GString::from(&namespace),
                    cpu_millicores,
                    memory_mib,
                    restart_count,
                );
            }
            AsyncMessage::ClusterMetrics {
                cluster_id,
                cpu_percent,
                memory_percent,
                cpu_trend,
                memory_trend,
            } => {
                self.signals().cluster_metrics().emit(
                    i64::from(cluster_id),
                    cpu_percent,
                    memory_percent,
                    cpu_trend.as_i32(),
                    memory_trend.as_i32(),
                );
            }
        }
    }

    fn setup_drain_timer(&mut self) {
        let mut timer = Timer::new_alloc();
        timer.set_wait_time(Self::DRAIN_INTERVAL_SECS);
        timer.set_one_shot(false);
        timer.connect("timeout", &self.base().callable("drain_async_channel"));
        self.base_mut().add_child(&timer.clone().upcast::<Node>());
        timer.start();
        self.drain_timer = Some(timer);
        debug!(
            "AsyncNotifier: drain timer started ({} s interval)",
            Self::DRAIN_INTERVAL_SECS,
        );
    }
}
