//! Per-pod CPU / memory / restart-count polling while a pod is selected.

use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::task::AbortHandle;
use tracing::{debug, warn};

use crate::async_runtime::{AsyncMessage, Notifier};
use crate::k8s::metrics::MetricsSource;

/// How often to poll the metrics-server and pod status for a specific pod.
pub const POD_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Spawn [`run_pod_metrics_monitor`] on `handle`.  Abort the returned handle
/// to stop it when the user selects a different resource.
pub fn spawn_pod_metrics_monitor(
    handle: &Handle,
    source: Arc<dyn MetricsSource>,
    notifier: Arc<dyn Notifier>,
    cluster_id: u32,
    pod_name: String,
    namespace: String,
    poll_interval: Duration,
) -> AbortHandle {
    handle
        .spawn(run_pod_metrics_monitor(
            source,
            notifier,
            cluster_id,
            pod_name,
            namespace,
            poll_interval,
        ))
        .abort_handle()
}

/// Convenience wrapper over [`spawn_pod_metrics_monitor`] for the Godot app:
/// uses the engine's Tokio runtime and the global notifier.  Main thread only.
#[cfg(feature = "godot")]
pub fn start_pod_metrics_monitoring(
    cluster_id: u32,
    pod_name: String,
    namespace: String,
    client: kube::Client,
) -> AbortHandle {
    use crate::async_runtime::{global_notifier, AsyncRuntime};
    use crate::k8s::metrics::KubeMetricsSource;
    spawn_pod_metrics_monitor(
        &AsyncRuntime::handle(),
        Arc::new(KubeMetricsSource::new(client)),
        global_notifier(),
        cluster_id,
        pod_name,
        namespace,
        POD_POLL_INTERVAL,
    )
}

/// Poll one pod forever (until the task is aborted), sending an
/// [`AsyncMessage::PodMetrics`] after each poll in which at least one of
/// the two reads succeeded.  A poll where both fail sends nothing rather
/// than a misleading all-zero sample.
pub async fn run_pod_metrics_monitor(
    source: Arc<dyn MetricsSource>,
    notifier: Arc<dyn Notifier>,
    cluster_id: u32,
    pod_name: String,
    namespace: String,
    poll_interval: Duration,
) {
    debug!(pod = %pod_name, namespace = %namespace, "PodMetricsMonitor: starting");
    loop {
        // The two reads are independent; issue them concurrently.
        let (usage, restarts) = tokio::join!(
            source.pod_usage(&namespace, &pod_name),
            source.pod_restarts(&namespace, &pod_name)
        );

        let usage = match usage {
            Ok(u) => Some(u),
            Err(e) => {
                debug!(pod = %pod_name, error = %e, "PodMetricsMonitor: metrics unavailable");
                None
            }
        };
        let restarts = match restarts {
            Ok(r) => Some(r),
            Err(e) => {
                warn!(pod = %pod_name, error = %e, "PodMetricsMonitor: could not read pod status");
                None
            }
        };

        if usage.is_some() || restarts.is_some() {
            let usage = usage.unwrap_or_default();
            notifier.notify(AsyncMessage::PodMetrics {
                cluster_id,
                pod_name: pod_name.clone(),
                namespace: namespace.clone(),
                cpu_millicores: usage.cpu_millicores,
                memory_mib: usage.memory_mib,
                restart_count: restarts.unwrap_or(0),
            });
        }

        tokio::time::sleep(poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_runtime::RecordingNotifier;
    use crate::k8s::metrics::{NodeCapacity, NodeUsage, PodUsage};
    use crate::k8s::MetricsError;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FakeSource {
        fail_everything: AtomicBool,
    }

    #[async_trait]
    impl MetricsSource for FakeSource {
        async fn node_capacity(&self) -> Result<NodeCapacity, MetricsError> {
            unimplemented!()
        }
        async fn node_usage(&self) -> Result<NodeUsage, MetricsError> {
            unimplemented!()
        }
        async fn pod_usage(&self, _: &str, _: &str) -> Result<PodUsage, MetricsError> {
            if self.fail_everything.load(Ordering::SeqCst) {
                Err(MetricsError::Quantity {
                    field: "x",
                    raw: "y".into(),
                })
            } else {
                Ok(PodUsage {
                    cpu_millicores: 250.0,
                    memory_mib: 64.0,
                })
            }
        }
        async fn pod_restarts(&self, _: &str, _: &str) -> Result<i32, MetricsError> {
            if self.fail_everything.load(Ordering::SeqCst) {
                Err(MetricsError::Quantity {
                    field: "x",
                    raw: "y".into(),
                })
            } else {
                Ok(3)
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn sends_sample_on_success_and_nothing_when_everything_fails() {
        let source = Arc::new(FakeSource {
            fail_everything: AtomicBool::new(false),
        });
        let notifier = Arc::new(RecordingNotifier::new());
        let handle = spawn_pod_metrics_monitor(
            &Handle::current(),
            source.clone(),
            notifier.clone(),
            4,
            "web-1".into(),
            "default".into(),
            Duration::from_secs(5),
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(notifier.len(), 1);
        assert_eq!(
            notifier.messages()[0],
            AsyncMessage::PodMetrics {
                cluster_id: 4,
                pod_name: "web-1".into(),
                namespace: "default".into(),
                cpu_millicores: 250.0,
                memory_mib: 64.0,
                restart_count: 3,
            }
        );

        source.fail_everything.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(11)).await;
        assert_eq!(notifier.len(), 1, "failed polls send nothing");
        handle.abort();
    }
}
