//! Cluster-wide and per-pod resource metrics, read through the
//! [`MetricsSource`] seam and reported via [`crate::async_runtime::Notifier`].

mod cluster_metrics_monitor;
mod pod_metrics_monitor;
mod source;

pub use cluster_metrics_monitor::{
    run_cluster_metrics_monitor, spawn_cluster_metrics_monitor, ClusterMetricsMonitor,
    ClusterMetricsSettings, MetricsTrendState,
};
#[cfg(feature = "godot")]
pub use pod_metrics_monitor::start_pod_metrics_monitoring;
pub use pod_metrics_monitor::{
    run_pod_metrics_monitor, spawn_pod_metrics_monitor, POD_POLL_INTERVAL,
};
pub use source::{KubeMetricsSource, MetricsSource, NodeCapacity, NodeUsage, PodUsage};
