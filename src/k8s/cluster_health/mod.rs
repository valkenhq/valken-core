//! Cluster and node health: capacity, pressure, scheduling and API latency.
//!
//! Every [`crate::k8s::KubeWatcher`] session runs one monitor that keeps a
//! [`ClusterHealth`] snapshot current and publishes it on a
//! [`tokio::sync::watch`] channel
//! ([`crate::k8s::KubeWatcher::subscribe_cluster_health`]).  Readers are UI
//! frames, so reading is an `Arc` clone; a new snapshot is a new `Arc`.
//!
//! Where each number comes from:
//!
//! | What | Source |
//! |---|---|
//! | Nodes, conditions, capacity | a Node reflector owned by the monitor |
//! | Requests, limits, pods per node | the watcher's Pod store (no second watch) |
//! | Live CPU / memory | `metrics.k8s.io` node metrics every 30 s |
//! | API latency and errors | [`ApiStatsLayer`] on the watcher's client |
//! | Unschedulable pods | the scheduler's `PodScheduled` condition |
//! | Sparklines | an in-memory ring, empty when the session starts |
//!
//! The pure parts (pod resource accounting, node translation, the summary,
//! the rings, the latency histogram) are separate from the one async task
//! so each can be tested on its own.

mod api_stats;
mod history;
mod monitor;
mod node;
mod resources;
mod snapshot;
mod summarize;
mod top_pods;

pub use api_stats::{ApiStats, ApiStatsLayer, ApiStatsService, ApiSummary};
pub use history::{History, Series, DEFAULT_HISTORY_LEN};
pub use monitor::ClusterHealthSettings;
pub use node::{
    Condition, ConditionKind, ConditionStatus, NodeHealth, NodeSystem, PodSlots, Pressure,
    Readiness, ResourceUse, Taint,
};
pub use resources::{pod_resources, PodResources, Resources};
pub use snapshot::{
    Basis, ClusterHealth, Headroom, Issue, Level, NodeAccess, NodeCounts, PodCounts, ResourceKind,
    Severity, Thresholds, Totals, UnschedulablePod, UsageStatus, MAX_UNSCHEDULABLE_LISTED,
};
pub use top_pods::{NodePods, PodUsageRow};

pub(crate) use monitor::{run_cluster_health, HealthPublisher, MonitorInputs};
pub(crate) use node::strip_node;
pub(crate) use top_pods::{pods_bound_to, top_pods_on_node};
