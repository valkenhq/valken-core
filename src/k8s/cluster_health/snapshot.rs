//! [`ClusterHealth`]: one immutable snapshot of a cluster's health.

use std::time::SystemTime;

use super::api_stats::ApiSummary;
use super::history::History;
use super::node::{NodeHealth, Pressure, ResourceUse};
use super::resources::Resources;

/// Whether node objects can be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeAccess {
    /// The node list has not arrived yet.
    #[default]
    Pending,
    Visible,
    /// RBAC forbids listing nodes (common with namespace-scoped access).
    /// Pod-derived numbers are still there; capacity and conditions aren't.
    Forbidden,
    /// The [`crate::k8s::ClusterSource`] has no node watch at all.
    Unsupported,
}

/// Where live CPU / memory usage comes from, and whether it is there.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum UsageStatus {
    /// No answer from the metrics API yet.
    #[default]
    Pending,
    /// `metrics.k8s.io` answered; `sampled` is when.
    MetricsServer { sampled: SystemTime },
    /// The `metrics.k8s.io` API is not registered: no metrics-server.
    NotInstalled,
    /// Registered but not answering (metrics-server down or starting).
    Unavailable,
    /// RBAC forbids reading node metrics.
    Forbidden,
    /// Some other failure; the text is safe to display.
    Failed(String),
}

impl UsageStatus {
    pub fn is_available(&self) -> bool {
        matches!(self, UsageStatus::MetricsServer { .. })
    }
}

/// What a cluster percentage is measured from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Basis {
    /// Live usage from the metrics-server.
    Usage,
    /// Summed pod requests: there is no usage to show.
    #[default]
    Requests,
}

/// The two resources a threshold or issue can be about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceKind {
    Cpu,
    Memory,
}

/// Node counts for the pill ("5/6 nodes · 1 cordoned").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NodeCounts {
    pub total: u32,
    pub ready: u32,
    pub cordoned: u32,
    /// Ready nodes with at least one pressure condition.
    pub under_pressure: u32,
}

/// Cluster-wide pod counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PodCounts {
    pub running: u32,
    pub pending: u32,
    /// Pending pods the scheduler said it cannot place.
    pub unschedulable: u32,
    /// Pods with a container in `CrashLoopBackOff`.
    pub crash_looping: u32,
    /// Pod slots on Ready nodes.
    pub capacity: u32,
}

/// Cluster totals.
///
/// Capacity, requests and usage cover **Ready** nodes only: a node that
/// can't run pods offers no capacity, and counting it would make the
/// cluster look emptier exactly when it is under strain.  When nodes are
/// not visible, requests are the sum over every bound pod and there is
/// nothing to take a percentage of.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Totals {
    pub cpu: ResourceUse,
    pub memory: ResourceUse,
    /// Usage when the metrics-server answered, else requests.
    pub basis: Basis,
    pub nodes: NodeCounts,
    pub pods: PodCounts,
}

impl Totals {
    /// Percent of allocatable on the current [`Basis`].
    pub fn percent(&self, kind: ResourceKind) -> Option<f32> {
        let r = match kind {
            ResourceKind::Cpu => &self.cpu,
            ResourceKind::Memory => &self.memory,
        };
        match self.basis {
            Basis::Usage => r.usage_percent(),
            Basis::Requests => r.requests_percent(),
        }
    }
}

/// How much more can be scheduled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Headroom {
    /// Allocatable minus requests, summed over schedulable nodes (Ready,
    /// not cordoned).
    pub free: Resources,
    /// The most free on any one schedulable node, per resource: the
    /// largest request that still fits somewhere.  A pod can't be split, so
    /// this is the number that predicts the next `Insufficient memory`.
    pub largest_fit: Resources,
}

/// A pending pod the scheduler could not place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnschedulablePod {
    pub namespace: String,
    pub name: String,
    /// The scheduler's message (`0/5 nodes are available: …`).
    pub reason: String,
}

/// How bad something is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Severity {
    Warning,
    Critical,
}

/// The overall verdict, the pill's colour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Level {
    #[default]
    Healthy,
    Warning,
    Critical,
}

impl From<Severity> for Level {
    fn from(s: Severity) -> Self {
        match s {
            Severity::Warning => Level::Warning,
            Severity::Critical => Level::Critical,
        }
    }
}

/// One thing that needs attention, with what the UI needs to explain it.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Issue {
    /// `Ready` is `False`, or `Unknown` (the kubelet stopped posting).
    NodeNotReady {
        node: String,
        since: Option<SystemTime>,
        /// `Ready=Unknown`: no word from the kubelet at all.
        unknown: bool,
        reason: Option<String>,
        message: Option<String>,
        /// Pods bound to the node.
        pods: u32,
    },
    NodePressure {
        node: String,
        pressure: Pressure,
        since: Option<SystemTime>,
    },
    NodeNetworkUnavailable {
        node: String,
        since: Option<SystemTime>,
    },
    PodsUnschedulable {
        count: u32,
        /// The most common scheduler message.
        reason: Option<String>,
    },
    /// Cluster-wide CPU or memory at or above its threshold.
    HighUtilisation {
        resource: ResourceKind,
        percent: f32,
        basis: Basis,
    },
}

impl Issue {
    pub fn severity(&self) -> Severity {
        match self {
            Issue::NodeNotReady { .. }
            | Issue::NodeNetworkUnavailable { .. }
            | Issue::PodsUnschedulable { .. } => Severity::Critical,
            Issue::NodePressure { .. } | Issue::HighUtilisation { .. } => Severity::Warning,
        }
    }

    /// The node this is about, if it is about one.
    pub fn node(&self) -> Option<&str> {
        match self {
            Issue::NodeNotReady { node, .. }
            | Issue::NodePressure { node, .. }
            | Issue::NodeNetworkUnavailable { node, .. } => Some(node),
            Issue::PodsUnschedulable { .. } | Issue::HighUtilisation { .. } => None,
        }
    }

    /// Display order within a severity.
    pub(crate) fn order(&self) -> u8 {
        match self {
            Issue::NodeNotReady { .. } => 0,
            Issue::NodeNetworkUnavailable { .. } => 1,
            Issue::PodsUnschedulable { .. } => 2,
            Issue::NodePressure { .. } => 3,
            Issue::HighUtilisation { .. } => 4,
        }
    }
}

/// When cluster-wide utilisation becomes a warning.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Thresholds {
    pub memory_warn_percent: f32,
    pub cpu_warn_percent: f32,
}

impl Default for Thresholds {
    /// Memory at 85 %: past that, evictions and OOM kills get likely.
    /// CPU at 90 %: CPU is compressible (throttling, not eviction), so it
    /// gets more room before it is worth a warning.
    fn default() -> Self {
        Self {
            memory_warn_percent: 85.0,
            cpu_warn_percent: 90.0,
        }
    }
}

/// Unschedulable pods listed by name, at most.  The count is always exact.
pub const MAX_UNSCHEDULABLE_LISTED: usize = 100;

/// One cluster's health, as of [`ClusterHealth::updated`].
///
/// Published as an `Arc` on a watch channel
/// ([`crate::k8s::KubeWatcher::subscribe_cluster_health`]); a new snapshot
/// replaces the old one, so `Arc::ptr_eq` tells a reader nothing changed.
#[derive(Debug, Clone, PartialEq, Default)]
#[non_exhaustive]
pub struct ClusterHealth {
    /// Every node, by name.
    pub nodes: Vec<NodeHealth>,
    pub node_access: NodeAccess,
    pub usage: UsageStatus,
    pub totals: Totals,
    pub headroom: Headroom,
    /// Up to [`MAX_UNSCHEDULABLE_LISTED`], by namespace and name.
    pub unschedulable: Vec<UnschedulablePod>,
    /// Most severe first.
    pub issues: Vec<Issue>,
    pub level: Level,
    pub api: ApiSummary,
    pub history: History,
    /// When the session (and so the history) started.
    pub since: Option<SystemTime>,
    /// When this snapshot was computed; `None` before the first one.
    pub updated: Option<SystemTime>,
}

impl ClusterHealth {
    pub fn node(&self, name: &str) -> Option<&NodeHealth> {
        self.nodes
            .binary_search_by(|n| n.name.as_str().cmp(name))
            .ok()
            .map(|i| &self.nodes[i])
    }

    /// Issues about one node.
    pub fn issues_for<'a>(&'a self, node: &'a str) -> impl Iterator<Item = &'a Issue> + 'a {
        self.issues.iter().filter(move |i| i.node() == Some(node))
    }
}
