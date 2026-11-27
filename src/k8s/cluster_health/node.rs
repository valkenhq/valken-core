//! One Node object, reduced to what the health view shows.
//!
//! Pool, zone and instance type have no single standard label, so each is
//! read from the first of the labels the common distributions set.  The
//! conditions are kept as the kubelet reported them (with their last
//! transition) so the UI can say "NotReady for 6m".

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use k8s_openapi::api::core::v1::{Node, NodeCondition};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

use super::resources::Resources;
use crate::k8s::metrics::{cpu_millicores, memory_bytes};

/// Node-pool labels, most specific first.
const POOL_LABELS: &[&str] = &[
    "cloud.google.com/gke-nodepool",
    "eks.amazonaws.com/nodegroup",
    "karpenter.sh/nodepool",
    "kubernetes.azure.com/agentpool",
    "agentpool",
    "doks.digitalocean.com/node-pool",
    "node.kubernetes.io/pool",
    "node-pool",
];
const ZONE_LABELS: &[&str] = &[
    "topology.kubernetes.io/zone",
    "failure-domain.beta.kubernetes.io/zone",
];
const INSTANCE_TYPE_LABELS: &[&str] = &[
    "node.kubernetes.io/instance-type",
    "beta.kubernetes.io/instance-type",
];
const ROLE_PREFIX: &str = "node-role.kubernetes.io/";

/// `Ready` as the kubelet last reported it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Readiness {
    Ready,
    /// `Ready=False`: the kubelet says the node can't run pods.
    NotReady,
    /// `Ready=Unknown` or no condition: the kubelet stopped posting status
    /// (node down, network partition).  Treated like NotReady.
    #[default]
    Unknown,
}

impl Readiness {
    pub fn is_ready(self) -> bool {
        self == Readiness::Ready
    }
}

/// The node condition types the health view knows about.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConditionKind {
    Ready,
    MemoryPressure,
    DiskPressure,
    PidPressure,
    NetworkUnavailable,
    /// Anything else (node-problem-detector conditions such as
    /// `KernelDeadlock`), kept by name.
    Other(String),
}

impl ConditionKind {
    pub fn parse(s: &str) -> Self {
        match s {
            "Ready" => Self::Ready,
            "MemoryPressure" => Self::MemoryPressure,
            "DiskPressure" => Self::DiskPressure,
            "PIDPressure" => Self::PidPressure,
            "NetworkUnavailable" => Self::NetworkUnavailable,
            other => Self::Other(other.to_string()),
        }
    }

    /// The Kubernetes name (`PIDPressure`).
    pub fn as_str(&self) -> &str {
        match self {
            Self::Ready => "Ready",
            Self::MemoryPressure => "MemoryPressure",
            Self::DiskPressure => "DiskPressure",
            Self::PidPressure => "PIDPressure",
            Self::NetworkUnavailable => "NetworkUnavailable",
            Self::Other(s) => s,
        }
    }

    /// For every kind except `Ready`, `True` is the bad state.
    fn bad_when_true(&self) -> bool {
        !matches!(self, Self::Ready)
    }
}

/// A condition's `status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionStatus {
    True,
    False,
    Unknown,
}

impl ConditionStatus {
    fn parse(s: &str) -> Self {
        match s {
            "True" => Self::True,
            "False" => Self::False,
            _ => Self::Unknown,
        }
    }
}

/// One node condition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Condition {
    pub kind: ConditionKind,
    pub status: ConditionStatus,
    /// `lastTransitionTime`: how long it has been like this.
    pub since: Option<SystemTime>,
    pub reason: Option<String>,
    pub message: Option<String>,
}

impl Condition {
    /// `true` when this condition is in its bad state (pressure `True`,
    /// `Ready` not `True`).
    pub fn is_problem(&self) -> bool {
        if self.kind.bad_when_true() {
            self.status == ConditionStatus::True
        } else {
            self.status != ConditionStatus::True
        }
    }
}

/// A node pressure condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Pressure {
    Memory,
    Disk,
    Pid,
}

impl Pressure {
    pub fn as_str(self) -> &'static str {
        match self {
            Pressure::Memory => "MemoryPressure",
            Pressure::Disk => "DiskPressure",
            Pressure::Pid => "PIDPressure",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Taint {
    pub key: String,
    pub value: Option<String>,
    /// `NoSchedule`, `PreferNoSchedule` or `NoExecute`.
    pub effect: String,
}

impl std::fmt::Display for Taint {
    /// `kubectl` form: `key=value:Effect`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.value {
            Some(v) => write!(f, "{}={}:{}", self.key, v, self.effect),
            None => write!(f, "{}:{}", self.key, self.effect),
        }
    }
}

/// What the kubelet reports about the machine.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NodeSystem {
    pub kubelet_version: Option<String>,
    pub kernel_version: Option<String>,
    pub container_runtime: Option<String>,
    pub os_image: Option<String>,
    pub architecture: Option<String>,
}

/// One resource on one node: what exists, what the scheduler can hand out,
/// what is promised to pods and what is actually in use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResourceUse {
    pub capacity: i64,
    pub allocatable: i64,
    /// Effective requests of the pods bound here.
    pub requests: i64,
    /// Limits that are set, summed.
    pub limits: i64,
    /// From the metrics-server; `None` without one.
    pub usage: Option<i64>,
}

impl ResourceUse {
    /// `value / allocatable` in percent; `None` when nothing is allocatable.
    fn of_allocatable(&self, value: i64) -> Option<f32> {
        (self.allocatable > 0).then(|| (value as f64 * 100.0 / self.allocatable as f64) as f32)
    }

    pub fn usage_percent(&self) -> Option<f32> {
        self.usage.and_then(|u| self.of_allocatable(u))
    }

    pub fn requests_percent(&self) -> Option<f32> {
        self.of_allocatable(self.requests)
    }

    pub fn limits_percent(&self) -> Option<f32> {
        self.of_allocatable(self.limits)
    }

    /// What can still be requested before the scheduler says no.
    pub fn free_to_request(&self) -> i64 {
        (self.allocatable - self.requests).max(0)
    }

    pub(crate) fn add(&mut self, other: &ResourceUse) {
        self.capacity += other.capacity;
        self.allocatable += other.allocatable;
        self.requests += other.requests;
        self.limits += other.limits;
        self.usage = match (self.usage, other.usage) {
            (Some(a), Some(b)) => Some(a + b),
            (a, b) => a.or(b),
        };
    }
}

/// Pod slots on a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PodSlots {
    /// Pods bound here that still hold resources (not Succeeded / Failed).
    pub count: u32,
    pub allocatable: u32,
}

/// One node as the health view shows it.
#[derive(Debug, Clone, PartialEq, Default)]
#[non_exhaustive]
pub struct NodeHealth {
    pub name: String,
    pub pool: Option<String>,
    pub zone: Option<String>,
    pub instance_type: Option<String>,
    /// From `node-role.kubernetes.io/<role>` labels (`control-plane`).
    pub roles: Vec<String>,
    pub created: Option<SystemTime>,
    pub readiness: Readiness,
    /// When `Ready` last changed.
    pub ready_since: Option<SystemTime>,
    /// `spec.unschedulable`: cordoned.
    pub cordoned: bool,
    pub conditions: Vec<Condition>,
    pub taints: Vec<Taint>,
    pub system: NodeSystem,
    pub cpu: ResourceUse,
    pub memory: ResourceUse,
    pub pods: PodSlots,
}

impl NodeHealth {
    /// Active pressure conditions.
    pub fn pressures(&self) -> impl Iterator<Item = (Pressure, &Condition)> {
        self.conditions.iter().filter_map(|c| {
            let p = match c.kind {
                ConditionKind::MemoryPressure => Pressure::Memory,
                ConditionKind::DiskPressure => Pressure::Disk,
                ConditionKind::PidPressure => Pressure::Pid,
                _ => return None,
            };
            (c.status == ConditionStatus::True).then_some((p, c))
        })
    }

    pub fn condition(&self, kind: &ConditionKind) -> Option<&Condition> {
        self.conditions.iter().find(|c| &c.kind == kind)
    }

    /// Can new pods land here (ignoring taints, which depend on the pod)?
    pub fn is_schedulable(&self) -> bool {
        self.readiness.is_ready() && !self.cordoned
    }

    /// Sort key that puts problems first: not ready, then pressure or other
    /// bad conditions, then cordoned, then healthy; by name within a rank.
    pub fn problem_rank(&self) -> u8 {
        if !self.readiness.is_ready() {
            0
        } else if self.conditions.iter().any(Condition::is_problem) {
            1
        } else if self.cordoned {
            2
        } else {
            3
        }
    }

    /// Requests and usage as [`Resources`].
    pub fn requests(&self) -> Resources {
        Resources::new(self.cpu.requests, self.memory.requests)
    }

    pub fn allocatable(&self) -> Resources {
        Resources::new(self.cpu.allocatable, self.memory.allocatable)
    }
}

type ResourceList =
    std::collections::BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>;

fn cpu_in(list: Option<&ResourceList>) -> i64 {
    list.and_then(|l| l.get("cpu"))
        .and_then(|q| cpu_millicores(q, "node.status.cpu").ok())
        .unwrap_or(0)
}

fn memory_in(list: Option<&ResourceList>) -> i64 {
    list.and_then(|l| l.get("memory"))
        .and_then(|q| memory_bytes(q, "node.status.memory").ok())
        .unwrap_or(0)
}

fn pods_in(list: Option<&ResourceList>) -> u32 {
    list.and_then(|l| l.get("pods"))
        .and_then(|q| q.0.parse().ok())
        .unwrap_or(0)
}

pub(crate) fn system_time(t: &Time) -> Option<SystemTime> {
    let secs = u64::try_from(t.0.as_second()).ok()?;
    Some(UNIX_EPOCH + Duration::from_secs(secs))
}

fn first_label(
    labels: Option<&std::collections::BTreeMap<String, String>>,
    keys: &[&str],
) -> Option<String> {
    let labels = labels?;
    keys.iter()
        .find_map(|k| labels.get(*k))
        .filter(|v| !v.is_empty())
        .cloned()
}

fn condition(c: &NodeCondition) -> Condition {
    Condition {
        kind: ConditionKind::parse(&c.type_),
        status: ConditionStatus::parse(&c.status),
        since: c.last_transition_time.as_ref().and_then(system_time),
        reason: c.reason.clone().filter(|s| !s.is_empty()),
        message: c.message.clone().filter(|s| !s.is_empty()),
    }
}

/// Translate a Node.  Requests, limits, usage and pod counts are filled in
/// later by the aggregation, which sees the pods and the metrics.
pub fn node_health(node: &Node) -> NodeHealth {
    let labels = node.metadata.labels.as_ref();
    let spec = node.spec.as_ref();
    let status = node.status.as_ref();
    let info = status.and_then(|s| s.node_info.as_ref());

    let conditions: Vec<Condition> = status
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| cs.iter().map(condition).collect())
        .unwrap_or_default();
    let ready = conditions.iter().find(|c| c.kind == ConditionKind::Ready);
    let readiness = match ready.map(|c| c.status) {
        Some(ConditionStatus::True) => Readiness::Ready,
        Some(ConditionStatus::False) => Readiness::NotReady,
        Some(ConditionStatus::Unknown) | None => Readiness::Unknown,
    };

    let mut roles: Vec<String> = labels
        .into_iter()
        .flatten()
        .filter_map(|(k, _)| k.strip_prefix(ROLE_PREFIX))
        .filter(|r| !r.is_empty())
        .map(str::to_string)
        .collect();
    roles.sort();

    let capacity = status.and_then(|s| s.capacity.as_ref());
    let allocatable = status.and_then(|s| s.allocatable.as_ref());

    NodeHealth {
        name: node.metadata.name.clone().unwrap_or_default(),
        pool: first_label(labels, POOL_LABELS),
        zone: first_label(labels, ZONE_LABELS),
        instance_type: first_label(labels, INSTANCE_TYPE_LABELS),
        roles,
        created: node
            .metadata
            .creation_timestamp
            .as_ref()
            .and_then(system_time),
        readiness,
        ready_since: ready.and_then(|c| c.since),
        cordoned: spec.and_then(|s| s.unschedulable).unwrap_or(false),
        taints: spec
            .and_then(|s| s.taints.as_ref())
            .map(|ts| {
                ts.iter()
                    .map(|t| Taint {
                        key: t.key.clone(),
                        value: t.value.clone().filter(|v| !v.is_empty()),
                        effect: t.effect.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        system: NodeSystem {
            kubelet_version: info.map(|i| i.kubelet_version.clone()),
            kernel_version: info.map(|i| i.kernel_version.clone()),
            container_runtime: info.map(|i| i.container_runtime_version.clone()),
            os_image: info.map(|i| i.os_image.clone()),
            architecture: info.map(|i| i.architecture.clone()),
        },
        cpu: ResourceUse {
            capacity: cpu_in(capacity),
            allocatable: cpu_in(allocatable),
            ..Default::default()
        },
        memory: ResourceUse {
            capacity: memory_in(capacity),
            allocatable: memory_in(allocatable),
            ..Default::default()
        },
        pods: PodSlots {
            count: 0,
            allocatable: pods_in(allocatable),
        },
        conditions,
    }
}

/// Drop what the health view never reads before a Node is stored:
/// `status.images` lists every image cached on the node (often dozens of
/// entries with all their tags) and is most of a Node's size.
pub(crate) fn strip_node(node: &mut Node) {
    use kube::ResourceExt;
    node.managed_fields_mut().clear();
    if let Some(status) = node.status.as_mut() {
        status.images = None;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::k8s::cluster_health::resources::tests::list;
    use k8s_openapi::api::core::v1::{
        ContainerImage, NodeSpec, NodeStatus, NodeSystemInfo, Taint as KTaint,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;

    pub(crate) fn ts(secs: i64) -> Time {
        Time(k8s_openapi::jiff::Timestamp::from_second(secs).unwrap())
    }

    pub(crate) fn cond(type_: &str, status: &str, at: i64) -> NodeCondition {
        NodeCondition {
            type_: type_.into(),
            status: status.into(),
            last_transition_time: Some(ts(at)),
            reason: Some(format!("{type_}{status}")),
            message: Some(format!("{type_} is {status}")),
            ..Default::default()
        }
    }

    /// A Ready node with 4 cores, 16 GiB and 110 pods allocatable.
    pub(crate) fn node(name: &str, labels: &[(&str, &str)]) -> Node {
        Node {
            metadata: ObjectMeta {
                name: Some(name.into()),
                labels: Some(
                    labels
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect::<BTreeMap<_, _>>(),
                ),
                creation_timestamp: Some(ts(1_000)),
                ..Default::default()
            },
            spec: Some(NodeSpec::default()),
            status: Some(NodeStatus {
                capacity: Some(list(&[("cpu", "4"), ("memory", "16Gi"), ("pods", "110")])),
                allocatable: Some(list(&[
                    ("cpu", "3900m"),
                    ("memory", "15Gi"),
                    ("pods", "110"),
                ])),
                conditions: Some(vec![
                    cond("Ready", "True", 2_000),
                    cond("MemoryPressure", "False", 2_000),
                ]),
                node_info: Some(NodeSystemInfo {
                    kubelet_version: "v1.31.4".into(),
                    kernel_version: "6.8.0".into(),
                    container_runtime_version: "containerd://1.7.22".into(),
                    architecture: "amd64".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        }
    }

    pub(crate) fn set_condition(n: &mut Node, type_: &str, status: &str, at: i64) {
        let cs = n
            .status
            .as_mut()
            .unwrap()
            .conditions
            .get_or_insert_with(Vec::new);
        cs.retain(|c| c.type_ != type_);
        cs.push(cond(type_, status, at));
    }

    #[test]
    fn reads_identity_capacity_and_system() {
        let n = node_health(&node(
            "pool-a-1",
            &[
                ("cloud.google.com/gke-nodepool", "pool-a"),
                ("topology.kubernetes.io/zone", "eu-1a"),
                ("node.kubernetes.io/instance-type", "c6a.2xlarge"),
                ("node-role.kubernetes.io/worker", ""),
            ],
        ));
        assert_eq!(n.name, "pool-a-1");
        assert_eq!(n.pool.as_deref(), Some("pool-a"));
        assert_eq!(n.zone.as_deref(), Some("eu-1a"));
        assert_eq!(n.instance_type.as_deref(), Some("c6a.2xlarge"));
        assert_eq!(n.roles, vec!["worker"]);
        assert_eq!(n.readiness, Readiness::Ready);
        assert_eq!(n.ready_since, Some(UNIX_EPOCH + Duration::from_secs(2_000)));
        assert_eq!(n.cpu.capacity, 4000);
        assert_eq!(n.cpu.allocatable, 3900);
        assert_eq!(n.memory.allocatable, 15 << 30);
        assert_eq!(n.pods.allocatable, 110);
        assert_eq!(n.system.kubelet_version.as_deref(), Some("v1.31.4"));
        assert_eq!(n.problem_rank(), 3);
        assert!(n.is_schedulable());
    }

    #[test]
    fn labels_fall_back_and_empty_values_are_ignored() {
        let n = node_health(&node(
            "n",
            &[
                ("node.kubernetes.io/pool", ""),
                ("failure-domain.beta.kubernetes.io/zone", "old-zone"),
            ],
        ));
        assert_eq!(n.pool, None, "an empty label value is no pool");
        assert_eq!(n.zone.as_deref(), Some("old-zone"));
        assert_eq!(n.instance_type, None);
    }

    #[test]
    fn readiness_from_the_ready_condition() {
        let mut raw = node("n", &[]);
        set_condition(&mut raw, "Ready", "False", 5_000);
        let n = node_health(&raw);
        assert_eq!(n.readiness, Readiness::NotReady);
        assert_eq!(n.problem_rank(), 0);
        assert!(!n.is_schedulable());

        set_condition(&mut raw, "Ready", "Unknown", 6_000);
        let n = node_health(&raw);
        assert_eq!(n.readiness, Readiness::Unknown);
        assert_eq!(n.ready_since, Some(UNIX_EPOCH + Duration::from_secs(6_000)));

        raw.status.as_mut().unwrap().conditions = None;
        assert_eq!(node_health(&raw).readiness, Readiness::Unknown);
    }

    #[test]
    fn pressure_and_cordon_rank_and_report() {
        let mut raw = node("n", &[]);
        set_condition(&mut raw, "MemoryPressure", "True", 7_000);
        set_condition(&mut raw, "KernelDeadlock", "False", 7_000);
        let n = node_health(&raw);
        let pressures: Vec<_> = n.pressures().map(|(p, _)| p).collect();
        assert_eq!(pressures, vec![Pressure::Memory]);
        assert_eq!(n.problem_rank(), 1);
        assert!(n
            .condition(&ConditionKind::Other("KernelDeadlock".into()))
            .is_some_and(|c| !c.is_problem()));

        let mut raw = node("n", &[]);
        raw.spec.as_mut().unwrap().unschedulable = Some(true);
        let n = node_health(&raw);
        assert!(n.cordoned);
        assert_eq!(n.problem_rank(), 2);
        assert!(!n.is_schedulable());
    }

    #[test]
    fn taints_print_like_kubectl() {
        let mut raw = node("n", &[]);
        raw.spec.as_mut().unwrap().taints = Some(vec![
            KTaint {
                key: "dedicated".into(),
                value: Some("web".into()),
                effect: "PreferNoSchedule".into(),
                ..Default::default()
            },
            KTaint {
                key: "node.kubernetes.io/unreachable".into(),
                value: None,
                effect: "NoExecute".into(),
                ..Default::default()
            },
        ]);
        let n = node_health(&raw);
        let printed: Vec<String> = n.taints.iter().map(ToString::to_string).collect();
        assert_eq!(
            printed,
            vec![
                "dedicated=web:PreferNoSchedule",
                "node.kubernetes.io/unreachable:NoExecute"
            ]
        );
    }

    #[test]
    fn condition_kinds_round_trip() {
        for s in [
            "Ready",
            "MemoryPressure",
            "DiskPressure",
            "PIDPressure",
            "NetworkUnavailable",
            "FrequentKubeletRestart",
        ] {
            assert_eq!(ConditionKind::parse(s).as_str(), s);
        }
    }

    #[test]
    fn strip_drops_images_and_managed_fields() {
        let mut raw = node("n", &[]);
        raw.status.as_mut().unwrap().images = Some(vec![ContainerImage {
            names: Some(vec!["nginx:1".into()]),
            size_bytes: Some(1),
        }]);
        raw.metadata.managed_fields = Some(vec![Default::default()]);
        strip_node(&mut raw);
        assert!(raw.status.as_ref().unwrap().images.is_none());
        assert!(raw
            .metadata
            .managed_fields
            .as_ref()
            .is_none_or(|m| m.is_empty()));
        // Everything the view reads survives.
        assert_eq!(node_health(&raw).cpu.allocatable, 3900);
    }

    #[test]
    fn resource_use_percentages() {
        let r = ResourceUse {
            capacity: 4000,
            allocatable: 2000,
            requests: 1000,
            limits: 3000,
            usage: Some(500),
        };
        assert_eq!(r.usage_percent(), Some(25.0));
        assert_eq!(r.requests_percent(), Some(50.0));
        assert_eq!(r.limits_percent(), Some(150.0));
        assert_eq!(r.free_to_request(), 1000);
        let empty = ResourceUse::default();
        assert_eq!(empty.requests_percent(), None);
        assert_eq!(empty.usage_percent(), None);
    }
}
