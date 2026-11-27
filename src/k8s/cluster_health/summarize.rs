//! Nodes + pods + usage → a [`ClusterHealth`] (pure).
//!
//! Called on every coalesced change, so it is one pass over the pods and
//! one over the nodes, with a name → index map in between and no per-pod
//! allocation beyond the unschedulable list.

use std::collections::HashMap;
use std::sync::Arc;

use k8s_openapi::api::core::v1::{Node, Pod};

use super::api_stats::ApiSummary;
use super::node::{node_health, ConditionKind, ConditionStatus, NodeHealth, Readiness};
use super::resources::{
    bound_node, is_crash_looping, phase, pod_resources, unschedulable_reason, Phase, Resources,
};
use super::snapshot::{
    Basis, ClusterHealth, Headroom, Issue, Level, NodeAccess, PodCounts, ResourceKind, Thresholds,
    Totals, UnschedulablePod, UsageStatus, MAX_UNSCHEDULABLE_LISTED,
};

/// Live usage per node name, from the metrics-server.
pub(crate) type NodeUsageMap = HashMap<String, Resources>;

/// Everything one summary is computed from.
pub(crate) struct Inputs<'a> {
    pub nodes: &'a [Arc<Node>],
    pub node_access: NodeAccess,
    pub pods: &'a [Arc<Pod>],
    pub usage: &'a NodeUsageMap,
    pub usage_status: &'a UsageStatus,
    pub api: ApiSummary,
    pub thresholds: Thresholds,
}

/// Compute the snapshot.  `history`, `since` and `updated` are left for
/// the caller, which owns the clock and the rings.
pub(crate) fn summarize(input: &Inputs<'_>) -> ClusterHealth {
    let mut nodes: Vec<NodeHealth> = input.nodes.iter().map(|n| node_health(n)).collect();
    nodes.sort_by(|a, b| a.name.cmp(&b.name));
    let index: HashMap<&str, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.name.as_str(), i))
        .collect();

    // One pass over the pods: per-node requests / limits / counts and the
    // cluster-wide pod numbers.
    let mut per_node = vec![(Resources::ZERO, Resources::ZERO, 0u32); nodes.len()];
    let mut unbound_requests = Resources::ZERO;
    let mut unbound_limits = Resources::ZERO;
    let mut pods = PodCounts::default();
    let mut unschedulable: Vec<UnschedulablePod> = Vec::new();
    let mut reasons: HashMap<&str, u32> = HashMap::new();

    for pod in input.pods {
        match phase(pod) {
            Phase::Running => pods.running += 1,
            Phase::Pending => pods.pending += 1,
            _ => {}
        }
        if is_crash_looping(pod) {
            pods.crash_looping += 1;
        }
        if let Some(reason) = unschedulable_reason(pod) {
            pods.unschedulable += 1;
            *reasons.entry(reason).or_default() += 1;
            if unschedulable.len() < MAX_UNSCHEDULABLE_LISTED {
                unschedulable.push(UnschedulablePod {
                    namespace: pod.metadata.namespace.clone().unwrap_or_default(),
                    name: pod.metadata.name.clone().unwrap_or_default(),
                    reason: reason.to_string(),
                });
            }
        }
        let Some(node) = bound_node(pod) else {
            continue;
        };
        let r = pod_resources(pod);
        match index.get(node) {
            Some(&i) => {
                let slot = &mut per_node[i];
                slot.0 += r.requests;
                slot.1 += r.limits;
                slot.2 += 1;
            }
            None => {
                unbound_requests += r.requests;
                unbound_limits += r.limits;
            }
        }
    }
    unschedulable.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));

    // Fill the nodes in and total the Ready ones.
    let metrics_up = input.usage_status.is_available();
    let mut totals = Totals {
        basis: if metrics_up {
            Basis::Usage
        } else {
            Basis::Requests
        },
        ..Default::default()
    };
    let mut headroom = Headroom::default();
    for (node, (requests, limits, count)) in nodes.iter_mut().zip(per_node) {
        node.cpu.requests = requests.cpu_millicores;
        node.cpu.limits = limits.cpu_millicores;
        node.memory.requests = requests.memory_bytes;
        node.memory.limits = limits.memory_bytes;
        node.pods.count = count;
        if metrics_up {
            if let Some(u) = input.usage.get(&node.name) {
                node.cpu.usage = Some(u.cpu_millicores);
                node.memory.usage = Some(u.memory_bytes);
            }
        }

        let n = &mut totals.nodes;
        n.total += 1;
        if node.cordoned {
            n.cordoned += 1;
        }
        if node.readiness.is_ready() {
            n.ready += 1;
            if node.pressures().next().is_some() {
                n.under_pressure += 1;
            }
            totals.cpu.add(&node.cpu);
            totals.memory.add(&node.memory);
            totals.pods.capacity += node.pods.allocatable;
        }
        if node.is_schedulable() {
            let free = node.allocatable().saturating_sub(node.requests());
            headroom.free += free;
            headroom.largest_fit = headroom.largest_fit.max(free);
        }
    }
    if input.node_access != NodeAccess::Visible {
        // Nothing to divide by, but the promised resources are still real.
        totals.cpu.requests += unbound_requests.cpu_millicores;
        totals.cpu.limits += unbound_limits.cpu_millicores;
        totals.memory.requests += unbound_requests.memory_bytes;
        totals.memory.limits += unbound_limits.memory_bytes;
    }
    totals.pods = PodCounts {
        capacity: totals.pods.capacity,
        ..pods
    };

    let issues = issues(&nodes, &totals, &reasons, input.thresholds);
    let level = issues
        .iter()
        .map(|i| Level::from(i.severity()))
        .max()
        .unwrap_or(Level::Healthy);

    ClusterHealth {
        nodes,
        node_access: input.node_access,
        usage: input.usage_status.clone(),
        totals,
        headroom,
        unschedulable,
        issues,
        level,
        api: input.api,
        ..Default::default()
    }
}

fn issues(
    nodes: &[NodeHealth],
    totals: &Totals,
    reasons: &HashMap<&str, u32>,
    thresholds: Thresholds,
) -> Vec<Issue> {
    let mut out = Vec::new();
    for n in nodes {
        if !n.readiness.is_ready() {
            let ready = n.condition(&ConditionKind::Ready);
            out.push(Issue::NodeNotReady {
                node: n.name.clone(),
                since: n.ready_since,
                unknown: n.readiness == Readiness::Unknown,
                reason: ready.and_then(|c| c.reason.clone()),
                message: ready.and_then(|c| c.message.clone()),
                pods: n.pods.count,
            });
            // A node that is down reports stale pressure; one issue is enough.
            continue;
        }
        for (pressure, c) in n.pressures() {
            out.push(Issue::NodePressure {
                node: n.name.clone(),
                pressure,
                since: c.since,
            });
        }
        if let Some(c) = n
            .condition(&ConditionKind::NetworkUnavailable)
            .filter(|c| c.status == ConditionStatus::True)
        {
            out.push(Issue::NodeNetworkUnavailable {
                node: n.name.clone(),
                since: c.since,
            });
        }
    }

    if totals.pods.unschedulable > 0 {
        // Most frequent message; ties broken by text so output is stable.
        let reason = reasons
            .iter()
            .max_by(|a, b| a.1.cmp(b.1).then_with(|| b.0.cmp(a.0)))
            .map(|(r, _)| r.to_string())
            .filter(|r| !r.is_empty());
        out.push(Issue::PodsUnschedulable {
            count: totals.pods.unschedulable,
            reason,
        });
    }

    for (resource, limit) in [
        (ResourceKind::Memory, thresholds.memory_warn_percent),
        (ResourceKind::Cpu, thresholds.cpu_warn_percent),
    ] {
        if let Some(percent) = totals.percent(resource).filter(|p| *p >= limit) {
            out.push(Issue::HighUtilisation {
                resource,
                percent,
                basis: totals.basis,
            });
        }
    }

    out.sort_by(|a, b| {
        b.severity()
            .cmp(&a.severity())
            .then_with(|| a.order().cmp(&b.order()))
            .then_with(|| a.node().cmp(&b.node()))
    });
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::k8s::cluster_health::node::tests::{node, set_condition};
    use crate::k8s::cluster_health::node::Pressure;
    use crate::k8s::cluster_health::resources::tests::{container, pod_with};
    use k8s_openapi::api::core::v1::PodCondition;
    use std::time::SystemTime;

    const GI: i64 = 1 << 30;

    pub(crate) fn bound(name: &str, node: &str, cpu: &str, mem: &str) -> Pod {
        let mut p = pod_with(
            vec![container("c", &[("cpu", cpu), ("memory", mem)], &[])],
            vec![],
        );
        p.metadata.name = Some(name.into());
        p.spec.as_mut().unwrap().node_name = Some(node.into());
        p
    }

    pub(crate) fn unschedulable(name: &str, message: &str) -> Pod {
        let mut p = pod_with(vec![container("c", &[("memory", "1Gi")], &[])], vec![]);
        p.metadata.name = Some(name.into());
        let st = p.status.as_mut().unwrap();
        st.phase = Some("Pending".into());
        st.conditions = Some(vec![PodCondition {
            type_: "PodScheduled".into(),
            status: "False".into(),
            reason: Some("Unschedulable".into()),
            message: Some(message.into()),
            ..Default::default()
        }]);
        p
    }

    fn arcs<T>(v: Vec<T>) -> Vec<Arc<T>> {
        v.into_iter().map(Arc::new).collect()
    }

    fn run(nodes: Vec<Node>, pods: Vec<Pod>, usage: &[(&str, i64, i64)]) -> ClusterHealth {
        let nodes = arcs(nodes);
        let pods = arcs(pods);
        let usage: NodeUsageMap = usage
            .iter()
            .map(|(n, c, m)| (n.to_string(), Resources::new(*c, *m)))
            .collect();
        let status = if usage.is_empty() {
            UsageStatus::NotInstalled
        } else {
            UsageStatus::MetricsServer {
                sampled: SystemTime::UNIX_EPOCH,
            }
        };
        summarize(&Inputs {
            nodes: &nodes,
            node_access: NodeAccess::Visible,
            pods: &pods,
            usage: &usage,
            usage_status: &status,
            api: ApiSummary::default(),
            thresholds: Thresholds::default(),
        })
    }

    #[test]
    fn healthy_cluster_totals_and_per_node_accounting() {
        let h = run(
            vec![node("b", &[]), node("a", &[])],
            vec![
                bound("p1", "a", "1", "2Gi"),
                bound("p2", "a", "500m", "1Gi"),
                bound("p3", "b", "250m", "512Mi"),
            ],
            &[("a", 1950, 3 * GI), ("b", 390, GI)],
        );
        assert_eq!(h.level, Level::Healthy);
        assert!(h.issues.is_empty());
        assert_eq!(
            h.nodes.iter().map(|n| n.name.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"],
            "sorted by name"
        );
        let a = h.node("a").unwrap();
        assert_eq!(a.cpu.requests, 1500);
        assert_eq!(a.memory.requests, 3 * GI);
        assert_eq!(a.pods.count, 2);
        assert_eq!(a.cpu.usage, Some(1950));
        assert_eq!(a.cpu.usage_percent(), Some(50.0));

        let t = &h.totals;
        assert_eq!(t.basis, Basis::Usage);
        assert_eq!(t.cpu.allocatable, 7800);
        assert_eq!(t.cpu.requests, 1750);
        assert_eq!(t.cpu.usage, Some(2340));
        assert_eq!(t.percent(ResourceKind::Cpu), Some(30.0));
        assert_eq!(t.nodes.total, 2);
        assert_eq!(t.nodes.ready, 2);
        assert_eq!(t.pods.running, 3);
        assert_eq!(t.pods.capacity, 220);

        assert_eq!(h.headroom.free.cpu_millicores, 7800 - 1750);
        assert_eq!(h.headroom.largest_fit.cpu_millicores, 3900 - 250);
    }

    #[test]
    fn without_metrics_the_basis_is_requests() {
        let h = run(
            vec![node("a", &[])],
            vec![bound("p", "a", "1950m", "1Gi")],
            &[],
        );
        assert_eq!(h.totals.basis, Basis::Requests);
        assert_eq!(h.totals.percent(ResourceKind::Cpu), Some(50.0));
        assert_eq!(h.node("a").unwrap().cpu.usage, None);
    }

    #[test]
    fn a_not_ready_node_is_critical_and_leaves_the_totals() {
        let mut down = node("down", &[]);
        set_condition(&mut down, "Ready", "Unknown", 9_000);
        // Stale pressure on a dead node is not reported separately.
        set_condition(&mut down, "MemoryPressure", "True", 9_000);
        let h = run(
            vec![node("up", &[]), down],
            vec![
                bound("p1", "down", "1", "1Gi"),
                bound("p2", "down", "1", "1Gi"),
            ],
            &[("up", 100, GI)],
        );
        assert_eq!(h.level, Level::Critical);
        assert_eq!(h.issues.len(), 1, "{:?}", h.issues);
        match &h.issues[0] {
            Issue::NodeNotReady {
                node,
                unknown,
                pods,
                since,
                ..
            } => {
                assert_eq!(node, "down");
                assert!(unknown);
                assert_eq!(*pods, 2);
                assert!(since.is_some());
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(h.totals.nodes.total, 2);
        assert_eq!(h.totals.nodes.ready, 1);
        assert_eq!(h.totals.cpu.allocatable, 3900, "only the Ready node");
        assert_eq!(h.totals.cpu.requests, 0);
        assert_eq!(h.headroom.free.cpu_millicores, 3900);
    }

    #[test]
    fn pressure_warns_and_cordoned_nodes_offer_no_headroom() {
        let mut hot = node("hot", &[]);
        set_condition(&mut hot, "MemoryPressure", "True", 1);
        let mut cordoned = node("cordoned", &[]);
        cordoned.spec.as_mut().unwrap().unschedulable = Some(true);
        let h = run(vec![hot, cordoned], vec![], &[]);
        assert_eq!(h.level, Level::Warning);
        assert!(matches!(
            &h.issues[..],
            [Issue::NodePressure { node, pressure: Pressure::Memory, .. }] if node == "hot"
        ));
        assert_eq!(h.totals.nodes.under_pressure, 1);
        assert_eq!(h.totals.nodes.cordoned, 1);
        assert_eq!(h.headroom.free.cpu_millicores, 3900, "cordoned excluded");
        assert_eq!(h.issues_for("hot").count(), 1);
        assert_eq!(h.issues_for("cordoned").count(), 0);
    }

    #[test]
    fn unschedulable_pods_are_counted_listed_and_explained() {
        let common = "0/5 nodes are available: 3 Insufficient memory.";
        let h = run(
            vec![node("a", &[])],
            vec![
                unschedulable("z", common),
                unschedulable("y", common),
                unschedulable(
                    "x",
                    "0/5 nodes are available: 5 node(s) had untolerated taint.",
                ),
            ],
            &[],
        );
        assert_eq!(h.level, Level::Critical);
        assert_eq!(h.totals.pods.unschedulable, 3);
        assert_eq!(h.totals.pods.pending, 3);
        assert_eq!(
            h.unschedulable
                .iter()
                .map(|p| p.name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y", "z"]
        );
        assert!(matches!(
            &h.issues[..],
            [Issue::PodsUnschedulable { count: 3, reason: Some(r) }] if r == common
        ));
    }

    #[test]
    fn the_unschedulable_list_is_capped_but_the_count_is_exact() {
        let pods: Vec<Pod> = (0..MAX_UNSCHEDULABLE_LISTED + 5)
            .map(|i| unschedulable(&format!("p{i:03}"), "no"))
            .collect();
        let h = run(vec![], pods, &[]);
        assert_eq!(h.unschedulable.len(), MAX_UNSCHEDULABLE_LISTED);
        assert_eq!(
            h.totals.pods.unschedulable as usize,
            MAX_UNSCHEDULABLE_LISTED + 5
        );
    }

    #[test]
    fn high_memory_utilisation_warns_at_the_threshold() {
        let h = run(
            vec![node("a", &[])],
            vec![],
            &[("a", 100, 15 * GI * 85 / 100)],
        );
        assert!(matches!(
            &h.issues[..],
            [Issue::HighUtilisation { resource: ResourceKind::Memory, basis: Basis::Usage, percent }]
                if (*percent - 85.0).abs() < 0.01
        ));
        let h = run(
            vec![node("a", &[])],
            vec![],
            &[("a", 100, 15 * GI * 84 / 100)],
        );
        assert!(h.issues.is_empty());
    }

    #[test]
    fn issues_are_ordered_by_severity_then_kind_then_node() {
        let mut down_b = node("b", &[]);
        set_condition(&mut down_b, "Ready", "False", 1);
        let mut down_a = node("a", &[]);
        set_condition(&mut down_a, "Ready", "False", 1);
        let mut hot = node("c", &[]);
        set_condition(&mut hot, "DiskPressure", "True", 1);
        let h = run(
            vec![hot, down_b, down_a],
            vec![unschedulable("p", "no")],
            &[],
        );
        let kinds: Vec<(Option<&str>, u8)> =
            h.issues.iter().map(|i| (i.node(), i.order())).collect();
        assert_eq!(
            kinds,
            vec![(Some("a"), 0), (Some("b"), 0), (None, 2), (Some("c"), 3)]
        );
    }

    #[test]
    fn hidden_nodes_still_total_requests_from_pods() {
        let pods = arcs(vec![bound("p", "somewhere", "1", "1Gi")]);
        let h = summarize(&Inputs {
            nodes: &[],
            node_access: NodeAccess::Forbidden,
            pods: &pods,
            usage: &NodeUsageMap::new(),
            usage_status: &UsageStatus::Pending,
            api: ApiSummary::default(),
            thresholds: Thresholds::default(),
        });
        assert_eq!(h.node_access, NodeAccess::Forbidden);
        assert_eq!(h.totals.cpu.requests, 1000);
        assert_eq!(h.totals.memory.requests, GI);
        assert_eq!(h.totals.percent(ResourceKind::Cpu), None);
        assert_eq!(h.level, Level::Healthy);
    }

    #[test]
    fn usage_for_unknown_nodes_and_stale_maps_is_ignored_when_metrics_are_down() {
        let nodes = arcs(vec![node("a", &[])]);
        let usage: NodeUsageMap = [("a".to_string(), Resources::new(1, 1))].into();
        let h = summarize(&Inputs {
            nodes: &nodes,
            node_access: NodeAccess::Visible,
            pods: &[],
            usage: &usage,
            usage_status: &UsageStatus::Unavailable,
            api: ApiSummary::default(),
            thresholds: Thresholds::default(),
        });
        assert_eq!(
            h.node("a").unwrap().cpu.usage,
            None,
            "stale usage not shown"
        );
        assert_eq!(h.totals.basis, Basis::Requests);
    }

    /// `cargo test --release --no-default-features --lib summarize_perf -- --ignored --nocapture`
    #[test]
    #[ignore = "timing, run in release by hand"]
    fn summarize_perf_10k_pods_500_nodes() {
        let nodes: Vec<Node> = (0..500).map(|i| node(&format!("n{i}"), &[])).collect();
        let pods: Vec<Pod> = (0..10_000)
            .map(|i| bound(&format!("p{i}"), &format!("n{}", i % 500), "100m", "128Mi"))
            .collect();
        let usage: Vec<(String, i64, i64)> =
            (0..500).map(|i| (format!("n{i}"), 1000, GI)).collect();
        let nodes = arcs(nodes);
        let pods = arcs(pods);
        let usage: NodeUsageMap = usage
            .iter()
            .map(|(n, c, m)| (n.clone(), Resources::new(*c, *m)))
            .collect();
        let status = UsageStatus::MetricsServer {
            sampled: SystemTime::UNIX_EPOCH,
        };
        let input = Inputs {
            nodes: &nodes,
            node_access: NodeAccess::Visible,
            pods: &pods,
            usage: &usage,
            usage_status: &status,
            api: ApiSummary::default(),
            thresholds: Thresholds::default(),
        };
        let runs = 20;
        let started = std::time::Instant::now();
        let mut h = None;
        for _ in 0..runs {
            h = Some(summarize(&input));
        }
        let per = started.elapsed() / runs;
        let h = h.unwrap();
        assert_eq!(h.totals.pods.running, 10_000);
        println!("summarize: {per:?} per run (10k pods, 500 nodes)");
    }
}
