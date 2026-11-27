//! "Top pods by memory" for one node, read on demand.
//!
//! The metrics API can't filter pod metrics by node, and listing every
//! pod's metrics cluster-wide is megabytes on a big cluster.  The pod store
//! already knows which pods are on the node, so this lists pod metrics only
//! in *their* namespaces (a handful, a few at a time) and joins locally.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::reflector::Store;

use super::monitor::usage_status_of;
use super::resources::{bound_node, pod_resources, Resources};
use super::snapshot::UsageStatus;
use crate::k8s::metrics::MetricsSource;

/// Namespaces whose pod metrics are listed at once.
const CONCURRENT_NAMESPACES: usize = 4;

/// One pod on the node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodUsageRow {
    pub namespace: String,
    pub name: String,
    /// Live usage; `None` without a metrics-server or for a pod it hasn't
    /// sampled yet.
    pub usage: Option<Resources>,
    pub requests: Resources,
}

/// The pods on one node, heaviest first.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NodePods {
    /// At most the requested number of rows.
    pub top: Vec<PodUsageRow>,
    /// Every pod bound to the node.
    pub total: usize,
    /// Whether `usage` could be read; rows are ranked by memory requests
    /// when it couldn't.
    pub usage: UsageStatus,
}

/// Heaviest by memory first: usage when there is any, else requests.
fn rank(mut rows: Vec<PodUsageRow>, limit: usize) -> Vec<PodUsageRow> {
    let key = |r: &PodUsageRow| {
        (
            r.usage.map_or(r.requests.memory_bytes, |u| u.memory_bytes),
            r.requests.memory_bytes,
        )
    };
    rows.sort_by(|a, b| {
        key(b)
            .cmp(&key(a))
            .then_with(|| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)))
    });
    rows.truncate(limit);
    rows
}

/// Pods bound to `node` that still hold resources there.
pub(crate) fn pods_bound_to(pods: &Store<Pod>, node: &str) -> Vec<Arc<Pod>> {
    pods.state()
        .into_iter()
        .filter(|p| bound_node(p) == Some(node))
        .collect()
}

pub(crate) async fn top_pods_on_node(
    pods: &Store<Pod>,
    metrics: Option<Arc<dyn MetricsSource>>,
    node: &str,
    limit: usize,
) -> NodePods {
    let mut rows: Vec<PodUsageRow> = pods_bound_to(pods, node)
        .iter()
        .map(|p| PodUsageRow {
            namespace: p.metadata.namespace.clone().unwrap_or_default(),
            name: p.metadata.name.clone().unwrap_or_default(),
            usage: None,
            requests: pod_resources(p).requests,
        })
        .collect();
    let total = rows.len();

    let Some(metrics) = metrics else {
        return NodePods {
            top: rank(rows, limit),
            total,
            usage: UsageStatus::NotInstalled,
        };
    };
    let namespaces: BTreeSet<String> = rows.iter().map(|r| r.namespace.clone()).collect();
    let results: Vec<_> = stream::iter(namespaces)
        .map(|ns| {
            let metrics = metrics.clone();
            async move {
                let r = metrics.pod_usages(&ns).await;
                (ns, r)
            }
        })
        .buffer_unordered(CONCURRENT_NAMESPACES)
        .collect()
        .await;

    let mut usage: HashMap<(String, String), Resources> = HashMap::new();
    let mut status = UsageStatus::MetricsServer {
        sampled: std::time::SystemTime::now(),
    };
    for (ns, result) in results {
        match result {
            Ok(samples) => {
                usage.extend(samples.into_iter().map(|s| ((ns.clone(), s.name), s.usage)));
            }
            // Report the first failure; the namespaces that answered still count.
            Err(e) if status.is_available() => status = usage_status_of(&e),
            Err(_) => {}
        }
    }
    for row in &mut rows {
        row.usage = usage
            .get(&(row.namespace.clone(), row.name.clone()))
            .copied();
    }
    NodePods {
        top: rank(rows, limit),
        total,
        usage: status,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(ns: &str, name: &str, usage: Option<i64>, req: i64) -> PodUsageRow {
        PodUsageRow {
            namespace: ns.into(),
            name: name.into(),
            usage: usage.map(|m| Resources::new(0, m)),
            requests: Resources::new(0, req),
        }
    }

    #[test]
    fn ranks_by_usage_then_requests_and_truncates() {
        let ranked = rank(
            vec![
                row("a", "small", Some(10), 500),
                row("a", "big", Some(900), 100),
                row("b", "unsampled", None, 300),
                row("a", "tie-b", Some(50), 0),
                row("a", "tie-a", Some(50), 0),
            ],
            4,
        );
        let names: Vec<_> = ranked.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, vec!["big", "unsampled", "tie-a", "tie-b"]);
    }
}
