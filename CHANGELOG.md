# Changelog

Notable changes, newest first. Versions are git tags; see *Contributing
and releases* in the README.

## Unreleased (0.3.0)

- **Cluster health** (`k8s::cluster_health`): every `KubeWatcher` session
  keeps a `ClusterHealth` snapshot — nodes with conditions, pool / zone /
  instance type, taints, capacity vs allocatable vs requests vs limits vs
  live usage; cluster totals over Ready nodes; scheduling headroom;
  unschedulable pods with the scheduler's reason; issues with a severity
  and an overall level; 1 h in-memory sparklines.  Read it with
  `KubeWatcher::cluster_health` / `subscribe_cluster_health`.
  `KubeWatcher::top_pods_on_node` reads pod usage on demand;
  `KubeWatcher::pods_on_node` lists a node's pods from the watch store.
- Pod requests and limits follow the scheduler's formula (init containers,
  sidecars, overhead, pod-level resources, pending resizes):
  `cluster_health::pod_resources`.
- **API latency**: `ApiStatsLayer` records time-to-headers and outcome of
  every non-streaming request on the watcher's client; p95, errors and a
  "slower than usual" flag are in the snapshot (`KubeWatcher::api_stats`).
- `ClusterSource::watch_nodes` (defaults to `None`).  Without permission to
  list nodes the snapshot is pod-derived and the session carries on.
- `MetricsSource::node_usages` / `pod_usages` (default
  `MetricsError::Unsupported`); new `MetricsError::Unsupported` and
  `MetricsError::Timeout` variants.
- Deprecated: `ClusterMetricsMonitor` and friends,
  `AsyncMessage::ClusterMetrics`, `KubeWatcher::with_notifier` /
  `with_metrics_settings`.  The watcher no longer runs the old cluster
  metrics monitor or sends `ClusterMetrics`.

## 0.1.0

Initial public release: cluster connection and watch, resource store and
relationship graph, live tables, events, metrics, ingress traffic tailing,
the security inventory, and the one write path (`k8s::write`, dry run
first).

What never leaves the cluster or reaches a log: Secret values, env values,
spec-copy annotations and credential-plugin environments — see
`SECURITY.md` and `AGENTS.md`.
