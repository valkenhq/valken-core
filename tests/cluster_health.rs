//! End-to-end tests of the cluster health monitor over an in-memory
//! cluster: [`KubeWatcher`] + fake nodes, pods and metrics, no API server.

mod support;

use std::sync::Arc;
use std::time::Duration;

use kube::runtime::watcher::Event;
use support::*;
use valken_core::k8s::cluster_health::{
    Basis, ClusterHealth, ClusterHealthSettings, Issue, Level, NodeAccess, Readiness, ResourceKind,
    UsageStatus,
};
use valken_core::k8s::{ClusterInfo, KubeWatcher, Watched};

const GI: i64 = 1 << 30;

fn fast() -> ClusterHealthSettings {
    ClusterHealthSettings {
        usage_interval: Duration::from_millis(100),
        coalesce: Duration::from_millis(20),
        node_retry: Duration::from_millis(200),
        usage_grace: Duration::ZERO,
        ..Default::default()
    }
}

/// A watching [`KubeWatcher`] over `cluster`, synced with `snapshot`.
async fn watching(
    cluster: Arc<FakeCluster>,
    snapshot: Snapshot,
    settings: ClusterHealthSettings,
) -> (KubeWatcher, crossbeam_channel::Receiver<Watched>) {
    let (tx, rx) = crossbeam_channel::unbounded();
    let w = KubeWatcher::new(tx).with_cluster_health_settings(settings);
    w.connect_with(cluster.clone(), ClusterInfo::default())
        .await;
    let c = cluster.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        c.sync(&snapshot);
    });
    w.begin_watching().await.expect("initial sync");
    // The monitor opens its node watch once the pod store is ready; events
    // sent before that would go nowhere.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while cluster.node_watch_count() == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "node watch never opened"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    (w, rx)
}

/// Wait until the latest snapshot satisfies `pred`; panics with the last
/// snapshot on timeout.
async fn health_where(
    w: &KubeWatcher,
    what: &str,
    pred: impl Fn(&ClusterHealth) -> bool,
) -> Arc<ClusterHealth> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let h = w.cluster_health();
        if h.updated.is_some() && pred(&h) {
            return h;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("timed out waiting for {what}; last snapshot: {h:#?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn shop_pods() -> Vec<k8s_openapi::api::core::v1::Pod> {
    vec![
        pod_on("shop", "api-1", "n1", "1", "4Gi"),
        pod_on("shop", "api-2", "n2", "500m", "2Gi"),
        pod_on("pay", "ledger-1", "n1", "1", "2Gi"),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_healthy_cluster_with_nodes_pods_and_usage() {
    let cluster = FakeCluster::new();
    let metrics = FakeMetrics::new();
    metrics.set_nodes(&[("n1", 1000, 8 * GI), ("n2", 600, 4 * GI)]);
    cluster.set_metrics(metrics.clone());
    let (w, _rx) = watching(
        cluster.clone(),
        Snapshot {
            pods: shop_pods(),
            ..Default::default()
        },
        fast(),
    )
    .await;
    cluster.sync_nodes(&[node("n1"), node("n2")]);

    let h = health_where(&w, "nodes + usage", |h| {
        h.node_access == NodeAccess::Visible && h.usage.is_available()
    })
    .await;
    assert_eq!(h.level, Level::Healthy, "{:?}", h.issues);
    assert_eq!(h.totals.basis, Basis::Usage);
    assert_eq!(h.totals.nodes.total, 2);
    assert_eq!(h.totals.nodes.ready, 2);
    assert_eq!(h.totals.pods.running, 3);
    assert_eq!(h.totals.pods.capacity, 220);
    assert_eq!(h.totals.cpu.allocatable, 8000);
    assert_eq!(h.totals.cpu.requests, 2500);
    assert_eq!(h.totals.percent(ResourceKind::Cpu), Some(20.0));
    assert_eq!(h.totals.percent(ResourceKind::Memory), Some(37.5));

    let n1 = h.node("n1").expect("n1");
    assert_eq!(n1.pods.count, 2);
    assert_eq!(n1.cpu.requests, 2000);
    assert_eq!(n1.memory.requests, 6 * GI);
    assert_eq!(n1.memory.usage, Some(8 * GI));
    assert_eq!(n1.zone.as_deref(), Some("zone-a"));

    w.stop().await;
    assert!(
        w.cluster_health().updated.is_none(),
        "stop resets the snapshot"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_going_not_ready_turns_the_verdict_critical_within_a_coalesce_window() {
    let cluster = FakeCluster::new();
    let (w, _rx) = watching(
        cluster.clone(),
        Snapshot {
            pods: shop_pods(),
            ..Default::default()
        },
        // Usage polling far away: only the node event can cause the change.
        ClusterHealthSettings {
            usage_interval: Duration::from_secs(3600),
            ..fast()
        },
    )
    .await;
    cluster.sync_nodes(&[node("n1"), node("n2")]);
    health_where(&w, "healthy", |h| {
        h.node_access == NodeAccess::Visible && h.level == Level::Healthy
    })
    .await;

    let started = std::time::Instant::now();
    cluster.send_node(Event::Apply(node_ready("n1", "Unknown")));
    let h = health_where(&w, "n1 not ready", |h| h.level == Level::Critical).await;
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "seen in {:?}",
        started.elapsed()
    );
    assert_eq!(h.node("n1").unwrap().readiness, Readiness::Unknown);
    match &h.issues[0] {
        Issue::NodeNotReady {
            node,
            unknown,
            pods,
            ..
        } => {
            assert_eq!(node, "n1");
            assert!(unknown);
            assert_eq!(*pods, 2, "pods bound to the node");
        }
        other => panic!("expected NodeNotReady first, got {other:?}"),
    }
    assert_eq!(h.totals.nodes.ready, 1);
    w.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pod_churn_is_coalesced_into_few_snapshots() {
    let cluster = FakeCluster::new();
    let (w, _rx) = watching(
        cluster.clone(),
        Snapshot::default(),
        ClusterHealthSettings {
            usage_interval: Duration::from_secs(3600),
            coalesce: Duration::from_millis(200),
            ..fast()
        },
    )
    .await;
    cluster.sync_nodes(&[node("n1")]);
    health_where(&w, "nodes", |h| h.node_access == NodeAccess::Visible).await;

    let mut rx = w.subscribe_cluster_health();
    rx.borrow_and_update();
    let started = std::time::Instant::now();
    for i in 0..500 {
        cluster.send_pod(Event::Apply(pod_on(
            "shop",
            &format!("p{i}"),
            "n1",
            "10m",
            "16Mi",
        )));
    }
    let h = health_where(&w, "all 500 pods", |h| h.totals.pods.running == 500).await;
    let mut snapshots = 0;
    while rx.has_changed().unwrap() {
        rx.borrow_and_update();
        snapshots += 1;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let windows = started.elapsed().as_millis() / 200 + 2;
    assert!(
        (snapshots as u128) <= windows,
        "500 pod events, {snapshots} snapshots in {:?}",
        started.elapsed()
    );
    assert_eq!(h.node("n1").unwrap().cpu.requests, 5000);
    w.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forbidden_nodes_degrade_without_failing_the_session_and_are_retried() {
    let cluster = FakeCluster::new();
    let (w, rx) = watching(
        cluster.clone(),
        Snapshot {
            pods: shop_pods(),
            ..Default::default()
        },
        fast(),
    )
    .await;
    cluster.send_node_error(403);

    let h = health_where(&w, "forbidden", |h| h.node_access == NodeAccess::Forbidden).await;
    assert!(h.nodes.is_empty());
    assert_eq!(
        h.totals.cpu.requests, 2500,
        "requests still summed from pods"
    );
    assert_eq!(h.totals.percent(ResourceKind::Cpu), None);
    assert!(w.is_running(), "the watch session is unaffected");
    // The resource watch still flows.
    cluster.send_pod(Event::Apply(pod_on("shop", "late", "n1", "1m", "1Mi")));
    let mut saw_pod = false;
    for _ in 0..10 {
        if let Ok(Watched::Pod(_)) = rx.recv_timeout(Duration::from_millis(500)) {
            saw_pod = true;
            break;
        }
    }
    assert!(saw_pod);

    // RBAC fixed: the retry picks the nodes up.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while cluster.node_watch_count() < 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "node watch not retried"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cluster.sync_nodes(&[node("n1"), node("n2")]);
    let h = health_where(&w, "visible after retry", |h| {
        h.node_access == NodeAccess::Visible
    })
    .await;
    assert_eq!(h.nodes.len(), 2);
    w.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_a_metrics_server_percentages_are_requests() {
    let cluster = FakeCluster::new();
    let metrics = FakeMetrics::new();
    metrics.fail_nodes(404);
    cluster.set_metrics(metrics.clone());
    let (w, _rx) = watching(
        cluster.clone(),
        Snapshot {
            pods: shop_pods(),
            ..Default::default()
        },
        fast(),
    )
    .await;
    cluster.sync_nodes(&[node("n1"), node("n2")]);

    let h = health_where(&w, "not installed", |h| {
        h.usage == UsageStatus::NotInstalled && h.node_access == NodeAccess::Visible
    })
    .await;
    assert_eq!(h.totals.basis, Basis::Requests);
    assert_eq!(h.totals.percent(ResourceKind::Cpu), Some(31.25));
    assert!(h.nodes.iter().all(|n| n.cpu.usage.is_none()));

    // Installed later: picked up on the next poll.
    metrics.set_nodes(&[("n1", 100, GI), ("n2", 100, GI)]);
    let h = health_where(&w, "usage", |h| h.usage.is_available()).await;
    assert_eq!(h.totals.basis, Basis::Usage);
    w.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_gets_one_sample_per_usage_interval() {
    let cluster = FakeCluster::new();
    let metrics = FakeMetrics::new();
    metrics.set_nodes(&[("n1", 2000, 8 * GI)]);
    cluster.set_metrics(metrics.clone());
    let (w, _rx) = watching(cluster.clone(), Snapshot::default(), fast()).await;
    cluster.sync_nodes(&[node("n1")]);

    let h = health_where(&w, "4 samples", |h| h.history.len() >= 4).await;
    let reads = metrics.node_read_count() as usize;
    assert!(
        h.history.len() <= reads,
        "never more samples ({}) than reads ({reads})",
        h.history.len()
    );
    let last_cpu = h.history.cpu_percent.last();
    assert_eq!(last_cpu, Some(50.0));
    assert_eq!(h.history.pods_running.last(), Some(0.0));
    assert!(h.since.is_some());
    w.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unschedulable_pods_make_the_verdict_critical() {
    use k8s_openapi::api::core::v1::PodCondition;
    let cluster = FakeCluster::new();
    let mut stuck = pod("shop", "stuck", "uid-stuck", labels(&[]));
    let st = stuck.status.as_mut().unwrap();
    st.phase = Some("Pending".into());
    st.conditions = Some(vec![PodCondition {
        type_: "PodScheduled".into(),
        status: "False".into(),
        reason: Some("Unschedulable".into()),
        message: Some("0/2 nodes are available: 2 Insufficient memory.".into()),
        ..Default::default()
    }]);
    let (w, _rx) = watching(
        cluster.clone(),
        Snapshot {
            pods: vec![stuck],
            ..Default::default()
        },
        fast(),
    )
    .await;
    cluster.sync_nodes(&[node("n1")]);
    let h = health_where(&w, "critical", |h| h.level == Level::Critical).await;
    assert_eq!(h.unschedulable.len(), 1);
    assert!(matches!(
        &h.issues[..],
        [Issue::PodsUnschedulable { count: 1, reason: Some(r) }] if r.contains("Insufficient memory")
    ));
    w.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn top_pods_reads_only_the_namespaces_on_the_node() {
    let cluster = FakeCluster::new();
    let metrics = FakeMetrics::new();
    metrics.set_pods("shop", &[("api-1", 100, 512 << 20), ("api-2", 10, 4 * GI)]);
    metrics.set_pods("pay", &[("ledger-1", 50, GI)]);
    cluster.set_metrics(metrics.clone());
    let (w, _rx) = watching(
        cluster.clone(),
        Snapshot {
            pods: shop_pods(),
            ..Default::default()
        },
        fast(),
    )
    .await;

    let top = w.top_pods_on_node("n1", 5).await;
    assert_eq!(top.total, 2);
    let names: Vec<_> = top.top.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(names, vec!["ledger-1", "api-1"], "by memory usage");
    assert!(top.usage.is_available());
    assert_eq!(
        top.top[1].requests.memory_bytes,
        4 * GI,
        "requests alongside usage"
    );
    let mut read = metrics.pod_namespaces_read.lock().unwrap().clone();
    read.sort();
    assert_eq!(read, vec!["pay", "shop"]);

    assert_eq!(w.top_pods_on_node("n1", 1).await.top.len(), 1);
    let mut on_n1: Vec<String> = w
        .pods_on_node("n1")
        .iter()
        .map(|p| p.metadata.name.clone().unwrap_or_default())
        .collect();
    on_n1.sort();
    assert_eq!(on_n1, vec!["api-1", "ledger-1"]);
    assert!(w.pods_on_node("nowhere").is_empty());
    assert_eq!(w.top_pods_on_node("nowhere", 5).await.total, 0);
    w.stop().await;
    assert!(w.pods_on_node("n1").is_empty(), "nothing while stopped");
    assert_eq!(
        w.top_pods_on_node("n1", 5).await.total,
        0,
        "nothing while stopped"
    );
}
