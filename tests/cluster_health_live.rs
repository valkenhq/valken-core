//! The cluster health monitor against a real cluster.
//!
//! ```sh
//! VALKEN_TEST_KUBECONFIG=/path/to/kubeconfig \
//! VALKEN_TEST_KUBECONFIG_LIMITED=/path/to/limited-kubeconfig \
//!   cargo test --no-default-features --test cluster_health_live -- --ignored --nocapture
//! ```
//!
//! The limited kubeconfig must be allowed pods / namespaces / services /
//! deployments / ingresses but not nodes.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use valken_core::k8s::cluster_health::{ClusterHealth, NodeAccess, UsageStatus};
use valken_core::k8s::kubeconfig::{discover, ExecEnv, KubeconfigSources, SourceKind};
use valken_core::k8s::KubeWatcher;

async fn watch(var: &str) -> Option<KubeWatcher> {
    let path = PathBuf::from(std::env::var_os(var)?);
    let d = discover(&KubeconfigSources {
        files: vec![(path, SourceKind::Env)],
    });
    let ctx = d
        .context(d.current_context.as_deref().unwrap())
        .unwrap()
        .clone();
    let exec = ExecEnv::default();
    let (tx, rx) = crossbeam_channel::unbounded();
    // Drain the resource channel so the watch never stalls on it.
    std::thread::spawn(move || while rx.recv().is_ok() {});
    let w = KubeWatcher::new(tx);
    w.connect_context(
        d.client_config(&ctx.name, &exec, Duration::from_secs(5))
            .await
            .unwrap(),
        ctx.name.clone(),
        &ctx.auth,
        &exec,
        &|_| {},
    )
    .await
    .unwrap();
    w.begin_watching().await.unwrap();
    Some(w)
}

async fn health_where(
    w: &KubeWatcher,
    pred: impl Fn(&ClusterHealth) -> bool,
) -> Arc<ClusterHealth> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let h = w.cluster_health();
        if h.updated.is_some() && pred(&h) {
            return h;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out; last: {h:#?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs VALKEN_TEST_KUBECONFIG"]
async fn real_cluster_nodes_requests_usage_and_api_latency() {
    let Some(w) = watch("VALKEN_TEST_KUBECONFIG").await else {
        return;
    };
    let h = health_where(&w, |h| {
        h.node_access == NodeAccess::Visible && h.usage != UsageStatus::Pending
    })
    .await;
    println!(
        "level {:?}, {} nodes ({} ready), usage {:?}, cpu {:?} % ({:?}), mem {:?} %, pods {}, api {:?}",
        h.level,
        h.totals.nodes.total,
        h.totals.nodes.ready,
        h.usage,
        h.totals.percent(valken_core::k8s::cluster_health::ResourceKind::Cpu),
        h.totals.basis,
        h.totals.percent(valken_core::k8s::cluster_health::ResourceKind::Memory),
        h.totals.pods.running,
        h.api,
    );
    assert!(!h.nodes.is_empty());
    let node = &h.nodes[0];
    assert!(node.cpu.allocatable > 0 && node.memory.allocatable > 0);
    assert!(node.system.kubelet_version.is_some());
    assert!(
        h.nodes.iter().any(|n| n.pods.count > 0),
        "system pods are bound somewhere"
    );
    assert!(h.totals.cpu.requests > 0, "control-plane pods request CPU");
    assert!(
        h.api.requests > 0,
        "the connect's own requests are recorded"
    );

    let top = w.top_pods_on_node(&node.name, 5).await;
    println!("top pods on {}: {:?}", node.name, top);
    assert_eq!(top.total as u32, node.pods.count);
    w.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs VALKEN_TEST_KUBECONFIG_LIMITED"]
async fn real_cluster_without_node_access_is_pod_derived() {
    let Some(w) = watch("VALKEN_TEST_KUBECONFIG_LIMITED").await else {
        return;
    };
    let h = health_where(&w, |h| h.node_access == NodeAccess::Forbidden).await;
    assert!(h.nodes.is_empty());
    assert!(h.totals.cpu.requests > 0);
    assert!(
        w.is_running(),
        "a forbidden node list never stops the watch"
    );
    w.stop().await;
}
