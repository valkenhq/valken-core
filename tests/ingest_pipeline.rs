//! Watcher events → [`ResourceIngest`] → store + graph, end to end, with
//! no engine involved.  This is what the Godot `KubeStore` node does on its
//! drain timer.

mod support;

use std::time::Duration;

use kube::runtime::watcher::Event;
use support::*;
use valken_core::k8s::{ClusterInfo, KubeResourceIdentifier, KubeWatcher, ResourceIngest};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_events_build_and_maintain_the_graph() {
    let (tx, rx) = crossbeam_channel::unbounded();
    let w = KubeWatcher::new(tx);
    let cluster = FakeCluster::new();
    w.connect_with(cluster.clone(), ClusterInfo::default())
        .await;
    w.set_cluster_id(4);
    cluster.sync(&Snapshot {
        pods: vec![
            pod("shop", "api-1", "u1", labels(&[("app", "api")])),
            pod("other", "api-9", "u9", labels(&[("app", "api")])),
        ],
        services: vec![service("shop", "api", labels(&[("app", "api")]))],
        ingresses: vec![ingress("shop", "api", "api")],
        namespaces: vec![namespace("shop", labels(&[("team", "shop")]))],
        ..Default::default()
    });
    w.begin_watching().await.unwrap();

    let handle = tokio::runtime::Handle::current();
    let mut ingest = ResourceIngest::new();

    // Drain what the watcher sent, as the drain timer would.
    let msgs: Vec<_> = rx.try_iter().collect();
    assert_eq!(msgs.len(), 2);
    let report = ingest.apply(msgs, &handle);
    assert!(report.full_list_received);
    assert!(report.namespaces_updated);
    assert_eq!(ingest.cluster_id(), 4);
    assert_eq!(ingest.get_all_kris().len(), 4);
    assert_eq!(ingest.get_namespace_labels()["shop"]["team"], "shop");

    // The graph is built in the background; poll it in as a frame would.
    assert!(wait_until(Duration::from_secs(5), || ingest.poll_graph_build(&handle)));
    assert_eq!(ingest.graph_node_count(), 4);
    // Service→Pod (same namespace only) and Ingress→Service.
    assert_eq!(ingest.graph_edge_count(), 2);

    let svc = KubeResourceIdentifier::new(4, Some("shop".into()), "core", "v1", "Service", "api");
    let out = ingest.get_outgoing_edges_for_kri(&svc).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].0.name, "api-1");
    assert_eq!(
        ingest.get_edges_for_kri(&svc).unwrap().len(),
        3,
        "ingress, service and pod form one component"
    );

    // A pod in the other namespace gets the same labels: no new edge.
    cluster.send_pod(Event::Apply(pod(
        "other",
        "api-10",
        "u10",
        labels(&[("app", "api")]),
    )));
    // A pod deletion drops its node and edge.
    cluster.send_pod(Event::Delete(pod("shop", "api-1", "u1", labels(&[]))));
    assert!(wait_until(Duration::from_secs(5), || rx.len() >= 2));
    let report = ingest.apply(rx.try_iter().collect(), &handle);
    assert_eq!(report.updated.len(), 1);
    assert_eq!(report.deleted.len(), 1);
    assert_eq!(ingest.graph_node_count(), 4);
    assert_eq!(ingest.graph_edge_count(), 1, "only Ingress→Service remains");
    assert!(ingest.get_outgoing_edges_for_kri(&svc).unwrap().is_empty());

    w.stop().await;
}
