//! End-to-end tests of [`KubeWatcher`] over an in-memory cluster: no API
//! server, no Godot.

mod support;

use std::sync::Arc;
use std::time::Duration;

use kube::runtime::watcher::Event;
use support::*;
use valken_core::k8s::{ClusterInfo, KubeResource, KubeWatcher, WatchError, Watched};

fn watcher() -> (
    KubeWatcher,
    crossbeam_channel::Receiver<Watched>,
    crossbeam_channel::Receiver<Watched>,
) {
    let (tx, rx) = crossbeam_channel::unbounded();
    let w = KubeWatcher::new(tx);
    let fan = w.create_fanout_channel();
    (w, rx, fan)
}

fn snapshot() -> Snapshot {
    Snapshot {
        pods: vec![pod("shop", "api-1", "u1", labels(&[("app", "api")]))],
        services: vec![service("shop", "api", labels(&[("app", "api")]))],
        ingresses: vec![ingress("shop", "api", "api")],
        namespaces: vec![namespace("shop", labels(&[("team", "shop")]))],
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_sync_sends_namespaces_then_full_list_to_every_channel() {
    let (w, rx, fan) = watcher();
    let cluster = FakeCluster::new();
    w.connect_with(cluster.clone(), ClusterInfo::default())
        .await;
    assert_eq!(w.get_cluster_uuid().as_deref(), Some("fake-cluster-uuid"));
    w.set_cluster_id(9);

    // begin_watching must not return before the sync is complete, so feed
    // the fake from a task.
    let c = cluster.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        c.sync(&snapshot());
    });
    w.begin_watching().await.expect("sync completes");
    assert!(w.is_running());

    for (name, ch) in [("primary", &rx), ("fanout", &fan)] {
        match recv(ch, name) {
            Watched::NamespaceList(infos) => {
                assert_eq!(infos.len(), 1);
                assert_eq!(infos[0].name, "shop");
                assert_eq!(infos[0].labels["team"], "shop");
            }
            other => panic!("{name}: expected NamespaceList first, got {other:?}"),
        }
        match recv(ch, name) {
            Watched::FullResourceList(list) => {
                assert_eq!(list.cluster_id, 9);
                assert_eq!(list.resources.len(), 3);
                assert!(list
                    .resources
                    .iter()
                    .any(|r| matches!(r, KubeResource::Ingress(_))));
            }
            other => panic!("{name}: expected FullResourceList, got {other:?}"),
        }
        assert!(ch.try_recv().is_err(), "{name}: nothing else yet");
    }

    w.stop().await;
    assert!(!w.is_running());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_events_relist_and_stop() {
    let (w, rx, _fan) = watcher();
    let cluster = FakeCluster::new();
    w.connect_with(cluster.clone(), ClusterInfo::default())
        .await;
    cluster.sync(&snapshot());
    w.begin_watching().await.unwrap();
    let _ = recv(&rx, "namespaces");
    let _ = recv(&rx, "full list");

    // Apply / delete of every watched kind are forwarded.
    cluster.send_pod(Event::Apply(pod("shop", "api-2", "u2", labels(&[]))));
    assert!(
        matches!(recv(&rx, "pod apply"), Watched::Pod(p) if p.metadata.name.as_deref() == Some("api-2"))
    );
    cluster.send_service(Event::Apply(service("shop", "web", labels(&[]))));
    assert!(matches!(recv(&rx, "service apply"), Watched::Service(_)));
    cluster.send_ingress(Event::Delete(ingress("shop", "api", "api")));
    assert!(matches!(
        recv(&rx, "ingress delete"),
        Watched::DeletedIngress(_)
    ));
    let dep = k8s_openapi::api::apps::v1::Deployment {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("api".into()),
            namespace: Some("shop".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    cluster.send_deployment(Event::Delete(dep));
    assert!(matches!(
        recv(&rx, "deployment delete"),
        Watched::DeletedDeployment(_)
    ));

    // A namespace change re-sends the namespace list.
    cluster.send_namespace(Event::Apply(namespace("billing", labels(&[]))));
    match recv(&rx, "namespace list") {
        Watched::NamespaceList(infos) => {
            let names: Vec<_> = infos.iter().map(|n| n.name.as_str()).collect();
            assert_eq!(names, vec!["billing", "shop"]);
        }
        other => panic!("expected NamespaceList, got {other:?}"),
    }

    // A re-list of one kind (watch expired) yields a fresh full snapshot
    // built from every reflector's current state.
    cluster.send_pod(Event::Init);
    cluster.send_pod(Event::InitApply(pod("shop", "api-3", "u3", labels(&[]))));
    cluster.send_pod(Event::InitDone);
    match recv(&rx, "re-list") {
        Watched::FullResourceList(list) => {
            let pods: Vec<String> = list
                .resources
                .iter()
                .filter_map(|r| match r {
                    KubeResource::Pod(p) => p.metadata.name.clone(),
                    _ => None,
                })
                .collect();
            assert_eq!(pods, vec!["api-3"], "only the re-listed pod remains");
            let services = list
                .resources
                .iter()
                .filter(|r| matches!(r, KubeResource::Service(_)))
                .count();
            assert_eq!(services, 2, "other kinds keep their state");
        }
        other => panic!("expected FullResourceList, got {other:?}"),
    }

    // Transient errors after the sync are logged, not fatal.
    cluster.send_pod_error();
    cluster.send_pod(Event::Apply(pod("shop", "api-4", "u4", labels(&[]))));
    assert!(matches!(recv(&rx, "pod after error"), Watched::Pod(_)));

    // Stop cancels the task; nothing arrives afterwards.
    w.stop().await;
    assert!(!w.is_running());
    cluster.send_pod(Event::Apply(pod("shop", "api-5", "u5", labels(&[]))));
    assert!(rx.recv_timeout(Duration::from_millis(300)).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_before_sync_is_reported_and_leaves_the_watcher_stopped() {
    let (w, rx, _fan) = watcher();
    let cluster = FakeCluster::new();
    w.connect_with(cluster.clone(), ClusterInfo::default())
        .await;
    cluster.send_pod_error();
    let err = w.begin_watching().await.unwrap_err();
    assert!(matches!(err, WatchError::InitialSync(_)), "{err}");
    assert!(!w.is_running());
    assert!(rx.try_recv().is_err(), "no partial snapshot is sent");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watching_twice_or_before_connecting_is_an_error() {
    let (w, _rx, _fan) = watcher();
    assert!(matches!(
        w.begin_watching().await.unwrap_err(),
        WatchError::NotConnected
    ));
    let cluster = FakeCluster::new();
    w.connect_with(cluster.clone(), ClusterInfo::default())
        .await;
    cluster.sync(&Snapshot::default());
    w.begin_watching().await.unwrap();
    assert!(matches!(
        w.begin_watching().await.unwrap_err(),
        WatchError::AlreadyWatching
    ));
    w.stop().await;
    w.stop().await; // idempotent
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_during_a_partial_sync_are_delivered_after_the_snapshot() {
    let (w, rx, _fan) = watcher();
    let cluster = FakeCluster::new();
    w.connect_with(cluster.clone(), ClusterInfo::default())
        .await;
    // Pods finish listing first and already see an Apply while the other
    // kinds are still listing.
    cluster.send_pod(Event::Init);
    cluster.send_pod(Event::InitDone);
    cluster.send_pod(Event::Apply(pod("a", "early", "e", labels(&[]))));
    let c = cluster.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let rest = Snapshot {
            pods: vec![pod("ignored", "x", "x", labels(&[]))], // pods already synced
            ..Default::default()
        };
        c.send_deployment(Event::Init);
        c.send_deployment(Event::InitDone);
        c.send_service(Event::Init);
        c.send_service(Event::InitDone);
        c.send_ingress(Event::Init);
        c.send_ingress(Event::InitDone);
        c.send_namespace(Event::Init);
        c.send_namespace(Event::InitDone);
        drop(rest);
    });
    w.begin_watching().await.unwrap();
    assert!(matches!(recv(&rx, "ns"), Watched::NamespaceList(_)));
    assert!(matches!(recv(&rx, "full"), Watched::FullResourceList(_)));
    assert!(
        matches!(recv(&rx, "buffered apply"), Watched::Pod(p) if p.metadata.name.as_deref() == Some("early"))
    );
    w.stop().await;
    drop(Arc::clone(&cluster));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_then_watch_another_cluster() {
    let (w, rx, fan) = watcher();
    let first = FakeCluster::new();
    w.connect_with(
        first.clone(),
        ClusterInfo {
            name: Some("a".into()),
            ..Default::default()
        },
    )
    .await;
    w.set_cluster_id(1);
    let c = first.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        c.sync(&snapshot());
    });
    w.begin_watching().await.expect("first sync");
    let _ = recv(&rx, "primary");
    let _ = recv(&rx, "primary");
    let _ = recv(&fan, "fanout");
    let _ = recv(&fan, "fanout");

    w.stop().await;
    assert!(!w.is_running());
    assert_eq!(w.get_cluster_name(), None, "identity cleared");
    assert_eq!(w.get_cluster_uuid(), None);
    assert_eq!(
        w.get_cluster_id_arc()
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );

    // Same watcher, same channels, a different cluster.
    let second = FakeCluster::new();
    w.connect_with(
        second.clone(),
        ClusterInfo {
            name: Some("b".into()),
            ..Default::default()
        },
    )
    .await;
    w.set_cluster_id(2);
    let c = second.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        c.sync(&Snapshot {
            namespaces: vec![namespace("other", labels(&[]))],
            ..Default::default()
        });
    });
    w.begin_watching().await.expect("second sync");
    assert_eq!(w.get_cluster_name().as_deref(), Some("b"));
    for (name, ch) in [("primary", &rx), ("fanout", &fan)] {
        assert!(matches!(recv(ch, name), Watched::NamespaceList(_)));
        match recv(ch, name) {
            Watched::FullResourceList(list) => assert_eq!(list.cluster_id, 2),
            other => panic!("{name}: expected FullResourceList, got {other:?}"),
        }
    }
    // Events from the first cluster no longer flow.
    first.send_pod(Event::Apply(pod("shop", "late", "u9", labels(&[]))));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(rx.try_recv().is_err(), "old session is gone");
    w.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_primary_receiver_ends_the_session() {
    let (w, rx, _fan) = watcher();
    let cluster = FakeCluster::new();
    w.connect_with(cluster.clone(), ClusterInfo::default())
        .await;
    let c = cluster.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        c.sync(&snapshot());
    });
    w.begin_watching().await.expect("sync completes");
    assert!(w.is_running());

    // Nobody is listening any more: the next event can't be delivered, the
    // watch task ends, and with it the health pinger.
    drop(rx);
    cluster.send_pod(Event::Apply(pod(
        "shop",
        "api-2",
        "u2",
        labels(&[("app", "api")]),
    )));
    assert!(
        wait_until(Duration::from_secs(5), || !w.is_running()),
        "is_running() must turn false once the watch task has ended"
    );
    let pings = cluster.ping_count();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        cluster.ping_count(),
        pings,
        "health pinger stopped with the watch"
    );

    // stop() is still safe, and a stale session is not "already watching".
    w.stop().await;
    assert!(!w.is_running());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_share_the_reflector_stores_copy() {
    // No fan-out channel, so the only other holder of a pod is the
    // watcher's reflector store.
    let (tx, rx) = crossbeam_channel::unbounded();
    let w = KubeWatcher::new(tx);
    let cluster = FakeCluster::new();
    w.connect_with(cluster.clone(), ClusterInfo::default())
        .await;
    let c = cluster.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        c.sync(&snapshot());
    });
    w.begin_watching().await.expect("sync completes");
    recv(&rx, "namespaces");
    match recv(&rx, "full list") {
        Watched::FullResourceList(list) => {
            for r in &list.resources {
                if let KubeResource::Pod(p) = r {
                    assert_eq!(Arc::strong_count(p), 2, "snapshot shares the store's Arc");
                }
            }
        }
        other => panic!("expected FullResourceList, got {other:?}"),
    }

    cluster.send_pod(Event::Apply(pod(
        "shop",
        "api-2",
        "u2",
        labels(&[("app", "api")]),
    )));
    match recv(&rx, "pod apply") {
        Watched::Pod(p) => {
            assert_eq!(Arc::strong_count(&p), 2, "event shares the store's Arc")
        }
        other => panic!("expected Pod, got {other:?}"),
    }
    w.stop().await;
}
