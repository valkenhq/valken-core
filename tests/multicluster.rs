//! Several [`KubeWatcher`]s running side by side (one per open cluster), and
//! per-watcher connection health.  No API server, no Godot.

mod support;

use std::time::Duration;

use kube::runtime::watcher::Event;
use support::*;
use valken_core::k8s::kubeconfig::probe::ProbeError;
use valken_core::k8s::{ClusterInfo, HealthSettings, KubeWatcher, LinkState, Watched};

/// Fast pinger for tests: a long interval, so any ping inside a test's
/// timeout window after the first one must have been a nudge.
fn health_settings(interval: Duration) -> HealthSettings {
    HealthSettings {
        interval,
        timeout: Duration::from_secs(1),
        min_gap: Duration::from_millis(20),
    }
}

async fn start(
    id: u32,
    name: &str,
    ns: &str,
    settings: HealthSettings,
) -> (
    KubeWatcher,
    crossbeam_channel::Receiver<Watched>,
    std::sync::Arc<FakeCluster>,
) {
    let (tx, rx) = crossbeam_channel::unbounded();
    let w = KubeWatcher::new(tx).with_health_settings(settings);
    let cluster = FakeCluster::new();
    w.connect_with(
        cluster.clone(),
        ClusterInfo {
            name: Some(name.into()),
            ..Default::default()
        },
    )
    .await;
    w.set_cluster_id(id);
    cluster.sync(&Snapshot {
        namespaces: vec![namespace(ns, labels(&[]))],
        pods: vec![pod(ns, "p", &format!("{name}-u"), labels(&[]))],
        ..Default::default()
    });
    w.begin_watching().await.expect("sync");
    (w, rx, cluster)
}

/// Poll `f` until it holds or `within` elapses.
async fn eventually(within: Duration, mut f: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + within;
    while tokio::time::Instant::now() < deadline {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    f()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_watchers_are_isolated_and_stop_independently() {
    let slow = health_settings(Duration::from_secs(60));
    let (a, rx_a, ca) = start(1, "a", "shop", slow).await;
    let (b, rx_b, cb) = start(2, "b", "billing", slow).await;

    for (rx, id, ns) in [(&rx_a, 1, "shop"), (&rx_b, 2, "billing")] {
        match recv(rx, "namespaces") {
            Watched::NamespaceList(infos) => assert_eq!(infos[0].name, ns),
            other => panic!("expected NamespaceList, got {other:?}"),
        }
        match recv(rx, "full list") {
            Watched::FullResourceList(list) => assert_eq!(list.cluster_id, id),
            other => panic!("expected FullResourceList, got {other:?}"),
        }
    }
    assert_eq!(a.get_cluster_name().as_deref(), Some("a"));
    assert_eq!(b.get_cluster_name().as_deref(), Some("b"));

    // Events stay on their own watcher's channel.
    cb.send_pod(Event::Apply(pod("billing", "q", "b-2", labels(&[]))));
    assert!(matches!(recv(&rx_b, "b pod"), Watched::Pod(_)));
    assert!(rx_a.recv_timeout(Duration::from_millis(100)).is_err());

    // Stopping one leaves the other running and healthy.
    a.stop().await;
    assert!(!a.is_running());
    assert_eq!(a.health().state, LinkState::Connecting, "health reset");
    assert!(b.is_running());
    assert_eq!(b.get_cluster_name().as_deref(), Some("b"));
    assert!(
        eventually(Duration::from_secs(2), || b.health().state
            == LinkState::Live)
        .await
    );
    cb.send_pod(Event::Apply(pod("billing", "r", "b-3", labels(&[]))));
    assert!(matches!(
        recv(&rx_b, "b pod after a stopped"),
        Watched::Pod(_)
    ));
    ca.send_pod(Event::Apply(pod("shop", "late", "a-9", labels(&[]))));
    assert!(rx_a.recv_timeout(Duration::from_millis(100)).is_err());

    b.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_with_has_no_client() {
    let (w, _rx, _c) = start(3, "fake", "x", health_settings(Duration::from_secs(60))).await;
    assert!(w.client().is_none(), "fake sources are not an API server");
    w.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_goes_live_then_expired_on_a_nudge_then_recovers() {
    // Interval far longer than the test: every ping after the first one is
    // caused by a watch-error nudge.
    let (w, _rx, cluster) = start(5, "h", "x", health_settings(Duration::from_secs(600))).await;
    let mut sub = w.subscribe_health();

    assert!(
        eventually(Duration::from_secs(2), || w.health().state
            == LinkState::Live)
        .await
    );
    let live = w.health();
    assert!(live.latency.is_some());
    assert!(live.last_ok.is_some());
    assert_eq!(cluster.ping_count(), 1);

    // The token expires: pings now fail, and the watch reports a 401.
    cluster.set_ping_error(Some(ProbeError::Unauthorized("token expired".into())));
    sub.mark_unchanged();
    cluster.send_pod_unauthorized();
    assert!(
        eventually(Duration::from_secs(2), || w.health().state
            == LinkState::AuthExpired)
        .await,
        "a failing watch triggers an early ping"
    );
    assert!(sub.has_changed().unwrap(), "subscribers see the change");
    let expired = w.health();
    assert_eq!(
        expired.last_ok, live.last_ok,
        "last answer is kept for 'stale · Nm'"
    );
    assert!(expired.detail.unwrap().contains("token expired"));

    // A watch error that says nothing about the link does not cause a ping.
    let before = cluster.ping_count();
    cluster.send_pod_error();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(cluster.ping_count(), before);

    // Re-authenticated: the next nudge finds the cluster live again.
    cluster.set_ping_error(None);
    cluster.send_pod_unauthorized();
    assert!(
        eventually(Duration::from_secs(2), || w.health().state
            == LinkState::Live)
        .await
    );

    w.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nudges_are_rate_limited() {
    let settings = HealthSettings {
        interval: Duration::from_secs(600),
        timeout: Duration::from_secs(1),
        min_gap: Duration::from_millis(300),
    };
    let (w, _rx, cluster) = start(6, "r", "x", settings).await;
    assert!(eventually(Duration::from_secs(2), || cluster.ping_count() == 1).await);

    // A burst of failures inside one gap yields at most one extra ping.
    for _ in 0..20 {
        cluster.send_pod_unauthorized();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(cluster.ping_count() <= 2, "pings: {}", cluster.ping_count());

    w.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_pings_update_unreachable_without_watch_errors() {
    let (w, _rx, cluster) = start(7, "p", "x", health_settings(Duration::from_millis(50))).await;
    assert!(
        eventually(Duration::from_secs(2), || w.health().state
            == LinkState::Live)
        .await
    );
    cluster.set_ping_error(Some(ProbeError::Unreachable("connection refused".into())));
    assert!(
        eventually(Duration::from_secs(2), || w.health().state
            == LinkState::Unreachable)
        .await
    );
    w.stop().await;
}
