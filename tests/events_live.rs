//! The cluster-wide event and config watches against a real cluster.
//!
//! ```sh
//! VALKEN_TEST_KUBECONFIG=/path/to/kubeconfig \
//!   cargo test --no-default-features --test events_live -- --ignored --nocapture
//! ```

use std::time::Duration;

use futures::StreamExt;
use kube::runtime::watcher::Event;
use valken_core::k8s::configs::ConfigKind;
use valken_core::k8s::events::{EventSource, KubeEventSource};

async fn client() -> Option<kube::Client> {
    let path = std::env::var_os("VALKEN_TEST_KUBECONFIG")?;
    let kubeconfig = kube::config::Kubeconfig::read_from(path).ok()?;
    let config = kube::Config::from_custom_kubeconfig(kubeconfig, &Default::default())
        .await
        .ok()?;
    kube::Client::try_from(config).ok()
}

#[tokio::test]
#[ignore = "needs VALKEN_TEST_KUBECONFIG"]
async fn lists_cluster_events_then_reports_sync() {
    let Some(client) = client().await else {
        eprintln!("skipped: VALKEN_TEST_KUBECONFIG unset or unreadable");
        return;
    };
    let source = KubeEventSource::new(client);
    let mut stream = source.watch_events();
    let mut listed = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let item = tokio::time::timeout_at(deadline, stream.next())
            .await
            .expect("sync within 30 s")
            .unwrap()
            .unwrap();
        match item {
            Event::Init => {}
            Event::InitApply(r) => {
                assert!(!r.uid.is_empty() && !r.namespace.is_empty());
                listed += 1;
            }
            Event::InitDone => break,
            other => panic!("unexpected before sync: {other:?}"),
        }
    }
    eprintln!("listed {listed} events");
}

#[tokio::test]
#[ignore = "needs VALKEN_TEST_KUBECONFIG"]
async fn config_maps_arrive_without_values() {
    let Some(client) = client().await else {
        eprintln!("skipped: VALKEN_TEST_KUBECONFIG unset or unreadable");
        return;
    };
    let source = KubeEventSource::new(client);
    let mut stream = source.watch_config_maps();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let item = tokio::time::timeout_at(deadline, stream.next())
            .await
            .expect("a configmap within 30 s")
            .unwrap()
            .unwrap();
        if let Event::InitApply(m) = item {
            assert_eq!(m.kind, ConfigKind::ConfigMap);
            // kube-root-ca.crt exists in every namespace and has one key.
            if m.name == "kube-root-ca.crt" {
                assert_eq!(m.key_names().collect::<Vec<_>>(), ["ca.crt"]);
                assert_eq!(m.keys[0].digest.len(), 64);
                eprintln!("{}/{} ok", m.namespace, m.label());
                return;
            }
        }
    }
}
