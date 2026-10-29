//! Logs and events against a real cluster.
//!
//! ```sh
//! VALKEN_TEST_KUBECONFIG=/path/to/kubeconfig \
//!   cargo test --no-default-features --test logs_events_live -- --ignored --nocapture
//! ```

use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use valken_core::k8s::events::watch_events;
use valken_core::k8s::logs::{open_logs, LogRequest};
use valken_core::k8s::LogError;

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
async fn reads_logs_with_timestamps_and_reports_missing_previous() {
    let Some(client) = client().await else {
        eprintln!("skipped: VALKEN_TEST_KUBECONFIG unset or unreadable");
        return;
    };
    let pods: kube::Api<Pod> = kube::Api::namespaced(client.clone(), "kube-system");
    let pod = pods
        .list(&ListParams::default().labels("k8s-app=kube-dns"))
        .await
        .unwrap()
        .items
        .remove(0);
    let name = pod.metadata.name.clone().unwrap();
    let req = LogRequest {
        namespace: "kube-system".into(),
        pod: name.clone(),
        container: "coredns".into(),
        tail_lines: Some(5),
        ..Default::default()
    };
    let lines: Vec<_> = open_logs(&client, &req).await.unwrap().collect().await;
    assert!(!lines.is_empty() && lines.len() <= 5);
    let first = lines[0].as_ref().unwrap();
    assert!(
        first.ts.is_some(),
        "timestamps=true gives every line a time"
    );
    eprintln!(
        "read {} lines from {name}; first: {}",
        lines.len(),
        first.text
    );
    let restarts = pod
        .status
        .and_then(|s| s.container_statuses)
        .map_or(0, |c| c[0].restart_count);
    if restarts == 0 {
        let prev = LogRequest {
            previous: true,
            ..req
        };
        assert!(matches!(
            open_logs(&client, &prev).await,
            Err(LogError::NoPrevious)
        ));
    }
}

#[tokio::test]
#[ignore = "needs VALKEN_TEST_KUBECONFIG"]
async fn watches_namespace_events() {
    let Some(client) = client().await else {
        eprintln!("skipped: VALKEN_TEST_KUBECONFIG unset or unreadable");
        return;
    };
    // `sandbox` has crash-looping pods in the test cluster, so events exist.
    let mut stream = watch_events(&client, "sandbox");
    let first = tokio::time::timeout(Duration::from_secs(20), stream.next())
        .await
        .expect("an event within 20 s")
        .unwrap()
        .unwrap();
    assert!(!first.reason.is_empty() && !first.object_name.is_empty());
    eprintln!(
        "event: {} {} {} ×{}",
        first.kind, first.reason, first.object_name, first.count
    );
}
