//! Discovery → probe → connect against a real cluster.
//!
//! ```sh
//! VALKEN_TEST_KUBECONFIG=/path/to/kubeconfig \
//! VALKEN_TEST_KUBECONFIG_LIMITED=/path/to/limited-kubeconfig \
//!   cargo test --no-default-features --test kubeconfig_live -- --ignored
//! ```

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use valken_core::k8s::kubeconfig::probe::{probe, Need};
use valken_core::k8s::kubeconfig::{discover, ExecEnv, KubeconfigSources, SourceKind};
use valken_core::k8s::{ConnectStep, KubeWatcher};

fn sources(var: &str) -> Option<KubeconfigSources> {
    let path = PathBuf::from(std::env::var_os(var)?);
    Some(KubeconfigSources {
        files: vec![(path, SourceKind::Env)],
    })
}

#[tokio::test]
#[ignore = "needs VALKEN_TEST_KUBECONFIG"]
async fn real_cluster_probe_and_connect() {
    let Some(sources) = sources("VALKEN_TEST_KUBECONFIG") else {
        return;
    };
    let d = discover(&sources);
    let ctx = d
        .context(d.current_context.as_deref().unwrap())
        .unwrap()
        .clone();
    let exec = ExecEnv::default();
    let timeout = Duration::from_secs(5);

    let report = probe(
        d.client_config(&ctx.name, &exec, timeout).await.unwrap(),
        &ctx.auth,
        &exec,
        timeout,
    )
    .await
    .unwrap();
    assert!(report.server_version.starts_with('v'), "{report:?}");
    assert!(report.minor.is_some());
    assert_eq!(report.access.len(), Need::ALL.len());
    assert_eq!(report.denied().count(), 0, "admin kubeconfig: {report:?}");

    let (tx, _rx) = crossbeam_channel::unbounded();
    let watcher = KubeWatcher::new(tx);
    let steps = Arc::new(Mutex::new(Vec::new()));
    let seen = steps.clone();
    watcher
        .connect_context(
            d.client_config(&ctx.name, &exec, timeout).await.unwrap(),
            ctx.name.clone(),
            &ctx.auth,
            &exec,
            &move |s| seen.lock().unwrap().push(s),
        )
        .await
        .unwrap();
    assert_eq!(
        *steps.lock().unwrap(),
        vec![
            ConnectStep::ReadConfig,
            ConnectStep::ReachApi,
            ConnectStep::Authenticate,
            ConnectStep::Discover
        ]
    );
    assert_eq!(watcher.get_cluster_name(), Some(ctx.name));
    assert!(watcher.get_cluster_uuid().is_some());
}

#[tokio::test]
#[ignore = "needs VALKEN_TEST_KUBECONFIG_LIMITED"]
async fn real_cluster_probe_reports_denied_verbs() {
    let Some(sources) = sources("VALKEN_TEST_KUBECONFIG_LIMITED") else {
        return;
    };
    let d = discover(&sources);
    let ctx = d
        .context(d.current_context.as_deref().unwrap())
        .unwrap()
        .clone();
    let exec = ExecEnv::default();
    let report = probe(
        d.client_config(&ctx.name, &exec, Duration::from_secs(5))
            .await
            .unwrap(),
        &ctx.auth,
        &exec,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let denied: Vec<_> = report.denied().collect();
    assert!(denied.contains(&Need::ListNodes), "{report:?}");
}
