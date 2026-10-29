//! Writes (always dry-run here), one-shot events and rollout history against
//! a real cluster.  Nothing is persisted: every write uses `dryRun=All`.
//!
//! ```sh
//! VALKEN_TEST_KUBECONFIG=/path/to/kubeconfig \
//!   cargo test --no-default-features --test write_live -- --ignored
//! ```

use serde_json::json;
use valken_core::k8s::events::list_events;
use valken_core::k8s::objects::{get_object, ObjectKey};
use valken_core::k8s::rollout::history;
use valken_core::k8s::write::{self, Mode, PatchKind};
use valken_core::k8s::ObjectError;

async fn client() -> Option<kube::Client> {
    let path = std::env::var_os("VALKEN_TEST_KUBECONFIG")?;
    let kubeconfig = kube::config::Kubeconfig::read_from(path).ok()?;
    let config = kube::Config::from_custom_kubeconfig(kubeconfig, &Default::default())
        .await
        .ok()?;
    kube::Client::try_from(config).ok()
}

fn coredns() -> ObjectKey {
    ObjectKey {
        api_version: "apps/v1".into(),
        kind: "Deployment".into(),
        namespace: Some("kube-system".into()),
        name: "coredns".into(),
    }
}

#[tokio::test]
#[ignore = "needs VALKEN_TEST_KUBECONFIG"]
async fn dry_run_patch_create_delete_change_nothing() {
    let Some(client) = client().await else {
        eprintln!("skipped: VALKEN_TEST_KUBECONFIG unset or unreadable");
        return;
    };
    let before = get_object(&client, "live", &coredns()).await.unwrap();
    let replicas = before["spec"]["replicas"].as_u64().unwrap() as u32;

    let after = write::patch(
        &client,
        "live",
        &coredns(),
        &write::scale_patch(replicas + 3),
        PatchKind::Strategic,
        Mode::DryRun,
    )
    .await
    .unwrap();
    assert_eq!(
        after["spec"]["replicas"],
        replicas + 3,
        "dry run returns the would-be object"
    );

    let restart = write::restart_patch(chrono::Utc::now());
    write::patch(
        &client,
        "live",
        &coredns(),
        &restart,
        PatchKind::Strategic,
        Mode::DryRun,
    )
    .await
    .unwrap();

    let still = get_object(&client, "live", &coredns()).await.unwrap();
    assert_eq!(still["spec"]["replicas"], replicas, "nothing persisted");
    assert_eq!(
        still["metadata"]["resourceVersion"],
        before["metadata"]["resourceVersion"]
    );

    let cm = json!({"apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": "valken-dry-run", "namespace": "default"}, "data": {"a": "b"}});
    let created = write::create(&client, "live", &cm, Mode::DryRun)
        .await
        .unwrap();
    assert_eq!(created["data"]["a"], "b");
    let missing = ObjectKey {
        api_version: "v1".into(),
        kind: "ConfigMap".into(),
        namespace: Some("default".into()),
        name: "valken-dry-run".into(),
    };
    assert!(matches!(
        get_object(&client, "live", &missing).await,
        Err(ObjectError::NotFound)
    ));

    let pods =
        valken_core::k8s::objects::list_objects(&client, "live", "v1", "Pod", Some("kube-system"))
            .await
            .unwrap();
    let pod = pods[0]["metadata"]["name"].as_str().unwrap().to_string();
    let key = ObjectKey {
        api_version: "v1".into(),
        kind: "Pod".into(),
        namespace: Some("kube-system".into()),
        name: pod,
    };
    write::delete(&client, "live", &key, Mode::DryRun)
        .await
        .unwrap();
    get_object(&client, "live", &key)
        .await
        .expect("pod survives a dry-run delete");
}

#[tokio::test]
#[ignore = "needs VALKEN_TEST_KUBECONFIG"]
async fn stale_resource_version_conflicts() {
    let Some(client) = client().await else {
        eprintln!("skipped: VALKEN_TEST_KUBECONFIG unset or unreadable");
        return;
    };
    let patch = write::with_precondition(write::scale_patch(2), "1");
    let err = write::patch(
        &client,
        "live",
        &coredns(),
        &patch,
        PatchKind::Strategic,
        Mode::DryRun,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ObjectError::Conflict(_)), "{err:?}");
}

#[tokio::test]
#[ignore = "needs VALKEN_TEST_KUBECONFIG"]
async fn events_and_rollout_history() {
    let Some(client) = client().await else {
        eprintln!("skipped: VALKEN_TEST_KUBECONFIG unset or unreadable");
        return;
    };
    let all = list_events(&client, "kube-system", None).await.unwrap();
    let windows = all.windows(2).all(|w| w[0].time >= w[1].time);
    assert!(windows, "newest first");
    if let Some(e) = all.first() {
        let one = list_events(
            &client,
            "kube-system",
            Some((&e.object_kind, &e.object_name)),
        )
        .await
        .unwrap();
        assert!(one.iter().all(|r| r.object_name == e.object_name));
    }
    let revs = history(&client, "kube-system", "coredns").await.unwrap();
    assert!(!revs.is_empty());
    assert!(revs.iter().any(|r| r.current));
    assert!(revs[0].images.iter().any(|i| i.contains("coredns")));
}
