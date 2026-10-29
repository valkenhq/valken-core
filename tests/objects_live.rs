//! `get_object` against a real cluster.
//!
//! ```sh
//! VALKEN_TEST_KUBECONFIG=/path/to/kubeconfig \
//!   cargo test --no-default-features --test objects_live -- --ignored
//! ```

use valken_core::k8s::objects::{get_object, list_objects, ObjectKey};
use valken_core::k8s::ObjectError;

async fn client() -> Option<kube::Client> {
    let path = std::env::var_os("VALKEN_TEST_KUBECONFIG")?;
    let kubeconfig = kube::config::Kubeconfig::read_from(path).ok()?;
    let config = kube::Config::from_custom_kubeconfig(kubeconfig, &Default::default())
        .await
        .ok()?;
    kube::Client::try_from(config).ok()
}

fn key(api_version: &str, kind: &str, namespace: Option<&str>, name: &str) -> ObjectKey {
    ObjectKey {
        api_version: api_version.into(),
        kind: kind.into(),
        namespace: namespace.map(str::to_string),
        name: name.into(),
    }
}

#[tokio::test]
#[ignore = "needs VALKEN_TEST_KUBECONFIG"]
async fn reads_namespaced_cluster_scoped_and_missing_objects() {
    let Some(client) = client().await else {
        eprintln!("skipped: VALKEN_TEST_KUBECONFIG unset or unreadable");
        return;
    };
    // Core group, namespaced, any case for the kind.
    let svc = get_object(
        &client,
        "live",
        &key("v1", "service", Some("default"), "kubernetes"),
    )
    .await
    .unwrap();
    assert_eq!(svc["kind"], "Service");
    assert_eq!(svc["metadata"]["name"], "kubernetes");
    assert!(svc["spec"].is_object(), "the whole object comes back");
    // Named group.
    let dep = get_object(
        &client,
        "live",
        &key("apps/v1", "Deployment", Some("kube-system"), "coredns"),
    )
    .await
    .unwrap();
    assert_eq!(dep["apiVersion"], "apps/v1");
    assert!(dep["metadata"]["managedFields"].is_array());
    // Cluster-scoped (the namespace argument is ignored).
    let ns = get_object(
        &client,
        "live",
        &key("v1", "namespace", Some("kube-system"), "kube-system"),
    )
    .await
    .unwrap();
    assert_eq!(ns["kind"], "Namespace");
    eprintln!(
        "read {} / {} / {}",
        svc["metadata"]["name"], dep["metadata"]["name"], ns["metadata"]["name"]
    );
    // Missing and unknown.
    assert!(matches!(
        get_object(
            &client,
            "live",
            &key("v1", "pod", Some("default"), "no-such-pod")
        )
        .await,
        Err(ObjectError::NotFound)
    ));
    assert!(matches!(
        get_object(&client, "live", &key("v1", "gizmo", Some("default"), "x")).await,
        Err(ObjectError::UnknownKind { .. })
    ));
}

#[tokio::test]
#[ignore = "needs VALKEN_TEST_KUBECONFIG"]
async fn lists_objects() {
    let Some(client) = client().await else {
        eprintln!("skipped: VALKEN_TEST_KUBECONFIG unset or unreadable");
        return;
    };
    let services = list_objects(&client, "live", "v1", "Service", Some("default"))
        .await
        .unwrap();
    assert!(services
        .iter()
        .any(|s| s["metadata"]["name"] == "kubernetes"));
    // A kind the cluster may have none of still lists fine.
    let pdbs = list_objects(
        &client,
        "live",
        "policy/v1",
        "PodDisruptionBudget",
        Some("default"),
    )
    .await
    .unwrap();
    eprintln!("listed {} services, {} pdbs", services.len(), pdbs.len());
}
