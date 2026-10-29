//! [`SecurityWatch`] driven by an in-memory [`SecuritySource`].

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};
use futures::FutureExt;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{Pod, Service};
use k8s_openapi::api::networking::v1::{Ingress, NetworkPolicy};
use kube::api::DynamicObject;
use kube::core::Status;
use kube::runtime::watcher::{self, Event};
use serde_json::json;
use tokio::sync::mpsc;

use valken_core::k8s::security::{
    KindAvailability, RegistryCredential, SecurityInventory, SecurityKind, SecuritySource,
    SecurityWatch, WorkloadKind, REDACTED,
};
use valken_core::k8s::WatchStream;

type Res<K> = Result<Event<K>, watcher::Error>;

struct Feed<K> {
    tx: mpsc::UnboundedSender<Res<K>>,
    rx: Mutex<Option<mpsc::UnboundedReceiver<Res<K>>>>,
}

impl<K: Send + 'static> Feed<K> {
    fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            tx,
            rx: Mutex::new(Some(rx)),
        }
    }
    fn stream(&self) -> BoxStream<'static, Res<K>> {
        let rx = self.rx.lock().unwrap().take().expect("watched once");
        stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|e| (e, rx)) }).boxed()
    }
    fn list(&self, objects: Vec<K>) {
        let _ = self.tx.send(Ok(Event::Init));
        for o in objects {
            let _ = self.tx.send(Ok(Event::InitApply(o)));
        }
        let _ = self.tx.send(Ok(Event::InitDone));
    }
    fn send(&self, event: Event<K>) {
        let _ = self.tx.send(Ok(event));
    }
    fn fail(&self, code: u16) {
        let status = Status::failure("nope", "Nope").with_code(code);
        let _ = self
            .tx
            .send(Err(watcher::Error::InitialListFailed(kube::Error::Api(
                Box::new(status),
            ))));
    }
}

struct Fake {
    pods: Feed<Pod>,
    replica_sets: Feed<ReplicaSet>,
    deployments: Feed<Deployment>,
    stateful_sets: Feed<StatefulSet>,
    daemon_sets: Feed<DaemonSet>,
    jobs: Feed<Job>,
    cron_jobs: Feed<CronJob>,
    services: Feed<Service>,
    ingresses: Feed<Ingress>,
    network_policies: Feed<NetworkPolicy>,
    http_routes: Feed<DynamicObject>,
}

impl SecuritySource for Fake {
    fn watch_pods(&self) -> WatchStream<Pod> {
        self.pods.stream()
    }
    fn watch_replica_sets(&self) -> WatchStream<ReplicaSet> {
        self.replica_sets.stream()
    }
    fn watch_deployments(&self) -> WatchStream<Deployment> {
        self.deployments.stream()
    }
    fn watch_stateful_sets(&self) -> WatchStream<StatefulSet> {
        self.stateful_sets.stream()
    }
    fn watch_daemon_sets(&self) -> WatchStream<DaemonSet> {
        self.daemon_sets.stream()
    }
    fn watch_jobs(&self) -> WatchStream<Job> {
        self.jobs.stream()
    }
    fn watch_cron_jobs(&self) -> WatchStream<CronJob> {
        self.cron_jobs.stream()
    }
    fn watch_services(&self) -> WatchStream<Service> {
        self.services.stream()
    }
    fn watch_ingresses(&self) -> WatchStream<Ingress> {
        self.ingresses.stream()
    }
    fn watch_network_policies(&self) -> WatchStream<NetworkPolicy> {
        self.network_policies.stream()
    }
    fn watch_http_routes(&self) -> WatchStream<DynamicObject> {
        self.http_routes.stream()
    }
    fn pull_credentials(
        &self,
        _: &str,
        _: &str,
    ) -> BoxFuture<'static, Result<Vec<RegistryCredential>, kube::Error>> {
        async { Ok(Vec::new()) }.boxed()
    }
}

fn fake() -> Arc<Fake> {
    Arc::new(Fake {
        pods: Feed::new(),
        replica_sets: Feed::new(),
        deployments: Feed::new(),
        stateful_sets: Feed::new(),
        daemon_sets: Feed::new(),
        jobs: Feed::new(),
        cron_jobs: Feed::new(),
        services: Feed::new(),
        ingresses: Feed::new(),
        network_policies: Feed::new(),
        http_routes: Feed::new(),
    })
}

fn obj<K: serde::de::DeserializeOwned>(v: serde_json::Value) -> K {
    serde_json::from_value(v).unwrap()
}

fn owned_pod(name: &str, owner_kind: &str, owner: &str, owner_uid: &str) -> Pod {
    obj(json!({
        "metadata": {"namespace": "lockbox", "name": name, "uid": format!("uid-{name}"),
            "labels": {"app": "web"},
            "ownerReferences": [{"apiVersion": "apps/v1", "kind": owner_kind, "name": owner, "uid": owner_uid, "controller": true}]},
        "spec": {"containers": [{"name": "web", "image": "ghcr.io/eldr/lockbox:1.4",
            "env": [{"name": "TOKEN", "value": "hunter2"}]}]},
    }))
}

fn recv(rx: &crossbeam_channel::Receiver<Arc<SecurityInventory>>) -> Arc<SecurityInventory> {
    rx.recv_timeout(Duration::from_secs(10)).expect("snapshot")
}

/// List every kind except HTTPRoute (404) and NetworkPolicy (403).
fn initial_sync(f: &Fake) {
    f.pods.list(vec![
        owned_pod("web-1", "ReplicaSet", "web-abc", "rs-1"),
        owned_pod("backup-1", "Job", "backup-123", "job-1"),
        obj(
            json!({"metadata": {"namespace": "lockbox", "name": "debug", "uid": "uid-debug"},
            "spec": {"containers": [{"name": "c", "image": "busybox",
                "env": [{"name": "PASSWORD", "value": "hunter2"}]}]}}),
        ),
    ]);
    f.replica_sets.list(vec![obj(json!({"metadata": {"namespace": "lockbox", "name": "web-abc", "uid": "rs-1",
        "ownerReferences": [{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "dep-1", "controller": true}]}}))]);
    f.deployments.list(vec![obj(
        json!({"metadata": {"namespace": "lockbox", "name": "web", "uid": "dep-1",
            "managedFields": [{"manager": "kubectl"}]},
        "spec": {"selector": {"matchLabels": {"app": "web"}}, "template": {"spec": {"containers": [
            {"name": "web", "image": "x", "env": [{"name": "TOKEN", "value": "hunter2"}]}]}}},
        "status": {"replicas": 1}}),
    )]);
    f.jobs.list(vec![obj(json!({"metadata": {"namespace": "lockbox", "name": "backup-123", "uid": "job-1",
        "ownerReferences": [{"apiVersion": "batch/v1", "kind": "CronJob", "name": "backup", "uid": "cron-1", "controller": true}]},
        "spec": {"template": {"spec": {"containers": [], "restartPolicy": "Never"}}}}))]);
    f.cron_jobs.list(vec![obj(json!({"metadata": {"namespace": "lockbox", "name": "backup", "uid": "cron-1"},
        "spec": {"schedule": "@daily", "jobTemplate": {"spec": {"template": {"spec": {"containers": []}}}}}}))]);
    f.stateful_sets.list(vec![]);
    f.daemon_sets.list(vec![]);
    f.services.list(vec![obj(
        json!({"metadata": {"namespace": "lockbox", "name": "lockbox-web"},
        "spec": {"type": "LoadBalancer", "selector": {"app": "web"}, "ports": [{"port": 443}]}}),
    )]);
    f.ingresses.list(vec![]);
    f.http_routes.fail(404);
    f.network_policies.fail(403);
}

#[test]
fn publishes_a_complete_snapshot_with_unavailable_kinds_marked() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let f = fake();
    let (tx, rx) = crossbeam_channel::unbounded();
    let watch = SecurityWatch::spawn(rt.handle(), f.clone(), 7, tx);
    initial_sync(&f);

    let inv = recv(&rx);
    assert_eq!(inv.cluster_id, 7);
    assert_eq!(inv.generation, 1);
    assert_eq!(
        inv.availability[&SecurityKind::HttpRoute],
        KindAvailability::NotInstalled
    );
    assert_eq!(
        inv.availability[&SecurityKind::NetworkPolicy],
        KindAvailability::Forbidden
    );
    assert!(inv.is_ready(SecurityKind::Pod) && inv.is_ready(SecurityKind::Service));
    assert!(
        !inv.is_ready(SecurityKind::NetworkPolicy),
        "forbidden is not 'no policies'"
    );
    assert_eq!(
        (inv.pods.len(), inv.workloads.len(), inv.services.len()),
        (3, 3, 1)
    );

    let pod = |n: &str| inv.pods.iter().find(|p| p.object.name == n).unwrap();
    let web = inv.workload_of(pod("web-1"));
    assert_eq!(
        (web.kind, web.object.name.as_str(), web.object.uid.as_str()),
        (WorkloadKind::Deployment, "web", "dep-1")
    );
    assert_eq!(inv.workload_of(pod("backup-1")).kind, WorkloadKind::CronJob);
    let debug = inv.workload_of(pod("debug"));
    assert_eq!(
        (debug.kind, debug.object.name.as_str()),
        (WorkloadKind::Pod, "debug")
    );

    // Nothing secret survives into the inventory.
    let text = format!("{:?}", inv);
    assert!(!text.contains("hunter2"), "env value leaked");
    assert!(!text.contains("managedFields"));
    assert!(pod("debug")
        .manifest
        .as_ref()
        .unwrap()
        .to_string()
        .contains(REDACTED));
    assert_eq!(watch.latest().unwrap().generation, 1);
}

#[test]
fn changes_and_relists_publish_new_snapshots() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let f = fake();
    let (tx, rx) = crossbeam_channel::unbounded();
    let _watch = SecurityWatch::spawn(rt.handle(), f.clone(), 1, tx);
    initial_sync(&f);
    recv(&rx);

    // A burst of changes collapses into one snapshot.
    for i in 0..5 {
        f.pods.send(Event::Apply(owned_pod(
            &format!("web-new-{i}"),
            "ReplicaSet",
            "web-abc",
            "rs-1",
        )));
    }
    let inv = recv(&rx);
    assert_eq!(inv.generation, 2);
    assert_eq!(inv.pods.len(), 8);

    // A re-list replaces state: objects missing from it are gone.
    f.pods
        .list(vec![owned_pod("web-1", "ReplicaSet", "web-abc", "rs-1")]);
    let inv = recv(&rx);
    assert_eq!(inv.pods.len(), 1);
}

#[test]
fn stop_ends_publishing() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let f = fake();
    let (tx, rx) = crossbeam_channel::unbounded();
    let watch = SecurityWatch::spawn(rt.handle(), f.clone(), 1, tx);
    initial_sync(&f);
    recv(&rx);
    watch.stop();
    std::thread::sleep(Duration::from_millis(100));
    f.pods.send(Event::Apply(owned_pod(
        "late",
        "ReplicaSet",
        "web-abc",
        "rs-1",
    )));
    assert!(
        rx.recv_timeout(Duration::from_secs(2)).is_err(),
        "no snapshots after stop"
    );
}
