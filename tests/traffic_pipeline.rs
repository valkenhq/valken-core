//! Pod events → traffic manager → listener → processor → sink, over an
//! in-memory log source and a trivial line-collecting processor.  The
//! application supplies the real processor (access-log parsing, buckets);
//! this exercises everything the core owns around it.

mod support;

use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use support::*;
use valken_core::k8s::{IngressControllerKind, KubeResource, Watched, WatchedResources};
use valken_core::traffic::{
    spawn_traffic_manager_with, supports, LineProcessor, ProcessorContext, ProcessorFactory, Sink,
    SupportFn,
};

/// Emits `(pod label, line)` for every line on each flush, and counts
/// non-empty lines as "requests" so the telemetry path is exercised too.
struct Collect {
    label: String,
    stats: Arc<valken_core::traffic::ListenerStats>,
    pending: Vec<(String, String)>,
}

impl LineProcessor for Collect {
    type Output = (String, String);
    fn ingest(&mut self, raw: &str) {
        if raw.trim().is_empty() {
            self.stats
                .ignored
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            self.stats
                .requests
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.pending.push((self.label.clone(), raw.to_string()));
        }
    }
    fn flush_closed(&mut self, _now: i64) -> Vec<Self::Output> {
        std::mem::take(&mut self.pending)
    }
    fn flush_all(&mut self) -> Vec<Self::Output> {
        std::mem::take(&mut self.pending)
    }
    fn open_count(&self) -> usize {
        self.pending.len()
    }
}

struct CollectFactory;

impl ProcessorFactory<(String, String)> for CollectFactory {
    fn supported(&self) -> SupportFn {
        supports(&[IngressControllerKind::Nginx])
    }
    fn build(
        &self,
        ctx: ProcessorContext,
    ) -> Option<Box<dyn LineProcessor<Output = (String, String)>>> {
        Some(Box::new(Collect {
            label: ctx.target.label(),
            stats: ctx.stats,
            pending: Vec::new(),
        }))
    }
}

#[test]
fn controller_pod_lines_reach_the_sink_and_telemetry() {
    let logs = FakeLogs::new();
    let feed = logs.feed("ingress-nginx/ctl-1");

    let received: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let r2 = received.clone();
    let sink: Sink<(String, String)> = Arc::new(move |rows| r2.lock().unwrap().extend(rows));

    let (tx, rx) = crossbeam_channel::unbounded();
    let manager = spawn_traffic_manager_with(
        rx,
        sink,
        Arc::new(AtomicU32::new(5)),
        logs.clone(),
        Arc::new(CollectFactory),
    );

    tx.send(Watched::NamespaceList(vec![])).unwrap();
    tx.send(Watched::FullResourceList(WatchedResources {
        resources: vec![
            KubeResource::Pod(nginx_controller_pod("ingress-nginx", "ctl-1", "c1").into()),
            KubeResource::Pod(pod("default", "web", "a1", labels(&[("app", "web")])).into()),
        ],
        cluster_id: 5,
    }))
    .unwrap();

    // Exactly one listener, on the controller pod.
    assert!(wait_until(Duration::from_secs(5), || {
        logs.opens.lock().unwrap().len() == 1
    }));
    assert_eq!(logs.opens.lock().unwrap()[0], "ingress-nginx/ctl-1");

    for i in 0..3 {
        feed.send(format!("line {i}")).unwrap();
    }
    feed.send("   ".into()).unwrap();

    assert!(wait_until(Duration::from_secs(5), || {
        received.lock().unwrap().len() >= 3
    }));
    let rows = received.lock().unwrap().clone();
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|(label, _)| label == "ingress-nginx/ctl-1"));
    assert_eq!(rows[0].1, "line 0");

    let t = manager.telemetry();
    assert_eq!(t.listeners.len(), 1);
    assert_eq!(t.controller_pods, 1);
    assert_eq!(t.total_lines(), 4);
    assert_eq!(t.total_requests(), 3);
    assert_eq!(t.total_ignored(), 1);
    assert!(t.listeners[0].stream_open);
    // The flush runs on the listener's own 1 s tick and telemetry is
    // republished every 500 ms, so the counter can lag the sink briefly.
    assert!(wait_until(Duration::from_secs(5), || {
        manager.telemetry().listeners[0].flushed_buckets == 3
    }));
    assert_eq!(t.controller_kind(), Some("NGINX"));

    // The controller pod goes away: its listener stops.
    tx.send(Watched::DeletedPod(
        nginx_controller_pod("ingress-nginx", "ctl-1", "c1").into(),
    ))
    .unwrap();
    assert!(wait_until(Duration::from_secs(5), || {
        manager.telemetry().listeners.is_empty()
    }));

    manager.shutdown();
}

#[test]
fn unsupported_controllers_are_counted_not_listened_to() {
    let logs = FakeLogs::new();
    let sink: Sink<(String, String)> = Arc::new(|_| {});
    let (tx, rx) = crossbeam_channel::unbounded();
    let manager = spawn_traffic_manager_with(
        rx,
        sink,
        Arc::new(AtomicU32::new(1)),
        logs.clone(),
        Arc::new(CollectFactory),
    );
    let mut traefik = nginx_controller_pod("traefik", "traefik-1", "t1");
    traefik.metadata.labels = Some(labels(&[("app.kubernetes.io/name", "traefik")]));
    traefik.spec.as_mut().unwrap().containers[0].image = Some("traefik:v3".into());
    tx.send(Watched::Pod(traefik.into())).unwrap();
    assert!(wait_until(Duration::from_secs(5), || {
        manager.telemetry().controller_pods == 1
    }));
    let t = manager.telemetry();
    assert!(t.listeners.is_empty());
    assert_eq!(t.unsupported_kinds, vec!["Traefik".to_string()]);
    assert!(logs.opens.lock().unwrap().is_empty());
    manager.shutdown();
}

#[test]
fn listener_reconnects_when_the_stream_ends_and_reports_open_failures() {
    let logs = FakeLogs::new();
    let first = logs.feed("ingress-nginx/ctl-1");
    let sink: Sink<(String, String)> = Arc::new(|_| {});
    let (tx, rx) = crossbeam_channel::unbounded();
    let manager = spawn_traffic_manager_with(
        rx,
        sink,
        Arc::new(AtomicU32::new(1)),
        logs.clone(),
        Arc::new(CollectFactory),
    );

    tx.send(Watched::Pod(
        nginx_controller_pod("ingress-nginx", "ctl-1", "c1").into(),
    ))
    .unwrap();
    assert!(wait_until(Duration::from_secs(5), || {
        logs.opens.lock().unwrap().len() == 1
    }));
    // End the stream: the listener backs off (1 s) and opens again.  No feed
    // is registered, so that open fails and is recorded as the last error.
    drop(first);
    assert!(wait_until(Duration::from_secs(5), || {
        logs.opens.lock().unwrap().len() >= 2
    }));
    assert!(wait_until(Duration::from_secs(5), || {
        manager
            .telemetry()
            .listeners
            .first()
            .is_some_and(|l| l.reconnects >= 1 && l.last_error.is_some() && !l.stream_open)
    }));
    manager.shutdown();
}
