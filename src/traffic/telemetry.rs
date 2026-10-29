//! Health telemetry for the traffic pipeline, published by the manager thread
//! for the UI to read.
//!
//! The lens has to explain *why* there is no traffic data — no controller,
//! an unparseable log format, RBAC denying `pods/log`, or simply not enough
//! buckets yet.  Each listener keeps lock-free counters ([`ListenerStats`]);
//! the manager folds those plus its own pod census into one
//! [`TrafficTelemetry`] value behind a shared [`TelemetryCell`] roughly every
//! poll tick.  Readers clone the whole struct, so the lock is held for
//! microseconds and never across I/O.
//!
//! [`ListenerStats`]: crate::traffic::listener::ListenerStats

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use k8s_openapi::api::core::v1::Pod;

use crate::k8s::ingress_controller::detect_ingress_controller_from_pod;
use crate::k8s::IngressControllerKind;
use crate::traffic::manager::{pod_key, SupportFn};

/// Why a listener's `pods/log` stream could not be opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamErrorKind {
    /// HTTP 403 — RBAC denies `pods/log` in the controller namespace.
    Forbidden,
    /// HTTP 404 — the pod vanished between the watch event and the open.
    NotFound,
    /// Anything else (network, timeout, no client yet).
    Other,
}

/// Map a kube client error to the coarse kind the UI cares about.
pub fn classify_stream_error(err: &kube::Error) -> StreamErrorKind {
    match err {
        kube::Error::Api(resp) => match resp.code {
            403 => StreamErrorKind::Forbidden,
            404 => StreamErrorKind::NotFound,
            _ => StreamErrorKind::Other,
        },
        _ => StreamErrorKind::Other,
    }
}

/// Last stream error of one listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamError {
    pub kind: StreamErrorKind,
    pub message: String,
}

/// Snapshot of one listener's counters.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListenerTelemetry {
    /// `namespace/pod`.
    pub label: String,
    pub namespace: String,
    pub kind: Option<IngressControllerKind>,
    pub lines: u64,
    pub requests: u64,
    pub ignored: u64,
    pub malformed: u64,
    pub reconnects: u64,
    pub flushed_buckets: u64,
    /// `true` while the log stream is open and being read.
    pub stream_open: bool,
    /// Unix seconds the log stream first opened; `None` until it has.
    pub first_open_at: Option<i64>,
    pub last_error: Option<StreamError>,
}

/// Whole-pipeline health as seen by the manager thread.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TrafficTelemetry {
    /// One entry per running listener (supported controller pods only).
    pub listeners: Vec<ListenerTelemetry>,
    /// Running ingress-controller pods of any kind, supported or not.
    pub controller_pods: usize,
    /// Display names of controller kinds seen that have no parser yet.
    pub unsupported_kinds: Vec<String>,
}

impl TrafficTelemetry {
    pub fn total_lines(&self) -> u64 {
        self.listeners.iter().map(|l| l.lines).sum()
    }
    pub fn total_requests(&self) -> u64 {
        self.listeners.iter().map(|l| l.requests).sum()
    }
    pub fn total_malformed(&self) -> u64 {
        self.listeners.iter().map(|l| l.malformed).sum()
    }
    pub fn total_ignored(&self) -> u64 {
        self.listeners.iter().map(|l| l.ignored).sum()
    }
    /// Earliest time any listener's log stream opened.
    pub fn tailing_since(&self) -> Option<i64> {
        self.listeners.iter().filter_map(|l| l.first_open_at).min()
    }
    /// First listener whose last error was an RBAC denial, if any.
    pub fn forbidden(&self) -> Option<&ListenerTelemetry> {
        self.listeners.iter().find(|l| {
            l.last_error
                .as_ref()
                .is_some_and(|e| e.kind == StreamErrorKind::Forbidden)
        })
    }
    /// Namespaces of the tailed controllers, deduplicated and sorted.
    pub fn controller_namespaces(&self) -> Vec<String> {
        let mut ns: Vec<String> = self.listeners.iter().map(|l| l.namespace.clone()).collect();
        ns.sort();
        ns.dedup();
        ns
    }
    /// Display name of the controller kind being tailed (first listener).
    pub fn controller_kind(&self) -> Option<&'static str> {
        self.listeners
            .iter()
            .find_map(|l| l.kind.map(|k| k.display_name()))
    }
}

/// Shared cell the manager writes and the UI reads.
pub type TelemetryCell = Arc<RwLock<TrafficTelemetry>>;

pub fn new_telemetry_cell() -> TelemetryCell {
    Arc::new(RwLock::new(TrafficTelemetry::default()))
}

/// Read a copy of the current telemetry (a poisoned lock still holds a valid
/// value, so it is used rather than propagated).
pub fn read_telemetry(cell: &TelemetryCell) -> TrafficTelemetry {
    cell.read().unwrap_or_else(|p| p.into_inner()).clone()
}

// ─── Controller census ────────────────────────────────────────────────────────

/// Running ingress-controller pods of every kind, keyed like the reconciler
/// so the two stay in step.  Pure: fed the same pod events as the reconciler.
pub struct ControllerCensus {
    pods: HashMap<String, IngressControllerKind>,
    supported: SupportFn,
}

impl std::fmt::Debug for ControllerCensus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControllerCensus")
            .field("pods", &self.pods)
            .finish_non_exhaustive()
    }
}

impl ControllerCensus {
    /// `supported` says which kinds the application can parse; the rest are
    /// reported through [`ControllerCensus::unsupported_kinds`].
    pub fn new(supported: SupportFn) -> Self {
        Self {
            pods: HashMap::new(),
            supported,
        }
    }

    /// A running, non-terminating controller pod counts; anything else is removed.
    pub fn observe_applied(&mut self, pod: &Pod) {
        let Some(key) = pod_key(pod) else { return };
        let running = pod.metadata.deletion_timestamp.is_none()
            && pod.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running");
        match (running, detect_ingress_controller_from_pod(pod)) {
            (true, Some(kind)) => {
                self.pods.insert(key, kind);
            }
            _ => {
                self.pods.remove(&key);
            }
        }
    }

    pub fn observe_deleted(&mut self, pod: &Pod) {
        if let Some(key) = pod_key(pod) {
            self.pods.remove(&key);
        }
    }

    /// Replace the census with a full pod list (reconnect / new cluster).
    pub fn observe_full_list<'a>(&mut self, pods: impl IntoIterator<Item = &'a Pod>) {
        self.pods.clear();
        for pod in pods {
            self.observe_applied(pod);
        }
    }

    pub fn controller_pods(&self) -> usize {
        self.pods.len()
    }

    /// Kinds present that have no parser, as display names (sorted, unique).
    pub fn unsupported_kinds(&self) -> Vec<String> {
        let mut kinds: Vec<String> = self
            .pods
            .values()
            .filter(|k| !(self.supported)(**k))
            .map(|k| k.display_name().to_string())
            .collect();
        kinds.sort();
        kinds.dedup();
        kinds
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{Container, PodSpec, PodStatus};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;

    fn controller_pod(name: &str, uid: &str, app: &str, image: &str, phase: &str) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some("ingress".into()),
                uid: Some(uid.into()),
                labels: Some(BTreeMap::from([(
                    "app.kubernetes.io/name".to_string(),
                    app.to_string(),
                )])),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "controller".into(),
                    image: Some(image.into()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(phase.into()),
                ..Default::default()
            }),
        }
    }

    fn nginx(name: &str, uid: &str, phase: &str) -> Pod {
        controller_pod(
            name,
            uid,
            "ingress-nginx",
            "registry.k8s.io/ingress-nginx/controller:v1.11.0",
            phase,
        )
    }

    fn traefik(name: &str, uid: &str) -> Pod {
        controller_pod(name, uid, "traefik", "traefik:v3", "Running")
    }

    fn haproxy(name: &str, uid: &str) -> Pod {
        controller_pod(
            name,
            uid,
            "haproxy-ingress",
            "haproxytech/kubernetes-ingress:3.0",
            "Running",
        )
    }

    #[test]
    fn census_counts_running_controllers_of_any_kind() {
        let mut c = ControllerCensus::new(crate::traffic::manager::supports(&[
            IngressControllerKind::Nginx,
            IngressControllerKind::Traefik,
        ]));
        c.observe_applied(&nginx("a", "1", "Running"));
        c.observe_applied(&traefik("t", "2"));
        c.observe_applied(&haproxy("h", "4"));
        c.observe_applied(&nginx("p", "3", "Pending"));
        assert_eq!(c.controller_pods(), 3);
        assert_eq!(
            c.unsupported_kinds(),
            vec!["HAProxy".to_string()],
            "Traefik is supported"
        );

        c.observe_deleted(&nginx("a", "1", "Running"));
        assert_eq!(c.controller_pods(), 2);
        c.observe_full_list([nginx("z", "9", "Running")].iter());
        assert_eq!(c.controller_pods(), 1);
        assert!(c.unsupported_kinds().is_empty());
    }

    #[test]
    fn telemetry_totals_and_forbidden_lookup() {
        let t = TrafficTelemetry {
            listeners: vec![
                ListenerTelemetry {
                    label: "ingress/a".into(),
                    namespace: "ingress".into(),
                    lines: 10,
                    requests: 4,
                    malformed: 1,
                    ..Default::default()
                },
                ListenerTelemetry {
                    label: "ingress/b".into(),
                    namespace: "ingress".into(),
                    lines: 5,
                    last_error: Some(StreamError {
                        kind: StreamErrorKind::Forbidden,
                        message: "forbidden".into(),
                    }),
                    ..Default::default()
                },
            ],
            controller_pods: 2,
            unsupported_kinds: vec![],
        };
        assert_eq!(t.total_lines(), 15);
        assert_eq!(t.total_requests(), 4);
        assert_eq!(t.total_malformed(), 1);
        assert_eq!(t.forbidden().map(|l| l.label.as_str()), Some("ingress/b"));
        assert_eq!(t.controller_namespaces(), vec!["ingress".to_string()]);
        assert_eq!(TrafficTelemetry::default().forbidden(), None);
    }

    fn api_error(code: u16, reason: &str) -> kube::Error {
        kube::Error::Api(
            kube::core::Status {
                status: None,
                code,
                message: reason.to_lowercase(),
                metadata: None,
                reason: reason.to_string(),
                details: None,
            }
            .boxed(),
        )
    }

    #[test]
    fn stream_error_classification() {
        assert_eq!(
            classify_stream_error(&api_error(403, "Forbidden")),
            StreamErrorKind::Forbidden
        );
        assert_eq!(
            classify_stream_error(&api_error(404, "NotFound")),
            StreamErrorKind::NotFound
        );
        assert_eq!(
            classify_stream_error(&api_error(500, "Internal")),
            StreamErrorKind::Other
        );
    }

    #[test]
    fn cell_round_trips() {
        let cell = new_telemetry_cell();
        cell.write().unwrap().controller_pods = 3;
        assert_eq!(read_telemetry(&cell).controller_pods, 3);
    }
}
