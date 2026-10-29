//! Slimmed, security-relevant views of cluster objects, and the pure
//! conversions from k8s-openapi types.
//!
//! Nothing here keeps a raw object: `managedFields`, `status` and every env
//! var `value` are gone before anything is stored (see
//! [`super::export::redacted_manifest`]).

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{
    Container, Pod, PodSecurityContext, PodSpec, SecurityContext, Service,
};
use k8s_openapi::api::networking::v1::{Ingress, IngressBackend, NetworkPolicy};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use kube::api::DynamicObject;
use serde_json::Value;

use super::export::redacted_manifest;

// ─── Identity ─────────────────────────────────────────────────────────────────

/// Namespace, name and uid of an object.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct ObjectRef {
    pub namespace: String,
    pub name: String,
    /// Empty if the server didn't set one (only in tests).
    pub uid: String,
}

impl ObjectRef {
    pub fn from_meta(meta: &ObjectMeta) -> Self {
        Self {
            namespace: meta.namespace.clone().unwrap_or_default(),
            name: meta.name.clone().unwrap_or_default(),
            uid: meta.uid.clone().unwrap_or_default(),
        }
    }

    /// Store key: `namespace/name`.
    pub fn key(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
}

/// The controlling owner of an object (`ownerReferences[controller=true]`,
/// else the first).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OwnerRef {
    pub kind: String,
    pub name: String,
    pub uid: String,
}

fn controller_owner(meta: &ObjectMeta) -> Option<OwnerRef> {
    let owners = meta.owner_references.as_ref()?;
    let owner = owners
        .iter()
        .find(|o| o.controller == Some(true))
        .or_else(|| owners.first())?;
    Some(OwnerRef {
        kind: owner.kind.clone(),
        name: owner.name.clone(),
        uid: owner.uid.clone(),
    })
}

/// The kinds of workload a pod can belong to. Bare pods are their own
/// workload; a ReplicaSet without a Deployment is its own workload; any other
/// controller (Argo Rollout, operator CRs, …) is [`WorkloadKind::Other`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WorkloadKind {
    Deployment,
    StatefulSet,
    DaemonSet,
    Job,
    CronJob,
    ReplicaSet,
    Pod,
    Other,
}

impl WorkloadKind {
    pub fn as_str(self) -> &'static str {
        match self {
            WorkloadKind::Deployment => "Deployment",
            WorkloadKind::StatefulSet => "StatefulSet",
            WorkloadKind::DaemonSet => "DaemonSet",
            WorkloadKind::Job => "Job",
            WorkloadKind::CronJob => "CronJob",
            WorkloadKind::ReplicaSet => "ReplicaSet",
            WorkloadKind::Pod => "Pod",
            WorkloadKind::Other => "Other",
        }
    }

    /// Short form used in UI labels (`deploy/web`).
    pub fn short(self) -> &'static str {
        match self {
            WorkloadKind::Deployment => "deploy",
            WorkloadKind::StatefulSet => "sts",
            WorkloadKind::DaemonSet => "ds",
            WorkloadKind::Job => "job",
            WorkloadKind::CronJob => "cronjob",
            WorkloadKind::ReplicaSet => "rs",
            WorkloadKind::Pod => "pod",
            WorkloadKind::Other => "workload",
        }
    }

    /// Map an owner `kind` to a workload kind ([`WorkloadKind::Other`] if
    /// unknown).
    pub fn from_kind(kind: &str) -> Self {
        match kind {
            "Deployment" => WorkloadKind::Deployment,
            "StatefulSet" => WorkloadKind::StatefulSet,
            "DaemonSet" => WorkloadKind::DaemonSet,
            "Job" => WorkloadKind::Job,
            "CronJob" => WorkloadKind::CronJob,
            "ReplicaSet" => WorkloadKind::ReplicaSet,
            "Pod" => WorkloadKind::Pod,
            _ => WorkloadKind::Other,
        }
    }
}

/// A workload, identified by kind + object.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkloadRef {
    pub kind: WorkloadKind,
    pub object: ObjectRef,
}

impl WorkloadRef {
    /// `deploy/web`
    pub fn label(&self) -> String {
        format!("{}/{}", self.kind.short(), self.object.name)
    }
}

// ─── Label selectors ──────────────────────────────────────────────────────────

/// A `metav1.LabelSelector`. An empty selector matches everything.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Selector {
    pub match_labels: BTreeMap<String, String>,
    pub match_expressions: Vec<SelectorExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectorExpr {
    pub key: String,
    pub operator: String,
    pub values: Vec<String>,
}

impl Selector {
    pub fn from_label_selector(sel: &LabelSelector) -> Self {
        Self {
            match_labels: sel.match_labels.clone().unwrap_or_default(),
            match_expressions: sel
                .match_expressions
                .iter()
                .flatten()
                .map(|e| SelectorExpr {
                    key: e.key.clone(),
                    operator: e.operator.clone(),
                    values: e.values.clone().unwrap_or_default(),
                })
                .collect(),
        }
    }

    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        self.match_labels
            .iter()
            .all(|(k, v)| labels.get(k) == Some(v))
            && self.match_expressions.iter().all(|e| {
                let value = labels.get(&e.key);
                match e.operator.as_str() {
                    "In" => value.is_some_and(|v| e.values.contains(v)),
                    "NotIn" => value.is_none_or(|v| !e.values.contains(v)),
                    "Exists" => value.is_some(),
                    "DoesNotExist" => value.is_none(),
                    // Unknown operators match nothing (fail closed).
                    _ => false,
                }
            })
    }
}

/// A Service's plain `selector` map: matches if every pair is present.
/// An empty map selects nothing (Services without selectors are manual).
pub fn service_selects(
    selector: &BTreeMap<String, String>,
    labels: &BTreeMap<String, String>,
) -> bool {
    !selector.is_empty() && selector.iter().all(|(k, v)| labels.get(k) == Some(v))
}

// ─── Images ───────────────────────────────────────────────────────────────────

/// An image pinned by digest: `repository@sha256:…`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ImageDigest {
    /// e.g. `ghcr.io/eldr/lockbox` (no tag).
    pub repository: String,
    /// e.g. `sha256:9f2c…`.
    pub digest: String,
}

impl ImageDigest {
    /// `repository@digest`, the reference a scan should use.
    pub fn reference(&self) -> String {
        format!("{}@{}", self.repository, self.digest)
    }

    /// Registry host of the repository (`docker.io` when implicit).
    pub fn registry(&self) -> &str {
        registry_of(&self.repository)
    }
}

/// Registry host of an image reference, `docker.io` when implicit.
pub fn registry_of(image: &str) -> &str {
    match image.split_once('/') {
        Some((first, _)) if first.contains('.') || first.contains(':') || first == "localhost" => {
            first
        }
        _ => "docker.io",
    }
}

/// Normalise `status.containerStatuses[].imageID` to `repo@sha256:…`.
///
/// Strips runtime prefixes (`docker-pullable://`, `docker://`, …). Returns
/// `None` for IDs that aren't a registry digest — a bare `sha256:` image
/// ID (local image) or an empty string (pod pending).
pub fn normalize_image_id(image_id: &str) -> Option<ImageDigest> {
    let id = image_id
        .split_once("://")
        .map_or(image_id, |(_, rest)| rest)
        .trim();
    let (repository, digest) = id.rsplit_once('@')?;
    let hex = digest.strip_prefix("sha256:")?;
    if repository.is_empty() || hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(ImageDigest {
        repository: repository.to_owned(),
        digest: digest.to_ascii_lowercase(),
    })
}

// ─── Pods ─────────────────────────────────────────────────────────────────────

/// Security settings in effect for one container (container overrides pod).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EffectiveSecurity {
    pub run_as_non_root: Option<bool>,
    pub run_as_user: Option<i64>,
    pub privileged: Option<bool>,
    pub allow_privilege_escalation: Option<bool>,
}

impl EffectiveSecurity {
    fn resolve(pod: Option<&PodSecurityContext>, container: Option<&SecurityContext>) -> Self {
        Self {
            run_as_non_root: container
                .and_then(|c| c.run_as_non_root)
                .or_else(|| pod.and_then(|p| p.run_as_non_root)),
            run_as_user: container
                .and_then(|c| c.run_as_user)
                .or_else(|| pod.and_then(|p| p.run_as_user)),
            privileged: container.and_then(|c| c.privileged),
            allow_privilege_escalation: container.and_then(|c| c.allow_privilege_escalation),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecContainer {
    pub name: String,
    pub init: bool,
    /// As written in the spec (`ghcr.io/eldr/lockbox:1.4`).
    pub image: String,
    /// From `status.*ContainerStatuses[].imageID`; `None` until pulled.
    pub digest: Option<ImageDigest>,
    pub host_ports: Vec<i32>,
    pub security: EffectiveSecurity,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SecPod {
    pub object: ObjectRef,
    pub labels: BTreeMap<String, String>,
    pub owner: Option<OwnerRef>,
    pub phase: Option<String>,
    pub containers: Vec<SecContainer>,
    pub host_network: bool,
    /// Names of `hostPath` volumes.
    pub host_path_volumes: Vec<String>,
    pub image_pull_secrets: Vec<String>,
    /// Redacted manifest, kept only for bare pods (no owner) for misconfig
    /// scanning; owned pods are covered by their workload's template.
    pub manifest: Option<Value>,
}

impl SecPod {
    pub fn from_pod(pod: &Pod) -> Self {
        let spec = pod.spec.as_ref();
        let status = pod.status.as_ref();
        let pod_sc = spec.and_then(|s| s.security_context.as_ref());

        let digest_for = |name: &str, init: bool| {
            let statuses = if init {
                status.and_then(|s| s.init_container_statuses.as_ref())
            } else {
                status.and_then(|s| s.container_statuses.as_ref())
            };
            statuses
                .into_iter()
                .flatten()
                .find(|cs| cs.name == name)
                .and_then(|cs| normalize_image_id(&cs.image_id))
        };
        let convert = |c: &Container, init: bool| SecContainer {
            name: c.name.clone(),
            init,
            image: c.image.clone().unwrap_or_default(),
            digest: digest_for(&c.name, init),
            host_ports: c
                .ports
                .iter()
                .flatten()
                .filter_map(|p| p.host_port)
                .filter(|p| *p > 0)
                .collect(),
            security: EffectiveSecurity::resolve(pod_sc, c.security_context.as_ref()),
        };

        let containers = spec
            .map(|s| {
                s.init_containers
                    .iter()
                    .flatten()
                    .map(|c| convert(c, true))
                    .chain(s.containers.iter().map(|c| convert(c, false)))
                    .collect()
            })
            .unwrap_or_default();

        let owner = controller_owner(&pod.metadata);
        Self {
            object: ObjectRef::from_meta(&pod.metadata),
            labels: pod.metadata.labels.clone().unwrap_or_default(),
            manifest: owner.is_none().then(|| redacted_manifest(pod)),
            owner,
            phase: status.and_then(|s| s.phase.clone()),
            containers,
            host_network: spec.and_then(|s| s.host_network).unwrap_or(false),
            host_path_volumes: host_path_volumes(spec),
            image_pull_secrets: spec
                .and_then(|s| s.image_pull_secrets.as_ref())
                .into_iter()
                .flatten()
                .map(|r| r.name.clone())
                .filter(|n| !n.is_empty())
                .collect(),
        }
    }
}

fn host_path_volumes(spec: Option<&PodSpec>) -> Vec<String> {
    spec.and_then(|s| s.volumes.as_ref())
        .into_iter()
        .flatten()
        .filter(|v| v.host_path.is_some())
        .map(|v| v.name.clone())
        .collect()
}

// ─── Workloads ────────────────────────────────────────────────────────────────

/// A controller workload with its redacted manifest (for misconfig scans).
#[derive(Debug, Clone, PartialEq)]
pub struct SecWorkload {
    pub workload: WorkloadRef,
    /// Controlling owner (a Job's CronJob).
    pub owner: Option<OwnerRef>,
    pub manifest: Value,
}

macro_rules! workload_from {
    ($fn_name:ident, $ty:ty, $kind:expr) => {
        pub fn $fn_name(obj: &$ty) -> Self {
            Self {
                workload: WorkloadRef {
                    kind: $kind,
                    object: ObjectRef::from_meta(&obj.metadata),
                },
                owner: controller_owner(&obj.metadata),
                manifest: redacted_manifest(obj),
            }
        }
    };
}

impl SecWorkload {
    workload_from!(from_deployment, Deployment, WorkloadKind::Deployment);
    workload_from!(from_stateful_set, StatefulSet, WorkloadKind::StatefulSet);
    workload_from!(from_daemon_set, DaemonSet, WorkloadKind::DaemonSet);
    workload_from!(from_job, Job, WorkloadKind::Job);
    workload_from!(from_cron_job, CronJob, WorkloadKind::CronJob);
}

/// A ReplicaSet reduced to what's needed to map pods to Deployments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecReplicaSet {
    pub object: ObjectRef,
    pub owner: Option<OwnerRef>,
}

impl SecReplicaSet {
    pub fn from_replica_set(rs: &ReplicaSet) -> Self {
        Self {
            object: ObjectRef::from_meta(&rs.metadata),
            owner: controller_owner(&rs.metadata),
        }
    }
}

// ─── Exposure ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecServicePort {
    pub name: Option<String>,
    pub port: i32,
    pub node_port: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecService {
    pub object: ObjectRef,
    /// `ClusterIP` when unset.
    pub service_type: String,
    pub selector: BTreeMap<String, String>,
    pub ports: Vec<SecServicePort>,
}

impl SecService {
    pub fn from_service(svc: &Service) -> Self {
        let spec = svc.spec.as_ref();
        Self {
            object: ObjectRef::from_meta(&svc.metadata),
            service_type: spec
                .and_then(|s| s.type_.clone())
                .unwrap_or_else(|| "ClusterIP".into()),
            selector: spec.and_then(|s| s.selector.clone()).unwrap_or_default(),
            ports: spec
                .and_then(|s| s.ports.as_ref())
                .into_iter()
                .flatten()
                .map(|p| SecServicePort {
                    name: p.name.clone(),
                    port: p.port,
                    node_port: p.node_port,
                })
                .collect(),
        }
    }

    /// LoadBalancer or NodePort: reachable from outside the cluster network.
    pub fn is_externally_exposed(&self) -> bool {
        matches!(self.service_type.as_str(), "LoadBalancer" | "NodePort")
    }
}

/// One host → Service route from an Ingress or HTTPRoute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    /// `None` for a catch-all (no host / default backend).
    pub host: Option<String>,
    pub service_namespace: String,
    pub service: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecIngress {
    pub object: ObjectRef,
    pub routes: Vec<Route>,
}

impl SecIngress {
    pub fn from_ingress(ing: &Ingress) -> Self {
        let object = ObjectRef::from_meta(&ing.metadata);
        let ns = object.namespace.clone();
        let backend_service = |b: &IngressBackend| b.service.as_ref().map(|s| s.name.clone());
        let mut routes = Vec::new();
        if let Some(spec) = &ing.spec {
            if let Some(svc) = spec.default_backend.as_ref().and_then(backend_service) {
                routes.push(Route {
                    host: None,
                    service_namespace: ns.clone(),
                    service: svc,
                });
            }
            for rule in spec.rules.iter().flatten() {
                let paths = rule.http.iter().flat_map(|h| h.paths.iter());
                for svc in paths.filter_map(|p| backend_service(&p.backend)) {
                    let route = Route {
                        host: rule.host.clone(),
                        service_namespace: ns.clone(),
                        service: svc,
                    };
                    if !routes.contains(&route) {
                        routes.push(route);
                    }
                }
            }
        }
        Self { object, routes }
    }
}

/// A Gateway API HTTPRoute (watched as a dynamic object; the CRD may be absent).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecHttpRoute {
    pub object: ObjectRef,
    pub routes: Vec<Route>,
}

impl SecHttpRoute {
    pub fn from_dynamic(obj: &DynamicObject) -> Self {
        let object = ObjectRef::from_meta(&obj.metadata);
        let spec = &obj.data["spec"];
        let hostnames: Vec<Option<String>> = match spec["hostnames"].as_array() {
            Some(h) if !h.is_empty() => h.iter().map(|v| v.as_str().map(String::from)).collect(),
            _ => vec![None],
        };
        let mut routes = Vec::new();
        let backends = spec["rules"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|rule| rule["backendRefs"].as_array().into_iter().flatten());
        for backend in backends {
            // Only core Services (kind defaults to Service, group to "").
            let kind = backend["kind"].as_str().unwrap_or("Service");
            let group = backend["group"].as_str().unwrap_or("");
            let Some(name) = backend["name"].as_str() else {
                continue;
            };
            if kind != "Service" || !group.is_empty() {
                continue;
            }
            let ns = backend["namespace"].as_str().unwrap_or(&object.namespace);
            for host in &hostnames {
                let route = Route {
                    host: host.clone(),
                    service_namespace: ns.to_owned(),
                    service: name.to_owned(),
                };
                if !routes.contains(&route) {
                    routes.push(route);
                }
            }
        }
        Self { object, routes }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecNetworkPolicy {
    pub object: ObjectRef,
    pub pod_selector: Selector,
    /// `policyTypes` includes Ingress (also the default when unset).
    pub covers_ingress: bool,
    pub covers_egress: bool,
}

impl SecNetworkPolicy {
    pub fn from_network_policy(np: &NetworkPolicy) -> Self {
        let spec = np.spec.as_ref();
        let types = spec.and_then(|s| s.policy_types.as_ref());
        let (covers_ingress, covers_egress) = match types {
            Some(t) => (
                t.iter().any(|t| t == "Ingress"),
                t.iter().any(|t| t == "Egress"),
            ),
            // Unset: Ingress always; Egress if egress rules are present.
            None => (true, spec.is_some_and(|s| s.egress.is_some())),
        };
        Self {
            object: ObjectRef::from_meta(&np.metadata),
            pod_selector: spec
                .and_then(|s| s.pod_selector.as_ref())
                .map(Selector::from_label_selector)
                .unwrap_or_default(),
            covers_ingress,
            covers_egress,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        ContainerPort, ContainerStatus, PodStatus, ServicePort, ServiceSpec,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
        LabelSelectorRequirement, OwnerReference,
    };

    const D: &str = "9f2c000000000000000000000000000000000000000000000000000000000ee1";

    #[test]
    fn image_ids_normalise() {
        let expect = ImageDigest {
            repository: "ghcr.io/eldr/lockbox".into(),
            digest: format!("sha256:{D}"),
        };
        for raw in [
            format!("docker-pullable://ghcr.io/eldr/lockbox@sha256:{D}"),
            format!("ghcr.io/eldr/lockbox@sha256:{D}"),
            format!("docker://ghcr.io/eldr/lockbox@sha256:{}", D.to_uppercase()),
        ] {
            assert_eq!(normalize_image_id(&raw), Some(expect.clone()), "{raw}");
        }
        assert_eq!(
            expect.reference(),
            format!("ghcr.io/eldr/lockbox@sha256:{D}")
        );
        assert_eq!(normalize_image_id(""), None);
        assert_eq!(normalize_image_id(&format!("sha256:{D}")), None);
        assert_eq!(normalize_image_id("repo@sha256:short"), None);
    }

    #[test]
    fn registry_hosts() {
        assert_eq!(registry_of("ghcr.io/eldr/x"), "ghcr.io");
        assert_eq!(registry_of("harbor.local:5000/x"), "harbor.local:5000");
        assert_eq!(registry_of("library/nginx"), "docker.io");
        assert_eq!(registry_of("nginx"), "docker.io");
        assert_eq!(registry_of("localhost/x"), "localhost");
    }

    #[test]
    fn selectors() {
        let labels: BTreeMap<String, String> = [("app", "web"), ("tier", "fe")]
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .into();
        assert!(Selector::default().matches(&labels));
        let sel = Selector::from_label_selector(&LabelSelector {
            match_labels: Some([("app".to_string(), "web".to_string())].into()),
            match_expressions: Some(vec![
                LabelSelectorRequirement {
                    key: "tier".into(),
                    operator: "In".into(),
                    values: Some(vec!["fe".into(), "be".into()]),
                },
                LabelSelectorRequirement {
                    key: "debug".into(),
                    operator: "DoesNotExist".into(),
                    values: None,
                },
            ]),
        });
        assert!(sel.matches(&labels));
        let mut other = labels.clone();
        other.insert("tier".into(), "db".into());
        assert!(!sel.matches(&other));
        assert!(!service_selects(&BTreeMap::new(), &labels));
        assert!(service_selects(
            &[("app".to_string(), "web".to_string())].into(),
            &labels
        ));
    }

    #[test]
    fn pod_facts() {
        let pod = Pod {
            metadata: ObjectMeta {
                namespace: Some("lockbox".into()),
                name: Some("web-1".into()),
                uid: Some("p1".into()),
                owner_references: Some(vec![OwnerReference {
                    kind: "ReplicaSet".into(),
                    name: "web-abc".into(),
                    uid: "rs1".into(),
                    controller: Some(true),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            spec: Some(PodSpec {
                security_context: Some(PodSecurityContext {
                    run_as_non_root: Some(true),
                    ..Default::default()
                }),
                containers: vec![Container {
                    name: "web".into(),
                    image: Some("ghcr.io/eldr/lockbox:1.4".into()),
                    ports: Some(vec![ContainerPort {
                        container_port: 80,
                        host_port: Some(8080),
                        ..Default::default()
                    }]),
                    security_context: Some(SecurityContext {
                        run_as_non_root: Some(false),
                        privileged: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                init_containers: Some(vec![Container {
                    name: "init".into(),
                    image: Some("busybox".into()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            status: Some(PodStatus {
                container_statuses: Some(vec![ContainerStatus {
                    name: "web".into(),
                    image_id: format!("docker-pullable://ghcr.io/eldr/lockbox@sha256:{D}"),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
        };
        let p = SecPod::from_pod(&pod);
        assert_eq!(p.owner.as_ref().unwrap().uid, "rs1");
        assert!(p.manifest.is_none(), "owned pods keep no manifest");
        assert_eq!(p.containers.len(), 2);
        let (init, web) = (&p.containers[0], &p.containers[1]);
        assert!(init.init && init.digest.is_none());
        assert_eq!(init.security.run_as_non_root, Some(true), "inherits pod");
        assert_eq!(
            web.digest.as_ref().unwrap().repository,
            "ghcr.io/eldr/lockbox"
        );
        assert_eq!(web.host_ports, vec![8080]);
        assert_eq!(
            web.security.run_as_non_root,
            Some(false),
            "container overrides pod"
        );
        assert_eq!(web.security.privileged, Some(true));
    }

    #[test]
    fn services_and_ingress_routes() {
        let svc = SecService::from_service(&Service {
            metadata: ObjectMeta {
                namespace: Some("a".into()),
                name: Some("s".into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                type_: Some("NodePort".into()),
                ports: Some(vec![ServicePort {
                    port: 80,
                    node_port: Some(30080),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert!(svc.is_externally_exposed());
        assert_eq!(svc.ports[0].node_port, Some(30080));

        let ing: Ingress = serde_json::from_value(serde_json::json!({
            "metadata": {"namespace": "lockbox", "name": "web"},
            "spec": {
                "defaultBackend": {"service": {"name": "fallback", "port": {"number": 80}}},
                "rules": [{"host": "lockbox.eldr.io", "http": {"paths": [
                    {"path": "/", "pathType": "Prefix", "backend": {"service": {"name": "lockbox-web", "port": {"number": 80}}}},
                    {"path": "/api", "pathType": "Prefix", "backend": {"service": {"name": "lockbox-web", "port": {"number": 80}}}}
                ]}}]
            }
        }))
        .unwrap();
        let routes = SecIngress::from_ingress(&ing).routes;
        assert_eq!(routes.len(), 2, "{routes:?}");
        assert_eq!(routes[0].host, None);
        assert_eq!(routes[1].host.as_deref(), Some("lockbox.eldr.io"));
        assert_eq!(routes[1].service, "lockbox-web");
    }

    #[test]
    fn http_routes() {
        let obj: DynamicObject = serde_json::from_value(serde_json::json!({
            "apiVersion": "gateway.networking.k8s.io/v1", "kind": "HTTPRoute",
            "metadata": {"namespace": "heimdall", "name": "zitadel"},
            "spec": {
                "hostnames": ["auth.eldr.io"],
                "rules": [{"backendRefs": [
                    {"name": "zitadel", "port": 8080},
                    {"name": "other", "namespace": "shared", "port": 80},
                    {"name": "ext", "kind": "ServiceImport", "group": "multicluster.x-k8s.io"}
                ]}]
            }
        }))
        .unwrap();
        let r = SecHttpRoute::from_dynamic(&obj);
        assert_eq!(r.routes.len(), 2);
        assert_eq!(r.routes[0].host.as_deref(), Some("auth.eldr.io"));
        assert_eq!(r.routes[1].service_namespace, "shared");
    }

    #[test]
    fn network_policy_types() {
        let np = |types: Option<Vec<&str>>, egress: bool| {
            SecNetworkPolicy::from_network_policy(
                &serde_json::from_value(serde_json::json!({
                    "metadata": {"namespace": "a", "name": "p"},
                    "spec": {
                        "podSelector": {"matchLabels": {"app": "web"}},
                        "policyTypes": types,
                        "egress": if egress { Some(serde_json::json!([{}])) } else { None },
                    }
                }))
                .unwrap(),
            )
        };
        let unset = np(None, false);
        assert!(unset.covers_ingress && !unset.covers_egress);
        assert!(np(None, true).covers_egress);
        let egress_only = np(Some(vec!["Egress"]), true);
        assert!(!egress_only.covers_ingress && egress_only.covers_egress);
    }
}
