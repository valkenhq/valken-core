use std::fmt;
use std::str::FromStr;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Pod, Service};
use k8s_openapi::api::networking::v1::Ingress;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

use super::KubeResourceType;

/// Identity of one Kubernetes resource within one cluster — the key of the
/// resource store and the node weight of the graph.
///
/// Serialised as `<cluster_id>::<namespace|__cluster__>::<group>/<version>::<kind>::<name>`
/// (see [`fmt::Display`] and [`FromStr`]).
///
/// # Case
///
/// `api_group`, `version`, `kind` and `name` are lower-cased on construction
/// so lookups are case-insensitive and the string form is stable.  Most
/// Kubernetes names are DNS labels and therefore already lower-case, but
/// RBAC objects and some custom resources allow mixed case; a KRI is a
/// store key, **not** a value to call the API back with.  `namespace` is
/// kept verbatim (namespaces are always DNS labels).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KubeResourceIdentifier {
    /// Local cluster identifier from the `clusters` DB table.  `0` means
    /// "unassigned / pre-connection".  Persisted as part of the KRI string so
    /// that resources from different clusters never collide in the same store.
    pub cluster_id: u32,
    pub namespace: Option<String>, // None for cluster-scoped
    pub api_group: String,
    pub version: String,
    pub kind: String,
    pub name: String,
}

/// A string that is not a well-formed KRI.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("malformed KubeResourceIdentifier: {0:?}")]
pub struct ParseKriError(pub String);

impl FromStr for KubeResourceIdentifier {
    type Err = ParseKriError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Expected: <cluster_id>::<ns_or_cluster>::<group/version>::<kind>::<name>
        let parts: Vec<&str> = s.split("::").collect();
        if parts.len() != 5 {
            return Err(ParseKriError(s.to_string()));
        }
        let cluster_id: u32 = parts[0].parse().map_err(|_| ParseKriError(s.to_string()))?;
        let namespace = if parts[1] == "__cluster__" {
            None
        } else {
            Some(parts[1].to_string())
        };
        let (group, version) = parts[2]
            .split_once('/')
            .ok_or_else(|| ParseKriError(s.to_string()))?;
        if group.contains('/') || version.contains('/') {
            return Err(ParseKriError(s.to_string()));
        }
        Ok(Self::new(
            cluster_id, namespace, group, version, parts[3], parts[4],
        ))
    }
}

impl fmt::Display for KubeResourceIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}::{}::{}/{}::{}::{}",
            self.cluster_id,
            self.frame_key(),
            self.api_group,
            self.version,
            self.kind,
            self.name
        )
    }
}

impl KubeResourceIdentifier {
    pub fn new(
        cluster_id: u32,
        namespace: Option<String>,
        api_group: &str,
        version: &str,
        kind: &str,
        name: &str,
    ) -> Self {
        Self {
            cluster_id,
            namespace,
            api_group: api_group.to_lowercase(),
            version: version.to_lowercase(),
            kind: kind.to_lowercase(),
            name: name.to_lowercase(),
        }
    }

    /// Parse the [`fmt::Display`] form; `None` if malformed.
    ///
    /// Kept as an inherent `Option`-returning method because valken-pro
    /// calls it directly; the [`FromStr`] impl is the idiomatic entry point.
    #[allow(clippy::should_implement_trait)]
    #[deprecated(note = "use `s.parse::<KubeResourceIdentifier>()`")]
    pub fn from_str(s: &str) -> Option<Self> {
        <Self as FromStr>::from_str(s).ok()
    }

    pub fn get_resource_type(&self) -> KubeResourceType {
        match self.kind.as_str() {
            "pod" => KubeResourceType::Pod,
            "service" => KubeResourceType::Service,
            "deployment" => KubeResourceType::Deployment,
            "statefulset" => KubeResourceType::StatefulSet,
            "daemonset" => KubeResourceType::DaemonSet,
            "replicaset" => KubeResourceType::ReplicaSet,
            "job" => KubeResourceType::Job,
            "cronjob" => KubeResourceType::CronJob,
            "configmap" => KubeResourceType::ConfigMap,
            "secret" => KubeResourceType::Secret,
            "namespace" => KubeResourceType::Namespace,
            "node" => KubeResourceType::Node,
            "persistentvolume" => KubeResourceType::PersistentVolume,
            "persistentvolumeclaim" => KubeResourceType::PersistentVolumeClaim,
            "ingress" => KubeResourceType::Ingress,
            "networkpolicy" => KubeResourceType::NetworkPolicy,
            "resourcequota" => KubeResourceType::ResourceQuota,
            "limitrange" => KubeResourceType::LimitRange,
            _ => KubeResourceType::Unknown(self.kind.clone()),
        }
    }

    pub fn is_pod(&self) -> bool {
        self.kind == "pod"
    }

    pub fn is_service(&self) -> bool {
        self.kind == "service"
    }

    pub fn is_deployment(&self) -> bool {
        self.kind == "deployment"
    }

    pub fn is_ingress(&self) -> bool {
        self.kind == "ingress"
    }

    pub fn from_pod(cluster_id: u32, pod: &Pod) -> Option<Self> {
        let name = pod.metadata.name.as_ref()?;
        let namespace = pod.metadata.namespace.clone();
        Some(Self::new(cluster_id, namespace, "core", "v1", "Pod", name))
    }

    pub fn from_service(cluster_id: u32, svc: &Service) -> Option<Self> {
        let name = svc.metadata.name.as_ref()?;
        let namespace = svc.metadata.namespace.clone();
        Some(Self::new(
            cluster_id, namespace, "core", "v1", "Service", name,
        ))
    }

    pub fn from_deployment(cluster_id: u32, dep: &Deployment) -> Option<Self> {
        let name = dep.metadata.name.as_ref()?;
        let namespace = dep.metadata.namespace.clone();
        Some(Self::new(
            cluster_id,
            namespace,
            "apps",
            "v1",
            "Deployment",
            name,
        ))
    }

    pub fn from_ingress(cluster_id: u32, ing: &Ingress) -> Option<Self> {
        let name = ing.metadata.name.as_ref()?;
        let namespace = ing.metadata.namespace.clone();
        Some(Self::new(
            cluster_id,
            namespace,
            "networking.k8s.io",
            "v1",
            "Ingress",
            name,
        ))
    }

    /// Build a [`KubeResourceIdentifier`] for any resource that does not have
    /// a dedicated constructor.  Callers supply the raw group/version/kind
    /// triple and the object's name and namespace as parsed from the raw API
    /// response or from a `KubeResource::CustomResource` value.
    ///
    /// `_creation_timestamp` is accepted for signature stability with
    /// existing callers; it does not take part in the identity.
    pub fn from_custom_resource(
        cluster_id: u32,
        namespace: Option<String>,
        api_group: &str,
        version: &str,
        kind: &str,
        name: &str,
        _creation_timestamp: Option<&Time>,
    ) -> Option<Self> {
        if name.is_empty() || kind.is_empty() {
            return None;
        }
        Some(Self::new(
            cluster_id, namespace, api_group, version, kind, name,
        ))
    }

    /// Build a KRI that represents a Kubernetes Namespace resource.
    /// Namespaces are cluster-scoped (`namespace: None`), so this KRI is
    /// stable across any namespaced context.
    pub fn for_namespace(cluster_id: u32, name: &str) -> Self {
        Self::new(cluster_id, None, "core", "v1", "Namespace", name)
    }

    pub fn is_cluster_scoped(&self) -> bool {
        self.namespace.is_none()
    }

    /// Returns the frame key used to look up this resource's namespace frame
    /// in the graph.  Namespaced resources use their namespace string;
    /// cluster-scoped resources (where `namespace` is `None`) use the sentinel
    /// `"__cluster__"` — the same value that `Display` emits and that
    /// `KubeResourceStore` registers as the namespace key for such resources.
    ///
    /// **Always use this method instead of
    /// `kri.namespace.clone().unwrap_or_else(|| "default".to_string())`**
    /// when mapping a KRI to a namespace frame, to avoid silently routing
    /// cluster-scoped resources into a real namespace frame called "default".
    pub fn frame_key(&self) -> &str {
        self.namespace.as_deref().unwrap_or("__cluster__")
    }

    #[deprecated(note = "same as to_string()")]
    pub fn resource_id_key(&self) -> String {
        self.to_string()
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use k8s_openapi::api::apps::v1::Deployment;
    use k8s_openapi::api::core::v1::{Pod, Service};
    use k8s_openapi::api::networking::v1::{
        HTTPIngressPath, HTTPIngressRuleValue, Ingress, IngressBackend, IngressRule,
        IngressServiceBackend, IngressSpec, ServiceBackendPort,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::HashSet;

    #[test]
    fn test_creation_and_display_namespaced() {
        let kri = KubeResourceIdentifier::new(
            1,
            Some("demo".into()),
            "apps",
            "v1",
            "Deployment",
            "whoami",
        );
        assert_eq!(kri.to_string(), "1::demo::apps/v1::deployment::whoami");
        assert!(!kri.is_cluster_scoped());
    }

    #[test]
    fn test_creation_and_display_cluster_scoped() {
        let kri = KubeResourceIdentifier::new(
            1,
            None,
            "rbac.authorization.k8s.io",
            "v1",
            "ClusterRole",
            "admin",
        );
        assert_eq!(
            kri.to_string(),
            "1::__cluster__::rbac.authorization.k8s.io/v1::clusterrole::admin"
        );
        assert!(kri.is_cluster_scoped());
    }

    #[test]
    fn test_parsing_namespaced() {
        let s = "1::demo::apps/v1::deployment::whoami";
        let kri = KubeResourceIdentifier::from_str(s).unwrap();
        assert_eq!(kri.cluster_id, 1);
        assert_eq!(kri.namespace, Some("demo".to_string()));
        assert_eq!(kri.api_group, "apps");
        assert_eq!(kri.version, "v1");
        assert_eq!(kri.kind, "deployment");
        assert_eq!(kri.name, "whoami");
    }

    #[test]
    fn test_parsing_cluster_scoped() {
        let s = "1::__cluster__::rbac.authorization.k8s.io/v1::clusterrole::admin";
        let kri = KubeResourceIdentifier::from_str(s).unwrap();
        assert_eq!(kri.cluster_id, 1);
        assert_eq!(kri.namespace, None);
        assert_eq!(kri.api_group, "rbac.authorization.k8s.io");
        assert_eq!(kri.version, "v1");
        assert_eq!(kri.kind, "clusterrole");
        assert_eq!(kri.name, "admin");
    }

    #[test]
    fn test_invalid_format_too_few_parts() {
        let s = "1::demo::apps/v1::deployment"; // missing name (only 4 parts)
        assert!(KubeResourceIdentifier::from_str(s).is_none());
    }

    #[test]
    fn test_invalid_format_bad_group_version() {
        let s = "1::demo::appsv1::deployment::whoami"; // group/version missing slash
        assert!(KubeResourceIdentifier::from_str(s).is_none());
    }

    #[test]
    fn test_invalid_format_bad_cluster_id() {
        let s = "abc::demo::apps/v1::deployment::whoami"; // cluster_id not an integer
        assert!(KubeResourceIdentifier::from_str(s).is_none());
    }

    #[test]
    fn test_from_str_trait_reports_the_input() {
        let err = "nope".parse::<KubeResourceIdentifier>().unwrap_err();
        assert_eq!(err, ParseKriError("nope".into()));
        let ok: KubeResourceIdentifier = "1::demo::apps/v1::deployment::whoami".parse().unwrap();
        assert_eq!(ok.name, "whoami");
        // Extra slashes in the group/version are rejected.
        assert!("1::demo::a/b/c::deployment::x"
            .parse::<KubeResourceIdentifier>()
            .is_err());
    }

    #[test]
    fn test_equality() {
        let kri1 = KubeResourceIdentifier::new(
            1,
            Some("demo".into()),
            "apps",
            "v1",
            "deployment",
            "whoami",
        );
        let kri2 =
            KubeResourceIdentifier::from_str("1::demo::apps/v1::deployment::whoami").unwrap();
        assert_eq!(kri1, kri2);
    }

    #[test]
    fn test_equality_different_cluster_ids_not_equal() {
        let kri1 =
            KubeResourceIdentifier::from_str("1::demo::apps/v1::deployment::whoami").unwrap();
        let kri2 =
            KubeResourceIdentifier::from_str("2::demo::apps/v1::deployment::whoami").unwrap();
        assert_ne!(kri1, kri2);
    }

    #[test]
    fn test_hashability() {
        let kri1 =
            KubeResourceIdentifier::from_str("1::demo::apps/v1::deployment::whoami").unwrap();
        let kri2 =
            KubeResourceIdentifier::from_str("1::demo::apps/v1::deployment::whoami").unwrap();
        let kri3 =
            KubeResourceIdentifier::from_str("1::demo::apps/v1::deployment::whoami-2").unwrap();

        let mut set = HashSet::new();
        set.insert(kri1);
        set.insert(kri2); // should not add duplicate
        set.insert(kri3); // should add new item

        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_resource_id_key_matches_display() {
        let kri =
            KubeResourceIdentifier::new(1, Some("ns".into()), "apps", "v1", "Deployment", "test");
        assert_eq!(kri.resource_id_key(), kri.to_string());
    }

    #[test]
    fn test_from_pod() {
        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("mypod".to_string()),
                namespace: Some("mynamespace".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let kri = KubeResourceIdentifier::from_pod(1, &pod).unwrap();
        assert_eq!(kri.cluster_id, 1);
        assert_eq!(kri.namespace.as_deref(), Some("mynamespace"));
        assert_eq!(kri.api_group, "core");
        assert_eq!(kri.version, "v1");
        assert_eq!(kri.kind, "pod");
        assert_eq!(kri.name, "mypod");
    }

    #[test]
    fn test_from_service() {
        let svc = Service {
            metadata: ObjectMeta {
                name: Some("myservice".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let kri = KubeResourceIdentifier::from_service(1, &svc).unwrap();
        assert_eq!(kri.namespace.as_deref(), Some("default"));
        assert_eq!(kri.api_group, "core");
        assert_eq!(kri.version, "v1");
        assert_eq!(kri.kind, "service");
        assert_eq!(kri.name, "myservice");
    }

    #[test]
    fn test_from_deployment() {
        let dep = Deployment {
            metadata: ObjectMeta {
                name: Some("mydeployment".to_string()),
                namespace: Some("ns1".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let kri = KubeResourceIdentifier::from_deployment(1, &dep).unwrap();
        assert_eq!(kri.namespace.as_deref(), Some("ns1"));
        assert_eq!(kri.api_group, "apps");
        assert_eq!(kri.version, "v1");
        assert_eq!(kri.kind, "deployment");
        assert_eq!(kri.name, "mydeployment");
    }

    #[test]
    fn test_from_ingress() {
        let ing = Ingress {
            metadata: ObjectMeta {
                name: Some("myingress".to_string()),
                namespace: Some("ns2".to_string()),
                ..Default::default()
            },
            spec: Some(IngressSpec {
                rules: Some(vec![IngressRule {
                    host: Some("example.com".to_string()),
                    http: Some(HTTPIngressRuleValue {
                        paths: vec![HTTPIngressPath {
                            backend: IngressBackend {
                                service: Some(IngressServiceBackend {
                                    name: "myservice".to_string(),
                                    port: Some(ServiceBackendPort {
                                        number: Some(80),
                                        ..Default::default()
                                    }),
                                }),
                                ..Default::default()
                            },
                            ..Default::default()
                        }],
                    }),
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let kri = KubeResourceIdentifier::from_ingress(1, &ing).unwrap();
        assert_eq!(kri.namespace.as_deref(), Some("ns2"));
        assert_eq!(kri.api_group, "networking.k8s.io");
        assert_eq!(kri.version, "v1");
        assert_eq!(kri.kind, "ingress");
        assert_eq!(kri.name, "myingress");
    }

    #[test]
    fn test_pod_missing_name() {
        let pod = Pod {
            metadata: ObjectMeta {
                namespace: Some("ns".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(KubeResourceIdentifier::from_pod(1, &pod).is_none());
    }

    #[test]
    fn test_frame_key_namespaced() {
        let kri = KubeResourceIdentifier::new(
            1,
            Some("production".into()),
            "apps",
            "v1",
            "Deployment",
            "api",
        );
        assert_eq!(kri.frame_key(), "production");
    }

    #[test]
    fn test_frame_key_cluster_scoped() {
        let kri = KubeResourceIdentifier::new(
            1,
            None,
            "rbac.authorization.k8s.io",
            "v1",
            "ClusterRole",
            "admin",
        );
        // Must return "__cluster__" — NOT "default" — so cluster-scoped
        // resources are never silently placed in a real namespace frame.
        assert_eq!(kri.frame_key(), "__cluster__");
    }

    #[test]
    fn test_frame_key_default_namespace_stays_default() {
        // A resource that genuinely lives in the "default" namespace must not
        // be confused with a cluster-scoped resource.
        let kri = KubeResourceIdentifier::new(
            1,
            Some("default".into()),
            "core",
            "v1",
            "Service",
            "kubernetes",
        );
        assert_eq!(kri.frame_key(), "default");
    }

    #[test]
    fn test_frame_key_matches_display_sentinel() {
        // The display string is "1::__cluster__::core/v1::node::worker-1".
        // frame_key() returns "__cluster__" — it must appear inside the display
        // string, though no longer as the prefix (the cluster_id prefix comes first).
        let kri = KubeResourceIdentifier::new(1, None, "core", "v1", "Node", "worker-1");
        let display = kri.to_string(); // "1::__cluster__::core/v1::node::worker-1"
        assert!(display.contains(kri.frame_key()));
    }

    #[test]
    fn test_pod_missing_namespace() {
        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("mypod".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let kri = KubeResourceIdentifier::from_pod(1, &pod).unwrap();
        assert_eq!(kri.namespace, None);
    }

    #[test]
    fn test_service_missing_name() {
        let svc = Service {
            metadata: ObjectMeta {
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(KubeResourceIdentifier::from_service(1, &svc).is_none());
    }

    #[test]
    fn test_deployment_missing_name() {
        let dep = Deployment {
            metadata: ObjectMeta {
                namespace: Some("apps".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(KubeResourceIdentifier::from_deployment(1, &dep).is_none());
    }

    #[test]
    fn test_ingress_missing_name() {
        let ing = Ingress {
            metadata: ObjectMeta {
                namespace: Some("web".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(KubeResourceIdentifier::from_ingress(1, &ing).is_none());
    }

    #[test]
    fn test_ingress_missing_namespace() {
        let ing = Ingress {
            metadata: ObjectMeta {
                name: Some("mying".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let kri = KubeResourceIdentifier::from_ingress(1, &ing).unwrap();
        assert_eq!(kri.namespace, None);
    }

    // ── from_pod / from_service display format ────────────────────────────────

    #[test]
    fn test_from_pod_display_uses_core_group() {
        // Serialised form must be "core/v1", never "/v1", so that from_str can
        // round-trip it and so it is consistent with from_custom_resource output
        // for core API resources.
        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("mypod".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let kri = KubeResourceIdentifier::from_pod(1, &pod).unwrap();
        assert_eq!(kri.to_string(), "1::default::core/v1::pod::mypod");
    }

    #[test]
    fn test_from_service_display_uses_core_group() {
        let svc = Service {
            metadata: ObjectMeta {
                name: Some("mysvc".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let kri = KubeResourceIdentifier::from_service(1, &svc).unwrap();
        assert_eq!(kri.to_string(), "1::default::core/v1::service::mysvc");
    }

    // ── from_custom_resource ──────────────────────────────────────────────────

    #[test]
    fn test_from_custom_resource_grouped_api() {
        // A resource whose api_version contains a slash (e.g. "apps/v1") should
        // use the left-hand side as the group verbatim.
        let kri = KubeResourceIdentifier::from_custom_resource(
            1,
            Some("demo".to_string()),
            "apps",
            "v1",
            "Deployment",
            "my-deploy",
            None,
        )
        .unwrap();
        assert_eq!(kri.namespace.as_deref(), Some("demo"));
        assert_eq!(kri.api_group, "apps");
        assert_eq!(kri.version, "v1");
        assert_eq!(kri.kind, "deployment");
        assert_eq!(kri.name, "my-deploy");
        assert_eq!(kri.to_string(), "1::demo::apps/v1::deployment::my-deploy");
    }

    #[test]
    fn test_from_custom_resource_core_api() {
        // Core API resources (api_version with no slash, e.g. "v1") are stored
        // with group "core" so the KRI string matches from_pod / from_service
        // and is always parseable by from_str.
        let kri = KubeResourceIdentifier::from_custom_resource(
            1,
            Some("default".to_string()),
            "core",
            "v1",
            "ConfigMap",
            "my-config",
            None,
        )
        .unwrap();
        assert_eq!(kri.api_group, "core");
        assert_eq!(kri.version, "v1");
        assert_eq!(kri.kind, "configmap");
        assert_eq!(kri.to_string(), "1::default::core/v1::configmap::my-config");
    }

    #[test]
    fn test_from_custom_resource_core_api_round_trips() {
        // A KRI built with group "core" must survive a to_string / from_str
        // round-trip — this is the path exercised by the graph builder every
        // time it resolves a KRI back from a GString signal argument.
        let original = KubeResourceIdentifier::from_custom_resource(
            1,
            Some("default".to_string()),
            "core",
            "v1",
            "ConfigMap",
            "my-config",
            None,
        )
        .unwrap();
        let roundtripped = KubeResourceIdentifier::from_str(&original.to_string()).unwrap();
        assert_eq!(original, roundtripped);
    }

    #[test]
    fn test_from_custom_resource_missing_name() {
        assert!(KubeResourceIdentifier::from_custom_resource(
            1,
            Some("ns".to_string()),
            "apps",
            "v1",
            "Deployment",
            "",
            None,
        )
        .is_none());
    }

    #[test]
    fn test_from_custom_resource_missing_kind() {
        assert!(KubeResourceIdentifier::from_custom_resource(
            1,
            Some("ns".to_string()),
            "apps",
            "v1",
            "",
            "my-deploy",
            None,
        )
        .is_none());
    }

    #[test]
    fn test_from_custom_resource_cluster_scoped() {
        // Cluster-scoped CRDs (no namespace) should use the "__cluster__"
        // sentinel in their display string and return it from frame_key().
        let kri = KubeResourceIdentifier::from_custom_resource(
            1,
            None,
            "apiextensions.k8s.io",
            "v1",
            "CustomResourceDefinition",
            "foos.example.com",
            None,
        )
        .unwrap();
        assert!(kri.is_cluster_scoped());
        assert_eq!(kri.frame_key(), "__cluster__");
        assert_eq!(
            kri.to_string(),
            "1::__cluster__::apiextensions.k8s.io/v1::customresourcedefinition::foos.example.com"
        );
    }

    #[test]
    fn test_from_custom_resource_core_matches_from_pod() {
        // from_custom_resource called with group "core" (the value that
        // kube_resource_store derives when api_version has no slash) must
        // produce a KRI identical to from_pod for the same object.
        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("mypod".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let from_pod_kri = KubeResourceIdentifier::from_pod(1, &pod).unwrap();
        let from_custom_kri = KubeResourceIdentifier::from_custom_resource(
            1,
            Some("default".to_string()),
            "core",
            "v1",
            "Pod",
            "mypod",
            None,
        )
        .unwrap();
        assert_eq!(from_pod_kri, from_custom_kri);
        assert_eq!(from_pod_kri.to_string(), from_custom_kri.to_string());
    }
}
