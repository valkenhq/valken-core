//! Ingress controller detection — identifies *which* ingress implementation is
//! running in a cluster from workload labels/images and from the Ingress
//! resource itself.

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Container, Pod};
use k8s_openapi::api::networking::v1::Ingress;

// ── Ingress controller kind ───────────────────────────────────────────────────

/// The ingress controller implementation detected in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressControllerKind {
    /// NGINX Ingress Controller (ingress-nginx or nginx-ingress)
    Nginx,
    /// Traefik proxy
    Traefik,
    /// HAProxy Ingress Controller
    HaProxy,
    /// Istio Ingress Gateway
    Istio,
    /// Contour (Envoy-based, from Projectcontour)
    Contour,
    /// Kong Ingress Controller
    Kong,
    /// Ambassador / Emissary Ingress
    Ambassador,
    /// Could not determine from available metadata
    Unknown,
}

impl IngressControllerKind {
    /// Human-readable display name.
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Nginx => "NGINX",
            Self::Traefik => "Traefik",
            Self::HaProxy => "HAProxy",
            Self::Istio => "Istio",
            Self::Contour => "Contour",
            Self::Kong => "Kong",
            Self::Ambassador => "Ambassador",
            Self::Unknown => "Unknown",
        }
    }

    /// Path to the logo asset relative to `res://` in the Valken Godot
    /// project.  (A UI concern that lives here so the enum stays the single
    /// source of truth for the controller list.)
    pub fn logo_path(&self) -> Option<&'static str> {
        match self {
            Self::Nginx => Some("res://assets/icons/ingress/nginx.svg"),
            Self::Traefik => Some("res://assets/icons/ingress/traefik.svg"),
            Self::HaProxy => Some("res://assets/icons/ingress/haproxy.svg"),
            Self::Istio => Some("res://assets/icons/ingress/istio.svg"),
            Self::Contour => Some("res://assets/icons/ingress/contour.svg"),
            Self::Kong => Some("res://assets/icons/ingress/kong.svg"),
            Self::Ambassador => Some("res://assets/icons/ingress/ambassador.svg"),
            Self::Unknown => None,
        }
    }
}

// ── Detection ─────────────────────────────────────────────────────────────────

/// Returns `true` when the pod appears to be an ingress controller pod.
pub fn is_ingress_controller_pod(pod: &Pod) -> bool {
    detect_ingress_controller_from_pod(pod).is_some()
}

/// Detect the ingress controller kind from a running pod.
///
/// Checks (in order of reliability):
/// 1. `app.kubernetes.io/name` label
/// 2. `app` label
/// 3. `app.kubernetes.io/part-of` label
/// 4. Container image names
pub fn detect_ingress_controller_from_pod(pod: &Pod) -> Option<IngressControllerKind> {
    detect_ingress_controller(
        pod.metadata.labels.as_ref(),
        pod.spec.as_ref().map(|s| s.containers.as_slice()),
    )
}

/// Detect the ingress controller kind from a Deployment's pod template,
/// using the same rules as [`detect_ingress_controller_from_pod`].
pub fn detect_ingress_controller_from_deployment(
    dep: &Deployment,
) -> Option<IngressControllerKind> {
    let template = dep.spec.as_ref().map(|s| &s.template);
    detect_ingress_controller(
        template
            .and_then(|t| t.metadata.as_ref())
            .and_then(|m| m.labels.as_ref()),
        template
            .and_then(|t| t.spec.as_ref())
            .map(|s| s.containers.as_slice()),
    )
}

/// Shared detection over a workload's labels and containers.
///
/// A bare `nginx` label value is deliberately **not** a match: it is the
/// most common label for a plain nginx web server, whose access logs would
/// otherwise be tailed and counted as ingress traffic.
pub fn detect_ingress_controller(
    labels: Option<&BTreeMap<String, String>>,
    containers: Option<&[Container]>,
) -> Option<IngressControllerKind> {
    // ── Label-based detection (most reliable) ─────────────────────────────
    if let Some(labels) = labels {
        let label_values = [
            "app.kubernetes.io/name",
            "app",
            "app.kubernetes.io/part-of",
            "app.kubernetes.io/instance",
        ]
        .iter()
        .filter_map(|key| labels.get(*key).map(|v| v.as_str()));

        for lv in label_values {
            let lv = lv.to_lowercase();
            if lv.contains("ingress-nginx") || lv.contains("nginx-ingress") {
                return Some(IngressControllerKind::Nginx);
            }
            if lv.contains("traefik") {
                return Some(IngressControllerKind::Traefik);
            }
            if lv.contains("haproxy") {
                return Some(IngressControllerKind::HaProxy);
            }
            // Require "ingressgateway" specifically — a broad "istio" match
            // would catch istiod, istio-pilot, and any workload whose name
            // contains "istio" (e.g. app pods in an Istio-mesh namespace).
            if lv.contains("ingressgateway") {
                return Some(IngressControllerKind::Istio);
            }
            if lv.contains("contour") {
                return Some(IngressControllerKind::Contour);
            }
            if lv.contains("kong") {
                return Some(IngressControllerKind::Kong);
            }
            if lv.contains("ambassador") || lv.contains("emissary") {
                return Some(IngressControllerKind::Ambassador);
            }
        }
    }

    // ── Image-based detection (fallback) ──────────────────────────────────
    if let Some(containers) = containers {
        for container in containers {
            let image = container.image.as_deref().unwrap_or("").to_lowercase();
            if image.contains("ingress-nginx") || image.contains("nginx-ingress") {
                return Some(IngressControllerKind::Nginx);
            }
            if image.contains("traefik") {
                return Some(IngressControllerKind::Traefik);
            }
            if image.contains("haproxy") {
                return Some(IngressControllerKind::HaProxy);
            }
            // Istio: no image-based fallback.  proxyv2/pilot/istio images are
            // shared with every sidecar-injected workload pod and cannot
            // distinguish a gateway from a regular app.  Detection relies
            // entirely on the "ingressgateway" label check above.
            if image.contains("contour") {
                return Some(IngressControllerKind::Contour);
            }
            if image.contains("kong") {
                return Some(IngressControllerKind::Kong);
            }
            if image.contains("ambassador") || image.contains("emissary") {
                return Some(IngressControllerKind::Ambassador);
            }
        }
    }

    None
}

/// Detect the ingress controller kind from a Kubernetes Ingress resource.
///
/// Checks (in order):
/// 1. `spec.ingressClassName` (k8s 1.18+ preferred field)
/// 2. `kubernetes.io/ingress.class` annotation (legacy)
/// 3. Controller-specific annotation key prefixes
pub fn detect_ingress_controller_from_ingress(ingress: &Ingress) -> Option<IngressControllerKind> {
    // 1. spec.ingressClassName
    if let Some(class_name) = ingress
        .spec
        .as_ref()
        .and_then(|s| s.ingress_class_name.as_deref())
    {
        if let Some(kind) = match_class_name(class_name) {
            return Some(kind);
        }
    }

    let annotations = ingress.metadata.annotations.as_ref()?;

    // 2. Legacy annotation
    if let Some(class) = annotations.get("kubernetes.io/ingress.class") {
        if let Some(kind) = match_class_name(class) {
            return Some(kind);
        }
    }

    // 3. Controller-specific annotation key prefixes
    for key in annotations.keys() {
        let k = key.to_lowercase();
        if k.contains("nginx.ingress.kubernetes.io") || k.contains("nginx.org") {
            return Some(IngressControllerKind::Nginx);
        }
        if k.contains("traefik.ingress.kubernetes.io") {
            return Some(IngressControllerKind::Traefik);
        }
        if k.contains("haproxy.org") {
            return Some(IngressControllerKind::HaProxy);
        }
        if k.contains("istio.io") {
            return Some(IngressControllerKind::Istio);
        }
        if k.contains("projectcontour.io") || k.contains("contour.heptio.com") {
            return Some(IngressControllerKind::Contour);
        }
        if k.contains("konghq.com") {
            return Some(IngressControllerKind::Kong);
        }
        if k.contains("getambassador.io") || k.contains("emissary") {
            return Some(IngressControllerKind::Ambassador);
        }
    }

    None
}

// ── Private helpers ───────────────────────────────────────────────────────────

fn match_class_name(class: &str) -> Option<IngressControllerKind> {
    let class = class.to_lowercase();
    if class.contains("nginx") {
        Some(IngressControllerKind::Nginx)
    } else if class.contains("traefik") {
        Some(IngressControllerKind::Traefik)
    } else if class.contains("haproxy") {
        Some(IngressControllerKind::HaProxy)
    } else if class.contains("istio") {
        Some(IngressControllerKind::Istio)
    } else if class.contains("contour") {
        Some(IngressControllerKind::Contour)
    } else if class.contains("kong") {
        Some(IngressControllerKind::Kong)
    } else if class.contains("ambassador") || class.contains("emissary") {
        Some(IngressControllerKind::Ambassador)
    } else {
        None
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{Container, Pod, PodSpec};
    use k8s_openapi::api::networking::v1::{Ingress, IngressSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Build a pod with the given label key/value pairs and no containers.
    fn pod_with_labels(labels: &[(&str, &str)]) -> Pod {
        Pod {
            metadata: ObjectMeta {
                labels: Some(
                    labels
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                ),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Build a pod with a single container image and no labels.
    fn pod_with_image(image: &str) -> Pod {
        Pod {
            spec: Some(PodSpec {
                containers: vec![Container {
                    image: Some(image.to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Build an ingress with only `spec.ingressClassName` set.
    fn ingress_with_class_name(class: &str) -> Ingress {
        Ingress {
            spec: Some(IngressSpec {
                ingress_class_name: Some(class.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Build an ingress with only the given annotation key/value pairs.
    fn ingress_with_annotations(annotations: &[(&str, &str)]) -> Ingress {
        Ingress {
            metadata: ObjectMeta {
                annotations: Some(
                    annotations
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                ),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    // =========================================================================
    // detect_ingress_controller_from_pod — label-based detection
    // =========================================================================

    // ── app.kubernetes.io/name ────────────────────────────────────────────────

    #[test]
    fn test_pod_label_app_name_ingress_nginx() {
        let pod = pod_with_labels(&[("app.kubernetes.io/name", "ingress-nginx")]);
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Nginx)
        );
    }

    #[test]
    fn test_pod_label_app_name_nginx_ingress() {
        let pod = pod_with_labels(&[("app.kubernetes.io/name", "nginx-ingress")]);
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Nginx)
        );
    }

    #[test]
    fn test_pod_label_app_name_traefik() {
        let pod = pod_with_labels(&[("app.kubernetes.io/name", "traefik")]);
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Traefik)
        );
    }

    #[test]
    fn test_pod_label_app_name_haproxy() {
        let pod = pod_with_labels(&[("app.kubernetes.io/name", "haproxy-ingress")]);
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::HaProxy)
        );
    }

    #[test]
    fn test_pod_label_app_name_istio() {
        let pod = pod_with_labels(&[("app.kubernetes.io/name", "istio-ingressgateway")]);
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Istio)
        );
    }

    #[test]
    fn test_pod_label_app_name_contour() {
        let pod = pod_with_labels(&[("app.kubernetes.io/name", "contour")]);
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Contour)
        );
    }

    #[test]
    fn test_pod_label_app_name_kong() {
        let pod = pod_with_labels(&[("app.kubernetes.io/name", "kong-ingress-controller")]);
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Kong)
        );
    }

    #[test]
    fn test_pod_label_app_name_ambassador() {
        let pod = pod_with_labels(&[("app.kubernetes.io/name", "ambassador")]);
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Ambassador)
        );
    }

    #[test]
    fn test_pod_label_app_name_emissary() {
        let pod = pod_with_labels(&[("app.kubernetes.io/name", "emissary-ingress")]);
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Ambassador)
        );
    }

    // ── app label ─────────────────────────────────────────────────────────────

    #[test]
    fn test_pod_label_app_nginx_bare_is_a_web_server_not_a_controller() {
        // `app=nginx` on an `nginx:1.27` container is the canonical plain
        // web server; it must not be tailed as an ingress controller.
        let mut pod = pod_with_labels(&[("app", "nginx")]);
        pod.spec = Some(PodSpec {
            containers: vec![Container {
                image: Some("nginx:1.27".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        });
        assert_eq!(detect_ingress_controller_from_pod(&pod), None);
        // …while the real controller is still found through its image.
        pod.spec.as_mut().unwrap().containers[0].image =
            Some("registry.k8s.io/ingress-nginx/controller:v1.11.0".into());
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Nginx)
        );
    }

    #[test]
    fn test_deployment_template_is_detected_like_a_pod() {
        use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec};
        use k8s_openapi::api::core::v1::PodTemplateSpec;
        let dep = Deployment {
            spec: Some(DeploymentSpec {
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(BTreeMap::from([(
                            "app.kubernetes.io/name".to_string(),
                            "traefik".to_string(),
                        )])),
                        ..Default::default()
                    }),
                    spec: None,
                },
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            detect_ingress_controller_from_deployment(&dep),
            Some(IngressControllerKind::Traefik)
        );
        assert_eq!(
            detect_ingress_controller_from_deployment(&Deployment::default()),
            None
        );
    }

    #[test]
    fn test_pod_label_app_ingressgateway() {
        // "ingressgateway" alone is an Istio signal
        let pod = pod_with_labels(&[("app", "ingressgateway")]);
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Istio)
        );
    }

    #[test]
    fn test_pod_label_app_traefik() {
        let pod = pod_with_labels(&[("app", "traefik")]);
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Traefik)
        );
    }

    // ── app.kubernetes.io/part-of ─────────────────────────────────────────────

    #[test]
    fn test_pod_label_part_of_ingress_nginx() {
        let pod = pod_with_labels(&[("app.kubernetes.io/part-of", "ingress-nginx")]);
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Nginx)
        );
    }

    #[test]
    fn test_pod_label_part_of_traefik() {
        let pod = pod_with_labels(&[("app.kubernetes.io/part-of", "traefik")]);
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Traefik)
        );
    }

    // ── app.kubernetes.io/instance ────────────────────────────────────────────

    #[test]
    fn test_pod_label_instance_kong() {
        let pod = pod_with_labels(&[("app.kubernetes.io/instance", "kong")]);
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Kong)
        );
    }

    #[test]
    fn test_pod_label_instance_contour() {
        let pod = pod_with_labels(&[("app.kubernetes.io/instance", "contour")]);
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Contour)
        );
    }

    // ── label case-insensitivity ──────────────────────────────────────────────

    #[test]
    fn test_pod_label_case_insensitive() {
        let pod = pod_with_labels(&[("app.kubernetes.io/name", "Ingress-NGINX")]);
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Nginx)
        );
    }

    // =========================================================================
    // detect_ingress_controller_from_pod — image-based detection (no labels)
    // =========================================================================

    #[test]
    fn test_pod_image_ingress_nginx() {
        let pod = pod_with_image("registry.k8s.io/ingress-nginx/controller:v1.9.0");
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Nginx)
        );
    }

    #[test]
    fn test_pod_image_nginx_ingress() {
        let pod = pod_with_image("nginx/nginx-ingress:3.5.0");
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Nginx)
        );
    }

    #[test]
    fn test_pod_image_traefik() {
        let pod = pod_with_image("traefik:v2.10");
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Traefik)
        );
    }

    #[test]
    fn test_pod_image_haproxy() {
        let pod = pod_with_image("haproxytech/haproxy-ingress:latest");
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::HaProxy)
        );
    }

    // Istio sidecar and control-plane images are shared with regular workload
    // pods and must NOT produce a match — only ingressgateway-specific labels
    // can reliably identify an Istio ingress gateway.
    #[test]
    fn test_pod_image_istio_proxyv2_sidecar_returns_none() {
        // proxyv2 is the Envoy sidecar injected into every pod in an Istio mesh.
        let pod = pod_with_image("docker.io/istio/proxyv2:1.18.0");
        assert_eq!(detect_ingress_controller_from_pod(&pod), None);
    }

    #[test]
    fn test_pod_image_istio_pilot_returns_none() {
        // pilot is the Istio control-plane component, not the ingress gateway.
        let pod = pod_with_image("docker.io/istio/pilot:1.18.0");
        assert_eq!(detect_ingress_controller_from_pod(&pod), None);
    }

    #[test]
    fn test_pod_image_istio_proxy_sidecar_returns_none() {
        // istio-proxy is the sidecar container name, not a gateway image.
        let pod = pod_with_image("gcr.io/istio-release/istio-proxy:1.18.0");
        assert_eq!(detect_ingress_controller_from_pod(&pod), None);
    }

    #[test]
    fn test_pod_image_contour() {
        let pod = pod_with_image("ghcr.io/projectcontour/contour:v1.27.0");
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Contour)
        );
    }

    #[test]
    fn test_pod_image_kong() {
        let pod = pod_with_image("kong:3.5");
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Kong)
        );
    }

    #[test]
    fn test_pod_image_ambassador() {
        let pod = pod_with_image("docker.io/datawire/ambassador:1.14.4");
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Ambassador)
        );
    }

    #[test]
    fn test_pod_image_emissary() {
        let pod = pod_with_image("ghcr.io/emissary-ingress/emissary:3.9.1");
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Ambassador)
        );
    }

    // ── image case-insensitivity ──────────────────────────────────────────────

    #[test]
    fn test_pod_image_case_insensitive() {
        let pod = pod_with_image("MyRegistry/TRAEFIK:latest");
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Traefik)
        );
    }

    // ── multiple containers — first match wins ────────────────────────────────

    #[test]
    fn test_pod_image_first_container_wins() {
        let pod = Pod {
            spec: Some(PodSpec {
                containers: vec![
                    Container {
                        image: Some("traefik:v2.10".to_string()),
                        ..Default::default()
                    },
                    Container {
                        image: Some("kong:3.5".to_string()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Traefik)
        );
    }

    // =========================================================================
    // detect_ingress_controller_from_pod — unknown / false-positive cases
    // =========================================================================

    #[test]
    fn test_pod_no_labels_no_spec_returns_none() {
        let pod = Pod::default();
        assert_eq!(detect_ingress_controller_from_pod(&pod), None);
    }

    #[test]
    fn test_pod_unrelated_labels_returns_none() {
        let pod = pod_with_labels(&[
            ("app.kubernetes.io/name", "my-web-app"),
            ("app", "frontend"),
            ("tier", "web"),
        ]);
        assert_eq!(detect_ingress_controller_from_pod(&pod), None);
    }

    #[test]
    fn test_pod_unrelated_image_returns_none() {
        let pod = pod_with_image("my-company/api-server:1.0.0");
        assert_eq!(detect_ingress_controller_from_pod(&pod), None);
    }

    #[test]
    fn test_pod_image_none_returns_none() {
        // Container with explicitly-None image must not panic and must not match.
        let pod = Pod {
            spec: Some(PodSpec {
                containers: vec![Container {
                    image: None,
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(detect_ingress_controller_from_pod(&pod), None);
    }

    #[test]
    fn test_pod_empty_containers_returns_none() {
        let pod = Pod {
            spec: Some(PodSpec {
                containers: vec![],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(detect_ingress_controller_from_pod(&pod), None);
    }

    // ── Istio false-positives: only "ingressgateway" in labels should match ───

    #[test]
    fn test_pod_label_istio_generic_returns_none() {
        // "istio" alone in a label value must not match — it covers istiod,
        // istio-pilot, and any app whose name contains the word "istio".
        let pod = pod_with_labels(&[("app.kubernetes.io/name", "istio")]);
        assert_eq!(detect_ingress_controller_from_pod(&pod), None);
    }

    #[test]
    fn test_pod_label_istiod_returns_none() {
        let pod = pod_with_labels(&[("app", "istiod")]);
        assert_eq!(detect_ingress_controller_from_pod(&pod), None);
    }

    #[test]
    fn test_pod_label_istio_pilot_returns_none() {
        let pod = pod_with_labels(&[("app", "istio-pilot")]);
        assert_eq!(detect_ingress_controller_from_pod(&pod), None);
    }

    #[test]
    fn test_pod_istio_injected_app_returns_none() {
        // The canonical false-positive: a regular workload in an Istio-mesh
        // namespace.  Its own labels identify it as an app pod, not a gateway,
        // so the injected proxyv2 sidecar must never trigger Istio detection.
        let pod = Pod {
            metadata: ObjectMeta {
                labels: Some(BTreeMap::from([(
                    "app".to_string(),
                    "my-service".to_string(),
                )])),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![
                    Container {
                        image: Some("my-company/my-service:1.0.0".to_string()),
                        ..Default::default()
                    },
                    Container {
                        image: Some("docker.io/istio/proxyv2:1.18.0".to_string()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(detect_ingress_controller_from_pod(&pod), None);
    }

    // =========================================================================
    // detect_ingress_controller_from_pod — label takes priority over image
    // =========================================================================

    #[test]
    fn test_pod_label_beats_image() {
        // Label says Nginx, image says Traefik — label wins.
        let pod = Pod {
            metadata: ObjectMeta {
                labels: Some(BTreeMap::from([(
                    "app.kubernetes.io/name".to_string(),
                    "ingress-nginx".to_string(),
                )])),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![Container {
                    image: Some("traefik:v2.10".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            detect_ingress_controller_from_pod(&pod),
            Some(IngressControllerKind::Nginx)
        );
    }

    // =========================================================================
    // is_ingress_controller_pod
    // =========================================================================

    #[test]
    fn test_is_ingress_controller_pod_true_for_known() {
        let pod = pod_with_labels(&[("app.kubernetes.io/name", "traefik")]);
        assert!(is_ingress_controller_pod(&pod));
    }

    #[test]
    fn test_is_ingress_controller_pod_false_for_unknown() {
        let pod = pod_with_labels(&[("app", "my-service")]);
        assert!(!is_ingress_controller_pod(&pod));
    }

    #[test]
    fn test_is_ingress_controller_pod_false_for_empty() {
        assert!(!is_ingress_controller_pod(&Pod::default()));
    }

    // =========================================================================
    // detect_ingress_controller_from_ingress — spec.ingressClassName
    // =========================================================================

    #[test]
    fn test_ingress_class_name_nginx() {
        let ing = ingress_with_class_name("nginx");
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Nginx)
        );
    }

    #[test]
    fn test_ingress_class_name_traefik() {
        let ing = ingress_with_class_name("traefik");
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Traefik)
        );
    }

    #[test]
    fn test_ingress_class_name_haproxy() {
        let ing = ingress_with_class_name("haproxy");
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::HaProxy)
        );
    }

    #[test]
    fn test_ingress_class_name_istio() {
        let ing = ingress_with_class_name("istio");
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Istio)
        );
    }

    #[test]
    fn test_ingress_class_name_contour() {
        let ing = ingress_with_class_name("contour");
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Contour)
        );
    }

    #[test]
    fn test_ingress_class_name_kong() {
        let ing = ingress_with_class_name("kong");
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Kong)
        );
    }

    #[test]
    fn test_ingress_class_name_ambassador() {
        let ing = ingress_with_class_name("ambassador");
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Ambassador)
        );
    }

    #[test]
    fn test_ingress_class_name_emissary() {
        let ing = ingress_with_class_name("emissary");
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Ambassador)
        );
    }

    #[test]
    fn test_ingress_class_name_case_insensitive() {
        let ing = ingress_with_class_name("NGINX");
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Nginx)
        );
    }

    #[test]
    fn test_ingress_class_name_unknown_returns_none() {
        let ing = ingress_with_class_name("my-custom-controller");
        assert_eq!(detect_ingress_controller_from_ingress(&ing), None);
    }

    // =========================================================================
    // detect_ingress_controller_from_ingress — legacy annotation
    // =========================================================================

    #[test]
    fn test_ingress_legacy_annotation_nginx() {
        let ing = ingress_with_annotations(&[("kubernetes.io/ingress.class", "nginx")]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Nginx)
        );
    }

    #[test]
    fn test_ingress_legacy_annotation_traefik() {
        let ing = ingress_with_annotations(&[("kubernetes.io/ingress.class", "traefik")]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Traefik)
        );
    }

    #[test]
    fn test_ingress_legacy_annotation_haproxy() {
        let ing = ingress_with_annotations(&[("kubernetes.io/ingress.class", "haproxy")]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::HaProxy)
        );
    }

    #[test]
    fn test_ingress_legacy_annotation_istio() {
        let ing = ingress_with_annotations(&[("kubernetes.io/ingress.class", "istio")]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Istio)
        );
    }

    #[test]
    fn test_ingress_legacy_annotation_contour() {
        let ing = ingress_with_annotations(&[("kubernetes.io/ingress.class", "contour")]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Contour)
        );
    }

    #[test]
    fn test_ingress_legacy_annotation_kong() {
        let ing = ingress_with_annotations(&[("kubernetes.io/ingress.class", "kong")]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Kong)
        );
    }

    #[test]
    fn test_ingress_legacy_annotation_ambassador() {
        let ing = ingress_with_annotations(&[("kubernetes.io/ingress.class", "ambassador")]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Ambassador)
        );
    }

    #[test]
    fn test_ingress_legacy_annotation_unknown_returns_none() {
        let ing = ingress_with_annotations(&[("kubernetes.io/ingress.class", "my-custom-class")]);
        assert_eq!(detect_ingress_controller_from_ingress(&ing), None);
    }

    // =========================================================================
    // detect_ingress_controller_from_ingress — annotation key prefix detection
    // =========================================================================

    #[test]
    fn test_ingress_annotation_key_nginx_ingress_kubernetes_io() {
        let ing = ingress_with_annotations(&[("nginx.ingress.kubernetes.io/rewrite-target", "/")]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Nginx)
        );
    }

    #[test]
    fn test_ingress_annotation_key_nginx_org() {
        let ing = ingress_with_annotations(&[("nginx.org/proxy-connect-timeout", "30s")]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Nginx)
        );
    }

    #[test]
    fn test_ingress_annotation_key_traefik() {
        let ing = ingress_with_annotations(&[(
            "traefik.ingress.kubernetes.io/router.entrypoints",
            "web",
        )]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Traefik)
        );
    }

    #[test]
    fn test_ingress_annotation_key_haproxy_org() {
        let ing = ingress_with_annotations(&[("haproxy.org/check", "enabled")]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::HaProxy)
        );
    }

    #[test]
    fn test_ingress_annotation_key_istio_io() {
        let ing = ingress_with_annotations(&[("istio.io/rev", "default")]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Istio)
        );
    }

    #[test]
    fn test_ingress_annotation_key_projectcontour_io() {
        let ing = ingress_with_annotations(&[("projectcontour.io/websocket-routes", "/")]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Contour)
        );
    }

    #[test]
    fn test_ingress_annotation_key_contour_heptio_com() {
        let ing = ingress_with_annotations(&[("contour.heptio.com/request-timeout", "30s")]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Contour)
        );
    }

    #[test]
    fn test_ingress_annotation_key_konghq_com() {
        let ing = ingress_with_annotations(&[("konghq.com/strip-path", "true")]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Kong)
        );
    }

    #[test]
    fn test_ingress_annotation_key_getambassador_io() {
        let ing = ingress_with_annotations(&[("getambassador.io/config", "")]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Ambassador)
        );
    }

    #[test]
    fn test_ingress_annotation_key_emissary_prefix() {
        let ing = ingress_with_annotations(&[("emissary-ingress.io/config", "")]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Ambassador)
        );
    }

    // =========================================================================
    // detect_ingress_controller_from_ingress — unknown / false-positive cases
    // =========================================================================

    #[test]
    fn test_ingress_empty_returns_none() {
        assert_eq!(
            detect_ingress_controller_from_ingress(&Ingress::default()),
            None
        );
    }

    #[test]
    fn test_ingress_no_spec_no_annotations_returns_none() {
        let ing = Ingress {
            metadata: ObjectMeta {
                name: Some("my-ingress".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(detect_ingress_controller_from_ingress(&ing), None);
    }

    #[test]
    fn test_ingress_unrelated_annotations_only_returns_none() {
        let ing = ingress_with_annotations(&[
            ("kubernetes.io/tls-acme", "true"),
            ("cert-manager.io/cluster-issuer", "letsencrypt-prod"),
        ]);
        assert_eq!(detect_ingress_controller_from_ingress(&ing), None);
    }

    #[test]
    fn test_ingress_empty_annotations_map_returns_none() {
        let ing = Ingress {
            metadata: ObjectMeta {
                annotations: Some(BTreeMap::new()),
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(detect_ingress_controller_from_ingress(&ing), None);
    }

    // =========================================================================
    // detect_ingress_controller_from_ingress — priority ordering
    // =========================================================================

    #[test]
    fn test_ingress_class_name_beats_legacy_annotation() {
        // spec.ingressClassName: nginx  vs  kubernetes.io/ingress.class: traefik
        // The spec field must win.
        let ing = Ingress {
            metadata: ObjectMeta {
                annotations: Some(BTreeMap::from([(
                    "kubernetes.io/ingress.class".to_string(),
                    "traefik".to_string(),
                )])),
                ..Default::default()
            },
            spec: Some(IngressSpec {
                ingress_class_name: Some("nginx".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Nginx)
        );
    }

    #[test]
    fn test_ingress_legacy_annotation_beats_annotation_key_prefix() {
        // kubernetes.io/ingress.class: nginx  vs  traefik.ingress.kubernetes.io/... annotation key
        // The explicit class annotation must win.
        let ing = ingress_with_annotations(&[
            ("kubernetes.io/ingress.class", "nginx"),
            ("traefik.ingress.kubernetes.io/router.entrypoints", "web"),
        ]);
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Nginx)
        );
    }

    #[test]
    fn test_ingress_class_name_unknown_falls_through_to_annotation_key() {
        // spec.ingressClassName doesn't match, but an annotation key prefix does.
        let ing = Ingress {
            metadata: ObjectMeta {
                annotations: Some(BTreeMap::from([(
                    "nginx.ingress.kubernetes.io/rewrite-target".to_string(),
                    "/".to_string(),
                )])),
                ..Default::default()
            },
            spec: Some(IngressSpec {
                ingress_class_name: Some("my-custom-controller".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            detect_ingress_controller_from_ingress(&ing),
            Some(IngressControllerKind::Nginx)
        );
    }
}
