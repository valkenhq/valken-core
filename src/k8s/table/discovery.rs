//! Per-cluster API discovery: every resource the server serves (core kinds,
//! aggregated APIs and CRDs) with its plural, singular, short names and
//! scope — what `kubectl api-resources` prints, and what `:` resolves
//! against (`po`, `deploy`, `cert`, `certificates.cert-manager.io`).
//!
//! One `/api` + `/apis` round and one request per group's preferred
//! version (run concurrently), plus a CRD list to tell CRDs apart.  The
//! caller caches the [`ApiCatalog`] per cluster; resolving is pure.

use std::collections::HashSet;

use futures::stream::{self, StreamExt};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::APIResource;
use kube::api::{Api, ListParams};
use kube::Client;

use crate::k8s::error::ObjectError;

/// Group-version requests in flight at once.
const CONCURRENCY: usize = 8;

/// One listable resource.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ApiResourceInfo {
    /// `""` for the core group.
    pub group: String,
    pub version: String,
    pub kind: String,
    /// `deployments`
    pub plural: String,
    /// `deployment` (derived from the kind when the server leaves it empty).
    pub singular: String,
    /// `deploy`
    pub short_names: Vec<String>,
    pub namespaced: bool,
    /// Defined by a CustomResourceDefinition.
    pub crd: bool,
    /// Can be watched (otherwise only listed once).
    pub watchable: bool,
}

impl ApiResourceInfo {
    /// `apps/v1`, or `v1` for the core group.
    pub fn api_version(&self) -> String {
        if self.group.is_empty() {
            self.version.clone()
        } else {
            format!("{}/{}", self.group, self.version)
        }
    }

    /// The collection URL, all namespaces or one.
    pub fn url(&self, namespace: Option<&str>) -> String {
        let base = if self.group.is_empty() {
            format!("/api/{}", self.version)
        } else {
            format!("/apis/{}/{}", self.group, self.version)
        };
        match namespace.filter(|_| self.namespaced) {
            Some(ns) => format!("{base}/namespaces/{ns}/{}", self.plural),
            None => format!("{base}/{}", self.plural),
        }
    }

    /// `deployments.apps`, or `pods` for the core group (kubectl's
    /// fully-qualified name).
    pub fn qualified(&self) -> String {
        if self.group.is_empty() {
            self.plural.clone()
        } else {
            format!("{}.{}", self.plural, self.group)
        }
    }

    fn from_api(
        group: &str,
        version: &str,
        r: &APIResource,
        crds: &HashSet<(String, String)>,
    ) -> Option<Self> {
        // Subresources (`pods/log`) and kinds you can't list aren't rows.
        let verbs = &r.verbs;
        if r.name.contains('/') || !verbs.iter().any(|v| v == "list") {
            return None;
        }
        let singular = if r.singular_name.is_empty() {
            r.kind.to_ascii_lowercase()
        } else {
            r.singular_name.clone()
        };
        Some(ApiResourceInfo {
            group: group.to_string(),
            version: version.to_string(),
            kind: r.kind.clone(),
            plural: r.name.clone(),
            singular,
            short_names: r.short_names.clone().unwrap_or_default(),
            namespaced: r.namespaced,
            crd: crds.contains(&(group.to_string(), r.name.clone())),
            watchable: verbs.iter().any(|v| v == "watch"),
        })
    }
}

/// Everything one cluster serves.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApiCatalog {
    pub resources: Vec<ApiResourceInfo>,
}

/// How well a typed name matches a resource (lower is better).
fn match_rank(r: &ApiResourceInfo, q: &str) -> Option<u8> {
    let (name, group) = match q.split_once('.') {
        Some((n, g)) => (n, Some(g)),
        None => (q, None),
    };
    if let Some(g) = group {
        if !r.group.eq_ignore_ascii_case(g) {
            return None;
        }
    }
    if r.plural.eq_ignore_ascii_case(name) {
        Some(0)
    } else if r.singular.eq_ignore_ascii_case(name) {
        Some(1)
    } else if r.short_names.iter().any(|s| s.eq_ignore_ascii_case(name)) {
        Some(2)
    } else if r.kind.eq_ignore_ascii_case(name) {
        Some(3)
    } else {
        None
    }
}

/// Built-in groups win a name clash (`events` is core, not
/// `events.k8s.io`), then the shorter group.
fn group_rank(r: &ApiResourceInfo) -> (bool, usize) {
    (
        r.crd || r.group.contains('.') && !r.group.ends_with(".k8s.io"),
        r.group.len(),
    )
}

impl ApiCatalog {
    pub fn new(mut resources: Vec<ApiResourceInfo>) -> Self {
        resources.sort_by(|a, b| {
            a.plural
                .cmp(&b.plural)
                .then_with(|| group_rank(a).cmp(&group_rank(b)))
        });
        ApiCatalog { resources }
    }

    /// The resource a typed name means: plural, singular, short name or
    /// kind, optionally `.group`-qualified, case-insensitive (`po`, `Pod`,
    /// `deployments.apps`, `cert`).
    pub fn resolve(&self, query: &str) -> Option<&ApiResourceInfo> {
        let q = query.trim();
        if q.is_empty() {
            return None;
        }
        self.resources
            .iter()
            .filter_map(|r| match_rank(r, q).map(|rank| (rank, group_rank(r), r)))
            .min_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)))
            .map(|(_, _, r)| r)
    }

    /// Completions for a prefix: resources whose plural or a short name
    /// starts with it, best first (exact names, then shortest plurals).
    pub fn complete(&self, prefix: &str, limit: usize) -> Vec<&ApiResourceInfo> {
        let p = prefix.trim().to_ascii_lowercase();
        let mut hits: Vec<(u8, &ApiResourceInfo)> = self
            .resources
            .iter()
            .filter_map(|r| {
                if match_rank(r, &p).is_some() {
                    Some((0, r))
                } else if r.plural.starts_with(&p) || r.qualified().starts_with(&p) {
                    Some((1, r))
                } else if r.short_names.iter().any(|s| s.starts_with(&p)) {
                    Some((2, r))
                } else {
                    None
                }
            })
            .collect();
        hits.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| a.1.plural.len().cmp(&b.1.plural.len()))
                .then_with(|| a.1.plural.cmp(&b.1.plural))
        });
        hits.into_iter().map(|(_, r)| r).take(limit).collect()
    }

    pub fn crd_count(&self) -> usize {
        self.resources.iter().filter(|r| r.crd).count()
    }
}

/// Ask the server what it serves (preferred version of every group).
#[tracing::instrument(name = "k8s.discovery", skip(client), fields(resources = tracing::field::Empty, crds = tracing::field::Empty))]
pub async fn discover(client: &Client) -> Result<ApiCatalog, ObjectError> {
    let crds = crd_names(client).await;
    let core = client
        .list_core_api_resources("v1")
        .await
        .map_err(ObjectError::Discovery)?;
    let mut resources: Vec<ApiResourceInfo> = core
        .resources
        .iter()
        .filter_map(|r| ApiResourceInfo::from_api("", "v1", r, &crds))
        .collect();
    let groups = client
        .list_api_groups()
        .await
        .map_err(ObjectError::Discovery)?;
    let versions: Vec<(String, String)> = groups
        .groups
        .iter()
        .filter_map(|g| {
            let v = g
                .preferred_version
                .as_ref()
                .or_else(|| g.versions.first())?;
            Some((g.name.clone(), v.version.clone()))
        })
        .collect();
    let fetched: Vec<_> = stream::iter(versions)
        .map(|(group, version)| async move {
            let gv = format!("{group}/{version}");
            // An aggregated API that's down (metrics-server) must not sink
            // the rest.
            match client.list_api_group_resources(&gv).await {
                Ok(list) => Some((group, version, list.resources)),
                Err(e) => {
                    tracing::debug!(%gv, error = %e, "discovery: group unavailable");
                    None
                }
            }
        })
        .buffer_unordered(CONCURRENCY)
        .collect()
        .await;
    for (group, version, list) in fetched.into_iter().flatten() {
        resources.extend(
            list.iter()
                .filter_map(|r| ApiResourceInfo::from_api(&group, &version, r, &crds)),
        );
    }
    let catalog = ApiCatalog::new(resources);
    let span = tracing::Span::current();
    span.record("resources", catalog.resources.len());
    span.record("crds", catalog.crd_count());
    Ok(catalog)
}

/// `(group, plural)` of every CRD; empty when CRDs can't be listed.
async fn crd_names(client: &Client) -> HashSet<(String, String)> {
    let api: Api<CustomResourceDefinition> = Api::all(client.clone());
    match api.list_metadata(&ListParams::default()).await {
        Ok(list) => list
            .items
            .iter()
            .filter_map(|c| {
                // CRD names are `<plural>.<group>`.
                let name = c.metadata.name.as_deref()?;
                let (plural, group) = name.split_once('.')?;
                Some((group.to_string(), plural.to_string()))
            })
            .collect(),
        Err(e) => {
            tracing::debug!(error = %e, "discovery: CRDs not listable");
            HashSet::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn res(group: &str, kind: &str, plural: &str, short: &[&str], crd: bool) -> ApiResourceInfo {
        ApiResourceInfo {
            group: group.into(),
            version: "v1".into(),
            kind: kind.into(),
            plural: plural.into(),
            singular: kind.to_ascii_lowercase(),
            short_names: short.iter().map(|s| s.to_string()).collect(),
            namespaced: true,
            crd,
            watchable: true,
        }
    }

    fn catalog() -> ApiCatalog {
        ApiCatalog::new(vec![
            res("", "Pod", "pods", &["po"], false),
            res("apps", "Deployment", "deployments", &["deploy"], false),
            res("", "Event", "events", &["ev"], false),
            res("events.k8s.io", "Event", "events", &["ev"], false),
            res(
                "cert-manager.io",
                "Certificate",
                "certificates",
                &["cert", "certs"],
                true,
            ),
            res(
                "cert-manager.io",
                "CertificateRequest",
                "certificaterequests",
                &["cr", "crs"],
                true,
            ),
            res(
                "certificates.k8s.io",
                "CertificateSigningRequest",
                "certificatesigningrequests",
                &["csr"],
                false,
            ),
        ])
    }

    #[test]
    fn resolves_k9s_aliases() {
        let c = catalog();
        assert_eq!(c.resolve("po").unwrap().plural, "pods");
        assert_eq!(c.resolve("Pod").unwrap().plural, "pods");
        assert_eq!(c.resolve("deploy").unwrap().group, "apps");
        assert_eq!(c.resolve("deployment").unwrap().plural, "deployments");
        assert_eq!(c.resolve("cert").unwrap().kind, "Certificate");
        assert!(c.resolve("nope").is_none());
        assert!(c.resolve("  ").is_none());
    }

    #[test]
    fn core_wins_name_clashes_and_groups_qualify() {
        let c = catalog();
        assert_eq!(c.resolve("events").unwrap().group, "");
        assert_eq!(
            c.resolve("events.events.k8s.io").unwrap().group,
            "events.k8s.io"
        );
        assert!(c.resolve("pods.apps").is_none());
    }

    #[test]
    fn completes_prefixes_best_first() {
        let c = catalog();
        let names: Vec<_> = c
            .complete("cert", 10)
            .iter()
            .map(|r| r.plural.as_str())
            .collect();
        assert_eq!(
            names,
            [
                "certificates",
                "certificaterequests",
                "certificatesigningrequests"
            ]
        );
        let names: Vec<_> = c
            .complete("cs", 10)
            .iter()
            .map(|r| r.plural.as_str())
            .collect();
        assert_eq!(names, ["certificatesigningrequests"]);
        assert_eq!(c.crd_count(), 2);
    }

    #[test]
    fn urls() {
        let c = catalog();
        let deploy = c.resolve("deploy").unwrap();
        assert_eq!(deploy.url(None), "/apis/apps/v1/deployments");
        assert_eq!(
            deploy.url(Some("shop")),
            "/apis/apps/v1/namespaces/shop/deployments"
        );
        assert_eq!(
            c.resolve("po").unwrap().url(Some("x")),
            "/api/v1/namespaces/x/pods"
        );
        assert_eq!(deploy.qualified(), "deployments.apps");
        assert_eq!(deploy.api_version(), "apps/v1");
    }

    #[test]
    fn subresources_and_unlistable_kinds_are_skipped() {
        let crds = HashSet::new();
        let mk = |name: &str, verbs: &[&str]| APIResource {
            name: name.into(),
            kind: "Pod".into(),
            verbs: verbs.iter().map(|v| v.to_string()).collect(),
            ..Default::default()
        };
        assert!(ApiResourceInfo::from_api("", "v1", &mk("pods/log", &["get"]), &crds).is_none());
        assert!(ApiResourceInfo::from_api("", "v1", &mk("bindings", &["create"]), &crds).is_none());
        let pods =
            ApiResourceInfo::from_api("", "v1", &mk("pods", &["list", "watch"]), &crds).unwrap();
        assert_eq!((pods.singular.as_str(), pods.watchable), ("pod", true));
    }
}
