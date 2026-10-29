//! On-demand reads of single objects of any kind (the YAML window, fix
//! plans), as plain JSON.
//!
//! The watch only keeps what the map needs; these go to the API server
//! every time, so the result is the live object with every field
//! (`managedFields`, `status`, annotations) present.  Kinds are resolved
//! through discovery once per `(cluster, group/version, kind)` and cached
//! (two clusters can serve different versions of the same CRD).
//!
//! **Secrets.**  A core `Secret` comes back with every value in `data` and
//! `stringData` replaced by [`REDACTED`] — key names and sizes are kept, the
//! base64 is not.  Only [`get_object_with`] called with
//! [`SecretValues::Revealed`] returns values, one object at a time, and it
//! logs that it did; lists never do.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

pub use kube::api::ListParams;
use kube::api::{Api, DynamicObject};
use kube::core::{ApiResource, GroupVersion};
use kube::discovery::Scope;
use kube::Client;

use crate::k8s::error::ObjectError;
pub use crate::k8s::security::REDACTED;

/// Which object to read.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectKey {
    /// `apps/v1`, or `v1` for the core group.
    pub api_version: String,
    /// Kind in any case (`deployment`, `Deployment`).
    pub kind: String,
    /// `None` for cluster-scoped objects (namespaces).
    pub namespace: Option<String>,
    pub name: String,
}

/// Whether a `Secret`'s values are returned or replaced by [`REDACTED`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SecretValues {
    #[default]
    Redacted,
    Revealed,
}

type Resolved = (ApiResource, bool);

/// `(cluster, group/version, lowercase kind)`.
type CacheKey = (String, String, String);

/// How long a resolved kind is trusted before discovery runs again, so a
/// CRD re-installed with a different served version is picked up.
const DISCOVERY_TTL: Duration = Duration::from_secs(10 * 60);

fn cache() -> &'static Mutex<HashMap<CacheKey, (Instant, Resolved)>> {
    static CACHE: OnceLock<Mutex<HashMap<CacheKey, (Instant, Resolved)>>> = OnceLock::new();
    CACHE.get_or_init(Default::default)
}

fn cached(key: &CacheKey, now: Instant) -> Option<Resolved> {
    let c = cache().lock().ok()?;
    let (at, resolved) = c.get(key)?;
    (now.duration_since(*at) < DISCOVERY_TTL).then(|| resolved.clone())
}

fn remember(key: CacheKey, resolved: &Resolved, now: Instant) {
    if let Ok(mut c) = cache().lock() {
        c.insert(key, (now, resolved.clone()));
    }
}

/// Forget what discovery said about `cluster` (on disconnect, or after a
/// CRD was installed or removed).
pub fn forget_cluster(cluster: &str) {
    if let Ok(mut c) = cache().lock() {
        c.retain(|(k, _, _), _| k != cluster);
    }
}

/// `(ApiResource, namespaced)` for a group/version + kind on `cluster` (any
/// stable id of the client's cluster), via discovery.
pub(crate) async fn resolve(
    client: &Client,
    cluster: &str,
    api_version: &str,
    kind: &str,
) -> Result<Resolved, ObjectError> {
    let key = (
        cluster.to_string(),
        api_version.to_string(),
        kind.to_ascii_lowercase(),
    );
    if let Some(hit) = cached(&key, Instant::now()) {
        return Ok(hit);
    }
    let gv: GroupVersion = api_version
        .parse()
        .map_err(|_| ObjectError::BadApiVersion(api_version.to_string()))?;
    let group = kube::discovery::pinned_group(client, &gv)
        .await
        .map_err(ObjectError::Discovery)?;
    let (ar, caps) = group
        .versioned_resources(&gv.version)
        .into_iter()
        .find(|(ar, _)| ar.kind.eq_ignore_ascii_case(kind))
        .ok_or_else(|| ObjectError::UnknownKind {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
        })?;
    let resolved = (ar, matches!(caps.scope, Scope::Namespaced));
    remember(key, &resolved, Instant::now());
    Ok(resolved)
}

/// `Api<DynamicObject>` for `kind` in `namespace` (all namespaces for
/// `None` or a cluster-scoped kind).
pub(crate) async fn dynamic_api(
    client: &Client,
    cluster: &str,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
) -> Result<Api<DynamicObject>, ObjectError> {
    let (ar, namespaced) = resolve(client, cluster, api_version, kind).await?;
    Ok(api_for(client, &ar, namespaced, namespace))
}

fn api_for(
    client: &Client,
    ar: &ApiResource,
    namespaced: bool,
    namespace: Option<&str>,
) -> Api<DynamicObject> {
    match (namespace, namespaced) {
        (Some(ns), true) => Api::namespaced_with(client.clone(), ns, ar),
        _ => Api::all_with(client.clone(), ar),
    }
}

fn is_secret(ar: &ApiResource) -> bool {
    ar.group.is_empty() && ar.kind == "Secret"
}

/// Replace every value in `data` and `stringData` with [`REDACTED`], keeping
/// the keys.
pub fn redact_secret_values(secret: &mut serde_json::Value) {
    for field in ["data", "stringData"] {
        if let Some(map) = secret
            .get_mut(field)
            .and_then(serde_json::Value::as_object_mut)
        {
            for v in map.values_mut() {
                *v = serde_json::Value::String(REDACTED.into());
            }
        }
    }
}

/// GET one object as JSON (`apiVersion`, `kind`, `metadata`, the rest).
/// Secret values are redacted; see [`get_object_with`].
pub async fn get_object(
    client: &Client,
    cluster: &str,
    key: &ObjectKey,
) -> Result<serde_json::Value, ObjectError> {
    get_object_with(client, cluster, key, SecretValues::Redacted).await
}

/// [`get_object`], choosing what happens to a `Secret`'s values.
#[tracing::instrument(name = "k8s.object.get", skip(client, key), fields(kind = %key.kind, ns = ?key.namespace, name = %key.name))]
pub async fn get_object_with(
    client: &Client,
    cluster: &str,
    key: &ObjectKey,
    secrets: SecretValues,
) -> Result<serde_json::Value, ObjectError> {
    let (ar, namespaced) = resolve(client, cluster, &key.api_version, &key.kind).await?;
    let api = api_for(client, &ar, namespaced, key.namespace.as_deref());
    let obj = api.get(&key.name).await.map_err(ObjectError::from_api)?;
    let mut value = serde_json::to_value(obj).map_err(ObjectError::Decode)?;
    if is_secret(&ar) {
        match secrets {
            SecretValues::Redacted => redact_secret_values(&mut value),
            SecretValues::Revealed => tracing::info!("Secret values revealed"),
        }
    }
    Ok(value)
}

/// LIST every object of a kind in `namespace` (all namespaces for `None`
/// or a cluster-scoped kind), as JSON.  Used for small kinds read on
/// demand (PodDisruptionBudgets for a fix plan's blast radius).  Secret
/// values are always redacted.
pub async fn list_objects(
    client: &Client,
    cluster: &str,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
) -> Result<Vec<serde_json::Value>, ObjectError> {
    list_objects_with(
        client,
        cluster,
        api_version,
        kind,
        namespace,
        &ListParams::default(),
    )
    .await
}

/// [`list_objects`] with list options: a label selector (`app=web`), a
/// field selector, or a page limit.
#[tracing::instrument(name = "k8s.object.list", skip(client, params), fields(rows = tracing::field::Empty))]
pub async fn list_objects_with(
    client: &Client,
    cluster: &str,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    params: &ListParams,
) -> Result<Vec<serde_json::Value>, ObjectError> {
    let (ar, namespaced) = resolve(client, cluster, api_version, kind).await?;
    let api = api_for(client, &ar, namespaced, namespace);
    let list = api.list(params).await.map_err(ObjectError::from_api)?;
    tracing::Span::current().record("rows", list.items.len());
    let secret = is_secret(&ar);
    list.items
        .into_iter()
        .map(|o| {
            let mut v = serde_json::to_value(o).map_err(ObjectError::Decode)?;
            if secret {
                redact_secret_values(&mut v);
            }
            Ok(v)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_versions_parse_for_core_and_named_groups() {
        let core: GroupVersion = "v1".parse().unwrap();
        assert_eq!((core.group.as_str(), core.version.as_str()), ("", "v1"));
        let apps: GroupVersion = "apps/v1".parse().unwrap();
        assert_eq!((apps.group.as_str(), apps.version.as_str()), ("apps", "v1"));
    }

    #[test]
    fn secret_values_are_redacted_but_keys_kept() {
        let mut s = serde_json::json!({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": {"name": "db", "namespace": "app"},
            "type": "Opaque",
            "data": {"password": "aHVudGVyMg==", "user": "cm9vdA=="},
            "stringData": {"url": "postgres://x"}
        });
        redact_secret_values(&mut s);
        let text = s.to_string();
        for leaked in ["aHVudGVyMg==", "cm9vdA==", "postgres://x"] {
            assert!(!text.contains(leaked), "{leaked} leaked");
        }
        assert_eq!(s["data"]["password"], REDACTED);
        assert_eq!(s["data"]["user"], REDACTED);
        assert_eq!(s["stringData"]["url"], REDACTED);
        assert_eq!(s["metadata"]["name"], "db");
        assert_eq!(s["type"], "Opaque");

        // Other kinds (and secrets without data) pass through unchanged.
        let mut cm = serde_json::json!({"kind": "ConfigMap", "data": {"a": "1"}});
        let before = cm.clone();
        // redact_secret_values is only called for Secrets by the callers;
        // on an arbitrary object it would still only touch data/stringData.
        redact_secret_values(&mut cm);
        assert_ne!(cm, before);
        let mut empty = serde_json::json!({"kind": "Secret", "metadata": {}});
        redact_secret_values(&mut empty);
        assert_eq!(empty, serde_json::json!({"kind": "Secret", "metadata": {}}));
    }

    #[test]
    fn discovery_cache_expires_and_forgets_clusters() {
        let ar = ApiResource::from_gvk(&kube::core::GroupVersionKind::gvk("x.io", "v1", "Gizmo"));
        let key = |c: &str| (c.to_string(), "x.io/v1".to_string(), "gizmo".to_string());
        let now = Instant::now();
        remember(key("ttl-a"), &(ar.clone(), true), now);
        remember(key("ttl-b"), &(ar, true), now);
        assert!(cached(&key("ttl-a"), now).is_some());
        assert!(
            cached(&key("ttl-a"), now + DISCOVERY_TTL).is_none(),
            "expired"
        );
        forget_cluster("ttl-a");
        assert!(cached(&key("ttl-a"), now).is_none());
        assert!(cached(&key("ttl-b"), now).is_some(), "other clusters kept");
        forget_cluster("ttl-b");
    }

    #[test]
    fn only_core_secret_counts_as_secret() {
        let core = ApiResource::from_gvk(&kube::core::GroupVersionKind::gvk("", "v1", "Secret"));
        assert!(is_secret(&core));
        let cm = ApiResource::from_gvk(&kube::core::GroupVersionKind::gvk("", "v1", "ConfigMap"));
        assert!(!is_secret(&cm));
        let sealed = ApiResource::from_gvk(&kube::core::GroupVersionKind::gvk(
            "bitnami.com",
            "v1alpha1",
            "SealedSecret",
        ));
        assert!(!is_secret(&sealed));
    }
}
