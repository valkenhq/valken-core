//! Writes: the only code in Valken that changes a cluster.
//!
//! Everything goes through server-side requests with field manager
//! [`FIELD_MANAGER`], and every call can run as a server-side dry run
//! (`dryRun=All`: admission webhooks run, nothing is persisted).  Callers
//! gate these behind proposals and approval; this module only executes.
//!
//! Optimistic concurrency: put `metadata.resourceVersion` in a patch body and
//! the server refuses with 409 ([`ObjectError::Conflict`]) if the object
//! changed since — that's how a proposal expires instead of clobbering.

use chrono::{DateTime, Utc};
use kube::api::{DeleteParams, DynamicObject, Patch, PatchParams, PostParams};
use kube::Client;
use serde_json::{json, Value};

use crate::k8s::error::ObjectError;
use crate::k8s::objects::{dynamic_api, ObjectKey};

/// Shown in `managedFields` for everything Valken writes.
pub const FIELD_MANAGER: &str = "valken";

/// How a patch body is interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchKind {
    /// `application/strategic-merge-patch+json` (built-in kinds).
    Strategic,
    /// `application/merge-patch+json` (CRDs, anything).
    Merge,
}

/// Dry run or for real.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    DryRun,
    Apply,
}

impl Mode {
    fn dry(self) -> bool {
        self == Mode::DryRun
    }
}

/// PATCH one object; returns the object as the server has (or would have) it.
#[tracing::instrument(name = "k8s.write.patch", skip(client, patch), fields(kind = %key.kind, ns = ?key.namespace, name = %key.name))]
pub async fn patch(
    client: &Client,
    cluster: &str,
    key: &ObjectKey,
    patch: &Value,
    kind: PatchKind,
    mode: Mode,
) -> Result<Value, ObjectError> {
    let api = dynamic_api(
        client,
        cluster,
        &key.api_version,
        &key.kind,
        key.namespace.as_deref(),
    )
    .await?;
    let mut params = PatchParams::apply(FIELD_MANAGER);
    params.force = false;
    params.dry_run = mode.dry();
    let body = match kind {
        PatchKind::Strategic => Patch::Strategic(patch),
        PatchKind::Merge => Patch::Merge(patch),
    };
    let obj = api
        .patch(&key.name, &params, &body)
        .await
        .map_err(ObjectError::from_api)?;
    serde_json::to_value(obj).map_err(ObjectError::Decode)
}

/// CREATE `object` (a full manifest with `apiVersion`, `kind`, `metadata`).
#[tracing::instrument(name = "k8s.write.create", skip(client, object))]
pub async fn create(
    client: &Client,
    cluster: &str,
    object: &Value,
    mode: Mode,
) -> Result<Value, ObjectError> {
    let (api_version, kind) = type_meta(object)?;
    let namespace = object["metadata"]["namespace"].as_str();
    let api = dynamic_api(client, cluster, api_version, kind, namespace).await?;
    let obj: DynamicObject = serde_json::from_value(object.clone()).map_err(ObjectError::Decode)?;
    let params = PostParams {
        dry_run: mode.dry(),
        field_manager: Some(FIELD_MANAGER.into()),
    };
    let created = api
        .create(&params, &obj)
        .await
        .map_err(ObjectError::from_api)?;
    serde_json::to_value(created).map_err(ObjectError::Decode)
}

/// DELETE one object (pods: the controller replaces it).
#[tracing::instrument(name = "k8s.write.delete", skip(client), fields(kind = %key.kind, ns = ?key.namespace, name = %key.name))]
pub async fn delete(
    client: &Client,
    cluster: &str,
    key: &ObjectKey,
    mode: Mode,
) -> Result<(), ObjectError> {
    let api = dynamic_api(
        client,
        cluster,
        &key.api_version,
        &key.kind,
        key.namespace.as_deref(),
    )
    .await?;
    let params = DeleteParams {
        dry_run: mode.dry(),
        ..DeleteParams::default()
    };
    api.delete(&key.name, &params)
        .await
        .map_err(ObjectError::from_api)?;
    Ok(())
}

/// `(apiVersion, kind)` of a manifest, or [`ObjectError::Rejected`] when
/// either is missing — caught here rather than as a confusing discovery
/// error for `""`.
fn type_meta(object: &Value) -> Result<(&str, &str), ObjectError> {
    match (object["apiVersion"].as_str(), object["kind"].as_str()) {
        (Some(av), Some(k)) if !av.is_empty() && !k.is_empty() => Ok((av, k)),
        _ => Err(ObjectError::Rejected(
            "manifest needs apiVersion and kind".into(),
        )),
    }
}

/// `kubectl rollout restart`: bump the pod template's `restartedAt`.
pub fn restart_patch(at: DateTime<Utc>) -> Value {
    json!({"spec": {"template": {"metadata": {"annotations": {
        "kubectl.kubernetes.io/restartedAt": at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    }}}}})
}

/// `kubectl scale --replicas=n`.
pub fn scale_patch(replicas: u32) -> Value {
    json!({"spec": {"replicas": replicas}})
}

/// Adds the `resourceVersion` precondition to a patch body (no-op when the
/// body isn't an object).
pub fn with_precondition(mut patch: Value, resource_version: &str) -> Value {
    if let Some(map) = patch.as_object_mut() {
        let meta = map.entry("metadata").or_insert_with(|| json!({}));
        if let Some(meta) = meta.as_object_mut() {
            meta.insert(
                "resourceVersion".into(),
                Value::String(resource_version.into()),
            );
        }
    }
    patch
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn restart_patch_sets_template_annotation() {
        let at = Utc.with_ymd_and_hms(2026, 10, 22, 9, 52, 3).unwrap();
        assert_eq!(
            restart_patch(at)["spec"]["template"]["metadata"]["annotations"]
                ["kubectl.kubernetes.io/restartedAt"],
            "2026-10-22T09:52:03Z"
        );
    }

    #[test]
    fn scale_patch_is_replicas_only() {
        assert_eq!(scale_patch(4), json!({"spec": {"replicas": 4}}));
    }

    #[test]
    fn precondition_merges_into_metadata() {
        let p = with_precondition(
            json!({"metadata": {"labels": {"a": "b"}}, "spec": {}}),
            "812331",
        );
        assert_eq!(p["metadata"]["resourceVersion"], "812331");
        assert_eq!(p["metadata"]["labels"]["a"], "b");
        assert_eq!(with_precondition(json!([1]), "1"), json!([1]));
    }

    #[test]
    fn manifests_without_type_meta_are_rejected_locally() {
        assert_eq!(
            type_meta(&json!({"apiVersion": "v1", "kind": "ConfigMap"})).unwrap(),
            ("v1", "ConfigMap")
        );
        for bad in [
            json!({"kind": "ConfigMap"}),
            json!({"apiVersion": "v1"}),
            json!({"apiVersion": "", "kind": "ConfigMap"}),
            json!([]),
        ] {
            assert!(
                matches!(type_meta(&bad), Err(ObjectError::Rejected(_))),
                "{bad}"
            );
        }
    }

    #[test]
    fn api_errors_map_to_variants() {
        let status = |code: u16| {
            kube::Error::Api(
                kube::core::Status::failure(&format!("code {code}"), "x")
                    .with_code(code)
                    .boxed(),
            )
        };
        assert!(matches!(
            ObjectError::from_api(status(404)),
            ObjectError::NotFound
        ));
        assert!(matches!(
            ObjectError::from_api(status(409)),
            ObjectError::Conflict(_)
        ));
        assert!(matches!(
            ObjectError::from_api(status(422)),
            ObjectError::Rejected(_)
        ));
        assert!(matches!(
            ObjectError::from_api(status(403)),
            ObjectError::Forbidden(_)
        ));
    }
}
