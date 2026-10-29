//! Manifests safe to hand to a misconfiguration scanner.
//!
//! [`redacted_manifest`] drops `status` and `metadata.managedFields`,
//! removes the annotations that carry copies of a spec or a command line
//! (`kubectl.kubernetes.io/last-applied-configuration`,
//! `kubernetes.io/change-cause`, `kapp.k14s.io/original`, …) from **every**
//! `metadata.annotations` — the object's own and its pod / job templates' —
//! and replaces **every** env var `value` — in containers, init containers
//! and ephemeral containers, at any depth (Pod, workload templates, CronJob
//! job templates) — with `"<redacted>"`.  `valueFrom` references are kept:
//! they name a source, not a value.
//!
//! `command` and `args` are exported as they are: configuration scanners
//! check flags.  Don't pass secrets on the command line.

use serde::Serialize;
use serde_json::Value;

/// Replacement for env var values.
pub const REDACTED: &str = "<redacted>";

const LAST_APPLIED: &str = "kubectl.kubernetes.io/last-applied-configuration";

/// Annotations that hold a copy of the spec (env values included) or the
/// command that produced it.
const SPEC_COPY_ANNOTATIONS: [&str; 5] = [
    LAST_APPLIED,
    "kubernetes.io/change-cause",
    "kapp.k14s.io/original",
    "kapp.k14s.io/original-diff",
    "deployment.kubernetes.io/revision-history",
];

/// Serialise `obj` (with `apiVersion`/`kind`) and redact it.
pub fn redacted_manifest<T: Serialize>(obj: &T) -> Value {
    let mut value = serde_json::to_value(obj).unwrap_or(Value::Null);
    redact(&mut value);
    value
}

/// Redact a manifest in place.
pub fn redact(manifest: &mut Value) {
    if let Some(root) = manifest.as_object_mut() {
        root.remove("status");
        if let Some(meta) = root.get_mut("metadata").and_then(Value::as_object_mut) {
            meta.remove("managedFields");
        }
    }
    redact_tree(manifest);
}

/// Walks the whole manifest: env values at any depth, and the spec-copy
/// annotations in any `metadata` (the object's, a pod template's, a
/// CronJob's job template's).
fn redact_tree(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                match key.as_str() {
                    "env" => {
                        if let Some(vars) = child.as_array_mut() {
                            for var in vars.iter_mut().filter_map(Value::as_object_mut) {
                                if var.contains_key("value") {
                                    var.insert("value".into(), Value::String(REDACTED.into()));
                                }
                            }
                        }
                    }
                    "metadata" => {
                        if let Some(annotations) =
                            child.get_mut("annotations").and_then(Value::as_object_mut)
                        {
                            for key in SPEC_COPY_ANNOTATIONS {
                                annotations.remove(key);
                            }
                        }
                    }
                    _ => {}
                }
                redact_tree(child);
            }
        }
        Value::Array(items) => items.iter_mut().for_each(redact_tree),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::batch::v1::CronJob;
    use serde_json::json;

    #[test]
    fn redacts_env_values_everywhere_and_strips_noise() {
        let cron: CronJob = serde_json::from_value(json!({
            "metadata": {
                "name": "backup", "namespace": "db",
                "managedFields": [{"manager": "kubectl"}],
                "annotations": {LAST_APPLIED: "{\"env\":[{\"value\":\"hunter2\"}]}", "keep": "me"}
            },
            "spec": {
                "schedule": "* * * * *",
                "jobTemplate": {"spec": {"template": {"spec": {
                    "restartPolicy": "Never",
                    "initContainers": [{"name": "i", "env": [{"name": "A", "value": "secret-a"}]}],
                    "containers": [{"name": "c", "env": [
                        {"name": "B", "value": "secret-b"},
                        {"name": "C", "valueFrom": {"secretKeyRef": {"name": "s", "key": "k"}}}
                    ]}]
                }}}}
            },
            "status": {"active": []}
        }))
        .unwrap();

        let out = redacted_manifest(&cron);
        let text = out.to_string();
        for leaked in [
            "secret-a",
            "secret-b",
            "hunter2",
            "managedFields",
            "\"status\"",
        ] {
            assert!(!text.contains(leaked), "{leaked} leaked: {text}");
        }
        assert_eq!(out["apiVersion"], "batch/v1");
        assert_eq!(out["kind"], "CronJob");
        assert_eq!(out["metadata"]["annotations"]["keep"], "me");
        let containers =
            &out["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]["env"];
        assert_eq!(containers[0]["value"], REDACTED);
        assert_eq!(containers[1]["valueFrom"]["secretKeyRef"]["name"], "s");
        assert!(containers[1].get("value").is_none());
    }

    #[test]
    fn spec_copy_annotations_go_at_every_depth_and_only_those() {
        let mut m = json!({
            "kind": "Deployment",
            "metadata": {"annotations": {
                "kubernetes.io/change-cause": "kubectl set env deploy/web DB_PASS=hunter2",
                "kapp.k14s.io/original": "{\"env\":[{\"value\":\"hunter2\"}]}",
                "deployment.kubernetes.io/revision": "3"
            }},
            "spec": {"template": {
                "metadata": {"annotations": {
                    LAST_APPLIED: "{\"value\":\"hunter2\"}",
                    "prometheus.io/scrape": "true",
                    "env": "prod"
                }, "labels": {"env": "prod"}},
                "spec": {"ephemeralContainers": [{"name": "dbg", "env": [{"name": "T", "value": "hunter2"}]}]}
            }}
        });
        redact(&mut m);
        assert!(!m.to_string().contains("hunter2"), "{m}");
        assert_eq!(
            m["metadata"]["annotations"]["deployment.kubernetes.io/revision"],
            "3"
        );
        let tmeta = &m["spec"]["template"]["metadata"];
        assert_eq!(tmeta["annotations"]["prometheus.io/scrape"], "true");
        assert_eq!(
            tmeta["annotations"]["env"], "prod",
            "an annotation called env is not an env list"
        );
        assert_eq!(tmeta["labels"]["env"], "prod");
        assert_eq!(
            m["spec"]["template"]["spec"]["ephemeralContainers"][0]["env"][0]["value"],
            REDACTED
        );
    }
}
