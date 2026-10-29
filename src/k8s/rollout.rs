//! A Deployment's rollout history, from its ReplicaSets (what
//! `kubectl rollout history` shows): revision, images, when, change cause.

use chrono::{DateTime, Utc};
use k8s_openapi::api::apps::v1::{Deployment, ReplicaSet};
use kube::api::ListParams;
use kube::{Api, Client};

use crate::k8s::error::ObjectError;

const REVISION: &str = "deployment.kubernetes.io/revision";
const CHANGE_CAUSE: &str = "kubernetes.io/change-cause";

/// One rollout revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Revision {
    pub revision: i64,
    pub replica_set: String,
    /// `container=image` per container, in spec order.
    pub images: Vec<String>,
    pub created: Option<DateTime<Utc>>,
    pub replicas: i32,
    pub ready: i32,
    pub change_cause: Option<String>,
    /// The revision the Deployment is on now.
    pub current: bool,
}

/// Revisions of `deployment`, newest first.
#[tracing::instrument(name = "k8s.rollout.history", skip(client))]
pub async fn history(
    client: &Client,
    namespace: &str,
    deployment: &str,
) -> Result<Vec<Revision>, ObjectError> {
    let deploy = Api::<Deployment>::namespaced(client.clone(), namespace)
        .get(deployment)
        .await
        .map_err(ObjectError::from_api)?;
    let sets = Api::<ReplicaSet>::namespaced(client.clone(), namespace)
        .list(&ListParams::default())
        .await
        .map_err(ObjectError::from_api)?;
    let uid = deploy.metadata.uid.as_deref().unwrap_or_default();
    Ok(revisions(
        uid,
        revision_of(&deploy.metadata.annotations),
        &sets.items,
    ))
}

fn revision_of(annotations: &Option<std::collections::BTreeMap<String, String>>) -> Option<i64> {
    annotations.as_ref()?.get(REVISION)?.parse().ok()
}

/// The ReplicaSets owned by `owner_uid`, as revisions, newest first.
pub fn revisions(owner_uid: &str, current: Option<i64>, sets: &[ReplicaSet]) -> Vec<Revision> {
    let mut out: Vec<Revision> = sets
        .iter()
        .filter(|rs| {
            rs.metadata
                .owner_references
                .as_ref()
                .is_some_and(|owners| owners.iter().any(|o| o.uid == owner_uid))
        })
        .filter_map(|rs| {
            let revision = revision_of(&rs.metadata.annotations)?;
            let images = rs
                .spec
                .as_ref()
                .and_then(|s| s.template.as_ref())
                .and_then(|t| t.spec.as_ref())
                .map(|p| {
                    p.containers
                        .iter()
                        .map(|c| format!("{}={}", c.name, c.image.as_deref().unwrap_or("?")))
                        .collect()
                })
                .unwrap_or_default();
            let status = rs.status.as_ref();
            Some(Revision {
                revision,
                replica_set: rs.metadata.name.clone().unwrap_or_default(),
                images,
                created: rs
                    .metadata
                    .creation_timestamp
                    .as_ref()
                    .and_then(|t| DateTime::from_timestamp(t.0.as_second(), 0)),
                replicas: status.map_or(0, |s| s.replicas),
                ready: status.and_then(|s| s.ready_replicas).unwrap_or(0),
                change_cause: rs
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get(CHANGE_CAUSE).cloned()),
                current: current == Some(revision),
            })
        })
        .collect();
    out.sort_by_key(|r| std::cmp::Reverse(r.revision));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::apps::v1::{ReplicaSetSpec, ReplicaSetStatus};
    use k8s_openapi::api::core::v1::{Container, PodSpec, PodTemplateSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};

    fn rs(name: &str, owner: &str, revision: Option<&str>, image: &str, ready: i32) -> ReplicaSet {
        let mut annotations = std::collections::BTreeMap::new();
        if let Some(r) = revision {
            annotations.insert(REVISION.to_string(), r.to_string());
        }
        ReplicaSet {
            metadata: ObjectMeta {
                name: Some(name.into()),
                annotations: Some(annotations),
                owner_references: Some(vec![OwnerReference {
                    uid: owner.into(),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            spec: Some(ReplicaSetSpec {
                template: Some(PodTemplateSpec {
                    spec: Some(PodSpec {
                        containers: vec![Container {
                            name: "app".into(),
                            image: Some(image.into()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            status: Some(ReplicaSetStatus {
                replicas: ready,
                ready_replicas: Some(ready),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn owned_sets_newest_first_with_current_marked() {
        let sets = [
            rs("web-1", "u1", Some("1"), "web:2.14.0", 0),
            rs("web-2", "u1", Some("2"), "web:2.14.1", 3),
            rs("other", "u2", Some("9"), "x:1", 1),
            rs("no-rev", "u1", None, "y:1", 1),
        ];
        let revs = revisions("u1", Some(2), &sets);
        assert_eq!(revs.iter().map(|r| r.revision).collect::<Vec<_>>(), [2, 1]);
        assert!(revs[0].current && !revs[1].current);
        assert_eq!(revs[0].images, ["app=web:2.14.1"]);
        assert_eq!((revs[0].replicas, revs[0].ready), (3, 3));
    }
}
