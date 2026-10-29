use std::collections::{HashMap, HashSet};
use tracing::trace;

use super::KubeResource;
use super::KubeResourceIdentifier;
use crate::k8s::StoreError;

/// Flat map of every known resource keyed by its KRI, plus an index of
/// the KRIs in each namespace frame so [`KubeResourceStore::iter_frame`]
/// (run on every incremental graph update) doesn't scan the whole store.
///
/// Resources are `Arc`-backed, so cloning the store is a snapshot that
/// costs one reference bump per resource — that's how a background graph
/// build reads it without holding the lock.
#[derive(Debug, Default, Clone)]
pub struct KubeResourceStore {
    inner: HashMap<KubeResourceIdentifier, KubeResource>,
    /// Frame key (namespace or `__cluster__`) → the KRIs in it.
    frames: HashMap<String, HashSet<KubeResourceIdentifier>>,
    /// Local cluster integer assigned by the DB clusters table.
    /// `0` = unassigned (pre-connection).
    cluster_id: u32,
}

/// Derive the KRI for a resource under `cluster_id`.
pub fn kri_for(
    cluster_id: u32,
    resource: &KubeResource,
) -> Result<KubeResourceIdentifier, StoreError> {
    let missing = || StoreError::MissingName {
        kind: resource.kind().to_string(),
    };
    match resource {
        KubeResource::Pod(pod) => {
            KubeResourceIdentifier::from_pod(cluster_id, pod).ok_or_else(missing)
        }
        KubeResource::Service(svc) => {
            KubeResourceIdentifier::from_service(cluster_id, svc).ok_or_else(missing)
        }
        KubeResource::Deployment(dep) => {
            KubeResourceIdentifier::from_deployment(cluster_id, dep).ok_or_else(missing)
        }
        KubeResource::Ingress(ing) => {
            KubeResourceIdentifier::from_ingress(cluster_id, ing).ok_or_else(missing)
        }
        KubeResource::CustomResource {
            kind,
            api_version,
            name,
            namespace,
            creation_timestamp,
            ..
        } => {
            // api_version may be "group/version" or just "version" for core resources.
            let (group, version) = match api_version.split_once('/') {
                Some((g, v)) => (g, v),
                None => ("core", api_version.as_str()),
            };
            KubeResourceIdentifier::from_custom_resource(
                cluster_id,
                namespace.clone(),
                group,
                version,
                kind,
                name,
                creation_timestamp.as_ref(),
            )
            .ok_or_else(missing)
        }
        #[cfg(test)]
        KubeResource::Dummy(_) => Err(StoreError::Unsupported("Dummy".into())),
    }
}

impl KubeResourceStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_cluster_id(&mut self, id: u32) {
        self.cluster_id = id;
    }

    pub fn cluster_id(&self) -> u32 {
        self.cluster_id
    }

    /// Remove every resource (a full snapshot is about to replace them).
    pub fn clear(&mut self) {
        self.inner.clear();
        self.frames.clear();
    }

    /// Insert under an explicit KRI.  Returns `true` if newly inserted.
    pub(crate) fn insert_with_kri(
        &mut self,
        kri: KubeResourceIdentifier,
        resource: KubeResource,
    ) -> bool {
        trace!(kri = %kri, "store insert");
        let is_new = !self.inner.contains_key(&kri);
        if is_new {
            self.frames
                .entry(kri.frame_key().to_string())
                .or_default()
                .insert(kri.clone());
        }
        self.inner.insert(kri, resource);
        is_new
    }

    /// Insert a resource, deriving its KRI from the store's cluster id.
    /// Returns the KRI and whether it was newly inserted (vs updated).
    pub fn insert(
        &mut self,
        resource: KubeResource,
    ) -> Result<(KubeResourceIdentifier, bool), StoreError> {
        let kri = kri_for(self.cluster_id, &resource)?;
        let is_new = self.insert_with_kri(kri.clone(), resource);
        Ok((kri, is_new))
    }

    /// Removes a resource by KRI. Returns `true` if it was present.
    pub fn remove(&mut self, kri: &KubeResourceIdentifier) -> bool {
        let removed = self.inner.remove(kri).is_some();
        if removed {
            let frame = kri.frame_key();
            if let Some(set) = self.frames.get_mut(frame) {
                set.remove(kri);
                if set.is_empty() {
                    self.frames.remove(frame);
                }
            }
        }
        removed
    }

    pub fn iter(
        &self,
    ) -> std::collections::hash_map::Iter<'_, KubeResourceIdentifier, KubeResource> {
        self.inner.iter()
    }

    /// Every resource whose namespace frame is `frame_key`.
    pub fn iter_frame<'a>(
        &'a self,
        frame_key: &str,
    ) -> impl Iterator<Item = (&'a KubeResourceIdentifier, &'a KubeResource)> + 'a {
        self.frames
            .get(frame_key)
            .into_iter()
            .flatten()
            .filter_map(move |kri| self.inner.get(kri).map(|r| (kri, r)))
    }

    pub fn get(&self, kri: &KubeResourceIdentifier) -> Option<KubeResource> {
        self.inner.get(kri).cloned()
    }

    pub fn get_ref(&self, kri: &KubeResourceIdentifier) -> Option<&KubeResource> {
        self.inner.get(kri)
    }

    pub fn contains(&self, kri: &KubeResourceIdentifier) -> bool {
        self.inner.contains_key(kri)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Frame keys that currently hold at least one resource.
    pub fn get_known_namespaces(&self) -> Vec<String> {
        self.frames.keys().cloned().collect()
    }

    pub fn get_all_resources(&self) -> Vec<(KubeResourceIdentifier, KubeResource)> {
        self.inner
            .iter()
            .map(|(kri, resource)| (kri.clone(), resource.clone()))
            .collect()
    }

    /// Returns clones of every KRI key in the store.  Much cheaper than
    /// `get_all_resources` because it never touches the resource values.
    pub fn get_all_kris(&self) -> Vec<KubeResourceIdentifier> {
        self.inner.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::Pod;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn dummy_kri(ns: &str, kind: &str, name: &str) -> KubeResourceIdentifier {
        KubeResourceIdentifier::new(1, Some(ns.to_string()), "", "v1", kind, name)
    }

    fn dummy_resource(name: &str) -> KubeResource {
        KubeResource::Dummy(name.to_string())
    }

    fn pod(ns: &str, name: &str) -> KubeResource {
        KubeResource::Pod(
            Pod {
                metadata: ObjectMeta {
                    name: Some(name.into()),
                    namespace: Some(ns.into()),
                    ..Default::default()
                },
                ..Default::default()
            }
            .into(),
        )
    }

    #[test]
    fn test_insert_and_get() {
        let mut store = KubeResourceStore::new();
        let kri = dummy_kri("default", "Pod", "nginx");
        let resource = dummy_resource("nginx");
        assert!(store.insert_with_kri(kri.clone(), resource.clone()));
        assert!(!store.insert_with_kri(kri.clone(), resource.clone()));
        assert!(store.contains(&kri));
        assert_eq!(store.get(&kri), Some(resource));
    }

    #[test]
    fn test_store_empty_and_len() {
        let mut store = KubeResourceStore::new();
        assert!(store.is_empty());
        store.insert_with_kri(dummy_kri("ns1", "Service", "svc1"), dummy_resource("svc1"));
        store.insert_with_kri(dummy_kri("ns1", "Pod", "pod1"), dummy_resource("pod1"));
        assert_eq!(store.len(), 2);
        assert_eq!(store.get_known_namespaces(), vec!["ns1".to_string()]);
    }

    #[test]
    fn namespaces_disappear_when_their_last_resource_is_removed() {
        let mut store = KubeResourceStore::new();
        let (a, _) = store.insert(pod("a", "x")).unwrap();
        let (b1, _) = store.insert(pod("b", "x")).unwrap();
        let (b2, _) = store.insert(pod("b", "y")).unwrap();
        let mut ns = store.get_known_namespaces();
        ns.sort();
        assert_eq!(ns, vec!["a", "b"]);

        assert!(store.remove(&b1));
        assert!(!store.remove(&b1), "second remove is a no-op");
        assert_eq!(store.get_known_namespaces().len(), 2, "b still has y");
        store.remove(&b2);
        assert_eq!(store.get_known_namespaces(), vec!["a".to_string()]);
        store.remove(&a);
        assert!(store.get_known_namespaces().is_empty());
    }

    #[test]
    fn clear_empties_everything() {
        let mut store = KubeResourceStore::new();
        store.insert(pod("a", "x")).unwrap();
        store.clear();
        assert!(store.is_empty());
        assert!(store.get_known_namespaces().is_empty());
    }

    #[test]
    fn insert_errors_are_typed() {
        let mut store = KubeResourceStore::new();
        let err = store
            .insert(KubeResource::Pod(Pod::default().into()))
            .unwrap_err();
        assert_eq!(err, StoreError::MissingName { kind: "Pod".into() });
        assert!(matches!(
            store.insert(dummy_resource("d")).unwrap_err(),
            StoreError::Unsupported(_)
        ));
    }

    #[test]
    fn frame_index_follows_removals_and_clear() {
        let mut store = KubeResourceStore::new();
        let (a, _) = store.insert(pod("a", "p1")).unwrap();
        let (b, _) = store.insert(pod("a", "p2")).unwrap();
        store.insert(pod("b", "q")).unwrap();
        assert_eq!(store.iter_frame("a").count(), 2);

        assert!(store.remove(&a));
        let left: Vec<_> = store.iter_frame("a").map(|(k, _)| k.clone()).collect();
        assert_eq!(left, vec![b.clone()]);
        assert!(store.remove(&b));
        assert_eq!(store.iter_frame("a").count(), 0);
        assert_eq!(store.get_known_namespaces(), vec!["b".to_string()]);

        // Updating an existing resource doesn't index it twice.
        store.insert(pod("b", "q")).unwrap();
        assert_eq!(store.iter_frame("b").count(), 1);

        let snapshot = store.clone();
        store.clear();
        assert_eq!(store.iter_frame("b").count(), 0);
        assert_eq!(
            snapshot.iter_frame("b").count(),
            1,
            "snapshot is independent"
        );
    }

    #[test]
    fn iter_frame_filters_by_namespace() {
        let mut store = KubeResourceStore::new();
        store.insert(pod("a", "x")).unwrap();
        store.insert(pod("b", "y")).unwrap();
        store.insert(pod("a", "z")).unwrap();
        assert_eq!(store.iter_frame("a").count(), 2);
        assert_eq!(store.iter_frame("c").count(), 0);
    }
}
