//! ConfigMaps and Secrets reduced to what the Events pane records about
//! them: which keys exist and whether a value changed.  Values never leave
//! this module — ConfigMap values become a keyed SHA-256 digest, Secret
//! values only a size — so "changes observed by Valken" can say "1 key
//! removed (LEDGER_DB_URL)" without keeping anything sensitive.
//!
//! The digest key is random per process, so a digest says "same as before"
//! within a run and nothing else: it can't be checked against a guessed
//! value offline, and it isn't comparable across runs or machines.

use std::hash::{BuildHasher, Hasher};
use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ConfigKind {
    ConfigMap,
    Secret,
}

impl ConfigKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ConfigKind::ConfigMap => "ConfigMap",
            ConfigKind::Secret => "Secret",
        }
    }

    /// The `kubectl` short form used in labels: `cm/name`, `secret/name`.
    pub fn short(self) -> &'static str {
        match self {
            ConfigKind::ConfigMap => "cm",
            ConfigKind::Secret => "secret",
        }
    }
}

/// One key of a ConfigMap or Secret.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ConfigKey {
    pub name: String,
    /// Bytes of the value (decoded for Secrets).
    pub size: usize,
    /// ConfigMaps: hex keyed SHA-256 of the value (see [`digest`]), so a
    /// changed value is seen.  Secrets: empty.
    pub digest: String,
}

/// A ConfigMap or Secret without its values.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConfigMeta {
    pub kind: ConfigKind,
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub resource_version: String,
    pub created: Option<DateTime<Utc>>,
    /// Sorted by name.
    pub keys: Vec<ConfigKey>,
    /// `type` of a Secret (`Opaque`, `kubernetes.io/tls`, …); empty for ConfigMaps.
    pub secret_type: String,
}

impl ConfigMeta {
    /// `cm/ledger-config`, `secret/db-creds`.
    pub fn label(&self) -> String {
        format!("{}/{}", self.kind.short(), self.name)
    }

    pub fn key_names(&self) -> impl Iterator<Item = &str> {
        self.keys.iter().map(|k| k.name.as_str())
    }
}

fn base(kind: ConfigKind, meta: &ObjectMeta) -> ConfigMeta {
    ConfigMeta {
        kind,
        namespace: meta.namespace.clone().unwrap_or_default(),
        name: meta.name.clone().unwrap_or_default(),
        uid: meta.uid.clone().unwrap_or_default(),
        resource_version: meta.resource_version.clone().unwrap_or_default(),
        created: meta
            .creation_timestamp
            .as_ref()
            .and_then(|t| DateTime::from_timestamp(t.0.as_second(), 0)),
        keys: Vec::new(),
        secret_type: String::new(),
    }
}

/// 32 random bytes, drawn once per process from the standard library's
/// randomly seeded hasher.
fn digest_key() -> &'static [u8; 32] {
    static KEY: OnceLock<[u8; 32]> = OnceLock::new();
    KEY.get_or_init(|| {
        let mut key = [0u8; 32];
        for (i, chunk) in key.as_chunks_mut::<8>().0.iter_mut().enumerate() {
            let mut h = std::collections::hash_map::RandomState::new().build_hasher();
            h.write_usize(i);
            chunk.copy_from_slice(&h.finish().to_le_bytes());
        }
        key
    })
}

/// Hex SHA-256 of a per-process random key followed by `bytes`.  Equal
/// values get equal digests within a run; the digest can't be matched to a
/// guessed value without the key, and keys aren't shared or stored.
pub fn digest(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(digest_key());
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// The ConfigMap's keys (`data` and `binaryData`) with value digests.
pub fn config_map_meta(cm: &ConfigMap) -> ConfigMeta {
    let mut meta = base(ConfigKind::ConfigMap, &cm.metadata);
    if let Some(data) = &cm.data {
        meta.keys.extend(data.iter().map(|(k, v)| ConfigKey {
            name: k.clone(),
            size: v.len(),
            digest: digest(v.as_bytes()),
        }));
    }
    if let Some(data) = &cm.binary_data {
        meta.keys.extend(data.iter().map(|(k, v)| ConfigKey {
            name: k.clone(),
            size: v.0.len(),
            digest: digest(&v.0),
        }));
    }
    meta.keys.sort_by(|a, b| a.name.cmp(&b.name));
    meta
}

/// The Secret's keys (`data` and `stringData`) with sizes only.
pub fn secret_meta(secret: &Secret) -> ConfigMeta {
    let mut meta = base(ConfigKind::Secret, &secret.metadata);
    meta.secret_type = secret.type_.clone().unwrap_or_default();
    if let Some(data) = &secret.data {
        meta.keys.extend(data.iter().map(|(k, v)| ConfigKey {
            name: k.clone(),
            size: v.0.len(),
            digest: String::new(),
        }));
    }
    if let Some(data) = &secret.string_data {
        meta.keys.extend(data.iter().map(|(k, v)| ConfigKey {
            name: k.clone(),
            size: v.len(),
            digest: String::new(),
        }));
    }
    meta.keys.sort_by(|a, b| a.name.cmp(&b.name));
    meta
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::ByteString;
    use std::collections::BTreeMap;

    fn meta(name: &str, rv: &str) -> ObjectMeta {
        ObjectMeta {
            name: Some(name.into()),
            namespace: Some("payments".into()),
            uid: Some(format!("uid-{name}")),
            resource_version: Some(rv.into()),
            ..Default::default()
        }
    }

    #[test]
    fn config_map_keys_carry_digests_not_values() {
        let cm = ConfigMap {
            metadata: meta("ledger-config", "41"),
            data: Some(BTreeMap::from([
                ("LEDGER_DB_URL".to_string(), "postgres://x".to_string()),
                ("A".to_string(), "1".to_string()),
            ])),
            binary_data: Some(BTreeMap::from([(
                "blob".to_string(),
                ByteString(vec![1, 2, 3]),
            )])),
            ..Default::default()
        };
        let m = config_map_meta(&cm);
        assert_eq!(
            (m.kind, m.label().as_str()),
            (ConfigKind::ConfigMap, "cm/ledger-config")
        );
        assert_eq!(
            m.key_names().collect::<Vec<_>>(),
            ["A", "LEDGER_DB_URL", "blob"],
            "sorted by name"
        );
        let url = &m.keys[1];
        assert_eq!(url.size, "postgres://x".len());
        assert_eq!(url.digest, digest(b"postgres://x"));
        assert_eq!(url.digest.len(), 64);
        assert_eq!(m.keys[2].size, 3);
        assert_eq!(m.resource_version, "41");
        assert!(
            !serde_json::to_string(&m).unwrap().contains("postgres://"),
            "values never serialised"
        );
    }

    #[test]
    fn secret_keys_carry_sizes_only() {
        let s = Secret {
            metadata: meta("db-creds", "7"),
            type_: Some("Opaque".into()),
            data: Some(BTreeMap::from([(
                "password".to_string(),
                ByteString(b"hunter2".to_vec()),
            )])),
            string_data: Some(BTreeMap::from([("user".to_string(), "root".to_string())])),
            ..Default::default()
        };
        let m = secret_meta(&s);
        assert_eq!(
            (m.kind, m.secret_type.as_str(), m.label().as_str()),
            (ConfigKind::Secret, "Opaque", "secret/db-creds")
        );
        assert_eq!(
            m.keys
                .iter()
                .map(|k| (k.name.as_str(), k.size))
                .collect::<Vec<_>>(),
            [("password", 7), ("user", 4)]
        );
        assert!(
            m.keys.iter().all(|k| k.digest.is_empty()),
            "no digest for secret values"
        );
        let json = serde_json::to_string(&m).unwrap();
        assert!(!json.contains("hunter2") && !json.contains("root"));
    }

    #[test]
    fn digest_is_keyed_hex_sha256() {
        let a = digest(b"abc");
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(a, digest(b"abc"), "stable within the process");
        assert_ne!(a, digest(b"abd"));
        assert_ne!(
            a, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            "not the plain SHA-256, so a guessed value can't be confirmed offline"
        );
        assert_ne!(
            digest(b""),
            digest_key()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
    }

    #[test]
    fn empty_objects_have_no_keys() {
        assert!(config_map_meta(&ConfigMap::default()).keys.is_empty());
        let m = secret_meta(&Secret::default());
        assert!(m.keys.is_empty() && m.created.is_none() && m.uid.is_empty());
    }
}
