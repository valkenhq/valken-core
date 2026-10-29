//! Registry credentials from `imagePullSecrets` (explicit opt-in only: it
//! needs `get secrets`, which Valken's read-only role doesn't grant).
//!
//! Only the secrets a pod references are read, one `get` each, and the
//! Secret object is dropped as soon as the credentials are parsed out.

use std::collections::BTreeMap;
use std::fmt;

use base64::Engine;
use k8s_openapi::api::core::v1::Secret;
use serde::Deserialize;

/// Username/password for one registry host.
#[derive(Clone, PartialEq, Eq)]
pub struct RegistryCredential {
    /// Registry host, e.g. `ghcr.io`, `docker.io`, `harbor.local:5000`.
    pub registry: String,
    pub username: String,
    pub password: String,
}

impl fmt::Debug for RegistryCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryCredential")
            .field("registry", &self.registry)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

#[derive(Deserialize)]
struct DockerConfigJson {
    #[serde(default)]
    auths: BTreeMap<String, DockerAuth>,
}

#[derive(Deserialize)]
struct DockerAuth {
    username: Option<String>,
    password: Option<String>,
    auth: Option<String>,
}

/// Credentials in a `kubernetes.io/dockerconfigjson` or
/// `kubernetes.io/dockercfg` Secret. Anything else yields nothing.
pub(crate) fn credentials_from_secret(secret: &Secret) -> Vec<RegistryCredential> {
    let data = secret.data.as_ref();
    let auths = if let Some(json) = data.and_then(|d| d.get(".dockerconfigjson")) {
        serde_json::from_slice::<DockerConfigJson>(&json.0)
            .map(|c| c.auths)
            .ok()
    } else if let Some(cfg) = data.and_then(|d| d.get(".dockercfg")) {
        serde_json::from_slice::<BTreeMap<String, DockerAuth>>(&cfg.0).ok()
    } else {
        None
    };

    auths
        .into_iter()
        .flatten()
        .filter_map(|(registry, auth)| {
            let (username, password) = match (auth.username, auth.password, auth.auth) {
                (Some(u), Some(p), _) => (u, p),
                (_, _, Some(encoded)) => decode_auth(&encoded)?,
                _ => return None,
            };
            Some(RegistryCredential {
                registry: registry_host(&registry),
                username,
                password,
            })
        })
        .collect()
}

/// `base64("user:pass")` → `(user, pass)`.
fn decode_auth(encoded: &str) -> Option<(String, String)> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .ok()?;
    let text = String::from_utf8(bytes).ok()?;
    let (user, pass) = text.split_once(':')?;
    Some((user.to_owned(), pass.to_owned()))
}

/// `https://index.docker.io/v1/` → `docker.io`; `ghcr.io/x` → `ghcr.io`.
fn registry_host(key: &str) -> String {
    let host = key
        .split_once("://")
        .map_or(key, |(_, rest)| rest)
        .split('/')
        .next()
        .unwrap_or(key)
        .to_ascii_lowercase();
    match host.as_str() {
        "index.docker.io" | "registry-1.docker.io" | "registry.hub.docker.com" => {
            "docker.io".into()
        }
        _ => host,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::ByteString;

    fn secret(key: &str, body: &str) -> Secret {
        Secret {
            data: Some([(key.to_string(), ByteString(body.as_bytes().to_vec()))].into()),
            ..Default::default()
        }
    }

    #[test]
    fn parses_dockerconfigjson_and_dockercfg() {
        let auth = base64::engine::general_purpose::STANDARD.encode("bot:tok:en");
        let json = format!(
            r#"{{"auths":{{"ghcr.io":{{"username":"u","password":"p"}},"https://index.docker.io/v1/":{{"auth":"{auth}"}},"broken":{{}}}}}}"#
        );
        let creds = credentials_from_secret(&secret(".dockerconfigjson", &json));
        assert_eq!(creds.len(), 2);
        assert_eq!(
            creds
                .iter()
                .find(|c| c.registry == "docker.io")
                .map(|c| (c.username.as_str(), c.password.as_str())),
            Some(("bot", "tok:en"))
        );
        assert!(creds.iter().any(|c| c.registry == "ghcr.io"));

        let legacy = credentials_from_secret(&secret(
            ".dockercfg",
            r#"{"harbor.local:5000":{"username":"a","password":"b"}}"#,
        ));
        assert_eq!(legacy[0].registry, "harbor.local:5000");

        assert!(credentials_from_secret(&secret("token", "x")).is_empty());
        assert!(credentials_from_secret(&secret(".dockerconfigjson", "not json")).is_empty());
    }

    #[test]
    fn debug_never_prints_the_password() {
        let c = RegistryCredential {
            registry: "ghcr.io".into(),
            username: "u".into(),
            password: "hunter2".into(),
        };
        assert!(!format!("{c:?}").contains("hunter2"));
    }
}
