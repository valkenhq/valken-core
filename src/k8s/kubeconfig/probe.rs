//! Pre-connect checks for one context.
//!
//! [`probe`] calls `GET /version` (reachability, latency, server version),
//! then sends one `SelfSubjectAccessReview` per [`Need`] so the picker can
//! say what won't work *before* connecting.  Callers run it on a Tokio task
//! and abort the task when the selection changes; [`ProbeCache`] keeps
//! results for a short while so flicking back and forth doesn't re-probe.
//!
//! Exec-plugin contexts must only be probed when the user asks: building the
//! client is harmless, but the first request runs the plugin.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures::future::join_all;
use k8s_openapi::api::authorization::v1::{
    ResourceAttributes, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec,
};
use kube::api::PostParams;
use kube::{Api, Client, Config};
use thiserror::Error;

use super::{AuthKind, ContextKey, ExecEnv, PluginNotFound};
use crate::k8s::error::describe;

/// A permission Valken relies on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Need {
    ListNamespaces,
    ListPods,
    ListServices,
    ListIngresses,
    ListNetworkPolicies,
    GetPodLogs,
    ListNodes,
}

impl Need {
    pub const ALL: [Need; 7] = [
        Need::ListNamespaces,
        Need::ListPods,
        Need::ListServices,
        Need::ListIngresses,
        Need::ListNetworkPolicies,
        Need::GetPodLogs,
        Need::ListNodes,
    ];

    fn attributes(self) -> ResourceAttributes {
        let (verb, group, resource, subresource) = match self {
            Need::ListNamespaces => ("list", "", "namespaces", None),
            Need::ListPods => ("list", "", "pods", None),
            Need::ListServices => ("list", "", "services", None),
            Need::ListIngresses => ("list", "networking.k8s.io", "ingresses", None),
            Need::ListNetworkPolicies => ("list", "networking.k8s.io", "networkpolicies", None),
            Need::GetPodLogs => ("get", "", "pods", Some("log")),
            Need::ListNodes => ("list", "", "nodes", None),
        };
        ResourceAttributes {
            verb: Some(verb.into()),
            group: Some(group.into()),
            resource: Some(resource.into()),
            subresource: subresource.map(Into::into),
            ..Default::default()
        }
    }

    /// `verb resource` as kubectl would write it, e.g. `get pods/log`.
    pub fn as_verb_resource(self) -> &'static str {
        match self {
            Need::ListNamespaces => "list namespaces",
            Need::ListPods => "list pods",
            Need::ListServices => "list services",
            Need::ListIngresses => "list ingresses",
            Need::ListNetworkPolicies => "list networkpolicies",
            Need::GetPodLogs => "get pods/log",
            Need::ListNodes => "list nodes",
        }
    }
}

/// The answer for one [`Need`].  `allowed` is `None` when the review itself
/// failed (old server, SSAR forbidden), so the UI can say "unknown".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessCheck {
    pub need: Need,
    pub allowed: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeReport {
    pub latency: Duration,
    /// `gitVersion`, e.g. `v1.31.4`.
    pub server_version: String,
    /// Kubernetes minor version, when it parses.
    pub minor: Option<u32>,
    pub access: Vec<AccessCheck>,
}

impl ProbeReport {
    pub fn denied(&self) -> impl Iterator<Item = Need> + '_ {
        self.access
            .iter()
            .filter(|a| a.allowed == Some(false))
            .map(|a| a.need)
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ProbeError {
    /// The kubeconfig entry can't be turned into a client (bad URL, missing
    /// certificate file, …).
    #[error("invalid kubeconfig: {0}")]
    Config(String),
    #[error(transparent)]
    PluginNotFound(#[from] PluginNotFound),
    /// The exec plugin or auth provider ran but failed.
    #[error("credential plugin failed: {0}")]
    Plugin(String),
    /// No answer within the timeout.
    #[error("no response after {}s", .0.as_secs())]
    Timeout(Duration),
    /// Connection refused, DNS, TLS, …
    #[error("unreachable: {0}")]
    Unreachable(String),
    /// The server rejected the credentials (401).
    #[error("unauthorized: {0}")]
    Unauthorized(String),
}

/// Probe `config`.  `auth` is the context's auth kind; for exec plugins the
/// binary is looked up first so a missing `aws` is reported as such instead
/// of as an opaque spawn error.
pub async fn probe(
    config: Config,
    auth: &AuthKind,
    exec: &ExecEnv,
    timeout: Duration,
) -> Result<ProbeReport, ProbeError> {
    if let AuthKind::Exec { command } = auth {
        exec.resolve(command)?;
    }
    let client = Client::try_from(config).map_err(|e| ProbeError::Config(describe(&e)))?;

    let start = Instant::now();
    let version = tokio::time::timeout(timeout, client.apiserver_version())
        .await
        .map_err(|_| ProbeError::Timeout(timeout))?
        .map_err(classify)?;
    let latency = start.elapsed();

    let api: Api<SelfSubjectAccessReview> = Api::all(client);
    let reviews = Need::ALL.iter().map(|&need| {
        let api = api.clone();
        async move {
            let review = SelfSubjectAccessReview {
                spec: SelfSubjectAccessReviewSpec {
                    resource_attributes: Some(need.attributes()),
                    ..Default::default()
                },
                ..Default::default()
            };
            let outcome = api
                .create(&PostParams::default(), &review)
                .await
                .map(|r| r.status.map(|s| s.allowed));
            (need, outcome)
        }
    });
    let outcomes = match tokio::time::timeout(timeout, join_all(reviews)).await {
        Ok(outcomes) => outcomes,
        Err(_) => Need::ALL.iter().map(|&need| (need, Ok(None))).collect(),
    };
    let mut access = Vec::with_capacity(Need::ALL.len());
    for (need, outcome) in outcomes {
        match outcome {
            Ok(allowed) => access.push(AccessCheck { need, allowed }),
            // `/version` is open to anonymous users on most clusters, so
            // bad or expired credentials first show up here.
            Err(e) if rejected_credentials(&e) => return Err(classify(e)),
            // No permission to review (403), an old server, …: unknown.
            Err(_) => access.push(AccessCheck {
                need,
                allowed: None,
            }),
        }
    }

    Ok(ProbeReport {
        latency,
        minor: parse_minor(&version.minor),
        server_version: version.git_version,
        access,
    })
}

fn rejected_credentials(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(s) if s.code == 401) || matches!(e, kube::Error::Auth(_))
}

fn parse_minor(raw: &str) -> Option<u32> {
    // EKS and GKE report minors like "31+".
    raw.trim_end_matches('+').parse().ok()
}

/// Sort a kube error into what the user can act on.
pub(crate) fn classify(e: kube::Error) -> ProbeError {
    classify_ref(&e)
}

/// [`classify`] without taking ownership (watch errors are borrowed).
pub(crate) fn classify_ref(e: &kube::Error) -> ProbeError {
    match e {
        kube::Error::Api(status) if status.code == 401 => ProbeError::Unauthorized(describe(e)),
        kube::Error::Auth(_) => ProbeError::Plugin(describe(e)),
        kube::Error::InferConfig(_) => ProbeError::Config(describe(e)),
        _ => ProbeError::Unreachable(describe(e)),
    }
}

/// Probe results for a short while, keyed by [`ContextKey`].
#[derive(Debug)]
pub struct ProbeCache {
    ttl: Duration,
    entries: HashMap<ContextKey, (Instant, Result<ProbeReport, ProbeError>)>,
}

impl Default for ProbeCache {
    fn default() -> Self {
        Self::new(Duration::from_secs(60))
    }
}

impl ProbeCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: HashMap::new(),
        }
    }

    pub fn get(&self, key: &ContextKey) -> Option<&Result<ProbeReport, ProbeError>> {
        self.get_at(key, Instant::now())
    }

    fn get_at(&self, key: &ContextKey, now: Instant) -> Option<&Result<ProbeReport, ProbeError>> {
        self.entries
            .get(key)
            .filter(|(at, _)| now.duration_since(*at) < self.ttl)
            .map(|(_, r)| r)
    }

    pub fn insert(&mut self, key: ContextKey, result: Result<ProbeReport, ProbeError>) {
        let now = Instant::now();
        self.entries
            .retain(|_, (at, _)| now.duration_since(*at) < self.ttl);
        self.entries.insert(key, (now, result));
    }

    pub fn invalidate(&mut self, key: &ContextKey) {
        self.entries.remove(key);
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn key(n: &str) -> ContextKey {
        ContextKey {
            origin: PathBuf::from("/k"),
            name: n.into(),
            server: "https://s".into(),
        }
    }

    #[test]
    fn cache_expires() {
        let mut c = ProbeCache::new(Duration::from_secs(60));
        c.insert(key("a"), Err(ProbeError::Timeout(Duration::from_secs(5))));
        assert!(c.get(&key("a")).is_some());
        assert!(c.get(&key("b")).is_none());
        let later = Instant::now() + Duration::from_secs(61);
        assert!(c.get_at(&key("a"), later).is_none());
    }

    #[test]
    fn minor_versions() {
        assert_eq!(parse_minor("31"), Some(31));
        assert_eq!(parse_minor("29+"), Some(29));
        assert_eq!(parse_minor(""), None);
    }

    #[tokio::test]
    async fn missing_plugin_is_reported_before_any_request() {
        let config = Config::new("https://127.0.0.1:1".parse().unwrap());
        let err = probe(
            config,
            &AuthKind::Exec {
                command: "valken-definitely-not-a-plugin".into(),
            },
            &ExecEnv::default(),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ProbeError::PluginNotFound(_)));
    }

    const VERSION_BODY: &str = r#"{"major":"1","minor":"32","gitVersion":"v1.32.0","gitCommit":"","gitTreeState":"","buildDate":"","goVersion":"","compiler":"","platform":""}"#;
    const UNAUTHORIZED: &str = r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"Unauthorized","reason":"Unauthorized","code":401}"#;
    const FORBIDDEN: &str = r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"forbidden","reason":"Forbidden","code":403}"#;

    /// `GET /version` answers 200; every other request gets `status`/`body`.
    async fn fake_server(status: u16, body: &'static str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 16 * 1024];
                    let n = match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    let head = String::from_utf8_lossy(&buf[..n]);
                    let (code, body) = if head.starts_with("GET /version") {
                        (200, VERSION_BODY)
                    } else {
                        (status, body)
                    };
                    // One request per connection keeps a late body from
                    // being read as the next request.
                    let resp = format!(
                        "HTTP/1.1 {code} X\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejected_credentials_fail_the_probe_even_when_version_is_open() {
        let url = fake_server(401, UNAUTHORIZED).await;
        let err = probe(
            Config::new(url.parse().unwrap()),
            &AuthKind::None,
            &ExecEnv::default(),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ProbeError::Unauthorized(_)), "{err:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forbidden_reviews_are_unknown_not_fatal() {
        let url = fake_server(403, FORBIDDEN).await;
        let report = probe(
            Config::new(url.parse().unwrap()),
            &AuthKind::None,
            &ExecEnv::default(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(report.minor, Some(32));
        assert_eq!(report.access.len(), Need::ALL.len());
        assert!(report.access.iter().all(|a| a.allowed.is_none()));
    }

    #[tokio::test]
    async fn closed_port_is_unreachable() {
        let mut config = Config::new("http://127.0.0.1:1".parse().unwrap());
        config.connect_timeout = Some(Duration::from_secs(1));
        let err = probe(
            config,
            &AuthKind::None,
            &ExecEnv::default(),
            Duration::from_secs(2),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, ProbeError::Unreachable(_) | ProbeError::Timeout(_)),
            "{err:?}"
        );
    }
}
