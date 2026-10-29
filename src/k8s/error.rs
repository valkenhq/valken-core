//! Typed errors for the Kubernetes layer.  Callers that do not care can
//! still `?` these into `anyhow::Error`.
//!
//! Every `Display` here that wraps a [`kube::Error`] goes through
//! [`describe`], which keeps credential-plugin failures free of the
//! plugin's environment and output.  `Debug` and `source()` are *not*
//! sanitised: log errors with `%`, not `?`, and don't print error chains.

use thiserror::Error;

/// A [`kube::Error`] as text that is safe to log or show.
///
/// kube's own `Display` for a failed exec plugin embeds the `Command`'s
/// `Debug` output — program, arguments **and every environment variable
/// set on it** (the kubeconfig's `exec.env`, which is where client secrets
/// live) — followed by the plugin's raw stdout and stderr.  This keeps only
/// the exit status and the last non-empty line of stderr.
pub fn describe(e: &kube::Error) -> String {
    match e {
        kube::Error::Auth(kube::client::AuthError::AuthExecRun { status, out, .. }) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            let last = stderr
                .lines()
                .rev()
                .map(str::trim)
                .find(|l| !l.is_empty())
                .unwrap_or_default();
            let mut msg = format!("credential plugin failed ({status})");
            if !last.is_empty() {
                msg.push_str(": ");
                msg.extend(last.chars().take(200));
            }
            msg
        }
        other => other.to_string(),
    }
}

/// [`describe`] for a watch error, whose `Display` embeds the kube error.
pub fn describe_watch(e: &kube::runtime::watcher::Error) -> String {
    use kube::runtime::watcher::Error as W;
    match e {
        W::InitialListFailed(k) => format!("initial list failed: {}", describe(k)),
        W::WatchStartFailed(k) => format!("watch start failed: {}", describe(k)),
        W::WatchFailed(k) => format!("watch failed: {}", describe(k)),
        other => other.to_string(),
    }
}

/// Failure to establish a connection to a cluster.
#[derive(Debug, Error)]
pub enum ConnectError {
    #[error("could not infer Kubernetes configuration: {0}")]
    Config(#[from] kube::config::InferConfigError),
    #[error("could not build Kubernetes client: {}", describe(.0))]
    Client(#[source] kube::Error),
    /// A pre-watch step of [`crate::k8s::KubeWatcher::connect_context`]
    /// failed (plugin missing, unreachable, unauthorized, timeout).
    #[error(transparent)]
    Cluster(#[from] crate::k8s::kubeconfig::probe::ProbeError),
}

/// Failure to start or run the resource watch.
#[derive(Debug, Error)]
pub enum WatchError {
    #[error("not connected — call connect() before begin_watching()")]
    NotConnected,
    #[error("already watching — call stop() first")]
    AlreadyWatching,
    #[error("initial resource sync failed: {}", describe_watch(.0))]
    InitialSync(#[source] kube::runtime::watcher::Error),
    /// A watch failed after its initial sync; the stream backs off and
    /// retries by itself, this is informational.
    #[error("watch failed: {}", describe_watch(.0))]
    Stream(#[source] kube::runtime::watcher::Error),
    #[error("watch task ended before the initial sync completed")]
    Aborted,
}

/// Failure to derive a [`crate::k8s::KubeResourceIdentifier`] for a resource.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StoreError {
    #[error("{kind} has no metadata.name")]
    MissingName { kind: String },
    #[error("resource kind {0} cannot be stored")]
    Unsupported(String),
}

/// Failure to read metrics from the cluster.
#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("Kubernetes API error: {}", describe(.0))]
    Api(#[from] kube::Error),
    #[error("could not parse quantity {raw:?} for {field}")]
    Quantity { field: &'static str, raw: String },
}

/// Failure to read one object ([`crate::k8s::objects::get_object`]).
#[derive(Debug, Error)]
pub enum ObjectError {
    #[error("not a group/version: {0}")]
    BadApiVersion(String),
    #[error("API discovery failed: {}", describe(.0))]
    Discovery(#[source] kube::Error),
    #[error("the server has no {kind} in {api_version}")]
    UnknownKind { api_version: String, kind: String },
    #[error("not found")]
    NotFound,
    #[error("not allowed: {0}")]
    Forbidden(String),
    #[error("request failed: {}", describe(.0))]
    Request(#[source] kube::Error),
    #[error("unexpected response: {0}")]
    Decode(#[source] serde_json::Error),
    /// A write's `resourceVersion` precondition failed, or it already exists.
    #[error("conflict: {0}")]
    Conflict(String),
    /// The API server or an admission webhook rejected a write.
    #[error("rejected: {0}")]
    Rejected(String),
}

impl ObjectError {
    /// Maps an API error to the variant callers branch on.
    pub fn from_api(e: kube::Error) -> Self {
        match &e {
            kube::Error::Api(s) if s.code == 404 => ObjectError::NotFound,
            kube::Error::Api(s) if s.code == 403 => ObjectError::Forbidden(s.message.clone()),
            kube::Error::Api(s) if s.code == 409 => ObjectError::Conflict(s.message.clone()),
            kube::Error::Api(s) if s.code == 400 || s.code == 422 => {
                ObjectError::Rejected(s.message.clone())
            }
            _ => ObjectError::Request(e),
        }
    }
}

/// Failure to read container logs ([`crate::k8s::logs::open_logs`]).
#[derive(Debug, Error)]
pub enum LogError {
    #[error("there is no previous container (it hasn't restarted)")]
    NoPrevious,
    #[error("not found")]
    NotFound,
    #[error("not allowed: {0}")]
    Forbidden(String),
    #[error("request failed: {}", describe(.0))]
    Request(#[source] kube::Error),
    #[error("stream broke: {0}")]
    Io(#[source] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Command, Output};

    fn failed_plugin() -> kube::Error {
        // A command whose Debug output carries a secret in its environment,
        // the way kube builds the real one.
        let mut cmd = Command::new("aws");
        cmd.arg("eks")
            .env("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG")
            .env("PATH", "/opt/bin");
        let status = Command::new("sh").arg("-c").arg("exit 3").status().unwrap();
        kube::Error::Auth(kube::client::AuthError::AuthExecRun {
            cmd: format!("{cmd:?}"),
            status,
            out: Output {
                status,
                stdout: b"token=eyJhbGciOi".to_vec(),
                stderr: b"warning: profile x\nUnable to locate credentials\n\n".to_vec(),
            },
        })
    }

    #[test]
    fn kube_display_leaks_and_describe_does_not() {
        let e = failed_plugin();
        let raw = e.to_string();
        assert!(
            raw.contains("wJalrXUtnFEMI"),
            "premise: kube's own Display leaks env: {raw}"
        );

        let safe = describe(&e);
        for leaked in [
            "wJalrXUtnFEMI",
            "AWS_SECRET_ACCESS_KEY",
            "/opt/bin",
            "eyJhbGciOi",
            "profile x",
        ] {
            assert!(!safe.contains(leaked), "{leaked} leaked: {safe}");
        }
        assert!(safe.contains("Unable to locate credentials"), "{safe}");
        assert!(safe.contains("exit status: 3"), "{safe}");

        let w = kube::runtime::watcher::Error::WatchFailed(failed_plugin());
        assert!(!describe_watch(&w).contains("wJalrXUtnFEMI"));
        assert!(!WatchError::InitialSync(w)
            .to_string()
            .contains("wJalrXUtnFEMI"));
        assert!(!ObjectError::Request(failed_plugin())
            .to_string()
            .contains("wJalrXUtnFEMI"));
        assert!(!MetricsError::Api(failed_plugin())
            .to_string()
            .contains("wJalrXUtnFEMI"));
    }

    #[test]
    fn describe_passes_other_errors_through() {
        let e = kube::Error::Api(
            kube::core::Status::failure("nope", "Forbidden")
                .with_code(403)
                .boxed(),
        );
        assert_eq!(describe(&e), e.to_string());
        let w = kube::runtime::watcher::Error::NoResourceVersion;
        assert_eq!(describe_watch(&w), w.to_string());
    }
}
