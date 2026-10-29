//! Kubeconfig discovery for the context picker.
//!
//! * [`KubeconfigSources`] decides which files to read: `$KUBECONFIG`
//!   (platform path-list separator) or `~/.kube/config`, then any extra files
//!   the user added.
//! * [`discover`] reads every file, merges them with kubectl's rules (the
//!   first file to define a name wins, `current-context` comes from the first
//!   file that sets it) and lists every context with its origin file.
//!   Contexts hidden by an earlier file are kept, marked
//!   [`ContextEntry::shadowed`], so the UI can grey them out.
//! * [`Discovery::client_config`] builds a [`kube::Config`] for one context,
//!   with the exec-plugin environment from [`ExecEnv`] applied.
//!
//! Nothing here runs an exec plugin; see [`probe`] for the API checks.

mod exec_env;
pub mod probe;
mod watch;

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use kube::config::{AuthInfo, KubeConfigOptions, Kubeconfig, KubeconfigError};
use kube::Config;

pub use exec_env::{capture_login_shell_path, ExecEnv, PluginNotFound};
pub use watch::KubeconfigWatcher;

/// Why a file is in the list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    /// Listed in `$KUBECONFIG`.
    Env,
    /// `~/.kube/config` (only when `$KUBECONFIG` is unset or empty).
    Default,
    /// Added by the user in Valken.
    Extra,
}

/// The ordered list of kubeconfig files to read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KubeconfigSources {
    pub files: Vec<(PathBuf, SourceKind)>,
}

impl KubeconfigSources {
    /// Files from the process environment plus `extra`, in merge order.
    pub fn from_env(extra: &[PathBuf]) -> Self {
        Self::resolve(
            std::env::var_os("KUBECONFIG").as_deref(),
            home_dir().as_deref(),
            extra,
        )
    }

    /// [`Self::from_env`] with the inputs passed in (tests).  `$KUBECONFIG`
    /// is split on `:` (Unix) or `;` (Windows); empty entries are ignored and
    /// a path listed twice is only read once.
    pub fn resolve(
        kubeconfig_env: Option<&std::ffi::OsStr>,
        home: Option<&Path>,
        extra: &[PathBuf],
    ) -> Self {
        let mut files = Vec::new();
        let mut seen = HashSet::new();
        let mut push = |path: PathBuf, kind: SourceKind| {
            if !path.as_os_str().is_empty() && seen.insert(path.clone()) {
                files.push((path, kind));
            }
        };

        let env_paths: Vec<PathBuf> = kubeconfig_env
            .map(|v| std::env::split_paths(v).collect())
            .unwrap_or_default();
        let env_paths: Vec<PathBuf> = env_paths
            .into_iter()
            .filter(|p| !p.as_os_str().is_empty())
            .collect();

        if env_paths.is_empty() {
            if let Some(home) = home {
                push(home.join(".kube").join("config"), SourceKind::Default);
            }
        } else {
            for p in env_paths {
                push(p, SourceKind::Env);
            }
        }
        for p in extra {
            push(p.clone(), SourceKind::Extra);
        }
        Self { files }
    }

    /// Whether `$KUBECONFIG` contributed any file.
    pub fn uses_env(&self) -> bool {
        self.files.iter().any(|(_, k)| *k == SourceKind::Env)
    }

    pub fn paths(&self) -> impl Iterator<Item = &Path> {
        self.files.iter().map(|(p, _)| p.as_path())
    }
}

fn home_dir() -> Option<PathBuf> {
    #[cfg(windows)]
    let var = "USERPROFILE";
    #[cfg(not(windows))]
    let var = "HOME";
    std::env::var_os(var)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// What happened when a file was read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileStatus {
    Loaded {
        contexts: usize,
    },
    /// The file does not exist (kubectl skips these too).
    Missing,
    /// The file exists but could not be read or parsed.  It is left out of
    /// the merge.
    Invalid(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KubeconfigFile {
    pub path: PathBuf,
    pub source: SourceKind,
    pub status: FileStatus,
}

/// How a context authenticates, for display and for the "never auto-probe
/// exec contexts" rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthKind {
    /// An exec credential plugin (aws, gke-gcloud-auth-plugin, kubelogin, …).
    Exec {
        command: String,
    },
    /// The legacy `auth-provider` block (gcp, azure, oidc).
    AuthProvider {
        name: String,
    },
    Token,
    ClientCertificate,
    Basic,
    /// No user, or a user with no credentials.
    None,
}

impl AuthKind {
    fn of(auth: Option<&AuthInfo>) -> Self {
        let Some(a) = auth else {
            return AuthKind::None;
        };
        if let Some(exec) = &a.exec {
            return AuthKind::Exec {
                command: exec.command.clone().unwrap_or_default(),
            };
        }
        if let Some(p) = &a.auth_provider {
            return AuthKind::AuthProvider {
                name: p.name.clone(),
            };
        }
        if a.token.is_some() || a.token_file.is_some() {
            return AuthKind::Token;
        }
        if a.client_certificate.is_some() || a.client_certificate_data.is_some() {
            return AuthKind::ClientCertificate;
        }
        if a.username.is_some() {
            return AuthKind::Basic;
        }
        AuthKind::None
    }

    /// Running the plugin can open a browser or prompt for MFA, so these are
    /// only probed when the user asks.
    pub fn is_exec(&self) -> bool {
        matches!(self, AuthKind::Exec { .. })
    }

    /// The plugin's command as written in the kubeconfig, without its
    /// directory (`/usr/local/bin/aws` → `aws`), for the list tag.
    pub fn exec_command_name(&self) -> Option<&str> {
        match self {
            AuthKind::Exec { command } => Some(
                Path::new(command)
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or(command),
            ),
            _ => None,
        }
    }
}

/// One context from one file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextEntry {
    pub name: String,
    /// The file that defines this entry.
    pub origin: PathBuf,
    /// Name of the cluster entry the context refers to.
    pub cluster: Option<String>,
    /// API server URL.
    pub server: Option<String>,
    /// Name of the user entry the context refers to.
    pub user: Option<String>,
    pub namespace: Option<String>,
    pub auth: AuthKind,
    /// An earlier file defines a context with the same name, so kubectl (and
    /// Valken) never use this one.
    pub shadowed: bool,
    /// This is the merged `current-context`.
    pub is_current: bool,
}

impl ContextEntry {
    /// Key used for recents and the probe cache.
    pub fn key(&self) -> ContextKey {
        ContextKey {
            origin: self.origin.clone(),
            name: self.name.clone(),
            server: self.server.clone().unwrap_or_default(),
        }
    }

    /// Whether the context or cluster name looks like production.
    pub fn looks_like_prod(&self) -> bool {
        looks_like_prod(&self.name) || self.cluster.as_deref().is_some_and(looks_like_prod)
    }
}

/// Identity of a context across launches: the same name in another file, or
/// pointing at another server, is a different context.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContextKey {
    pub origin: PathBuf,
    pub name: String,
    pub server: String,
}

/// `(^|[-_.])(prod|prd|production|live)([-_.]|$)`, case-insensitive: one of
/// the words appears as a whole `-`, `_` or `.`-separated part of the name.
pub fn looks_like_prod(name: &str) -> bool {
    name.split(['-', '_', '.']).any(|part| {
        matches!(
            part.to_ascii_lowercase().as_str(),
            "prod" | "prd" | "production" | "live"
        )
    })
}

/// The result of reading every kubeconfig file.
#[derive(Clone, Default)]
pub struct Discovery {
    pub files: Vec<KubeconfigFile>,
    /// Every context in file order, shadowed duplicates included.
    pub contexts: Vec<ContextEntry>,
    pub current_context: Option<String>,
    merged: Kubeconfig,
}

// `merged` holds credentials; keep it out of debug output.
impl fmt::Debug for Discovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Discovery")
            .field("files", &self.files)
            .field("contexts", &self.contexts)
            .field("current_context", &self.current_context)
            .finish_non_exhaustive()
    }
}

/// Read and merge every file in `sources`.  Never fails as a whole: a
/// missing or broken file is reported in [`Discovery::files`] and skipped.
pub fn discover(sources: &KubeconfigSources) -> Discovery {
    let mut files = Vec::new();
    let mut loaded: Vec<(PathBuf, Kubeconfig)> = Vec::new();

    for (path, source) in &sources.files {
        let status = if !path.exists() {
            FileStatus::Missing
        } else {
            match Kubeconfig::read_from(path) {
                Ok(kc) => {
                    let contexts = kc.contexts.len();
                    loaded.push((path.clone(), kc));
                    FileStatus::Loaded { contexts }
                }
                Err(e) => FileStatus::Invalid(e.to_string()),
            }
        };
        files.push(KubeconfigFile {
            path: path.clone(),
            source: *source,
            status,
        });
    }

    let mut merged = Kubeconfig::default();
    for (path, kc) in &loaded {
        merged = match merged.clone().merge(kc.clone()) {
            Ok(m) => m,
            Err(e) => {
                // kind / apiVersion mismatch: treat like a parse error.
                if let Some(f) = files.iter_mut().find(|f| &f.path == path) {
                    f.status = FileStatus::Invalid(e.to_string());
                }
                merged
            }
        };
    }

    let current_context = merged.current_context.clone().filter(|c| !c.is_empty());
    let mut seen = HashSet::new();
    let mut contexts = Vec::new();
    for (path, kc) in &loaded {
        if !files
            .iter()
            .any(|f| &f.path == path && matches!(f.status, FileStatus::Loaded { .. }))
        {
            continue;
        }
        for named in &kc.contexts {
            let shadowed = !seen.insert(named.name.clone());
            // kubectl resolves cluster and user names against the merged
            // config; a shadowed entry is never used, so show what its own
            // file says.
            let lookup = if shadowed { kc } else { &merged };
            let ctx = named.context.as_ref();
            let cluster_name = ctx.map(|c| c.cluster.clone()).filter(|c| !c.is_empty());
            let user_name = ctx.and_then(|c| c.user.clone()).filter(|u| !u.is_empty());
            let server = cluster_name.as_ref().and_then(|cn| {
                lookup
                    .clusters
                    .iter()
                    .find(|c| &c.name == cn)
                    .and_then(|c| c.cluster.as_ref())
                    .and_then(|c| c.server.clone())
            });
            let auth = user_name.as_ref().and_then(|un| {
                lookup
                    .auth_infos
                    .iter()
                    .find(|a| &a.name == un)
                    .and_then(|a| a.auth_info.as_ref())
            });
            contexts.push(ContextEntry {
                name: named.name.clone(),
                origin: path.clone(),
                cluster: cluster_name,
                server,
                user: user_name,
                namespace: ctx.and_then(|c| c.namespace.clone()),
                auth: AuthKind::of(auth),
                shadowed,
                is_current: !shadowed && current_context.as_deref() == Some(named.name.as_str()),
            });
        }
    }

    Discovery {
        files,
        contexts,
        current_context,
        merged,
    }
}

impl Discovery {
    /// The usable (non-shadowed) entry for a context name.
    pub fn context(&self, name: &str) -> Option<&ContextEntry> {
        self.contexts.iter().find(|c| c.name == name && !c.shadowed)
    }

    /// The entry matching a stored key, if it is still in the list and not
    /// shadowed.
    pub fn find(&self, key: &ContextKey) -> Option<&ContextEntry> {
        self.contexts
            .iter()
            .find(|c| !c.shadowed && &c.key() == key)
    }

    pub fn usable_contexts(&self) -> impl Iterator<Item = &ContextEntry> {
        self.contexts.iter().filter(|c| !c.shadowed)
    }

    /// Whether no file had any context (the "No kubeconfig found" state).
    pub fn is_empty(&self) -> bool {
        self.contexts.is_empty()
    }

    /// A client config for `context`, with exec plugins pointed at
    /// [`ExecEnv`]'s PATH and binary overrides, and both connect and read
    /// timeouts set to `timeout`.  Does not contact the cluster or run any
    /// plugin.
    pub async fn client_config(
        &self,
        context: &str,
        exec: &ExecEnv,
        timeout: Duration,
    ) -> Result<Config, KubeconfigError> {
        let mut kc = self.merged.clone();
        exec.apply(&mut kc);
        let options = KubeConfigOptions {
            context: Some(context.to_string()),
            cluster: None,
            user: None,
        };
        let mut config = Config::from_custom_kubeconfig(kc, &options).await?;
        config.connect_timeout = Some(timeout);
        config.read_timeout = Some(timeout);
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write(dir: &Path, name: &str, body: &str) -> PathBuf {
        let p = dir.join(name);
        fs::write(&p, body).unwrap();
        p
    }

    const A: &str = r#"
apiVersion: v1
kind: Config
current-context: shared
clusters:
- name: c-a
  cluster: { server: "https://a.example:6443" }
- name: c-shared
  cluster: { server: "https://first.example:6443" }
users:
- name: u-token
  user: { token: abc }
- name: u-cert
  user: { client-certificate: cert.pem, client-key: key.pem }
contexts:
- name: shared
  context: { cluster: c-shared, user: u-token, namespace: team }
- name: alpha-prod
  context: { cluster: c-a, user: u-cert }
"#;

    const B: &str = r#"
apiVersion: v1
kind: Config
current-context: beta
clusters:
- name: c-shared
  cluster: { server: "https://second.example:6443" }
- name: c-b
  cluster: { server: "https://b.example:6443" }
users:
- name: u-exec
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      command: /usr/local/bin/aws
      args: [eks, get-token]
contexts:
- name: shared
  context: { cluster: c-b, user: u-exec }
- name: beta
  context: { cluster: c-shared, user: u-exec }
"#;

    #[test]
    fn resolve_prefers_env_over_default_and_appends_extras() {
        let home = Path::new("/home/me");
        let sep = if cfg!(windows) { ";" } else { ":" };
        let env = format!("/a{sep}{sep}/b{sep}/a");
        let s = KubeconfigSources::resolve(
            Some(std::ffi::OsStr::new(&env)),
            Some(home),
            &[PathBuf::from("/x"), PathBuf::from("/b")],
        );
        assert_eq!(
            s.files,
            vec![
                (PathBuf::from("/a"), SourceKind::Env),
                (PathBuf::from("/b"), SourceKind::Env),
                (PathBuf::from("/x"), SourceKind::Extra),
            ]
        );
        assert!(s.uses_env());

        let s = KubeconfigSources::resolve(Some(std::ffi::OsStr::new("")), Some(home), &[]);
        assert_eq!(
            s.files,
            vec![(home.join(".kube").join("config"), SourceKind::Default)]
        );
    }

    #[test]
    fn merges_with_kubectl_precedence_and_keeps_shadowed() {
        let dir = tempfile::tempdir().unwrap();
        let a = write(dir.path(), "a.yaml", A);
        let b = write(dir.path(), "b.yaml", B);
        let missing = dir.path().join("nope.yaml");
        let sources = KubeconfigSources {
            files: vec![
                (a.clone(), SourceKind::Env),
                (b.clone(), SourceKind::Env),
                (missing.clone(), SourceKind::Extra),
            ],
        };
        let d = discover(&sources);

        assert_eq!(d.files[0].status, FileStatus::Loaded { contexts: 2 });
        assert_eq!(d.files[1].status, FileStatus::Loaded { contexts: 2 });
        assert_eq!(d.files[2].status, FileStatus::Missing);
        assert_eq!(d.current_context.as_deref(), Some("shared"));

        let names: Vec<_> = d
            .contexts
            .iter()
            .map(|c| (c.name.as_str(), c.origin == a, c.shadowed))
            .collect();
        assert_eq!(
            names,
            vec![
                ("shared", true, false),
                ("alpha-prod", true, false),
                ("shared", false, true),
                ("beta", false, false),
            ]
        );

        let shared = d.context("shared").unwrap();
        assert!(shared.is_current);
        assert_eq!(shared.server.as_deref(), Some("https://first.example:6443"));
        assert_eq!(shared.namespace.as_deref(), Some("team"));
        assert_eq!(shared.auth, AuthKind::Token);

        // beta's cluster name is defined in both files; the first wins.
        let beta = d.context("beta").unwrap();
        assert_eq!(beta.server.as_deref(), Some("https://first.example:6443"));
        assert_eq!(beta.auth.exec_command_name(), Some("aws"));
        assert!(beta.auth.is_exec());

        // The shadowed copy is described from its own file.
        let shadow = d.contexts.iter().find(|c| c.shadowed).unwrap();
        assert_eq!(shadow.server.as_deref(), Some("https://b.example:6443"));
        assert!(!shadow.is_current);

        assert_eq!(
            d.context("alpha-prod").unwrap().auth,
            AuthKind::ClientCertificate
        );
        assert!(d.context("alpha-prod").unwrap().looks_like_prod());
    }

    #[test]
    fn invalid_file_is_reported_and_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let bad = write(dir.path(), "bad.yaml", "contexts: [ {");
        let a = write(dir.path(), "a.yaml", A);
        let d = discover(&KubeconfigSources {
            files: vec![(bad, SourceKind::Env), (a, SourceKind::Env)],
        });
        assert!(matches!(d.files[0].status, FileStatus::Invalid(_)));
        assert_eq!(d.usable_contexts().count(), 2);
    }

    #[test]
    fn prod_names() {
        for yes in [
            "prod",
            "cdo-prd-eucentral-01",
            "eu.production",
            "shop_live",
            "PROD-1",
        ] {
            assert!(looks_like_prod(yes), "{yes}");
        }
        for no in ["product-dev", "kind-kind", "liveness", "preprod", "staging"] {
            assert!(!looks_like_prod(no), "{no}");
        }
    }

    #[tokio::test]
    async fn client_config_targets_the_named_context() {
        let dir = tempfile::tempdir().unwrap();
        let a = write(dir.path(), "a.yaml", A);
        let b = write(dir.path(), "b.yaml", B);
        let d = discover(&KubeconfigSources {
            files: vec![(a, SourceKind::Env), (b, SourceKind::Env)],
        });
        let cfg = d
            .client_config("shared", &ExecEnv::default(), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(cfg.cluster_url.to_string(), "https://first.example:6443/");
        assert_eq!(cfg.default_namespace, "team");
        assert_eq!(cfg.connect_timeout, Some(Duration::from_secs(5)));
        assert!(d
            .client_config("missing", &ExecEnv::default(), Duration::from_secs(5))
            .await
            .is_err());
    }
}
