//! The environment exec credential plugins run in.
//!
//! Apps started from the macOS dock (and many Linux launchers) don't get the
//! user's shell `PATH`, so `aws` or `kubelogin` isn't found.
//! [`capture_login_shell_path`] asks the login shell for its `PATH` once at
//! startup, and [`ExecEnv`] adds it (plus per-command "Locate binary…"
//! overrides) to every exec block before a client is built.

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::time::Duration;

use kube::config::Kubeconfig;
use thiserror::Error;

/// PATH and binary overrides applied to exec plugins.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecEnv {
    /// `PATH` from the user's login shell, if it could be captured.
    pub login_path: Option<OsString>,
    /// Command as written in the kubeconfig (e.g. `aws`) → binary to run.
    pub overrides: HashMap<String, PathBuf>,
}

/// The plugin binary could not be found.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("exec plugin {command:?} not found")]
pub struct PluginNotFound {
    pub command: String,
    /// The directories that were searched, in order.
    pub searched: Vec<PathBuf>,
}

impl ExecEnv {
    /// The process `PATH` followed by any login-shell entries it lacks.
    pub fn search_path(&self) -> OsString {
        self.search_path_with(std::env::var_os("PATH").as_deref())
    }

    fn search_path_with(&self, process: Option<&OsStr>) -> OsString {
        let mut dirs: Vec<PathBuf> = process
            .map(|p| std::env::split_paths(p).collect())
            .unwrap_or_default();
        if let Some(login) = &self.login_path {
            for d in std::env::split_paths(login) {
                if !dirs.contains(&d) {
                    dirs.push(d);
                }
            }
        }
        dirs.retain(|d| !d.as_os_str().is_empty());
        std::env::join_paths(dirs).unwrap_or_default()
    }

    /// The binary that will run for `command`, without running it.
    pub fn resolve(&self, command: &str) -> Result<PathBuf, PluginNotFound> {
        self.resolve_in(command, &self.search_path())
    }

    fn resolve_in(&self, command: &str, path: &OsStr) -> Result<PathBuf, PluginNotFound> {
        if let Some(p) = self.overrides.get(command) {
            return if p.is_file() {
                Ok(p.clone())
            } else {
                Err(PluginNotFound {
                    command: command.to_string(),
                    searched: vec![p.clone()],
                })
            };
        }
        let as_path = Path::new(command);
        if as_path.components().count() > 1 {
            return if as_path.is_file() {
                Ok(as_path.to_path_buf())
            } else {
                Err(PluginNotFound {
                    command: command.to_string(),
                    searched: vec![as_path.to_path_buf()],
                })
            };
        }
        let searched: Vec<PathBuf> = std::env::split_paths(path).collect();
        for dir in &searched {
            for name in candidate_names(command) {
                let p = dir.join(&name);
                if p.is_file() {
                    return Ok(p);
                }
            }
        }
        Err(PluginNotFound {
            command: command.to_string(),
            searched,
        })
    }

    /// Point every exec block in `kc` at the override binary (if any) and
    /// give it the combined `PATH`, unless the kubeconfig sets `PATH` itself.
    pub fn apply(&self, kc: &mut Kubeconfig) {
        let path = self.search_path().to_string_lossy().into_owned();
        for named in kc.auth_infos.iter_mut() {
            let Some(exec) = named.auth_info.as_mut().and_then(|a| a.exec.as_mut()) else {
                continue;
            };
            if let Some(cmd) = &exec.command {
                if let Some(p) = self.overrides.get(cmd) {
                    exec.command = Some(p.to_string_lossy().into_owned());
                }
            }
            let env = exec.env.get_or_insert_with(Vec::new);
            let sets_path = env
                .iter()
                .any(|e| e.get("name").is_some_and(|n| n == "PATH"));
            if !sets_path && !path.is_empty() {
                env.push(HashMap::from([
                    ("name".to_string(), "PATH".to_string()),
                    ("value".to_string(), path.clone()),
                ]));
            }
        }
    }
}

#[cfg(windows)]
fn candidate_names(command: &str) -> Vec<String> {
    if Path::new(command).extension().is_some() {
        return vec![command.to_string()];
    }
    let exts = std::env::var("PATHEXT").unwrap_or_else(|_| ".EXE;.CMD;.BAT".into());
    std::iter::once(command.to_string())
        .chain(
            exts.split(';')
                .filter(|e| !e.is_empty())
                .map(|e| format!("{command}{e}")),
        )
        .collect()
}

#[cfg(not(windows))]
fn candidate_names(command: &str) -> Vec<String> {
    vec![command.to_string()]
}

/// Run `$SHELL -l -c 'printf %s "$PATH"'` and return what it prints, or
/// `None` on Windows, without `$SHELL`, on failure, or after `timeout`.
/// Only the text after the last newline is used, so a chatty profile that
/// prints a banner doesn't end up in `PATH`.
pub async fn capture_login_shell_path(timeout: Duration) -> Option<OsString> {
    #[cfg(unix)]
    {
        use std::process::Stdio;
        let shell = std::env::var_os("SHELL").filter(|s| !s.is_empty())?;
        let child = tokio::process::Command::new(shell)
            .args(["-l", "-c", "printf %s \"$PATH\""])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .ok()?;
        let out = tokio::time::timeout(timeout, child.wait_with_output())
            .await
            .ok()?
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let text = String::from_utf8(out.stdout).ok()?;
        let last = text.rsplit('\n').next()?.trim();
        (!last.is_empty()).then(|| OsString::from(last))
    }
    #[cfg(not(unix))]
    {
        let _ = timeout;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_path_appends_missing_login_dirs() {
        let sep = if cfg!(windows) { ";" } else { ":" };
        let env = ExecEnv {
            login_path: Some(format!("/opt/bin{sep}/usr/bin").into()),
            overrides: HashMap::new(),
        };
        let got = env.search_path_with(Some(OsStr::new(&format!("/usr/bin{sep}/bin"))));
        let dirs: Vec<_> = std::env::split_paths(&got).collect();
        assert_eq!(
            dirs,
            vec![
                PathBuf::from("/usr/bin"),
                PathBuf::from("/bin"),
                PathBuf::from("/opt/bin")
            ]
        );
    }

    #[cfg(unix)]
    #[test]
    fn resolve_uses_login_path_and_overrides() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("aws");
        std::fs::write(&bin, "").unwrap();

        let env = ExecEnv::default();
        let err = env
            .resolve_in("aws", OsStr::new("/nonexistent"))
            .unwrap_err();
        assert_eq!(err.searched, vec![PathBuf::from("/nonexistent")]);

        let env = ExecEnv {
            login_path: Some(dir.path().as_os_str().to_owned()),
            ..Default::default()
        };
        assert_eq!(
            env.resolve_in("aws", &env.search_path_with(None)).unwrap(),
            bin
        );

        let env = ExecEnv {
            overrides: HashMap::from([("kubelogin".to_string(), bin.clone())]),
            ..Default::default()
        };
        assert_eq!(env.resolve("kubelogin").unwrap(), bin);
    }

    #[test]
    fn apply_sets_path_and_override() {
        let mut kc = Kubeconfig::from_yaml(
            r#"
users:
- name: a
  user:
    exec: { apiVersion: client.authentication.k8s.io/v1beta1, command: aws }
- name: b
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      command: kubelogin
      env: [ { name: PATH, value: /mine } ]
- name: c
  user: { token: t }
"#,
        )
        .unwrap();
        let env = ExecEnv {
            login_path: Some("/opt/bin".into()),
            overrides: HashMap::from([("aws".to_string(), PathBuf::from("/x/aws"))]),
        };
        env.apply(&mut kc);

        let exec_a = kc.auth_infos[0]
            .auth_info
            .as_ref()
            .unwrap()
            .exec
            .as_ref()
            .unwrap();
        assert_eq!(exec_a.command.as_deref(), Some("/x/aws"));
        let path = exec_a.env.as_ref().unwrap()[0].get("value").unwrap();
        assert!(path.contains("/opt/bin"));

        let exec_b = kc.auth_infos[1]
            .auth_info
            .as_ref()
            .unwrap()
            .exec
            .as_ref()
            .unwrap();
        assert_eq!(
            exec_b.env.as_ref().unwrap().len(),
            1,
            "kubeconfig PATH kept"
        );
        assert!(kc.auth_infos[2].auth_info.as_ref().unwrap().exec.is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn capture_takes_the_last_line() {
        // A fake "shell" that ignores its arguments.
        let dir = tempfile::tempdir().unwrap();
        let sh = dir.path().join("fake-sh");
        std::fs::write(&sh, "#!/bin/sh\necho banner\nprintf %s /from/login\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&sh, std::fs::Permissions::from_mode(0o755)).unwrap();

        // Test-local; no other test reads SHELL.
        std::env::set_var("SHELL", &sh);
        let got = capture_login_shell_path(Duration::from_secs(3)).await;
        assert_eq!(got.as_deref(), Some(OsStr::new("/from/login")));
    }
}
