//! Live updates when a kubeconfig file changes.
//!
//! Tools like `aws eks update-kubeconfig` and most editors replace the file
//! rather than writing it in place, which drops a watch on the file itself.
//! So the *parent directories* are watched (non-recursively) and events are
//! filtered to the kubeconfig paths.  Bursts are debounced into one signal.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tracing::{debug, warn};

/// Watches a set of kubeconfig files; dropping it stops the watch.
pub struct KubeconfigWatcher {
    _watcher: RecommendedWatcher,
}

impl KubeconfigWatcher {
    /// Send `()` on `changed` (at most once per `debounce`) whenever one of
    /// `files` is created, modified, renamed or removed.  Files whose
    /// directory doesn't exist yet are skipped.
    pub fn new(files: &[PathBuf], debounce: Duration, changed: Sender<()>) -> notify::Result<Self> {
        let targets: HashSet<PathBuf> = files.iter().map(|p| normalize(p)).collect();
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded::<()>();

        let filter = targets.clone();
        let mut watcher =
            notify::recommended_watcher(move |res: notify::Result<notify::Event>| match res {
                Ok(event) if event.kind.is_access() => {}
                Ok(event) => {
                    if event.paths.iter().any(|p| filter.contains(&normalize(p))) {
                        let _ = raw_tx.send(());
                    }
                }
                Err(e) => warn!(error = %e, "kubeconfig watch error"),
            })?;

        let dirs: HashSet<PathBuf> = targets
            .iter()
            .filter_map(|p| p.parent().map(Path::to_path_buf))
            .filter(|d| d.is_dir())
            .collect();
        for dir in &dirs {
            if let Err(e) = watcher.watch(dir, RecursiveMode::NonRecursive) {
                warn!(dir = %dir.display(), error = %e, "cannot watch kubeconfig directory");
            }
        }
        debug!(
            files = targets.len(),
            dirs = dirs.len(),
            "watching kubeconfig files"
        );

        thread::Builder::new()
            .name("kubeconfig-watch".into())
            .spawn(move || debounce_loop(raw_rx, changed, debounce))
            .map_err(|e| notify::Error::generic(&e.to_string()))?;

        Ok(Self { _watcher: watcher })
    }
}

/// Forward one signal per burst; ends when the watcher (and with it the raw
/// sender) is dropped, or the consumer goes away.
fn debounce_loop(raw: Receiver<()>, out: Sender<()>, quiet: Duration) {
    while raw.recv().is_ok() {
        loop {
            match raw.recv_timeout(quiet) {
                Ok(()) => continue,
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
        if out.send(()).is_err() {
            return;
        }
    }
}

/// Canonicalize the directory part when it exists, so `~/.kube/../.kube/config`
/// and symlinked dirs match the paths notify reports.
fn normalize(p: &Path) -> PathBuf {
    match (p.parent(), p.file_name()) {
        (Some(dir), Some(name)) => dir
            .canonicalize()
            .map(|d| d.join(name))
            .unwrap_or_else(|_| p.to_path_buf()),
        _ => p.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signals_once_per_burst_for_watched_files_only() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config");
        std::fs::write(&file, "a").unwrap();
        let (tx, rx) = crossbeam_channel::unbounded();
        let _w =
            KubeconfigWatcher::new(std::slice::from_ref(&file), Duration::from_millis(150), tx)
                .unwrap();

        // Unrelated file in the same dir: no signal.
        std::fs::write(dir.path().join("other"), "x").unwrap();
        assert!(rx.recv_timeout(Duration::from_millis(500)).is_err());

        // Atomic replace, the way editors and cloud CLIs write.
        for i in 0..3 {
            let tmp = dir.path().join(format!(".config.tmp{i}"));
            std::fs::write(&tmp, format!("b{i}")).unwrap();
            std::fs::rename(&tmp, &file).unwrap();
        }
        rx.recv_timeout(Duration::from_secs(3))
            .expect("change signalled");
        assert!(
            rx.recv_timeout(Duration::from_millis(400)).is_err(),
            "burst debounced"
        );
    }
}
