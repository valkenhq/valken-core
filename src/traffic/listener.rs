//! One log listener per ingress controller pod.
//!
//! A listener follows a pod's container log through a [`LogSource`], hands
//! every line to its [`LineProcessor`], and about once a second forwards
//! whatever the processor has finished to the [`Sink`].  What a "line"
//! means and what the processor produces is the application's business;
//! this crate only owns the following, reconnecting and supervision.
//!
//! Listeners are isolated tokio tasks.  A dropped stream is reconnected with
//! exponential backoff (1 s → 30 s); a panic inside one task is contained by
//! the runtime and reported by the supervisor wrapper in
//! [`spawn_listener_with`].

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::traffic::log_source::LogSource;
use crate::traffic::manager::PodTarget;
use crate::traffic::telemetry::StreamError;

/// Receives what a [`LineProcessor`] has finished (closed rollups, …).
/// The application typically wraps them into a DB event.
pub type Sink<T> = Arc<dyn Fn(Vec<T>) + Send + Sync>;

/// Shared, read-mostly list of known namespaces, maintained by the manager
/// from the namespace watch for processors that need to split dashed names.
pub type NamespaceHints = Arc<RwLock<Vec<String>>>;

/// Shared pod IP → pod name map, kept current by the manager from the pod
/// watch so a processor can resolve an upstream address to the pod that
/// *was* at that IP when the line was written (IPs are reused after
/// restarts, so resolving later would misattribute traffic).
pub type PodHints = Arc<RwLock<std::collections::HashMap<String, String>>>;

/// The per-line logic of a listener: line in, finished items out.
///
/// Implementations own their own state (parsers, open rollups).  They run
/// on the listener's task and must never block.
pub trait LineProcessor: Send + 'static {
    type Output: Send + 'static;

    /// One raw line as delivered by the [`LogSource`] (with the kubelet
    /// timestamp prefix when the source adds one).
    fn ingest(&mut self, raw: &str);

    /// Items that are complete as of `now` (unix seconds).  Called about
    /// once a second.
    fn flush_closed(&mut self, now: i64) -> Vec<Self::Output>;

    /// Everything still open, regardless of age (stream drop, stop).
    fn flush_all(&mut self) -> Vec<Self::Output>;

    /// Number of items still open, for diagnostics.
    fn open_count(&self) -> usize {
        0
    }
}

const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const READ_TIMEOUT: Duration = Duration::from_secs(1);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const STATS_LOG_INTERVAL: Duration = Duration::from_secs(60);

// ─── Stats ────────────────────────────────────────────────────────────────────

/// Telemetry counters for one listener; shared with the handle so the
/// manager (and tests) can inspect them.
///
/// `lines`, `reconnects`, `flushed` and the stream flags are maintained by
/// the listener; `requests`, `ignored` and `malformed` are for the
/// [`LineProcessor`] to bump as it classifies lines.
#[derive(Debug, Default)]
pub struct ListenerStats {
    pub lines: AtomicU64,
    pub requests: AtomicU64,
    pub ignored: AtomicU64,
    pub malformed: AtomicU64,
    pub reconnects: AtomicU64,
    /// Items handed to the sink so far.
    pub flushed_buckets: AtomicU64,
    /// `true` while `pods/log` is open and being read.
    pub stream_open: AtomicBool,
    /// Unix seconds the stream first opened (0 = never).  Kept across
    /// reconnects so "how long have we heard nothing" keeps counting.
    pub first_open_at: AtomicI64,
    /// Why the last stream open failed (cleared once a stream opens) — the
    /// lens turns a 403 here into its "can't read logs" state.
    pub last_error: Mutex<Option<StreamError>>,
}

impl ListenerStats {
    /// Copy of the last stream error (a poisoned lock still holds a value).
    pub fn last_error(&self) -> Option<StreamError> {
        self.last_error
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    fn set_last_error(&self, err: Option<StreamError>) {
        *self.last_error.lock().unwrap_or_else(|p| p.into_inner()) = err;
    }

    fn snapshot(&self) -> (u64, u64, u64, u64, u64, u64) {
        (
            self.lines.load(Ordering::Relaxed),
            self.requests.load(Ordering::Relaxed),
            self.ignored.load(Ordering::Relaxed),
            self.malformed.load(Ordering::Relaxed),
            self.reconnects.load(Ordering::Relaxed),
            self.flushed_buckets.load(Ordering::Relaxed),
        )
    }
}

pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ─── Handle ───────────────────────────────────────────────────────────────────

/// Control handle for one running listener task.
pub struct ListenerHandle {
    pub target: PodTarget,
    stop: CancellationToken,
    pub stats: Arc<ListenerStats>,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl ListenerHandle {
    /// Ask the task to stop.  It flushes its open items and exits promptly
    /// (the next poll of its log stream).
    pub fn stop(&self) {
        self.stop.cancel();
    }

    pub fn is_stopped(&self) -> bool {
        self.stop.is_cancelled()
    }

    /// Take the join handle (used at shutdown to wait for the final flush).
    pub fn take_join(&mut self) -> Option<tokio::task::JoinHandle<()>> {
        self.join.take()
    }
}

// ─── Spawn ────────────────────────────────────────────────────────────────────

/// Everything a listener needs besides its target and processor.
#[derive(Clone)]
pub struct ListenerDeps<T> {
    pub cluster_id: u32,
    pub log_source: Arc<dyn LogSource>,
    pub sink: Sink<T>,
}

/// Spawn a listener for `target` on `runtime`.
///
/// `stats` is shared with the processor (built by the caller against the
/// same `Arc`) so it can report what it classified.
pub fn spawn_listener_with<T, P>(
    runtime: &tokio::runtime::Handle,
    target: PodTarget,
    deps: ListenerDeps<T>,
    stats: Arc<ListenerStats>,
    processor: P,
) -> ListenerHandle
where
    T: Send + 'static,
    P: LineProcessor<Output = T>,
{
    let stop = CancellationToken::new();
    let label = target.label();

    let inner = run_listener(target.clone(), processor, deps, stop.clone(), stats.clone());

    // Supervisor: the inner task's panic (if any) is contained by tokio and
    // surfaces here as a JoinError, leaving every other listener untouched.
    let inner_join = runtime.spawn(inner);
    let join = runtime.spawn(async move {
        if let Err(e) = inner_join.await {
            if e.is_panic() {
                error!(listener = %label, "Traffic listener panicked; other listeners unaffected");
            }
        }
    });

    ListenerHandle {
        target,
        stop,
        stats,
        join: Some(join),
    }
}

async fn run_listener<T: Send + 'static>(
    target: PodTarget,
    mut processor: impl LineProcessor<Output = T>,
    deps: ListenerDeps<T>,
    stop: CancellationToken,
    stats: Arc<ListenerStats>,
) {
    let label = target.label();
    info!(listener = %label, cluster_id = deps.cluster_id, container = %target.container, kind = target.kind.display_name(), "Traffic listener starting");

    let sink = deps.sink;
    let flush = |rows: Vec<T>| {
        if !rows.is_empty() {
            stats
                .flushed_buckets
                .fetch_add(rows.len() as u64, Ordering::Relaxed);
            sink(rows);
        }
    };

    let mut backoff = BACKOFF_MIN;
    let mut last_stats_log = Instant::now();
    let mut first_attempt = true;

    'outer: while !stop.is_cancelled() {
        if !first_attempt {
            stats.reconnects.fetch_add(1, Ordering::Relaxed);
        }
        first_attempt = false;

        // A stop while the request is in flight must not wait for the
        // server (the client's read timeout can be minutes).
        let opened = tokio::select! {
            _ = stop.cancelled() => break 'outer,
            opened = deps.log_source.open(&target) => opened,
        };
        let mut lines = match opened {
            Ok(lines) => lines,
            Err(e) => {
                warn!(listener = %label, error = %e.message, "Could not open log stream; retrying in {:?}", backoff);
                stats.set_last_error(Some(e));
                if sleep_unless_stopped(backoff, &stop).await {
                    break;
                }
                backoff = next_backoff(backoff);
                continue;
            }
        };
        info!(listener = %label, "Log stream open");
        stats.set_last_error(None);
        stats.stream_open.store(true, Ordering::Relaxed);
        let _ = stats.first_open_at.compare_exchange(
            0,
            now_unix(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );

        let mut last_flush = Instant::now();

        loop {
            let next = tokio::select! {
                _ = stop.cancelled() => break 'outer,
                next = tokio::time::timeout(READ_TIMEOUT, lines.try_next()) => next,
            };
            match next {
                Err(_elapsed) => {}
                Ok(Ok(Some(line))) => {
                    backoff = BACKOFF_MIN;
                    stats.lines.fetch_add(1, Ordering::Relaxed);
                    processor.ingest(&line);
                }
                Ok(Ok(None)) => {
                    info!(listener = %label, "Log stream ended; reconnecting in {:?}", backoff);
                    break;
                }
                Ok(Err(e)) => {
                    warn!(listener = %label, error = %e, "Log stream error; reconnecting in {:?}", backoff);
                    break;
                }
            }

            if last_flush.elapsed() >= FLUSH_INTERVAL {
                last_flush = Instant::now();
                flush(processor.flush_closed(now_unix()));
            }

            if last_stats_log.elapsed() >= STATS_LOG_INTERVAL {
                last_stats_log = Instant::now();
                let (lines_n, req, ign, mal, rec, fl) = stats.snapshot();
                debug!(listener = %label, lines = lines_n, requests = req, ignored = ign, malformed = mal, reconnects = rec, flushed = fl, open = processor.open_count(), "Traffic listener stats");
            }
        }

        // Stream dropped: push whatever we have (the writer merges on re-touch).
        stats.stream_open.store(false, Ordering::Relaxed);
        flush(processor.flush_all());
        if sleep_unless_stopped(backoff, &stop).await {
            break;
        }
        backoff = next_backoff(backoff);
    }

    stats.stream_open.store(false, Ordering::Relaxed);
    flush(processor.flush_all());
    let (lines_n, req, ign, mal, rec, fl) = stats.snapshot();
    info!(listener = %label, lines = lines_n, requests = req, ignored = ign, malformed = mal, reconnects = rec, flushed = fl, "Traffic listener stopped");
}

fn next_backoff(current: Duration) -> Duration {
    (current * 2).min(BACKOFF_MAX)
}

/// Sleep for `dur`; returns `true` if `stop` was cancelled first.
async fn sleep_unless_stopped(dur: Duration, stop: &CancellationToken) -> bool {
    tokio::select! {
        _ = stop.cancelled() => true,
        _ = tokio::time::sleep(dur) => false,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_caps() {
        let mut b = BACKOFF_MIN;
        let mut seen = vec![b];
        for _ in 0..8 {
            b = next_backoff(b);
            seen.push(b);
        }
        assert_eq!(seen[1], Duration::from_secs(2));
        assert_eq!(seen[2], Duration::from_secs(4));
        assert_eq!(*seen.last().unwrap(), BACKOFF_MAX);
        assert!(seen.iter().all(|d| *d <= BACKOFF_MAX));
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_unless_stopped_returns_early_on_stop() {
        let stop = CancellationToken::new();
        let s2 = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            s2.cancel();
        });
        let started = tokio::time::Instant::now();
        assert!(sleep_unless_stopped(Duration::from_secs(10), &stop).await);
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(!sleep_unless_stopped(Duration::from_millis(1), &CancellationToken::new()).await);
    }
}
