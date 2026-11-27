//! API server latency and errors as Valken itself sees them.
//!
//! [`ApiStatsLayer`] sits in the kube client's tower stack and records the
//! time to response headers and the outcome of every request the client
//! makes, except long-lived streams (`watch=true`, `follow=true`), whose
//! "latency" would be minutes.  That makes the number honest and free: it
//! is the latency Valken's own lists, gets and pings experienced, with no
//! extra traffic and nothing read from the control plane.
//!
//! Samples land in log-scale buckets (25 % wide, so a quantile is within
//! about ±12 % of the true value) inside fixed time slots; a ring of slots
//! covers the last hour.  Recording is one short mutex hold per request.

use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::BoxFuture;
use http::{Request, Response, StatusCode, Uri};
use tokio::time::Instant;
use tower_layer::Layer;
use tower_service::Service;

const BUCKETS: usize = 64;
/// Bucket `i >= 1` holds `[RATIO^(i-1), RATIO^i)` milliseconds; bucket 0
/// holds everything under a millisecond.  64 buckets reach ~19 minutes.
const RATIO: f64 = 1.25;

/// Default slot width: one sparkline point.
pub const DEFAULT_SLOT: Duration = Duration::from_secs(30);
/// Default ring length: an hour of 30 s slots.
pub const DEFAULT_SLOTS: usize = 120;
/// The "recent" window that is compared against the rest of the hour.
const RECENT_SLOTS: u64 = 10;

fn bucket_of(latency: Duration) -> usize {
    let ms = latency.as_secs_f64() * 1000.0;
    if ms < 1.0 {
        0
    } else {
        (1 + (ms.ln() / RATIO.ln()).floor() as usize).min(BUCKETS - 1)
    }
}

/// A bucket's representative value: the geometric middle of its bounds.
fn bucket_value(i: usize) -> Duration {
    let ms = if i == 0 {
        0.5
    } else {
        RATIO.powf(i as f64 - 0.5)
    };
    Duration::from_secs_f64(ms / 1000.0)
}

#[derive(Clone)]
struct Slot {
    /// Which slot this is since the stats started; slots of an older lap
    /// of the ring are stale and read as empty.
    id: u64,
    counts: [u32; BUCKETS],
    total: u32,
    errors: u32,
}

impl Slot {
    const EMPTY: Slot = Slot {
        id: u64::MAX,
        counts: [0; BUCKETS],
        total: 0,
        errors: 0,
    };
}

/// A merged view over some slots.
#[derive(Default)]
struct Merged {
    counts: Vec<u64>,
    total: u64,
    errors: u64,
}

impl Merged {
    fn add(&mut self, s: &Slot) {
        if self.counts.is_empty() {
            self.counts = vec![0; BUCKETS];
        }
        for (acc, c) in self.counts.iter_mut().zip(s.counts.iter()) {
            *acc += u64::from(*c);
        }
        self.total += u64::from(s.total);
        self.errors += u64::from(s.errors);
    }

    fn quantile(&self, q: f64) -> Option<Duration> {
        if self.total == 0 {
            return None;
        }
        let rank = ((q * self.total as f64).ceil() as u64).max(1);
        let mut seen = 0;
        for (i, c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= rank {
                return Some(bucket_value(i));
            }
        }
        None
    }
}

/// Latency and error counters for one cluster's API client.
pub struct ApiStats {
    start: Instant,
    slot: Duration,
    ring: Mutex<Box<[Slot]>>,
}

/// What the health view shows for the API server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ApiSummary {
    /// p95 over the whole ring (an hour by default).
    pub p95: Option<Duration>,
    /// p95 over the last five minutes.
    pub recent_p95: Option<Duration>,
    pub requests: u64,
    /// Transport failures, timeouts, 5xx and 429 (throttling).  4xx are the
    /// caller's problem (forbidden, not found) and say nothing about the
    /// server's health.
    pub errors: u64,
    /// The last five minutes are clearly slower than the rest of the hour:
    /// at least 1.5× and 50 ms over it, with enough samples on both sides
    /// to mean something.
    pub slower_than_usual: bool,
}

impl Default for ApiStats {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ApiStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiStats")
            .field("slot", &self.slot)
            .field("summary", &self.summary())
            .finish()
    }
}

impl ApiStats {
    pub fn new() -> Self {
        Self::with_slots(DEFAULT_SLOT, DEFAULT_SLOTS)
    }

    /// `slots` slots of `slot` each (at least one slot, at least 1 ms).
    pub fn with_slots(slot: Duration, slots: usize) -> Self {
        Self {
            start: Instant::now(),
            slot: slot.max(Duration::from_millis(1)),
            ring: Mutex::new(vec![Slot::EMPTY; slots.max(1)].into_boxed_slice()),
        }
    }

    fn current_slot(&self) -> u64 {
        (self.start.elapsed().as_nanos() / self.slot.as_nanos()) as u64
    }

    fn ring(&self) -> std::sync::MutexGuard<'_, Box<[Slot]>> {
        // A panic while holding this lock can only leave counters
        // half-updated; they are statistics, so keep going.
        self.ring.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Record one request.
    pub fn record(&self, latency: Duration, ok: bool) {
        let id = self.current_slot();
        let mut ring = self.ring();
        let len = ring.len() as u64;
        let slot = &mut ring[(id % len) as usize];
        if slot.id != id {
            *slot = Slot { id, ..Slot::EMPTY };
        }
        slot.counts[bucket_of(latency)] += 1;
        slot.total += 1;
        if !ok {
            slot.errors += 1;
        }
    }

    /// Merge the slots whose id is in `ids`.
    fn merged(&self, ids: std::ops::RangeInclusive<u64>) -> Merged {
        let ring = self.ring();
        let mut m = Merged::default();
        for s in ring.iter().filter(|s| ids.contains(&s.id)) {
            m.add(s);
        }
        m
    }

    /// The slots covering the last `window` up to now.
    fn window(&self, window: Duration) -> std::ops::RangeInclusive<u64> {
        let now = self.current_slot();
        let back = (window.as_nanos().div_ceil(self.slot.as_nanos()) as u64).max(1) - 1;
        now.saturating_sub(back)..=now
    }

    /// p95 over (about) the last `window`, rounded out to whole slots.
    pub fn p95_over(&self, window: Duration) -> Option<Duration> {
        self.merged(self.window(window)).quantile(0.95)
    }

    pub fn summary(&self) -> ApiSummary {
        let now = self.current_slot();
        let len = self.ring().len() as u64;
        let all = self.merged(now.saturating_sub(len - 1)..=now);
        let recent_from = now.saturating_sub(RECENT_SLOTS - 1);
        let recent = self.merged(recent_from..=now);
        let baseline = match recent_from.checked_sub(1) {
            Some(end) => self.merged(now.saturating_sub(len - 1)..=end),
            None => Merged::default(),
        };
        let recent_p95 = recent.quantile(0.95);
        let slower_than_usual = match (recent_p95, baseline.quantile(0.95)) {
            (Some(r), Some(b)) if recent.total >= 5 && baseline.total >= 30 => {
                r.as_secs_f64() >= b.as_secs_f64() * 1.5
                    && r.saturating_sub(b) >= Duration::from_millis(50)
            }
            _ => false,
        };
        ApiSummary {
            p95: all.quantile(0.95),
            recent_p95,
            requests: all.total,
            errors: all.errors,
            slower_than_usual,
        }
    }

    /// Forget everything (a new connection).
    pub fn reset(&self) {
        self.ring().fill(Slot::EMPTY);
    }
}

/// `true` for requests that stay open: watches and followed logs.
fn is_streaming(uri: &Uri) -> bool {
    uri.query().is_some_and(|q| {
        q.split('&')
            .any(|kv| matches!(kv, "watch=true" | "watch=1" | "follow=true" | "follow=1"))
    })
}

fn is_error_status(s: StatusCode) -> bool {
    s.is_server_error() || s == StatusCode::TOO_MANY_REQUESTS
}

/// Tower layer that records every non-streaming request into an
/// [`ApiStats`].  Install it with
/// [`kube::client::ClientBuilder::with_layer`].
#[derive(Clone, Debug)]
pub struct ApiStatsLayer {
    stats: Arc<ApiStats>,
}

impl ApiStatsLayer {
    pub fn new(stats: Arc<ApiStats>) -> Self {
        Self { stats }
    }
}

impl<S> Layer<S> for ApiStatsLayer {
    type Service = ApiStatsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ApiStatsService {
            inner,
            stats: self.stats.clone(),
        }
    }
}

/// The service [`ApiStatsLayer`] wraps around the client's stack.
#[derive(Clone, Debug)]
pub struct ApiStatsService<S> {
    inner: S,
    stats: Arc<ApiStats>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for ApiStatsService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        if is_streaming(req.uri()) {
            return Box::pin(self.inner.call(req));
        }
        let stats = self.stats.clone();
        let started = Instant::now();
        let fut = self.inner.call(req);
        Box::pin(async move {
            let res = fut.await;
            let ok = matches!(&res, Ok(r) if !is_error_status(r.status()));
            stats.record(started.elapsed(), ok);
            res
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(v: u64) -> Duration {
        Duration::from_millis(v)
    }

    #[test]
    fn buckets_are_monotonic_and_within_a_quarter() {
        let mut last = 0;
        for v in [0u64, 1, 2, 5, 10, 84, 212, 1_000, 30_000] {
            let b = bucket_of(ms(v));
            assert!(b >= last, "bucket order at {v} ms");
            last = b;
            if v >= 1 {
                let rep = bucket_value(b).as_secs_f64() * 1000.0;
                let err = (rep - v as f64).abs() / v as f64;
                assert!(err <= 0.125, "{v} ms → {rep:.1} ms ({:.0} %)", err * 100.0);
            }
        }
        assert_eq!(bucket_of(Duration::from_secs(100_000)), BUCKETS - 1);
    }

    #[tokio::test(start_paused = true)]
    async fn p95_and_errors_over_the_hour() {
        let stats = ApiStats::new();
        for _ in 0..95 {
            stats.record(ms(20), true);
        }
        for _ in 0..5 {
            stats.record(ms(400), false);
        }
        let s = stats.summary();
        assert_eq!(s.requests, 100);
        assert_eq!(s.errors, 5);
        let p95 = s.p95.unwrap().as_millis();
        assert!((18..=22).contains(&p95), "p95 is the 95th sample: {p95}");
        stats.record(ms(400), true);
        let p95 = stats.summary().p95.unwrap().as_millis();
        assert!(p95 > 350, "one more slow sample tips it: {p95}");
    }

    #[tokio::test(start_paused = true)]
    async fn old_slots_age_out() {
        let stats = ApiStats::with_slots(Duration::from_secs(30), 4);
        stats.record(ms(10), false);
        tokio::time::advance(Duration::from_secs(30 * 3)).await;
        assert_eq!(stats.summary().requests, 1, "still inside the ring");
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(stats.summary().requests, 0, "a full lap later it's gone");
        // A new sample reuses the stale slot without inheriting its counts.
        stats.record(ms(10), true);
        let s = stats.summary();
        assert_eq!((s.requests, s.errors), (1, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn p95_over_a_window_only_sees_recent_slots() {
        let stats = ApiStats::new();
        stats.record(ms(500), true);
        tokio::time::advance(Duration::from_secs(90)).await;
        stats.record(ms(10), true);
        let recent = stats.p95_over(Duration::from_secs(30)).unwrap();
        assert!(recent < ms(20));
        assert!(stats.p95_over(Duration::from_secs(120)).unwrap() > ms(400));
        assert_eq!(
            stats.p95_over(Duration::ZERO).map(|d| d < ms(20)),
            Some(true),
            "a zero window still reads the current slot"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn slower_than_usual_needs_a_real_and_supported_change() {
        let stats = ApiStats::new();
        for _ in 0..60 {
            stats.record(ms(80), true);
        }
        // Move past the recent window so the 80 ms samples are baseline.
        tokio::time::advance(DEFAULT_SLOT * RECENT_SLOTS as u32).await;
        for _ in 0..4 {
            stats.record(ms(250), true);
        }
        assert!(!stats.summary().slower_than_usual, "too few recent samples");
        stats.record(ms(250), true);
        let s = stats.summary();
        assert!(s.slower_than_usual, "{s:?}");

        // 1.5× of a tiny baseline is not "slower" in any way a person notices.
        let fast = ApiStats::new();
        for _ in 0..60 {
            fast.record(ms(4), true);
        }
        tokio::time::advance(DEFAULT_SLOT * RECENT_SLOTS as u32).await;
        for _ in 0..10 {
            fast.record(ms(12), true);
        }
        assert!(!fast.summary().slower_than_usual);
    }

    #[test]
    fn reset_forgets() {
        let stats = ApiStats::new();
        stats.record(ms(5), false);
        stats.reset();
        assert_eq!(stats.summary(), ApiSummary::default());
    }

    #[test]
    fn streaming_requests_are_recognised() {
        let u = |s: &str| s.parse::<Uri>().unwrap();
        assert!(is_streaming(&u(
            "/api/v1/pods?watch=true&resourceVersion=1"
        )));
        assert!(is_streaming(&u(
            "/api/v1/namespaces/a/pods/b/log?follow=true"
        )));
        assert!(!is_streaming(&u("/api/v1/pods?limit=500")));
        assert!(!is_streaming(&u("/api/v1/pods?watchful=true")));
        assert!(!is_streaming(&u("/version")));
    }

    /// A service that answers with a scripted status after a delay.
    #[derive(Clone)]
    struct Scripted(StatusCode, Duration);

    impl Service<Request<()>> for Scripted {
        type Response = Response<()>;
        type Error = std::io::Error;
        type Future = BoxFuture<'static, Result<Response<()>, std::io::Error>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _: Request<()>) -> Self::Future {
            let (status, delay) = (self.0, self.1);
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                let mut r = Response::new(());
                *r.status_mut() = status;
                Ok(r)
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_layer_times_requests_and_classifies_outcomes() {
        let stats = Arc::new(ApiStats::new());
        let layer = ApiStatsLayer::new(stats.clone());
        let req = |uri: &str| Request::builder().uri(uri).body(()).unwrap();

        let mut ok = layer.layer(Scripted(StatusCode::OK, ms(84)));
        ok.call(req("/api/v1/nodes")).await.unwrap();
        let mut forbidden = layer.layer(Scripted(StatusCode::FORBIDDEN, ms(5)));
        forbidden.call(req("/api/v1/secrets")).await.unwrap();
        let mut throttled = layer.layer(Scripted(StatusCode::TOO_MANY_REQUESTS, ms(5)));
        throttled.call(req("/api/v1/pods")).await.unwrap();
        let mut watch = layer.layer(Scripted(StatusCode::OK, Duration::from_secs(290)));
        watch.call(req("/api/v1/pods?watch=true")).await.unwrap();

        let s = stats.summary();
        assert_eq!(s.requests, 3, "the watch is not counted");
        assert_eq!(s.errors, 1, "429 is an error, 403 is not");
        let p95 = s.p95.unwrap().as_millis();
        assert!((75..=95).contains(&p95), "p95 ≈ 84 ms, got {p95}");
    }
}
