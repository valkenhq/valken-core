//! A live table of any resource: list as server-side tables, then watch
//! from the list's resourceVersion and turn each change into one row
//! upsert or delete — the list view updates cells in place instead of
//! re-reading everything.
//!
//! Lists are paged (500 rows); an expired watch (410 Gone) relists and
//! says so with a fresh [`TableEvent::Reset`]; other failures back off
//! (1 s doubling to 30 s) and are reported as [`TableEvent::Error`] so the
//! view can say "forbidden" rather than show an empty table.

use std::collections::VecDeque;
use std::time::Duration;

use futures::stream::{self, BoxStream};
use futures::StreamExt;
use http::header::ACCEPT;
use http::Request;
use kube::core::WatchEvent;
use kube::Client;
use tracing::Instrument;

use super::discovery::ApiResourceInfo;
use super::rows::{TableColumn, TableRow, WireTable};

const TABLE_ACCEPT: &str = "application/json;as=Table;g=meta.k8s.io;v=v1,application/json";
const PAGE: u32 = 500;
/// Server-side watch timeout; the stream reconnects from the last version.
const WATCH_SECS: u32 = 290;
const MAX_BACKOFF_SECS: u64 = 30;

#[derive(Debug, Clone, PartialEq)]
pub enum TableEvent {
    /// The whole table: the first list, or a relist (watch expired).
    Reset {
        columns: Vec<TableColumn>,
        rows: Vec<TableRow>,
    },
    /// A row was added or changed.
    Upsert(TableRow),
    Delete {
        uid: String,
    },
    /// Listing or watching failed (`forbidden: …`); retried.
    Error(String),
}

fn query(namespace_url: &str, params: &[(&str, String)]) -> String {
    let q: Vec<String> = params
        .iter()
        .map(|(k, v)| format!("{k}={}", urlencode(v)))
        .collect();
    format!("{namespace_url}?{}", q.join("&"))
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn request(url: String) -> Result<Request<Vec<u8>>, String> {
    Request::get(url)
        .header(ACCEPT, TABLE_ACCEPT)
        .body(Vec::new())
        .map_err(|e| e.to_string())
}

fn describe(e: &kube::Error) -> String {
    match e {
        kube::Error::Api(s) if s.code == 403 => format!("forbidden: {}", s.message),
        kube::Error::Api(s) => format!("{} ({})", s.message, s.code),
        other => other.to_string(),
    }
}

/// Query for a watch from `rv`.  Bookmarks keep `rv` current while nothing
/// changes, so the reconnect every [`WATCH_SECS`] doesn't fall off the
/// server's history and force a relist.
fn watch_params(rv: &str) -> [(&'static str, String); 5] {
    [
        ("watch", "true".to_string()),
        ("includeObject", "Metadata".to_string()),
        ("allowWatchBookmarks", "true".to_string()),
        ("resourceVersion", rv.to_string()),
        ("timeoutSeconds", WATCH_SECS.to_string()),
    ]
}

fn gone(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(s) if s.code == 410)
}

/// Every page of the table, with the version to watch from.
async fn list(
    client: &Client,
    base: &str,
) -> Result<(Vec<TableColumn>, Vec<TableRow>, String), kube::Error> {
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    let mut cont: Option<String> = None;
    loop {
        let mut params = vec![
            ("includeObject", "Metadata".to_string()),
            ("limit", PAGE.to_string()),
        ];
        if let Some(c) = &cont {
            params.push(("continue", c.clone()));
        }
        let req = request(query(base, &params)).map_err(|e| kube::Error::Service(e.into()))?;
        let page: WireTable = client.request(req).await?;
        if columns.is_empty() {
            columns = page
                .column_definitions
                .iter()
                .map(TableColumn::from)
                .collect();
        }
        rows.extend(page.rows.iter().filter_map(|r| r.to_row()));
        match page.metadata.continue_.filter(|c| !c.is_empty()) {
            Some(c) => cont = Some(c),
            None => {
                return Ok((
                    columns,
                    rows,
                    page.metadata.resource_version.unwrap_or_default(),
                ))
            }
        }
    }
}

enum Phase {
    List,
    Watch {
        rv: String,
    },
    Streaming {
        rv: String,
        events: BoxStream<'static, Result<WatchEvent<WireTable>, kube::Error>>,
    },
}

struct State {
    client: Client,
    base: String,
    kind: String,
    phase: Phase,
    out: VecDeque<TableEvent>,
    failures: u32,
}

impl State {
    async fn backoff(&mut self) {
        if self.failures > 0 {
            let secs = (1u64 << (self.failures - 1).min(5)).min(MAX_BACKOFF_SECS);
            tokio::time::sleep(Duration::from_secs(secs)).await;
        }
    }

    fn fail(&mut self, e: &kube::Error, phase: Phase) {
        self.failures += 1;
        tracing::debug!(kind = %self.kind, error = %e, failures = self.failures, "table watch: failed");
        self.out.push_back(TableEvent::Error(describe(e)));
        self.phase = phase;
    }

    async fn step(&mut self) {
        match std::mem::replace(&mut self.phase, Phase::List) {
            Phase::List => {
                self.backoff().await;
                let span = tracing::debug_span!("k8s.table.list", kind = %self.kind, rows = tracing::field::Empty);
                let listed = list(&self.client, &self.base)
                    .instrument(span.clone())
                    .await;
                match listed {
                    Ok((columns, rows, rv)) => {
                        span.record("rows", rows.len());
                        self.failures = 0;
                        self.out.push_back(TableEvent::Reset { columns, rows });
                        self.phase = Phase::Watch { rv };
                    }
                    Err(e) => self.fail(&e, Phase::List),
                }
            }
            Phase::Watch { rv } => {
                self.backoff().await;
                let started = match request(query(&self.base, &watch_params(&rv))) {
                    Ok(req) => self.client.request_events::<WireTable>(req).await,
                    Err(e) => Err(kube::Error::Service(e.into())),
                };
                match started {
                    Ok(events) => {
                        self.phase = Phase::Streaming {
                            rv,
                            events: events.boxed(),
                        }
                    }
                    Err(e) if gone(&e) => self.phase = Phase::List,
                    Err(e) => self.fail(&e, Phase::Watch { rv }),
                }
            }
            Phase::Streaming { mut rv, mut events } => match events.next().await {
                // The server closed it (timeout): carry on from where we are.
                None => self.phase = Phase::Watch { rv },
                Some(Ok(WatchEvent::Added(t) | WatchEvent::Modified(t))) => {
                    self.failures = 0;
                    for row in t.rows.iter().filter_map(|r| r.to_row()) {
                        rv = row.resource_version.clone();
                        self.out.push_back(TableEvent::Upsert(row));
                    }
                    self.phase = Phase::Streaming { rv, events };
                }
                Some(Ok(WatchEvent::Deleted(t))) => {
                    for row in t.rows.iter().filter_map(|r| r.to_row()) {
                        rv = row.resource_version.clone();
                        self.out.push_back(TableEvent::Delete { uid: row.uid });
                    }
                    self.phase = Phase::Streaming { rv, events };
                }
                Some(Ok(WatchEvent::Bookmark(b))) => {
                    self.phase = Phase::Streaming {
                        rv: b.metadata.resource_version,
                        events,
                    };
                }
                Some(Ok(WatchEvent::Error(status))) if status.code == 410 => {
                    self.phase = Phase::List
                }
                Some(Ok(WatchEvent::Error(status))) => {
                    let e = kube::Error::Api(status);
                    self.fail(&e, Phase::Watch { rv });
                }
                Some(Err(e)) if gone(&e) => self.phase = Phase::List,
                Some(Err(e)) => self.fail(&e, Phase::Watch { rv }),
            },
        }
    }
}

/// Live rows of `resource` (all namespaces for `None`, or a cluster-scoped
/// kind), forever: drop the stream to stop.
pub fn watch_table(
    client: Client,
    resource: &ApiResourceInfo,
    namespace: Option<&str>,
) -> BoxStream<'static, TableEvent> {
    let state = State {
        client,
        base: resource.url(namespace),
        kind: resource.qualified(),
        phase: Phase::List,
        out: VecDeque::new(),
        failures: 0,
    };
    let watchable = resource.watchable;
    stream::unfold(state, move |mut s| async move {
        loop {
            if let Some(ev) = s.out.pop_front() {
                return Some((ev, s));
            }
            // A kind that can't be watched is listed once.
            if !watchable && matches!(s.phase, Phase::Watch { .. }) {
                return None;
            }
            s.step().await;
        }
    })
    .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queries_are_encoded() {
        let url = query(
            "/api/v1/pods",
            &[("continue", "a b/c=".into()), ("limit", "500".into())],
        );
        assert_eq!(url, "/api/v1/pods?continue=a%20b%2Fc%3D&limit=500");
    }

    #[test]
    fn watches_resume_with_bookmarks_and_metadata_only() {
        let url = query("/api/v1/secrets", &watch_params("812"));
        for p in [
            "watch=true",
            "includeObject=Metadata",
            "allowWatchBookmarks=true",
            "resourceVersion=812",
        ] {
            assert!(url.contains(p), "{p} missing from {url}");
        }
    }

    #[test]
    fn requests_ask_for_tables() {
        let req = request("/api/v1/pods".into()).unwrap();
        assert!(req.headers()[ACCEPT].to_str().unwrap().contains("as=Table"));
    }
}
