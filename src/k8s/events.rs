//! Kubernetes Events.
//!
//! Two shapes of the same objects:
//!
//! * [`EventRow`] — `core/v1`, watched per namespace for the Logs and
//!   Describe windows ([`watch_events`], [`list_events`]).  The caller
//!   filters to the objects it shows.
//! * [`EventRecord`] — `events.k8s.io/v1`, one watch per cluster
//!   ([`watch_cluster_events`]) for the Events pane: it carries the involved
//!   object's uid, the reporting controller and the series counts the
//!   pane groups by.  [`EventSource`] is the seam that lets the pane's
//!   ingest run against a fake in tests, together with the ConfigMap and
//!   Secret metadata watches from [`crate::k8s::configs`].

use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::StreamExt;
use k8s_openapi::api::core::v1::{Event, ObjectReference};
use k8s_openapi::api::events::v1::Event as EventV1;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, Time};
use kube::api::ListParams;
use kube::runtime::watcher::{self, watcher};
use kube::runtime::WatchStreamExt;
use kube::{Api, Client};

use crate::k8s::configs::{config_map_meta, secret_meta, ConfigMeta};
use crate::k8s::error::{ObjectError, WatchError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRow {
    pub uid: String,
    /// When it (last) happened.
    pub time: Option<DateTime<Utc>>,
    /// `Warning` or `Normal`.
    pub kind: String,
    pub reason: String,
    pub message: String,
    pub object_kind: String,
    pub object_name: String,
    pub count: i32,
}

impl EventRow {
    pub fn is_warning(&self) -> bool {
        self.kind == "Warning"
    }
}

fn chrono_of(secs: i64, nanos: i32) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(secs, nanos.max(0) as u32)
}

pub fn event_row(e: &Event) -> EventRow {
    let time = e
        .event_time
        .as_ref()
        .map(|t| chrono_of(t.0.as_second(), t.0.subsec_nanosecond()))
        .or_else(|| {
            e.last_timestamp
                .as_ref()
                .map(|t| chrono_of(t.0.as_second(), 0))
        })
        .or_else(|| {
            e.first_timestamp
                .as_ref()
                .map(|t| chrono_of(t.0.as_second(), 0))
        })
        .or_else(|| {
            e.metadata
                .creation_timestamp
                .as_ref()
                .map(|t| chrono_of(t.0.as_second(), 0))
        })
        .flatten();
    EventRow {
        uid: e.metadata.uid.clone().unwrap_or_default(),
        time,
        kind: e.type_.clone().unwrap_or_else(|| "Normal".into()),
        reason: e.reason.clone().unwrap_or_default(),
        message: e.message.clone().unwrap_or_default().trim().to_string(),
        object_kind: e.involved_object.kind.clone().unwrap_or_default(),
        object_name: e.involved_object.name.clone().unwrap_or_default(),
        count: e.count.unwrap_or(1),
    }
}

/// Every event in `namespace`, existing ones first, then as they happen
/// (an updated event — `count` going up — comes again with the same uid).
/// Errors before the first sync are [`WatchError::InitialSync`], later ones
/// [`WatchError::Stream`]; the watch backs off and retries after both.
pub fn watch_events(
    client: &Client,
    namespace: &str,
) -> BoxStream<'static, Result<EventRow, WatchError>> {
    let api: Api<Event> = Api::namespaced(client.clone(), namespace);
    watcher(api, watcher::Config::default())
        .default_backoff()
        .scan(false, |synced, ev| {
            let was_synced = *synced;
            if matches!(ev, Ok(watcher::Event::InitDone)) {
                *synced = true;
            }
            futures::future::ready(Some((was_synced, ev)))
        })
        .filter_map(|(synced, ev)| futures::future::ready(row_or_error(synced, ev)))
        .boxed()
}

fn row_or_error(
    synced: bool,
    ev: Result<watcher::Event<Event>, watcher::Error>,
) -> Option<Result<EventRow, WatchError>> {
    match ev {
        Ok(watcher::Event::Apply(e) | watcher::Event::InitApply(e)) => Some(Ok(event_row(&e))),
        Ok(_) => None,
        Err(e) if synced => Some(Err(WatchError::Stream(e))),
        Err(e) => Some(Err(WatchError::InitialSync(e))),
    }
}

/// The events in `namespace` right now, newest first, optionally only those
/// about one object (`involved = Some((kind, name))`).  One LIST, no watch:
/// the agent's `get_events` and watch-mode evidence.
#[tracing::instrument(name = "k8s.events.list", skip(client), fields(rows = tracing::field::Empty))]
pub async fn list_events(
    client: &Client,
    namespace: &str,
    involved: Option<(&str, &str)>,
) -> Result<Vec<EventRow>, ObjectError> {
    let api: Api<Event> = Api::namespaced(client.clone(), namespace);
    let mut params = ListParams::default();
    if let Some((kind, name)) = involved {
        params = params.fields(&format!(
            "involvedObject.kind={kind},involvedObject.name={name}"
        ));
    }
    let list = api.list(&params).await.map_err(ObjectError::from_api)?;
    tracing::Span::current().record("rows", list.items.len());
    Ok(newest_first(list.items.iter().map(event_row).collect()))
}

fn newest_first(mut rows: Vec<EventRow>) -> Vec<EventRow> {
    rows.sort_by_key(|r| std::cmp::Reverse(r.time));
    rows
}

// ─── events.k8s.io/v1, one watch per cluster ─────────────────────────────────

/// `Warning` or `Normal`; anything else the API server might send is kept
/// as [`EventType::Other`] rather than dropped.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum EventType {
    Normal,
    Warning,
    Other(String),
}

impl EventType {
    pub fn parse(s: &str) -> Self {
        match s {
            "Normal" | "" => EventType::Normal,
            "Warning" => EventType::Warning,
            other => EventType::Other(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            EventType::Normal => "Normal",
            EventType::Warning => "Warning",
            EventType::Other(s) => s,
        }
    }

    pub fn is_warning(&self) -> bool {
        matches!(self, EventType::Warning)
    }
}

/// The object an event is about (`regarding`) or related to (`related`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct EventObject {
    pub api_version: String,
    pub kind: String,
    pub namespace: String,
    pub name: String,
    /// Empty when the reporter didn't set it (rare; some controllers
    /// reference objects by name only).
    pub uid: String,
}

impl EventObject {
    pub fn from_reference(r: &ObjectReference) -> Self {
        Self {
            api_version: r.api_version.clone().unwrap_or_default(),
            kind: r.kind.clone().unwrap_or_default(),
            namespace: r.namespace.clone().unwrap_or_default(),
            name: r.name.clone().unwrap_or_default(),
            uid: r.uid.clone().unwrap_or_default(),
        }
    }

    /// Something stable to group by when the uid is missing:
    /// `kind/namespace/name`.
    pub fn identity(&self) -> String {
        if self.uid.is_empty() {
            format!("{}/{}/{}", self.kind, self.namespace, self.name)
        } else {
            self.uid.clone()
        }
    }
}

/// One `events.k8s.io/v1` Event, flattened.  The deprecated `core/v1`
/// fields are folded in so events written by old clients (kubelet still
/// uses `count` / `firstTimestamp`) look the same as new ones.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EventRecord {
    /// The Event object's own uid (not the involved object's).
    pub uid: String,
    pub namespace: String,
    pub name: String,
    pub type_: EventType,
    pub reason: String,
    /// `note` (`message` in core/v1), trimmed.
    pub note: String,
    pub action: String,
    pub regarding: EventObject,
    pub related: Option<EventObject>,
    /// `reportingController`, else the deprecated `source.component`.
    pub reporting_controller: String,
    /// `reportingInstance`, else the deprecated `source.host`.
    pub reporting_instance: String,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_seen: Option<DateTime<Utc>>,
    /// Occurrences so far: `series.count`, else the deprecated `count`,
    /// else 1.
    pub count: i32,
}

impl EventRecord {
    pub fn is_warning(&self) -> bool {
        self.type_.is_warning()
    }
}

fn time_of(t: &Time) -> Option<DateTime<Utc>> {
    chrono_of(t.0.as_second(), 0)
}

fn micro_time_of(t: &MicroTime) -> Option<DateTime<Utc>> {
    chrono_of(t.0.as_second(), t.0.subsec_nanosecond())
}

/// Flatten an `events.k8s.io/v1` Event.
pub fn event_record(e: &EventV1) -> EventRecord {
    let created = e.metadata.creation_timestamp.as_ref().and_then(time_of);
    let event_time = e.event_time.as_ref().and_then(micro_time_of);
    let first_seen = e
        .deprecated_first_timestamp
        .as_ref()
        .and_then(time_of)
        .or(event_time)
        .or(created);
    let last_seen = e
        .series
        .as_ref()
        .and_then(|s| micro_time_of(&s.last_observed_time))
        .or_else(|| e.deprecated_last_timestamp.as_ref().and_then(time_of))
        .or(event_time)
        .or(first_seen);
    let count = e
        .series
        .as_ref()
        .map(|s| s.count)
        .or(e.deprecated_count)
        .unwrap_or(1)
        .max(1);
    let source = e.deprecated_source.as_ref();
    EventRecord {
        uid: e.metadata.uid.clone().unwrap_or_default(),
        namespace: e.metadata.namespace.clone().unwrap_or_default(),
        name: e.metadata.name.clone().unwrap_or_default(),
        type_: EventType::parse(e.type_.as_deref().unwrap_or_default()),
        reason: e.reason.clone().unwrap_or_default(),
        note: e.note.clone().unwrap_or_default().trim().to_string(),
        action: e.action.clone().unwrap_or_default(),
        regarding: e
            .regarding
            .as_ref()
            .map(EventObject::from_reference)
            .unwrap_or_default(),
        related: e.related.as_ref().map(EventObject::from_reference),
        reporting_controller: e
            .reporting_controller
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| source.and_then(|s| s.component.clone()))
            .unwrap_or_default(),
        reporting_instance: e
            .reporting_instance
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| source.and_then(|s| s.host.clone()))
            .unwrap_or_default(),
        first_seen,
        last_seen,
        count,
    }
}

/// A watch whose items are already converted: `Init` / `InitApply` /
/// `InitDone` on every (re)list, then `Apply` / `Delete` as they happen.
pub type RecordWatch<T> = BoxStream<'static, Result<watcher::Event<T>, watcher::Error>>;

fn map_event<K, T>(ev: watcher::Event<K>, f: impl Fn(&K) -> T) -> watcher::Event<T> {
    match ev {
        watcher::Event::Apply(k) => watcher::Event::Apply(f(&k)),
        watcher::Event::Delete(k) => watcher::Event::Delete(f(&k)),
        watcher::Event::InitApply(k) => watcher::Event::InitApply(f(&k)),
        watcher::Event::Init => watcher::Event::Init,
        watcher::Event::InitDone => watcher::Event::InitDone,
    }
}

fn converted_watch<K, T>(api: Api<K>, f: impl Fn(&K) -> T + Send + 'static) -> RecordWatch<T>
where
    K: kube::Resource + Clone + std::fmt::Debug + Send + serde::de::DeserializeOwned + 'static,
    K::DynamicType: Default,
    T: Send + 'static,
{
    watcher(api, watcher::Config::default())
        .default_backoff()
        .map(move |item| item.map(|ev| map_event(ev, &f)))
        .boxed()
}

/// Every event in the cluster (`events.k8s.io/v1`, every namespace),
/// retried with backoff when the watch drops.  The API server keeps events
/// for about an hour (`--event-ttl`), so the initial list is the last hour.
pub fn watch_cluster_events(client: &Client) -> RecordWatch<EventRecord> {
    converted_watch(Api::<EventV1>::all(client.clone()), event_record)
}

/// Read-only access to what the Events pane records: the cluster's events
/// and the ConfigMap / Secret metadata it watches for changes.  Production
/// wraps a [`kube::Client`] ([`KubeEventSource`]); tests feed streams.
pub trait EventSource: Send + Sync + 'static {
    fn watch_events(&self) -> RecordWatch<EventRecord>;
    /// ConfigMaps as [`ConfigMeta`]: key names and value hashes, no values.
    fn watch_config_maps(&self) -> RecordWatch<ConfigMeta>;
    /// Secrets as [`ConfigMeta`]: key names and sizes only.  Fails with
    /// 403 when the kubeconfig can't list Secrets; the caller treats that
    /// as "not watched", not as an error.
    fn watch_secrets(&self) -> RecordWatch<ConfigMeta>;
}

/// [`EventSource`] backed by a real API server.
#[derive(Clone)]
pub struct KubeEventSource {
    client: Client,
}

impl KubeEventSource {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl EventSource for KubeEventSource {
    fn watch_events(&self) -> RecordWatch<EventRecord> {
        watch_cluster_events(&self.client)
    }

    fn watch_config_maps(&self) -> RecordWatch<ConfigMeta> {
        converted_watch(
            Api::<k8s_openapi::api::core::v1::ConfigMap>::all(self.client.clone()),
            config_map_meta,
        )
    }

    fn watch_secrets(&self) -> RecordWatch<ConfigMeta> {
        converted_watch(
            Api::<k8s_openapi::api::core::v1::Secret>::all(self.client.clone()),
            secret_meta,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::EventSource as CoreSource;
    use k8s_openapi::api::events::v1::EventSeries;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    #[test]
    fn rows_from_events() {
        let e = Event {
            type_: Some("Warning".into()),
            reason: Some("BackOff".into()),
            message: Some("Back-off restarting failed container\n".into()),
            involved_object: ObjectReference {
                kind: Some("Pod".into()),
                name: Some("ledger-x2kq".into()),
                ..Default::default()
            },
            count: Some(212),
            ..Default::default()
        };
        let r = event_row(&e);
        assert!(r.is_warning());
        assert_eq!(
            (r.reason.as_str(), r.object_name.as_str(), r.count),
            ("BackOff", "ledger-x2kq", 212)
        );
        assert_eq!(r.message, "Back-off restarting failed container");
        assert_eq!(r.time, None);
    }

    #[test]
    fn sorts_newest_first_with_untimed_last() {
        let row = |uid: &str, secs: Option<i64>| EventRow {
            uid: uid.into(),
            time: secs.and_then(|s| DateTime::from_timestamp(s, 0)),
            kind: "Normal".into(),
            reason: String::new(),
            message: String::new(),
            object_kind: String::new(),
            object_name: String::new(),
            count: 1,
        };
        let rows = newest_first(vec![
            row("a", Some(1)),
            row("none", None),
            row("b", Some(5)),
        ]);
        assert_eq!(
            rows.iter().map(|r| r.uid.as_str()).collect::<Vec<_>>(),
            ["b", "a", "none"]
        );
    }

    fn micro(secs: i64) -> MicroTime {
        MicroTime(k8s_openapi::jiff::Timestamp::from_second(secs).unwrap())
    }

    fn time(secs: i64) -> Time {
        Time(k8s_openapi::jiff::Timestamp::from_second(secs).unwrap())
    }

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    #[test]
    fn event_type_parses_and_keeps_unknown_values() {
        assert_eq!(EventType::parse("Warning"), EventType::Warning);
        assert_eq!(EventType::parse("Normal"), EventType::Normal);
        assert_eq!(EventType::parse(""), EventType::Normal);
        assert_eq!(EventType::parse("Error"), EventType::Other("Error".into()));
        assert!(EventType::Warning.is_warning());
        assert_eq!(EventType::Other("Error".into()).as_str(), "Error");
    }

    #[test]
    fn record_from_new_style_event_uses_series_and_reporting_fields() {
        let e = EventV1 {
            metadata: ObjectMeta {
                uid: Some("ev-1".into()),
                namespace: Some("storefront".into()),
                name: Some("checkout-x2kq.18".into()),
                creation_timestamp: Some(time(1_000)),
                ..Default::default()
            },
            type_: Some("Warning".into()),
            reason: Some("OOMKilled".into()),
            note: Some("  Container app was OOMKilled \n".into()),
            action: Some("Killing".into()),
            event_time: Some(micro(1_100)),
            regarding: Some(ObjectReference {
                api_version: Some("v1".into()),
                kind: Some("Pod".into()),
                namespace: Some("storefront".into()),
                name: Some("checkout-x2kq".into()),
                uid: Some("8f3c".into()),
                ..Default::default()
            }),
            reporting_controller: Some("kubelet".into()),
            reporting_instance: Some("pool-a-3".into()),
            series: Some(EventSeries {
                count: 3,
                last_observed_time: micro(1_400),
            }),
            ..Default::default()
        };
        let r = event_record(&e);
        assert!(r.is_warning());
        assert_eq!(r.uid, "ev-1");
        assert_eq!(r.namespace, "storefront");
        assert_eq!(r.note, "Container app was OOMKilled");
        assert_eq!(r.action, "Killing");
        assert_eq!(r.regarding.uid, "8f3c");
        assert_eq!(r.regarding.identity(), "8f3c");
        assert_eq!(
            (
                r.reporting_controller.as_str(),
                r.reporting_instance.as_str()
            ),
            ("kubelet", "pool-a-3")
        );
        assert_eq!(
            r.first_seen,
            Some(at(1_100)),
            "eventTime is first seen when there is no deprecated firstTimestamp"
        );
        assert_eq!(
            r.last_seen,
            Some(at(1_400)),
            "the series' last observed time is last seen"
        );
        assert_eq!(r.count, 3);
        assert!(r.related.is_none());
    }

    #[test]
    fn record_from_old_style_event_folds_deprecated_fields() {
        let e = EventV1 {
            metadata: ObjectMeta {
                uid: Some("ev-2".into()),
                ..Default::default()
            },
            type_: Some("Normal".into()),
            reason: Some("Pulled".into()),
            deprecated_count: Some(212),
            deprecated_first_timestamp: Some(time(100)),
            deprecated_last_timestamp: Some(time(900)),
            deprecated_source: Some(CoreSource {
                component: Some("kubelet".into()),
                host: Some("node-1".into()),
            }),
            regarding: Some(ObjectReference {
                kind: Some("Pod".into()),
                name: Some("p".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let r = event_record(&e);
        assert_eq!(r.type_, EventType::Normal);
        assert_eq!(r.count, 212);
        assert_eq!((r.first_seen, r.last_seen), (Some(at(100)), Some(at(900))));
        assert_eq!(
            (
                r.reporting_controller.as_str(),
                r.reporting_instance.as_str()
            ),
            ("kubelet", "node-1")
        );
        assert_eq!(
            r.regarding.identity(),
            "Pod//p",
            "no uid: grouped by kind/namespace/name"
        );
    }

    #[test]
    fn record_without_any_time_or_count_defaults_sanely() {
        let e = EventV1 {
            metadata: ObjectMeta {
                creation_timestamp: Some(time(50)),
                ..Default::default()
            },
            ..Default::default()
        };
        let r = event_record(&e);
        assert_eq!(r.count, 1);
        assert_eq!(
            (r.first_seen, r.last_seen),
            (Some(at(50)), Some(at(50))),
            "creation time is the fallback"
        );
        assert_eq!(r.type_, EventType::Normal);
        let none = event_record(&EventV1::default());
        assert_eq!((none.first_seen, none.last_seen), (None, None));
    }

    #[test]
    fn map_event_keeps_the_variant() {
        let f = |k: &i32| k * 2;
        assert!(matches!(
            map_event(watcher::Event::Apply(2), f),
            watcher::Event::Apply(4)
        ));
        assert!(matches!(
            map_event(watcher::Event::Delete(3), f),
            watcher::Event::Delete(6)
        ));
        assert!(matches!(
            map_event(watcher::Event::InitApply(5), f),
            watcher::Event::InitApply(10)
        ));
        assert!(matches!(
            map_event(watcher::Event::<i32>::Init, f),
            watcher::Event::Init
        ));
        assert!(matches!(
            map_event(watcher::Event::<i32>::InitDone, f),
            watcher::Event::InitDone
        ));
    }
}
