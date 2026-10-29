//! Ingress access-log tailing: the cluster-facing half of the traffic
//! pipeline.
//!
//! ```text
//!  kube watcher (Pod events) ──▶ TrafficManager ──▶ one Listener per ingress pod
//!                                                     │  LogSource → LineProcessor
//!                                                     ▼
//!                                                 Sink<T>
//! ```
//!
//! | Part                          | Responsibility                                              |
//! |-------------------------------|-------------------------------------------------------------|
//! | [`LogSource`], [`KubeLogSource`] | The `pods/log` seam and its implementation                |
//! | [`spawn_listener_with`]       | Per-pod log follower with reconnect/backoff; feeds a [`LineProcessor`] |
//! | [`spawn_traffic_manager_with`], [`Reconciler`] | Pod → listener reconciliation, namespace / pod-IP hints, the manager thread |
//! | [`TrafficTelemetry`]          | Pipeline health published to the UI                         |
//!
//! This crate decides *which* pods to follow and delivers their lines.  What
//! a line means — access-log parsing, rollup buckets, persistence — is the
//! application's: it implements [`LineProcessor`] and a [`ProcessorFactory`]
//! and passes them to [`spawn_traffic_manager_with`].

mod listener;
mod log_source;
mod manager;
mod telemetry;

pub use listener::{
    now_unix, spawn_listener_with, LineProcessor, ListenerDeps, ListenerHandle, ListenerStats,
    NamespaceHints, PodHints, Sink,
};
pub use log_source::{ClientProvider, KubeLogSource, LogLines, LogSource};
pub use manager::{
    pod_key, pod_target, spawn_traffic_manager_with, supports, wants_listener, Action,
    NamespaceHintSet, PodIpIndex, PodTarget, ProcessorContext, ProcessorFactory, Reconciler,
    SupportFn, TrafficManagerHandle,
};
pub use telemetry::{
    classify_stream_error, new_telemetry_cell, read_telemetry, ControllerCensus, ListenerTelemetry,
    StreamError, StreamErrorKind, TelemetryCell, TrafficTelemetry,
};
