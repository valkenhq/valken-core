//! valken-core — Kubernetes connectivity for Valken.
//!
//! * [`k8s`] — cluster connection ([`k8s::KubeWatcher`]), the resource store
//!   and relationship graph ([`k8s::ResourceIngest`], [`k8s::KubeGoat`]),
//!   metrics, and ingress-controller detection.
//! * [`traffic`] — ingress access-log tailing rolled up into 10 s buckets.
//! * [`async_runtime`] — messages from background tasks to the UI.
//!
//! The crate is plain Rust except for three Godot nodes behind the `godot`
//! feature (on by default).  I/O goes through traits
//! ([`k8s::ClusterSource`], [`k8s::MetricsSource`],
//! [`traffic::LogSource`], [`async_runtime::Notifier`]) so every pipeline
//! can be driven end to end in tests without a cluster or an engine.

pub mod async_runtime;
pub mod k8s;
pub mod traffic;
