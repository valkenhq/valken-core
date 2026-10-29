//! Bridge between Tokio background tasks and the Godot main thread.
//!
//! * [`message`] — [`AsyncMessage`], the [`Notifier`] trait and the global
//!   channel.  Engine-free.
//! * `async_runtime` / `async_notifier` — the Godot singleton that owns the
//!   Tokio runtime and the node that turns messages into signals.  Only
//!   built with the `godot` feature.

pub mod message;

#[cfg(feature = "godot")]
mod async_notifier;
#[cfg(feature = "godot")]
#[allow(clippy::module_inception)]
mod async_runtime;

pub use message::{
    global_notifier, global_receiver, global_sender, AsyncMessage, MetricTrend, Notifier,
    RecordingNotifier,
};

#[cfg(feature = "godot")]
pub use async_notifier::AsyncNotifier;
#[cfg(feature = "godot")]
pub use async_runtime::AsyncRuntime;
