//! Godot node wrapping [`ResourceIngest`]: drains the watcher channel on a
//! timer, applies the events, and emits signals for what changed.

use crossbeam_channel::Receiver;
use godot::classes::Timer;
use godot::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::debug;

use super::{EdgeType, KubeResource, KubeResourceIdentifier, KubeResourceStore, ResourceIngest};
use crate::async_runtime::AsyncRuntime;
use crate::k8s::Watched;

#[derive(GodotClass)]
#[class(base=Node)]
pub struct KubeStore {
    base: Base<Node>,
    receiver: Option<Receiver<Watched>>,
    ingest: ResourceIngest,
    /// Repeating timer that drives watcher-channel draining at a fixed
    /// interval instead of polling every physics frame.
    drain_timer: Option<Gd<Timer>>,
}

#[godot_api]
impl INode for KubeStore {
    fn init(base: Base<Node>) -> Self {
        Self {
            base,
            receiver: None,
            ingest: ResourceIngest::new(),
            drain_timer: None,
        }
    }

    fn ready(&mut self) {
        debug!("KubeStore ready() called");
        self.setup_drain_timer();
    }

    // Called every physics frame — only polls the pending background graph
    // build so the graph swap lands within one frame of the build finishing.
    fn physics_process(&mut self, _delta: f64) {
        if self.ingest.poll_graph_build(&AsyncRuntime::handle()) {
            self.signals().kube_goat_updated().emit();
        }
    }
}

// ---------------------------------------------------------------------------
// Plain Rust API — not exported to Godot
// ---------------------------------------------------------------------------

impl KubeStore {
    /// Inject the watcher receiver channel into this store.
    ///
    /// Should be called once by the parent scene node after both this node
    /// and the `AppState` singleton are fully initialised.  The drain timer
    /// silently no-ops until a receiver is provided.
    pub fn set_receiver(&mut self, receiver: Arc<Receiver<Watched>>) {
        self.receiver = Some((*receiver).clone());
        debug!("KubeStore: receiver channel set");
    }

    /// The engine-free core, for callers that want direct access.
    pub fn ingest(&self) -> &ResourceIngest {
        &self.ingest
    }
}

// ---------------------------------------------------------------------------
// Godot API — signals, #[func] callbacks, and public Rust getters
// ---------------------------------------------------------------------------

#[godot_api]
impl KubeStore {
    #[signal]
    pub fn kube_resource_updated(kri: GString, is_new: bool);

    #[signal]
    pub fn full_resource_list_updated();

    #[signal]
    pub fn kube_resource_deleted(kri: GString);

    #[signal]
    pub fn kube_goat_updated();

    /// A fresh Namespace snapshot (names + labels) was stored.
    #[signal]
    pub fn namespace_labels_updated();

    /// Drains all pending messages from the kube-watcher channel, applies
    /// them, and emits the matching signals.  Called by the drain timer.
    #[func]
    fn drain_watcher_channel(&mut self) {
        let Some(receiver) = &self.receiver else {
            return;
        };
        // Non-blocking drain — the Godot main thread must never block.
        let msgs: Vec<Watched> = receiver.try_iter().collect();
        if msgs.is_empty() {
            return;
        }

        let report = self.ingest.apply(msgs, &AsyncRuntime::handle());

        if report.full_list_received {
            self.signals().full_resource_list_updated().emit();
        }
        if report.namespaces_updated {
            self.signals().namespace_labels_updated().emit();
        }
        for (kri, is_new) in &report.updated {
            self.signals()
                .kube_resource_updated()
                .emit(&GString::from(&kri.to_string()), *is_new);
        }
        for kri in &report.deleted {
            self.signals()
                .kube_resource_deleted()
                .emit(&GString::from(&kri.to_string()));
        }
    }

    /// Disconnect support: discard anything still queued from the previous
    /// watch session, clear the store and graph, and emit
    /// `full_resource_list_updated`, `namespace_labels_updated` and
    /// `kube_goat_updated` so views empty out.  Call after
    /// `KubeWatcher::stop()` has returned and before the next session starts
    /// (a snapshot already queued from the new session would be dropped).
    #[func]
    pub fn reset(&mut self) {
        if let Some(receiver) = &self.receiver {
            let dropped = receiver.try_iter().count();
            debug!(
                dropped,
                "KubeStore: discarded queued events from the previous session"
            );
        }
        self.ingest.reset();
        self.signals().full_resource_list_updated().emit();
        self.signals().namespace_labels_updated().emit();
        self.signals().kube_goat_updated().emit();
    }

    /// Multi-cluster: make this store show another cluster.  Puts `ingest`
    /// and `receiver` in place and returns the previous pair, so the caller
    /// can keep ingesting that cluster in the background (it must also keep
    /// calling [`ResourceIngest::poll_graph_build`] on it).  Emits the same
    /// signals as [`Self::reset`] so every view rebuilds from the new data.
    /// O(1): nothing is copied.
    pub fn swap_source(
        &mut self,
        ingest: ResourceIngest,
        receiver: Option<Receiver<Watched>>,
    ) -> (ResourceIngest, Option<Receiver<Watched>>) {
        let old_ingest = std::mem::replace(&mut self.ingest, ingest);
        let old_receiver = std::mem::replace(&mut self.receiver, receiver);
        debug!(
            cluster_id = self.ingest.cluster_id(),
            "KubeStore: source swapped"
        );
        self.signals().full_resource_list_updated().emit();
        self.signals().namespace_labels_updated().emit();
        self.signals().kube_goat_updated().emit();
        (old_ingest, old_receiver)
    }

    /// Set the local cluster integer ID resolved from the `clusters` DB
    /// table.  Godot callers pass `i64`; values outside `[0, u32::MAX]` are
    /// clamped to 0.
    #[func]
    pub fn set_cluster_id(&mut self, id: i64) {
        let safe_id = u32::try_from(id).unwrap_or(0);
        self.ingest.set_cluster_id(safe_id);
        debug!("KubeStore: cluster_id set to {}", safe_id);
    }

    // ── Public Rust getters ───────────────────────────────────────────────

    pub fn get_all_resources(&self) -> Vec<(KubeResourceIdentifier, KubeResource)> {
        self.ingest.get_all_resources()
    }

    /// Read the store in place (see [`ResourceIngest::with_store`]).
    pub fn with_store<R>(&self, f: impl FnOnce(&KubeResourceStore) -> R) -> R {
        self.ingest.with_store(f)
    }

    /// Labels of every Namespace object keyed by namespace name.
    pub fn get_namespace_labels(&self) -> HashMap<String, HashMap<String, String>> {
        self.ingest.get_namespace_labels().clone()
    }

    /// Creation time (unix seconds) of every Namespace object keyed by name.
    pub fn get_namespace_creation_times(&self) -> HashMap<String, i64> {
        self.ingest.get_namespace_creation_times().clone()
    }

    pub fn get_all_namespaces(&self) -> Vec<String> {
        self.ingest.get_all_namespaces()
    }

    pub fn get_resource_by_kri(&self, kri: &KubeResourceIdentifier) -> Option<KubeResource> {
        self.ingest.get_resource_by_kri(kri)
    }

    /// Namespace adjacency index from the KubeGoat graph.
    pub fn get_namespace_adjacency(
        &self,
    ) -> HashMap<KubeResourceIdentifier, HashSet<KubeResourceIdentifier>> {
        self.ingest.get_namespace_adjacency()
    }

    /// Given a namespace KRI, return all namespace KRIs directly connected to it.
    pub fn get_affected_namespaces(
        &self,
        ns_kri: &KubeResourceIdentifier,
    ) -> Vec<KubeResourceIdentifier> {
        self.ingest.get_affected_namespaces(ns_kri)
    }

    /// Every node in the connected component of `kri`, including itself.
    pub fn get_edges_for_kri(
        &self,
        kri: &KubeResourceIdentifier,
    ) -> Option<Vec<KubeResourceIdentifier>> {
        self.ingest.get_edges_for_kri(kri)
    }

    pub fn get_outgoing_edges_for_kri(
        &self,
        kri: &KubeResourceIdentifier,
    ) -> Option<Vec<(KubeResourceIdentifier, EdgeType)>> {
        self.ingest.get_outgoing_edges_for_kri(kri)
    }

    /// Returns all KRIs currently in the store.
    pub fn get_all_kris(&self) -> Vec<KubeResourceIdentifier> {
        self.ingest.get_all_kris()
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

impl KubeStore {
    /// How often the watcher channel is drained (seconds).
    const DRAIN_INTERVAL_SECS: f64 = 0.5;

    fn setup_drain_timer(&mut self) {
        let mut timer = Timer::new_alloc();
        timer.set_wait_time(Self::DRAIN_INTERVAL_SECS);
        timer.set_one_shot(false);
        timer.connect("timeout", &self.base().callable("drain_watcher_channel"));
        self.base_mut().add_child(&timer.clone().upcast::<Node>());
        timer.start();
        self.drain_timer = Some(timer);
        debug!(
            interval_secs = Self::DRAIN_INTERVAL_SECS,
            "KubeStore: drain timer started"
        );
    }
}
