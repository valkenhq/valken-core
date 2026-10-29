//! Cluster connection and resource watch.
//!
//! [`KubeWatcher`] runs one reflector per watched kind (Pod, Deployment,
//! Service, Ingress, Namespace) on top of a [`ClusterSource`].  The
//! reflectors' stores are the source of truth for snapshots:
//!
//! * when every kind has completed its initial list, one
//!   [`Watched::NamespaceList`] and one [`Watched::FullResourceList`] are sent;
//! * afterwards every `Apply` / `Delete` is forwarded as an incremental
//!   [`Watched`] event;
//! * whenever any watch has to re-list (resource version expired), a fresh
//!   [`Watched::FullResourceList`] is sent so consumers can replace stale state.
//!
//! Each watcher owns its own [`Client`] and publishes its connection
//! [`Health`], so any number of watchers (one per open cluster) can run side
//! by side.  Everything is cancelled by a single token on [`KubeWatcher::stop`].

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use crossbeam_channel::unbounded;
use futures::stream::{self, StreamExt};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Namespace, Pod, Service};
use k8s_openapi::api::networking::v1::Ingress;
use kube::config::Kubeconfig;
use kube::runtime::reflector::{self, ObjectRef, Store};
use kube::runtime::watcher::{self, Event};
use kube::{Client, Config};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn, Instrument};

use crate::async_runtime::{global_notifier, Notifier};
use crate::k8s::error::describe_watch;
use crate::k8s::health::{
    run_health_monitor, watch_error_cause, Health, HealthReporter, HealthSettings,
};
use crate::k8s::kubeconfig::probe::{classify as classify_probe_error, ProbeError};
use crate::k8s::kubeconfig::{AuthKind, ExecEnv};
use crate::k8s::metrics::{spawn_cluster_metrics_monitor, ClusterMetricsSettings};
use crate::k8s::{
    ClusterSource, ConnectError, KubeClusterSource, KubeResource, NamespaceInfo, WatchError,
    Watched, WatchedResources,
};

// ---------------------------------------------------------------------------
// Watcher
// ---------------------------------------------------------------------------

/// Identity of the connected cluster, as shown in the UI.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterInfo {
    /// Display name (kubeconfig context name, else the API server URL).
    pub name: Option<String>,
    /// Default namespace from the kubeconfig.
    pub namespace: Option<String>,
    /// Stable per-cluster identifier (kube-system UID).
    pub uuid: Option<String>,
    /// API server version (`v1.32.2`), when the connect read it.
    pub version: Option<String>,
}

/// The steps of [`KubeWatcher::connect_context`] plus the watch, in order,
/// for the connecting checklist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectStep {
    ReadConfig,
    ReachApi,
    Authenticate,
    Discover,
    Watch,
}

/// Client read timeout for a watching client: kube's default, longer than
/// the API server's 290 s watch timeout, so an idle watch is never cut on
/// our side.
pub const WATCH_READ_TIMEOUT: Duration = Duration::from_secs(295);

/// Split a picker config (whose read timeout is the short per-step timeout)
/// into a config for the long-lived watching client and that step timeout.
/// The connect steps enforce the step timeout themselves.
fn for_watching(mut config: Config) -> (Config, Duration) {
    let step = config.read_timeout.unwrap_or(Duration::from_secs(10));
    config.read_timeout = Some(WATCH_READ_TIMEOUT);
    (config, step)
}

/// Run a request that may open the client's pooled connection outside the
/// caller's span.  The connection task inherits the span that is current
/// when it is spawned and lives as long as the client, so a request made
/// inside the caller's connect span would keep that span open until the
/// cluster is closed (traces would show a minutes-long "connect").
async fn detached<F: std::future::Future>(fut: F) -> F::Output {
    use tracing::instrument::WithSubscriber;
    fut.with_subscriber(tracing::subscriber::NoSubscriber::default())
        .await
}

/// One running watch session: cancelling the token stops every task.
struct Session {
    cancel: CancellationToken,
    /// The watch itself.  When it has ended — [`KubeWatcher::stop`], or the
    /// primary receiver was dropped — the session is over.
    watch: JoinHandle<()>,
    /// Health pinger and metrics monitor; they die with the token.
    tasks: Vec<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct KubeWatcher {
    /// Set by [`KubeWatcher::connect`]; `None` while disconnected.
    source: Arc<RwLock<Option<Arc<dyn ClusterSource>>>>,
    /// The API client for this cluster, for callers that make their own
    /// requests (logs, pod metrics, security, traffic).  `None` while
    /// disconnected and for sources that are not a real API server.
    client: Arc<RwLock<Option<Client>>>,
    health: HealthReporter,
    health_settings: HealthSettings,
    /// Primary sender — consumed by the UI / display layer.
    sender_channel: crossbeam_channel::Sender<Watched>,
    /// Secondary senders — every [`Watched`] event is cloned to each of them
    /// (DB adapter, traffic manager, …).  Register via
    /// [`KubeWatcher::create_fanout_channel`] before `begin_watching()`.
    fanout_senders: Arc<RwLock<Vec<crossbeam_channel::Sender<Watched>>>>,
    info: Arc<RwLock<ClusterInfo>>,
    /// Local integer cluster ID from the `clusters` DB table.  Shared with
    /// the kube-db adapter so it uses the correct ID when building KRIs.
    cluster_id: Arc<AtomicU32>,
    session: Arc<Mutex<Option<Session>>>,
    is_running: Arc<AtomicBool>,
    notifier: Arc<dyn Notifier>,
    metrics_settings: ClusterMetricsSettings,
}

impl KubeWatcher {
    /// Create a new `KubeWatcher` using an externally-constructed sender half
    /// of a `crossbeam` channel.  The caller retains the receiver:
    ///
    /// ```rust,ignore
    /// let (tx, rx) = crossbeam_channel::unbounded();
    /// let watcher = KubeWatcher::new(tx);
    /// ```
    pub fn new(sender_channel: crossbeam_channel::Sender<Watched>) -> Self {
        KubeWatcher {
            source: Arc::new(RwLock::new(None)),
            client: Arc::new(RwLock::new(None)),
            health: HealthReporter::new(),
            health_settings: HealthSettings::default(),
            sender_channel,
            fanout_senders: Arc::new(RwLock::new(Vec::new())),
            info: Arc::new(RwLock::new(ClusterInfo::default())),
            cluster_id: Arc::new(AtomicU32::new(0)),
            session: Arc::new(Mutex::new(None)),
            is_running: Arc::new(AtomicBool::new(false)),
            notifier: global_notifier(),
            metrics_settings: ClusterMetricsSettings::default(),
        }
    }

    /// Route metrics messages somewhere other than the global channel.
    pub fn with_notifier(mut self, notifier: Arc<dyn Notifier>) -> Self {
        self.notifier = notifier;
        self
    }

    /// Override the metrics monitor timing (tests).
    pub fn with_metrics_settings(mut self, settings: ClusterMetricsSettings) -> Self {
        self.metrics_settings = settings;
        self
    }

    /// Override the health pinger timing (tests).
    pub fn with_health_settings(mut self, settings: HealthSettings) -> Self {
        self.health_settings = settings;
        self
    }

    /// This cluster's API client.  `None` before `connect_*` succeeds,
    /// after [`Self::stop`], and for [`Self::connect_with`] sources.
    pub fn client(&self) -> Option<Client> {
        self.client.read().ok()?.clone()
    }

    fn set_client(&self, client: Option<Client>) {
        match self.client.write() {
            Ok(mut guard) => *guard = client,
            Err(e) => warn!("Failed to update kube client: {}", e),
        }
    }

    /// Latest connection health.  [`LinkState::Connecting`] until the first
    /// ping after [`Self::begin_watching`].
    ///
    /// [`LinkState::Connecting`]: crate::k8s::LinkState::Connecting
    pub fn health(&self) -> Health {
        self.health.current()
    }

    /// A receiver that sees every health change, for async consumers.
    pub fn subscribe_health(&self) -> watch::Receiver<Health> {
        self.health.subscribe()
    }

    /// Register a secondary channel that will receive a clone of every
    /// [`Watched`] event alongside the primary UI channel.
    ///
    /// Must be called **before** [`KubeWatcher::begin_watching`] so the
    /// background task captures the sender at spawn time.
    pub fn create_fanout_channel(&self) -> crossbeam_channel::Receiver<Watched> {
        let (tx, rx) = unbounded::<Watched>();
        if let Ok(mut guard) = self.fanout_senders.write() {
            guard.push(tx);
        }
        rx
    }

    /// Fan-out channel consumed by the DB adapter thread.
    /// Alias for [`KubeWatcher::create_fanout_channel`].
    #[deprecated(note = "use create_fanout_channel")]
    pub fn create_db_channel(&self) -> crossbeam_channel::Receiver<Watched> {
        self.create_fanout_channel()
    }

    fn fanout_snapshot(&self) -> Vec<crossbeam_channel::Sender<Watched>> {
        self.fanout_senders
            .read()
            .map(|g| g.clone())
            .unwrap_or_default()
    }

    /// Get the current cluster name (display name from kubeconfig context).
    pub fn get_cluster_name(&self) -> Option<String> {
        self.info.read().ok()?.name.clone()
    }

    /// Get the current namespace.
    pub fn get_namespace(&self) -> Option<String> {
        self.info.read().ok()?.namespace.clone()
    }

    /// API server version (`v1.32.2`) read while connecting, if any.
    pub fn get_server_version(&self) -> Option<String> {
        self.info.read().ok()?.version.clone()
    }

    /// Get the kube-system namespace UID — stable, unique per cluster.
    pub fn get_cluster_uuid(&self) -> Option<String> {
        self.info.read().ok()?.uuid.clone()
    }

    /// Set the local integer cluster ID (from the `clusters` DB table).
    /// Call this after upserting the cluster UUID into the database, before
    /// [`KubeWatcher::begin_watching`], so every KRI carries the correct ID.
    pub fn set_cluster_id(&self, id: u32) {
        self.cluster_id.store(id, Ordering::SeqCst);
    }

    /// Return a clone of the cluster-id `Arc` for sharing with the DB adapter.
    pub fn get_cluster_id_arc(&self) -> Arc<AtomicU32> {
        self.cluster_id.clone()
    }

    /// `true` while a watch session is live.  `false` after
    /// [`KubeWatcher::stop`], and also once the primary receiver has been
    /// dropped and the watch task has ended.
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
            && self
                .session
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
                .is_some_and(|s| !s.watch.is_finished())
    }

    // ── Phase 1: connect ──────────────────────────────────────────────────

    /// Phase 1 of the two-phase connect flow, against the cluster the local
    /// kubeconfig points at.  Does **not** start any watchers.
    ///
    /// After `connect()` returns:
    /// 1. Upsert the cluster UUID into the `clusters` DB table.
    /// 2. Call [`Self::set_cluster_id`] with the resulting integer ID.
    /// 3. Call [`Self::begin_watching`] to start event flow.
    pub async fn connect(&self) -> Result<(), ConnectError> {
        if self.is_running() {
            debug!("KubeWatcher is already running");
            return Ok(());
        }

        info!("Inferring Kubernetes configuration...");
        let config = Config::infer().await?;
        info!(cluster_url = %config.cluster_url, "Configuration inferred");
        let client = Client::try_from(config.clone()).map_err(ConnectError::Client)?;

        // The kubeconfig's current context is the best display name.
        let name = Kubeconfig::read()
            .ok()
            .and_then(|kc| {
                kc.current_context
                    .clone()
                    .or_else(|| kc.contexts.first().map(|c| c.name.clone()))
            })
            .unwrap_or_else(|| config.cluster_url.to_string());

        self.set_client(Some(client.clone()));
        self.connect_with(
            Arc::new(KubeClusterSource::new(client)),
            ClusterInfo {
                name: Some(name),
                namespace: Some(config.default_namespace.clone()),
                uuid: None,
                version: None,
            },
        )
        .await;
        Ok(())
    }

    /// Phase 1 for a context picked from [`crate::k8s::kubeconfig::discover`]:
    /// `config` comes from [`crate::k8s::kubeconfig::Discovery::client_config`].
    /// Reports each step through `progress` as it starts, and fails with the
    /// step's own error (plugin not found, unreachable, unauthorized) so the
    /// checklist can show it.  [`ConnectStep::Watch`] is left to the caller,
    /// which reports it around [`Self::begin_watching`].
    pub async fn connect_context(
        &self,
        config: Config,
        name: String,
        auth: &AuthKind,
        exec: &ExecEnv,
        progress: &(dyn Fn(ConnectStep) + Send + Sync),
    ) -> Result<(), ConnectError> {
        if self.is_running() {
            debug!("KubeWatcher is already running");
            return Ok(());
        }
        progress(ConnectStep::ReadConfig);
        if let AuthKind::Exec { command } = auth {
            exec.resolve(command).map_err(ProbeError::from)?;
        }
        let namespace = config.default_namespace.clone();
        let (config, timeout) = for_watching(config);
        let client = Client::try_from(config).map_err(ConnectError::Client)?;

        progress(ConnectStep::ReachApi);
        let version = tokio::time::timeout(timeout, detached(client.apiserver_version()))
            .await
            .map_err(|_| ProbeError::Timeout(timeout))?
            .map_err(classify_probe_error)?
            .git_version;

        // /version is often open to anonymous users; a one-item namespace
        // list proves the credentials work.  403 means authenticated but not
        // allowed, which the watch reports per kind.
        progress(ConnectStep::Authenticate);
        let namespaces: kube::Api<Namespace> = kube::Api::all(client.clone());
        match tokio::time::timeout(
            timeout,
            detached(namespaces.list_metadata(&kube::api::ListParams::default().limit(1))),
        )
        .await
        {
            Err(_) => return Err(ProbeError::Timeout(timeout).into()),
            Ok(Err(kube::Error::Api(s))) if s.code == 403 => {}
            Ok(Err(e)) => return Err(classify_probe_error(e).into()),
            Ok(Ok(_)) => {}
        }

        progress(ConnectStep::Discover);
        self.set_client(Some(client.clone()));
        self.connect_with(
            Arc::new(KubeClusterSource::new(client)),
            ClusterInfo {
                name: Some(name),
                namespace: Some(namespace),
                uuid: None,
                version: Some(version),
            },
        )
        .await;
        Ok(())
    }

    /// Phase 1 against an arbitrary [`ClusterSource`] (tests, alternative
    /// transports).  Reads the cluster UUID from the source and stores
    /// `info` for the getters; does not start any watchers.
    pub async fn connect_with(&self, source: Arc<dyn ClusterSource>, mut info: ClusterInfo) {
        if info.uuid.is_none() {
            info.uuid = detached(source.cluster_uuid()).await;
        }
        if let Ok(mut guard) = self.source.write() {
            *guard = Some(source);
        }
        if let Ok(mut guard) = self.info.write() {
            *guard = info.clone();
        }
        info!(name = ?info.name, uuid = ?info.uuid, namespace = ?info.namespace, "Connected to cluster");
    }

    // ── Phase 2: watch ────────────────────────────────────────────────────

    /// Phase 2 of the two-phase connect flow.
    ///
    /// Starts the reflectors and returns once the initial sync has completed
    /// and the first [`Watched::NamespaceList`] and
    /// [`Watched::FullResourceList`] have been sent, so the caller can rely on
    /// consumers having a complete snapshot.  Fails fast if the initial list
    /// of any kind fails (RBAC, unreachable server).
    pub async fn begin_watching(&self) -> Result<(), WatchError> {
        let source = self
            .source
            .read()
            .ok()
            .and_then(|g| g.clone())
            .ok_or(WatchError::NotConnected)?;

        // A session whose watch task has ended (the primary receiver was
        // dropped) is over: take it down and start afresh.
        let ended = {
            let mut session = self.session.lock().unwrap_or_else(|p| p.into_inner());
            match &*session {
                Some(s) if s.watch.is_finished() => session.take(),
                Some(_) => return Err(WatchError::AlreadyWatching),
                None => None,
            }
        };
        if let Some(ended) = ended {
            Self::teardown(ended).await;
        }

        info!("Starting KubeWatcher...");
        let cancel = CancellationToken::new();
        let (ready_tx, ready_rx) = oneshot::channel();
        // Root spans: they live as long as the watch, so they must not keep
        // the caller's (short) connect span open.
        let cluster_id = self.cluster_id.load(Ordering::SeqCst);

        // Held until the session is stored, so two concurrent calls can't
        // both pass the check and spawn.  Nothing in this block awaits.
        {
            let mut session = self.session.lock().unwrap_or_else(|p| p.into_inner());
            if session.is_some() {
                return Err(WatchError::AlreadyWatching);
            }

            let watch_task = tokio::spawn(
                run_watch(
                    source.clone(),
                    self.sender_channel.clone(),
                    self.fanout_snapshot(),
                    self.cluster_id.clone(),
                    self.health.clone(),
                    cancel.clone(),
                    ready_tx,
                )
                .instrument(tracing::info_span!(parent: None, "kube_watch", cluster_id)),
            );

            let health_task = tokio::spawn(
                run_health_monitor(
                    source.clone(),
                    self.health.clone(),
                    cancel.child_token(),
                    self.health_settings,
                )
                .instrument(tracing::info_span!(parent: None, "kube_health", cluster_id)),
            );

            let mut tasks = vec![health_task];
            if let Some(metrics) = source.metrics() {
                info!("Starting cluster metrics monitoring...");
                let monitor = spawn_cluster_metrics_monitor(
                    &tokio::runtime::Handle::current(),
                    metrics,
                    self.notifier.clone(),
                    cluster_id,
                    cancel.child_token(),
                    self.metrics_settings,
                );
                tasks.push(monitor.into_join());
            }

            *session = Some(Session {
                cancel,
                watch: watch_task,
                tasks,
            });
        }
        self.is_running.store(true, Ordering::SeqCst);

        match ready_rx.await {
            Ok(Ok(())) => {
                info!("KubeWatcher initial sync complete");
                Ok(())
            }
            Ok(Err(e)) => {
                error!(error = %e, "KubeWatcher initial sync failed");
                self.stop().await;
                Err(e)
            }
            Err(_) => {
                self.stop().await;
                Err(WatchError::Aborted)
            }
        }
    }

    /// Cancel a session's tasks and give them a moment to exit.
    async fn teardown(session: Session) {
        session.cancel.cancel();
        for task in std::iter::once(session.watch).chain(session.tasks) {
            if tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .is_err()
            {
                warn!("A KubeWatcher task did not exit within 2 s of cancellation");
            }
        }
    }

    /// Convenience wrapper: calls [`Self::connect`] then [`Self::begin_watching`].
    ///
    /// Prefer the two phases individually when you need to upsert the cluster
    /// UUID into the database and call [`Self::set_cluster_id`] in between.
    pub async fn start(&self) -> anyhow::Result<()> {
        self.connect().await?;
        self.begin_watching().await?;
        Ok(())
    }

    /// Stop the watch session: cancels every background task, waits briefly
    /// for them to exit, and clears this watcher's client, health and the
    /// cluster identity (name, UUID, cluster ID).  Other watchers are not
    /// affected.  Safe to call twice; `connect_*`
    /// and [`Self::begin_watching`] work again afterwards.
    pub async fn stop(&self) {
        debug!("Stopping KubeWatcher...");
        self.is_running.store(false, Ordering::SeqCst);
        let session = self
            .session
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(session) = session {
            Self::teardown(session).await;
        }
        self.set_client(None);
        self.health.reset();
        if let Ok(mut guard) = self.source.write() {
            *guard = None;
        }
        // Forget the cluster's identity so nothing reads the old UUID/ID
        // while the next connect is in progress.
        if let Ok(mut guard) = self.info.write() {
            *guard = ClusterInfo::default();
        }
        self.cluster_id.store(0, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// Watch task
// ---------------------------------------------------------------------------

/// Which kinds have completed their initial list.
#[derive(Default)]
struct SyncState {
    pods: bool,
    deployments: bool,
    services: bool,
    ingresses: bool,
    namespaces: bool,
}

impl SyncState {
    fn all(&self) -> bool {
        self.pods && self.deployments && self.services && self.ingresses && self.namespaces
    }
}

/// One event from any of the five reflectors.
enum Ev {
    Pod(Result<Event<Pod>, watcher::Error>),
    Deployment(Result<Event<Deployment>, watcher::Error>),
    Service(Result<Event<Service>, watcher::Error>),
    Ingress(Result<Event<Ingress>, watcher::Error>),
    Namespace(Result<Event<Namespace>, watcher::Error>),
}

struct Stores {
    pods: Store<Pod>,
    deployments: Store<Deployment>,
    services: Store<Service>,
    ingresses: Store<Ingress>,
    namespaces: Store<Namespace>,
}

macro_rules! shared_store {
    ($kind:ty, $field:ident) => {
        impl SharedStore<$kind> for Stores {
            fn store(&self) -> &Store<$kind> {
                &self.$field
            }
        }
    };
}
shared_store!(Pod, pods);
shared_store!(Deployment, deployments);
shared_store!(Service, services);
shared_store!(Ingress, ingresses);

/// Looks a just-applied object up in its reflector store, so the event
/// carries the store's `Arc` rather than a second copy.
trait SharedStore<K: kube::Resource<DynamicType = ()>> {
    fn store(&self) -> &Store<K>;
}

impl Stores {
    /// The reflector's copy of `obj` (falls back to `obj` itself if the
    /// store doesn't have it, which only happens in tests that bypass it).
    fn shared<K>(&self, obj: &K) -> Arc<K>
    where
        K: kube::Resource<DynamicType = ()> + Clone + 'static,
        Self: SharedStore<K>,
    {
        self.store()
            .get(&ObjectRef::from_obj(obj))
            .unwrap_or_else(|| Arc::new(obj.clone()))
    }

    fn full_list(&self, cluster_id: u32) -> Watched {
        let mut resources = Vec::with_capacity(
            self.pods.len() + self.deployments.len() + self.services.len() + self.ingresses.len(),
        );
        resources.extend(self.pods.state().into_iter().map(KubeResource::Pod));
        resources.extend(
            self.deployments
                .state()
                .into_iter()
                .map(KubeResource::Deployment),
        );
        resources.extend(self.services.state().into_iter().map(KubeResource::Service));
        resources.extend(
            self.ingresses
                .state()
                .into_iter()
                .map(KubeResource::Ingress),
        );
        Watched::FullResourceList(WatchedResources {
            resources,
            cluster_id,
        })
    }

    fn namespace_list(&self) -> Watched {
        let mut infos: Vec<NamespaceInfo> = self
            .namespaces
            .state()
            .iter()
            .map(|ns| NamespaceInfo::from_namespace(ns))
            .collect();
        infos.sort_by(|a, b| a.name.cmp(&b.name));
        Watched::NamespaceList(infos)
    }
}

/// Where events go: the primary channel plus fan-outs.  Returns `false`
/// when the primary receiver is gone.
struct Outlet {
    tx: crossbeam_channel::Sender<Watched>,
    fan: Vec<crossbeam_channel::Sender<Watched>>,
}

impl Outlet {
    fn send(&self, w: Watched) -> bool {
        for fan_tx in &self.fan {
            if let Err(e) = fan_tx.send(w.clone()) {
                debug!("Fan-out channel closed, skipping: {}", e);
            }
        }
        if let Err(e) = self.tx.send(w) {
            error!("UI channel closed, stopping watcher: {}", e);
            return false;
        }
        true
    }
}

async fn run_watch(
    source: Arc<dyn ClusterSource>,
    tx: crossbeam_channel::Sender<Watched>,
    fan: Vec<crossbeam_channel::Sender<Watched>>,
    cluster_id: Arc<AtomicU32>,
    health: HealthReporter,
    cancel: CancellationToken,
    ready: oneshot::Sender<Result<(), WatchError>>,
) {
    let (pod_store, pod_writer) = reflector::store::<Pod>();
    let (dep_store, dep_writer) = reflector::store::<Deployment>();
    let (svc_store, svc_writer) = reflector::store::<Service>();
    let (ing_store, ing_writer) = reflector::store::<Ingress>();
    let (ns_store, ns_writer) = reflector::store::<Namespace>();
    let stores = Stores {
        pods: pod_store,
        deployments: dep_store,
        services: svc_store,
        ingresses: ing_store,
        namespaces: ns_store,
    };

    let mut merged = stream::select_all(vec![
        reflector::reflector(pod_writer, source.watch_pods())
            .map(Ev::Pod)
            .boxed(),
        reflector::reflector(dep_writer, source.watch_deployments())
            .map(Ev::Deployment)
            .boxed(),
        reflector::reflector(svc_writer, source.watch_services())
            .map(Ev::Service)
            .boxed(),
        reflector::reflector(ing_writer, source.watch_ingresses())
            .map(Ev::Ingress)
            .boxed(),
        reflector::reflector(ns_writer, source.watch_namespaces())
            .map(Ev::Namespace)
            .boxed(),
    ]);

    let outlet = Outlet { tx, fan };
    let mut sync = SyncState::default();
    let mut initial_sent = false;
    let mut ready = Some(ready);
    // Incremental events that arrive while other kinds are still listing.
    let mut pending: Vec<Watched> = Vec::new();
    let mut consecutive_errors: u32 = 0;

    loop {
        let ev = tokio::select! {
            _ = cancel.cancelled() => {
                debug!("KubeWatcher cancelled, exiting watch loop");
                break;
            }
            ev = merged.next() => match ev {
                Some(ev) => ev,
                None => {
                    info!("Watch streams ended");
                    break;
                }
            },
        };

        // Translate the typed event into: (kind synced?, incremental, error).
        let mut send_full = false;
        let mut send_namespaces = false;
        let mut incremental: Option<Watched> = None;
        let mut err: Option<watcher::Error> = None;

        macro_rules! handle {
            ($res:expr, $flag:ident, $apply:expr, $delete:expr) => {
                match $res {
                    Ok(Event::Init) | Ok(Event::InitApply(_)) => {}
                    Ok(Event::InitDone) => {
                        let first = !sync.$flag;
                        sync.$flag = true;
                        if !first {
                            // Re-list after a watch expiry: snapshot replaces.
                            send_full = true;
                        }
                    }
                    // The reflector already holds this object; share its
                    // Arc instead of sending a second copy.
                    Ok(Event::Apply(o)) => incremental = Some($apply(stores.shared(&o))),
                    Ok(Event::Delete(o)) => incremental = Some($delete(Arc::new(o))),
                    Err(e) => err = Some(e),
                }
            };
        }

        match ev {
            Ev::Pod(r) => handle!(r, pods, Watched::Pod, Watched::DeletedPod),
            Ev::Deployment(r) => {
                handle!(
                    r,
                    deployments,
                    Watched::Deployment,
                    Watched::DeletedDeployment
                )
            }
            Ev::Service(r) => handle!(r, services, Watched::Service, Watched::DeletedService),
            Ev::Ingress(r) => handle!(r, ingresses, Watched::Ingress, Watched::DeletedIngress),
            Ev::Namespace(r) => match r {
                Ok(Event::Init) | Ok(Event::InitApply(_)) => {}
                Ok(Event::InitDone) => {
                    sync.namespaces = true;
                    send_namespaces = true;
                }
                Ok(Event::Apply(_)) | Ok(Event::Delete(_)) => send_namespaces = true,
                Err(e) => err = Some(e),
            },
        }

        if let Some(e) = err {
            consecutive_errors = consecutive_errors.saturating_add(1);
            if !initial_sent {
                error!(error = %describe_watch(&e), "Watch failed before the initial sync completed");
                if let Some(r) = ready.take() {
                    let _ = r.send(Err(WatchError::InitialSync(e)));
                }
                break;
            }
            if watch_error_cause(&e).is_some() {
                // Let the pinger decide whether the cluster is gone.
                health.nudge();
            }
            if consecutive_errors == 1 {
                warn!(error = %describe_watch(&e), "Transient watch error — the watcher will reconnect");
            } else if consecutive_errors.is_multiple_of(10) {
                error!(error = %describe_watch(&e), consecutive_errors, "Watch still failing");
            }
            continue;
        }
        consecutive_errors = 0;

        // First full sync: namespaces first, then the snapshot, then
        // anything that arrived in the meantime.
        if !initial_sent && sync.all() {
            initial_sent = true;
            let full = stores.full_list(cluster_id.load(Ordering::SeqCst));
            if let Watched::FullResourceList(list) = &full {
                info!(
                    resources = list.resources.len(),
                    "Initial resource sync complete"
                );
            }
            if !outlet.send(stores.namespace_list()) || !outlet.send(full) {
                break;
            }
            for w in pending.drain(..) {
                if !outlet.send(w) {
                    break;
                }
            }
            if let Some(r) = ready.take() {
                let _ = r.send(Ok(()));
            }
            continue;
        }

        if !initial_sent {
            if let Some(w) = incremental {
                pending.push(w);
            }
            continue;
        }

        if send_namespaces && !outlet.send(stores.namespace_list()) {
            break;
        }
        if send_full {
            info!("A watch re-listed; sending a fresh full resource list");
            if !outlet.send(stores.full_list(cluster_id.load(Ordering::SeqCst))) {
                break;
            }
        }
        if let Some(w) = incremental {
            if !outlet.send(w) {
                break;
            }
        }
    }

    if let Some(r) = ready.take() {
        let _ = r.send(Err(WatchError::Aborted));
    }
    // Stopped, or the primary receiver went away: either way the session is
    // over, so the health pinger and metrics monitor go with it.
    cancel.cancel();
    info!("KubeWatcher background task ended");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};
    use tracing_core::span::Current;

    /// Just enough of a subscriber to see when spans close.
    #[derive(Default)]
    struct CloseTracker {
        next: std::sync::atomic::AtomicU64,
        /// id → (refs, metadata)
        spans: Mutex<HashMap<u64, (usize, &'static Metadata<'static>)>>,
        stack: Mutex<Vec<Id>>,
        closed: Mutex<HashSet<&'static str>>,
    }

    impl Subscriber for CloseTracker {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, attrs: &Attributes<'_>) -> Id {
            let id = self.next.fetch_add(1, Ordering::SeqCst) + 1;
            self.spans.lock().unwrap().insert(id, (1, attrs.metadata()));
            Id::from_u64(id)
        }
        fn record(&self, _: &Id, _: &Record<'_>) {}
        fn record_follows_from(&self, _: &Id, _: &Id) {}
        fn event(&self, _: &Event<'_>) {}
        fn enter(&self, id: &Id) {
            self.stack.lock().unwrap().push(id.clone());
        }
        fn exit(&self, _: &Id) {
            self.stack.lock().unwrap().pop();
        }
        fn current_span(&self) -> Current {
            let top = self.stack.lock().unwrap().last().cloned();
            match top {
                Some(id) => {
                    let meta = self.spans.lock().unwrap()[&id.into_u64()].1;
                    Current::new(id, meta)
                }
                None => Current::none(),
            }
        }
        fn clone_span(&self, id: &Id) -> Id {
            self.spans
                .lock()
                .unwrap()
                .get_mut(&id.into_u64())
                .unwrap()
                .0 += 1;
            id.clone()
        }
        fn try_close(&self, id: Id) -> bool {
            let mut spans = self.spans.lock().unwrap();
            let entry = spans.get_mut(&id.into_u64()).unwrap();
            entry.0 -= 1;
            if entry.0 == 0 {
                self.closed.lock().unwrap().insert(entry.1.name());
                return true;
            }
            false
        }
    }

    /// A keep-alive HTTP server answering every request with a `/version`
    /// body, so the client opens and pools a real connection.
    async fn version_server() -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let body = r#"{"major":"1","minor":"32","gitVersion":"v1.32.0","gitCommit":"","gitTreeState":"","buildDate":"","goVersion":"","compiler":"","platform":""}"#;
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        if sock.write_all(resp.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_client_does_not_keep_the_connect_span_open() {
        let tracker = Arc::new(CloseTracker::default());
        let url = version_server().await;
        let config = Config::new(url.parse().unwrap());
        let dispatch = tracing::Dispatch::from(tracker.clone());
        let client = {
            let _default = tracing::dispatcher::set_default(&dispatch);
            let span = tracing::info_span!("cluster.connect");
            async {
                let client = Client::try_from(config).unwrap();
                detached(client.apiserver_version()).await.expect("version");
                client
            }
            .instrument(span)
            .await
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            tracker.closed.lock().unwrap().contains("cluster.connect"),
            "the span must close although the client (and its pooled connection) is alive"
        );
        drop(client);
    }

    #[test]
    fn watching_client_outlives_the_server_watch_timeout() {
        let mut config = Config::new("https://127.0.0.1:6443".parse().unwrap());
        config.read_timeout = Some(Duration::from_secs(10));
        let (watch, step) = for_watching(config);
        assert_eq!(
            step,
            Duration::from_secs(10),
            "connect steps keep the short timeout"
        );
        assert_eq!(watch.read_timeout, Some(WATCH_READ_TIMEOUT));
        assert!(WATCH_READ_TIMEOUT > Duration::from_secs(290));

        let mut no_timeout = Config::new("https://127.0.0.1:6443".parse().unwrap());
        no_timeout.read_timeout = None;
        assert_eq!(for_watching(no_timeout).1, Duration::from_secs(10));
    }
}
