//! What a pod asks of its node: the scheduler's view of requests and limits.
//!
//! A pod's effective request is not the sum of its containers.  The
//! scheduler ([`k8s.io/component-helpers/resource`]) takes
//!
//! ```text
//! max( sum(app containers) + sum(sidecars),
//!      max over init containers of (init + sidecars started before it) )
//! + pod overhead
//! ```
//!
//! where a sidecar is an init container with `restartPolicy: Always`, and a
//! pod-level `spec.resources` (PodLevelResources) replaces the container
//! formula for each resource it sets.  While an in-place resize is pending
//! the kubelet's `allocatedResources` can exceed the spec, and the scheduler
//! counts the larger of the two.  Getting this wrong makes "requested 58 %"
//! disagree with `kubectl describe node`, so it is done once, here.
//!
//! [`k8s.io/component-helpers/resource`]: https://github.com/kubernetes/component-helpers/blob/master/resource/helpers.go

use std::collections::BTreeMap;
use std::ops::{Add, AddAssign, Sub};

use k8s_openapi::api::core::v1::{Container, ContainerStatus, Pod};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

use crate::k8s::metrics::{cpu_millicores, memory_bytes};

/// CPU and memory, the two resources the health view tracks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct Resources {
    pub cpu_millicores: i64,
    pub memory_bytes: i64,
}

impl Resources {
    pub const ZERO: Resources = Resources {
        cpu_millicores: 0,
        memory_bytes: 0,
    };

    pub const fn new(cpu_millicores: i64, memory_bytes: i64) -> Self {
        Self {
            cpu_millicores,
            memory_bytes,
        }
    }

    /// Component-wise maximum.
    pub fn max(self, other: Self) -> Self {
        Self {
            cpu_millicores: self.cpu_millicores.max(other.cpu_millicores),
            memory_bytes: self.memory_bytes.max(other.memory_bytes),
        }
    }

    /// `self - other`, floored at zero per component (free capacity can't be
    /// negative even when a node is overcommitted).
    pub fn saturating_sub(self, other: Self) -> Self {
        Self {
            cpu_millicores: (self.cpu_millicores - other.cpu_millicores).max(0),
            memory_bytes: (self.memory_bytes - other.memory_bytes).max(0),
        }
    }

    /// Read `cpu` and `memory` from a resource list.  Unparseable values
    /// count as zero: the API server validates quantities on write, so this
    /// only happens with a hand-made object.
    pub(crate) fn from_list(list: Option<&BTreeMap<String, Quantity>>) -> Self {
        Self {
            cpu_millicores: cpu_of(list).unwrap_or(0),
            memory_bytes: memory_of(list).unwrap_or(0),
        }
    }
}

impl Add for Resources {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Self {
            cpu_millicores: self.cpu_millicores + rhs.cpu_millicores,
            memory_bytes: self.memory_bytes + rhs.memory_bytes,
        }
    }
}

impl AddAssign for Resources {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}

impl Sub for Resources {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self {
        Self {
            cpu_millicores: self.cpu_millicores - rhs.cpu_millicores,
            memory_bytes: self.memory_bytes - rhs.memory_bytes,
        }
    }
}

impl std::iter::Sum for Resources {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::ZERO, Add::add)
    }
}

fn cpu_of(list: Option<&BTreeMap<String, Quantity>>) -> Option<i64> {
    list?
        .get("cpu")
        .and_then(|q| cpu_millicores(q, "resources.cpu").ok())
}

fn memory_of(list: Option<&BTreeMap<String, Quantity>>) -> Option<i64> {
    list?
        .get("memory")
        .and_then(|q| memory_bytes(q, "resources.memory").ok())
}

/// A pod's effective requests and limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PodResources {
    pub requests: Resources,
    /// Limits that are set.  A container without a limit contributes
    /// nothing (it is unbounded), which is also how `kubectl describe node`
    /// sums them.
    pub limits: Resources,
}

/// Which side of `resources` to read.
#[derive(Clone, Copy)]
enum Side {
    Requests,
    Limits,
}

fn container_side(c: &Container, side: Side) -> Resources {
    let r = c.resources.as_ref();
    let list = match side {
        Side::Requests => r.and_then(|r| r.requests.as_ref()),
        Side::Limits => r.and_then(|r| r.limits.as_ref()),
    };
    let mut out = Resources::from_list(list);
    // A container that sets a limit but no request gets request = limit
    // from the API server's defaulting; objects seen through the API have
    // it filled in, but be equally correct for ones that don't.
    if matches!(side, Side::Requests) {
        let limits = r.and_then(|r| r.limits.as_ref());
        if list.and_then(|l| l.get("cpu")).is_none() {
            out.cpu_millicores = cpu_of(limits).unwrap_or(0);
        }
        if list.and_then(|l| l.get("memory")).is_none() {
            out.memory_bytes = memory_of(limits).unwrap_or(0);
        }
    }
    out
}

/// The larger of the spec and the kubelet's allocation during a resize.
fn with_allocated(spec: Resources, status: Option<&ContainerStatus>) -> Resources {
    match status.and_then(|s| s.allocated_resources.as_ref()) {
        Some(allocated) => spec.max(Resources::from_list(Some(allocated))),
        None => spec,
    }
}

fn is_sidecar(c: &Container) -> bool {
    c.restart_policy.as_deref() == Some("Always")
}

fn status_of<'a>(
    statuses: Option<&'a Vec<ContainerStatus>>,
    name: &str,
) -> Option<&'a ContainerStatus> {
    statuses.and_then(|s| s.iter().find(|cs| cs.name == name))
}

/// The container formula for one side (see the module docs).
fn containers_side(pod: &Pod, side: Side) -> Resources {
    let Some(spec) = pod.spec.as_ref() else {
        return Resources::ZERO;
    };
    let status = pod.status.as_ref();
    let app_statuses = status.and_then(|s| s.container_statuses.as_ref());
    let init_statuses = status.and_then(|s| s.init_container_statuses.as_ref());

    let apps: Resources = spec
        .containers
        .iter()
        .map(|c| {
            let r = container_side(c, side);
            match side {
                Side::Requests => with_allocated(r, status_of(app_statuses, &c.name)),
                Side::Limits => r,
            }
        })
        .sum();

    let mut sidecars = Resources::ZERO;
    let mut init_peak = Resources::ZERO;
    for c in spec.init_containers.iter().flatten() {
        let mut r = container_side(c, side);
        if matches!(side, Side::Requests) {
            r = with_allocated(r, status_of(init_statuses, &c.name));
        }
        let during = if is_sidecar(c) {
            sidecars += r;
            sidecars
        } else {
            r + sidecars
        };
        init_peak = init_peak.max(during);
    }
    (apps + sidecars).max(init_peak)
}

/// The pod's effective requests and limits, as the scheduler counts them.
pub fn pod_resources(pod: &Pod) -> PodResources {
    let Some(spec) = pod.spec.as_ref() else {
        return PodResources::default();
    };
    let mut requests = containers_side(pod, Side::Requests);
    let mut limits = containers_side(pod, Side::Limits);

    // Pod-level resources win per resource they set.
    if let Some(pod_level) = spec.resources.as_ref() {
        let req = pod_level.requests.as_ref();
        let lim = pod_level.limits.as_ref();
        // Same defaulting as containers: a pod-level limit without a
        // request is also the request.
        if let Some(cpu) = cpu_of(req).or_else(|| cpu_of(lim)) {
            requests.cpu_millicores = cpu;
        }
        if let Some(mem) = memory_of(req).or_else(|| memory_of(lim)) {
            requests.memory_bytes = mem;
        }
        if let Some(cpu) = cpu_of(lim) {
            limits.cpu_millicores = cpu;
        }
        if let Some(mem) = memory_of(lim) {
            limits.memory_bytes = mem;
        }
    }

    // Overhead (RuntimeClass) is added to both, but only to limits that
    // are actually bounded.
    let overhead = Resources::from_list(spec.overhead.as_ref());
    requests += overhead;
    if limits.cpu_millicores > 0 {
        limits.cpu_millicores += overhead.cpu_millicores;
    }
    if limits.memory_bytes > 0 {
        limits.memory_bytes += overhead.memory_bytes;
    }
    PodResources { requests, limits }
}

/// The pod's phase, for the counts the health view shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

pub(crate) fn phase(pod: &Pod) -> Phase {
    match pod.status.as_ref().and_then(|s| s.phase.as_deref()) {
        Some("Pending") => Phase::Pending,
        Some("Running") => Phase::Running,
        Some("Succeeded") => Phase::Succeeded,
        Some("Failed") => Phase::Failed,
        _ => Phase::Unknown,
    }
}

/// The node the pod is bound to, if it still holds resources there.
/// Finished pods (`Succeeded` / `Failed`) don't, and neither does the
/// scheduler count them.
pub(crate) fn bound_node(pod: &Pod) -> Option<&str> {
    if matches!(phase(pod), Phase::Succeeded | Phase::Failed) {
        return None;
    }
    pod.spec
        .as_ref()?
        .node_name
        .as_deref()
        .filter(|n| !n.is_empty())
}

/// Why the scheduler could not place the pod, when it says it can't:
/// the `PodScheduled=False, reason=Unschedulable` condition's message
/// (`0/5 nodes are available: 3 Insufficient memory, …`).  Gated pods
/// (`SchedulingGated`) are waiting on purpose and are not reported.
pub(crate) fn unschedulable_reason(pod: &Pod) -> Option<&str> {
    if phase(pod) != Phase::Pending || bound_node(pod).is_some() {
        return None;
    }
    pod.status
        .as_ref()?
        .conditions
        .as_ref()?
        .iter()
        .find(|c| c.type_ == "PodScheduled" && c.status == "False")
        .filter(|c| c.reason.as_deref() == Some("Unschedulable"))
        .map(|c| c.message.as_deref().unwrap_or(""))
}

/// `true` when any container is waiting in `CrashLoopBackOff`.
pub(crate) fn is_crash_looping(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .into_iter()
        .flat_map(|s| {
            s.container_statuses
                .iter()
                .flatten()
                .chain(s.init_container_statuses.iter().flatten())
        })
        .any(|cs| {
            cs.state
                .as_ref()
                .and_then(|st| st.waiting.as_ref())
                .and_then(|w| w.reason.as_deref())
                == Some("CrashLoopBackOff")
        })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        ContainerState, ContainerStateWaiting, PodCondition, PodSpec, PodStatus,
        ResourceRequirements,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    pub(crate) fn list(pairs: &[(&str, &str)]) -> BTreeMap<String, Quantity> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), Quantity(v.to_string())))
            .collect()
    }

    pub(crate) fn container(name: &str, req: &[(&str, &str)], lim: &[(&str, &str)]) -> Container {
        Container {
            name: name.into(),
            resources: Some(ResourceRequirements {
                requests: (!req.is_empty()).then(|| list(req)),
                limits: (!lim.is_empty()).then(|| list(lim)),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn sidecar(name: &str, req: &[(&str, &str)]) -> Container {
        Container {
            restart_policy: Some("Always".into()),
            ..container(name, req, &[])
        }
    }

    pub(crate) fn pod_with(containers: Vec<Container>, init: Vec<Container>) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some("p".into()),
                namespace: Some("ns".into()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers,
                init_containers: (!init.is_empty()).then_some(init),
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some("Running".into()),
                ..Default::default()
            }),
        }
    }

    const MI: i64 = 1 << 20;

    #[test]
    fn app_containers_sum() {
        let p = pod_with(
            vec![
                container("a", &[("cpu", "100m"), ("memory", "64Mi")], &[]),
                container("b", &[("cpu", "250m"), ("memory", "128Mi")], &[]),
            ],
            vec![],
        );
        assert_eq!(pod_resources(&p).requests, Resources::new(350, 192 * MI));
    }

    #[test]
    fn a_big_init_container_dominates() {
        let p = pod_with(
            vec![container(
                "app",
                &[("cpu", "100m"), ("memory", "64Mi")],
                &[],
            )],
            vec![container(
                "migrate",
                &[("cpu", "1"), ("memory", "32Mi")],
                &[],
            )],
        );
        // cpu from the init container, memory from the app.
        assert_eq!(pod_resources(&p).requests, Resources::new(1000, 64 * MI));
    }

    #[test]
    fn sidecars_add_to_the_app_and_to_later_init_containers() {
        let p = pod_with(
            vec![container("app", &[("cpu", "100m")], &[])],
            vec![
                sidecar("proxy", &[("cpu", "50m")]),
                container("init", &[("cpu", "200m")], &[]),
            ],
        );
        // init runs next to the proxy: 250m; steady state: 150m.
        assert_eq!(pod_resources(&p).requests.cpu_millicores, 250);

        // An init container *before* the sidecar doesn't overlap it.
        let p = pod_with(
            vec![container("app", &[("cpu", "100m")], &[])],
            vec![
                container("init", &[("cpu", "120m")], &[]),
                sidecar("proxy", &[("cpu", "50m")]),
            ],
        );
        assert_eq!(pod_resources(&p).requests.cpu_millicores, 150);
    }

    #[test]
    fn a_limit_without_a_request_is_the_request() {
        let p = pod_with(vec![container("a", &[], &[("memory", "1Gi")])], vec![]);
        let r = pod_resources(&p);
        assert_eq!(r.requests.memory_bytes, 1 << 30);
        assert_eq!(r.limits.memory_bytes, 1 << 30);
        assert_eq!(r.limits.cpu_millicores, 0, "no cpu limit: unbounded");
    }

    #[test]
    fn overhead_is_added_to_requests_and_bounded_limits() {
        let mut p = pod_with(
            vec![container("a", &[("cpu", "100m")], &[("cpu", "200m")])],
            vec![],
        );
        p.spec.as_mut().unwrap().overhead = Some(list(&[("cpu", "10m"), ("memory", "8Mi")]));
        let r = pod_resources(&p);
        assert_eq!(r.requests, Resources::new(110, 8 * MI));
        assert_eq!(r.limits, Resources::new(210, 0), "memory unbounded stays 0");
    }

    #[test]
    fn pod_level_resources_win_per_resource() {
        let mut p = pod_with(
            vec![container("a", &[("cpu", "100m"), ("memory", "64Mi")], &[])],
            vec![],
        );
        p.spec.as_mut().unwrap().resources = Some(ResourceRequirements {
            requests: Some(list(&[("cpu", "2")])),
            limits: Some(list(&[("memory", "1Gi")])),
            ..Default::default()
        });
        let r = pod_resources(&p);
        assert_eq!(r.requests, Resources::new(2000, 1 << 30));
        assert_eq!(r.limits, Resources::new(0, 1 << 30));
    }

    #[test]
    fn a_pending_resize_counts_the_larger_allocation() {
        let mut p = pod_with(vec![container("a", &[("memory", "64Mi")], &[])], vec![]);
        p.status.as_mut().unwrap().container_statuses = Some(vec![ContainerStatus {
            name: "a".into(),
            allocated_resources: Some(list(&[("memory", "256Mi")])),
            ..Default::default()
        }]);
        assert_eq!(pod_resources(&p).requests.memory_bytes, 256 * MI);
    }

    #[test]
    fn no_spec_is_nothing() {
        assert_eq!(pod_resources(&Pod::default()), PodResources::default());
    }

    #[test]
    fn finished_pods_hold_no_node() {
        let mut p = pod_with(vec![], vec![]);
        p.spec.as_mut().unwrap().node_name = Some("n1".into());
        assert_eq!(bound_node(&p), Some("n1"));
        p.status.as_mut().unwrap().phase = Some("Succeeded".into());
        assert_eq!(bound_node(&p), None);
        p.status.as_mut().unwrap().phase = Some("Failed".into());
        assert_eq!(bound_node(&p), None);
    }

    fn scheduled_condition(reason: &str, message: &str) -> PodCondition {
        PodCondition {
            type_: "PodScheduled".into(),
            status: "False".into(),
            reason: Some(reason.into()),
            message: Some(message.into()),
            ..Default::default()
        }
    }

    #[test]
    fn unschedulable_reads_the_scheduler_condition_and_skips_gated_pods() {
        let mut p = pod_with(vec![], vec![]);
        let st = p.status.as_mut().unwrap();
        st.phase = Some("Pending".into());
        st.conditions = Some(vec![scheduled_condition(
            "Unschedulable",
            "0/5 nodes are available: 3 Insufficient memory.",
        )]);
        assert_eq!(
            unschedulable_reason(&p),
            Some("0/5 nodes are available: 3 Insufficient memory.")
        );

        p.status.as_mut().unwrap().conditions =
            Some(vec![scheduled_condition("SchedulingGated", "gated")]);
        assert_eq!(unschedulable_reason(&p), None);

        // Pending but already bound (pulling images) is not unschedulable.
        p.status.as_mut().unwrap().conditions = Some(vec![scheduled_condition(
            "Unschedulable",
            "stale condition",
        )]);
        p.spec.as_mut().unwrap().node_name = Some("n1".into());
        assert_eq!(unschedulable_reason(&p), None);
    }

    #[test]
    fn crash_loop_is_seen_in_app_and_init_containers() {
        let waiting = |reason: &str| ContainerStatus {
            name: "c".into(),
            state: Some(ContainerState {
                waiting: Some(ContainerStateWaiting {
                    reason: Some(reason.into()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut p = pod_with(vec![], vec![]);
        assert!(!is_crash_looping(&p));
        p.status.as_mut().unwrap().init_container_statuses =
            Some(vec![waiting("CrashLoopBackOff")]);
        assert!(is_crash_looping(&p));
        p.status.as_mut().unwrap().init_container_statuses = None;
        p.status.as_mut().unwrap().container_statuses = Some(vec![waiting("ImagePullBackOff")]);
        assert!(!is_crash_looping(&p));
    }

    #[test]
    fn resource_arithmetic() {
        let a = Resources::new(100, 10);
        let b = Resources::new(30, 50);
        assert_eq!(a + b, Resources::new(130, 60));
        assert_eq!(a - b, Resources::new(70, -40));
        assert_eq!(a.saturating_sub(b), Resources::new(70, 0));
        assert_eq!(a.max(b), Resources::new(100, 50));
        assert_eq!([a, b].into_iter().sum::<Resources>(), a + b);
    }
}
