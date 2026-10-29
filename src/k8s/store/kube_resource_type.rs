/// Well-known resource kinds, derived from a KRI's `kind` string.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KubeResourceType {
    Pod,
    Service,
    Deployment,
    StatefulSet,
    DaemonSet,
    ReplicaSet,
    Job,
    CronJob,
    ConfigMap,
    Secret,
    Namespace,
    Node,
    PersistentVolume,
    PersistentVolumeClaim,
    Ingress,
    NetworkPolicy,
    ResourceQuota,
    LimitRange,
    Unknown(String), // For any resource type not explicitly defined
}
