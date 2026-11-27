//! [`MetricsSource`]: the seam between the metrics monitors and the API.

use async_trait::async_trait;
use k8s_metrics::v1beta1::{NodeMetrics, PodMetrics};
use k8s_metrics::QuantityExt;
use k8s_openapi::api::core::v1::{Node, Pod};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::{Api, Client};
use tracing::debug;

use crate::k8s::cluster_health::Resources;
use crate::k8s::MetricsError;

/// Cluster-wide node capacity, summed over every node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NodeCapacity {
    pub cpu_millicores: i64,
    pub memory_bytes: i64,
}

/// Cluster-wide node usage from the metrics-server, summed over every node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NodeUsage {
    pub cpu_millicores: i64,
    pub memory_bytes: i64,
}

/// One pod's usage, summed over its containers.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct PodUsage {
    pub cpu_millicores: f32,
    pub memory_mib: f32,
}

/// One object's live usage from the metrics-server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageSample {
    /// Node name, or pod name for [`MetricsSource::pod_usages`].
    pub name: String,
    pub usage: Resources,
}

/// Everything the metrics monitors read from a cluster.
#[async_trait]
pub trait MetricsSource: Send + Sync + 'static {
    async fn node_capacity(&self) -> Result<NodeCapacity, MetricsError>;
    async fn node_usage(&self) -> Result<NodeUsage, MetricsError>;
    async fn pod_usage(&self, namespace: &str, pod: &str) -> Result<PodUsage, MetricsError>;
    /// Sum of `restartCount` over the pod's containers.
    async fn pod_restarts(&self, namespace: &str, pod: &str) -> Result<i32, MetricsError>;

    /// Usage of every node, in one request.  Sources without per-node
    /// metrics answer [`MetricsError::Unsupported`].
    async fn node_usages(&self) -> Result<Vec<UsageSample>, MetricsError> {
        Err(MetricsError::Unsupported)
    }

    /// Usage of every pod in `namespace` (summed over its containers), in
    /// one request.
    async fn pod_usages(&self, namespace: &str) -> Result<Vec<UsageSample>, MetricsError> {
        let _ = namespace;
        Err(MetricsError::Unsupported)
    }
}

/// [`MetricsSource`] backed by the core API and the metrics-server.
#[derive(Clone)]
pub struct KubeMetricsSource {
    client: Client,
}

impl KubeMetricsSource {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

/// CPU quantity (`"4"`, `"500m"`, `"412345678n"`, `"0.5"`) to millicores.
pub(crate) fn cpu_millicores(q: &Quantity, field: &'static str) -> Result<i64, MetricsError> {
    q.to_f64()
        .map(|cores| (cores * 1000.0).round() as i64)
        .map_err(|_| MetricsError::Quantity {
            field,
            raw: q.0.clone(),
        })
}

/// Memory quantity (`"16384000Ki"`, `"16Gi"`, `"17179869184"`) to bytes.
pub(crate) fn memory_bytes(q: &Quantity, field: &'static str) -> Result<i64, MetricsError> {
    q.to_memory().map_err(|_| MetricsError::Quantity {
        field,
        raw: q.0.clone(),
    })
}

#[async_trait]
impl MetricsSource for KubeMetricsSource {
    async fn node_capacity(&self) -> Result<NodeCapacity, MetricsError> {
        let nodes: Api<Node> = Api::all(self.client.clone());
        let list = nodes.list(&Default::default()).await?;
        let mut total = NodeCapacity::default();
        for node in &list.items {
            let Some(capacity) = node.status.as_ref().and_then(|s| s.capacity.as_ref()) else {
                continue;
            };
            if let Some(cpu) = capacity.get("cpu") {
                match cpu_millicores(cpu, "node.status.capacity.cpu") {
                    Ok(mc) => total.cpu_millicores += mc,
                    Err(e) => debug!(error = %e, "skipping node CPU capacity"),
                }
            }
            if let Some(mem) = capacity.get("memory") {
                match memory_bytes(mem, "node.status.capacity.memory") {
                    Ok(b) => total.memory_bytes += b,
                    Err(e) => debug!(error = %e, "skipping node memory capacity"),
                }
            }
        }
        Ok(total)
    }

    async fn node_usage(&self) -> Result<NodeUsage, MetricsError> {
        let metrics: Api<NodeMetrics> = Api::all(self.client.clone());
        let list = metrics.list(&Default::default()).await?;
        let mut total = NodeUsage::default();
        for m in &list.items {
            match cpu_millicores(&m.usage.cpu, "nodemetrics.usage.cpu") {
                Ok(mc) => total.cpu_millicores += mc,
                Err(e) => debug!(error = %e, "skipping node CPU usage"),
            }
            match memory_bytes(&m.usage.memory, "nodemetrics.usage.memory") {
                Ok(b) => total.memory_bytes += b,
                Err(e) => debug!(error = %e, "skipping node memory usage"),
            }
        }
        Ok(total)
    }

    async fn pod_usage(&self, namespace: &str, pod: &str) -> Result<PodUsage, MetricsError> {
        let api: Api<PodMetrics> = Api::namespaced(self.client.clone(), namespace);
        let pm = api.get(pod).await?;
        let mut cores = 0.0_f64;
        let mut bytes = 0_i64;
        for c in &pm.containers {
            if let Ok(cpu) = c.cpu() {
                cores += cpu;
            }
            if let Ok(mem) = c.memory() {
                bytes += mem;
            }
        }
        Ok(PodUsage {
            cpu_millicores: (cores * 1000.0) as f32,
            memory_mib: bytes as f32 / (1024.0 * 1024.0),
        })
    }

    async fn node_usages(&self) -> Result<Vec<UsageSample>, MetricsError> {
        let metrics: Api<NodeMetrics> = Api::all(self.client.clone());
        let list = metrics.list(&Default::default()).await?;
        Ok(list
            .items
            .iter()
            .filter_map(|m| {
                let name = m.metadata.name.clone()?;
                let cpu = cpu_millicores(&m.usage.cpu, "nodemetrics.usage.cpu");
                let mem = memory_bytes(&m.usage.memory, "nodemetrics.usage.memory");
                match (cpu, mem) {
                    (Ok(c), Ok(b)) => Some(UsageSample {
                        name,
                        usage: Resources::new(c, b),
                    }),
                    (Err(e), _) | (_, Err(e)) => {
                        debug!(node = %name, error = %e, "skipping node usage");
                        None
                    }
                }
            })
            .collect())
    }

    async fn pod_usages(&self, namespace: &str) -> Result<Vec<UsageSample>, MetricsError> {
        let api: Api<PodMetrics> = Api::namespaced(self.client.clone(), namespace);
        let list = api.list(&Default::default()).await?;
        Ok(list
            .items
            .iter()
            .filter_map(|pm| {
                let name = pm.metadata.name.clone()?;
                let mut usage = Resources::ZERO;
                for c in &pm.containers {
                    if let Ok(cores) = c.cpu() {
                        usage.cpu_millicores += (cores * 1000.0).round() as i64;
                    }
                    if let Ok(bytes) = c.memory() {
                        usage.memory_bytes += bytes;
                    }
                }
                Some(UsageSample { name, usage })
            })
            .collect())
    }

    async fn pod_restarts(&self, namespace: &str, pod: &str) -> Result<i32, MetricsError> {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let p = api.get(pod).await?;
        Ok(p.status
            .as_ref()
            .and_then(|s| s.container_statuses.as_deref())
            .map(|cs| cs.iter().map(|c| c.restart_count).sum())
            .unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(s: &str) -> Quantity {
        Quantity(s.to_string())
    }

    #[test]
    fn cpu_quantities_in_every_unit() {
        assert_eq!(cpu_millicores(&q("4"), "f").unwrap(), 4000);
        assert_eq!(cpu_millicores(&q("500m"), "f").unwrap(), 500);
        assert_eq!(cpu_millicores(&q("0.5"), "f").unwrap(), 500);
        assert_eq!(cpu_millicores(&q("412345678n"), "f").unwrap(), 412);
        assert_eq!(cpu_millicores(&q("1500u"), "f").unwrap(), 2);
        assert!(cpu_millicores(&q("lots"), "f").is_err());
    }

    #[test]
    fn memory_quantities_in_every_unit() {
        assert_eq!(memory_bytes(&q("2048Ki"), "f").unwrap(), 2048 * 1024);
        assert_eq!(memory_bytes(&q("16Mi"), "f").unwrap(), 16 << 20);
        assert_eq!(memory_bytes(&q("2Gi"), "f").unwrap(), 2 << 30);
        assert_eq!(memory_bytes(&q("4096"), "f").unwrap(), 4096);
        assert!(memory_bytes(&q("x"), "f").is_err());
    }
}
