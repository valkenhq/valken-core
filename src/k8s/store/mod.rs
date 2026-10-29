mod ingest;
mod kube_goat;
mod kube_resource;
mod kube_resource_identifier;
mod kube_resource_store;
mod kube_resource_type;
#[cfg(feature = "godot")]
mod kube_store;

pub use ingest::{IngestReport, ResourceIngest};
pub(crate) use kube_goat::GraphParts;
pub use kube_goat::{EdgeType, KubeGoat, Upsert};
pub use kube_resource::KubeResource;
pub use kube_resource_identifier::{KubeResourceIdentifier, ParseKriError};
pub use kube_resource_store::{kri_for, KubeResourceStore};
pub use kube_resource_type::KubeResourceType;
#[cfg(feature = "godot")]
pub use kube_store::KubeStore;
