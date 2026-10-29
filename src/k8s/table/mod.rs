//! Any resource as a live `kubectl get` table, for the list view's `:`
//! command: per-cluster [`discovery`] (what `po`, `deploy` or `cert`
//! mean), server-side printed [`rows`], and a [`watch`] that turns changes
//! into row upserts / deletes.

pub mod discovery;
pub mod rows;
pub mod watch;

pub use discovery::{discover, ApiCatalog, ApiResourceInfo};
pub use rows::{Cell, Owner, TableColumn, TableRow};
pub use watch::{watch_table, TableEvent};
