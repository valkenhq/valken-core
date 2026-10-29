//! Server-side printing (`Accept: …;as=Table;g=meta.k8s.io;v=v1`): the API
//! server renders `kubectl get` columns for any kind — built-ins with their
//! hand-written printers, CRDs from `additionalPrinterColumns` — so every
//! resource gets the right columns without a printer per kind here.
//!
//! Rows carry the object's metadata (`includeObject=Metadata`), never its
//! spec or data: Secrets stay metadata-only.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;

/// One column of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableColumn {
    pub name: String,
    /// OpenAPI type: `string`, `integer`, `number`, `boolean`, `date`.
    pub kind: String,
    /// `name`, `date`, …
    pub format: String,
    /// 0 = shown by `kubectl get`; higher = `-o wide` only.
    pub priority: i32,
    pub description: String,
}

/// One cell, typed so numbers sort as numbers.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Empty,
    Text(String),
    Int(i64),
    Num(f64),
    Bool(bool),
}

impl Cell {
    fn from_json(v: &serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => Cell::Empty,
            serde_json::Value::String(s) if s.is_empty() || s == "<none>" => Cell::Empty,
            serde_json::Value::String(s) => Cell::Text(s.clone()),
            serde_json::Value::Bool(b) => Cell::Bool(*b),
            serde_json::Value::Number(n) => n
                .as_i64()
                .map(Cell::Int)
                .or_else(|| n.as_f64().map(Cell::Num))
                .unwrap_or(Cell::Empty),
            other => Cell::Text(other.to_string()),
        }
    }

    /// As printed.
    pub fn text(&self) -> String {
        match self {
            Cell::Empty => String::new(),
            Cell::Text(s) => s.clone(),
            Cell::Int(i) => i.to_string(),
            Cell::Num(n) => format!("{n}"),
            Cell::Bool(b) => b.to_string(),
        }
    }
}

/// Who owns a row (ReplicaSet → Deployment, Pod → ReplicaSet / Job …).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner {
    pub kind: String,
    pub name: String,
    pub uid: String,
    pub controller: bool,
}

/// One object, as a table row.
#[derive(Debug, Clone, PartialEq)]
pub struct TableRow {
    pub uid: String,
    pub name: String,
    pub namespace: Option<String>,
    pub created: Option<DateTime<Utc>>,
    pub labels: BTreeMap<String, String>,
    pub owners: Vec<Owner>,
    pub resource_version: String,
    /// One per column, in [`TableColumn`] order.
    pub cells: Vec<Cell>,
}

// ── Wire format ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WireTable {
    #[serde(default)]
    pub metadata: WireListMeta,
    /// `null` (not absent) on every watch event after the first.
    #[serde(default, deserialize_with = "null_as_empty")]
    pub column_definitions: Vec<WireColumn>,
    #[serde(default, deserialize_with = "null_as_empty")]
    pub rows: Vec<WireRow>,
}

fn null_as_empty<'de, D, T>(d: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(d)?.unwrap_or_default())
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WireListMeta {
    #[serde(default)]
    pub resource_version: Option<String>,
    #[serde(default, rename = "continue")]
    pub continue_: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WireColumn {
    pub name: String,
    #[serde(default, rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub format: String,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WireRow {
    #[serde(default, deserialize_with = "null_as_empty")]
    pub cells: Vec<serde_json::Value>,
    #[serde(default)]
    pub object: Option<WireObject>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WireObject {
    #[serde(default)]
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
}

impl From<&WireColumn> for TableColumn {
    fn from(c: &WireColumn) -> Self {
        TableColumn {
            name: c.name.clone(),
            kind: c.kind.clone(),
            format: c.format.clone(),
            priority: c.priority,
            description: c.description.clone(),
        }
    }
}

impl WireRow {
    /// `None` for a row without metadata (it can't be told apart from the
    /// others, so it can't be updated in place).
    pub fn to_row(&self) -> Option<TableRow> {
        let meta = &self.object.as_ref()?.metadata;
        let name = meta.name.clone()?;
        let uid = meta
            .uid
            .clone()
            .unwrap_or_else(|| format!("{}/{}", meta.namespace.as_deref().unwrap_or(""), name));
        let created = meta
            .creation_timestamp
            .as_ref()
            .and_then(|t| DateTime::from_timestamp(t.0.as_second(), 0));
        let owners = meta
            .owner_references
            .iter()
            .flatten()
            .map(|o| Owner {
                kind: o.kind.clone(),
                name: o.name.clone(),
                uid: o.uid.clone(),
                controller: o.controller.unwrap_or(false),
            })
            .collect();
        Some(TableRow {
            uid,
            name,
            namespace: meta.namespace.clone(),
            created,
            labels: meta.labels.clone().unwrap_or_default(),
            owners,
            resource_version: meta.resource_version.clone().unwrap_or_default(),
            cells: self.cells.iter().map(Cell::from_json).collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIST: &str = r#"{
      "kind": "Table", "apiVersion": "meta.k8s.io/v1",
      "metadata": {"resourceVersion": "4242", "continue": ""},
      "columnDefinitions": [
        {"name": "Name", "type": "string", "format": "name", "description": "Name must be unique", "priority": 0},
        {"name": "Ready", "type": "string", "format": "", "description": "", "priority": 0},
        {"name": "Up-to-date", "type": "integer", "format": "", "description": "", "priority": 0},
        {"name": "Age", "type": "string", "format": "", "description": "", "priority": 0},
        {"name": "Images", "type": "string", "format": "", "description": "", "priority": 1}
      ],
      "rows": [
        {"cells": ["checkout", "2/3", 3, "11d", "shop/checkout:2.14.1"],
         "object": {"kind": "PartialObjectMetadata", "apiVersion": "meta.k8s.io/v1",
           "metadata": {"name": "checkout", "namespace": "storefront", "uid": "u-1", "resourceVersion": "41",
             "creationTimestamp": "2026-10-10T08:00:00Z", "labels": {"app": "checkout"},
             "ownerReferences": [{"apiVersion": "v1", "kind": "Foo", "name": "f", "uid": "u-0", "controller": true}]}}},
        {"cells": ["orphan", "", 0, "1d", "<none>"]}
      ]
    }"#;

    #[test]
    fn parses_columns_rows_and_metadata() {
        let t: WireTable = serde_json::from_str(LIST).unwrap();
        assert_eq!(t.metadata.resource_version.as_deref(), Some("4242"));
        let cols: Vec<TableColumn> = t.column_definitions.iter().map(TableColumn::from).collect();
        assert_eq!(cols.len(), 5);
        assert_eq!((cols[2].kind.as_str(), cols[4].priority), ("integer", 1));
        let row = t.rows[0].to_row().unwrap();
        assert_eq!(
            (
                row.uid.as_str(),
                row.name.as_str(),
                row.namespace.as_deref()
            ),
            ("u-1", "checkout", Some("storefront"))
        );
        assert_eq!(row.cells[1], Cell::Text("2/3".into()));
        assert_eq!(row.cells[2], Cell::Int(3));
        assert_eq!(row.labels.get("app").map(String::as_str), Some("checkout"));
        assert_eq!(row.owners[0].uid, "u-0");
        assert!(row.created.is_some());
        assert!(t.rows[1].to_row().is_none());
        assert_eq!(Cell::from_json(&serde_json::json!("<none>")), Cell::Empty);
        assert_eq!(Cell::from_json(&serde_json::json!(1.5)).text(), "1.5");
    }

    #[test]
    fn watch_events_carry_null_columns() {
        let t: WireTable = serde_json::from_str(r#"{"kind":"Table","metadata":{},"columnDefinitions":null,"rows":[{"cells":["a"],"object":{"metadata":{"name":"a","uid":"u"}}}]}"#).unwrap();
        assert!(t.column_definitions.is_empty());
        assert_eq!(t.rows[0].to_row().unwrap().uid, "u");
    }
}
