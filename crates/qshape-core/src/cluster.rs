//! `clusters.json` wire format.
//!
//! Field order and `skip_serializing_if` predicates mirror Go's
//! `encoding/json` + `,omitempty` semantics so a Go-emitted document
//! round-trips byte-for-byte through the Rust serde models.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub const CURRENT_SCHEMA_VERSION: &str = "1";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Query {
    pub raw: String,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub queryid: i64,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub calls: i64,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub total_exec_time_ms: f64,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub mean_exec_time_ms: f64,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub stddev_exec_time_ms: f64,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub rows: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Cluster {
    pub fingerprint: String,
    pub canonical: String,
    pub members: Vec<Query>,
    pub total_calls: i64,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub total_exec_time_ms: f64,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub mean_exec_time_ms: f64,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub rows: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<ParamAttribution>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ParamAttribution {
    pub position: i32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub schema: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub table: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub column: String,
    pub confidence: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub note: String,
}

/// Top-level clusters.json document. Use [`load_clusters_doc`] to parse;
/// it deserializes and validates `schema_version` in one step.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClustersDoc {
    #[serde(default)]
    pub schema_version: String,
    #[serde(default)]
    pub clusters: Vec<Cluster>,
}

/// Parse and validate a clusters.json byte buffer.
pub fn load_clusters_doc(bytes: &[u8]) -> Result<ClustersDoc> {
    let doc: ClustersDoc = serde_json::from_slice(bytes)?;
    match doc.schema_version.as_str() {
        CURRENT_SCHEMA_VERSION => Ok(doc),
        "" => Err(Error::MissingSchemaVersion(CURRENT_SCHEMA_VERSION)),
        other => Err(Error::UnsupportedSchemaVersion(
            other.to_string(),
            CURRENT_SCHEMA_VERSION,
        )),
    }
}

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

fn is_zero_f64(v: &f64) -> bool {
    *v == 0.0
}
