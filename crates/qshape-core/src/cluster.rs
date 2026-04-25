//! `clusters.json` wire format.
//!
//! Field order and `skip_serializing_if` predicates mirror Go's
//! `encoding/json` + `,omitempty` semantics so a Go-emitted document
//! round-trips byte-for-byte through the Rust serde models.

use std::cmp::Ordering;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::fingerprint::fingerprint;
use crate::normalize::normalize;

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

/// Group queries by their canonical fingerprint, sorted by total exec
/// time and total calls as fallback
pub fn group(queries: Vec<Query>) -> Result<Vec<Cluster>> {
    let mut groups: HashMap<String, Cluster> = HashMap::new();
    let mut unparseable: Vec<Cluster> = Vec::new();

    for q in queries {
        let fp = match fingerprint(&q.raw) {
            Ok(fp) => fp,
            Err(_) => {
                // separate cluster for unparsable queries
                unparseable.push(Cluster {
                    fingerprint: String::new(),
                    canonical: q.raw.clone(),
                    total_calls: q.calls,
                    total_exec_time_ms: q.total_exec_time_ms,
                    mean_exec_time_ms: q.mean_exec_time_ms,
                    rows: q.rows,
                    members: vec![q],
                    params: Vec::new(),
                });
                continue;
            }
        };

        let c = groups.entry(fp.clone()).or_insert_with(|| {
            let canonical = normalize(&q.raw).unwrap_or_else(|_| q.raw.clone());
            Cluster {
                fingerprint: fp,
                canonical,
                ..Cluster::default()
            }
        });
        c.total_calls += q.calls;
        c.total_exec_time_ms += q.total_exec_time_ms;
        c.rows += q.rows;
        c.members.push(q);
    }

    let mut out: Vec<Cluster> = Vec::with_capacity(groups.len() + unparseable.len());
    for mut c in groups.into_values() {
        if c.total_calls > 0 {
            c.mean_exec_time_ms = c.total_exec_time_ms / c.total_calls as f64;
        }
        out.push(c);
    }
    out.append(&mut unparseable);

    let has_timing = out.iter().any(|c| c.total_exec_time_ms > 0.0);

    out.sort_by(|a, b| {
        if has_timing && a.total_exec_time_ms != b.total_exec_time_ms {
            return b
                .total_exec_time_ms
                .partial_cmp(&a.total_exec_time_ms)
                .unwrap_or(Ordering::Equal);
        }
        if a.total_calls != b.total_calls {
            return b.total_calls.cmp(&a.total_calls);
        }
        a.fingerprint.cmp(&b.fingerprint)
    });

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(raw: &str, calls: i64) -> Query {
        Query { raw: raw.to_string(), calls, ..Query::default() }
    }

    #[test]
    fn aggregates_calls() {
        let out = group(vec![
            q("SELECT id FROM users WHERE id = 1", 100),
            q("SELECT id FROM users WHERE id = 99", 200),
        ])
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].total_calls, 300);
        assert_eq!(out[0].members.len(), 2);
    }

    #[test]
    fn aggregates_timing() {
        let out = group(vec![
            Query {
                raw: "SELECT id FROM users WHERE id = 1".to_string(),
                calls: 100,
                total_exec_time_ms: 250.0,
                rows: 100,
                ..Query::default()
            },
            Query {
                raw: "SELECT id FROM users WHERE id = 99".to_string(),
                calls: 400,
                total_exec_time_ms: 750.0,
                rows: 400,
                ..Query::default()
            },
        ])
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].total_exec_time_ms, 1000.0);
        assert_eq!(out[0].rows, 500);
        assert_eq!(out[0].mean_exec_time_ms, 1000.0 / 500.0);
    }

    #[test]
    fn sorts_by_timing_when_present() {
        let out = group(vec![
            Query {
                raw: "SELECT id FROM users".to_string(),
                calls: 1000,
                total_exec_time_ms: 50.0,
                ..Query::default()
            },
            Query {
                raw: "SELECT name FROM users".to_string(),
                calls: 10,
                total_exec_time_ms: 5000.0,
                ..Query::default()
            },
        ])
        .unwrap();
        assert_eq!(out.len(), 2);
        assert!(
            out[0].total_exec_time_ms >= out[1].total_exec_time_ms,
            "expected sort by total_exec_time_ms desc"
        );
    }

    #[test]
    fn sorts_by_calls_when_no_timing() {
        let out = group(vec![
            q("SELECT name FROM users", 10),
            q("SELECT id FROM users", 500),
        ])
        .unwrap();
        assert_eq!(out.len(), 2);
        assert!(out[0].total_calls >= out[1].total_calls);
    }

    // Alias-only variants collapse (reshape strips decorative aliases);
    // the LIMIT variant stays in its own cluster because LIMIT changes
    // plan shape and LIMIT subsumption is out of scope.
    #[test]
    fn orm_variants_current_behavior() {
        let out = group(vec![
            q("SELECT id, name FROM users WHERE id = $1", 1),
            q("SELECT u.id, u.name FROM users u WHERE u.id = $1", 1),
            q("SELECT id, name FROM users WHERE id = $1 LIMIT $2", 1),
        ])
        .unwrap();
        assert_eq!(out.len(), 2);
        let total: i64 = out.iter().map(|c| c.total_calls).sum();
        assert_eq!(total, 3);
    }

    // Alias-only, optional AS, and AND-predicate reorder all collapse.
    #[test]
    fn orm_variants_collapse() {
        let inputs = [
            "SELECT id, name FROM users WHERE id = $1 AND status = $2",
            "SELECT id, name FROM users WHERE status = $2 AND id = $1",
            "SELECT u.id, u.name FROM users u WHERE u.id = $1 AND u.status = $2",
            "SELECT u.id, u.name FROM users AS u WHERE u.status = $2 AND u.id = $1",
        ];
        let out = group(inputs.iter().map(|s| q(s, 1)).collect()).unwrap();
        assert_eq!(out.len(), 1, "got: {out:#?}");
        assert_eq!(out[0].total_calls, inputs.len() as i64);
    }

    #[test]
    fn unparseable_becomes_singleton() {
        let out = group(vec![q("SELECT FROM WHERE", 5)]).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].fingerprint, "");
        assert_eq!(out[0].canonical, "SELECT FROM WHERE");
    }

    #[test]
    fn empty_input() {
        let out = group(Vec::new()).unwrap();
        assert!(out.is_empty());
    }
}
