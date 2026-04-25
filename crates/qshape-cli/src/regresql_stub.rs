//! Generate regresql `sql/` stubs for the top-N clusters

use std::fs;
use std::path::Path;
use std::sync::OnceLock;

use anyhow::{Context, Result};
use qshape_core::Cluster;
use regex::Regex;

use crate::loader::load_clusters;

pub fn run(in_path: Option<&str>, out_dir: &str, top: usize, min_calls: i64) -> Result<()> {
    let doc = load_clusters(in_path)?;

    let sql_dir = Path::new(out_dir).join("sql");
    fs::create_dir_all(&sql_dir).with_context(|| format!("create {}", sql_dir.display()))?;

    let mut emitted = 0usize;
    for c in &doc.clusters {
        if emitted >= top {
            break;
        }
        if c.fingerprint.is_empty() || c.total_calls <= min_calls {
            continue;
        }
        emitted += 1;
        let slug = stub_slug(emitted, &c.fingerprint);
        let sql = rewrite_params(&c.canonical);
        let path = sql_dir.join(format!("{slug}.sql"));
        write_sql_stub(&path, &slug, c, &sql)?;
    }

    eprintln!("wrote {emitted} stubs to {out_dir}");
    Ok(())
}

fn stub_slug(rank: usize, fp: &str) -> String {
    let body = fp.strip_prefix("sha1:").unwrap_or(fp);
    let prefix: String = body.chars().take(8).collect();
    format!("q{rank:02}-{prefix}")
}

// $N to:paramN
fn rewrite_params(sql: &str) -> String {
    static R: OnceLock<Regex> = OnceLock::new();
    let re = R.get_or_init(|| Regex::new(r"\$(\d+)").expect("static regex"));
    re.replace_all(sql, ":param$1").into_owned()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use qshape_core::{CURRENT_SCHEMA_VERSION, ClustersDoc, Query};
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn rewrites_params_to_named() {
        let got =
            rewrite_params("SELECT id FROM users WHERE id = $1 AND tenant_id = $2 AND id = $1");
        assert_eq!(
            got,
            "SELECT id FROM users WHERE id = :param1 AND tenant_id = :param2 AND id = :param1"
        );
    }

    #[test]
    fn rewrite_no_params_passthrough() {
        let sql = "SELECT 1";
        assert_eq!(rewrite_params(sql), sql);
    }

    #[test]
    fn rewrite_double_digit_params() {
        let got = rewrite_params("SELECT $10 + $2");
        assert_eq!(got, "SELECT :param10 + :param2");
    }

    #[test]
    fn slug_strips_sha1_prefix_and_truncates() {
        assert_eq!(stub_slug(1, "sha1:abc12345def"), "q01-abc12345");
        // rank zero-pads to 2 digits
        assert_eq!(stub_slug(7, "sha1:0123456789ab"), "q07-01234567");
    }

    #[test]
    fn slug_handles_short_fingerprint() {
        // pathological — fingerprint shorter than 8 hex chars
        assert_eq!(stub_slug(3, "sha1:abc"), "q03-abc");
    }

    #[test]
    fn slug_handles_missing_prefix() {
        // fingerprint without sha1: prefix (defensive)
        assert_eq!(stub_slug(2, "fedcba9876543210"), "q02-fedcba98");
    }

    #[test]
    fn run_writes_expected_files() {
        let tmp = TempDir::new().unwrap();
        let in_path = tmp.path().join("clusters.json");
        let out_dir = tmp.path().join("stubs");

        let doc = ClustersDoc {
            schema_version: CURRENT_SCHEMA_VERSION.to_string(),
            clusters: vec![
                Cluster {
                    fingerprint: "sha1:aaaaaaaa11111111".to_string(),
                    canonical: "SELECT id FROM users WHERE id = $1".to_string(),
                    members: vec![Query::default()],
                    total_calls: 100,
                    ..Cluster::default()
                },
                Cluster {
                    fingerprint: "sha1:bbbbbbbb22222222".to_string(),
                    canonical: "SELECT 1".to_string(),
                    total_calls: 5,
                    ..Cluster::default()
                },
                // skipped: empty fingerprint
                Cluster {
                    fingerprint: String::new(),
                    canonical: "junk".to_string(),
                    total_calls: 999,
                    ..Cluster::default()
                },
            ],
        };
        fs::write(&in_path, serde_json::to_vec(&doc).unwrap()).unwrap();

        run(Some(in_path.to_str().unwrap()), out_dir.to_str().unwrap(), 10, 0).unwrap();

        let sql_dir = out_dir.join("sql");
        let entries: Vec<_> = fs::read_dir(&sql_dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(entries.len(), 2, "third cluster has empty fingerprint, must be skipped");

        let first = fs::read_to_string(sql_dir.join("q01-aaaaaaaa.sql")).unwrap();
        assert!(first.contains("-- name: q01-aaaaaaaa"));
        assert!(first.contains("-- Generated from qshape cluster sha1:aaaaaaaa11111111"));
        assert!(first.contains("Total calls (prod): 100 across 1 member variants"));
        assert!(first.contains("SELECT id FROM users WHERE id = :param1"));
        assert!(first.ends_with('\n'), "trailing newline must be present");
    }

    #[test]
    fn run_honours_top_and_min_calls() {
        let tmp = TempDir::new().unwrap();
        let in_path = tmp.path().join("clusters.json");
        let out_dir = tmp.path().join("stubs");

        let mk = |fp: &str, calls: i64| Cluster {
            fingerprint: fp.to_string(),
            canonical: "SELECT 1".to_string(),
            total_calls: calls,
            ..Cluster::default()
        };
        let doc = ClustersDoc {
            schema_version: CURRENT_SCHEMA_VERSION.to_string(),
            clusters: vec![
                mk("sha1:aa11", 100),
                mk("sha1:bb22", 50),
                mk("sha1:cc33", 10),
                mk("sha1:dd44", 5),
            ],
        };
        fs::write(&in_path, serde_json::to_vec(&doc).unwrap()).unwrap();

        // top=2 caps emission
        run(Some(in_path.to_str().unwrap()), out_dir.to_str().unwrap(), 2, 0).unwrap();
        let n = fs::read_dir(out_dir.join("sql")).unwrap().count();
        assert_eq!(n, 2);

        // min_calls=20 filters
        let out2 = tmp.path().join("stubs2");
        run(Some(in_path.to_str().unwrap()), out2.to_str().unwrap(), 10, 20).unwrap();
        let n2 = fs::read_dir(out2.join("sql")).unwrap().count();
        assert_eq!(n2, 2, "only sha1:aa11 (100) and sha1:bb22 (50) survive min_calls=20");
    }
}

fn write_sql_stub(path: &Path, slug: &str, c: &Cluster, sql: &str) -> Result<()> {
    let trailing = if sql.ends_with('\n') { "" } else { "\n" };
    let content = format!(
        "-- name: {slug}\n\
         -- Generated from qshape cluster {fp}\n\
         -- Total calls (prod): {tc} across {n} member variants\n\
         -- TODO: rename this slug, review canonical SQL, replace :paramN with meaningful names\n\
         {sql}{trailing}",
        fp = c.fingerprint,
        tc = c.total_calls,
        n = c.members.len(),
    );
    fs::write(path, content).with_context(|| format!("write {}", path.display()))
}
