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
