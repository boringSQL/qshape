use std::io::{self, Write};

use anyhow::{Context, Result};
use postgres::{Client, NoTls};
use qshape_core::{CURRENT_SCHEMA_VERSION, ClustersDoc, Query, group};

// Skip session/meta statements
const SKIP_META_REGEX: &str = r"^\s*(discard|begin|start|commit|rollback|savepoint|release|set|reset|show|listen|unlisten|notify|checkpoint|vacuum|analyze|reindex|cluster|explain|prepare|deallocate|execute|close|fetch|move|lock)\M";

pub fn run(conn_str: &str, min_calls: i64, limit: i32, database: Option<&str>) -> Result<()> {
    let mut client = Client::connect(conn_str, NoTls).context("connect")?;

    let sql = build_sql(database.is_some(), limit);
    let db_owned = database.map(|s| s.to_string());
    let mut params: Vec<&(dyn postgres::types::ToSql + Sync)> = Vec::new();
    if let Some(db) = db_owned.as_ref() {
        params.push(db);
    }
    params.push(&min_calls);

    let rows = client
        .query(sql.as_str(), &params)
        .context("query pg_stat_statements (extension installed? PG 13+?)")?;

    let mut queries: Vec<Query> = Vec::with_capacity(rows.len());
    for row in rows {
        queries.push(Query {
            queryid: row.get(0),
            calls: row.get(1),
            raw: row.get(2),
            total_exec_time_ms: row.get(3),
            mean_exec_time_ms: row.get(4),
            stddev_exec_time_ms: row.get(5),
            rows: row.get(6),
        });
    }

    let n_queries = queries.len();
    let clusters = group(queries)?;

    eprintln!("captured {} queries → {} clusters", n_queries, clusters.len());

    let doc = ClustersDoc {
        schema_version: CURRENT_SCHEMA_VERSION.to_string(),
        clusters,
    };
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer_pretty(&mut handle, &doc)?;
    handle.write_all(b"\n")?;
    Ok(())
}

fn build_sql(has_database: bool, limit: i32) -> String {
    let mut sql = String::from(
        "SELECT s.queryid, s.calls, s.query, s.total_exec_time, s.mean_exec_time, s.stddev_exec_time, s.rows FROM pg_stat_statements s",
    );
    let mut where_clauses: Vec<String> = Vec::new();
    let mut next_param = 1;
    if has_database {
        sql.push_str(" JOIN pg_database d ON d.oid = s.dbid");
        where_clauses.push(format!("d.datname = ${next_param}"));
        next_param += 1;
    }
    where_clauses.push(format!("s.calls > ${next_param}"));
    where_clauses.push(format!("s.query !~* '{SKIP_META_REGEX}'"));
    sql.push_str(" WHERE ");
    sql.push_str(&where_clauses.join(" AND "));
    sql.push_str(" ORDER BY s.total_exec_time DESC");
    if limit > 0 {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_database_no_limit() {
        let sql = build_sql(false, 0);
        assert!(!sql.contains("pg_database"));
        assert!(sql.contains("s.calls > $1"));
        assert!(sql.contains("s.query !~*"));
        assert!(sql.contains("ORDER BY s.total_exec_time DESC"));
        assert!(!sql.contains("LIMIT"));
    }

    #[test]
    fn with_database_renumbers_min_calls() {
        let sql = build_sql(true, 0);
        assert!(sql.contains("JOIN pg_database d ON d.oid = s.dbid"));
        assert!(sql.contains("d.datname = $1"));
        assert!(sql.contains("s.calls > $2"));
    }

    #[test]
    fn limit_appended_when_positive() {
        let sql = build_sql(false, 25);
        assert!(sql.trim_end().ends_with(" LIMIT 25"));
    }

    #[test]
    fn limit_omitted_when_zero_or_negative() {
        assert!(!build_sql(false, 0).contains("LIMIT"));
        assert!(!build_sql(true, -1).contains("LIMIT"));
    }

    // Regression: the skip-meta regex must remain in the WHERE so qshape's
    // own EXPLAIN/PREPARE/DEALLOCATE probes don't pollute the result.
    #[test]
    fn skip_meta_regex_present_in_both_branches() {
        assert!(build_sql(false, 0).contains("explain|prepare|deallocate"));
        assert!(build_sql(true, 0).contains("explain|prepare|deallocate"));
    }
}
