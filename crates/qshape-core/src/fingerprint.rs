use crate::error::Result;
use crate::normalize::normalize;

pub fn fingerprint(sql: &str) -> Result<String> {
    let target = normalize(sql).unwrap_or_else(|_| sql.to_string());
    let fp = pg_query::fingerprint(&target)?;

    Ok(format!("sha1:{}", fp.hex))
}
