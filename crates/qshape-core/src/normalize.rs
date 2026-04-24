use crate::error::Result;

pub fn normalize(sql: &str) -> Result<String> {
    let mut result = pg_query::parse(sql)?;
    crate::reshape::reshape(&mut result.protobuf);
    Ok(pg_query::deparse(&result.protobuf)?)
}
