crates/qshape-core/src/lib.rs
use crate::error::Result;

// TODO reshape do be done later
pub fn normalize(sql: &str) -> Result<String> {
    let tree = pg_query::parse(sql)?;
    Ok(tree.deparse()?)
}
