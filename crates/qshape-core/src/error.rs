#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("pg_query: {0}")]
    PgQuery(#[from] pg_query::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("clusters.json missing schema_version; must be {0:?}")]
    MissingSchemaVersion(&'static str),

    #[error("clusters.json schema_version={0:?} not supported; must be {1:?}")]
    UnsupportedSchemaVersion(String, &'static str),
}

pub type Result<T> = std::result::Result<T, Error>;
