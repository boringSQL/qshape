#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("pg_query: {0}")]
    PgQuery(#[from] pg_query::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
