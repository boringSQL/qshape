//! qshape core library.

mod cluster;
mod error;
mod fingerprint;
mod normalize;
mod reshape;

pub use cluster::{
    CURRENT_SCHEMA_VERSION, Cluster, ClustersDoc, ParamAttribution, Query, SchemaError,
    validate_schema_version,
};
pub use error::{Error, Result};
pub use fingerprint::fingerprint;
pub use normalize::normalize;
