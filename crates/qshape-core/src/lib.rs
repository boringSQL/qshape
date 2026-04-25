//! qshape core library.

mod cluster;
mod error;
mod fingerprint;
mod normalize;
mod reshape;
mod typecast;

pub use cluster::{
    CURRENT_SCHEMA_VERSION, Cluster, ClustersDoc, ParamAttribution, Query, group,
    load_clusters_doc,
};
pub use error::{Error, Result};
pub use fingerprint::fingerprint;
pub use normalize::normalize;
pub use typecast::{cast_func_param_refs, max_param_number};
