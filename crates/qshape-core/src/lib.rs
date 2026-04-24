//! qshape core library.

mod error;
mod fingerprint;
mod normalize;
mod reshape;

pub use error::{Error, Result};
pub use fingerprint::fingerprint;
pub use normalize::normalize;
