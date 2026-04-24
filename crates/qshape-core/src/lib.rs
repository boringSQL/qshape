//! qshape core library.

mod error;
mod fingerprint;
mod normalize;

pub use error::{Error, Result};
pub use fingerprint::fingerprint;
pub use normalize::normalize;
