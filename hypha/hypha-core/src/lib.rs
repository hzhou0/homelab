//! Shared library for the hypha binaries: backend S3 client wrapper, object/tombstone metadata
//! model, config, and the error → `s3s::S3Error` mapping (§3).

pub mod backend;
pub mod config;
pub mod error;
pub mod meta;

pub use backend::Backend;
pub use config::Config;
pub use error::{Error, Result};
