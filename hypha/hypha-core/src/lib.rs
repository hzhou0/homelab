//! Shared backend, metadata, configuration, and error types.

pub mod backend;
pub mod config;
pub mod error;
pub mod meta;

pub use backend::Backend;
pub use config::Config;
pub use error::{Error, Result};
