//! The hypha encryption envelope: a thin wrapper around the `age` crate plus the
//! offset arithmetic and seekable-reader adapter that ranged GETs need.
//!
//! No S3, no Tokio, no network I/O — everything here is sync `std::io` over caller-supplied
//! readers/writers, so the whole crate is exercisable by proptest/fuzz without a server.

pub mod envelope;
pub mod offset;
pub mod stream;

pub use envelope::Envelope;
pub use stream::{RangeReader, RangeSource};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid age identity: {0}")]
    Identity(String),
    #[error(transparent)]
    Encrypt(#[from] age::EncryptError),
    #[error(transparent)]
    Decrypt(#[from] age::DecryptError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
