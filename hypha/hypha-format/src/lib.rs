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

#[derive(Debug)]
pub enum Error {
    Identity(String),
    Encrypt(age::EncryptError),
    Decrypt(age::DecryptError),
    Io(std::io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // The wrapped-error variants render transparently (delegate to the inner Display),
            // so a caller printing the chain doesn't see doubled prefixes.
            Error::Identity(s) => write!(f, "invalid age identity: {s}"),
            Error::Encrypt(e) => write!(f, "{e}"),
            Error::Decrypt(e) => write!(f, "{e}"),
            Error::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {
    // `source` exposes the underlying cause so `{:?}`/error-chain printers can walk it.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Identity(_) => None,
            Error::Encrypt(e) => Some(e),
            Error::Decrypt(e) => Some(e),
            Error::Io(e) => Some(e),
        }
    }
}

// The `From` impls are what let call sites use `?` to convert into this error.
impl From<age::EncryptError> for Error {
    fn from(e: age::EncryptError) -> Self {
        Error::Encrypt(e)
    }
}
impl From<age::DecryptError> for Error {
    fn from(e: age::DecryptError) -> Self {
        Error::Decrypt(e)
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
