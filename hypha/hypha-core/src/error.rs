//! Error model for the library layers, plus the mapping to `s3s::S3Error` the serving binary
//! returns to clients. Keeping the mapping here means the `s3/` op handlers can `?`-propagate a
//! `hypha_core::Error` and get a protocol-correct status without restating the match each time.

use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use s3s::{S3Error, S3ErrorCode};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Object (or the ciphertext body behind a tombstone) is absent on the backend.
    #[error("no such key")]
    NotFound,
    /// Target bucket does not exist.
    #[error("no such bucket")]
    NoSuchBucket,
    /// An `If-Match` / `If-None-Match` precondition did not hold.
    #[error("precondition failed")]
    PreconditionFailed,
    /// Client sent something hypha rejects at admission (bad key byte, oversized part, …).
    #[error("invalid request: {0}")]
    Invalid(String),
    /// age envelope failure — decrypt authentication, truncation, or a malformed header.
    #[error("crypto: {0}")]
    Crypto(#[from] hypha_format::Error),
    /// Anything the backend SDK reported that isn't one of the modelled cases above.
    #[error("backend: {0}")]
    Backend(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl Error {
    /// Collapse an aws-sdk-s3 `SdkError` into a hypha `Error`, recognising the S3 error codes
    /// hypha's control flow branches on (missing key/bucket, failed precondition). Everything
    /// else stays an opaque `Backend` string — the client sees a 500, which is correct for an
    /// unexpected backend fault.
    pub fn from_sdk<E, R>(err: SdkError<E, R>) -> Self
    where
        E: ProvideErrorMetadata + std::fmt::Debug,
        R: std::fmt::Debug,
    {
        match err.code() {
            Some("NoSuchKey") | Some("404") | Some("NotFound") | Some("NoSuchUpload") => {
                Error::NotFound
            }
            Some("NoSuchBucket") => Error::NoSuchBucket,
            Some("PreconditionFailed") | Some("412") => Error::PreconditionFailed,
            _ => Error::Backend(format!("{err:?}")),
        }
    }
}

impl From<Error> for S3Error {
    fn from(e: Error) -> S3Error {
        let code = match &e {
            Error::NotFound => S3ErrorCode::NoSuchKey,
            Error::NoSuchBucket => S3ErrorCode::NoSuchBucket,
            Error::PreconditionFailed => S3ErrorCode::PreconditionFailed,
            Error::Invalid(_) => S3ErrorCode::InvalidRequest,
            // A decrypt/authentication failure is a server-side integrity fault, not client error.
            Error::Crypto(_) | Error::Backend(_) | Error::Io(_) => S3ErrorCode::InternalError,
        };
        S3Error::with_message(code, e.to_string())
    }
}
