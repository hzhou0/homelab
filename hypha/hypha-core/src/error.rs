//! Backend and protocol error mapping.

use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use s3s::{S3Error, S3ErrorCode};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("no such key")]
    NotFound,
    #[error("no such bucket")]
    NoSuchBucket,
    /// A `DeleteBucket` whose client namespace still holds objects. hypha decides this itself rather
    /// than delegating to the backend's refusal — SeaweedFS deletes a non-empty bucket and its
    /// contents outright (`allowDeleteBucketNotEmpty` defaults on), so a delegated gate is no gate.
    #[error("bucket not empty")]
    BucketNotEmpty,
    /// A `DeleteBucket` racing a write, or the write that lost once the delete committed to closing.
    /// Both sides retry: the refused delete leaves the bucket serving, and the refused write learns
    /// the settled truth on retry (§7).
    #[error("a conflicting write is in progress")]
    OperationAborted,
    #[error("reduce your request rate")]
    SlowDown,
    #[error("precondition failed")]
    PreconditionFailed,
    #[error("content-md5 mismatch")]
    BadDigest,
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("crypto: {0}")]
    Crypto(#[from] hypha_format::Error),
    #[error("backend: {0}")]
    Backend(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl Error {
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
            Some("BucketNotEmpty") => Error::BucketNotEmpty,
            Some("PreconditionFailed") | Some("412") => Error::PreconditionFailed,
            Some("BadDigest") | Some("InvalidDigest") => Error::BadDigest,
            _ => Error::Backend(format!("{err:?}")),
        }
    }
}

impl From<Error> for S3Error {
    fn from(e: Error) -> S3Error {
        let code = match &e {
            Error::NotFound => S3ErrorCode::NoSuchKey,
            Error::NoSuchBucket => S3ErrorCode::NoSuchBucket,
            Error::BucketNotEmpty => S3ErrorCode::BucketNotEmpty,
            Error::OperationAborted => S3ErrorCode::OperationAborted,
            Error::SlowDown => S3ErrorCode::SlowDown,
            Error::PreconditionFailed => S3ErrorCode::PreconditionFailed,
            Error::BadDigest => S3ErrorCode::BadDigest,
            Error::Invalid(_) => S3ErrorCode::InvalidRequest,
            // A decrypt/authentication failure is a server-side integrity fault, not client error.
            Error::Crypto(_) | Error::Backend(_) | Error::Io(_) => S3ErrorCode::InternalError,
        };
        S3Error::with_message(code, e.to_string())
    }
}
