//! Backend and protocol error mapping.

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use s3s::{S3Error, S3ErrorCode};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("no such key")]
    NotFound,
    #[error("no such bucket")]
    NoSuchBucket,
    /// The backend refused the caller, which is never a statement about whether the object or
    /// bucket exists — a backend that hides what it will not serve answers this *instead of* an
    /// absence, so the two must not collapse.
    #[error("access denied")]
    AccessDenied,
    /// A `DeleteBucket` whose client namespace still holds objects. hypha decides this itself rather
    /// than delegating to the backend's refusal — SeaweedFS deletes a non-empty bucket and its
    /// contents outright (`allowDeleteBucketNotEmpty` defaults on), so a delegated gate is no gate.
    #[error("bucket not empty")]
    BucketNotEmpty,
    /// A `DeleteBucket` racing a write, or the write that lost once the delete committed to closing.
    /// Both sides retry: the refused delete leaves the bucket serving, and the refused write learns
    /// the settled truth on retry .
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
    pub fn from_sdk<E>(err: SdkError<E, HttpResponse>) -> Self
    where
        E: ProvideErrorMetadata + std::fmt::Debug,
    {
        // Status before code, for `403` alone. A HEAD carries no error document, so the SDK has no
        // code to read and falls back to the single error its operation models — on `HeadObject`
        // that is `NotFound`, which would turn every refusal into an absence and hand a caller a
        // deletion it never saw. No other status needs this: a code that did parse is the more
        // specific answer.
        if let SdkError::ServiceError(e) = &err {
            if e.raw().status().as_u16() == 403 {
                return Error::AccessDenied;
            }
        }
        match err.code() {
            Some("NoSuchKey") | Some("404") | Some("NotFound") | Some("NoSuchUpload") => {
                Error::NotFound
            }
            Some("NoSuchBucket") => Error::NoSuchBucket,
            Some("BucketNotEmpty") => Error::BucketNotEmpty,
            Some("PreconditionFailed") | Some("412") => Error::PreconditionFailed,
            Some("BadDigest") | Some("InvalidDigest") | Some("XAmzContentChecksumMismatch") => {
                Error::BadDigest
            }
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
            // `AccessDenied` belongs here for the same reason: the client that authenticated to
            // hypha is not the caller the backend refused, and blaming it sends the operator to
            // the wrong set of credentials.
            Error::Crypto(_) | Error::Backend(_) | Error::AccessDenied | Error::Io(_) => {
                S3ErrorCode::InternalError
            }
        };
        S3Error::with_message(code, e.to_string())
    }
}
