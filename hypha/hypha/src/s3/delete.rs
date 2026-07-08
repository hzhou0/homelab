//! DELETE. In `durable` mode, hard-delete both sides under the key lock: the cache body/tombstone
//! (plus its twins) and the remote ciphertext. The delete-tombstone + marker machinery that keeps
//! a crash from resurrecting an object is a cached-path (async) concern, added in Phase 4.

use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use hypha_core::config::Mode;
use hypha_core::error::Error;

use super::Hypha;

impl Hypha {
    pub(super) async fn op_delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let key = req.input.key.clone();
        if self.mode != Mode::Durable {
            return Err(s3_error!(NotImplemented, "cached-mode DeleteObject pending"));
        }

        let _guard = self.tier.locks.lock(&key).await;
        self.tier.delete_twins(&key).await?;
        self.cache().delete(&key).await?;
        // The remote may legitimately lack the object (e.g. it was never uploaded); ignore that.
        match self.remote().delete(&key).await {
            Ok(()) | Err(Error::NotFound) => {}
            Err(e) => return Err(e.into()),
        }
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }
}
