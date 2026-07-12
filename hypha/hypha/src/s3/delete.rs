//! DELETE. Durable mode runs the §7 bracket under K's write lock: mark → remote `DeleteObject`
//! (the commit; NotFound ⇒ already absent, still committed) → settle by removing K's cache entry
//! and twins — absent is the authoritative 404. While marked, readers keep serving the object
//! from the remote, so an unacked delete stays invisible. The delete-tombstone + marker
//! machinery that propagates deletes asynchronously is a cached-path concern (Phase 4).

use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use hypha_core::config::Mode;
use hypha_core::error::Error;
use hypha_core::meta;

use super::Hypha;

impl Hypha {
    pub(super) async fn op_delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let key = req.input.key.clone();
        meta::validate_client_key(&key).map_err(|e| Error::Invalid(e.to_string()))?;
        if self.mode != Mode::Durable {
            return Err(s3_error!(
                NotImplemented,
                "cached-mode DeleteObject pending"
            ));
        }

        let _guard = self.tier.locks.lock(&key).await;

        // A leftover mark is repaired before this op takes its own (§7) — the bracket below must
        // start from a settled projection or a mark could hide an object the delete should 404.
        match self.cache().head(&key).await {
            Ok(head) => {
                let md = head.metadata.clone().unwrap_or_default();
                if meta::tomb_kind(&md) == Some(meta::TombKind::Transit) {
                    self.tier.repair_locked(&key).await?;
                }
            }
            Err(Error::NotFound) => {}
            Err(e) => return Err(e.into()),
        }

        // Mark → commit → settle. Crash before the remote delete: the object survives and repair
        // restores its projection. Crash after: 404 everywhere, repair removes the entry.
        self.tier.mark_transit_locked(&key).await?;
        match self.remote().delete(&key).await {
            Ok(()) | Err(Error::NotFound) => {}
            Err(e) => {
                // Failed or indeterminate commit — settle K to what the remote actually holds.
                let _ = self.tier.repair_locked(&key).await;
                return Err(e.into());
            }
        }
        self.tier.settle_absent_locked(&key).await?;

        Ok(S3Response::new(DeleteObjectOutput::default()))
    }
}
