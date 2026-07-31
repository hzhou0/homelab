//! Persistence for the deployment-wide recency ring.

use hypha_core::error::{Error, Result};
use hypha_core::meta;
use hypha_core::Backend;

use super::ring::RetiredSlice;

#[derive(Clone)]
pub(super) struct GcStore {
    backend: Backend,
    bucket: String,
}

impl GcStore {
    pub(super) fn new(backend: Backend, bucket: String) -> Self {
        GcStore { backend, bucket }
    }

    /// Created here rather than by the bucket-control actor: that actor owns *client* bucket
    /// lifecycle (§7), and this bucket has none — it exists for the life of the deployment.
    pub(super) async fn ensure(&self) -> Result<()> {
        match self.backend.head_bucket(&self.bucket).await {
            Ok(()) => Ok(()),
            Err(Error::NoSuchBucket) => match self.backend.create_bucket(&self.bucket).await {
                Ok(()) => Ok(()),
                // A concurrent creator winning the race is success.
                Err(e) => self.backend.head_bucket(&self.bucket).await.map_err(|_| e),
            },
            Err(e) => Err(e),
        }
    }

    pub(super) async fn persist(&self, slice: &RetiredSlice, depth: usize) -> Result<()> {
        self.backend
            .put_small(
                &self.bucket,
                &meta::recency_slice_key(slice.seq),
                slice.body.clone(),
                Default::default(),
                None,
                None,
            )
            .await?;
        self.prune(depth).await
    }

    pub(super) async fn load(&self, depth: usize) -> Result<Vec<(u64, Vec<u8>)>> {
        let mut seqs: Vec<u64> = self
            .slice_keys()
            .await?
            .iter()
            .filter_map(|k| meta::parse_recency_seq(k))
            .collect();
        seqs.sort_unstable_by(|a, b| b.cmp(a));
        seqs.truncate(depth);

        let mut slices = Vec::with_capacity(seqs.len());
        for seq in seqs {
            let body = self
                .backend
                .get(&self.bucket, &meta::recency_slice_key(seq), None)
                .await?
                .body
                .collect()
                .await
                .map_err(|e| Error::Backend(e.to_string()))?
                .into_bytes()
                .to_vec();
            slices.push((seq, body));
        }
        Ok(slices)
    }

    /// Keep the newest `depth` slices. Sequence numbers are zero-padded hex, so the listing's own
    /// order is rotation order and everything before the last `depth` is expired.
    async fn prune(&self, depth: usize) -> Result<()> {
        let keys = self.slice_keys().await?;
        let expired = keys.len().saturating_sub(depth);
        let deletes = keys
            .iter()
            .take(expired)
            .map(|k| self.backend.delete(&self.bucket, k));
        futures::future::try_join_all(deletes).await?;
        Ok(())
    }

    async fn slice_keys(&self) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut token = None;
        loop {
            let page = self
                .backend
                .list(
                    &self.bucket,
                    Some(meta::RECENCY_PREFIX.to_string()),
                    None,
                    token.take(),
                    None,
                    None,
                )
                .await?;
            keys.extend(
                page.contents
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|o| o.key),
            );
            match page.next_continuation_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        Ok(keys)
    }
}
