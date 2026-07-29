//! Debris sweeps (§8): the reclaims that have no owner on the client path.

use std::collections::{HashMap, HashSet};

use hypha_core::error::Result;
use hypha_core::meta;

use crate::tier::Tiering;

const PAGE_KEYS: i32 = 1000;

/// Reclaim the record ranges of uploads the remote is no longer running.
///
/// Every upload's records share the `0x01 0x01 m ‖ <upload-id> ‖ 0x01` range (§6), so the whole set
/// is one prefix scan and needs no side index — which is what lets complete and abort skip the
/// delete entirely and hand it here, off the client path.
///
/// **The remote decides, and it is asked second.** An upload id the remote is still running has
/// records this pass must not touch. Listing the cache first and the remote second is what makes
/// that safe: `CreateMultipartUpload` initiates the remote upload *before* writing the cache record
/// ([`crate::s3`]), so any range this pass observed was written after its upload existed remotely —
/// and therefore before the in-progress snapshot taken below. An upload absent from that snapshot
/// had already completed or aborted. Asking in the other order would let an upload created between
/// the two calls look abandoned, and this pass would delete a live upload's parts.
pub(super) async fn sweep_mpu_ranges(tier: &Tiering, bucket: &str) -> Result<Reclaimed> {
    let ranges = cache_ranges(tier, bucket).await?;
    if ranges.is_empty() {
        return Ok(Reclaimed::default());
    }
    let live = live_upload_ids(tier, bucket).await?;

    let mut reclaimed = Reclaimed::default();
    for (upload_id, records) in ranges {
        if live.contains(&upload_id) {
            continue;
        }
        // Record keys carry the `0x01` control byte, which the batch `DeleteObjects` XML body
        // cannot represent — single-object deletes only (§6), as with twins.
        let deletes = records.iter().map(|(key, _)| tier.meta.delete(bucket, key));
        futures::future::try_join_all(deletes).await?;
        reclaimed.uploads += 1;
        reclaimed.bytes += records.iter().map(|(_, bytes)| bytes).sum::<u64>();
    }
    Ok(reclaimed)
}

/// What one sweep returned. The bytes are what a pressured pass counts against its target (§8, rung
/// 0) — mostly stashed part ciphertext, since the records themselves are tiny.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Reclaimed {
    pub(super) uploads: usize,
    pub(super) bytes: u64,
}

impl std::ops::AddAssign for Reclaimed {
    fn add_assign(&mut self, other: Self) {
        self.uploads += other.uploads;
        self.bytes += other.bytes;
    }
}

/// Every mpu record in `<meta>` with its size, grouped by the upload it belongs to.
async fn cache_ranges(tier: &Tiering, bucket: &str) -> Result<HashMap<String, Vec<(String, u64)>>> {
    let prefix = meta::mpu_scan_prefix();
    let mut ranges: HashMap<String, Vec<(String, u64)>> = HashMap::new();
    let mut token = None;
    loop {
        let page = tier
            .meta
            .list(
                bucket,
                Some(prefix.clone()),
                None,
                token.take(),
                None,
                Some(PAGE_KEYS),
            )
            .await?;
        for obj in page.contents.unwrap_or_default() {
            let Some(key) = obj.key else { continue };
            // A malformed key under this prefix has no upload to judge it against, so leaving it is
            // the only safe reading — it is not evidence of an abandoned upload.
            if let Some(id) = meta::parse_mpu_upload_id(&key) {
                let bytes = obj.size.unwrap_or(0).max(0) as u64;
                ranges
                    .entry(id.to_string())
                    .or_default()
                    .push((key.clone(), bytes));
            }
        }
        match page.next_continuation_token {
            Some(t) => token = Some(t),
            None => return Ok(ranges),
        }
    }
}

async fn live_upload_ids(tier: &Tiering, bucket: &str) -> Result<HashSet<String>> {
    let mut live = HashSet::new();
    let mut key_marker = None;
    let mut upload_id_marker = None;
    loop {
        let page = tier
            .remote
            .list_multipart_uploads(bucket, None, None, key_marker, upload_id_marker, None)
            .await?;
        live.extend(
            page.uploads
                .unwrap_or_default()
                .into_iter()
                .filter_map(|u| u.upload_id),
        );
        if page.is_truncated != Some(true) {
            return Ok(live);
        }
        key_marker = page.next_key_marker;
        upload_id_marker = page.next_upload_id_marker;
        // A truncated page that carries no markers would loop forever re-reading the first one.
        if key_marker.is_none() && upload_id_marker.is_none() {
            return Ok(live);
        }
    }
}
