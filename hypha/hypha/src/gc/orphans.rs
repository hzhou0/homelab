//! Reclaims plaintext shadows made unreachable by later writes.
//!
//! Cached writes cannot cheaply know whether they superseded a composite, so all of them enqueue an
//! obligation. Listing after receiving a batch is load-bearing: any shadow in that snapshot predates
//! the write, while a later shadow belongs to a newer generation and must remain live.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinSet;

use hypha_core::error::{Error, Result};
use hypha_core::meta;

use crate::bucket::{BucketCtl, Readiness};
use crate::tier::{quote, Tiering};

const DRAIN_BATCH: usize = 256;

/// Pages of a bucket's shadow range read at once. The range holds one entry per rehydrated composite,
/// so this is a ceiling rather than a budget — a bucket past it keeps its later shadows for the next
/// listing, which costs a deferred reclaim and nothing else.
const RANGE_PAGES: usize = 16;
const PAGE_KEYS: i32 = 1000;

/// What the queue carries. As in [`crate::markers`], the seal is a **message, not the channel
/// closing**: the serving future owns the [`OrphanSeal`], so an aborted process closes the channel
/// exactly as a drain would, and closure alone must not authorize a marker.
enum OrphanMsg {
    Owed(Superseded),
    Seal,
}

/// A key whose shadow a client write may have just orphaned.
///
/// Deliberately *not* carrying the generation the write replaced, even where the path knew it: asking K
/// what it currently holds is correct in every case, including the ones a remembered generation would
/// also get right, and it costs a HEAD only for a key that actually has a shadow. One path to reason
/// about beats a fast path for a rare case.
struct Superseded {
    bucket: String,
    key: String,
}

impl Superseded {
    fn dedup_key(&self) -> (String, String) {
        (self.bucket.clone(), self.key.clone())
    }
}

struct OrphanQueue {
    tier: Tiering,
    /// Durable mode never rehydrates, so it has no shadows to orphan — obligations there would cost a
    /// listing per batch to discover nothing.
    cached: bool,
    /// Weak for the same reason [`crate::markers`]'s is: the service must not hold the channel open, or
    /// its closing would prove nothing about the run having drained.
    tx: mpsc::WeakUnboundedSender<OrphanMsg>,
    buckets: BucketCtl,
}

/// The write path's handle.
#[derive(Clone)]
pub(crate) struct Orphans {
    queue: Arc<OrphanQueue>,
}

pub(crate) fn spawn(
    tier: Tiering,
    buckets: BucketCtl,
    retry: Duration,
) -> (Orphans, OrphanSeal, OrphanActor) {
    let cached = tier.cached;
    let (tx, rx) = mpsc::unbounded_channel();
    let queue = Arc::new(OrphanQueue {
        cached: tier.cached,
        tier,
        tx: tx.downgrade(),
        buckets,
    });
    (
        Orphans {
            queue: queue.clone(),
        },
        OrphanSeal(tx),
        OrphanActor {
            queue,
            rx,
            retry: retry.max(Duration::from_millis(1)),
            cached,
        },
    )
}

/// Holds the queue open for the life of the run; sending is what authorizes the markers. See
/// [`crate::markers::RunSeal`] for why dropping it must not.
pub(crate) struct OrphanSeal(mpsc::UnboundedSender<OrphanMsg>);

impl OrphanSeal {
    pub(crate) fn seal(self) {
        let _ = self.0.send(OrphanMsg::Seal);
    }
}

impl Orphans {
    /// Hand over a key whose shadow this write may have orphaned. Called after the commit, so it
    /// neither blocks nor fails — and does no work here: whether a shadow exists at all is the actor's
    /// question, and for the overwhelming majority of buckets it answers in one listing for a whole
    /// batch.
    pub(crate) fn owe(&self, bucket: &str, key: &str) {
        if !self.queue.cached {
            return;
        }
        let Some(tx) = self.queue.tx.upgrade() else {
            // Same remedy as a closed marker queue: withdraw the evidence rather than let the drain
            // vouch for a bucket whose obligations this run cannot account for.
            tracing::warn!(bucket, key, "shadow queue closed under a live write");
            self.queue.buckets.unaccount_shadows(bucket);
            return;
        };
        let _ = tx.send(OrphanMsg::Owed(Superseded {
            bucket: bucket.to_string(),
            key: key.to_string(),
        }));
    }
}

pub(crate) struct OrphanActor {
    queue: Arc<OrphanQueue>,
    rx: mpsc::UnboundedReceiver<OrphanMsg>,
    retry: Duration,
    /// Durable mode has nothing for a marker to vouch for.
    cached: bool,
}

impl OrphanActor {
    pub(crate) async fn run(mut self) {
        let mut owed: HashMap<(String, String), Superseded> = HashMap::new();
        let mut batch = Vec::with_capacity(DRAIN_BATCH);
        let mut sealed = false;
        loop {
            tokio::select! {
                n = self.rx.recv_many(&mut batch, DRAIN_BATCH) => {
                    if n == 0 {
                        break; // dropped rather than sealed — the run did not end gracefully
                    }
                    for msg in batch.drain(..) {
                        match msg {
                            OrphanMsg::Owed(s) => { owed.insert(s.dedup_key(), s); }
                            OrphanMsg::Seal => sealed = true,
                        }
                    }
                    self.resolve_all(&mut owed).await;
                    if sealed {
                        break;
                    }
                }
                () = tokio::time::sleep(self.retry), if !owed.is_empty() => {
                    self.resolve_all(&mut owed).await;
                }
            }
        }
        self.resolve_all(&mut owed).await;
        match (sealed, owed.is_empty()) {
            (true, true) => self.mark_clean().await,
            (true, false) => tracing::warn!(
                owed = owed.len(),
                "shadow obligations still owed at drain; no shadow-clean markers written"
            ),
            (false, _) => {
                tracing::warn!("shadow queue closed without a drain; no shadow-clean markers")
            }
        }
    }

    /// Resolve every obligation, retaining the ones that could not be settled. Grouped by bucket
    /// because the shadow range is per bucket and one listing of it serves every obligation there.
    async fn resolve_all(&self, owed: &mut HashMap<(String, String), Superseded>) {
        let mut by_bucket: HashMap<String, Vec<Superseded>> = HashMap::new();
        for (_, superseded) in owed.drain() {
            by_bucket
                .entry(superseded.bucket.clone())
                .or_default()
                .push(superseded);
        }
        for (bucket, batch) in by_bucket {
            // The state map, not the backend, decides a bucket is gone — the same rule
            // [`crate::markers`] follows, and here it also spares a listing that can only 404.
            if self.queue.buckets.readiness(&bucket) == Readiness::Absent {
                tracing::info!(bucket, "shadow obligations dropped; the bucket was deleted");
                continue;
            }
            let shadows = match list_shadows(&self.queue.tier, &bucket, RANGE_PAGES).await {
                Ok(shadows) => shadows,
                // The bucket is gone, so its whole `<meta>` projection went with it (§7) and there is
                // nothing left to reclaim. Dropped rather than retried for the reason
                // [`crate::markers`] spells out: one permanently owed obligation withholds the marker
                // of *every* bucket at drain, so a single deleted bucket would send the next run
                // sweeping buckets it had no reason to doubt.
                Err(Error::NoSuchBucket) => {
                    tracing::info!(bucket, "shadow obligations dropped; the bucket was deleted");
                    continue;
                }
                // Anything else and nothing can be judged, so the whole batch stays owed — which is
                // what withholds the marker if it never clears.
                Err(e) => {
                    tracing::warn!(bucket, error = %e,
                        "shadow range unreadable; obligations retained");
                    for superseded in batch {
                        owed.insert(superseded.dedup_key(), superseded);
                    }
                    continue;
                }
            };
            for superseded in batch {
                // The common case, at no cost: this bucket has no shadow for the key, so the write
                // orphaned nothing.
                let shadow = meta::shadow_key(&superseded.key);
                if !shadows.contains(&shadow) {
                    continue;
                }
                if let Err(e) = self.reclaim(&superseded, &shadow).await {
                    tracing::warn!(bucket = superseded.bucket, key = superseded.key, error = %e,
                        "orphaned shadow not reclaimed; retrying");
                    owed.insert(superseded.dedup_key(), superseded);
                }
            }
        }
    }

    /// Drop the shadow unless K still names its generation.
    ///
    /// That test is also what keeps this from racing a rehydrate which has *just* landed a shadow for a
    /// newer generation of the same key: K names the new generation, the shadow carries it too, and the
    /// shadow is kept. Deleting it would have been safe — a shadow is only ever a copy of what the
    /// remote holds — but it would discard a transfer a client just waited for.
    async fn reclaim(&self, superseded: &Superseded, shadow: &str) -> Result<()> {
        let tier = &self.queue.tier;
        let (bucket, key) = (&superseded.bucket, &superseded.key);

        let head = match tier.meta.head(bucket, shadow).await {
            Ok(head) => head,
            // Already gone: a concurrent reclaim, or the bucket went away.
            Err(Error::NotFound) | Err(Error::NoSuchBucket) => return Ok(()),
            Err(e) => return Err(e),
        };
        let md = head.metadata.clone().unwrap_or_default();
        // No generation recorded ⇒ not a shadow this hypha wrote; judging it is not this pass's place.
        let Some(shadow_cetag) = md.get(meta::CETAG) else {
            return Ok(());
        };
        if still_reachable(tier, bucket, key, shadow_cetag).await? {
            return Ok(());
        }
        drop_shadow(tier, bucket, shadow, head.e_tag()).await
    }

    async fn mark_clean(&self) {
        if !self.cached {
            return;
        }
        let marker = meta::shadow_clean_marker_key();
        for bucket in self.queue.buckets.shadows_accounted() {
            if let Err(e) = self
                .queue
                .tier
                .meta
                .put_small(&bucket, &marker, Vec::new(), HashMap::new(), None, None)
                .await
            {
                tracing::warn!(bucket, error = %e,
                    "shadow-clean marker not written; next run sweeps");
            }
        }
    }
}

/// The backstop (§8): judge **every** shadow in a bucket against the key it names. Owed by a bucket
/// whose shadow-clean marker was absent at startup.
///
/// This is the pass the marker exists to avoid, and the only one that can find an orphan no obligation
/// covered — a shadow left behind by a process that crashed, or by one whose queue never drained. It is
/// also the only reader of the shadow's `ck` back-pointer: a shadow whose K is gone cannot be reached
/// from the key side at all, so there is nowhere else the question can be asked from.
///
/// Not a readiness gate — an orphan is invisible to clients, so this runs behind startup resolution and
/// its only deadline is the drain that would like to vouch for the bucket.
pub(crate) async fn sweep(tier: &Tiering, bucket: &str) -> Result<usize> {
    let mut reclaimed = 0;
    for shadow in list_shadows(tier, bucket, RANGE_PAGES).await? {
        let head = match tier.meta.head(bucket, &shadow).await {
            Ok(head) => head,
            Err(Error::NotFound) => continue,
            Err(e) => return Err(e),
        };
        let md = head.metadata.clone().unwrap_or_default();
        // Written before the back-pointer existed, or not a shadow this hypha wrote: either way there
        // is no key to judge it against.
        let (Some(cetag), Some(encoded)) = (md.get(meta::CETAG), md.get(meta::SHADOW_CLIENT_KEY))
        else {
            continue;
        };
        // An unreadable back-pointer is "cannot judge", never "orphan" — the alternative is deleting a
        // live shadow because its metadata was surprising.
        let Some(key) = meta::decode_shadow_client_key(encoded) else {
            tracing::warn!(bucket, "shadow back-pointer unreadable; left in place");
            continue;
        };
        if still_reachable(tier, bucket, &key, cetag).await? {
            continue;
        }
        drop_shadow(tier, bucket, &shadow, head.e_tag()).await?;
        reclaimed += 1;
    }
    Ok(reclaimed)
}

/// Whether K still names `cetag` as its current generation — the only thing that makes a shadow
/// reachable (§6). A K that is absent, a live body, or a tombstone of another generation leaves the
/// shadow unreachable forever.
///
/// A **transition mark** answers `true`: K is mid-bracket and its settle may land a tombstone carrying
/// exactly this generation, so reading it as unreachable would delete a shadow about to become live
/// again.
async fn still_reachable(tier: &Tiering, bucket: &str, key: &str, cetag: &str) -> Result<bool> {
    let head = match tier.data.head(bucket, key).await {
        Ok(head) => head,
        Err(Error::NotFound) | Err(Error::NoSuchBucket) => return Ok(false),
        Err(e) => return Err(e),
    };
    let md = head.metadata.clone().unwrap_or_default();
    Ok(match meta::tomb_kind(&md) {
        Some(meta::TombKind::Evict) => md.get(meta::CETAG).map(String::as_str) == Some(cetag),
        Some(meta::TombKind::Transit) => true,
        None => false,
    })
}

/// Conditional on the ETag the caller observed, so a rehydrate that landed a fresher shadow in the gap
/// keeps it.
async fn drop_shadow(
    tier: &Tiering,
    bucket: &str,
    shadow: &str,
    observed: Option<&str>,
) -> Result<()> {
    let etag = observed.unwrap_or_default().trim_matches('"');
    match tier.meta.delete_if_match(bucket, shadow, quote(etag)).await {
        Ok(()) | Err(Error::PreconditionFailed) | Err(Error::NotFound) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Every shadow key in `bucket`, up to `pages` pages of the range.
async fn list_shadows(tier: &Tiering, bucket: &str, pages: usize) -> Result<HashSet<String>> {
    let prefix = meta::shadow_scan_prefix();
    let mut shadows = HashSet::new();
    let mut token = None;
    for _ in 0..pages {
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
        shadows.extend(
            page.contents
                .unwrap_or_default()
                .into_iter()
                .filter_map(|o| o.key),
        );
        match page.next_continuation_token {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    Ok(shadows)
}

/// Run [`sweep`] for a bucket whose shadow-clean marker was absent, and account for it if the sweep
/// succeeds. Nothing on the serving path waits on it — an orphan is invisible to clients, and a sweep
/// that fails simply leaves the bucket unaccounted, so the drain withholds its marker and the next run
/// tries again. Positive evidence only, as everywhere else in this pair (§7).
///
/// It joins `sweeps` rather than running detached so the drain can wait for it: a sweep killed a moment
/// before it would have accounted for its bucket costs the next run a listing for nothing.
pub(crate) fn dispatch_sweep(
    sweeps: &mut JoinSet<()>,
    tier: Tiering,
    buckets: BucketCtl,
    bucket: String,
) {
    sweeps.spawn(async move {
        match sweep(&tier, &bucket).await {
            Ok(reclaimed) => {
                if reclaimed > 0 {
                    tracing::info!(bucket, reclaimed, "reclaimed orphaned shadow bodies");
                }
                buckets.account_shadows_for(&bucket);
            }
            Err(e) => tracing::warn!(bucket, error = %e,
                "orphaned-shadow sweep failed; the bucket ends the run unaccounted"),
        }
    });
}
