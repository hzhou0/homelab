//! The two recoveries, and the one thing they have in common.
//!
//! hypha recovers from exactly two failures, one per marker (§6). Their premises are opposites — a
//! [`restore`] may assume nothing about the cache namespace, a [`rebuild_pending`] may assume it is
//! authoritative — which is why they are two passes rather than one pass with a flag.
//!
//! **They never run together on a bucket.** Which one a bucket needs is decided once, from its two
//! markers, by the startup resolution that dispatches it ([`crate::bucket_ctl::resolve_all`]) — and
//! both markers absent is a restore, since a bucket with no trustworthy namespace has nothing for a
//! pending rebuild to work from, and the restore leaves the pending set empty and complete on its
//! own. The bucket-control actor holds one recovery slot per bucket, so even a retry cannot put two
//! in flight.
//!
//! Neither pass overwrites a cache entry from a listing snapshot, which is what lets both run
//! against a bucket that is being served the whole time.
//!
//! They share no traversal. The restore walks the remote cursor alone — its absence check is made
//! under K's own lock, so a cache listing could only ever be a stale hint about the same question —
//! while the rebuild genuinely needs both sides correlated, for the remote's framed length and for
//! the remote-only keys invariant I2 is about. A common walk would mean each pass carrying the
//! other's machinery to no end.

mod rebuild;
mod restore;

pub(crate) use rebuild::rebuild_pending;
pub(crate) use restore::restore;
