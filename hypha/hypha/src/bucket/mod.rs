//! The bucket lifecycle: what a run may assume about a bucket's cache projections, and how a run
//! that inherits doubt about them gets back to certainty.
//!
//! One client bucket is three physical buckets — `<remote><b>`, the source of truth for existence,
//! plus the `<data><b>`/`<meta><b>` cache projections. Two per-bucket markers (§6) say what this run
//! may assume about that pair, and between them decide everything below:
//!
//! - the **sync** marker: the cache namespace is authoritative. Its absence means the projections
//!   cannot be trusted at all, which is what a lost cache volume looks like.
//! - the **clean** marker: the previous run proved it had indexed every acked write, so this run
//!   inherits a complete pending set (§6). Its absence means the set has to be re-derived.
//!
//! # Phases
//!
//! [`Readiness`] is the whole of what the data plane branches on, and each phase is a claim about
//! the cache that the rest of the crate is entitled to rely on:
//!
//! - `Ready` — the cache namespace is authoritative, so **an absent key is the client's 404**. That
//!   is the load-bearing claim: it is what lets a read answer without touching the remote, and what
//!   makes a lost cache volume a correctness failure rather than a slow one
//!   ([`crate::volume_watch`]).
//! - `Restoring` — the remote is the read source of truth and writes run **durable**, committing on
//!   the remote. The bucket is served correctly throughout, which is why the phase can outlive a
//!   failed pass: only the sync marker waits on the retry.
//! - `Absent` — a definitive `NoSuchBucket`, not "unclassified". hypha owns both backends outright,
//!   so the map [`resolve_all`] publishes at startup is the complete set of buckets.
//!
//! # The two recoveries
//!
//! [`restore`] (**R1**) rebuilds the cache projection of a bucket whose sync marker is gone;
//! [`rebuild`] (**R2**) re-derives the pending markers of a bucket whose clean marker is gone. Their
//! premises are opposites — R1 may assume nothing about the cache namespace, R2 may assume it is
//! authoritative — which is why they are two passes rather than one pass with a flag, and why they
//! share no traversal: R1 walks the remote alone, while R2 needs both sides correlated. A common
//! walk would mean each pass carrying the other's machinery to no end.
//!
//! Which one a bucket needs is decided once, at startup, from the two markers ([`resolve_all`]) —
//! and both markers absent is an R1, since a bucket with no trustworthy namespace has nothing for R2
//! to work from, and R1 leaves the pending set empty and complete on its own. Neither pass
//! overwrites a cache entry from a listing snapshot, which is what lets both run against a bucket
//! that is served the whole time.
//!
//! **They never run together on a bucket**, structurally rather than by convention: [`ctl`] holds
//! one recovery slot per bucket and one task draining it, so even a retry cannot put two in flight.

mod ctl;
mod gate;
mod rebuild;
mod restore;

pub(crate) use ctl::resolve_all;
pub use ctl::{spawn, BucketCtl, Readiness};
pub use gate::WriteGuard;
