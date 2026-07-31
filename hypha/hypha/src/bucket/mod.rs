//! Bucket cache authority and recovery.
//!
//! A sync marker makes the cache namespace authoritative; without it, reads resolve remotely while
//! restore rebuilds the projection. A clean marker proves the pending set was complete at shutdown;
//! without it, rebuild re-derives pending work from an otherwise trusted cache namespace. These
//! recoveries have opposite premises and are serialized per bucket.

mod ctl;
mod gate;
mod rebuild;
mod restore;

pub(crate) use ctl::resolve_all;
pub use ctl::{spawn, BucketCtl, Readiness};
pub use gate::WriteGuard;
