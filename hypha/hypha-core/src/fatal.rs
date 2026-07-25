//! Invariant violations hypha cannot serve through.
//!
//! Nothing but hypha writes to the remote buckets — that assumption underpins the whole tombstone
//! model, so an object whose tail trailer does not authenticate is not a stray to be tidied away.
//! It means either something else has write access to the deployment's buckets, or this process is
//! holding the wrong trailer key / reading a format it does not understand. In every one of those
//! cases hypha's own data is intact and hypha's picture of it is wrong, so continuing would either
//! serve wrong answers or destroy data. Terminate instead, with a code an operator can alert on.

/// Exit status for a remote object whose trailer does not verify. Distinct from any conventional
/// status (and from `sysexits.h`) so a supervisor can tell this apart from an ordinary crash.
pub const EXIT_FOREIGN_OBJECT: i32 = 86;

/// Log the violating object and terminate with [`EXIT_FOREIGN_OBJECT`]. Called from every site that
/// observes a trailer failing to authenticate, so no path can quietly route around one.
pub fn foreign_object(bucket: &str, key: &str) -> ! {
    tracing::error!(
        bucket,
        key,
        "remote object carries no verifiable hypha trailer: either something other than hypha \
         writes to this bucket, or the trailer key is wrong. Terminating rather than serving or \
         deleting."
    );
    std::process::exit(EXIT_FOREIGN_OBJECT)
}
