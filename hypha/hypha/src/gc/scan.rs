//! §8's probabilistic scan — how the scavenger finds eviction candidates without walking the
//! keyspace.
//!
//! **Sampling, not a walk.** A *probe* lists from a random position and reads a few pages. Nothing
//! tracks a cursor, and the position is fresh every time: a rotating cursor would make eviction
//! pressure correlate with key *name* — keys early in the keyspace examined on every boot, keys late
//! in it only under sustained pressure — and would have both replicas sweep in lockstep after a
//! failover. Sampling also keeps scan cost proportional to the pressure rather than to the keyspace,
//! which is what a full loop over a cold, mostly-untouched bucket spends its round trips on.
//!
//! **The position is biased, and the bias is corrected downstream.** `start-after` takes a key, so a
//! random position is a random key-shaped string and the probe lands on the first key at or after it
//! — uniform over the *keyspace*, not over keys, which favours regions sitting behind large gaps.
//! [`Yields`] is one correction; §6's prefix-distribution hint, once it exists, is the other.

use std::collections::HashMap;

use rand::Rng as _;

use hypha_core::error::Result;
use hypha_core::{meta, Backend};

use crate::tier::Tiering;

const PAGE_KEYS: i32 = 1000;

/// How fast a bucket's yield forgets what it used to be. Slow enough that one unlucky probe of a
/// genuinely cold bucket doesn't cost it its share, fast enough to notice a bucket going cold.
const YIELD_SMOOTHING: f64 = 0.3;

/// The yield credited to a bucket nothing has probed yet — deliberately optimistic, so a new bucket
/// is sampled before it has earned it rather than after.
const UNPROBED_YIELD: f64 = 1.0;

/// Something a probe found that holds reclaimable plaintext, with everything eviction needs to judge
/// it and to CAS it.
pub(super) struct Candidate {
    pub(super) bucket: String,
    pub(super) artifact: Artifact,
    /// The version token the eviction conditions on (§8): a writer — or a fresher rehydrate — landing
    /// anywhere between the gates and the reclaim moves it, and the conditional write fails instead of
    /// discarding what landed.
    pub(super) etag: String,
    /// Plaintext bytes, since a cache body *is* plaintext — so this is what reclaiming it returns.
    pub(super) bytes: u64,
    /// The tie-break within one age bucket (§8). Meaningful for a body, where rehydration lands a
    /// fresh mtime so a just-restored one sorts young; for a shadow it records only when the shadow
    /// landed, since reads never move it — which is why the ring, not this, has to order shadows.
    pub(super) mtime_ms: i64,
}

/// The two things in the cache that hold a client's plaintext, and they are reclaimed differently.
pub(super) enum Artifact {
    /// A live client body at bare K in `<data>`. Reclaiming it means tombstoning K, so it answers to
    /// all three of §8's gates.
    Body(String),
    /// A rehydrated composite's plaintext in `<meta>` (§6), keyed by the digest of K — so this carries
    /// the shadow key, and **K is not recoverable from it**. That shapes both halves: the ring must
    /// have been fed this same key ([`super::Plaintext::InShadow`]), and the durability gates are
    /// unavailable *and* unnecessary, since a shadow is a copy of a composite the remote demonstrably
    /// holds and K's tombstone stands throughout.
    Shadow(String),
}

impl Candidate {
    /// What the ring is asked about. Shadow keys lead with two `0x01` bytes, which client keys may not
    /// contain, so bodies and shadows share one ring without any chance of collision.
    pub(super) fn qualified(&self) -> String {
        let key = match &self.artifact {
            Artifact::Body(key) => key,
            Artifact::Shadow(shadow) => shadow,
        };
        super::ring::qualified(&self.bucket, key)
    }
}

pub(super) struct Probed {
    pub(super) candidates: Vec<Candidate>,
    /// What the yield is per — a probe that ran out of keyspace read fewer pages than its budget.
    pub(super) pages: usize,
}

/// What one probe taught [`Yields`], carried back to the actor that owns them once the pass — which
/// runs off the actor's task — has taken the candidates for itself.
pub(super) struct ProbeYield {
    pub(super) bucket: String,
    pub(super) candidates: usize,
    pub(super) pages: usize,
}

impl Probed {
    pub(super) fn yielded(&self, bucket: &str) -> ProbeYield {
        ProbeYield {
            bucket: bucket.to_string(),
            candidates: self.candidates.len(),
            pages: self.pages,
        }
    }
}

/// Sample `bucket`'s live client bodies: list `<data>` from a random position and keep what is not a
/// tombstone.
pub(super) async fn probe_bodies(tier: &Tiering, bucket: &str, pages: usize) -> Result<Probed> {
    let sampled = sample(&tier.data, bucket, None, random_key_position(), pages).await?;
    Ok(sampled.map(|entry| {
        // A tombstone holds no bytes worth reclaiming and has already been evicted or deleted.
        meta::classify_entry(entry.bytes as i64, &entry.etag)
            .is_none()
            .then(|| entry.into_candidate(bucket, Artifact::Body))
    }))
}

/// Sample `bucket`'s shadow bodies (§6). A prefix scan, because a shadow key is a digest and there is
/// no client key to derive one from — which is also why this is the only way GC ever sees a shadow.
///
/// Every entry under the prefix is a candidate: unlike `<data>`, this range holds nothing else.
pub(super) async fn probe_shadows(tier: &Tiering, bucket: &str, pages: usize) -> Result<Probed> {
    let prefix = meta::shadow_scan_prefix();
    let position = format!("{prefix}{}", random_digest_position());
    let sampled = sample(&tier.meta, bucket, Some(prefix), position, pages).await?;
    Ok(sampled.map(|entry| Some(entry.into_candidate(bucket, Artifact::Shadow))))
}

/// One listing entry, before anything has decided what kind of artifact it is.
struct Entry {
    key: String,
    etag: String,
    bytes: u64,
    mtime_ms: i64,
}

impl Entry {
    fn into_candidate(self, bucket: &str, artifact: impl FnOnce(String) -> Artifact) -> Candidate {
        Candidate {
            bucket: bucket.to_string(),
            artifact: artifact(self.key),
            etag: self.etag,
            bytes: self.bytes,
            mtime_ms: self.mtime_ms,
        }
    }
}

struct Sampled {
    entries: Vec<Entry>,
    pages: usize,
}

impl Sampled {
    fn map(self, classify: impl FnMut(Entry) -> Option<Candidate>) -> Probed {
        Probed {
            candidates: self.entries.into_iter().filter_map(classify).collect(),
            pages: self.pages,
        }
    }
}

/// **One wrap, and only for an overshoot.** A random position past the last key is a legitimate
/// outcome of sampling the keyspace uniformly rather than the keys, and a probe that returns nothing
/// would teach [`Yields`] the bucket is cold when what it hit was the tail. So a listing that returned
/// *no entries at all* — the exact signature of an overshoot — restarts once from the beginning.
///
/// Deliberately not conditioned on finding no *candidates*: a page full of tombstones is real
/// evidence that the bucket yields nothing there, and wrapping on it would pin every probe of a
/// mostly-tombstoned bucket to the head of the keyspace — reinventing the fixed cursor, and its
/// correlation between eviction pressure and key name, that sampling exists to avoid.
async fn sample(
    backend: &Backend,
    bucket: &str,
    prefix: Option<String>,
    position: String,
    page_budget: usize,
) -> Result<Sampled> {
    let sampled = read_pages(backend, bucket, prefix.clone(), Some(position), page_budget).await?;
    if !sampled.entries.is_empty() {
        return Ok(sampled);
    }
    read_pages(backend, bucket, prefix, None, page_budget).await
}

async fn read_pages(
    backend: &Backend,
    bucket: &str,
    prefix: Option<String>,
    start_after: Option<String>,
    page_budget: usize,
) -> Result<Sampled> {
    let mut entries = Vec::new();
    let mut token = None;
    let mut pages = 0;
    while pages < page_budget {
        let page = backend
            .list(
                bucket,
                prefix.clone(),
                None,
                token.take(),
                start_after.clone().filter(|_| pages == 0),
                Some(PAGE_KEYS),
            )
            .await?;
        pages += 1;
        for obj in page.contents.unwrap_or_default() {
            let (Some(key), Some(etag)) = (obj.key, obj.e_tag) else {
                continue;
            };
            entries.push(Entry {
                key,
                etag: etag.trim_matches('"').to_string(),
                bytes: obj.size.unwrap_or(0).max(0) as u64,
                mtime_ms: obj
                    .last_modified
                    .and_then(|t| t.to_millis().ok())
                    .unwrap_or_default(),
            });
        }
        match page.next_continuation_token {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    Ok(Sampled { entries, pages })
}

/// Enough characters that positions are fine-grained relative to any realistic keyspace, few enough
/// that each one still lands well inside it rather than past the end.
const POSITION_LEN: usize = 3;

/// Printable ASCII, which is where real client-key distributions live. Positions outside it are
/// unnecessary rather than limiting — every key is reachable from *some* position, because a probe
/// lands on the first key at or after it.
fn random_key_position() -> String {
    let mut rng = rand::thread_rng();
    (0..POSITION_LEN)
        .map(|_| rng.gen_range(b'!'..=b'~') as char)
        .collect()
}

/// A position *within* the shadow range, so it must be drawn from base64url's alphabet — a printable
/// ASCII position could sort outside the prefix entirely and turn every probe into an overshoot.
fn random_digest_position() -> String {
    const B64URL: &[u8] = b"-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
    let mut rng = rand::thread_rng();
    (0..POSITION_LEN)
        .map(|_| B64URL[rng.gen_range(0..B64URL.len())] as char)
        .collect()
}

/// Where to probe, learned (§8). Each bucket carries a running **cold yield** — evictable candidates
/// per page — and probes are handed out in proportion to it, so a bucket that is mostly working set
/// stops consuming probes a mostly-cold one can use.
pub(super) struct Yields {
    per_bucket: HashMap<String, f64>,
    /// The share handed out evenly regardless of yield. Pure proportional sampling locks onto early
    /// winners and never revisits a bucket that went cold later — a scan that learns once and then
    /// stops learning.
    floor: f64,
}

impl Yields {
    pub(super) fn new(floor: f64) -> Self {
        Yields {
            per_bucket: HashMap::new(),
            floor: floor.clamp(0.0, 1.0),
        }
    }

    pub(super) fn observe(&mut self, probed: &ProbeYield) {
        if probed.pages == 0 {
            return;
        }
        let observed = probed.candidates as f64 / probed.pages as f64;
        let entry = self
            .per_bucket
            .entry(probed.bucket.clone())
            .or_insert(observed);
        *entry = *entry * (1.0 - YIELD_SMOOTHING) + observed * YIELD_SMOOTHING;
    }

    /// Drop buckets that no longer exist, so a deleted bucket's yield cannot keep influencing the
    /// weighting of the ones that remain.
    pub(super) fn retain(&mut self, live: &[String]) {
        self.per_bucket.retain(|bucket, _| live.contains(bucket));
    }

    /// Draw `probes` buckets, with replacement, weighted by yield — so a pass can spend several
    /// probes on the one bucket that is actually yielding.
    pub(super) fn sample(&self, buckets: &[String], probes: usize) -> Vec<String> {
        if buckets.is_empty() {
            return Vec::new();
        }
        let weights: Vec<f64> = buckets
            .iter()
            .map(|b| self.per_bucket.get(b).copied().unwrap_or(UNPROBED_YIELD))
            .collect();
        let total: f64 = weights.iter().sum();
        let even = self.floor / buckets.len() as f64;
        // Every bucket yielding nothing leaves the proportional term undefined; the floor is then the
        // whole weighting, which is the right answer — nothing is known, so sample evenly.
        let scale = if total > 0.0 {
            (1.0 - self.floor) / total
        } else {
            0.0
        };

        let mut rng = rand::thread_rng();
        (0..probes)
            .map(|_| {
                let mut draw = rng.gen::<f64>() * (even * buckets.len() as f64 + total * scale);
                for (bucket, weight) in buckets.iter().zip(&weights) {
                    draw -= even + weight * scale;
                    if draw <= 0.0 {
                        return bucket.clone();
                    }
                }
                // Floating-point slack at the very top of the range: the last bucket is the one the
                // cumulative sum was walking toward.
                buckets[buckets.len() - 1].clone()
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probed(bucket: &str, candidates: usize, pages: usize) -> ProbeYield {
        ProbeYield {
            bucket: bucket.to_string(),
            candidates,
            pages,
        }
    }

    fn share(drawn: &[String], bucket: &str) -> f64 {
        drawn.iter().filter(|b| *b == bucket).count() as f64 / drawn.len() as f64
    }

    #[test]
    fn random_positions_are_key_shaped_and_vary() {
        let positions: std::collections::HashSet<String> =
            (0..200).map(|_| random_key_position()).collect();
        assert!(
            positions.len() > 100,
            "a cursor-free scan needs fresh positions, got {} distinct in 200",
            positions.len()
        );
        for position in &positions {
            assert_eq!(position.len(), POSITION_LEN);
            assert!(meta::validate_client_key(position).is_ok());
        }
    }

    #[test]
    fn a_yielding_bucket_takes_most_of_the_probes() {
        let mut yields = Yields::new(0.2);
        let buckets = vec!["cold".to_string(), "working-set".to_string()];
        for _ in 0..10 {
            yields.observe(&probed("cold", 50, 1));
            yields.observe(&probed("working-set", 0, 1));
        }
        let drawn = yields.sample(&buckets, 4000);
        assert!(
            share(&drawn, "cold") > 0.8,
            "cold share was {}",
            share(&drawn, "cold")
        );
    }

    /// The floor is the whole point of the weighting: a bucket that went cold after losing its share
    /// has to be re-probed eventually, or the scan learns once and stops.
    #[test]
    fn the_floor_keeps_probing_a_bucket_that_yields_nothing() {
        let mut yields = Yields::new(0.4);
        let buckets = vec!["cold".to_string(), "working-set".to_string()];
        for _ in 0..50 {
            yields.observe(&probed("cold", 100, 1));
            yields.observe(&probed("working-set", 0, 1));
        }
        let drawn = yields.sample(&buckets, 4000);
        let starved = share(&drawn, "working-set");
        assert!(
            starved > 0.1,
            "the exploration floor must not be starved out, got {starved}"
        );
    }

    #[test]
    fn unprobed_buckets_are_sampled_before_they_have_earned_it() {
        let mut yields = Yields::new(0.0);
        let buckets = vec!["known-cold".to_string(), "brand-new".to_string()];
        for _ in 0..10 {
            yields.observe(&probed("known-cold", 0, 1));
        }
        let drawn = yields.sample(&buckets, 1000);
        assert!(
            share(&drawn, "brand-new") > 0.9,
            "an unprobed bucket must outweigh one known to yield nothing"
        );
    }

    #[test]
    fn a_retired_bucket_stops_influencing_the_weighting() {
        let mut yields = Yields::new(0.0);
        yields.observe(&probed("deleted", 100, 1));
        yields.observe(&probed("live", 1, 1));
        yields.retain(&["live".to_string()]);
        let drawn = yields.sample(&["live".to_string()], 10);
        assert!(drawn.iter().all(|b| b == "live"));
    }

    /// A shadow position that fell outside the shadow prefix would overshoot the range on every
    /// probe, and the wrap would then pin every shadow probe to the head of the range.
    #[test]
    fn shadow_positions_stay_inside_the_shadow_range() {
        let prefix = meta::shadow_scan_prefix();
        for _ in 0..200 {
            let position = format!("{prefix}{}", random_digest_position());
            assert!(position.starts_with(&prefix));
            // Sorts at or before some real shadow key, and after the prefix itself.
            assert!(position > prefix);
            assert!(position < format!("{prefix}\u{7f}"));
        }
    }

    /// Bodies and shadows share one ring, so their qualified forms must not be able to collide — the
    /// shadow key's doubled `0x01` lead is what guarantees it, since client keys may not contain it.
    #[test]
    fn body_and_shadow_ring_keys_cannot_collide() {
        let candidate = |artifact| Candidate {
            bucket: "b".into(),
            artifact,
            etag: "e".into(),
            bytes: 1,
            mtime_ms: 0,
        };
        let body = candidate(Artifact::Body("k".into()));
        let shadow = candidate(Artifact::Shadow(meta::shadow_key("k")));
        assert_ne!(body.qualified(), shadow.qualified());
        assert!(shadow.qualified().contains(char::from(meta::CTRL)));
        assert!(
            !body.qualified().contains(char::from(meta::CTRL)),
            "a client key cannot contain the control byte, which is what keeps the two apart"
        );
    }

    #[test]
    fn sampling_an_empty_bucket_set_draws_nothing() {
        assert!(Yields::new(0.2).sample(&[], 8).is_empty());
    }
}
