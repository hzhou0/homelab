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
//! [`Yields`] corrects it across buckets and [`Prefixes`] within one.

use std::collections::HashMap;
use std::sync::Arc;

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
    /// Shared with the probe that found it rather than copied per candidate: one probe can return
    /// thousands, and they all came from the same bucket.
    pub(super) bucket: Arc<str>,
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

struct Probed {
    candidates: Vec<Candidate>,
    /// What the yield is per — a probe that ran out of keyspace read fewer pages than its budget.
    pages: usize,
    /// Leading prefixes of the *keys* the probe walked past, candidates or not: this is a statement
    /// about where keys are, which is a different question from where evictable ones are.
    prefixes: HashMap<String, usize>,
}

/// What one probe taught the actor's learned state, carried back once the pass — which runs off the
/// actor's task — has taken the candidates for itself.
pub(super) struct ProbeYield {
    pub(super) bucket: String,
    pub(super) candidates: usize,
    pub(super) pages: usize,
    /// Empty for a shadow probe, whose positions are drawn over digests and need no shaping.
    pub(super) prefixes: HashMap<String, usize>,
}

impl Probed {
    fn split(self, bucket: &str) -> (ProbeYield, Vec<Candidate>) {
        (
            ProbeYield {
                bucket: bucket.to_string(),
                candidates: self.candidates.len(),
                pages: self.pages,
                prefixes: self.prefixes,
            },
            self.candidates,
        )
    }

    /// A shadow probe teaches the yields and nothing else: its positions are drawn over digests,
    /// which are uniform over the range by construction, so there is no bias for a distribution to
    /// correct.
    fn split_yield_only(mut self, bucket: &str) -> (ProbeYield, Vec<Candidate>) {
        self.prefixes = HashMap::new();
        self.split(bucket)
    }
}

/// Sample `bucket`'s live client bodies from `position` (drawn by [`Prefixes`]) and keep what is not
/// a tombstone — plus, free of charge, the transition marks passed on the way.
///
/// The marks cost nothing because every entry is classified here anyway to find the candidates, and
/// this is the only listing that ever sees them: [`super::debris`] gives them no walk of their own,
/// since a mark is repaired by any read of its key (§7/§8).
pub(super) async fn probe_bodies(
    tier: &Tiering,
    bucket: &Arc<str>,
    position: String,
    pages: usize,
) -> Result<(ProbeYield, Vec<Candidate>, Vec<String>)> {
    let sampled = sample(&tier.data, bucket, None, position, pages).await?;
    let mut marked = Vec::new();
    let probed = sampled.map(
        |entry| match meta::classify_entry(entry.bytes as i64, &entry.etag) {
            None => Some(entry.into_candidate(bucket, Artifact::Body)),
            // Evict/Delete hold no bytes worth reclaiming and have already been settled.
            Some(meta::TombKind::Transit) => {
                marked.push(entry.key);
                None
            }
            Some(_) => None,
        },
    );
    let (yielded, candidates) = probed.split(bucket);
    Ok((yielded, candidates, marked))
}

/// Sample the two things `<meta>` holds that GC acts on: shadow bodies to evict (§6), and twins that
/// may no longer project any key. One walk, because they are one listing apart — the ranges are
/// adjacent under the `0x01` lead — and neither earns a walk of its own.
///
/// **Aimed at one range or the other, at random.** The ranges are disjoint and not contiguous (range
/// A's markers and mpu records sit between them), so a single position drawn over the whole prefix
/// would land wherever that space happens to be widest rather than where the entries are. Aiming
/// explicitly makes the split a property of the code instead of of the keyspace's shape. A
/// shadow-aimed probe usually reads on into the twins anyway — shadows exist only for composites
/// something read back after eviction, so for most buckets that range is nearly empty — which is why
/// the aim matters mainly for the bucket where it does not spill.
pub(super) async fn probe_meta(
    tier: &Tiering,
    bucket: &Arc<str>,
    pages: usize,
) -> Result<(ProbeYield, Vec<Candidate>, Vec<String>)> {
    let lead = (meta::CTRL as char).to_string();
    let sampled = sample(
        &tier.meta,
        bucket,
        Some(lead),
        random_meta_position(),
        pages,
    )
    .await?;

    let mut twins = Vec::new();
    let probed = sampled.map(|entry| {
        // The base key is a slice of the twin's own, so it is re-derived where the twin is judged
        // rather than carried alongside it.
        if meta::parse_twin(&entry.key).is_some() {
            twins.push(entry.key);
            return None;
        }
        entry
            .key
            .starts_with(&meta::shadow_scan_prefix())
            .then(|| entry.into_candidate(bucket, Artifact::Shadow))
    });
    let (yielded, candidates) = probed.split_yield_only(bucket);
    Ok((yielded, candidates, twins))
}

/// One listing entry, before anything has decided what kind of artifact it is.
struct Entry {
    key: String,
    etag: String,
    bytes: u64,
    mtime_ms: i64,
}

impl Entry {
    fn into_candidate(
        self,
        bucket: &Arc<str>,
        artifact: impl FnOnce(String) -> Artifact,
    ) -> Candidate {
        Candidate {
            bucket: Arc::clone(bucket),
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
        let mut prefixes: HashMap<String, usize> = HashMap::new();
        for entry in &self.entries {
            *prefixes.entry(leading(&entry.key)).or_default() += 1;
        }
        Probed {
            candidates: self.entries.into_iter().filter_map(classify).collect(),
            pages: self.pages,
            prefixes,
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

/// How much of a position [`Prefixes`] shapes. The leading characters are where a keyspace's
/// structure lives — a bucket's keys sit under a handful of `logs/`, `2026/`-style leads — so
/// shaping them is what turns a draw over the string space into a draw over the populated part of
/// it. The rest stays random, which is what still spreads probes *within* a busy prefix.
const SHAPED_LEN: usize = 2;

/// Printable ASCII, which is where real client-key distributions live. Positions outside it are
/// unnecessary rather than limiting — every key is reachable from *some* position, because a probe
/// lands on the first key at or after it.
fn random_key_position() -> String {
    let mut rng = rand::thread_rng();
    (0..POSITION_LEN)
        .map(|_| rng.gen_range(b'!'..=b'~') as char)
        .collect()
}

/// The shaped head of a key, as [`Prefixes`] counts and draws it. Whole **characters**, not bytes: a
/// window cut mid-sequence would have to be repaired into a replacement character, which sorts above
/// nearly every real key and would turn every probe of a non-ASCII prefix into an overshoot. A key
/// shorter than the window contributes its whole self, so single-character keys stay learnable.
fn leading(key: &str) -> String {
    key.chars().take(SHAPED_LEN).collect()
}

/// Where in a bucket to probe, learned — the within-bucket half of the correction [`Yields`] makes
/// across buckets.
///
/// The bias it exists for is coarse and it dominates: a random key-shaped position is uniform over
/// the *string space*, and almost all of that space holds no keys at all — a bucket whose keys all
/// begin `logs/` or `2026-` takes nearly every probe from somewhere before its first key or after
/// its last, so the probes pile onto the head of the one populated run and the tail is examined
/// only by the wrap. Drawing the leading characters from prefixes that were *observed to contain
/// keys* removes that by construction, and it needs nothing persisted: the counts are a sketch of
/// the live keyspace, so a cold start is one pass of unshaped probes, not a wrong answer.
///
/// **Kept honest by an exploration share**, for the same reason [`Yields`] keeps a floor: a
/// distribution that only ever draws where it has already looked cannot discover a prefix that
/// appeared after it started, and a scan that learns once and stops learning is the failure both
/// halves of this file are shaped to avoid.
pub(super) struct Prefixes {
    /// Per bucket, a distribution over observed leading windows. Weights sum to ~1 rather than
    /// counting keys: what matters is a prefix's *share* of the keyspace, and totals that grow with
    /// how long the process has been running would make an old observation outweigh a fresh one.
    per_bucket: HashMap<String, HashMap<String, f64>>,
}

/// Share of positions drawn unshaped. Not configurable, unlike [`Yields`]'s floor: that one trades
/// probes between buckets an operator can see and name, whereas this is an internal property of a
/// sketch — there is no deployment fact that would tell anyone to move it.
const EXPLORE_SHARE: f64 = 0.2;

/// How fast the distribution forgets, matched to [`YIELD_SMOOTHING`] because it is fed by the same
/// probes at the same cadence — one probe's view of a bucket is a few pages, so no single one should
/// be able to reshape where the next round looks.
const PREFIX_SMOOTHING: f64 = YIELD_SMOOTHING;

/// Below this a prefix has been absent from the observations long enough to be noise, and dropping it
/// keeps the map bounded by the keyspace's live shape rather than by everything it has ever had.
const PREFIX_FLOOR: f64 = 0.001;

impl Prefixes {
    pub(super) fn new() -> Self {
        Prefixes {
            per_bucket: HashMap::new(),
        }
    }

    pub(super) fn observe(&mut self, bucket: &str, observed: &HashMap<String, usize>) {
        let total: usize = observed.values().sum();
        if total == 0 {
            return;
        }
        let distribution = self.per_bucket.entry(bucket.to_string()).or_default();
        for weight in distribution.values_mut() {
            *weight *= 1.0 - PREFIX_SMOOTHING;
        }
        for (prefix, count) in observed {
            let share = *count as f64 / total as f64;
            *distribution.entry(prefix.clone()).or_default() += share * PREFIX_SMOOTHING;
        }
        distribution.retain(|_, weight| *weight >= PREFIX_FLOOR);
    }

    pub(super) fn retain(&mut self, live: &[String]) {
        self.per_bucket.retain(|bucket, _| live.contains(bucket));
    }

    /// A `start-after` for `bucket`: a known-populated head, then random characters that place the
    /// probe somewhere inside that head's run rather than always at the front of it.
    pub(super) fn position(&self, bucket: &str) -> String {
        let mut rng = rand::thread_rng();
        if rng.gen::<f64>() < EXPLORE_SHARE {
            return random_key_position();
        }
        let Some(distribution) = self.per_bucket.get(bucket).filter(|d| !d.is_empty()) else {
            return random_key_position();
        };

        let total: f64 = distribution.values().sum();
        let mut draw = rng.gen::<f64>() * total;
        let head = distribution
            .iter()
            .find(|(_, weight)| {
                draw -= **weight;
                draw <= 0.0
            })
            .map(|(prefix, _)| prefix.clone())
            // Floating-point slack at the very top of the range.
            .unwrap_or_else(|| distribution.keys().next().cloned().unwrap_or_default());

        let mut position = head;
        position.extend(
            (position.chars().count()..POSITION_LEN).map(|_| rng.gen_range(b'!'..=b'~') as char),
        );
        position
    }
}

/// A position in `<meta>`, aimed at the shadow range or the twin range with even odds.
fn random_meta_position() -> String {
    let c = meta::CTRL as char;
    if rand::thread_rng().gen::<bool>() {
        format!("{}{}", meta::shadow_scan_prefix(), random_digest_position())
    } else {
        // Range B is `0x01 ‖ key ‖ …`, and a client key's first byte is >= 0x02 by admission, so a
        // printable-ASCII tail lands inside the twins rather than back in range A.
        format!("{c}{}", random_key_position())
    }
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
            prefixes: HashMap::new(),
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

    fn observed(keys: &[&str]) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        for key in keys {
            *counts.entry(leading(key)).or_default() += 1;
        }
        counts
    }

    fn shaped_share(prefixes: &Prefixes, bucket: &str, head: &str) -> f64 {
        let drawn = (0..2000).map(|_| prefixes.position(bucket));
        drawn.filter(|p| p.starts_with(head)).count() as f64 / 2000.0
    }

    /// The bias this exists for: a keyspace that lives under one prefix would otherwise take almost
    /// every probe from somewhere it holds no keys at all.
    #[test]
    fn probes_land_where_the_keys_actually_are() {
        let mut prefixes = Prefixes::new();
        prefixes.observe("b", &observed(&["logs/a", "logs/b", "logs/c", "logs/d"]));
        let share = shaped_share(&prefixes, "b", "lo");
        assert!(share > 0.7, "shaped share was {share}");
    }

    /// The mirror of [`Yields`]'s floor: a distribution that only draws where it has already looked
    /// can never find a prefix that appeared afterwards.
    #[test]
    fn some_positions_stay_unshaped_so_new_prefixes_are_reachable() {
        let mut prefixes = Prefixes::new();
        prefixes.observe("b", &observed(&["logs/a", "logs/b"]));
        let unshaped = 1.0 - shaped_share(&prefixes, "b", "lo");
        assert!(unshaped > 0.05, "exploration share was {unshaped}");
    }

    /// A cold start is one round of unshaped probes, not a wrong answer — which is what lets the
    /// distribution live in memory alone.
    #[test]
    fn an_unlearned_bucket_draws_a_plain_random_position() {
        let prefixes = Prefixes::new();
        for _ in 0..200 {
            let position = prefixes.position("never-probed");
            assert_eq!(position.len(), POSITION_LEN);
            assert!(meta::validate_client_key(&position).is_ok());
        }
    }

    /// Positions are `start-after` values, so a shaped one has to be as usable as a random one —
    /// including for a keyspace whose keys begin with multi-byte characters, where a byte-sliced
    /// window would produce a replacement character that sorts past every real key.
    #[test]
    fn every_shaped_position_is_a_usable_start_after() {
        let mut prefixes = Prefixes::new();
        prefixes.observe(
            "b",
            &observed(&[
                "a",
                "zz/x",
                "0-1/y",
                "\u{e9}t\u{e9}/x",
                "\u{4e2d}\u{6587}/y",
            ]),
        );
        for _ in 0..200 {
            let position = prefixes.position("b");
            assert_eq!(position.chars().count(), POSITION_LEN);
            assert!(meta::validate_client_key(&position).is_ok());
        }
    }

    /// The keyspace moves — a prefix nothing has held keys under for a while must stop taking probes,
    /// or the distribution is a record of what the bucket used to be.
    #[test]
    fn a_prefix_that_stops_appearing_is_forgotten() {
        let mut prefixes = Prefixes::new();
        prefixes.observe("b", &observed(&["old/a", "old/b"]));
        for _ in 0..50 {
            prefixes.observe("b", &observed(&["new/a", "new/b"]));
        }
        let stale = shaped_share(&prefixes, "b", "ol");
        assert!(
            stale < 0.02,
            "a retired prefix still took {stale} of probes"
        );
    }

    #[test]
    fn a_retired_bucket_stops_shaping_positions() {
        let mut prefixes = Prefixes::new();
        prefixes.observe("deleted", &observed(&["logs/a"]));
        prefixes.retain(&["live".to_string()]);
        assert!(shaped_share(&prefixes, "deleted", "lo") < 0.05);
    }

    #[test]
    fn sampling_an_empty_bucket_set_draws_nothing() {
        assert!(Yields::new(0.2).sample(&[], 8).is_empty());
    }
}
