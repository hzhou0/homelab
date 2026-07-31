//! Typed and boot-validated configuration.

use serde::Deserialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Acknowledge only after the remote commit.
    Durable,
    /// Acknowledge the cache commit and reconcile asynchronously.
    Cached,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Endpoint {
    pub endpoint: String,
    /// Backend SigV4 signing region — a dummy for SeaweedFS/MinIO, which ignore it. Not a
    /// client-facing concern: client buckets pass through, so this is purely how hypha's SDK
    /// client signs against the backend.
    #[serde(default = "default_region")]
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
}

/// The role each backend bucket plays, as the fixed segment between the deployment's own prefix and
/// the client bucket name — `<prefix>-d-<b>`, `<prefix>-m-<b>`, `<prefix>-r-<b>`.
///
/// Fixed rather than configured: three independently settable prefixes could be made to overlap (one
/// a prefix of another on a shared endpoint), which had to be caught at boot and meant nothing good
/// if it ever slipped through. Deriving them from one prefix makes disjointness structural, and the
/// deployment-sharing the prefixes existed for is still served by varying the one prefix.
pub const DATA_ROLE: &str = "d";
pub const META_ROLE: &str = "m";
pub const REMOTE_ROLE: &str = "r";
/// GC's own bucket (§8). Not per client bucket — one bucket for the whole deployment, since the
/// recency ring is global and its slices are keyed by fully qualified `<bucket>/<key>`.
pub const GC_ROLE: &str = "g";

fn default_region() -> String {
    "us-east-1".to_string()
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientAuth {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Serving {
    #[serde(default = "default_listen")]
    pub listen: String,
    /// Where §10's metrics and health probes are served. A second listener rather than paths on the
    /// S3 port: those are unauthenticated and in-cluster, and the S3 port is neither.
    #[serde(default = "default_admin_listen")]
    pub admin_listen: String,
}

fn default_listen() -> String {
    "0.0.0.0:8014".to_string()
}
fn default_admin_listen() -> String {
    "0.0.0.0:9014".to_string()
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reconcile {
    /// Delay between passes. Short enough that the async-lag loss window stays small, long enough
    /// that an idle cache isn't polled hot.
    #[serde(default = "default_reconcile_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_reconcile_concurrency")]
    pub concurrency: usize,
}

fn default_reconcile_interval_ms() -> u64 {
    5_000
}
fn default_reconcile_concurrency() -> usize {
    16
}

impl Default for Reconcile {
    fn default() -> Self {
        Reconcile {
            interval_ms: default_reconcile_interval_ms(),
            concurrency: default_reconcile_concurrency(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Background {
    /// Transitions running at once. Each is a whole-object fetch + decrypt + cache write, so this
    /// sits far below `reconcile.concurrency`: the bound is remote bandwidth, not request count.
    #[serde(default = "default_background_concurrency")]
    pub concurrency: usize,
    /// Queued-but-unstarted transitions. A full queue **drops** new submissions instead of blocking
    /// the read that raised them — a dropped rehydrate costs the next read of that key one remote
    /// fetch, which is exactly what the read is already doing.
    #[serde(default = "default_background_queue_depth")]
    pub queue_depth: usize,
}

fn default_background_concurrency() -> usize {
    4
}
fn default_background_queue_depth() -> usize {
    256
}

impl Default for Background {
    fn default() -> Self {
        Background {
            concurrency: default_background_concurrency(),
            queue_depth: default_background_queue_depth(),
        }
    }
}

/// GC's unpressured settings and escalation bounds. The upper concurrency bound prevents emergency
/// reclaim from starving client reads.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Gc {
    /// Delay between passes with no pressure. Long relative to the reconcile sweep — nothing here is
    /// a durability obligation, so the only cost of a slow pass is reclaimable bytes left unreclaimed.
    #[serde(default = "default_gc_interval_ms")]
    pub interval_ms: u64,
    /// The floor pressure may shorten the interval to (rung 1) — its own limit is a continuously
    /// running walk.
    #[serde(default = "default_gc_min_interval_ms")]
    pub min_interval_ms: u64,
    #[serde(default = "default_gc_concurrency")]
    pub concurrency: usize,
    #[serde(default = "default_gc_max_concurrency")]
    pub max_concurrency: usize,
    #[serde(default = "default_gc_high_water")]
    pub high_water: f64,
    /// Usage fraction a pressured pass reclaims down to — the difference from `high_water` is the
    /// byte target, so a gap this wide is what stops the scavenger from re-triggering on every pass.
    #[serde(default = "default_gc_low_water")]
    pub low_water: f64,
    /// Pages one probe reads from its random position before moving on (§8). Small on purpose: the
    /// point of sampling is that scan cost tracks pressure rather than keyspace size.
    #[serde(default = "default_gc_probe_pages")]
    pub probe_pages: usize,
    /// Share of each pass's probes handed out evenly regardless of yield. Pure proportional sampling
    /// locks onto early winners and never revisits a bucket that went cold later — a scan that learns
    /// once and then stops learning.
    #[serde(default = "default_gc_yield_floor")]
    pub yield_floor: f64,
    /// Evictions a pass may keep making after its target is met, taking only ring *misses* (§8).
    /// Over-evicting an affirmatively cold key is nearly free in rehydration risk, but each one still
    /// costs a remote HEAD, a twin write, and a CAS — hence a bound rather than no limit.
    #[serde(default = "default_gc_opportunistic_evictions")]
    pub opportunistic_evictions: usize,
    /// Where usage is measured. Absent, GC never evicts (§8): with no measure of pressure there is no
    /// target to evict against, and cached mode warns at boot because its cache will only fill.
    #[serde(default)]
    pub usage: Option<Usage>,
    #[serde(default)]
    pub recency: Recency,
}

/// Physical bytes rather than live object sizes, because
/// dead bytes awaiting compaction are exactly what makes a cache fill with nobody writing to it.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "lowercase")]
pub enum Usage {
    /// SeaweedFS: the master's topology names the volume servers, each server's disk status is the
    /// measurement, and the master's vacuum is the dead-byte reclaim.
    Seaweedfs {
        master: String,
        /// Dead-byte fraction a volume must exceed before a vacuum rewrites it. The master applies
        /// this per volume, so a compaction request costs nothing when nothing is dirty enough.
        #[serde(default = "default_garbage_threshold")]
        garbage_threshold: f64,
    },
}

/// The shape of §8's recency ring: how much traffic a slice covers, and how far back the ring
/// remembers.
///
/// The slice's **bit count is derived**, not configured — it follows from `fill_target` and
/// `false_positive_rate`, which are the two properties an operator can actually reason about. A
/// directly configured size would let the two drift into a filter that is nominally k deep but
/// saturated, and a saturated slice reports every key as recent, which is the protect-everything
/// failure the fill-driven rotation exists to prevent.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Recency {
    /// Distinct keys a slice absorbs before it rotates. This — not a duration — is what recency is
    /// denominated in (§8), so it is really the question "how much competing traffic should it take
    /// to make a key look old".
    #[serde(default = "default_recency_fill_target")]
    pub fill_target: usize,
    /// Retired slices kept behind the current one, so the ring resolves `depth + 1` ages before
    /// falling through to *miss*.
    #[serde(default = "default_recency_depth")]
    pub depth: usize,
    /// Per-slice false-positive rate at the fill target. A false positive makes a cold key look
    /// warmer than it is, costing one deferred eviction — hence a rate this loose.
    #[serde(default = "default_recency_fpp")]
    pub false_positive_rate: f64,
}

fn default_recency_fill_target() -> usize {
    100_000
}
fn default_recency_depth() -> usize {
    7
}
fn default_recency_fpp() -> f64 {
    0.01
}

impl Default for Recency {
    fn default() -> Self {
        Recency {
            fill_target: default_recency_fill_target(),
            depth: default_recency_depth(),
            false_positive_rate: default_recency_fpp(),
        }
    }
}

fn default_gc_interval_ms() -> u64 {
    300_000
}
fn default_gc_min_interval_ms() -> u64 {
    1_000
}
fn default_gc_concurrency() -> usize {
    4
}
fn default_gc_max_concurrency() -> usize {
    16
}
fn default_gc_high_water() -> f64 {
    0.85
}
fn default_gc_low_water() -> f64 {
    0.70
}
fn default_gc_probe_pages() -> usize {
    5
}
fn default_gc_yield_floor() -> f64 {
    0.2
}
fn default_gc_opportunistic_evictions() -> usize {
    64
}
fn default_garbage_threshold() -> f64 {
    0.3
}

impl Default for Gc {
    fn default() -> Self {
        Gc {
            interval_ms: default_gc_interval_ms(),
            min_interval_ms: default_gc_min_interval_ms(),
            concurrency: default_gc_concurrency(),
            max_concurrency: default_gc_max_concurrency(),
            high_water: default_gc_high_water(),
            low_water: default_gc_low_water(),
            probe_pages: default_gc_probe_pages(),
            yield_floor: default_gc_yield_floor(),
            opportunistic_evictions: default_gc_opportunistic_evictions(),
            usage: None,
            recency: Recency::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub remote: S3Endpoint,
    /// Required in durable mode too, where it remains the namespace and ETag projection.
    pub cache: S3Endpoint,
    /// This deployment's own prefix, ahead of the fixed role segment ([`DATA_ROLE`] and friends) on
    /// every backend bucket it touches. Two deployments sharing one account stay in disjoint
    /// namespaces by differing here, and it is charged against S3's 63-byte bucket-name cap.
    pub bucket_prefix: String,
    pub mode: Mode,
    pub auth: ClientAuth,
    pub master_passphrase: String,
    #[serde(default)]
    pub serving: Serving,
    #[serde(default)]
    pub reconcile: Reconcile,
    #[serde(default)]
    pub background: Background,
    #[serde(default)]
    pub gc: Gc,
    /// How often to re-check that each `Ready` bucket still has its sync marker (§7). One HEAD per
    /// ready bucket per tick, so it is cheap at homelab bucket counts; the cost of a slow tick is
    /// only how long a live volume loss goes unnoticed.
    #[serde(default = "default_volume_watch_interval_ms")]
    pub volume_watch_interval_ms: u64,
}

fn default_volume_watch_interval_ms() -> u64 {
    30_000
}

impl Default for Serving {
    fn default() -> Self {
        Serving {
            listen: default_listen(),
            admin_listen: default_admin_listen(),
        }
    }
}

impl Config {
    // `figment::Error` is ~208 bytes; box it so the (boot-only, cold) error path doesn't bloat
    // this `Result`.
    pub fn load() -> Result<Self, Box<figment::Error>> {
        use figment::providers::{Env, Format, Toml};
        use figment::Figment;

        let cfg: Config = Figment::new()
            .merge(Toml::file("hypha.toml"))
            .merge(Env::prefixed("HYPHA_").split("__"))
            .extract()
            .map_err(Box::new)?;
        cfg.validate()
            .map_err(|e| Box::new(figment::Error::from(e)))?;
        Ok(cfg)
    }

    pub fn role_prefix(&self, role: &str) -> String {
        format!("{}-{role}-", self.bucket_prefix)
    }

    pub fn gc_bucket(&self) -> String {
        format!("{}-{GC_ROLE}", self.bucket_prefix)
    }

    /// Charged against S3's 63-byte bucket-name cap, so the client-visible cap is `63 − this`
    /// (§7 *Buckets*). Every role prefix is the same length, so one answer covers them all.
    pub fn max_bucket_prefix_len(&self) -> usize {
        self.role_prefix(DATA_ROLE).len()
    }

    fn validate(&self) -> Result<(), String> {
        // Empty would put the role segment at the head of every bucket name, where `d-`/`m-`/`r-`
        // are plausible client bucket names — and `ListBuckets` strips this prefix to recover the
        // client name, so a deployment sharing an account would start answering for another's
        // buckets.
        if self.bucket_prefix.is_empty() {
            return Err(
                "bucket_prefix must be non-empty: it is what separates this deployment's \
                        backend buckets from another's on a shared account"
                    .to_string(),
            );
        }
        if !self
            .bucket_prefix
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(format!(
                "bucket_prefix ({:?}) must be lowercase alphanumeric or '-' — it is part of an S3 \
                 bucket name",
                self.bucket_prefix
            ));
        }
        // An inverted bound doesn't degrade the §8 ladder, it reverses it: escalating would slow the
        // scavenger down and reach the age threshold having spent nothing cheap first.
        if self.gc.min_interval_ms > self.gc.interval_ms {
            return Err(format!(
                "gc.min_interval_ms ({}) exceeds gc.interval_ms ({}) — pressure would lengthen the \
                 interval it is supposed to shorten",
                self.gc.min_interval_ms, self.gc.interval_ms
            ));
        }
        if self.gc.max_concurrency < self.gc.concurrency {
            return Err(format!(
                "gc.max_concurrency ({}) is below gc.concurrency ({}) — pressure would narrow the \
                 pass it is supposed to widen",
                self.gc.max_concurrency, self.gc.concurrency
            ));
        }
        let fpp = self.gc.recency.false_positive_rate;
        if !(f64::EPSILON..1.0).contains(&fpp) {
            return Err(format!(
                "gc.recency.false_positive_rate ({fpp}) must be in (0, 1) — the slice's bit count \
                 is derived from it"
            ));
        }
        let (high, low) = (self.gc.high_water, self.gc.low_water);
        if !(0.0..=1.0).contains(&low) || !(0.0..=1.0).contains(&high) {
            return Err(format!(
                "gc water marks ({low}, {high}) must be fractions of cache capacity in [0, 1]"
            ));
        }
        // Equal marks are the degenerate case, not a mild one: every pass would trigger and owe a
        // zero-byte target, so the ladder would ratchet on evidence it can't help but produce.
        if low >= high {
            return Err(format!(
                "gc.low_water ({low}) is not below gc.high_water ({high}) — the gap between them is \
                 the byte target a pressured pass owes"
            ));
        }
        if self.gc.probe_pages == 0 {
            return Err(
                "gc.probe_pages must be non-zero — a probe that reads no pages finds no candidates"
                    .to_string(),
            );
        }
        if !(0.0..1.0).contains(&self.gc.yield_floor) {
            return Err(format!(
                "gc.yield_floor ({}) must be in [0, 1) — at 1 every bucket gets an equal share and \
                 the yield feedback is switched off",
                self.gc.yield_floor
            ));
        }
        if self.gc.recency.fill_target == 0 {
            return Err(
                "gc.recency.fill_target must be non-zero — a slice that rotates on every \
                        insert remembers nothing"
                    .to_string(),
            );
        }
        Ok(())
    }
}
