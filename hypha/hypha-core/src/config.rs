//! Typed configuration, layered file + env via figment and validated at boot so a bad value
//! fails the process rather than surfacing as a runtime 500 on the hot path.

use serde::Deserialize;

/// How a deployment moves writes to the remote. Both modes use the cache; the difference is
/// timing and whether the cache retains bodies (see the unified tiering design).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Durable: upload to the remote inline, tombstone immediately, never restore to cache.
    /// Writes ack only once persisted on the remote — zero data loss on cache failure.
    Durable,
    /// Cached: ack after the cache write, upload via background reconcile, GC tombstones under
    /// pressure, tombstoned GET rehydrates. (Phases 4–5.)
    Cached,
}

/// One S3 endpoint hypha talks to (remote, or the optional cache — same shape, §2).
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
    /// Prepended to every client bucket name (architecture § *Two modes*), so deployments sharing
    /// one remote account land in disjoint bucket namespaces. Empty for a dedicated account.
    #[serde(default)]
    pub bucket_prefix: String,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

/// The access-key/secret hypha's own clients authenticate with — distinct from the backend
/// credentials above (§2, `S3Auth`).
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
    /// A contiguous encrypt/decrypt larger than this offloads to `spawn_blocking` to keep any
    /// single async poll bounded (§5). Bytes of pending plaintext.
    #[serde(default = "default_offload")]
    pub offload_threshold: usize,
}

fn default_listen() -> String {
    "0.0.0.0:8014".to_string()
}
fn default_offload() -> usize {
    1024 * 1024
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub remote: S3Endpoint,
    /// Required in both modes: the cache is the ETag/namespace source of truth even for the
    /// `durable` deployment, where it holds only tombstones (unified tiering design). Its
    /// `bucket_prefix` names the `<data>` bucket (client bodies + tombstones, §6).
    pub cache: S3Endpoint,
    /// Prefix for the cache's `<meta>` bucket (§6): hypha's own object-side state — facts twins,
    /// pending markers, mpu records — lives in `<meta><b>`, kept disjoint from the `<data><b>`
    /// client bodies by this prefix. Distinct from, and not a prefix of, the `<data>`/remote
    /// prefixes (validated at boot).
    pub cache_meta_prefix: String,
    pub mode: Mode,
    pub auth: ClientAuth,
    /// The 256-bit random passphrase age's scrypt recipient wraps every file key to (§6),
    /// delivered via a Secret. One passphrase for the whole remote; the string form here lives
    /// for the process lifetime.
    pub master_passphrase: String,
    #[serde(default)]
    pub serving: Serving,
}

impl Default for Serving {
    fn default() -> Self {
        Serving {
            listen: default_listen(),
            offload_threshold: default_offload(),
        }
    }
}

impl Config {
    /// Load `hypha.toml` (if present) then overlay `HYPHA_`-prefixed env vars (double underscore
    /// nests: `HYPHA_REMOTE__BUCKET`).
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

    /// The longest of the three configured bucket prefixes — charged against S3's 63-byte
    /// bucket-name cap, so the client-visible cap is `63 − this` (§7 *Buckets*).
    pub fn max_bucket_prefix_len(&self) -> usize {
        self.remote
            .bucket_prefix
            .len()
            .max(self.cache.bucket_prefix.len())
            .max(self.cache_meta_prefix.len())
    }

    /// Startup bucket-prefix invariants (§7 *Buckets*, §9). One client bucket maps to three backend
    /// buckets — `<data>`/`<meta>` on the cache, `<remote>` on the remote — each distinguished only
    /// by its prefix. So no two prefixes that share an endpoint may collide: neither may be a prefix
    /// of the other (which subsumes the empty-prefix case, since `""` is a prefix of everything),
    /// or `ListBuckets`' strip-and-filter would misclassify and client buckets leak or vanish. The
    /// `<data>` and `<meta>` prefixes always share the cache endpoint; the remote joins them when it
    /// points at the same endpoint (the integration harness's single MinIO does).
    fn validate(&self) -> Result<(), String> {
        // (prefix, endpoint) for the three backend buckets.
        let entries = [
            (
                &self.cache.bucket_prefix,
                &self.cache.endpoint,
                "cache <data>",
            ),
            (
                &self.cache_meta_prefix,
                &self.cache.endpoint,
                "cache <meta>",
            ),
            (&self.remote.bucket_prefix, &self.remote.endpoint, "remote"),
        ];
        for (i, (pa, ea, na)) in entries.iter().enumerate() {
            for (pb, eb, nb) in entries.iter().skip(i + 1) {
                if ea == eb && (pa.starts_with(pb.as_str()) || pb.starts_with(pa.as_str())) {
                    return Err(format!(
                        "bucket prefixes for {na} ({pa:?}) and {nb} ({pb:?}) share an endpoint and \
                         one is a prefix of the other — they cannot occupy disjoint bucket namespaces"
                    ));
                }
            }
        }
        Ok(())
    }
}
