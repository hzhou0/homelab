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
pub struct S3Endpoint {
    pub endpoint: String,
    #[serde(default = "default_region")]
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    /// Prepended to every key this deployment stores, so deployments sharing one remote land in
    /// disjoint keyspaces (architecture § *Caching is optional*). Empty for a dedicated bucket.
    #[serde(default)]
    pub prefix: String,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

/// The access-key/secret hypha's own clients authenticate with — distinct from the backend
/// credentials above (§2, `S3Auth`).
#[derive(Clone, Debug, Deserialize)]
pub struct ClientAuth {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Clone, Debug, Deserialize)]
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
pub struct Config {
    pub remote: S3Endpoint,
    /// Required in both modes: the cache is the ETag/namespace source of truth even for the
    /// `durable` deployment, where it holds only tombstones (unified tiering design).
    pub cache: S3Endpoint,
    pub mode: Mode,
    pub auth: ClientAuth,
    /// The age X25519 identity string (`AGE-SECRET-KEY-1…`) hypha wraps every file key to,
    /// delivered via a Secret. One identity for the whole remote. age zeroizes the parsed key
    /// material on drop; the string form here lives for the process lifetime.
    pub master_identity: String,
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

        Figment::new()
            .merge(Toml::file("hypha.toml"))
            .merge(Env::prefixed("HYPHA_").split("__"))
            .extract()
            .map_err(Box::new)
    }
}
