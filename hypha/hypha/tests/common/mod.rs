//! Integration-test harness. Every test is fully self-contained and stateless: it starts its own
//! MinIO server (serving as **both** the cache and the remote backend, kept in disjoint bucket
//! namespaces by hypha's per-backend `bucket_prefix`), runs hypha in-process over an ephemeral
//! port, and drives it with a real `aws-sdk-s3` client. All state — the MinIO data dir and the
//! server process — is torn down on `Drop`, so a test leaves nothing behind whether it passes,
//! fails, or panics.
//!
//! One MinIO **per test**: the cheapest thing that is unconditionally clean. Tests run in parallel
//! (each `#[tokio::test]` on its own runtime), each on its own ports and data dir.

#![allow(dead_code)] // each test binary uses only part of this shared module

use std::collections::HashMap;
use std::net::TcpListener as StdTcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use hypha_core::config::{
    Background, ClientAuth, Config, Gc, Mode, Recency, Reconcile, S3Endpoint, Serving, DATA_ROLE,
    META_ROLE, REMOTE_ROLE,
};

/// Fixed root credentials for the throwaway MinIO (password must be ≥ 8 chars).
const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";

/// The client-facing credentials hypha authenticates its own S3 clients with (§2) — distinct from
/// the MinIO backend creds above.
const HYPHA_ACCESS: &str = "hyphatestaccess";
const HYPHA_SECRET: &str = "hyphatestsecretkey";

/// A random 256-bit-ish passphrase stand-in; any stable string works for a single run (§6).
const MASTER_PASSPHRASE: &str = "integration-test-master-passphrase-0123456789abcdef";

/// This deployment's prefix on the shared MinIO. Every backend bucket is `<prefix>-<role>-<b>`
/// (§9), which is what keeps the cache's tombstones/twins and the remote's ciphertext from
/// colliding on one endpoint.
const BUCKET_PREFIX: &str = "hyphatest";

// ── MinIO ────────────────────────────────────────────────────────────────────────────────────

/// Booting a MinIO is the expensive part of a test (process spawn + disk init). With one per test
/// and the default test parallelism they all land at once and the slowest miss their readiness
/// budget, so cap how many boot concurrently — running servers are near-idle, only the burst needs
/// bounding.
static BOOT_GATE: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

/// A throwaway MinIO server. Killed and its data dir removed on `Drop`.
pub struct Minio {
    child: Child,
    _data_dir: tempfile::TempDir,
    pub endpoint: String,
}

impl Minio {
    pub async fn start() -> Self {
        let _boot = BOOT_GATE
            .acquire()
            .await
            .expect("boot gate is never closed");
        let data_dir = tempfile::tempdir().expect("minio data dir");
        let api_port = free_port();
        let console_port = free_port();
        let endpoint = format!("http://127.0.0.1:{api_port}");

        let bin = std::env::var("HYPHA_TEST_MINIO_BIN").unwrap_or_else(|_| "minio".to_string());
        // Console must not share the API port; both are pinned to free ephemeral ports.
        let child = Command::new(&bin)
            .arg("server")
            .arg(data_dir.path())
            .arg("--address")
            .arg(format!("127.0.0.1:{api_port}"))
            .arg("--console-address")
            .arg(format!("127.0.0.1:{console_port}"))
            .env("MINIO_ROOT_USER", MINIO_USER)
            .env("MINIO_ROOT_PASSWORD", MINIO_PASS)
            .env("MINIO_UPDATE", "off")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("spawning `{bin} server` (set HYPHA_TEST_MINIO_BIN?): {e}"));

        let minio = Self {
            child,
            _data_dir: data_dir,
            endpoint,
        };
        minio.await_ready().await;
        minio
    }

    /// An S3 client bound straight to this MinIO with its root credentials — used to inspect the
    /// backend directly (ciphertext-at-rest checks, cache-state assertions).
    pub fn raw_client(&self) -> Client {
        s3_client(&self.endpoint, MINIO_USER, MINIO_PASS)
    }

    async fn await_ready(&self) {
        let client = self.raw_client();
        for _ in 0..240 {
            if client.list_buckets().send().await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        panic!("MinIO at {} did not become ready within 60s", self.endpoint);
    }
}

impl Drop for Minio {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ── hypha, in-process ──────────────────────────────────────────────────────────────────────────

/// hypha served on an ephemeral loopback port. Shuts the server down and drains on `Drop`.
pub struct Hypha {
    pub endpoint: String,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl Hypha {
    async fn start(config: &Config) -> Self {
        let (service, lifecycle) = hypha::build_service(config).expect("build hypha service");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind hypha");
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _ = hypha::serve(listener, service, lifecycle, async {
                let _ = rx.await;
            })
            .await;
        });
        Self {
            endpoint: format!("http://{addr}"),
            shutdown: Some(tx),
            task: Some(task),
        }
    }

    async fn stop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }

    /// Drop the serving task where it stands — no shutdown signal, no drain. Models SIGKILL.
    fn kill(&mut self) {
        self.shutdown.take();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl Drop for Hypha {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        // The spawned task is aborted when its handle drops; the graceful drain is best-effort here.
    }
}

// ── hypha, as a real subprocess ────────────────────────────────────────────────────────────────

/// hypha run as the shipped binary in its own process, configured through the `HYPHA_` env layer
/// `Config::load` reads. Needed only where a test asserts *process-level* behaviour — the exit an
/// invariant violation ends the process with (`hypha::halt`) would take the test runner down with
/// it in-process.
pub struct ChildHypha {
    child: Child,
    pub endpoint: String,
}

impl ChildHypha {
    /// Spawn the binary against `config` and wait for it to accept requests. `config.serving.listen`
    /// must already name a free port.
    async fn start(config: &Config) -> Self {
        let hypha = Self::spawn(config);
        hypha.await_ready().await;
        hypha
    }

    /// Spawn without waiting for readiness — for a run expected to exit *before* it can serve, which
    /// is what a recorded invariant violation makes every subsequent run do (`hypha::halt`).
    fn spawn(config: &Config) -> Self {
        let listen = config.serving.listen.clone();
        let child = Command::new(env!("CARGO_BIN_EXE_hypha"))
            .envs(config_env(config))
            // A stray `hypha.toml` in the crate dir would otherwise layer under the env config.
            .current_dir(std::env::temp_dir())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawning the hypha binary");

        Self {
            child,
            endpoint: format!("http://{listen}"),
        }
    }

    async fn await_ready(&self) {
        let client = s3_client(&self.endpoint, HYPHA_ACCESS, HYPHA_SECRET);
        for _ in 0..120 {
            if client.list_buckets().send().await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("hypha at {} did not become ready within 12s", self.endpoint);
    }

    /// Block until the process exits, returning its status. Panics on timeout (with the child
    /// killed), so a test asserting termination fails loudly instead of hanging.
    pub async fn wait_exit(&mut self, within: Duration) -> std::process::ExitStatus {
        let deadline = std::time::Instant::now() + within;
        loop {
            match self.child.try_wait().expect("wait on hypha") {
                Some(status) => return status,
                None if std::time::Instant::now() >= deadline => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    panic!("hypha did not exit within {within:?}");
                }
                None => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    }

    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ChildHypha {
    fn drop(&mut self) {
        self.stop();
    }
}

/// `config` as the `HYPHA_`-prefixed environment `Config::load` parses (`__` nests).
fn config_env(config: &Config) -> HashMap<String, String> {
    let mut env: HashMap<String, String> = HashMap::new();
    for (role, ep) in [("REMOTE", &config.remote), ("CACHE", &config.cache)] {
        env.insert(format!("HYPHA_{role}__ENDPOINT"), ep.endpoint.clone());
        env.insert(format!("HYPHA_{role}__REGION"), ep.region.clone());
        env.insert(format!("HYPHA_{role}__ACCESS_KEY"), ep.access_key.clone());
        env.insert(format!("HYPHA_{role}__SECRET_KEY"), ep.secret_key.clone());
    }
    env.insert("HYPHA_BUCKET_PREFIX".into(), config.bucket_prefix.clone());
    env.insert(
        "HYPHA_MODE".into(),
        match config.mode {
            Mode::Durable => "durable".into(),
            Mode::Cached => "cached".into(),
        },
    );
    env.insert(
        "HYPHA_AUTH__ACCESS_KEY".into(),
        config.auth.access_key.clone(),
    );
    env.insert(
        "HYPHA_AUTH__SECRET_KEY".into(),
        config.auth.secret_key.clone(),
    );
    env.insert(
        "HYPHA_MASTER_PASSPHRASE".into(),
        config.master_passphrase.clone(),
    );
    env.insert(
        "HYPHA_SERVING__LISTEN".into(),
        config.serving.listen.clone(),
    );
    env.insert(
        "HYPHA_SERVING__OFFLOAD_THRESHOLD".into(),
        config.serving.offload_threshold.to_string(),
    );
    env.insert(
        "HYPHA_RECONCILE__INTERVAL_MS".into(),
        config.reconcile.interval_ms.to_string(),
    );
    env.insert(
        "HYPHA_RECONCILE__CONCURRENCY".into(),
        config.reconcile.concurrency.to_string(),
    );
    env
}

// ── The harness ──────────────────────────────────────────────────────────────────────────────

/// How the hypha under test is running. In-process is the default — cheaper and directly
/// debuggable; a child process is only for assertions about the process itself.
pub enum Server {
    InProcess(Hypha),
    Child(ChildHypha),
}

impl Server {
    fn endpoint(&self) -> &str {
        match self {
            Server::InProcess(h) => &h.endpoint,
            Server::Child(h) => &h.endpoint,
        }
    }
}

pub struct Harness {
    pub minio: Minio,
    pub hypha: Server,
    pub config: Config,
}

impl Harness {
    /// A durable-mode deployment: one MinIO backing both roles, hypha in front of it.
    pub async fn durable() -> Self {
        Self::with_mode(Mode::Durable).await
    }

    /// A cached-mode deployment: writes ack after the cache write, the reconcile sweep trails them to
    /// the remote (Phase 4).
    pub async fn cached() -> Self {
        Self::with_mode(Mode::Cached).await
    }

    /// The same deployment with hypha as a real subprocess — for tests that assert on how the
    /// process itself lives or dies.
    pub async fn durable_subprocess() -> Self {
        Self::subprocess(Mode::Durable).await
    }

    pub async fn cached_subprocess() -> Self {
        Self::subprocess(Mode::Cached).await
    }

    async fn subprocess(mode: Mode) -> Self {
        let minio = Minio::start().await;
        let mut config = base_config(&minio, mode);
        config.serving.listen = format!("127.0.0.1:{}", free_port());
        let hypha = Server::Child(ChildHypha::start(&config).await);
        Self {
            minio,
            hypha,
            config,
        }
    }

    /// The child process, for tests driving it directly. Panics on an in-process harness.
    pub fn child(&mut self) -> &mut ChildHypha {
        match &mut self.hypha {
            Server::Child(h) => h,
            Server::InProcess(_) => panic!("harness is running hypha in-process"),
        }
    }

    pub async fn with_mode(mode: Mode) -> Self {
        let minio = Minio::start().await;
        let config = base_config(&minio, mode);
        let hypha = Server::InProcess(Hypha::start(&config).await);
        Self {
            minio,
            hypha,
            config,
        }
    }

    /// A fresh S3 client pointed at hypha, authenticating as a hypha client.
    pub fn client(&self) -> Client {
        s3_client(self.hypha.endpoint(), HYPHA_ACCESS, HYPHA_SECRET)
    }

    /// A client pointed straight at the MinIO backend (root creds) — bypasses hypha.
    pub fn raw(&self) -> Client {
        self.minio.raw_client()
    }

    pub fn remote_bucket(&self, client_bucket: &str) -> String {
        format!("{}{client_bucket}", self.config.role_prefix(REMOTE_ROLE))
    }

    /// The `<data>` cache bucket — client bodies + tombstones (§6).
    pub fn cache_bucket(&self, client_bucket: &str) -> String {
        format!("{}{client_bucket}", self.config.role_prefix(DATA_ROLE))
    }

    /// The `<meta>` cache bucket — hypha's twins, markers, and mpu records (§6).
    pub fn meta_bucket(&self, client_bucket: &str) -> String {
        format!("{}{client_bucket}", self.config.role_prefix(META_ROLE))
    }

    /// GC's own bucket — the recency ring's slices (§8). One per deployment, not per client bucket.
    pub fn gc_bucket(&self) -> String {
        self.config.gc_bucket()
    }

    /// Restart hypha against the same MinIO and config — models a process restart (crash/redeploy).
    /// Cache-resident state (mpu records, tombstones) persists on the backend across this.
    pub async fn restart_hypha(&mut self) {
        self.stop_hypha().await;
        self.start_hypha().await;
    }

    /// Stop hypha **gracefully**: signal shutdown and wait out the drain, so cached mode writes its
    /// clean markers (§7). Leaves the harness without a running server until [`Self::start_hypha`].
    pub async fn stop_hypha(&mut self) {
        match &mut self.hypha {
            Server::InProcess(h) => h.stop().await,
            Server::Child(h) => h.stop(),
        }
    }

    /// Kill hypha **without** a drain — the SIGKILL/crash case, which must leave every clean marker
    /// absent so the next run rescans.
    pub async fn kill_hypha(&mut self) {
        match &mut self.hypha {
            Server::InProcess(h) => h.kill(),
            Server::Child(h) => h.stop(),
        }
    }

    pub async fn start_hypha(&mut self) {
        self.hypha = match &self.hypha {
            Server::InProcess(_) => Server::InProcess(Hypha::start(&self.config).await),
            Server::Child(_) => Server::Child(ChildHypha::start(&self.config).await),
        };
    }

    /// Start a subprocess hypha without waiting for it to serve — the caller is asserting that it
    /// exits instead ([`ChildHypha::wait_exit`]).
    pub fn start_hypha_expecting_exit(&mut self) {
        self.hypha = Server::Child(ChildHypha::spawn(&self.config));
    }

    pub async fn create_bucket(&self, bucket: &str) {
        self.client()
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("create bucket");
    }
}

fn base_config(minio: &Minio, mode: Mode) -> Config {
    Config {
        remote: endpoint_cfg(&minio.endpoint),
        cache: endpoint_cfg(&minio.endpoint),
        bucket_prefix: BUCKET_PREFIX.to_string(),
        mode,
        auth: ClientAuth {
            access_key: HYPHA_ACCESS.to_string(),
            secret_key: HYPHA_SECRET.to_string(),
        },
        master_passphrase: MASTER_PASSPHRASE.to_string(),
        serving: Serving::default(),
        // A tight reconcile cadence so cached-mode tests observe uploads/propagation promptly rather
        // than waiting out the production interval.
        reconcile: Reconcile {
            interval_ms: 150,
            concurrency: 8,
        },
        background: Background::default(),
        // Tight for the same reason as the reconcile cadence: a test asserting a reclaim shouldn't
        // wait out a production interval that is deliberately measured in minutes. Bounds pinned to
        // the base so the §8 ladder stays flat — a test that wants it to escalate says so itself,
        // rather than every unrelated test racing an interval that moves underneath it.
        gc: Gc {
            interval_ms: 200,
            min_interval_ms: 200,
            concurrency: 4,
            max_concurrency: 4,
            // A fill target low enough that a test can rotate the ring with a handful of keys.
            recency: Recency {
                fill_target: 16,
                depth: 3,
                false_positive_rate: 0.01,
            },
            // MinIO reports no cache usage, so the harness has no pressure source: passes sweep
            // debris and never evict. An eviction test supplies its own source.
            usage: None,
            ..Gc::default()
        },
        // Tight, so a test that takes a cache volume away mid-run doesn't wait out the production
        // cadence for the watchdog to notice.
        volume_watch_interval_ms: 200,
    }
}

// ── S3 client + small helpers ────────────────────────────────────────────────────────────────

/// Build a path-style S3 client for `endpoint`. Request checksums are pinned to `WhenRequired` so
/// the SDK's default flexible-checksum trailers don't reach s3s's SigV4 verification.
pub fn s3_client(endpoint: &str, access: &str, secret: &str) -> Client {
    let creds = Credentials::new(access, secret, None, None, "hypha-test");
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .force_path_style(true)
        .request_checksum_calculation(aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired)
        .build();
    Client::from_conf(conf)
}

fn endpoint_cfg(endpoint: &str) -> S3Endpoint {
    S3Endpoint {
        endpoint: endpoint.to_string(),
        region: "us-east-1".to_string(),
        access_key: MINIO_USER.to_string(),
        secret_key: MINIO_PASS.to_string(),
    }
}

/// Grab a currently-free localhost port by binding to :0 and immediately releasing it. A small
/// window exists before the port is re-claimed; acceptable for a test harness.
fn free_port() -> u16 {
    StdTcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .unwrap()
        .port()
}

/// The S3 error code (e.g. `"PreconditionFailed"`, `"NoSuchKey"`) from a failed SDK call, if it
/// carried one. `None` for transport-level failures.
pub fn sdk_err_code<E, R>(err: &aws_sdk_s3::error::SdkError<E, R>) -> Option<String>
where
    E: aws_sdk_s3::error::ProvideErrorMetadata,
{
    err.as_service_error()
        .and_then(|e| e.code())
        .map(str::to_string)
}

pub async fn get_suffix(client: &Client, bucket: &str, key: &str, n: u64) -> Vec<u8> {
    let out = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range(format!("bytes=-{n}"))
        .send()
        .await
        .expect("suffix get_object");
    out.body.collect().await.expect("collect suffix").to_vec()
}

/// The magic bytes at the head of every stock age binary file — what hypha writes to the remote.
pub const AGE_MAGIC: &[u8] = b"age-encryption.org/v1";

/// MinIO (like S3) rejects a non-final multipart part smaller than 5 MiB.
pub const MIN_PART: usize = 5 * 1024 * 1024;

pub async fn create_mpu(client: &Client, bucket: &str, key: &str) -> String {
    client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("create_multipart_upload")
        .upload_id()
        .expect("upload id")
        .to_string()
}

/// Upload one part; returns the ETag hypha reports for it (the part's plaintext MD5).
pub async fn upload_part(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: &[u8],
) -> String {
    client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .body(ByteStream::from(body.to_vec()))
        .content_length(body.len() as i64)
        .send()
        .await
        .unwrap_or_else(|e| panic!("upload_part {part_number}: {e}"))
        .e_tag()
        .expect("part etag")
        .trim_matches('"')
        .to_string()
}

/// Complete a multipart upload from `(part_number, etag)` pairs (`etag` empty ⇒ omitted, letting
/// hypha resolve the winner itself). Returns the composite ETag hypha reports.
pub async fn complete_mpu(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: &[(i32, String)],
) -> String {
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
    let completed: Vec<CompletedPart> = parts
        .iter()
        .map(|(n, etag)| {
            let mut b = CompletedPart::builder().part_number(*n);
            if !etag.is_empty() {
                b = b.e_tag(etag.clone());
            }
            b.build()
        })
        .collect();
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        )
        .send()
        .await
        .expect("complete_multipart_upload")
        .e_tag()
        .expect("composite etag")
        .trim_matches('"')
        .to_string()
}

/// The S3 composite ETag for parts uploaded through hypha: `md5(pmd5₀‖…‖pmd5ₙ)-N`, where each
/// `pmd5` is the part's *plaintext* MD5 (§6). Mirrors `hypha_core::meta::composite_etag`.
pub fn expected_composite_etag(parts: &[&[u8]]) -> String {
    use md5::{Digest, Md5};
    let mut outer = Md5::new();
    for p in parts {
        outer.update(Md5::digest(p));
    }
    format!("{}-{}", hex::encode(outer.finalize()), parts.len())
}

pub fn md5_hex(bytes: &[u8]) -> String {
    use md5::{Digest, Md5};
    hex::encode(Md5::digest(bytes))
}

/// The `Content-MD5` header value for a body — base64 of the raw digest, not hex.
pub fn base64_md5(bytes: &[u8]) -> String {
    use md5::{Digest, Md5};
    base64_simd::STANDARD.encode_to_string(Md5::digest(bytes))
}

/// A deterministic byte pattern of `len` bytes — distinct per offset so a mis-sliced range is
/// caught (matches the `hypha-format` roundtrip test's pattern).
pub fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// A distinct byte pattern seeded by `seed`, so two same-length payloads differ.
pub fn pattern_seeded(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i as u64 + seed as u64 * 7) % 251) as u8)
        .collect()
}

pub fn bytes_body(bytes: &[u8]) -> ByteStream {
    ByteStream::from(bytes.to_vec())
}

pub async fn get_all(client: &Client, bucket: &str, key: &str) -> Vec<u8> {
    let out = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("get_object");
    out.body.collect().await.expect("collect body").to_vec()
}

/// GET a byte range `[first, last]` (inclusive, HTTP semantics) and return the bytes.
pub async fn get_range(client: &Client, bucket: &str, key: &str, first: u64, last: u64) -> Vec<u8> {
    let out = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range(format!("bytes={first}-{last}"))
        .send()
        .await
        .expect("ranged get_object");
    out.body.collect().await.expect("collect range").to_vec()
}

pub async fn put(client: &Client, bucket: &str, key: &str, body: &[u8]) -> String {
    let out = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(bytes_body(body))
        .content_length(body.len() as i64)
        .send()
        .await
        .expect("put_object");
    out.e_tag()
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

/// The raw ciphertext hypha wrote to the remote for `key` (bypasses hypha, reads MinIO directly).
pub async fn raw_remote_object(harness: &Harness, bucket: &str, key: &str) -> Vec<u8> {
    let out = harness
        .raw()
        .get_object()
        .bucket(harness.remote_bucket(bucket))
        .key(key)
        .send()
        .await
        .expect("raw remote get");
    out.body.collect().await.expect("collect raw").to_vec()
}

pub async fn raw_list(client: &Client, bucket: &str, prefix: Option<&str>) -> Vec<String> {
    let mut out = Vec::new();
    let mut token: Option<String> = None;
    loop {
        // encoding-type=url so `<meta>` twin/mpu keys (which carry the 0x01 control byte, illegal in
        // XML) survive the response; decode back to raw bytes.
        let mut req = client
            .list_objects_v2()
            .bucket(bucket)
            .encoding_type(aws_sdk_s3::types::EncodingType::Url);
        if let Some(p) = prefix {
            req = req.prefix(p);
        }
        if let Some(t) = &token {
            req = req.continuation_token(t.clone());
        }
        let page = req.send().await.expect("raw list");
        for o in page.contents() {
            if let Some(k) = o.key() {
                out.push(
                    percent_encoding::percent_decode_str(k)
                        .decode_utf8_lossy()
                        .into_owned(),
                );
            }
        }
        if page.is_truncated() != Some(true) {
            break;
        }
        token = page.next_continuation_token().map(str::to_string);
        if token.is_none() {
            break;
        }
    }
    out
}

/// Directly overwrite a cache object (bypassing hypha) with arbitrary bytes + user-metadata.
/// Used to plant crash-window states (e.g. a lone transition mark) the data path must recover from.
pub async fn raw_cache_put(
    harness: &Harness,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    metadata: HashMap<String, String>,
) {
    harness
        .raw()
        .put_object()
        .bucket(harness.cache_bucket(bucket))
        .key(key)
        .body(ByteStream::from(body))
        .set_metadata(Some(metadata))
        .send()
        .await
        .expect("raw cache put");
}
