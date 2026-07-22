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

use hypha_core::config::{ClientAuth, Config, Mode, S3Endpoint, Serving};

/// Fixed root credentials for the throwaway MinIO (password must be ≥ 8 chars).
const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";

/// The client-facing credentials hypha authenticates its own S3 clients with (§2) — distinct from
/// the MinIO backend creds above.
const HYPHA_ACCESS: &str = "hyphatestaccess";
const HYPHA_SECRET: &str = "hyphatestsecretkey";

/// A random 256-bit-ish passphrase stand-in; any stable string works for a single run (§6).
const MASTER_PASSPHRASE: &str = "integration-test-master-passphrase-0123456789abcdef";

/// hypha maps one client bucket onto `cache-<b>` and `remote-<b>` on the shared MinIO; the two
/// prefixes are what keep the cache's tombstones/twins and the remote's ciphertext from colliding.
const CACHE_PREFIX: &str = "cache-";
const REMOTE_PREFIX: &str = "remote-";

// ── MinIO ────────────────────────────────────────────────────────────────────────────────────

/// A throwaway MinIO server. Killed and its data dir removed on `Drop`.
pub struct Minio {
    child: Child,
    _data_dir: tempfile::TempDir,
    pub endpoint: String,
}

impl Minio {
    pub async fn start() -> Self {
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
        for _ in 0..120 {
            if client.list_buckets().send().await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        panic!("MinIO at {} did not become ready within 30s", self.endpoint);
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
        let service = hypha::build_service(config).expect("build hypha service");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind hypha");
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _ = hypha::serve(listener, service, async {
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
}

impl Drop for Hypha {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        // The spawned task is aborted when its handle drops; the graceful drain is best-effort here.
    }
}

// ── The harness ──────────────────────────────────────────────────────────────────────────────

pub struct Harness {
    pub minio: Minio,
    pub hypha: Hypha,
    pub config: Config,
}

impl Harness {
    /// A durable-mode deployment: one MinIO backing both roles, hypha in front of it.
    pub async fn durable() -> Self {
        Self::with_mode(Mode::Durable).await
    }

    pub async fn with_mode(mode: Mode) -> Self {
        let minio = Minio::start().await;
        let config = Config {
            remote: endpoint_cfg(&minio.endpoint, REMOTE_PREFIX),
            cache: endpoint_cfg(&minio.endpoint, CACHE_PREFIX),
            mode,
            auth: ClientAuth {
                access_key: HYPHA_ACCESS.to_string(),
                secret_key: HYPHA_SECRET.to_string(),
            },
            master_passphrase: MASTER_PASSPHRASE.to_string(),
            serving: Serving::default(),
        };
        let hypha = Hypha::start(&config).await;
        Self {
            minio,
            hypha,
            config,
        }
    }

    /// A fresh S3 client pointed at hypha, authenticating as a hypha client.
    pub fn client(&self) -> Client {
        s3_client(&self.hypha.endpoint, HYPHA_ACCESS, HYPHA_SECRET)
    }

    /// A client pointed straight at the MinIO backend (root creds) — bypasses hypha.
    pub fn raw(&self) -> Client {
        self.minio.raw_client()
    }

    pub fn remote_bucket(&self, client_bucket: &str) -> String {
        format!("{REMOTE_PREFIX}{client_bucket}")
    }

    pub fn cache_bucket(&self, client_bucket: &str) -> String {
        format!("{CACHE_PREFIX}{client_bucket}")
    }

    /// Restart hypha against the same MinIO and config — models a process restart (crash/redeploy).
    /// Cache-resident state (mpu records, tombstones) persists on the backend across this.
    pub async fn restart_hypha(&mut self) {
        self.hypha.stop().await;
        self.hypha = Hypha::start(&self.config).await;
    }

    /// Create a client bucket (hypha creates the paired cache + remote buckets).
    pub async fn create_bucket(&self, bucket: &str) {
        self.client()
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("create bucket");
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

fn endpoint_cfg(endpoint: &str, prefix: &str) -> S3Endpoint {
    S3Endpoint {
        endpoint: endpoint.to_string(),
        region: "us-east-1".to_string(),
        access_key: MINIO_USER.to_string(),
        secret_key: MINIO_PASS.to_string(),
        bucket_prefix: prefix.to_string(),
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

/// GET the last `n` bytes (an HTTP suffix range) and return them.
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

/// Start a multipart upload; returns its upload id.
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

/// GET the whole object through hypha and return its plaintext body.
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

/// PUT a full object through hypha; returns the ETag hypha reports (unquoted).
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

/// List every raw key under a prefix directly from a backend bucket (cache or remote).
pub async fn raw_list(client: &Client, bucket: &str, prefix: Option<&str>) -> Vec<String> {
    let mut out = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut req = client.list_objects_v2().bucket(bucket);
        if let Some(p) = prefix {
            req = req.prefix(p);
        }
        if let Some(t) = &token {
            req = req.continuation_token(t.clone());
        }
        let page = req.send().await.expect("raw list");
        for o in page.contents() {
            if let Some(k) = o.key() {
                out.push(k.to_string());
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
