//! Isolated real-backend integration harness with independent cache and remote fault proxies.
//!
//! **`TEST_CACHE_S3_ENDPOINT` replaces only the cache** with a server this process does not own.
//! `TEST_S3_ENDPOINT` remains the focused backend-contract mode where both roles use that server.
//! The SeaweedFS runner uses the former for the suite and the latter only for the direct
//! conditional-delete contract MinIO cannot satisfy.

#![allow(dead_code)] // each test binary uses only part of this shared module

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::TcpListener as StdTcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use http_body_util::{BodyExt as _, Full};
use hyper::body::{Bytes, Incoming};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use hypha_core::config::{
    Backpressure, Background, ClientAuth, Config, Gc, Mode, Recency, Reconcile, S3Endpoint, Serving,
    Usage,
    DATA_ROLE, META_ROLE, REMOTE_ROLE,
};

/// The client-facing credentials hypha authenticates its own S3 clients with (§2) — distinct from
/// each MinIO's backend credentials.
const HYPHA_ACCESS: &str = "hyphatestaccess";
const HYPHA_SECRET: &str = "hyphatestsecretkey";

/// A random 256-bit-ish passphrase stand-in; any stable string works for a single run (§6).
const MASTER_PASSPHRASE: &str = "integration-test-master-passphrase-0123456789abcdef";

/// This harness's own deployment prefix. Every backend bucket is `<prefix>-<role>-<b>` (§9), which
/// is what keeps the cache's tombstones/twins and the remote's ciphertext from colliding on one
/// endpoint — and, when the endpoint is shared, what keeps one fixture's buckets invisible to
/// another's.
fn unique_bucket_prefix() -> String {
    format!("hy{:08x}", rand::random::<u32>())
}

/// The cache endpoint the suite is pointed at, when it is one this process does not own.
pub fn external_cache_backend() -> Option<String> {
    endpoint_env("TEST_CACHE_S3_ENDPOINT").or_else(external_remote_backend)
}

/// The focused mode where the remote is also the externally supplied backend.
pub fn external_remote_backend() -> Option<String> {
    endpoint_env("TEST_S3_ENDPOINT")
}

fn endpoint_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|endpoint| !endpoint.is_empty())
}

/// Credentials for a server the compose fixtures provide. Anything signs against a SeaweedFS with no
/// identities configured, but one that does configure them — as both fixtures do, since the SDK
/// always signs — needs the identity they declare.
pub fn fixture_credentials() -> (String, String) {
    (
        std::env::var("TEST_S3_ACCESS_KEY").unwrap_or_else(|_| "hyphatest".into()),
        std::env::var("TEST_S3_SECRET_KEY").unwrap_or_else(|_| "hyphatestsecret".into()),
    )
}

// ── the backing S3 ───────────────────────────────────────────────────────────────────────────

/// Booting a MinIO is the expensive part of a test (process spawn + disk init). With one per test
/// and the default test parallelism they all land at once and the slowest miss their readiness
/// budget, so cap how many boot concurrently — running servers are near-idle, only the burst needs
/// bounding.
static BOOT_GATE: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

/// The S3 a fixture runs against: a throwaway MinIO this process spawns, or a shared server it was
/// pointed at. Named for the role rather than the product because which one it is decides nothing
/// above this file — the only difference is who tears it down.
pub struct TestS3 {
    pub endpoint: String,
    access_key: String,
    secret_key: String,
    /// `None` for a server this process did not start, and therefore must not stop.
    local: Option<LocalMinio>,
}

/// The spawned server and its data dir, so `Drop` takes both.
struct LocalMinio {
    child: Child,
    _data_dir: tempfile::TempDir,
}

impl TestS3 {
    pub async fn start() -> Self {
        if let Some(endpoint) = external_remote_backend() {
            return Self::connect(endpoint, "TEST_S3_ENDPOINT").await;
        }
        Self::start_minio().await
    }

    async fn connect(endpoint: String, variable: &str) -> Self {
        let (access_key, secret_key) = fixture_credentials();
        let shared = Self {
            endpoint,
            access_key,
            secret_key,
            local: None,
        };
        shared
            .await_serving()
            .await
            .unwrap_or_else(|e| panic!("{variable} is not serving: {e}"));
        shared
    }

    async fn start_minio() -> Self {
        let _boot = BOOT_GATE
            .acquire()
            .await
            .expect("boot gate is never closed");
        let bin = std::env::var("TEST_MINIO_BIN").unwrap_or_else(|_| "minio".to_string());
        let mut last_error = String::new();
        for _ in 0..8 {
            let data_dir = tempfile::tempdir().expect("minio data dir");
            let api_port = free_port();
            let mut console_port = free_port();
            while console_port == api_port {
                console_port = free_port();
            }
            let endpoint = format!("http://127.0.0.1:{api_port}");
            let access_key = format!("h{:016x}", rand::random::<u64>());
            let secret_key = format!("s{:016x}", rand::random::<u64>());

            let child = Command::new(&bin)
                .arg("server")
                .arg(data_dir.path())
                .arg("--address")
                .arg(format!("127.0.0.1:{api_port}"))
                .arg("--console-address")
                .arg(format!("127.0.0.1:{console_port}"))
                .env("MINIO_ROOT_USER", &access_key)
                .env("MINIO_ROOT_PASSWORD", &secret_key)
                .env("MINIO_UPDATE", "off")
                .stdout(if std::env::var("TEST_HYPHA_LOGS").is_ok() {
                    Stdio::inherit()
                } else {
                    Stdio::null()
                })
                .stderr(if std::env::var("TEST_HYPHA_LOGS").is_ok() {
                    Stdio::inherit()
                } else {
                    Stdio::null()
                })
                .spawn()
                .unwrap_or_else(|e| panic!("spawning `{bin} server` (set TEST_MINIO_BIN?): {e}"));

            let mut minio = Self {
                endpoint,
                access_key,
                secret_key,
                local: Some(LocalMinio {
                    child,
                    _data_dir: data_dir,
                }),
            };
            match minio.await_spawned().await {
                Ok(()) => return minio,
                Err(e) => last_error = e,
            }
        }
        panic!("MinIO could not claim a test port after 8 attempts: {last_error}");
    }

    /// An S3 client bound straight to the backend with its own credentials — used to inspect it
    /// directly (ciphertext-at-rest checks, cache-state assertions).
    pub fn raw_client(&self) -> Client {
        s3_client(&self.endpoint, &self.access_key, &self.secret_key)
    }

    /// Wait for a *spawned* server, which can also die: a port this fixture lost the race for shows
    /// up as an exited child rather than as a timeout, and the caller retries on a fresh port.
    async fn await_spawned(&mut self) -> Result<(), String> {
        for _ in 0..240 {
            let exited = self
                .local
                .as_mut()
                .expect("only a spawned server is awaited here")
                .child
                .try_wait()
                .map_err(|e| format!("checking MinIO process: {e}"))?;
            if let Some(status) = exited {
                return Err(format!(
                    "MinIO at {} exited during startup: {status}",
                    self.endpoint
                ));
            }
            if self.raw_client().list_buckets().send().await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Err(format!(
            "MinIO at {} did not become ready within 60s",
            self.endpoint
        ))
    }

    /// Wait for a server someone else owns. It may still be starting when the first test reaches it
    /// — a compose fixture answers its port before its filer is up — and there is no process to
    /// watch, so this is a plain readiness poll.
    async fn await_serving(&self) -> Result<(), String> {
        let mut last = String::from("no attempt made");
        for _ in 0..120 {
            match self.raw_client().list_buckets().send().await {
                Ok(_) => return Ok(()),
                Err(e) => last = e.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(format!("{} after 60s: {last}", self.endpoint))
    }
}

impl Drop for TestS3 {
    fn drop(&mut self) {
        if let Some(local) = &mut self.local {
            let _ = local.child.kill();
            let _ = local.child.wait();
        }
    }
}

// ── fault-injecting S3 proxy ─────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct CapturedRequest {
    pub method: Method,
    pub path: String,
    pub headers: hyper::HeaderMap,
}

enum FaultAction {
    FailBefore(StatusCode, usize),
    FailAfter(StatusCode, usize),
    Pause(oneshot::Receiver<()>),
    PauseThenFail(oneshot::Receiver<()>, StatusCode),
}

struct FaultRule {
    method: Method,
    path: String,
    prefix: bool,
    action: FaultAction,
    hit: Option<oneshot::Sender<CapturedRequest>>,
}

/// Indiscriminate, probabilistic faults — the half of the fault surface the explicit rules cannot
/// reach.
///
/// A rule names one method and one path, which is what makes a *targeted* test readable, and also
/// what makes it a test of the failure the author already thought of. Chaos disturbs a share of
/// **everything**: a backend call hypha makes for a reason nobody wrote a rule about is exactly the
/// one worth breaking.
///
/// `seed` fixes the *distribution*, not the interleaving. Requests arrive concurrently, so which
/// request draws which fault is not reproducible from the seed alone; a failure is reproduced by
/// re-running with the seed printed by the test, which converges quickly because the faults are dense
/// rather than by hitting the same request twice.
struct Chaos {
    rng: Mutex<u64>,
    /// Share of requests disturbed at all, in `[0, 1]`.
    rate: f64,
    /// Include faults injected *after* the backend acted, so the operation lands and the caller is
    /// told it did not. hypha must treat that as indeterminate rather than as "did not happen".
    lose_responses: bool,
}

impl Chaos {
    /// splitmix64 — a decent scrambler in a few lines, so the harness has a seeded RNG without a
    /// dependency whose version would change what a seed means.
    fn next(&self) -> u64 {
        let mut state = self.rng.lock().expect("chaos rng mutex poisoned");
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// The fault this request draws, if any.
    fn roll(&self) -> Option<FaultAction> {
        let draw = self.next();
        if (draw >> 11) as f64 / (1u64 << 53) as f64 >= self.rate {
            return None;
        }
        // 503 and 500 are the two a backend under duress actually returns, and both reach hypha as
        // `Error::Backend`. Deliberately no 412: a precondition that fails at random would not be a
        // fault, it would be a backend that lies about the CAS every hypha guarantee rests on.
        let status = match draw & 1 {
            0 => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::SERVICE_UNAVAILABLE,
        };
        match (self.lose_responses, (draw >> 1) & 3) {
            (true, 0) => Some(FaultAction::FailAfter(status, 1)),
            _ => Some(FaultAction::FailBefore(status, 1)),
        }
    }
}

#[derive(Clone, Default)]
pub struct FaultInjector {
    rules: Arc<Mutex<Vec<FaultRule>>>,
    /// Consulted only when no explicit rule matches, so a test can still aim a precise fault at one
    /// request while everything around it is being disturbed at random.
    chaos: Arc<Mutex<Option<Chaos>>>,
}

impl FaultInjector {
    pub fn fail_next(
        &self,
        method: Method,
        path: impl Into<String>,
        status: StatusCode,
    ) -> oneshot::Receiver<CapturedRequest> {
        self.fail_times(method, path, status, 1)
    }

    pub fn fail_times(
        &self,
        method: Method,
        path: impl Into<String>,
        status: StatusCode,
        attempts: usize,
    ) -> oneshot::Receiver<CapturedRequest> {
        assert!(attempts > 0, "a repeated fault needs at least one attempt");
        self.push(
            method,
            path.into(),
            false,
            FaultAction::FailBefore(status, attempts),
        )
    }

    pub fn fail_next_prefix(
        &self,
        method: Method,
        path_prefix: impl Into<String>,
        status: StatusCode,
    ) -> oneshot::Receiver<CapturedRequest> {
        self.fail_prefix_times(method, path_prefix, status, 1)
    }

    pub fn fail_prefix_times(
        &self,
        method: Method,
        path_prefix: impl Into<String>,
        status: StatusCode,
        attempts: usize,
    ) -> oneshot::Receiver<CapturedRequest> {
        assert!(attempts > 0, "a repeated fault needs at least one attempt");
        self.push(
            method,
            path_prefix.into(),
            true,
            FaultAction::FailBefore(status, attempts),
        )
    }

    /// Forward the matching request successfully, then replace the backend response with `status`.
    /// This creates the indeterminate-commit case a client-visible transport failure normally does.
    pub fn fail_next_response(
        &self,
        method: Method,
        path: impl Into<String>,
        status: StatusCode,
    ) -> oneshot::Receiver<CapturedRequest> {
        self.fail_response_times(method, path, status, 1)
    }

    pub fn fail_response_times(
        &self,
        method: Method,
        path: impl Into<String>,
        status: StatusCode,
        attempts: usize,
    ) -> oneshot::Receiver<CapturedRequest> {
        assert!(attempts > 0, "a repeated fault needs at least one attempt");
        self.push(
            method,
            path.into(),
            false,
            FaultAction::FailAfter(status, attempts),
        )
    }

    pub fn pause_next(&self, method: Method, path: impl Into<String>) -> PausedRequest {
        let (release, wait) = oneshot::channel();
        let hit = self.push(method, path.into(), false, FaultAction::Pause(wait));
        PausedRequest {
            hit: Some(hit),
            release: Some(release),
        }
    }

    /// Pause the next request whose path *starts with* `path_prefix` — for bucket-level ops, where
    /// whether the SDK emits `/{bucket}` or `/{bucket}/` is not worth pinning in a test.
    pub fn pause_next_prefix(
        &self,
        method: Method,
        path_prefix: impl Into<String>,
    ) -> PausedRequest {
        let (release, wait) = oneshot::channel();
        let hit = self.push(method, path_prefix.into(), true, FaultAction::Pause(wait));
        PausedRequest {
            hit: Some(hit),
            release: Some(release),
        }
    }

    pub fn pause_next_then_fail(
        &self,
        method: Method,
        path: impl Into<String>,
        status: StatusCode,
    ) -> PausedRequest {
        let (release, wait) = oneshot::channel();
        let hit = self.push(
            method,
            path.into(),
            false,
            FaultAction::PauseThenFail(wait, status),
        );
        PausedRequest {
            hit: Some(hit),
            release: Some(release),
        }
    }

    fn push(
        &self,
        method: Method,
        path: String,
        prefix: bool,
        action: FaultAction,
    ) -> oneshot::Receiver<CapturedRequest> {
        let (hit, observed) = oneshot::channel();
        self.rules
            .lock()
            .expect("fault rule mutex poisoned")
            .push(FaultRule {
                method,
                path,
                prefix,
                action,
                hit: Some(hit),
            });
        observed
    }

    fn take(&self, method: &Method, path: &str) -> Option<FaultRule> {
        if let Some(rule) = self.take_rule(method, path) {
            return Some(rule);
        }
        self.chaos_roll()
    }

    fn take_rule(&self, method: &Method, path: &str) -> Option<FaultRule> {
        let mut rules = self.rules.lock().expect("fault rule mutex poisoned");
        let decoded = percent_encoding::percent_decode_str(path)
            .decode_utf8_lossy()
            .into_owned();
        let index = rules.iter().position(|r| {
            r.method == *method
                && if r.prefix {
                    path.starts_with(&r.path) || decoded.starts_with(&r.path)
                } else {
                    r.path == path || r.path == decoded
                }
        })?;
        let repeated = match &rules[index].action {
            FaultAction::FailBefore(status, remaining) if *remaining > 1 => Some((*status, false)),
            FaultAction::FailAfter(status, remaining) if *remaining > 1 => Some((*status, true)),
            _ => None,
        };
        if let Some((status, after)) = repeated {
            let rule = &mut rules[index];
            let remaining = match &mut rule.action {
                FaultAction::FailBefore(_, remaining) | FaultAction::FailAfter(_, remaining) => {
                    remaining
                }
                _ => unreachable!("repeat classification and mutation share one lock"),
            };
            *remaining -= 1;
            return Some(FaultRule {
                method: rule.method.clone(),
                path: rule.path.clone(),
                prefix: rule.prefix,
                action: if after {
                    FaultAction::FailAfter(status, 1)
                } else {
                    FaultAction::FailBefore(status, 1)
                },
                hit: rule.hit.take(),
            });
        }
        Some(rules.remove(index))
    }

    /// Disturb `rate` of all requests from here on. Returns the seed, so a test can print it and a
    /// failure can be re-run against the same distribution.
    pub fn chaos(&self, seed: u64, rate: f64, lose_responses: bool) -> u64 {
        *self.chaos.lock().expect("chaos mutex poisoned") = Some(Chaos {
            rng: Mutex::new(seed),
            rate,
            lose_responses,
        });
        seed
    }

    /// Stop disturbing requests. Everything after this reaches the backend, which is what lets a test
    /// separate "what survived the storm" from "what the storm is still doing".
    pub fn calm(&self) {
        *self.chaos.lock().expect("chaos mutex poisoned") = None;
    }

    fn chaos_roll(&self) -> Option<FaultRule> {
        let chaos = self.chaos.lock().expect("chaos mutex poisoned");
        let action = chaos.as_ref()?.roll()?;
        Some(FaultRule {
            method: Method::GET,
            path: String::new(),
            prefix: false,
            action,
            hit: None,
        })
    }

    pub fn clear(&self) {
        self.rules
            .lock()
            .expect("fault rule mutex poisoned")
            .clear();
    }
}

pub struct PausedRequest {
    hit: Option<oneshot::Receiver<CapturedRequest>>,
    release: Option<oneshot::Sender<()>>,
}

impl PausedRequest {
    pub async fn reached(&mut self) -> CapturedRequest {
        self.hit
            .take()
            .expect("pause can be observed only once")
            .await
            .expect("fault proxy stopped before the request arrived")
    }

    pub fn release(mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

impl Drop for PausedRequest {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

pub struct FaultProxy {
    pub endpoint: String,
    pub faults: FaultInjector,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl FaultProxy {
    async fn start(target: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fault proxy");
        let addr = listener.local_addr().expect("fault proxy address");
        let target = target.trim_end_matches('/').to_string();
        let faults = FaultInjector::default();
        let serve_faults = faults.clone();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("fault proxy client");
        let (shutdown, mut stop) = oneshot::channel();
        let task = tokio::spawn(async move {
            let http = Arc::new(ConnBuilder::new(TokioExecutor::new()));
            loop {
                let accepted = tokio::select! {
                    accepted = listener.accept() => accepted,
                    _ = &mut stop => return,
                };
                let Ok((stream, _)) = accepted else {
                    continue;
                };
                let (target, faults, client, http) = (
                    target.clone(),
                    serve_faults.clone(),
                    client.clone(),
                    http.clone(),
                );
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(move |req| {
                        proxy_s3(req, target.clone(), faults.clone(), client.clone())
                    });
                    let _ = http.serve_connection(TokioIo::new(stream), service).await;
                });
            }
        });
        Self {
            endpoint: format!("http://{addr}"),
            faults,
            shutdown: Some(shutdown),
            task,
        }
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.abort();
    }
}

async fn proxy_s3(
    req: Request<Incoming>,
    target: String,
    faults: FaultInjector,
    client: reqwest::Client,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let (parts, body) = req.into_parts();
    let method = parts.method;
    let path = parts.uri.path().to_string();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|v| v.as_str())
        .unwrap_or(parts.uri.path())
        .to_string();
    let headers = parts.headers;
    let body = match body.collect().await {
        Ok(body) => body.to_bytes(),
        Err(e) => return Ok(proxy_error(StatusCode::BAD_GATEWAY, &e.to_string())),
    };

    let mut fail_after = None;
    if let Some(rule) = faults.take(&method, &path) {
        if let Some(hit) = rule.hit {
            let _ = hit.send(CapturedRequest {
                method: method.clone(),
                path: path_and_query.clone(),
                headers: headers.clone(),
            });
        }
        match rule.action {
            FaultAction::FailBefore(status, _) => {
                return Ok(proxy_error(status, "injected fault"));
            }
            FaultAction::FailAfter(status, _) => fail_after = Some(status),
            FaultAction::Pause(release) => {
                let _ = release.await;
            }
            FaultAction::PauseThenFail(release, status) => {
                let _ = release.await;
                return Ok(proxy_error(status, "injected fault after pause"));
            }
        }
    }

    let url = format!("{target}{path_and_query}");
    let forwarded = client
        .request(method, url)
        .headers(headers)
        .body(body)
        .send()
        .await;
    let response = match forwarded {
        Ok(response) => response,
        Err(e) => return Ok(proxy_error(StatusCode::BAD_GATEWAY, &e.to_string())),
    };
    if let Some(status) = fail_after {
        return Ok(proxy_error(status, "injected response loss"));
    }

    let status = response.status();
    let headers = response.headers().clone();
    let body = match response.bytes().await {
        Ok(body) => body,
        Err(e) => return Ok(proxy_error(StatusCode::BAD_GATEWAY, &e.to_string())),
    };
    let mut out = Response::builder().status(status);
    for (name, value) in &headers {
        if name != hyper::header::CONNECTION && name != hyper::header::TRANSFER_ENCODING {
            out = out.header(name, value);
        }
    }
    Ok(out
        .body(Full::new(body))
        .expect("backend response status and headers are valid"))
}

fn proxy_error(status: StatusCode, message: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "text/plain")
        .body(Full::new(Bytes::copy_from_slice(message.as_bytes())))
        .expect("fixed fault response is valid")
}

// ── the cache-usage source ───────────────────────────────────────────────────────────────────

/// The SeaweedFS master + volume server GC reads its pressure from (§8), with the numbers under the
/// test's control.
///
/// MinIO reports no usage at all, so the plain harness has no pressure source and its passes sweep
/// debris and never evict — which is why nothing before this could reach the eviction half of §8.
/// Setting the figure rather than measuring it is also what makes an *unmeetable* target
/// expressible: a real cache shrinks as GC evicts, so a pass could never be observed escalating
/// past the rung its first reclaim satisfied.
pub struct CacheUsage {
    /// The master base URL, as `gc.usage.master`. It doubles as its own volume server, since the
    /// topology names whatever address this reports.
    pub endpoint: String,
    state: Arc<Mutex<UsageState>>,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

#[derive(Default)]
struct UsageState {
    used: u64,
    capacity: u64,
    /// Disk-status reads, i.e. how many samples GC has taken — a pass takes one either side of its
    /// work, so this is also the evidence that passes are running at all.
    samples: usize,
    vacuums: usize,
}

impl CacheUsage {
    /// A source reporting `capacity` bytes of space and nothing used yet.
    pub async fn start(capacity: u64) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind usage source");
        let addr = listener.local_addr().expect("usage source address");
        let endpoint = format!("http://{addr}");
        let state = Arc::new(Mutex::new(UsageState {
            capacity,
            ..UsageState::default()
        }));

        let (shutdown, mut stop) = oneshot::channel();
        let served = state.clone();
        let base = endpoint.clone();
        let task = tokio::spawn(async move {
            let http = Arc::new(ConnBuilder::new(TokioExecutor::new()));
            loop {
                let accepted = tokio::select! {
                    accepted = listener.accept() => accepted,
                    _ = &mut stop => return,
                };
                let Ok((stream, _)) = accepted else { continue };
                let (base, state, http) = (base.clone(), served.clone(), http.clone());
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(move |req| {
                        serve_usage(req, base.clone(), state.clone())
                    });
                    let _ = http.serve_connection(TokioIo::new(stream), service).await;
                });
            }
        });
        Self {
            endpoint,
            state,
            shutdown: Some(shutdown),
            task,
        }
    }

    /// Report `used` bytes from now on. Whether that is over the high-water mark is the whole of
    /// what decides if a pass evicts.
    pub fn set_used(&self, used: u64) {
        self.lock().used = used;
    }

    /// Report usage as a fraction of capacity — how a test says "over the high-water mark" without
    /// restating the capacity it chose.
    pub fn set_ratio(&self, ratio: f64) {
        let capacity = self.lock().capacity;
        self.set_used((capacity as f64 * ratio) as u64);
    }

    pub fn samples(&self) -> usize {
        self.lock().samples
    }

    pub fn vacuums(&self) -> usize {
        self.lock().vacuums
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, UsageState> {
        self.state.lock().expect("usage state mutex poisoned")
    }
}

impl Drop for CacheUsage {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.abort();
    }
}

/// The two response shapes `gc::usage` reads, plus the vacuum it asks for on a pressured pass.
async fn serve_usage(
    req: Request<Incoming>,
    base: String,
    state: Arc<Mutex<UsageState>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let body = match req.uri().path() {
        // The master knows the topology, not the bytes: it names volume servers, and this one names
        // itself so `/status` below is the second hop.
        "/dir/status" => format!(
            r#"{{"Topology":{{"DataCenterInfos":[{{"RackInfos":[{{"DataNodeInfos":[
                 {{"Url":"{base}"}}]}}]}}]}}}}"#
        ),
        "/status" => {
            let mut state = state.lock().expect("usage state mutex poisoned");
            state.samples += 1;
            format!(
                r#"{{"DiskStatuses":[{{"dir":"/data","all":{},"used":{}}}]}}"#,
                state.capacity, state.used
            )
        }
        "/vol/vacuum" => {
            state.lock().expect("usage state mutex poisoned").vacuums += 1;
            "{}".to_string()
        }
        _ => {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::new()))
                .expect("fixed 404 is valid"))
        }
    };
    Ok(Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
        .expect("fixed JSON response is valid"))
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
        let (service, lifecycle) = hypha::build_service(config)
            .await
            .expect("build hypha service");
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
            // The binary's config layer claims the whole `HYPHA_` namespace and **rejects** a key it
            // does not know, so anything of that shape in the test runner's environment would fail
            // the child at load. Dropping them all first also means the child's config is exactly
            // `config` — no ambient value can layer under it.
            .env_clear()
            .envs(std::env::vars().filter(|(k, _)| !k.starts_with("HYPHA_")))
            .envs(config_env(config))
            // A stray `hypha.toml` in the crate dir would otherwise layer under the env config.
            .current_dir(std::env::temp_dir())
            .stdout(if std::env::var("TEST_HYPHA_LOGS").is_ok() {
                Stdio::inherit()
            } else {
                Stdio::null()
            })
            .stderr(if std::env::var("TEST_HYPHA_LOGS").is_ok() {
                Stdio::inherit()
            } else {
                Stdio::null()
            })
            .spawn()
            .expect("spawning the hypha binary");

        Self {
            child,
            endpoint: format!("http://{listen}"),
        }
    }

    /// Generous because startup resolves every bucket of the deployment before it accepts anything,
    /// and a shared backend carries every fixture's.
    async fn await_ready(&self) {
        const READY_BUDGET: Duration = Duration::from_secs(60);
        let client = s3_client(&self.endpoint, HYPHA_ACCESS, HYPHA_SECRET);
        let deadline = std::time::Instant::now() + READY_BUDGET;
        while std::time::Instant::now() < deadline {
            if client.list_buckets().send().await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "hypha at {} did not become ready within {READY_BUDGET:?}",
            self.endpoint
        );
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
        "HYPHA_SERVING__ADMIN_LISTEN".into(),
        config.serving.admin_listen.clone(),
    );
    env.insert(
        "HYPHA_RECONCILE__INTERVAL_MS".into(),
        config.reconcile.interval_ms.to_string(),
    );
    env.insert(
        "HYPHA_RECONCILE__CONCURRENCY".into(),
        config.reconcile.concurrency.to_string(),
    );
    env.insert(
        "HYPHA_BACKGROUND__CONCURRENCY".into(),
        config.background.concurrency.to_string(),
    );
    env.insert(
        "HYPHA_BACKGROUND__QUEUE_DEPTH".into(),
        config.background.queue_depth.to_string(),
    );
    env.insert(
        "HYPHA_VOLUME_WATCH_INTERVAL_MS".into(),
        config.volume_watch_interval_ms.to_string(),
    );
    // Every GC field, not the handful a test happens to move: without them the child would run GC's
    // *production* settings — a five-minute interval, a hundred-thousand-key ring — while every
    // in-process test runs the harness's, so a subprocess test that touched GC at all would silently
    // be testing nothing. Passing them all is what keeps that true as fields are added.
    let gc = &config.gc;
    env.insert("HYPHA_GC__INTERVAL_MS".into(), gc.interval_ms.to_string());
    env.insert(
        "HYPHA_GC__MIN_INTERVAL_MS".into(),
        gc.min_interval_ms.to_string(),
    );
    env.insert("HYPHA_GC__CONCURRENCY".into(), gc.concurrency.to_string());
    env.insert(
        "HYPHA_GC__MAX_CONCURRENCY".into(),
        gc.max_concurrency.to_string(),
    );
    env.insert("HYPHA_GC__HIGH_WATER".into(), gc.high_water.to_string());
    env.insert("HYPHA_GC__LOW_WATER".into(), gc.low_water.to_string());
    env.insert("HYPHA_GC__PROBE_PAGES".into(), gc.probe_pages.to_string());
    env.insert("HYPHA_GC__YIELD_FLOOR".into(), gc.yield_floor.to_string());
    env.insert(
        "HYPHA_GC__OPPORTUNISTIC_EVICTIONS".into(),
        gc.opportunistic_evictions.to_string(),
    );
    env.insert(
        "HYPHA_GC__RECENCY__FILL_TARGET".into(),
        gc.recency.fill_target.to_string(),
    );
    env.insert(
        "HYPHA_GC__RECENCY__DEPTH".into(),
        gc.recency.depth.to_string(),
    );
    env.insert(
        "HYPHA_GC__RECENCY__FALSE_POSITIVE_RATE".into(),
        gc.recency.false_positive_rate.to_string(),
    );
    if let Some(Usage::Seaweedfs {
        master,
        garbage_threshold,
    }) = &gc.usage
    {
        env.insert("HYPHA_GC__USAGE__KIND".into(), "seaweedfs".into());
        env.insert("HYPHA_GC__USAGE__MASTER".into(), master.clone());
        env.insert(
            "HYPHA_GC__USAGE__GARBAGE_THRESHOLD".into(),
            garbage_threshold.to_string(),
        );
    }
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
    /// The cache backend, shared with the remote only in the default all-MinIO fixture.
    pub storage: TestS3,
    remote_storage: Option<TestS3>,
    pub hypha: Server,
    pub config: Config,
    cache_proxy: Option<FaultProxy>,
    remote_proxy: Option<FaultProxy>,
    usage: Option<CacheUsage>,
}

/// How a harness differs from the default one, chosen before anything is started.
///
/// A builder rather than a constructor per combination: the axes (mode, fault proxies, in-process vs
/// the real binary, a usage source, and the §8 knobs a test needs to move) are independent, and
/// naming their product was already the reason `with_faults`/`subprocess` had begun to duplicate
/// each other.
pub struct HarnessBuilder {
    mode: Mode,
    faults: bool,
    subprocess: bool,
    /// Cache capacity the usage source reports, if a test wants one at all.
    capacity: Option<u64>,
    tune: Option<Tune>,
}

/// A test's own adjustment to the config, applied last — after the proxies and the usage source have
/// filled in their endpoints, so it can override anything.
type Tune = Box<dyn FnOnce(&mut Config)>;

impl HarnessBuilder {
    /// Interpose independent cache and remote proxies, so a test can fail, pause, or lose the
    /// response to one backend operation without changing MinIO's behaviour for any other call.
    pub fn with_faults(mut self) -> Self {
        self.faults = true;
        self
    }

    /// Run hypha as the shipped binary. Needed only where a test asserts *process-level* behaviour —
    /// the metrics recorder and the admin listener exist nowhere else (§10), and an invariant
    /// violation's `process::exit` would take the test runner down in-process.
    pub fn subprocess(mut self) -> Self {
        self.subprocess = true;
        self
    }

    /// Give GC a pressure source reporting `capacity` bytes of space, initially unused.
    pub fn with_usage(mut self, capacity: u64) -> Self {
        self.capacity = Some(capacity);
        self
    }

    /// Adjust the config before hypha reads it — for the §8 knobs whose defaults are deliberately
    /// production-shaped (the water marks, the ladder's bounds, the ring's geometry).
    pub fn tune(mut self, tune: impl FnOnce(&mut Config) + 'static) -> Self {
        self.tune = Some(Box::new(tune));
        self
    }

    pub async fn start(self) -> Harness {
        if external_remote_backend().is_some() && endpoint_env("TEST_CACHE_S3_ENDPOINT").is_some() {
            panic!("TEST_S3_ENDPOINT and TEST_CACHE_S3_ENDPOINT are mutually exclusive");
        }
        let (storage, remote_storage) =
            if let Some(endpoint) = endpoint_env("TEST_CACHE_S3_ENDPOINT") {
                (
                    TestS3::connect(endpoint, "TEST_CACHE_S3_ENDPOINT").await,
                    Some(TestS3::start_minio().await),
                )
            } else {
                (TestS3::start().await, None)
            };
        let remote = remote_storage.as_ref().unwrap_or(&storage);
        let (cache_proxy, remote_proxy) = if self.faults {
            (
                Some(FaultProxy::start(&storage.endpoint).await),
                Some(FaultProxy::start(&remote.endpoint).await),
            )
        } else {
            (None, None)
        };
        let usage = match self.capacity {
            Some(capacity) => Some(CacheUsage::start(capacity).await),
            None => None,
        };

        let mut config = base_config(&storage, remote, self.mode);
        if let Some(proxy) = &cache_proxy {
            config.cache.endpoint = proxy.endpoint.clone();
        }
        if let Some(proxy) = &remote_proxy {
            config.remote.endpoint = proxy.endpoint.clone();
        }
        if let Some(source) = &usage {
            config.gc.usage = Some(Usage::Seaweedfs {
                master: source.endpoint.clone(),
                garbage_threshold: 0.3,
            });
        }
        if self.subprocess {
            config.serving.listen = format!("127.0.0.1:{}", free_port());
        }
        if let Some(tune) = self.tune {
            tune(&mut config);
        }

        let hypha = if self.subprocess {
            Server::Child(ChildHypha::start(&config).await)
        } else {
            Server::InProcess(Hypha::start(&config).await)
        };
        Harness {
            storage,
            remote_storage,
            hypha,
            config,
            cache_proxy,
            remote_proxy,
            usage,
        }
    }
}

impl Harness {
    pub fn builder(mode: Mode) -> HarnessBuilder {
        HarnessBuilder {
            mode,
            faults: false,
            subprocess: false,
            capacity: None,
            tune: None,
        }
    }

    /// A durable-mode deployment: MinIO backs both roles unless the cache fixture is overridden.
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
        Self::builder(Mode::Durable).subprocess().start().await
    }

    pub async fn cached_subprocess() -> Self {
        Self::builder(Mode::Cached).subprocess().start().await
    }

    /// The child process, for tests driving it directly. Panics on an in-process harness.
    pub fn child(&mut self) -> &mut ChildHypha {
        match &mut self.hypha {
            Server::Child(h) => h,
            Server::InProcess(_) => panic!("harness is running hypha in-process"),
        }
    }

    pub async fn with_mode(mode: Mode) -> Self {
        Self::builder(mode).start().await
    }

    pub async fn cached_with_faults() -> Self {
        Self::builder(Mode::Cached).with_faults().start().await
    }

    pub async fn durable_with_faults() -> Self {
        Self::builder(Mode::Durable).with_faults().start().await
    }

    /// The usage source this harness gave GC. Panics on a harness built without one, since a test
    /// that reads pressure it never configured is asserting against a source GC cannot see.
    pub fn usage(&self) -> &CacheUsage {
        self.usage
            .as_ref()
            .expect("harness has no cache-usage source")
    }

    pub fn cache_faults(&self) -> FaultInjector {
        self.cache_proxy
            .as_ref()
            .expect("harness has no cache fault proxy")
            .faults
            .clone()
    }

    pub fn remote_faults(&self) -> FaultInjector {
        self.remote_proxy
            .as_ref()
            .expect("harness has no remote fault proxy")
            .faults
            .clone()
    }

    /// A fresh S3 client pointed at hypha, authenticating as a hypha client.
    pub fn client(&self) -> Client {
        s3_client(self.hypha.endpoint(), HYPHA_ACCESS, HYPHA_SECRET)
    }

    /// A client pointed straight at the cache backend — bypasses hypha.
    pub fn raw(&self) -> Client {
        self.storage.raw_client()
    }

    /// A client pointed straight at the remote backend — bypasses hypha.
    pub fn raw_remote(&self) -> Client {
        self.remote_storage
            .as_ref()
            .unwrap_or(&self.storage)
            .raw_client()
    }

    pub fn raw_for_bucket(&self, bucket: &str) -> Client {
        if bucket.starts_with(&self.config.role_prefix(REMOTE_ROLE)) {
            self.raw_remote()
        } else {
            self.raw()
        }
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

    /// Block until hypha answers a request. [`serve`](hypha::serve) runs startup to completion before
    /// it accepts anything, so this is also the signal that startup *finished* — clean markers taken
    /// off disk, every bucket resolved, recoveries dispatched. A test asserting on any of those has to
    /// wait for it; the in-process server is spawned and returns before startup has run.
    pub async fn await_ready(&self) {
        let client = self.client();
        wait_until(15_000, "hypha to finish startup and serve", || {
            let client = client.clone();
            async move { client.list_buckets().send().await.is_ok() }
        })
        .await;
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

fn base_config(cache: &TestS3, remote: &TestS3, mode: Mode) -> Config {
    Config {
        remote: endpoint_cfg(remote),
        cache: endpoint_cfg(cache),
        bucket_prefix: unique_bucket_prefix(),
        mode,
        auth: ClientAuth {
            access_key: HYPHA_ACCESS.to_string(),
            secret_key: HYPHA_SECRET.to_string(),
        },
        master_passphrase: MASTER_PASSPHRASE.to_string(),
        serving: Serving {
            // Its own port per harness, since the production default is a fixed one (§10) and these
            // run concurrently. Only the subprocess ones bind it at all — and there, binding it is
            // itself under test, because a bind failure takes the whole process down.
            admin_listen: format!("127.0.0.1:{}", free_port()),
            ..Serving::default()
        },
        // A tight reconcile cadence so cached-mode tests observe uploads/propagation promptly rather
        // than waiting out the production interval.
        reconcile: Reconcile {
            interval_ms: 150,
            concurrency: 8,
            backpressure: Backpressure::default(),
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

fn endpoint_cfg(storage: &TestS3) -> S3Endpoint {
    S3Endpoint {
        endpoint: storage.endpoint.clone(),
        region: "us-east-1".to_string(),
        access_key: storage.access_key.clone(),
        secret_key: storage.secret_key.clone(),
    }
}

/// Grab a currently-free localhost port by binding to :0 and immediately releasing it. MinIO startup
/// retries if another test claims it before the child binds.
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

/// Complete a multipart upload from `(part_number, etag)` pairs. Returns the composite ETag hypha
/// reports.
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
        .raw_remote()
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

pub async fn drop_backend_bucket(harness: &Harness, bucket: &str) {
    for key in raw_list(&harness.raw(), bucket, None).await {
        harness
            .raw()
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("delete backend object");
    }
    harness
        .raw()
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("drop backend bucket");
}

// ── polling and backend inspection ───────────────────────────────────────────────────────────

/// Poll `cond` every 50 ms until it holds or `ms` elapses (then panic naming `what`). Every
/// background duty here — reconcile, rehydrate, a GC pass — lands asynchronously, so this is how a
/// test states the outcome it is waiting for rather than a sleep long enough to usually work.
/// One request to the binary's admin listener (§10) — `(status, body)`. In-process harnesses bind no
/// admin port, so this is only meaningful for a `subprocess()` one.
pub async fn admin_get(h: &Harness, path: &str) -> (u16, String) {
    let url = format!("http://{}{path}", h.config.serving.admin_listen);
    let resp = reqwest::get(&url).await.expect("admin request");
    let status = resp.status().as_u16();
    (status, resp.text().await.expect("admin body"))
}

pub async fn wait_until<F, Fut>(ms: u64, what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + Duration::from_millis(ms);
    loop {
        if cond().await {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out after {ms}ms waiting for: {what}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The opposite assertion, and one worth spelling out: hold for `ms` and fail the moment `cond`
/// becomes true. A background duty that must *not* act needs a real exposure window, or the test
/// passes by outrunning it.
pub async fn stays_false<F, Fut>(ms: u64, what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + Duration::from_millis(ms);
    while std::time::Instant::now() < deadline {
        assert!(!cond().await, "{what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub async fn raw_exists(h: &Harness, bucket: &str, key: &str) -> bool {
    h.raw_for_bucket(bucket)
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .is_ok()
}

/// The pending marker for `key` lives at bare `K` in `<meta>` (§6).
pub async fn marker_present(h: &Harness, bucket: &str, key: &str) -> bool {
    raw_exists(h, &h.meta_bucket(bucket), key).await
}

pub async fn remote_present(h: &Harness, bucket: &str, key: &str) -> bool {
    raw_exists(h, &h.remote_bucket(bucket), key).await
}

/// Classify the `<data>` entry at `key`: `None` ⇒ a live body, `Some(kind)` ⇒ a tombstone (§6).
/// Panics if nothing is there at all, which is a third state a caller has to distinguish itself.
pub async fn data_class(
    h: &Harness,
    bucket: &str,
    key: &str,
) -> Option<hypha_core::meta::TombKind> {
    let head = h
        .raw()
        .head_object()
        .bucket(h.cache_bucket(bucket))
        .key(key)
        .send()
        .await
        .expect("data head");
    hypha_core::meta::classify_entry(
        head.content_length().unwrap_or(0),
        head.e_tag().unwrap_or_default().trim_matches('"'),
    )
}

/// The `<data>` entry's user metadata, where a tombstone's authoritative facts live (§6).
pub async fn data_metadata(h: &Harness, bucket: &str, key: &str) -> HashMap<String, String> {
    h.raw()
        .head_object()
        .bucket(h.cache_bucket(bucket))
        .key(key)
        .send()
        .await
        .expect("data head")
        .metadata
        .unwrap_or_default()
}

/// Every twin of `key` currently in `<meta>` (range B, `0x01 ‖ key ‖ 0x01 ‖ facts`). More than one
/// is debris from a crash between a twin refresh's delete and its write (§6).
pub async fn twins_of(h: &Harness, bucket: &str, key: &str) -> Vec<String> {
    let c = hypha_core::meta::CTRL as char;
    raw_list(
        &h.raw(),
        &h.meta_bucket(bucket),
        Some(&format!("{c}{key}{c}")),
    )
    .await
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

/// The same into `<meta>` — where hypha's own artifacts live (twins, markers, mpu records, shadow
/// bodies), so this is how a test plants one of those directly.
pub async fn raw_meta_put(
    harness: &Harness,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    metadata: HashMap<String, String>,
) {
    harness
        .raw()
        .put_object()
        .bucket(harness.meta_bucket(bucket))
        .key(key)
        .body(ByteStream::from(body))
        .set_metadata(Some(metadata))
        .send()
        .await
        .expect("raw meta put");
}

/// Plant an eviction tombstone over `key` exactly as GC's own transition would (§8) — the facts twin
/// first, then the evict sentinel carrying the authoritative facts — leaving the key resolvable from
/// the remote and rehydratable on read. The caller is responsible for the remote already holding this
/// generation; an eviction tombstone without it is invariant **I2**, not a valid state to plant.
pub async fn plant_eviction_tombstone(h: &Harness, bucket: &str, key: &str, body: &[u8]) {
    let facts = hypha_core::meta::Facts {
        client_etag: md5_hex(body),
        plen: body.len() as u64,
        mtime_ms: 1,
    };
    if let Some(twin) = facts.twin_key(key) {
        h.raw()
            .put_object()
            .bucket(h.meta_bucket(bucket))
            .key(twin)
            .body(ByteStream::from(Vec::new()))
            .send()
            .await
            .expect("plant twin");
    }
    let mut md = HashMap::new();
    md.insert(
        hypha_core::meta::TOMB.to_string(),
        hypha_core::meta::TOMB_EVICT.to_string(),
    );
    md.insert(hypha_core::meta::PLEN.to_string(), facts.plen.to_string());
    md.insert(
        hypha_core::meta::CETAG.to_string(),
        facts.client_etag.clone(),
    );
    md.insert(
        hypha_core::meta::MTIME.to_string(),
        facts.mtime_ms.to_string(),
    );
    md.insert(
        hypha_core::meta::SCLASS.to_string(),
        hypha_core::meta::STANDARD.to_string(),
    );
    raw_cache_put(
        h,
        bucket,
        key,
        hypha_core::meta::EVICT_SENTINEL.to_vec(),
        md,
    )
    .await;
}
