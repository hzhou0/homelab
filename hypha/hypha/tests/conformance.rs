//! Phase-2 exit: the durable S3 surface end-to-end against a real backend (MinIO), driven over
//! HTTP with a real `aws-sdk-s3` client. Covers PUT/GET/HEAD/DELETE round-trips, ranges, the
//! conditional-write preconditions, LIST classification, buckets, control-byte keys, and the
//! ciphertext-at-rest guarantee. Every test owns its MinIO and cleans up on drop.

mod common;

use common::*;

const B: &str = "objs";

/// PUT→GET identity and ETag correctness across sizes spanning the 64 KiB chunk boundary, plus the
/// at-rest guarantee: what lands on the remote is age ciphertext, never the plaintext.
#[tokio::test]
async fn roundtrip_sizes_etag_and_encryption_at_rest() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    // 0 and 1 byte, sub-chunk, exactly one chunk, chunk±1, and a multi-chunk body.
    let sizes = [0usize, 1, 100, 65_535, 65_536, 65_537, 200_000];
    for &n in &sizes {
        let key = format!("size/{n}");
        let body = pattern(n);
        let etag = put(&client, B, &key, &body).await;
        assert_eq!(
            etag,
            md5_hex(&body),
            "PUT ETag must be the plaintext MD5 (size {n})"
        );

        let got = get_all(&client, B, &key).await;
        assert_eq!(got, body, "GET must return the bytes PUT (size {n})");

        let head = client
            .head_object()
            .bucket(B)
            .key(&key)
            .send()
            .await
            .expect("head");
        assert_eq!(
            head.content_length(),
            Some(n as i64),
            "HEAD length (size {n})"
        );
        assert_eq!(
            head.e_tag().unwrap().trim_matches('"'),
            md5_hex(&body),
            "HEAD ETag (size {n})"
        );
    }

    // At rest: a recognizable plaintext must not appear in the remote object, which must be age.
    let marker = b"TOP-SECRET-PLAINTEXT-MARKER".repeat(64);
    let key = "secret";
    put(&client, B, key, &marker).await;
    let ct = raw_remote_object(&h, B, key).await;
    assert!(
        ct.starts_with(AGE_MAGIC),
        "remote object must be an age file"
    );
    assert!(
        !contains_subslice(&ct, b"TOP-SECRET-PLAINTEXT-MARKER"),
        "plaintext must not appear in the remote ciphertext"
    );
    assert!(
        ct.len() > marker.len(),
        "ciphertext+trailer must exceed the plaintext"
    );
}

/// Ranged GET: offsets, open-ended, suffix, and a range straddling a chunk boundary.
#[tokio::test]
async fn ranged_reads() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let body = pattern(200_000);
    let key = "ranged";
    put(&client, B, key, &body).await;

    assert_eq!(get_range(&client, B, key, 0, 9).await, body[0..10]);
    assert_eq!(
        get_range(&client, B, key, 1000, 2000).await,
        body[1000..2001]
    );
    // Straddle the 65 536-byte chunk boundary.
    assert_eq!(
        get_range(&client, B, key, 65_530, 65_540).await,
        body[65_530..65_541]
    );
    // Open-ended `bytes=N-`.
    let out = client
        .get_object()
        .bucket(B)
        .key(key)
        .range("bytes=199000-")
        .send()
        .await
        .expect("open-ended range");
    let tail = out.body.collect().await.unwrap().to_vec();
    assert_eq!(tail, body[199_000..]);
    // Suffix.
    assert_eq!(
        get_suffix(&client, B, key, 128).await,
        body[body.len() - 128..]
    );

    // A range wholly beyond the object is rejected.
    let err = client
        .get_object()
        .bucket(B)
        .key(key)
        .range("bytes=999999-1000000")
        .send()
        .await;
    assert!(err.is_err(), "range past EOF must error");
}

/// `If-None-Match: *` (no double-create) and `If-Match` (no lost update) preconditions.
#[tokio::test]
async fn conditional_writes() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "cond";

    // If-None-Match:* creates when absent…
    let v1 = pattern_seeded(1000, 1);
    let etag1 = client
        .put_object()
        .bucket(B)
        .key(key)
        .body(bytes_body(&v1))
        .content_length(v1.len() as i64)
        .if_none_match("*")
        .send()
        .await
        .expect("create-if-absent succeeds")
        .e_tag()
        .unwrap()
        .trim_matches('"')
        .to_string();
    assert_eq!(etag1, md5_hex(&v1));

    // …and refuses to overwrite an existing key (no double-create).
    let dupe = client
        .put_object()
        .bucket(B)
        .key(key)
        .body(bytes_body(&pattern_seeded(1000, 2)))
        .content_length(1000)
        .if_none_match("*")
        .send()
        .await;
    assert_eq!(
        sdk_err_code(&dupe.unwrap_err()).as_deref(),
        Some("PreconditionFailed")
    );
    assert_eq!(
        get_all(&client, B, key).await,
        v1,
        "refused write must not mutate"
    );

    // If-Match with the current ETag succeeds (compare-and-swap).
    let v2 = pattern_seeded(2000, 3);
    let etag2 = client
        .put_object()
        .bucket(B)
        .key(key)
        .body(bytes_body(&v2))
        .content_length(v2.len() as i64)
        .if_match(&etag1)
        .send()
        .await
        .expect("cas with current etag")
        .e_tag()
        .unwrap()
        .trim_matches('"')
        .to_string();
    assert_eq!(etag2, md5_hex(&v2));
    assert_eq!(get_all(&client, B, key).await, v2);

    // If-Match with a stale ETag is refused (no lost update).
    let stale = client
        .put_object()
        .bucket(B)
        .key(key)
        .body(bytes_body(&pattern_seeded(500, 9)))
        .content_length(500)
        .if_match(&etag1)
        .send()
        .await;
    assert_eq!(
        sdk_err_code(&stale.unwrap_err()).as_deref(),
        Some("PreconditionFailed")
    );
    assert_eq!(
        get_all(&client, B, key).await,
        v2,
        "stale CAS must not mutate"
    );
}

/// DELETE makes a key client-visibly absent (GET/HEAD 404, gone from LIST); it is idempotent.
#[tokio::test]
async fn delete_semantics() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "victim";

    put(&client, B, key, &pattern(4096)).await;
    client
        .delete_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("delete");

    let get = client.get_object().bucket(B).key(key).send().await;
    assert_eq!(
        sdk_err_code(&get.unwrap_err()).as_deref(),
        Some("NoSuchKey")
    );
    let head = client.head_object().bucket(B).key(key).send().await;
    assert!(head.is_err(), "HEAD of a deleted key must 404");

    let listed = list_keys(&client, B, None).await;
    assert!(
        !listed.contains(&key.to_string()),
        "deleted key must not list"
    );

    // Idempotent: deleting an absent key succeeds.
    client
        .delete_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("idempotent delete of absent key");
}

/// LIST: prefix filtering, delimiter/common-prefixes, pagination, and `start-after`, with
/// plaintext facts (size, ETag) reported for durable-mode (tombstoned) objects.
#[tokio::test]
async fn list_objects() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let bodies = [("a/1", 10usize), ("a/2", 20), ("b/1", 30)];
    for (k, n) in bodies {
        put(&client, B, k, &pattern(n)).await;
    }

    // Full listing reports all three with correct plaintext sizes and ETags.
    let out = client
        .list_objects_v2()
        .bucket(B)
        .send()
        .await
        .expect("list");
    let objs = out.contents();
    assert_eq!(objs.len(), 3, "all three keys must list");
    for o in objs {
        let (_, want) = bodies.iter().find(|(k, _)| *k == o.key().unwrap()).unwrap();
        assert_eq!(
            o.size(),
            Some(*want as i64),
            "plaintext size for {:?}",
            o.key()
        );
        assert_eq!(
            o.e_tag().unwrap().trim_matches('"'),
            md5_hex(&pattern(*want)),
            "plaintext ETag for {:?}",
            o.key()
        );
    }

    // Prefix.
    assert_eq!(list_keys(&client, B, Some("a/")).await, vec!["a/1", "a/2"]);

    // Delimiter → common prefixes, no contents.
    let d = client
        .list_objects_v2()
        .bucket(B)
        .delimiter("/")
        .send()
        .await
        .expect("delimited list");
    let mut cps: Vec<String> = d
        .common_prefixes()
        .iter()
        .filter_map(|c| c.prefix().map(str::to_string))
        .collect();
    cps.sort();
    assert_eq!(cps, vec!["a/", "b/"]);
    assert!(
        d.contents().is_empty(),
        "delimited list has no direct contents here"
    );

    // Pagination: one key per page walks the whole set.
    let mut seen = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut req = client.list_objects_v2().bucket(B).max_keys(1);
        if let Some(t) = &token {
            req = req.continuation_token(t.clone());
        }
        let page = req.send().await.expect("paged list");
        seen.extend(
            page.contents()
                .iter()
                .filter_map(|o| o.key().map(str::to_string)),
        );
        if page.is_truncated() != Some(true) {
            break;
        }
        token = page.next_continuation_token().map(str::to_string);
    }
    assert_eq!(
        seen,
        vec!["a/1", "a/2", "b/1"],
        "pagination must cover all keys in order"
    );

    // start-after skips up to and including its argument.
    let after = client
        .list_objects_v2()
        .bucket(B)
        .start_after("a/1")
        .send()
        .await
        .expect("start-after list");
    let keys: Vec<String> = after
        .contents()
        .iter()
        .filter_map(|o| o.key().map(str::to_string))
        .collect();
    assert_eq!(keys, vec!["a/2", "b/1"]);
}

/// LIST pagination under twin dilution. In durable mode every key is an eviction tombstone with a
/// facts twin beside it, so a raw backend page is ~half client-visible: a page may return **fewer**
/// than `MaxKeys` client keys (a short page — valid S3). Following the forwarded continuation token
/// until `IsTruncated` is false must still cover every key exactly once, in order, with no gaps or
/// repeats and never more than `MaxKeys` per page.
#[tokio::test]
async fn list_pagination_short_pages() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let n = 25usize;
    let mut expected: Vec<String> = (0..n).map(|i| format!("obj/{i:03}")).collect();
    expected.sort();
    for k in &expected {
        put(&client, B, k, &pattern(32)).await;
    }

    // Confirm the dilution is real: the cache holds ≥ 2n raw objects (tombstone + twin per key).
    let raw = raw_list(&h.raw(), &h.cache_bucket(B), None).await;
    assert!(
        raw.len() >= 2 * n,
        "expected twin dilution, got {} raw objects",
        raw.len()
    );

    for page_size in [1i32, 7, 10] {
        let mut collected = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = client.list_objects_v2().bucket(B).max_keys(page_size);
            if let Some(t) = &token {
                req = req.continuation_token(t.clone());
            }
            let page = req.send().await.expect("list page");
            let keys: Vec<String> = page
                .contents()
                .iter()
                .filter_map(|o| o.key().map(str::to_string))
                .collect();

            // Short pages are allowed (dilution), but never over MaxKeys, and KeyCount must be honest.
            assert!(
                keys.len() <= page_size as usize,
                "never exceed MaxKeys ({page_size})"
            );
            assert_eq!(
                page.key_count(),
                Some(keys.len() as i32),
                "KeyCount matches contents"
            );
            let more = page.is_truncated() == Some(true);
            // A truncated page must carry a token; a final page must not.
            assert_eq!(
                page.next_continuation_token().is_some(),
                more,
                "NextContinuationToken present iff truncated (size {page_size})"
            );

            collected.extend(keys);
            match page.next_continuation_token() {
                Some(t) if more => token = Some(t.to_string()),
                _ => break,
            }
        }
        assert_eq!(
            collected, expected,
            "page size {page_size}: every key exactly once, in order, no gap/dup/twin-leak"
        );
    }
}

/// Keys with control bytes and prefix-adjacent names round-trip byte-exact through PUT/GET, and
/// prefix-adjacent keys list in correct lexicographic order with their twins paired away.
#[tokio::test]
async fn control_byte_and_prefix_keys() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    // 0x00/0x01 are reserved (twin separator); every other byte is admissible on the write path,
    // where the key rides the percent-encoded URL. Includes bytes XML can't carry (0x02, 0x1f).
    // (No object-vs-`x/` pairs here: MinIO's single-drive backend won't keep a plain object `x`
    // alongside an object `x/y` — a backend quirk, not S3 semantics — so those live in the
    // ordering set below under a shared directory instead.)
    let keys = ["plain", "a!b", "tab\there", "ctrl\x1fx", "low\x02y"];
    for k in keys {
        let body = pattern(64 + k.len());
        put(&client, B, k, &body).await;
        assert_eq!(get_all(&client, B, k).await, body, "roundtrip key {k:?}");
        // HEAD must also handle the byte-exact key.
        client
            .head_object()
            .bucket(B)
            .key(k)
            .send()
            .await
            .unwrap_or_else(|e| panic!("head {k:?}: {e}"));
    }

    // Twin ordering with a base key that is a byte-prefix of a sibling: `d/a` sorts before `d/a!b`,
    // and `d/a`'s twin (`d/a` ‖ 0x01 ‖ facts) must sort between them (0x01 < '!' = 0x21) and be
    // paired away — never swallowing `d/a!b`. All three coexist (no plain object named `d`).
    for k in ["d/a", "d/a!b", "d/b"] {
        put(&client, B, k, &pattern(32)).await;
    }
    let ordered = list_keys(&client, B, Some("d/")).await;
    assert_eq!(
        ordered,
        vec!["d/a".to_string(), "d/a!b".to_string(), "d/b".to_string()],
        "prefix-adjacent keys must list in byte order with no twin leakage"
    );
}

/// Bucket lifecycle: create, HEAD, appears in ListBuckets, delete, then gone.
#[tokio::test]
async fn bucket_lifecycle() {
    let h = Harness::durable().await;
    let client = h.client();
    let bucket = "lifecycle";

    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("create");
    client
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("head existing bucket");

    let names: Vec<String> = client
        .list_buckets()
        .send()
        .await
        .expect("list buckets")
        .buckets()
        .iter()
        .filter_map(|b| b.name().map(str::to_string))
        .collect();
    assert!(
        names.contains(&bucket.to_string()),
        "bucket must appear under its client name"
    );

    client
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("delete bucket");
    let head = client.head_bucket().bucket(bucket).send().await;
    assert!(head.is_err(), "deleted bucket must not HEAD");
}

/// User metadata survives PUT→HEAD/GET, and never collides with the facts sharing the same cache
/// carrier — a client key named `plen` must not shadow the tombstone's own.
///
/// Non-ASCII values come back as an **RFC 2047 encoded-word**, which is what S3 implementations do
/// generally, not an s3s quirk: HTTP field values are US-ASCII (RFC 9110), so a UTF-8 metadata
/// value needs an escape hatch on the response. Measured against this harness's MinIO, driven
/// directly with no hypha in the path, the same value comes back `=?UTF-8?q?caf=C3=A9_=E2=98=95?=`
/// — the Q (quoted-printable) variant where s3s emits B (base64). Both are valid encoded-words
/// decoding to identical bytes. `aws-sdk-s3` encodes neither on request (it parses the value
/// straight into a `HeaderValue`) nor decodes on response, so a client sees the encoded form.
///
/// hypha owns none of that leg — it stores the value s3s hands it — so what this pins for hypha is
/// that the bytes survive the round trip intact, asserted by decoding the payload below.
#[tokio::test]
async fn user_metadata_roundtrips() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let body = pattern(4096);

    client
        .put_object()
        .bucket(B)
        .key("meta/obj")
        .body(bytes_body(&body))
        .content_length(body.len() as i64)
        .metadata("colour", "café ☕")
        .metadata("plain", "value")
        .metadata("plen", "not-the-facts-plen")
        .send()
        .await
        .expect("put with metadata");

    let expected = |md: &std::collections::HashMap<String, String>| {
        assert_eq!(md.get("plain").map(String::as_str), Some("value"));
        assert_eq!(
            md.get("plen").map(String::as_str),
            Some("not-the-facts-plen")
        );
        // s3s's RFC 2047 form; the payload is the original value byte-for-byte.
        let colour = md.get("colour").expect("non-ascii key present");
        assert_eq!(colour, "=?UTF-8?B?Y2Fmw6kg4piV?=");
        assert_eq!(
            String::from_utf8(
                base64_simd::STANDARD
                    .decode_to_vec(
                        colour
                            .trim_start_matches("=?UTF-8?B?")
                            .trim_end_matches("?=")
                            .as_bytes()
                    )
                    .expect("rfc2047 payload is base64")
            )
            .expect("utf-8"),
            "café ☕"
        );
        assert_eq!(
            md.len(),
            3,
            "hypha's own facts must not leak as client keys"
        );
    };

    let head = client
        .head_object()
        .bucket(B)
        .key("meta/obj")
        .send()
        .await
        .expect("head");
    expected(head.metadata().expect("head metadata"));
    // The facts riding the same carrier are unharmed by the colliding client key.
    assert_eq!(head.content_length(), Some(body.len() as i64));

    let got = client
        .get_object()
        .bucket(B)
        .key("meta/obj")
        .send()
        .await
        .expect("get");
    expected(got.metadata().expect("get metadata"));
    assert_eq!(got.body.collect().await.unwrap().to_vec(), body);

    // An object written without metadata reports none, not a stale or defaulted map.
    put(&client, B, "meta/bare", &body).await;
    let bare = client
        .head_object()
        .bucket(B)
        .key("meta/bare")
        .send()
        .await
        .expect("head bare");
    assert!(bare.metadata().is_none_or(|m| m.is_empty()));
}

/// A wrong `Content-MD5` is rejected with `BadDigest`, and — the part that matters — the commit
/// never lands: an existing object at the key is left fully intact (§7's transition bracket, whose
/// repair settles K back from the remote).
#[tokio::test]
async fn content_md5_is_validated() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let original = pattern(8192);
    let original_etag = put(&client, B, "digest/obj", &original).await;

    let body = pattern_seeded(4096, 9);
    let wrong = base64_md5(&pattern_seeded(4096, 200));
    let err = client
        .put_object()
        .bucket(B)
        .key("digest/obj")
        .body(bytes_body(&body))
        .content_length(body.len() as i64)
        .content_md5(wrong)
        .send()
        .await
        .expect_err("wrong Content-MD5 must be rejected");
    assert_eq!(sdk_err_code(&err).as_deref(), Some("BadDigest"));

    // The rejected write must not have replaced (or torn) the object already there.
    assert_eq!(get_all(&client, B, "digest/obj").await, original);
    let head = client
        .head_object()
        .bucket(B)
        .key("digest/obj")
        .send()
        .await
        .expect("head after rejected put");
    assert_eq!(
        head.e_tag().unwrap_or_default().trim_matches('"'),
        original_etag
    );

    // The matching digest goes through.
    client
        .put_object()
        .bucket(B)
        .key("digest/obj")
        .body(bytes_body(&body))
        .content_length(body.len() as i64)
        .content_md5(base64_md5(&body))
        .send()
        .await
        .expect("correct Content-MD5 must be accepted");
    assert_eq!(get_all(&client, B, "digest/obj").await, body);
}

/// Storage class is an echoed label (§7): non-archive classes round-trip, the archive family is
/// refused, and an unset class reads back as STANDARD.
#[tokio::test]
async fn storage_class_passthrough() {
    use aws_sdk_s3::types::StorageClass;

    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let body = pattern(1024);

    client
        .put_object()
        .bucket(B)
        .key("sc/ia")
        .body(bytes_body(&body))
        .content_length(body.len() as i64)
        .storage_class(StorageClass::StandardIa)
        .send()
        .await
        .expect("put with storage class");

    let head = client
        .head_object()
        .bucket(B)
        .key("sc/ia")
        .send()
        .await
        .expect("head");
    assert_eq!(head.storage_class(), Some(&StorageClass::StandardIa));
    let got = client
        .get_object()
        .bucket(B)
        .key("sc/ia")
        .send()
        .await
        .expect("get");
    assert_eq!(got.storage_class(), Some(&StorageClass::StandardIa));

    // Archive classes imply RestoreObject, which one physical tier cannot honour.
    for archive in [StorageClass::Glacier, StorageClass::DeepArchive] {
        let err = client
            .put_object()
            .bucket(B)
            .key("sc/archive")
            .body(bytes_body(&body))
            .content_length(body.len() as i64)
            .storage_class(archive.clone())
            .send()
            .await
            .expect_err("archive storage class must be refused");
        assert_eq!(sdk_err_code(&err).as_deref(), Some("InvalidStorageClass"));
    }

    put(&client, B, "sc/default", &body).await;
    let head = client
        .head_object()
        .bucket(B)
        .key("sc/default")
        .send()
        .await
        .expect("head default");
    assert_eq!(head.storage_class(), Some(&StorageClass::Standard));
}

// ── helpers ──────────────────────────────────────────────────────────────────────────────────

/// Client-visible keys, optionally prefix-filtered, in listing order.
async fn list_keys(client: &aws_sdk_s3::Client, bucket: &str, prefix: Option<&str>) -> Vec<String> {
    let mut req = client.list_objects_v2().bucket(bucket);
    if let Some(p) = prefix {
        req = req.prefix(p);
    }
    req.send()
        .await
        .expect("list_objects_v2")
        .contents()
        .iter()
        .filter_map(|o| o.key().map(str::to_string))
        .collect()
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}
