mod common;

use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ChecksumType, CompletedMultipartUpload, CompletedPart,
};
use common::*;
use s3s::crypto::{Checksum as _, Crc64Nvme, Sha1, Sha256};

const B: &str = "checksum";

fn sha256(body: &[u8]) -> (Vec<u8>, String) {
    let raw = Sha256::checksum(body).to_vec();
    let encoded = base64_simd::STANDARD.encode_to_string(&raw);
    (raw, encoded)
}

fn sha1(body: &[u8]) -> String {
    base64_simd::STANDARD.encode_to_string(Sha1::checksum(body))
}

#[tokio::test]
async fn single_part_checksum_roundtrips_and_rejects_a_mismatch() {
    for (mode, h) in [
        ("durable", Harness::durable().await),
        ("cached", Harness::cached().await),
    ] {
        h.create_bucket(B).await;
        let client = h.client();
        let body = pattern_seeded(128 * 1024, 7);
        let (_, encoded) = sha256(&body);

        let put = client
            .put_object()
            .bucket(B)
            .key("single")
            .body(bytes_body(&body))
            .content_length(body.len() as i64)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(&encoded)
            .send()
            .await
            .expect("checksummed put");
        assert_eq!(put.checksum_sha256(), Some(encoded.as_str()));
        assert_eq!(put.checksum_type(), Some(&ChecksumType::FullObject));

        let head = client
            .head_object()
            .bucket(B)
            .key("single")
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .expect("checksummed head");
        assert_eq!(head.checksum_sha256(), Some(encoded.as_str()));

        let get = client
            .get_object()
            .bucket(B)
            .key("single")
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .expect("checksummed get");
        assert_eq!(get.checksum_sha256(), Some(encoded.as_str()));
        assert_eq!(get.body.collect().await.expect("body").to_vec(), body);

        let err = client
            .put_object()
            .bucket(B)
            .key("rejected")
            .body(bytes_body(b"actual"))
            .content_length(6)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(sha256(b"different").1)
            .send()
            .await
            .expect_err("mismatched checksum");
        assert_eq!(
            sdk_err_code(&err).as_deref(),
            Some("BadDigest"),
            "{mode}: {err}"
        );
    }
}

#[tokio::test]
async fn multipart_sha256_is_the_checksum_of_part_checksums() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "multipart";
    let p1 = pattern_seeded(MIN_PART, 1);
    let p2 = pattern_seeded(1024 * 1024, 2);
    let (raw1, sum1) = sha256(&p1);
    let (raw2, sum2) = sha256(&p2);

    let created = client
        .create_multipart_upload()
        .bucket(B)
        .key(key)
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .checksum_type(ChecksumType::Composite)
        .send()
        .await
        .expect("create checksummed mpu");
    let upload_id = created.upload_id().expect("upload id");

    let upload = |number, body: &[u8], sum: &str| {
        client
            .upload_part()
            .bucket(B)
            .key(key)
            .upload_id(upload_id)
            .part_number(number)
            .body(bytes_body(body))
            .content_length(body.len() as i64)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(sum)
            .send()
    };
    let u1 = upload(1, &p1, &sum1).await.expect("part 1");
    let u2 = upload(2, &p2, &sum2).await.expect("part 2");
    assert_eq!(u1.checksum_sha256(), Some(sum1.as_str()));
    assert_eq!(u2.checksum_sha256(), Some(sum2.as_str()));

    let mut joined = raw1;
    joined.extend_from_slice(&raw2);
    let (_, composite_raw) = sha256(&joined);
    let composite = format!("{composite_raw}-2");
    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(u1.e_tag().expect("etag 1"))
                .checksum_sha256(&sum1)
                .build(),
        )
        .parts(
            CompletedPart::builder()
                .part_number(2)
                .e_tag(u2.e_tag().expect("etag 2"))
                .checksum_sha256(&sum2)
                .build(),
        )
        .build();
    let out = client
        .complete_multipart_upload()
        .bucket(B)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed)
        .checksum_type(ChecksumType::Composite)
        .checksum_sha256(&composite)
        .send()
        .await
        .expect("complete checksummed mpu");
    assert_eq!(out.checksum_sha256(), Some(composite.as_str()));
    assert_eq!(out.checksum_type(), Some(&ChecksumType::Composite));

    let head = client
        .head_object()
        .bucket(B)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .expect("multipart head");
    assert_eq!(head.checksum_sha256(), Some(composite.as_str()));
}

#[tokio::test]
async fn copy_can_preserve_or_replace_a_checksum() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let body = pattern_seeded(MIN_PART + 17, 9);
    let (_, sha) = sha256(&body);
    let expected_sha1 = sha1(&body);
    let source = client
        .put_object()
        .bucket(B)
        .key("source")
        .body(bytes_body(&body))
        .content_length(body.len() as i64)
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .checksum_sha256(&sha)
        .send()
        .await
        .expect("source put");
    let source_etag = source.e_tag().expect("source etag");

    let preserved = client
        .copy_object()
        .bucket(B)
        .key("preserved")
        .copy_source(format!("{B}/source"))
        .send()
        .await
        .expect("preserving copy");
    assert_eq!(
        preserved
            .copy_object_result()
            .and_then(|result| result.checksum_sha256()),
        Some(sha.as_str())
    );
    let preserved_result = preserved.copy_object_result().expect("preserved result");
    assert_eq!(preserved_result.e_tag(), Some(source_etag));
    assert_eq!(
        preserved_result.checksum_type(),
        Some(&ChecksumType::FullObject)
    );
    let preserved_head = client
        .head_object()
        .bucket(B)
        .key("preserved")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .expect("preserved head");
    assert_eq!(preserved_head.e_tag(), Some(source_etag));
    assert_eq!(preserved_head.checksum_sha256(), Some(sha.as_str()));
    assert_eq!(
        preserved_head.checksum_type(),
        Some(&ChecksumType::FullObject)
    );
    assert_eq!(get_all(&client, B, "preserved").await, body);

    let changed = client
        .copy_object()
        .bucket(B)
        .key("changed")
        .copy_source(format!("{B}/source"))
        .checksum_algorithm(ChecksumAlgorithm::Sha1)
        .send()
        .await
        .expect("checksum-changing copy");
    let changed_result = changed.copy_object_result().expect("changed result");
    assert_eq!(changed_result.e_tag(), Some(source_etag));
    assert_eq!(changed_result.checksum_sha1(), Some(expected_sha1.as_str()));
    assert_eq!(
        changed_result.checksum_type(),
        Some(&ChecksumType::FullObject)
    );
    let changed_head = client
        .head_object()
        .bucket(B)
        .key("changed")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .expect("changed head");
    assert_eq!(changed_head.e_tag(), Some(source_etag));
    assert_eq!(changed_head.checksum_sha1(), Some(expected_sha1.as_str()));
    assert_eq!(
        changed_head.checksum_type(),
        Some(&ChecksumType::FullObject)
    );
    assert_eq!(get_all(&client, B, "changed").await, body);
}

#[tokio::test]
async fn upload_part_copy_calculates_the_uploads_checksum() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let body = pattern_seeded(1024 * 1024, 4);
    let (_, expected) = sha256(&body);
    put(&client, B, "part-source", &body).await;

    let created = client
        .create_multipart_upload()
        .bucket(B)
        .key("part-copy")
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .checksum_type(ChecksumType::Composite)
        .send()
        .await
        .expect("create mpu");
    let upload_id = created.upload_id().expect("upload id");
    let copied = client
        .upload_part_copy()
        .bucket(B)
        .key("part-copy")
        .upload_id(upload_id)
        .part_number(1)
        .copy_source(format!("{B}/part-source"))
        .send()
        .await
        .expect("upload part copy");
    let result = copied.copy_part_result().expect("copy result");
    assert_eq!(result.checksum_sha256(), Some(expected.as_str()));

    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(result.e_tag().expect("part etag"))
                .checksum_sha256(&expected)
                .build(),
        )
        .build();
    client
        .complete_multipart_upload()
        .bucket(B)
        .key("part-copy")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("complete copied part");
    assert_eq!(get_all(&client, B, "part-copy").await, body);
}

#[tokio::test]
async fn multipart_crc64_combines_to_a_full_object_checksum() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let p1 = pattern_seeded(MIN_PART, 11);
    let p2 = pattern_seeded(512 * 1024, 12);
    let whole = [p1.as_slice(), p2.as_slice()].concat();
    let encoded = |body: &[u8]| base64_simd::STANDARD.encode_to_string(Crc64Nvme::checksum(body));
    let sum1 = encoded(&p1);
    let sum2 = encoded(&p2);
    let expected = encoded(&whole);

    let created = client
        .create_multipart_upload()
        .bucket(B)
        .key("crc64")
        .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
        .checksum_type(ChecksumType::FullObject)
        .send()
        .await
        .expect("create crc64 mpu");
    let upload_id = created.upload_id().expect("upload id");
    let u1 = client
        .upload_part()
        .bucket(B)
        .key("crc64")
        .upload_id(upload_id)
        .part_number(1)
        .body(bytes_body(&p1))
        .content_length(p1.len() as i64)
        .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
        .checksum_crc64_nvme(&sum1)
        .send()
        .await
        .expect("crc64 part 1");
    let u2 = client
        .upload_part()
        .bucket(B)
        .key("crc64")
        .upload_id(upload_id)
        .part_number(2)
        .body(bytes_body(&p2))
        .content_length(p2.len() as i64)
        .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
        .checksum_crc64_nvme(&sum2)
        .send()
        .await
        .expect("crc64 part 2");
    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(u1.e_tag().expect("etag 1"))
                .checksum_crc64_nvme(&sum1)
                .build(),
        )
        .parts(
            CompletedPart::builder()
                .part_number(2)
                .e_tag(u2.e_tag().expect("etag 2"))
                .checksum_crc64_nvme(&sum2)
                .build(),
        )
        .build();
    let out = client
        .complete_multipart_upload()
        .bucket(B)
        .key("crc64")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .checksum_type(ChecksumType::FullObject)
        .checksum_crc64_nvme(&expected)
        .send()
        .await
        .expect("complete crc64 mpu");
    assert_eq!(out.checksum_crc64_nvme(), Some(expected.as_str()));
    assert_eq!(out.checksum_type(), Some(&ChecksumType::FullObject));
}
