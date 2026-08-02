use std::collections::HashMap;

use hypha_core::error::Error;
use hypha_format::{ChecksumAlgorithm, ChecksumKind, StoredChecksum};
use s3s::checksum::ChecksumHasher;
use s3s::crypto::{Checksum as _, Crc32, Crc32c, Crc64Nvme, Sha1, Sha256};
use s3s::dto::{self, Checksum};
use s3s::{s3_error, S3Result};

const MPU_ALGORITHM: &str = "mpu-ca";
const MPU_TYPE: &str = "mpu-ct";

#[derive(Clone, Copy, Debug)]
pub(crate) struct MultipartChecksum {
    pub algorithm: ChecksumAlgorithm,
    pub kind: ChecksumKind,
}

impl MultipartChecksum {
    pub(crate) fn from_create(input: &dto::CreateMultipartUploadInput) -> S3Result<Option<Self>> {
        let Some(algorithm) = input.checksum_algorithm.as_ref() else {
            if input.checksum_type.is_some() {
                return Err(s3_error!(
                    InvalidRequest,
                    "checksum type requires a checksum algorithm"
                ));
            }
            return Ok(None);
        };
        let algorithm = parse_algorithm(algorithm)?;
        let kind = match input.checksum_type.as_ref().map(|kind| kind.as_str()) {
            Some(dto::ChecksumType::COMPOSITE) => ChecksumKind::Composite,
            Some(dto::ChecksumType::FULL_OBJECT) => ChecksumKind::FullObject,
            None if algorithm == ChecksumAlgorithm::Crc64Nvme => ChecksumKind::FullObject,
            None => ChecksumKind::Composite,
            _ => unreachable!(),
        };
        if (kind == ChecksumKind::FullObject
            && matches!(
                algorithm,
                ChecksumAlgorithm::Sha1 | ChecksumAlgorithm::Sha256
            ))
            || (kind == ChecksumKind::Composite && algorithm == ChecksumAlgorithm::Crc64Nvme)
        {
            return Err(s3_error!(
                InvalidRequest,
                "checksum algorithm does not support the requested multipart checksum type"
            ));
        }
        Ok(Some(Self { algorithm, kind }))
    }

    pub(crate) fn from_metadata(md: &HashMap<String, String>) -> S3Result<Option<Self>> {
        let Some(algorithm) = md.get(MPU_ALGORITHM) else {
            return Ok(None);
        };
        let kind = match md.get(MPU_TYPE).map(String::as_str) {
            Some(dto::ChecksumType::COMPOSITE) => ChecksumKind::Composite,
            Some(dto::ChecksumType::FULL_OBJECT) => ChecksumKind::FullObject,
            _ => {
                return Err(Error::Backend("multipart checksum record is malformed".into()).into())
            }
        };
        Ok(Some(Self {
            algorithm: parse_algorithm(&dto::ChecksumAlgorithm::from(algorithm.clone()))?,
            kind,
        }))
    }

    pub(crate) fn store(self, md: &mut HashMap<String, String>) {
        md.insert(
            MPU_ALGORITHM.to_string(),
            algorithm_dto(self.algorithm).as_str().to_string(),
        );
        md.insert(
            MPU_TYPE.to_string(),
            kind_dto(self.kind).as_str().to_string(),
        );
    }

    pub(crate) fn encode_part(checksum: &StoredChecksum) -> String {
        debug_assert_eq!(checksum.kind, ChecksumKind::FullObject);
        base64_simd::URL_SAFE_NO_PAD.encode_to_string(&checksum.value)
    }

    pub(crate) fn decode_part(self, encoded: &str) -> Option<StoredChecksum> {
        let value = base64_simd::URL_SAFE_NO_PAD
            .decode_to_vec(encoded.as_bytes())
            .ok()?;
        StoredChecksum::new(self.algorithm, ChecksumKind::FullObject, value)
    }

    pub(crate) fn completed_part(self, part: &dto::CompletedPart) -> S3Result<Vec<u8>> {
        let value = match self.algorithm {
            ChecksumAlgorithm::Crc32 => part.checksum_crc32.as_deref(),
            ChecksumAlgorithm::Crc32c => part.checksum_crc32c.as_deref(),
            ChecksumAlgorithm::Crc64Nvme => part.checksum_crc64nvme.as_deref(),
            ChecksumAlgorithm::Sha1 => part.checksum_sha1.as_deref(),
            ChecksumAlgorithm::Sha256 => part.checksum_sha256.as_deref(),
        }
        .ok_or_else(|| s3_error!(InvalidPart, "part entry is missing its checksum"))?;
        decode_value(self.algorithm, value)
    }

    pub(crate) fn upload_part_request(
        policy: Option<Self>,
        input: &dto::UploadPartInput,
    ) -> S3Result<Option<RequestedChecksum>> {
        match (policy, upload_part_request(input)?) {
            (None, None) => Ok(None),
            (Some(policy), Some(request)) if policy.algorithm == request.algorithm => {
                Ok(Some(request))
            }
            (Some(_), None) => Err(s3_error!(
                InvalidRequest,
                "UploadPart must provide the upload's checksum algorithm"
            )),
            _ => Err(s3_error!(
                BadDigest,
                "part checksum algorithm does not match the upload"
            )),
        }
    }

    pub(crate) fn complete(
        policy: Option<Self>,
        input: &dto::CompleteMultipartUploadInput,
        parts: &[StoredChecksum],
        part_lengths: &[u64],
        part_count: u32,
    ) -> S3Result<Option<StoredChecksum>> {
        let stored = policy
            .map(|policy| match policy.kind {
                ChecksumKind::Composite => composite(policy.algorithm, parts),
                ChecksumKind::FullObject => combine_crc(policy.algorithm, parts, part_lengths),
            })
            .transpose()?;
        validate_complete(input, stored.as_ref(), part_count)?;
        Ok(stored)
    }
}

fn validate_complete(
    input: &dto::CompleteMultipartUploadInput,
    stored: Option<&StoredChecksum>,
    part_count: u32,
) -> S3Result<()> {
    let supplied = [
        (ChecksumAlgorithm::Crc32, input.checksum_crc32.as_deref()),
        (ChecksumAlgorithm::Crc32c, input.checksum_crc32c.as_deref()),
        (
            ChecksumAlgorithm::Crc64Nvme,
            input.checksum_crc64nvme.as_deref(),
        ),
        (ChecksumAlgorithm::Sha1, input.checksum_sha1.as_deref()),
        (ChecksumAlgorithm::Sha256, input.checksum_sha256.as_deref()),
    ];
    let values = supplied
        .into_iter()
        .filter_map(|(algorithm, value)| value.map(|value| (algorithm, value)))
        .collect::<Vec<_>>();
    if values.len() > 1 {
        return Err(s3_error!(
            InvalidRequest,
            "exactly one checksum may be supplied"
        ));
    }
    match (stored, values.first()) {
        (None, None) => Ok(()),
        (None, Some(_)) => Err(s3_error!(
            InvalidRequest,
            "completion checksum requires a checksummed upload"
        )),
        (Some(_), None) => Ok(()),
        (Some(stored), Some((algorithm, encoded))) => {
            if stored.algorithm != *algorithm
                || input
                    .checksum_type
                    .as_ref()
                    .is_some_and(|kind| kind.as_str() != kind_dto(stored.kind).as_str())
            {
                return Err(s3_error!(
                    BadDigest,
                    "completion checksum does not match the upload"
                ));
            }
            let suffix = format!("-{part_count}");
            let raw = match stored.kind {
                ChecksumKind::Composite => encoded.strip_suffix(&suffix).ok_or_else(|| {
                    s3_error!(BadDigest, "composite checksum has the wrong part count")
                })?,
                ChecksumKind::FullObject => encoded,
            };
            if decode_value(*algorithm, raw)? != stored.value {
                return Err(s3_error!(BadDigest, "completion checksum does not match"));
            }
            Ok(())
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RequestedChecksum {
    pub algorithm: ChecksumAlgorithm,
    expected: Option<Vec<u8>>,
}

impl RequestedChecksum {
    pub(crate) fn computed(algorithm: ChecksumAlgorithm) -> Self {
        Self {
            algorithm,
            expected: None,
        }
    }
    pub(crate) fn verify(&self, checksum: &StoredChecksum) -> S3Result<()> {
        if self
            .expected
            .as_ref()
            .is_some_and(|expected| expected != &checksum.value)
        {
            return Err(s3_error!(
                BadDigest,
                "checksum does not match the request body"
            ));
        }
        Ok(())
    }

    pub(crate) fn backend(&self) -> hypha_core::backend::PutChecksum {
        hypha_core::backend::PutChecksum {
            algorithm: self.algorithm,
            value: self
                .expected
                .as_ref()
                .map(|value| base64_simd::STANDARD.encode_to_string(value)),
        }
    }
}

pub(crate) fn put_request(input: &dto::PutObjectInput) -> S3Result<Option<RequestedChecksum>> {
    request(
        input.checksum_algorithm.as_ref(),
        [
            (ChecksumAlgorithm::Crc32, input.checksum_crc32.as_deref()),
            (ChecksumAlgorithm::Crc32c, input.checksum_crc32c.as_deref()),
            (
                ChecksumAlgorithm::Crc64Nvme,
                input.checksum_crc64nvme.as_deref(),
            ),
            (ChecksumAlgorithm::Sha1, input.checksum_sha1.as_deref()),
            (ChecksumAlgorithm::Sha256, input.checksum_sha256.as_deref()),
        ],
    )
}

pub(crate) fn upload_part_request(
    input: &dto::UploadPartInput,
) -> S3Result<Option<RequestedChecksum>> {
    request(
        input.checksum_algorithm.as_ref(),
        [
            (ChecksumAlgorithm::Crc32, input.checksum_crc32.as_deref()),
            (ChecksumAlgorithm::Crc32c, input.checksum_crc32c.as_deref()),
            (
                ChecksumAlgorithm::Crc64Nvme,
                input.checksum_crc64nvme.as_deref(),
            ),
            (ChecksumAlgorithm::Sha1, input.checksum_sha1.as_deref()),
            (ChecksumAlgorithm::Sha256, input.checksum_sha256.as_deref()),
        ],
    )
}

fn request(
    declared: Option<&dto::ChecksumAlgorithm>,
    supplied: [(ChecksumAlgorithm, Option<&str>); 5],
) -> S3Result<Option<RequestedChecksum>> {
    let declared = declared.map(parse_algorithm).transpose()?;
    let mut values = supplied
        .into_iter()
        .filter_map(|(algorithm, value)| value.map(|value| (algorithm, value)));
    let supplied = values.next();
    if values.next().is_some() {
        return Err(s3_error!(
            InvalidRequest,
            "exactly one checksum value may be supplied"
        ));
    }
    let algorithm = match (declared, supplied.as_ref()) {
        (None, None) => return Ok(None),
        (Some(algorithm), None) => algorithm,
        (None, Some((algorithm, _))) => *algorithm,
        (Some(declared), Some((supplied, _))) if declared == *supplied => declared,
        _ => {
            return Err(s3_error!(
                BadDigest,
                "checksum value does not match the declared algorithm"
            ))
        }
    };
    let expected = supplied
        .map(|(_, value)| decode_value(algorithm, value))
        .transpose()?;
    Ok(Some(RequestedChecksum {
        algorithm,
        expected,
    }))
}

pub(crate) fn parse_algorithm(algorithm: &dto::ChecksumAlgorithm) -> S3Result<ChecksumAlgorithm> {
    match algorithm.as_str() {
        dto::ChecksumAlgorithm::CRC32 => Ok(ChecksumAlgorithm::Crc32),
        dto::ChecksumAlgorithm::CRC32C => Ok(ChecksumAlgorithm::Crc32c),
        dto::ChecksumAlgorithm::CRC64NVME => Ok(ChecksumAlgorithm::Crc64Nvme),
        dto::ChecksumAlgorithm::SHA1 => Ok(ChecksumAlgorithm::Sha1),
        dto::ChecksumAlgorithm::SHA256 => Ok(ChecksumAlgorithm::Sha256),
        _ => Err(s3_error!(InvalidRequest, "unsupported checksum algorithm")),
    }
}

pub(crate) fn algorithm_dto(algorithm: ChecksumAlgorithm) -> dto::ChecksumAlgorithm {
    dto::ChecksumAlgorithm::from_static(match algorithm {
        ChecksumAlgorithm::Crc32 => dto::ChecksumAlgorithm::CRC32,
        ChecksumAlgorithm::Crc32c => dto::ChecksumAlgorithm::CRC32C,
        ChecksumAlgorithm::Crc64Nvme => dto::ChecksumAlgorithm::CRC64NVME,
        ChecksumAlgorithm::Sha1 => dto::ChecksumAlgorithm::SHA1,
        ChecksumAlgorithm::Sha256 => dto::ChecksumAlgorithm::SHA256,
    })
}

pub(crate) fn kind_dto(kind: ChecksumKind) -> dto::ChecksumType {
    dto::ChecksumType::from_static(match kind {
        ChecksumKind::FullObject => dto::ChecksumType::FULL_OBJECT,
        ChecksumKind::Composite => dto::ChecksumType::COMPOSITE,
    })
}

pub(crate) fn decode_value(algorithm: ChecksumAlgorithm, encoded: &str) -> S3Result<Vec<u8>> {
    let raw = base64_simd::STANDARD
        .decode_to_vec(encoded.as_bytes())
        .map_err(|_| s3_error!(InvalidDigest, "checksum is not valid base64"))?;
    if raw.len() != algorithm.digest_len() {
        return Err(s3_error!(
            InvalidDigest,
            "checksum has the wrong digest length"
        ));
    }
    Ok(raw)
}

pub(crate) struct Hasher {
    algorithm: ChecksumAlgorithm,
    inner: ChecksumHasher,
}

impl Hasher {
    pub(crate) fn new(algorithm: ChecksumAlgorithm) -> Self {
        let inner = match algorithm {
            ChecksumAlgorithm::Crc32 => ChecksumHasher {
                crc32: Some(Crc32::new()),
                ..Default::default()
            },
            ChecksumAlgorithm::Crc32c => ChecksumHasher {
                crc32c: Some(Crc32c::new()),
                ..Default::default()
            },
            ChecksumAlgorithm::Crc64Nvme => ChecksumHasher {
                crc64nvme: Some(Crc64Nvme::new()),
                ..Default::default()
            },
            ChecksumAlgorithm::Sha1 => ChecksumHasher {
                sha1: Some(Sha1::new()),
                ..Default::default()
            },
            ChecksumAlgorithm::Sha256 => ChecksumHasher {
                sha256: Some(Sha256::new()),
                ..Default::default()
            },
        };
        Self { algorithm, inner }
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        self.inner.update(bytes);
    }

    pub(crate) fn finalize(self, kind: ChecksumKind) -> StoredChecksum {
        let checksum = self.inner.finalize();
        let encoded = match self.algorithm {
            ChecksumAlgorithm::Crc32 => checksum.checksum_crc32,
            ChecksumAlgorithm::Crc32c => checksum.checksum_crc32c,
            ChecksumAlgorithm::Crc64Nvme => checksum.checksum_crc64nvme,
            ChecksumAlgorithm::Sha1 => checksum.checksum_sha1,
            ChecksumAlgorithm::Sha256 => checksum.checksum_sha256,
        }
        .expect("the selected checksum hasher produces its digest");
        let value = base64_simd::STANDARD
            .decode_to_vec(encoded.as_bytes())
            .expect("ChecksumHasher emits base64");
        StoredChecksum::new(self.algorithm, kind, value)
            .expect("ChecksumHasher emits the algorithm's digest length")
    }
}

pub(crate) fn composite(
    algorithm: ChecksumAlgorithm,
    parts: &[StoredChecksum],
) -> S3Result<StoredChecksum> {
    if parts
        .iter()
        .any(|part| part.algorithm != algorithm || part.kind != ChecksumKind::FullObject)
    {
        return Err(s3_error!(
            InvalidPart,
            "part checksum does not match the upload"
        ));
    }
    let mut hasher = Hasher::new(algorithm);
    for part in parts {
        hasher.update(&part.value);
    }
    Ok(hasher.finalize(ChecksumKind::Composite))
}

pub(crate) fn combine_crc(
    algorithm: ChecksumAlgorithm,
    parts: &[StoredChecksum],
    part_lengths: &[u64],
) -> S3Result<StoredChecksum> {
    use crc_fast::CrcAlgorithm;

    if parts.len() != part_lengths.len() || parts.is_empty() {
        return Err(s3_error!(
            InvalidPart,
            "part checksum geometry is incomplete"
        ));
    }
    let crc_algorithm = match algorithm {
        ChecksumAlgorithm::Crc32 => CrcAlgorithm::Crc32IsoHdlc,
        ChecksumAlgorithm::Crc32c => CrcAlgorithm::Crc32Iscsi,
        ChecksumAlgorithm::Crc64Nvme => CrcAlgorithm::Crc64Nvme,
        _ => {
            return Err(s3_error!(
                InvalidRequest,
                "full-object multipart checksums require a CRC algorithm"
            ))
        }
    };
    let read = |part: &StoredChecksum| -> S3Result<u64> {
        if part.algorithm != algorithm || part.kind != ChecksumKind::FullObject {
            return Err(s3_error!(
                InvalidPart,
                "part checksum does not match the upload"
            ));
        }
        let mut bytes = [0u8; 8];
        bytes[8 - part.value.len()..].copy_from_slice(&part.value);
        Ok(u64::from_be_bytes(bytes))
    };
    let mut combined = read(&parts[0])?;
    for (part, length) in parts.iter().zip(part_lengths).skip(1) {
        combined = crc_fast::checksum_combine(crc_algorithm, combined, read(part)?, *length);
    }
    let bytes = combined.to_be_bytes();
    let value = bytes[8 - algorithm.digest_len()..].to_vec();
    Ok(
        StoredChecksum::new(algorithm, ChecksumKind::FullObject, value)
            .expect("combined CRC has the selected algorithm's width"),
    )
}

pub(crate) fn dto(checksum: &StoredChecksum, part_count: u32) -> Checksum {
    let mut encoded = base64_simd::STANDARD.encode_to_string(&checksum.value);
    if checksum.kind == ChecksumKind::Composite {
        encoded.push('-');
        encoded.push_str(&part_count.to_string());
    }
    let mut out = Checksum {
        checksum_type: Some(kind_dto(checksum.kind)),
        ..Default::default()
    };
    match checksum.algorithm {
        ChecksumAlgorithm::Crc32 => out.checksum_crc32 = Some(encoded),
        ChecksumAlgorithm::Crc32c => out.checksum_crc32c = Some(encoded),
        ChecksumAlgorithm::Crc64Nvme => out.checksum_crc64nvme = Some(encoded),
        ChecksumAlgorithm::Sha1 => out.checksum_sha1 = Some(encoded),
        ChecksumAlgorithm::Sha256 => out.checksum_sha256 = Some(encoded),
    }
    out
}

pub(crate) fn from_backend_put(
    output: &aws_sdk_s3::operation::put_object::PutObjectOutput,
    algorithm: ChecksumAlgorithm,
) -> S3Result<StoredChecksum> {
    let value = match algorithm {
        ChecksumAlgorithm::Crc32 => output.checksum_crc32(),
        ChecksumAlgorithm::Crc32c => output.checksum_crc32_c(),
        ChecksumAlgorithm::Crc64Nvme => output.checksum_crc64_nvme(),
        ChecksumAlgorithm::Sha1 => output.checksum_sha1(),
        ChecksumAlgorithm::Sha256 => output.checksum_sha256(),
    }
    .ok_or_else(|| {
        s3_error!(
            InternalError,
            "cache backend omitted the requested checksum"
        )
    })?;
    let value = decode_value(algorithm, value)?;
    Ok(
        StoredChecksum::new(algorithm, ChecksumKind::FullObject, value)
            .expect("decode_value checked the digest length"),
    )
}

pub(crate) fn from_backend_head(
    output: &aws_sdk_s3::operation::head_object::HeadObjectOutput,
) -> Option<StoredChecksum> {
    let values = [
        (ChecksumAlgorithm::Crc32, output.checksum_crc32()),
        (ChecksumAlgorithm::Crc32c, output.checksum_crc32_c()),
        (ChecksumAlgorithm::Crc64Nvme, output.checksum_crc64_nvme()),
        (ChecksumAlgorithm::Sha1, output.checksum_sha1()),
        (ChecksumAlgorithm::Sha256, output.checksum_sha256()),
    ];
    values.into_iter().find_map(|(algorithm, value)| {
        let value = decode_value(algorithm, value?).ok()?;
        StoredChecksum::new(algorithm, ChecksumKind::FullObject, value)
    })
}

pub(crate) fn from_backend_get(
    output: &aws_sdk_s3::operation::get_object::GetObjectOutput,
) -> Option<StoredChecksum> {
    let values = [
        (ChecksumAlgorithm::Crc32, output.checksum_crc32()),
        (ChecksumAlgorithm::Crc32c, output.checksum_crc32_c()),
        (ChecksumAlgorithm::Crc64Nvme, output.checksum_crc64_nvme()),
        (ChecksumAlgorithm::Sha1, output.checksum_sha1()),
        (ChecksumAlgorithm::Sha256, output.checksum_sha256()),
    ];
    values.into_iter().find_map(|(algorithm, value)| {
        let value = decode_value(algorithm, value?).ok()?;
        StoredChecksum::new(algorithm, ChecksumKind::FullObject, value)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_and_projects_full_checksum() {
        let mut hasher = Hasher::new(ChecksumAlgorithm::Sha256);
        hasher.update(b"hello");
        let stored = hasher.finalize(ChecksumKind::FullObject);
        assert_eq!(
            dto(&stored, 1).checksum_sha256.as_deref(),
            Some("LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=")
        );
    }

    #[test]
    fn composite_value_has_part_count_suffix() {
        let parts = [b"one".as_slice(), b"two".as_slice()]
            .into_iter()
            .map(|body| {
                let mut hasher = Hasher::new(ChecksumAlgorithm::Sha1);
                hasher.update(body);
                hasher.finalize(ChecksumKind::FullObject)
            })
            .collect::<Vec<_>>();
        let stored = composite(ChecksumAlgorithm::Sha1, &parts).unwrap();
        assert!(dto(&stored, 2).checksum_sha1.unwrap().ends_with("-2"));
    }
}
