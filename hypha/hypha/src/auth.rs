//! `S3Auth` for hypha's *own* client credentials — the access-key/secret its S3 clients present,
//! distinct from the backend credentials in [`hypha_core::config::S3Endpoint`] (§2).

use s3s::auth::{S3Auth, SecretKey};
use s3s::{S3Result, S3ErrorCode, S3Error};

pub struct SingleKeyAuth {
    access_key: String,
    secret_key: SecretKey,
}

impl SingleKeyAuth {
    pub fn new(access_key: String, secret_key: String) -> Self {
        Self {
            access_key,
            secret_key: SecretKey::from(secret_key),
        }
    }
}

#[async_trait::async_trait]
impl S3Auth for SingleKeyAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        if access_key == self.access_key {
            Ok(self.secret_key.clone())
        } else {
            Err(S3Error::with_message(
                S3ErrorCode::InvalidAccessKeyId,
                "unknown access key",
            ))
        }
    }
}
