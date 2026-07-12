//! Encryptor/Decryptor construction over age's native scrypt recipient (§6).
//!
//! File keys are wrapped by `age::scrypt` over a 256-bit random master passphrase with the work
//! factor **pinned to the minimum** — load-bearing, not an optimization: security lives in the
//! passphrase's 256 bits, stretching adds nothing, and the crate's default auto-tunes toward
//! ~1 s and ~256 MiB *per file*, fatal for a small-object namespace. Wholly stock age, so DR is
//! any age binary + the passphrase.

use std::io::{Read, Write};

use age::secrecy::{ExposeSecret, SecretString};
use age::stream::{StreamReader, StreamWriter};

use crate::Error;

/// `log_n = 1` ⇒ scrypt N = 2 — the smallest value `age::scrypt::Recipient` accepts (0 panics).
const PINNED_WORK_FACTOR: u8 = 1;

pub struct Envelope {
    passphrase: SecretString,
    /// Decryption bound: reject stanzas demanding more work than we ever emit, so a corrupted or
    /// foreign work factor can't stall a GET for seconds (§6).
    max_work_factor: u8,
}

impl Envelope {
    pub fn from_passphrase(s: &str) -> Result<Self, Error> {
        if s.is_empty() {
            return Err(Error::Identity("empty passphrase".into()));
        }
        Ok(Self {
            passphrase: SecretString::from(s.to_owned()),
            max_work_factor: PINNED_WORK_FACTOR,
        })
    }

    /// Random-passphrase envelope for tests/benches. Reuses age's own RNG (an x25519 secret's
    /// bech32 form) so the crate needs no direct `rand` dependency.
    pub fn generate() -> Self {
        let secret = age::x25519::Identity::generate();
        Self {
            passphrase: SecretString::from(secret.to_string().expose_secret().to_owned()),
            max_work_factor: PINNED_WORK_FACTOR,
        }
    }

    /// Wrap `writer` in an encrypting writer. Each call generates a fresh random file key —
    /// the coordination-free property parallel PUT/UploadPart workers rely on — wrapped by a
    /// scrypt stanza with a fresh salt and the pinned work factor.
    /// Callers must call `finish()` on the result to write the final (possibly short) chunk.
    ///
    /// The header length varies per file (random salt, and the scrypt label mechanism), so
    /// callers that need offset math later must record it — tee the ciphertext prefix through
    /// [`crate::offset::parse_header_len`] while uploading.
    pub fn encrypt<W: Write>(&self, writer: W) -> Result<StreamWriter<W>, Error> {
        let mut recipient = age::scrypt::Recipient::new(self.passphrase.clone());
        recipient.set_work_factor(PINNED_WORK_FACTOR);
        let encryptor =
            age::Encryptor::with_recipients(std::iter::once(&recipient as &dyn age::Recipient))?;
        Ok(encryptor.wrap_output(writer)?)
    }

    /// Decrypting reader over a full-file ciphertext stream. For ranged reads, hand it a
    /// [`crate::RangeReader`] and use `Seek` (see `stream.rs`).
    pub fn decrypt<R: Read>(&self, reader: R) -> Result<StreamReader<R>, Error> {
        let mut identity = age::scrypt::Identity::new(self.passphrase.clone());
        identity.set_max_work_factor(self.max_work_factor);
        let decryptor = age::Decryptor::new(reader)?;
        Ok(decryptor.decrypt(std::iter::once(&identity as &dyn age::Identity))?)
    }
}
