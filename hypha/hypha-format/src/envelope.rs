//! Encryptor/Decryptor construction against hypha's static X25519 identity.

use std::io::{Read, Write};
use std::str::FromStr;

use age::stream::{StreamReader, StreamWriter};

use crate::Error;

pub struct Envelope {
    identity: age::x25519::Identity,
}

impl Envelope {
    pub fn from_identity_str(s: &str) -> Result<Self, Error> {
        let identity =
            age::x25519::Identity::from_str(s).map_err(|e| Error::Identity(e.to_string()))?;
        Ok(Self { identity })
    }

    pub fn generate() -> Self {
        Self {
            identity: age::x25519::Identity::generate(),
        }
    }

    pub fn recipient(&self) -> age::x25519::Recipient {
        self.identity.to_public()
    }

    /// Wrap `writer` in an encrypting writer. Each call generates a fresh random file key —
    /// the coordination-free property parallel PUT/UploadPart workers rely on.
    /// Callers must call `finish()` on the result to write the final (possibly short) chunk.
    ///
    /// The header length varies per file (rage greases headers with a random stanza), so
    /// callers that need offset math later must record it — tee the ciphertext prefix through
    /// [`crate::offset::parse_header_len`] while uploading.
    pub fn encrypt<W: Write>(&self, writer: W) -> Result<StreamWriter<W>, Error> {
        let recipient = self.recipient();
        let encryptor =
            age::Encryptor::with_recipients(std::iter::once(&recipient as &dyn age::Recipient))?;
        Ok(encryptor.wrap_output(writer)?)
    }

    /// Decrypting reader over a full-file ciphertext stream. For ranged reads, hand it a
    /// [`crate::RangeReader`] and use `Seek` (see `stream.rs`).
    pub fn decrypt<R: Read>(&self, reader: R) -> Result<StreamReader<R>, Error> {
        let decryptor = age::Decryptor::new(reader)?;
        Ok(decryptor.decrypt(std::iter::once(&self.identity as &dyn age::Identity))?)
    }
}
