//! Age encryption with a deployment passphrase.
//!
//! File keys are wrapped by `age::scrypt` with the work factor **pinned to the minimum** —
//! the default auto-tunes toward roughly one second and 256 MiB per file, which is untenable for
//! small objects.

use std::io::{Read, Write};

use age::secrecy::SecretString;
use age::stream::{StreamReader, StreamWriter};
use futures::io::{AsyncRead, AsyncWrite};

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
    pub fn new(passphrase: &str) -> Result<Self, Error> {
        if passphrase.is_empty() {
            return Err(Error::Identity("empty passphrase".into()));
        }
        Ok(Self {
            passphrase: SecretString::from(passphrase.to_owned()),
            max_work_factor: PINNED_WORK_FACTOR,
        })
    }

    /// Each call gets an independent file key, allowing parallel multipart encryption.
    pub fn encrypt<W: Write>(&self, writer: W) -> Result<StreamWriter<W>, Error> {
        let mut recipient = age::scrypt::Recipient::new(self.passphrase.clone());
        recipient.set_work_factor(PINNED_WORK_FACTOR);
        let encryptor =
            age::Encryptor::with_recipients(std::iter::once(&recipient as &dyn age::Recipient))?;
        Ok(encryptor.wrap_output(writer)?)
    }

    pub async fn encrypt_async<W: AsyncWrite + Unpin>(
        &self,
        writer: W,
    ) -> Result<StreamWriter<W>, Error> {
        let mut recipient = age::scrypt::Recipient::new(self.passphrase.clone());
        recipient.set_work_factor(PINNED_WORK_FACTOR);
        let encryptor =
            age::Encryptor::with_recipients(std::iter::once(&recipient as &dyn age::Recipient))?;
        Ok(encryptor.wrap_async_output(writer).await?)
    }

    pub fn decrypt<R: Read>(&self, reader: R) -> Result<StreamReader<R>, Error> {
        let mut identity = age::scrypt::Identity::new(self.passphrase.clone());
        identity.set_max_work_factor(self.max_work_factor);
        let decryptor = age::Decryptor::new(reader)?;
        Ok(decryptor.decrypt(std::iter::once(&identity as &dyn age::Identity))?)
    }

    pub async fn decrypt_async<R: AsyncRead + Unpin>(
        &self,
        reader: R,
    ) -> Result<StreamReader<R>, Error> {
        let mut identity = age::scrypt::Identity::new(self.passphrase.clone());
        identity.set_max_work_factor(self.max_work_factor);
        let decryptor = age::Decryptor::new_async(reader).await?;
        Ok(decryptor.decrypt_async(std::iter::once(&identity as &dyn age::Identity))?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offset::{HLEN, PAYLOAD_NONCE, TAG};

    /// An empty plaintext encrypts to `header ‖ payload_nonce(16) ‖ one empty chunk (tag)`, so the
    /// header is what is left when those are taken off the end.
    fn header_of(env: &Envelope) -> Vec<u8> {
        let mut ct = Vec::new();
        let w = env.encrypt(&mut ct).unwrap();
        w.finish().unwrap();
        ct.truncate(ct.len() - (PAYLOAD_NONCE + TAG) as usize);
        ct
    }

    #[test]
    fn async_round_trip() {
        futures::executor::block_on(async {
            let env = Envelope::new("async round-trip passphrase").unwrap();
            let mut ciphertext = Vec::new();
            let mut writer = env.encrypt_async(&mut ciphertext).await.unwrap();
            futures::io::AsyncWriteExt::write_all(&mut writer, b"streamed through age async")
                .await
                .unwrap();
            futures::io::AsyncWriteExt::close(&mut writer)
                .await
                .unwrap();

            let mut reader = env.decrypt_async(&ciphertext[..]).await.unwrap();
            let mut plaintext = Vec::new();
            futures::io::AsyncReadExt::read_to_end(&mut reader, &mut plaintext)
                .await
                .unwrap();
            assert_eq!(plaintext, b"streamed through age async");
        });
    }

    /// Pins `HLEN`. age can't grease a scrypt sole-stanza header and the stanza is fixed-shape, so
    /// the header length is constant; if a future age changes it, this trips ⇒ bump the trailer
    /// version.
    #[test]
    fn hlen_is_constant() {
        let env = Envelope::new("hlen pinning test passphrase").unwrap();
        let hlen = header_of(&env).len() as u64;
        assert_eq!(
            hlen, HLEN,
            "age scrypt header length changed to {hlen}; bump the trailer version"
        );
    }

    /// The work factor **as emitted**, read off the stanza rather than off the value handed to the
    /// recipient. What this guards is a silent fallback to the crate's auto-tuned default: that costs
    /// ~1 s and ~256 MiB *per file*, which for a small-object namespace is not a slow path but an
    /// unusable one, and nothing else in the system would report it as anything but latency.
    ///
    /// `hlen_is_constant` would also trip on a two-digit exponent, but only as a side effect of the
    /// digit count — a fallback that happened to be one digit would pass it. This reads the number.
    #[test]
    fn the_emitted_stanza_carries_the_pinned_work_factor() {
        assert_eq!(
            PINNED_WORK_FACTOR, 1,
            "the pin is the smallest value age accepts; 0 panics"
        );
        let env = Envelope::new("work factor pinning test passphrase").unwrap();
        let header = String::from_utf8(header_of(&env)).expect("an age header is ASCII text");
        let stanza = header
            .lines()
            .find(|line| line.starts_with("-> scrypt "))
            .unwrap_or_else(|| panic!("no scrypt stanza in the emitted header:\n{header}"));

        // `-> scrypt <salt> <log_n>`: the sole stanza of a hypha file, hence the fixed shape HLEN
        // rests on.
        let fields: Vec<&str> = stanza.split(' ').collect();
        assert_eq!(fields.len(), 4, "unexpected stanza shape: {stanza:?}");
        assert_eq!(
            fields[3],
            PINNED_WORK_FACTOR.to_string(),
            "the emitted work factor is not the pinned one: {stanza:?}"
        );
    }

    /// The decryption bound, from the side that matters: a file demanding more work than hypha ever
    /// emits is refused rather than honoured, so a corrupted or foreign work factor cannot stall a GET
    /// for seconds (§6). Asserted against a file *this* crate can otherwise read — same passphrase,
    /// same format — so the only reason it fails is the bound.
    #[test]
    fn a_file_over_the_work_factor_bound_is_refused() {
        use std::io::{Read, Write};

        let passphrase = "work factor bound test passphrase";
        let mut recipient = age::scrypt::Recipient::new(SecretString::from(passphrase.to_owned()));
        recipient.set_work_factor(PINNED_WORK_FACTOR + 1);
        let mut ct = Vec::new();
        let mut w =
            age::Encryptor::with_recipients(std::iter::once(&recipient as &dyn age::Recipient))
                .unwrap()
                .wrap_output(&mut ct)
                .unwrap();
        w.write_all(b"costlier than we ever emit").unwrap();
        w.finish().unwrap();

        let env = Envelope::new(passphrase).unwrap();
        let read = env
            .decrypt(&ct[..])
            .and_then(|mut r| r.read_to_end(&mut Vec::new()).map_err(Error::Io));
        assert!(
            read.is_err(),
            "a work factor above the bound must be refused, not honoured"
        );
    }
}
