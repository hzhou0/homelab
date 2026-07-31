//! Encryption envelope, authenticated trailer, and ranged-read support.
pub mod envelope;
pub mod offset;
pub mod stream;
pub mod trailer;

pub use envelope::Envelope;
pub use stream::{RangeReader, RangeSource};
pub use trailer::{
    decode_tail, encode_trailer, Footer, FooterKind, Tail, TrailerKey, FACTS_LEN, MAX_PARTS,
    MAX_TAIL_LEN, SINGLE_TRAILER_LEN, TAG_LEN, VERSION_LEN,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid age identity: {0}")]
    Identity(String),
    #[error(transparent)]
    Encrypt(#[from] age::EncryptError),
    #[error(transparent)]
    Decrypt(#[from] age::DecryptError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
