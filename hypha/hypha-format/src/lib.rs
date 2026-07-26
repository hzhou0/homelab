//! The hypha encryption envelope: a thin wrapper around the `age` crate plus the
//! offset arithmetic and seekable-reader adapter that ranged GETs need.
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

// transparent so a printed error chain doesn't repeat the inner error's prefix.
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
