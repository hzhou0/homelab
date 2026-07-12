//! Round-trip, adversarial, and offset-arithmetic properties, exercised through hypha's own
//! wrapper (not re-testing age itself): encrypt→decrypt identity; corrupt/truncate/splice must
//! fail authentication; the closed-form offset math must agree with real ciphertext produced by
//! the real crate — this is what pins the 64 KiB / 65552 constants.

use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use hypha_format::offset::{
    chunk_ciphertext_offset, ciphertext_len, ciphertext_range, CHUNK_CIPHERTEXT, CHUNK_PLAINTEXT,
    HLEN,
};
use hypha_format::{Envelope, RangeReader, RangeSource};
use proptest::prelude::*;

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn encrypt(env: &Envelope, plaintext: &[u8]) -> Vec<u8> {
    let mut ct = Vec::new();
    let mut w = env.encrypt(&mut ct).unwrap();
    w.write_all(plaintext).unwrap();
    w.finish().unwrap();
    ct
}

fn hlen(_ct: &[u8]) -> u64 {
    HLEN
}

fn decrypt(env: &Envelope, ciphertext: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut r = env.decrypt(ciphertext).map_err(std::io::Error::other)?;
    let mut pt = Vec::new();
    r.read_to_end(&mut pt)?;
    Ok(pt)
}

#[test]
fn ciphertext_length_matches_closed_form() {
    let env = Envelope::generate();
    // sizes straddling every interesting boundary
    for len in [
        0usize,
        1,
        CHUNK_PLAINTEXT as usize - 1,
        CHUNK_PLAINTEXT as usize,
        CHUNK_PLAINTEXT as usize + 1,
        2 * CHUNK_PLAINTEXT as usize,
        2 * CHUNK_PLAINTEXT as usize + 17,
    ] {
        let ct = encrypt(&env, &pattern(len));
        assert_eq!(
            ct.len() as u64,
            ciphertext_len(len as u64, hlen(&ct)),
            "closed-form ciphertext_len wrong for plaintext len {len}"
        );
    }
}

#[test]
fn corruption_fails_authentication() {
    let env = Envelope::generate();
    let pt = pattern(3 * CHUNK_PLAINTEXT as usize / 2);
    let ct = encrypt(&env, &pt);
    let h = hlen(&ct) as usize;

    // flip one payload byte in each chunk region
    for &at in &[
        h + 20,
        h + 16 + CHUNK_CIPHERTEXT as usize + 20,
        ct.len() - 1,
    ] {
        let mut bad = ct.clone();
        bad[at] ^= 0x01;
        assert!(
            decrypt(&env, &bad).is_err(),
            "corruption at byte {at} not detected"
        );
    }
}

#[test]
fn truncation_fails() {
    let env = Envelope::generate();
    let ct = encrypt(&env, &pattern(CHUNK_PLAINTEXT as usize + 100));
    // drop the final short chunk entirely, and separately drop one byte
    for cut in [ct.len() - 1, ct.len() - 100 - 16] {
        let bad = &ct[..cut];
        assert!(
            decrypt(&env, bad).is_err(),
            "truncation to {cut} bytes not detected"
        );
    }
}

#[test]
fn cross_file_chunk_splice_fails() {
    // Same recipient, two files ⇒ two random file keys. Transplant file B's first payload
    // chunk into file A: authentication must fail (key separation is the binding).
    let env = Envelope::generate();
    let pt = pattern(2 * CHUNK_PLAINTEXT as usize);
    let a = encrypt(&env, &pt);
    let b = encrypt(&env, &pt);
    // headers are the fixed HLEN; locate each file's chunk 0
    let (ha, hb) = (hlen(&a) as usize, hlen(&b) as usize);

    let mut spliced = a.clone();
    spliced[ha + 16..ha + 16 + CHUNK_CIPHERTEXT as usize]
        .copy_from_slice(&b[hb + 16..hb + 16 + CHUNK_CIPHERTEXT as usize]);
    assert!(
        decrypt(&env, &spliced).is_err(),
        "cross-file chunk splice not detected"
    );
}

#[test]
fn chunk_reorder_fails() {
    let env = Envelope::generate();
    let pt = pattern(3 * CHUNK_PLAINTEXT as usize);
    let ct = encrypt(&env, &pt);
    let h = hlen(&ct) as usize;

    let c = |i: usize| {
        (
            h + 16 + i * CHUNK_CIPHERTEXT as usize,
            h + 16 + (i + 1) * CHUNK_CIPHERTEXT as usize,
        )
    };
    let (a0, a1) = c(0);
    let (b0, b1) = c(1);
    let mut swapped = ct.clone();
    let tmp = swapped[a0..a1].to_vec();
    swapped.copy_within(b0..b1, a0);
    swapped[b0..b1].copy_from_slice(&tmp);
    assert!(
        decrypt(&env, &swapped).is_err(),
        "chunk reorder not detected"
    );
}

#[test]
fn wrong_identity_fails() {
    let ct = encrypt(&Envelope::generate(), &pattern(100));
    assert!(decrypt(&Envelope::generate(), &ct).is_err());
}

/// The invariant hypha's single-stream composite read rests on: a concatenation of independent
/// age files decrypts correctly by feeding each part's exact ciphertext window to a fresh
/// decryptor off *one* shared reader, via `by_ref().take(len)`. age's reader is EOF-delimited, so
/// a `Take` bounded to a part's window makes age stop at that part's final chunk and consume
/// precisely `len` bytes — leaving the shared stream aligned on the next part. Ragged part sizes
/// (empty, sub-chunk, multi-chunk, chunk-aligned) exercise the boundary cases.
#[test]
fn concatenated_parts_decrypt_in_one_stream() {
    let env = Envelope::generate();
    let plens = [
        0u64,
        1,
        CHUNK_PLAINTEXT - 1,
        CHUNK_PLAINTEXT,
        CHUNK_PLAINTEXT + 7,
        2 * CHUNK_PLAINTEXT,
    ];

    let mut blob = Vec::new();
    let mut ct_lens = Vec::new();
    let mut expected = Vec::new();
    for (i, &plen) in plens.iter().enumerate() {
        let pt: Vec<u8> = (0..plen).map(|b| (b as usize + i) as u8).collect();
        let part = encrypt(&env, &pt);
        assert_eq!(part.len() as u64, ciphertext_len(plen, HLEN));
        ct_lens.push(part.len() as u64);
        blob.extend_from_slice(&part);
        expected.extend_from_slice(&pt);
    }

    let mut cursor = std::io::Cursor::new(blob);
    let mut got = Vec::new();
    for &len in &ct_lens {
        let mut dec = env.decrypt(Read::by_ref(&mut cursor).take(len)).unwrap();
        dec.read_to_end(&mut got).unwrap();
    }
    assert_eq!(got, expected);
    // Every byte consumed, stream landed exactly at the end — no drift across parts.
    assert_eq!(cursor.position(), cursor.get_ref().len() as u64);
}

// --- ranged read via RangeReader + StreamReader::seek --------------------------------------

struct MemSource(Arc<Vec<u8>>);

impl RangeSource for MemSource {
    type Reader = std::io::Cursor<Vec<u8>>;
    fn len(&self) -> u64 {
        self.0.len() as u64
    }
    fn open_at(&mut self, offset: u64) -> std::io::Result<Self::Reader> {
        // clone-from-offset stands in for issuing a fresh byte-range GET
        Ok(std::io::Cursor::new(self.0[offset as usize..].to_vec()))
    }
}

#[test]
fn ranged_read_via_seek() {
    let env = Envelope::generate();
    let pt = pattern(3 * CHUNK_PLAINTEXT as usize + 12345);
    let ct = Arc::new(encrypt(&env, &pt));

    for (a, b) in [
        (0u64, 10u64),
        (CHUNK_PLAINTEXT - 5, CHUNK_PLAINTEXT + 5), // straddles a chunk boundary
        (2 * CHUNK_PLAINTEXT + 7, 3 * CHUNK_PLAINTEXT),
        (pt.len() as u64 - 20, pt.len() as u64), // tail, short final chunk
    ] {
        let mut r = env
            .decrypt(RangeReader::new(MemSource(ct.clone())))
            .unwrap();
        r.seek(SeekFrom::Start(a)).unwrap();
        let mut got = vec![0u8; (b - a) as usize];
        r.read_exact(&mut got).unwrap();
        assert_eq!(
            &got[..],
            &pt[a as usize..b as usize],
            "range [{a}, {b}) mismatch"
        );
    }
}

#[test]
fn ciphertext_range_covers_decryption_needs() {
    // The computed covering range, fetched together with the header prefix, must be exactly
    // enough to decrypt the plaintext range: verify byte identity via a source that panics if
    // read outside header ∪ covering-range.
    struct Restricted {
        data: Arc<Vec<u8>>,
        allowed_from: u64,
        allowed_to: u64,
        header_end: u64,
    }
    impl RangeSource for Restricted {
        type Reader = std::io::Cursor<Vec<u8>>;
        fn len(&self) -> u64 {
            self.data.len() as u64
        }
        fn open_at(&mut self, offset: u64) -> std::io::Result<Self::Reader> {
            assert!(
                offset < self.header_end
                    || (offset >= self.allowed_from && offset < self.allowed_to),
                "read outside computed covering range: offset {offset}"
            );
            Ok(std::io::Cursor::new(self.data[offset as usize..].to_vec()))
        }
    }

    let env = Envelope::generate();
    let pt = pattern(2 * CHUNK_PLAINTEXT as usize + 999);
    let ct = Arc::new(encrypt(&env, &pt));
    let h = hlen(&ct);

    let (a, b) = (CHUNK_PLAINTEXT + 100, 2 * CHUNK_PLAINTEXT + 200);
    let cover = ciphertext_range(a..b, pt.len() as u64, h);
    assert_eq!(cover.start, chunk_ciphertext_offset(1, h));

    let src = Restricted {
        data: ct.clone(),
        allowed_from: cover.start,
        allowed_to: cover.end,
        header_end: h + 16,
    };
    let mut r = env.decrypt(RangeReader::new(src)).unwrap();
    r.seek(SeekFrom::Start(a)).unwrap();
    let mut got = vec![0u8; (b - a) as usize];
    r.read_exact(&mut got).unwrap();
    assert_eq!(&got[..], &pt[a as usize..b as usize]);
}

// --- properties ------------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
    fn prop_roundtrip(pt in proptest::collection::vec(any::<u8>(), 0..200_000)) {
        let env = Envelope::generate();
        let ct = encrypt(&env, &pt);
        prop_assert_eq!(decrypt(&env, &ct).unwrap(), pt.clone());
        prop_assert_eq!(ct.len() as u64, ciphertext_len(pt.len() as u64, hlen(&ct)));
    }

    #[test]
    fn prop_ranged_read(
        len in 1usize..200_000,
        a_frac in 0.0f64..1.0,
        b_frac in 0.0f64..1.0,
    ) {
        let env = Envelope::generate();
        let pt = pattern(len);
        let ct = Arc::new(encrypt(&env, &pt));
        let (mut a, mut b) = (
            (a_frac * len as f64) as u64,
            (b_frac * len as f64) as u64,
        );
        if a > b { std::mem::swap(&mut a, &mut b); }

        let mut r = env.decrypt(RangeReader::new(MemSource(ct))).unwrap();
        r.seek(SeekFrom::Start(a)).unwrap();
        let mut got = vec![0u8; (b - a) as usize];
        r.read_exact(&mut got).unwrap();
        prop_assert_eq!(&got[..], &pt[a as usize..b as usize]);
    }
}
