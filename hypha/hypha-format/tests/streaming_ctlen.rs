//! Guards the invariant hypha's streaming remote upload depends on: `age`'s `wrap_output` emits
//! the whole header **and** the 16-byte payload nonce before the first body byte, so the total
//! ciphertext length is `prefix + plen + chunks·TAG` — knowable without spilling, once the
//! (grease-randomized, hence unpredictable) header prefix has been length-measured.
//!
//! If a future `age` version writes the nonce lazily (on first body write instead of at
//! `wrap_output`), this fails and hypha's `ct_len` computation must change.

use std::cell::RefCell;
use std::io::{self, Write};
use std::rc::Rc;

use hypha_format::offset::{chunk_count, TAG};
use hypha_format::Envelope;

#[derive(Clone)]
struct SharedSink(Rc<RefCell<Vec<u8>>>);
impl Write for SharedSink {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn wrap_output_prefix_plus_payload_is_total_ciphertext() {
    let env = Envelope::generate();
    let mut distinct_header_lens = std::collections::HashSet::new();

    for plen in [0u64, 1, 100, 65_536, 70_000, 200_000] {
        let pt = vec![0x5Au8; plen as usize];
        for _ in 0..8 {
            let buf = Rc::new(RefCell::new(Vec::new()));
            let mut w = env.encrypt(SharedSink(buf.clone())).unwrap();
            // Prefix emitted before any body byte = header + 16-byte payload nonce.
            let prefix = buf.borrow().len() as u64;
            w.write_all(&pt).unwrap();
            w.finish().unwrap();
            let total = buf.borrow().len() as u64;

            // The load-bearing equation for the no-spill streaming upload.
            assert_eq!(
                total,
                prefix + plen + chunk_count(plen) * TAG,
                "wrap_output must emit header+nonce before body (plen={plen})"
            );
            distinct_header_lens.insert(prefix - plen.min(0)); // prefix is plen-independent here
        }
    }

    // Grease makes the header length vary run to run; if it were ever constant we could predict
    // ct_len in closed form, but it isn't — this documents why capture-and-measure is required.
    assert!(
        distinct_header_lens.len() > 1,
        "expected grease to vary the header length across runs"
    );
}
