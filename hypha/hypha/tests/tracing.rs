//! Per-request span fields with an isolated process-global subscriber.
//!
//! Worth a test for the reason the metrics endpoint is: the fields are recorded by name from several
//! places at once — the macro table declares them, the handlers fill `bytes` and `cache_hit` in — and
//! a name that stops matching produces a line that is merely missing a field, never an error.

mod common;

use std::io::Write;
use std::sync::{Arc, Mutex};

use common::*;

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("capture buffer")
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test]
async fn every_request_closes_a_span_carrying_its_own_fields() {
    let captured = Captured::default();
    tracing_subscriber::fmt()
        .json()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .with_max_level(tracing::Level::INFO)
        .with_writer(captured.clone())
        .init();

    let h = Harness::durable().await;
    let c = h.client();
    h.create_bucket("spans").await;
    put(&c, "spans", "deep/key", b"hello spans").await;
    // Durable mode tombstones on write, so this read resolves from the remote — a miss, which is the
    // field's more interesting value and the one no other assertion here would pin.
    get_all(&c, "spans", "deep/key").await;

    let log = String::from_utf8(captured.0.lock().expect("capture buffer").clone()).expect("utf-8");
    let line = |op: &str| {
        log.lines()
            .find(|l| l.contains(&format!("\"op\":\"{op}\"")))
            .unwrap_or_else(|| panic!("no closed span for {op} in:\n{log}"))
            .to_string()
    };

    let put_line = line("PutObject");
    assert!(put_line.contains("\"key\":\"deep/key\""), "{put_line}");
    assert!(put_line.contains("\"bucket\":\"spans\""), "{put_line}");
    assert!(put_line.contains("\"bytes\":11"), "{put_line}");

    let get_line = line("GetObject");
    assert!(get_line.contains("\"key\":\"deep/key\""), "{get_line}");
    assert!(get_line.contains("\"cache_hit\":false"), "{get_line}");
    assert!(get_line.contains("\"bytes\":11"), "{get_line}");

    // A bucket op has no key to record, and an empty string there would be a lie about the request.
    assert!(!line("CreateBucket").contains("\"key\""));
}
