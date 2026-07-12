//! Encrypt/decrypt throughput — sets the §5 spawn_blocking offload threshold empirically.

use std::io::{Read, Write};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use hypha_format::Envelope;

fn bench_codec(c: &mut Criterion) {
    let envelope = Envelope::generate();
    let mut g = c.benchmark_group("codec");
    for size in [64 * 1024, 1024 * 1024, 8 * 1024 * 1024] {
        let pt = vec![0xA5u8; size];
        let mut ct_buf = Vec::with_capacity(size + size / 32);
        g.throughput(Throughput::Bytes(size as u64));
        g.bench_with_input(BenchmarkId::new("encrypt", size), &pt, |b, pt| {
            b.iter(|| {
                ct_buf.clear();
                let mut w = envelope.encrypt(&mut ct_buf).unwrap();
                w.write_all(pt).unwrap();
                w.finish().unwrap();
            })
        });
        let ct = {
            let mut buf = Vec::new();
            let mut w = envelope.encrypt(&mut buf).unwrap();
            w.write_all(&pt).unwrap();
            w.finish().unwrap();
            buf
        };
        let mut pt_buf = Vec::with_capacity(size);
        g.bench_with_input(BenchmarkId::new("decrypt", size), &ct, |b, ct| {
            b.iter(|| {
                pt_buf.clear();
                envelope.decrypt(&ct[..])
                    .unwrap()
                    .read_to_end(&mut pt_buf)
                    .unwrap();
            })
        });
    }
    g.finish();
}

criterion_group!(benches, bench_codec);
criterion_main!(benches);
