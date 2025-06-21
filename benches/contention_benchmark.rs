//! Measure lock-contention throughput with many concurrent readers & writers.
//!
//!   $ cargo bench --bench contention_benchmark
//!
//! Toggle your lock implementation with a feature flag, build profile
//! setting or Cargo alias (e.g. `cargo bench --features parking_lot`).

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main, measurement::WallTime};
use futures::future::join_all;
use rand::{Rng, rng};
use simd_r_drive::{DataStore, traits::DataStoreWriter};
use std::{path::PathBuf, sync::Arc};
use tempfile::tempdir;
use tokio::runtime::Runtime;

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

const PAYLOAD_SIZES: [usize; 3] = [128, 4_096, 64 * 1024]; // 128 B, 4 KiB, 64 KiB
const THREADS: usize = 8;
const WRITES_PER_THREAD: usize = 1_000;

// ---------------------------------------------------------------------------
// Benchmark
// ---------------------------------------------------------------------------

fn contention_bench(c: &mut Criterion<WallTime>) {
    let mut group = c.benchmark_group("writers_vs_lock");
    group.sample_size(10); // fewer, longer runs → cleaner flamegraphs

    // One Tokio runtime that we reuse for every sample
    let rt = Runtime::new().unwrap();

    for &len in &PAYLOAD_SIZES {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{len}_bytes")),
            &len,
            |b, &len| {
                b.iter(|| {
                    rt.block_on(async move {
                        // ----- one fresh store per *iteration* -----
                        let dir = tempdir().unwrap();
                        let path = PathBuf::from(dir.path()).join("bench.bin");
                        let store = Arc::new(DataStore::open(&path).unwrap());

                        // ----- spawn N independent writer tasks -----
                        let mut handles = Vec::with_capacity(THREADS);
                        for t in 0..THREADS {
                            let s = store.clone();
                            handles.push(tokio::spawn(async move {
                                let mut rng = rng();
                                for i in 0..WRITES_PER_THREAD {
                                    let key = format!("t{t}_{i}");
                                    // random payload prevents easy compression
                                    let payload: Vec<u8> = (0..len).map(|_| rng.random()).collect();
                                    s.write(key.as_bytes(), &payload).unwrap();
                                }
                            }));
                        }

                        // wait for all tasks to finish
                        join_all(handles).await;
                    });
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

criterion_group!(benches, contention_bench);
criterion_main!(benches);
