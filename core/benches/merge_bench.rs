use core::hydradb::HydraDBBuilder;
use criterion::{Criterion, criterion_group, criterion_main};
use std::fs;

fn setup() -> core::hydradb::HydraDB {
    // entry = 16 (header) + 9 (key "key-00000") + 7 (value "v-0") ≈ 32 bytes
    // 100,000 * 32 bytes ≈ 3.2 MB
    let entry_per_file = 100_000;
    let file_size = 3_200_000;

    let mut db = HydraDBBuilder::new()
        .with_cask("merge_bench_data")
        .with_file_limit(file_size)
        .build()
        .unwrap();

    // we loop 10 times. each time we update the same 100k keys.
    // file 0: key-0="val-0" ...
    // file 1: key-0="val-1" ... (file 0 is now stale)
    for i in 0..10 {
        for j in 0..entry_per_file {
            let key = format!("key-{:05}", j);
            let val = format!("val-{}", i);
            db.put(key, val).unwrap();
        }
    }

    db
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_operations");
    group.sample_size(10);
    group.bench_function("merge 1 million entries", |b| {
        b.iter_batched(
            || setup(),
            |db| db.merge(),
            criterion::BatchSize::LargeInput,
        )
    });
    group.finish();

    let _ = fs::remove_dir_all("merge_bench_data");
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
