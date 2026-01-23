use core::hydradb::HydraDBBuilder;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::Rng;
use std::fs;

fn setup() -> core::hydradb::HydraDB {
    // entry = 16 (header) + 9 (key "key-00000") + 7 (value "v-0") ≈ 32 bytes
    // 100,000 * 32 bytes ≈ 3.2 MB
    let file_size = 3_200_000;

    let mut db = HydraDBBuilder::new()
        .with_cask("get_bench_data")
        .with_file_limit(file_size)
        .build()
        .unwrap();

    for i in 0..100_000 {
        db.put(format!("key-{}", i), "value").unwrap();
    }

    db
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_operations");
    group.sample_size(10);
    group.bench_function("get 100k entries", |b| {
        b.iter_batched(
            || setup(),
            |db| {
                let mut rng = rand::rng();

                for _ in 0..100_000 {
                    let random_index = rng.random_range(0..100_000);
                    let key = format!("key-{}", random_index);

                    let val = db.get(&key).unwrap();

                    std::hint::black_box(val);
                }
            },
            criterion::BatchSize::LargeInput,
        )
    });
    group.finish();

    let _ = fs::remove_dir_all("get_bench_data");
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
